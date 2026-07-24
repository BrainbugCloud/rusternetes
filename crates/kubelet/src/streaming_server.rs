//! Kubelet streaming server — proxies SPDY upgrade connections from the
//! api-server to the CRI runtime's streaming URL.
//!
//! Architecture (matching upstream kubelet):
//!   api-server  ──SPDY──▶  kubelet streaming server  ──SPDY──▶  CRI runtime
//!
//! The api-server receives WebSocket from kubectl, translates to SPDY, and
//! connects to the kubelet's streaming port (default 10250). This server
//! reads the HTTP upgrade request, calls CRI gRPC `Exec`/`Attach`/`PortForward`
//! to get a tokenised streaming URL, then opens a new SPDY connection to
//! that URL and relays bytes between the two connections.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use cri_proto::v1;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::cri::CriClient;

/// Start the streaming server on the given port. Returns immediately after
/// spawning the accept loop; the caller should `.await` the returned handle
/// if it wants to block.
///
/// This is the single kubelet serving port advertised in the node's
/// `DaemonEndpoints`. It handles exec/attach/portForward SPDY upgrades
/// directly (relaying to the CRI runtime's streaming URL) and transparently
/// proxies every other HTTP request (notably `GET /containerLogs/...`) to the
/// kubelet's plain HTTP API server on `http_forward_port`.
pub async fn start(cri: CriClient, port: u16, http_forward_port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind streaming server to {addr}"))?;
    info!("kubelet streaming server listening on {addr} (HTTP forward → :{http_forward_port})");

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let cri = cri.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, cri, http_forward_port).await {
                            debug!("streaming connection from {peer} ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("streaming accept error: {e}");
                }
            }
        }
    });

    Ok(())
}

/// Representations of what the api-server is asking for.
#[derive(Debug)]
enum StreamMethod {
    Exec,
    Attach,
    PortForward,
}

/// Parsed HTTP request from the api-server.
#[derive(Debug)]
struct ClientRequest {
    method: StreamMethod,
    namespace: String,
    pod: String,
    container: String,
    /// Raw URL-encoded query string (without leading `?`), e.g. `command=sh&stdin=1`.
    query: String,
    /// The full raw HTTP request head (request line + headers, ending with \r\n\r\n)
    /// so we can forward headers like `X-Stream-Protocol-Version` to the CRI runtime.
    head: Vec<u8>,
}

/// Parsed HTTP response from the CRI streaming server (the 101 Switching Protocols).
#[derive(Debug)]
struct BackendResponse {
    /// The full raw HTTP response head (status line + headers, ending with \r\n\r\n).
    head: Vec<u8>,
    /// Bytes already read from the backend that belong to the SPDY stream (fragments
    /// after the \r\n\r\n in the initial read buffer).
    spdy_tail: Vec<u8>,
}

// ── connection handler ────────────────────────────────────────────────

async fn handle_connection(
    mut client: TcpStream,
    cri: CriClient,
    http_forward_port: u16,
) -> Result<()> {
    // 1. Read the raw HTTP request head (byte at a time, so SPDY frames after
    //    the blank line stay unconsumed).
    let head = read_request_head(&mut client).await?;

    // Only exec/attach/portForward are handled by this streaming server.
    // Everything else (notably GET /containerLogs/...) is transparently
    // proxied to the kubelet's plain HTTP API server.
    if !is_streaming_request(&head) {
        return proxy_http(client, head, http_forward_port).await;
    }

    let client_req = parse_client_request(head)?;

    // 2. Find the container via CRI labels.
    let container = cri
        .find_container(
            &client_req.namespace,
            &client_req.pod,
            &client_req.container,
        )
        .await?
        .with_context(|| {
            format!(
                "container {} not found for pod {}/{}",
                client_req.container, client_req.namespace, client_req.pod
            )
        })?;

    // 3. Call CRI gRPC to get the streaming URL.
    let streaming_url = match &client_req.method {
        StreamMethod::Exec => {
            let (cmd, stdin, stdout, stderr, tty) = parse_exec_query(&client_req.query)?;
            let req = v1::ExecRequest {
                container_id: container.id.clone(),
                cmd,
                tty,
                stdin,
                stdout,
                stderr,
            };
            cri.exec(req).await?
        }
        StreamMethod::Attach => {
            let (stdin, stdout, stderr, tty) = parse_attach_query(&client_req.query)?;
            let req = v1::AttachRequest {
                container_id: container.id.clone(),
                stdin,
                stdout,
                stderr,
                tty,
            };
            cri.attach(req).await?
        }
        StreamMethod::PortForward => {
            let ports = parse_portforward_query(&client_req.query)?;
            let req = v1::PortForwardRequest {
                pod_sandbox_id: container.pod_sandbox_id.clone(),
                port: ports,
            };
            cri.port_forward(req).await?
        }
    };

    debug!("streaming URL from CRI: {streaming_url}");

    // 4. Connect to the CRI streaming server and do the SPDY handshake.
    let url = url::Url::parse(&streaming_url)
        .with_context(|| format!("invalid streaming URL: {streaming_url}"))?;
    let host = url.host_str().context("streaming URL has no host")?;
    let port = url.port().unwrap_or(80);
    let addr = format!("{host}:{port}");

    let mut backend = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect to CRI streaming at {addr}"))?;

    // 5. Send the upgrade request to the CRI streaming server.
    //    Reconstruct the HTTP request from the original request head,
    //    substituting the path with the CRI streaming token path.
    let backend_req = build_backend_request(&client_req.head, &url);
    backend
        .write_all(&backend_req)
        .await
        .context("failed to write upgrade request to CRI streaming")?;

    // 6. Read the 101 Switching Protocols response from the CRI server.
    let backend_resp = read_backend_response(&mut backend)
        .await
        .context("failed to read upgrade response from CRI streaming")?;

    // 7. Forward the 101 response to the api-server.
    client
        .write_all(&backend_resp.head)
        .await
        .context("failed to forward 101 response to api-server")?;

    // 8. Relay all remaining bytes bidirectionally (SPDY frames).
    relay(client, backend, backend_resp.spdy_tail).await;

    Ok(())
}

// ── HTTP request parsing ──────────────────────────────────────────────

/// Read the HTTP request head (byte at a time, so SPDY frames after the blank
/// line stay unconsumed). Returns the raw head bytes ending in `\r\n\r\n`.
async fn read_request_head(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut head = Vec::with_capacity(2048);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            anyhow::bail!("client closed before sending request");
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        if head.len() > 32 * 1024 {
            anyhow::bail!("request head too large");
        }
    }
    Ok(head)
}

/// True if the request targets an exec/attach/portForward streaming endpoint.
/// Any other request (e.g. `/containerLogs/...`) is proxied as plain HTTP.
fn is_streaming_request(head: &[u8]) -> bool {
    let text = String::from_utf8_lossy(head);
    let request_line = text.split("\r\n").next().unwrap_or_default();
    let raw_path = request_line.split(' ').nth(1).unwrap_or("/");
    let path = raw_path.split('?').next().unwrap_or(raw_path);
    let first = path.trim_matches('/').split('/').next().unwrap_or("");
    matches!(first, "exec" | "attach" | "portForward" | "portforward")
}

/// Transparently proxy a plain HTTP request (already-read head + no body, as
/// the streaming endpoints only ever receive GETs) to the kubelet's HTTP API
/// server, relaying the response back unchanged.
async fn proxy_http(mut client: TcpStream, head: Vec<u8>, forward_port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{forward_port}");
    let mut backend = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            let body = format!("kubelet HTTP API unavailable: {e}");
            let resp = format!(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = client.write_all(resp.as_bytes()).await;
            return Ok(());
        }
    };
    backend
        .write_all(&head)
        .await
        .with_context(|| format!("failed to forward request to kubelet HTTP API at {addr}"))?;
    // No SPDY tail to flush: relay bytes both ways until either side closes.
    relay(client, backend, Vec::new()).await;
    Ok(())
}

/// Parse the pre-read HTTP request head into route info. Retains the raw head
/// bytes so headers like `X-Stream-Protocol-Version` can be forwarded to CRI.
fn parse_client_request(head: Vec<u8>) -> Result<ClientRequest> {
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.split("\r\n");

    // Parse request line: GET /exec/namespace/pod/container?query HTTP/1.1
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let _http_method = parts.next().unwrap_or("GET");
    let raw_path = parts.next().unwrap_or("/");

    // Split path from query
    let (path, query) = match raw_path.split_once('?') {
        Some((p, q)) => (p, q.to_string()),
        None => (raw_path, String::new()),
    };

    // Path: /{method}/{namespace}/{pod}/{container}
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    if segments.len() < 4 {
        anyhow::bail!(
            "malformed streaming path (expected /<method>/<ns>/<pod>/<container>): {path}"
        );
    }

    let method = match segments[0] {
        "exec" => StreamMethod::Exec,
        "attach" => StreamMethod::Attach,
        "portForward" | "portforward" => StreamMethod::PortForward,
        other => anyhow::bail!("unknown streaming method: {other}"),
    };
    let namespace = segments[1].to_string();
    let pod = segments[2].to_string();
    let container = segments[3].to_string();

    debug!(
        "streaming request: {:?} ns={namespace} pod={pod} container={container}",
        method
    );

    Ok(ClientRequest {
        method,
        namespace,
        pod,
        container,
        query,
        head,
    })
}

/// Build the HTTP request to send to the CRI streaming server, substituting
/// the path from the original request head with the token path from the URL.
fn build_backend_request(client_head: &[u8], backend_url: &url::Url) -> Vec<u8> {
    let backend_path = backend_url.path();
    let backend_host = backend_url.host_str().unwrap_or("localhost");
    let backend_port = backend_url.port().unwrap_or(80);

    let head_str = String::from_utf8_lossy(client_head);
    let mut head_lines = head_str.split("\r\n");

    // Get the HTTP method from the original request line
    let request_line = head_lines.next().unwrap_or("GET / HTTP/1.1");
    let method = request_line.split(' ').next().unwrap_or("GET");

    let mut out = Vec::with_capacity(2048);
    // Build new request line with the CRI streaming token path
    out.extend_from_slice(format!("{method} {backend_path} HTTP/1.1\r\n").as_bytes());
    // New Host header
    out.extend_from_slice(format!("Host: {backend_host}:{backend_port}\r\n").as_bytes());

    // Copy remaining headers (skip the original Host header)
    for line in head_lines {
        if line.is_empty() {
            continue;
        }
        let lower = line.to_lowercase();
        if lower.starts_with("host:") {
            continue;
        }
        out.extend_from_slice(line.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");

    out
}

/// Read the 101 Switching Protocols response from the CRI streaming server
/// plus any SPDY frame bytes that arrived with it.
async fn read_backend_response(stream: &mut TcpStream) -> Result<BackendResponse> {
    let mut buf = Vec::with_capacity(4096);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            anyhow::bail!("CRI streaming server closed before sending response");
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            anyhow::bail!("response head too large");
        }
    }

    // After the blank line, there might be SPDY frame bytes already buffered.
    // Read whatever is immediately available without blocking.
    let head_len = buf.len();
    let mut tail = Vec::new();
    let mut tail_buf = [0u8; 4096];
    loop {
        match stream.try_read(&mut tail_buf) {
            Ok(0) => break,
            Ok(n) => tail.extend_from_slice(&tail_buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e.into()),
        }
    }

    // Verify it's a 101
    let head_str = String::from_utf8_lossy(&buf[..head_len]);
    if !head_str.contains("101") {
        anyhow::bail!("CRI streaming server did not return 101: {head_str}");
    }

    Ok(BackendResponse {
        head: buf[..head_len].to_vec(),
        spdy_tail: tail,
    })
}

// ── bidirectional relay ────────────────────────────────────────────────

/// Shuttle bytes between client (api-server) and backend (CRI streaming)
/// until one side closes or errors.
async fn relay(client: TcpStream, backend: TcpStream, backend_tail: Vec<u8>) {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut backend_read, mut backend_write) = backend.into_split();

    // First, flush any SPDY tail from the backend to the client.
    if !backend_tail.is_empty() {
        if client_write.write_all(&backend_tail).await.is_err() {
            return;
        }
    }

    // Client → Backend
    let c2b = tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match client_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if backend_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = backend_write.shutdown().await;
    });

    // Backend → Client
    let b2c = tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match backend_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if client_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = client_write.shutdown().await;
    });

    // Wait for either direction to finish
    tokio::select! {
        _ = c2b => {},
        _ = b2c => {},
    }
}

// ── query parsing helpers ─────────────────────────────────────────────

/// Parse exec query: `command=sh&command=-c&command=echo+hi&stdin=1&stdout=1&stderr=1&tty=1`
fn parse_exec_query(query: &str) -> Result<(Vec<String>, bool, bool, bool, bool)> {
    let mut cmd = Vec::new();
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    let mut tty = false;

    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let value = urlencoding_decode(value);
        match key {
            "command" => cmd.push(value),
            "stdin" => stdin = value == "true" || value == "1",
            "stdout" => stdout = value == "true" || value == "1",
            "stderr" => stderr = value == "true" || value == "1",
            "tty" => tty = value == "true" || value == "1",
            _ => {}
        }
    }

    Ok((cmd, stdin, stdout, stderr, tty))
}

/// Parse attach query: `stdin=1&stdout=1&stderr=1&tty=1`
fn parse_attach_query(query: &str) -> Result<(bool, bool, bool, bool)> {
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    let mut tty = false;

    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let value = urlencoding_decode(value);
        match key {
            "stdin" => stdin = value == "true" || value == "1",
            "stdout" => stdout = value == "true" || value == "1",
            "stderr" => stderr = value == "true" || value == "1",
            "tty" => tty = value == "true" || value == "1",
            _ => {}
        }
    }

    Ok((stdin, stdout, stderr, tty))
}

/// Parse portforward query: `ports=8080&ports=9090`
fn parse_portforward_query(query: &str) -> Result<Vec<i32>> {
    let mut ports = Vec::new();
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "ports" {
            if let Ok(p) = value.parse::<i32>() {
                ports.push(p);
            }
        }
    }
    if ports.is_empty() {
        anyhow::bail!("no ports specified in portforward request");
    }
    Ok(ports)
}

/// Simple URL percent-decoding (handles `+` for space and `%XX` hex).
fn urlencoding_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        match b {
            b'+' => out.push(' '),
            b'%' => {
                let hi = chars.next().and_then(hex_val);
                let lo = chars.next().and_then(hex_val);
                match (hi, lo) {
                    (Some(h), Some(l)) => out.push((h << 4 | l) as char),
                    _ => out.push('%'),
                }
            }
            _ => out.push(b as char),
        }
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

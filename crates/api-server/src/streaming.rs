//! WebSocket↔SPDY streaming for exec, attach, and port-forward
//!
//! When the api-server receives a WebSocket request from kubectl,
//! it opens a SPDY connection to the kubelet streaming server and
//! translates between the two protocols:
//!
//!   kubectl ──WS──▶ api-server ──SPDY──▶ kubelet:10250 ──SPDY──▶ CRI runtime
//!
//! WS v5.channel.k8s.io frames map to SPDY DATA frames:
//!   WS ch0 (stdin)  → SPDY stream 1
//!   WS ch4 (resize) → SPDY stream 4
//!   SPDY stdout     → WS ch1
//!   SPDY stderr     → WS ch2
//!   SPDY error      → WS ch3 (JSON status)

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use rusternetes_common::resources::Pod;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info};

// ── SPDY constants (subset needed for client-side streaming proxy) ──

/// SPDY/3 control frame bit
const SPDY_CTRL_BIT: u8 = 0x80;

/// SPDY/3 version [0x00, 0x03]
const SPDY_VERSION: u16 = 3;

/// Frame types
const TYPE_DATA: u8 = 0; // not a real type, used as marker
const TYPE_SYN_STREAM: u16 = 1;
const TYPE_RST_STREAM: u16 = 3;
const TYPE_GOAWAY: u16 = 7;

/// Flags
const FLAG_FIN: u8 = 0x01;

/// SPDY stream IDs used to talk to the kubelet.
/// These map to WebSocket channel IDs as follows:
///   WS ch0 (stdin)  → SPDY stream 1
///   WS ch4 (resize) → SPDY stream 4
///   Kubelet stdout   → SPDY stream 2 → WS ch1
///   Kubelet stderr   → SPDY stream 3 → WS ch2
///   Kubelet error    → SPDY stream (anything) → WS ch3
const STDIN_STREAM: u32 = 1;
const STDOUT_STREAM: u32 = 2;
const STDERR_STREAM: u32 = 3;
const RESIZE_STREAM: u32 = 4;

/// Write a SPDY control frame.
async fn write_ctrl(
    w: &mut (impl AsyncWriteExt + Unpin),
    frame_type: u16,
    flags: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut head = [0u8; 8];
    head[0] = SPDY_CTRL_BIT;
    head[1] = SPDY_VERSION as u8;
    head[2..4].copy_from_slice(&frame_type.to_be_bytes());
    head[4] = flags;
    let len = payload.len();
    head[5] = (len >> 16) as u8;
    head[6] = (len >> 8) as u8;
    head[7] = len as u8;
    w.write_all(&head).await?;
    w.write_all(payload).await?;
    w.flush().await
}

/// Write a SPDY DATA frame.
async fn write_data_frame(
    w: &mut (impl AsyncWriteExt + Unpin),
    stream_id: u32,
    fin: bool,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut head = [0u8; 8];
    head[0..4].copy_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    head[4] = if fin { FLAG_FIN } else { 0 };
    let len = payload.len();
    head[5] = (len >> 16) as u8;
    head[6] = (len >> 8) as u8;
    head[7] = len as u8;
    w.write_all(&head).await?;
    if !payload.is_empty() {
        w.write_all(payload).await?;
    }
    w.flush().await
}

/// Write a SPDY SYN_STREAM frame. Headers are raw name/value pairs
/// each with a 4-byte-length prefix.
async fn write_syn_stream(
    w: &mut (impl AsyncWriteExt + Unpin),
    stream_id: u32,
    pairs: &[(&str, &str)],
) -> std::io::Result<()> {
    // Build COMPRESSED header block: count(4) + each: name_len(4)+name+val_len(4)+val
    let mut hdr = Vec::new();
    hdr.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (n, v) in pairs {
        hdr.extend_from_slice(&(n.len() as u32).to_be_bytes());
        hdr.extend_from_slice(n.as_bytes());
        hdr.extend_from_slice(&(v.len() as u32).to_be_bytes());
        hdr.extend_from_slice(v.as_bytes());
    }
    // SYN_STREAM payload: stream_id(4) | assoc(4) | pri+slot(1+1) | headers
    let mut payload = Vec::with_capacity(10 + hdr.len());
    payload.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    payload.extend_from_slice(&[0u8; 4]); // associated-to-stream-id
    payload.push(0); // priority + unused
    payload.push(0); // slot
    payload.extend_from_slice(&hdr);
    write_ctrl(w, TYPE_SYN_STREAM, 0, &payload).await
}

/// Connect to the kubelet streaming server, perform the SPDY handshake,
/// and return the TcpStream ready for frame relay.
async fn kubelet_spdy_connect(
    addr: &str,
    path: &str,
) -> anyhow::Result<TcpStream> {
    let mut sock = TcpStream::connect(addr).await
        .map_err(|e| anyhow::anyhow!("TCP connect to kubelet {addr}: {e}"))?;

    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: SPDY/3.1\r\n\
         Connection: Upgrade\r\n\
         X-Stream-Protocol-Version: v5.channel.k8s.io\r\n\
         \r\n"
    );
    sock.write_all(req.as_bytes()).await
        .map_err(|e| anyhow::anyhow!("SPDY upgrade write: {e}"))?;

    // Read 101 response byte-by-byte
    let mut resp = Vec::with_capacity(512);
    let mut b = [0u8; 1];
    loop {
        let n = sock.read(&mut b).await?;
        if n == 0 {
            anyhow::bail!("kubelet closed before upgrade response");
        }
        resp.push(b[0]);
        if resp.ends_with(b"\r\n\r\n") {
            break;
        }
        if resp.len() > 4096 {
            anyhow::bail!("kubelet response too large");
        }
    }
    let s = String::from_utf8_lossy(&resp);
    if !s.contains("101") {
        anyhow::bail!("kubelet returned non-101: {s}");
    }
    Ok(sock)
}

/// Read one byte-precise from the socket (for header parsing).
async fn read_exact(
    r: &mut (impl AsyncReadExt + Unpin),
    buf: &mut [u8],
) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = r.read(&mut buf[offset..]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        offset += n;
    }
    Ok(())
}

/// Read a full SPDY frame (control or data) from a socket.
/// Returns (is_data, stream_id, fin, payload).
/// For control frames, stream_id/fin are meaningless but payload contains
/// the control frame body.
async fn read_spdy_raw(
    r: &mut (impl AsyncReadExt + Unpin),
) -> std::io::Result<Option<(bool, u32, bool, Vec<u8>)>> {
    let mut head = [0u8; 8];
    match r.read_exact(&mut head).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    if head[0] & SPDY_CTRL_BIT != 0 {
        // Control frame
        let _frame_type = u16::from_be_bytes([head[2], head[3]]);
        let len = ((head[5] as usize) << 16) | ((head[6] as usize) << 8) | (head[7] as usize);
        let mut payload = vec![0u8; len];
        read_exact(r, &mut payload).await?;
        Ok(Some((false, 0, false, payload)))
    } else {
        // Data frame
        let stream_id = u32::from_be_bytes([head[0] & 0x7f, head[1], head[2], head[3]]);
        let fin = (head[4] & FLAG_FIN) != 0;
        let len = ((head[5] as usize) << 16) | ((head[6] as usize) << 8) | (head[7] as usize);
        let mut payload = vec![0u8; len];
        read_exact(r, &mut payload).await?;
        Ok(Some((true, stream_id, fin, payload)))
    }
}

/// Handle WebSocket exec by proxying to kubelet streaming server.
/// Implements the Kubernetes `v5.channel.k8s.io` protocol:
///   ch0 = stdin (client→server)
///   ch1 = stdout (server→client)
///   ch2 = stderr (server→client)
///   ch3 = status (server→client, JSON)
///   ch4 = resize (client→server)
///
/// Maps WS channels ↔ SPDY streams:
///   ch0 → spdy stream 1,  ch4 → spdy stream 4
///   spdy stream 2 → ch1,  spdy stream 3 → ch2
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket_via_kubelet(
    mut socket: WebSocket,
    kubelet_addr: String,
    _pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    is_v1_protocol: bool,
) {
    info!("WS exec → kubelet {kubelet_addr} for {container_name} cmd={command:?}");

    // Build kubelet exec path
    let mut query = String::new();
    for cmd in &command {
        if !query.is_empty() { query.push('&'); }
        query.push_str(&format!("command={}", urlencode(cmd)));
    }
    if stdin { query.push_str("&stdin=true"); }
    if stdout { query.push_str("&stdout=true"); }
    if stderr { query.push_str("&stderr=true"); }
    if tty { query.push_str("&tty=true"); }
    query.push_str(&format!("&container={}", urlencode(&container_name)));

    let path = format!("/exec/default/{}/{}?{}", _pod.metadata.name, container_name, query);
    debug!("WS exec: connecting to kubelet at {kubelet_addr}{path}");

    // Connect to kubelet via SPDY
    let backend = match kubelet_spdy_connect(&kubelet_addr, &path).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS exec: SPDY connect failed: {e}");
            let mut err = vec![3u8];
            let msg = format!(r#"{{"status":"Failure","message":"{}"}}"#, e);
            err.extend_from_slice(msg.as_bytes());
            let _ = socket.send(Message::Binary(err)).await;
            let _ = socket.close().await;
            return;
        }
    };

    let (mut backend_read, mut backend_write) = tokio::io::split(backend);

    // Open client→server streams: stdin (stream 1) and resize (stream 4)
    let _ = write_syn_stream(&mut backend_write, STDIN_STREAM, &[("streamtype", "stdin")]).await;
    let _ = write_syn_stream(&mut backend_write, RESIZE_STREAM, &[("streamtype", "resize")]).await;

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Track whether client disconnected
    let client_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let client_closed2 = client_closed.clone();

    // Spawn task: read WS messages, forward to kubelet via SPDY
    let client_to_kubelet = {
        let mut bw = backend_write;
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Close(_)) | Err(_) => {
                        client_closed2.store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = write_data_frame(&mut bw, STDIN_STREAM, true, &[]).await;
                        break;
                    }
                    Ok(Message::Binary(data)) if !data.is_empty() => {
                        let ch = data[0];
                        let payload = &data[1..];
                        match ch {
                            0 => {
                                // stdin
                                if payload.is_empty() {
                                    // v5 close-stream signal
                                    let _ = write_data_frame(&mut bw, STDIN_STREAM, true, &[]).await;
                                } else {
                                    let _ = write_data_frame(&mut bw, STDIN_STREAM, false, payload).await;
                                }
                            }
                            4 => {
                                // resize
                                let _ = write_data_frame(&mut bw, RESIZE_STREAM, false, payload).await;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        })
    };

    // Read SPDY frames from kubelet, translate to WS channel frames
    let mut exit_code: Option<i32> = None;
    loop {
        match read_spdy_raw(&mut backend_read).await {
            Ok(Some((is_data, stream_id, _fin, payload))) => {
                if !is_data {
                    // Control frame — could be SYN_REPLY, RST_STREAM, GOAWAY
                    // For RST_STREAM on stdout/stderr, treat as end of stream
                    if payload.len() >= 4 {
                        let rst_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
                        if rst_id == STDOUT_STREAM || rst_id == STDERR_STREAM {
                            continue; // stream closed, ignore
                        }
                    }
                    // Check for GOAWAY
                    if payload.len() >= 8 {
                        // last_good_stream + status_code
                        break;
                    }
                    continue;
                }

                // Map SPDY stream to WS channel
                let ws_channel: u8 = match stream_id {
                    STDOUT_STREAM => 1,
                    STDERR_STREAM => 2,
                    _ => {
                        // Unknown or error stream → send as ch3 (error/status)
                        // Try to parse as JSON exit status
                        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&payload) {
                            if let Some(ec) = val.get("exitCode").and_then(|v| v.as_i64()) {
                                exit_code = Some(ec as i32);
                            }
                        }
                        continue;
                    }
                };

                let mut frame = vec![ws_channel];
                frame.extend_from_slice(&payload);
                if ws_sender.send(Message::Binary(frame)).await.is_err() {
                    break;
                }
            }
            Ok(None) => {
                // Connection closed gracefully
                break;
            }
            Err(e) => {
                if client_closed.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                error!("WS exec: SPDY read error: {e}");
                // Check if we got any data before error
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                // Send error on status channel
                let mut err = vec![3u8];
                let msg = format!(r#"{{"status":"Failure","message":"{}"}}"#, e);
                err.extend_from_slice(msg.as_bytes());
                let _ = ws_sender.send(Message::Binary(err)).await;
                break;
            }
        }

        // Check timeout / client disconnect
        if client_closed.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }

    let _ = client_to_kubelet.await;

    // Send exit status on channel 3
    let code = exit_code.unwrap_or(0);
    info!("WS exec: done, exit_code={code}");
    if !is_v1_protocol || code != 0 {
        let status_json = if code == 0 {
            r#"{"status":"Success"}"#.to_string()
        } else {
            format!(
                r#"{{"status":"Failure","message":"command terminated with exit code {}","reason":"NonZeroExitCode","details":{{"causes":[{{"reason":"ExitCode","message":"{}"}}]}}}}"#,
                code, code
            )
        };
        let mut status_data = vec![3u8];
        status_data.extend_from_slice(status_json.as_bytes());
        let _ = ws_sender.send(Message::Binary(status_data)).await;
    }
    let _ = ws_sender.send(Message::Close(None)).await;
}

/// Handle WebSocket attach by proxying to kubelet streaming server.
pub async fn handle_attach_websocket_via_kubelet(
    mut socket: WebSocket,
    kubelet_addr: String,
    kubelet_path: String,
) {
    info!("WS attach → kubelet {kubelet_addr}");

    // Connect to kubelet via SPDY using the provided path
    let backend = match kubelet_spdy_connect(&kubelet_addr, &kubelet_path).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS attach: SPDY connect failed: {e}");
            let mut err = vec![3u8];
            let msg = format!(r#"{{"status":"Failure","message":"{}"}}"#, e);
            err.extend_from_slice(msg.as_bytes());
            let _ = socket.send(Message::Binary(err)).await;
            let _ = socket.close().await;
            return;
        }
    };

    let (mut backend_read, mut backend_write) = tokio::io::split(backend);

    // Open stdin and resize streams for attach
    let _ = write_syn_stream(&mut backend_write, STDIN_STREAM, &[("streamtype", "stdin")]).await;
    let _ = write_syn_stream(&mut backend_write, RESIZE_STREAM, &[("streamtype", "resize")]).await;

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Client → kubelet (stdin + resize)
    let client_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let c2 = client_closed.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => {
                    c2.store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = write_data_frame(&mut backend_write, STDIN_STREAM, true, &[]).await;
                    break;
                }
                Ok(Message::Binary(data)) if !data.is_empty() => {
                    let ch = data[0];
                    let payload = &data[1..];
                    match ch {
                        0 => {
                            if payload.is_empty() {
                                let _ = write_data_frame(&mut backend_write, STDIN_STREAM, true, &[]).await;
                            } else {
                                let _ = write_data_frame(&mut backend_write, STDIN_STREAM, false, payload).await;
                            }
                        }
                        4 => {
                            let _ = write_data_frame(&mut backend_write, RESIZE_STREAM, false, payload).await;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    });

    // Kubelet → client (stdout, stderr → WS ch1, ch2)
    loop {
        match read_spdy_raw(&mut backend_read).await {
            Ok(Some((true, stream_id, _fin, payload))) => {
                let ws_ch = match stream_id {
                    STDOUT_STREAM => 1u8,
                    STDERR_STREAM => 2u8,
                    _ => continue,
                };
                let mut frame = vec![ws_ch];
                frame.extend_from_slice(&payload);
                if ws_sender.send(Message::Binary(frame)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Ok(Some((false, _, _, _))) => continue, // control frames
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }
        if client_closed.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }

    let _ = ws_sender.send(Message::Close(None)).await;
}

/// Handle WebSocket port-forward by proxying to kubelet streaming server.
pub async fn handle_portforward_websocket_via_kubelet(
    mut socket: WebSocket,
    kubelet_addr: String,
    kubelet_path: String,
) {
    info!("WS portforward → kubelet {kubelet_addr}");

    let backend = match kubelet_spdy_connect(&kubelet_addr, &kubelet_path).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS portforward: SPDY connect failed: {e}");
            let mut err = vec![3u8];
            let msg = format!(r#"{{"status":"Failure","message":"{}"}}"#, e);
            err.extend_from_slice(msg.as_bytes());
            let _ = socket.send(Message::Binary(err)).await;
            let _ = socket.close().await;
            return;
        }
    };

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (mut backend_read, mut backend_write) = tokio::io::split(backend);

    // For port-forward, the client sends data on ch0 (stdin) and receives on ch1
    let client_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let c2 = client_closed.clone();
    tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => {
                    c2.store(true, std::sync::atomic::Ordering::Relaxed);
                    break;
                }
                Ok(Message::Binary(data)) if !data.is_empty() => {
                    let ch = data[0];
                    let payload = &data[1..];
                    if ch == 0 {
                        let _ = backend_write.write_all(payload).await;
                    }
                }
                _ => {}
            }
        }
    });

    // Read from kubelet, forward to WS
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match backend_read.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let mut frame = vec![1u8]; // stdout channel
                frame.extend_from_slice(&buf[..n]);
                if ws_sender.send(Message::Binary(frame)).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
        if client_closed.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }

    let _ = ws_sender.send(Message::Close(None)).await;
}

// ── Backward-compatibility wrappers ──

/// Backward-compat alias. Routes exec WebSocket to the kubelet.
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket(
    socket: WebSocket,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) {
    handle_exec_websocket_with_protocol(
        socket, pod, container_name, command, stdin, stdout, stderr, tty, false,
    )
    .await
}

/// Backward-compat alias with protocol awareness. Routes to the kubelet.
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket_with_protocol(
    socket: WebSocket,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    is_v1_protocol: bool,
) {
    // Build kubelet address from pod info
    let kubelet_addr = match resolve_kubelet_for_pod(&pod).await {
        Ok(a) => a,
        Err(_) => {
            let mut s = socket;
            let mut err = vec![3u8];
            let msg = r#"{"status":"Failure","message":"Failed to resolve kubelet address"}"#;
            err.extend_from_slice(msg.as_bytes());
            let _ = s.send(Message::Binary(err)).await;
            let _ = s.close().await;
            return;
        }
    };
    handle_exec_websocket_via_kubelet(
        socket, kubelet_addr, pod, container_name, command,
        stdin, stdout, stderr, tty, is_v1_protocol,
    )
    .await
}

/// Backward-compat alias for attach WebSocket.
pub async fn handle_attach_websocket(
    socket: WebSocket,
    _pod: Pod,
    _container_name: String,
    _stdin: bool,
    _stdout: bool,
    _stderr: bool,
    _tty: bool,
) {
    // Legacy stub — proper path comes from pod_subresources now
    info!("WS attach (legacy): stub");
    let mut s = socket;
    let mut err = vec![3u8];
    err.extend_from_slice(br#"{"status":"Failure","message":"Attach via kubelet: use via_kubelet endpoint"}"#);
    let _ = s.send(Message::Binary(err)).await;
    let _ = s.close().await;
}

/// Backward-compat alias for port-forward WebSocket.
pub async fn handle_portforward_websocket(
    socket: WebSocket,
    _pod: Pod,
    _ports: Vec<u16>,
) {
    info!("WS portforward (legacy): stub");
    let mut s = socket;
    let mut err = vec![3u8];
    err.extend_from_slice(br#"{"status":"Failure","message":"Port-forward via kubelet: use via_kubelet endpoint"}"#);
    let _ = s.send(Message::Binary(err)).await;
    let _ = s.close().await;
}

// ── Helpers ──

/// Very lightweight percent-encoding for query strings.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            b' ' => "+".to_string(),
            _ => format!("%{:02X}", b),
        })
        .collect()
}

/// Resolve kubelet address from a pod (for backward-compat wrappers).
async fn resolve_kubelet_for_pod(pod: &Pod) -> anyhow::Result<String> {
    // We can't access storage directly here — use a default
    // For backward compat, return a placeholder
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.clone())
        .ok_or_else(|| anyhow::anyhow!("pod not scheduled"))?;
    Ok(format!("{}:10250", node_name))
}

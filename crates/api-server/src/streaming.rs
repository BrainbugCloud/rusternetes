//! WebSocket↔SPDY streaming for exec, attach, and port-forward.
//!
//! When the api-server receives a WebSocket request from kubectl, it opens a
//! SPDY connection to the kubelet streaming server and translates between the
//! two protocols:
//!
//!   kubectl ──WS(v5)──▶ api-server ──SPDY(v4)──▶ kubelet:10251 ──SPDY──▶ CRI runtime
//!
//! The SPDY framing/zlib codec is reused from `cri-server::streaming::spdy`
//! (the one maintained SPDY-for-Kubernetes implementation — see
//! plan/10-get-rid-of-spdy.md); this module only implements the *remotecommand
//! client* on top of it and the WebSocket-channel ↔ SPDY-stream translation.
//!
//! Remotecommand SPDY protocol (what the CRI runtime's server expects):
//!   - the client creates every stream via SYN_STREAM with a `streamtype`
//!     header: `error` (always, created first), then `stdin`/`stdout`/`stderr`
//!     as requested, and `resize` when a TTY is used (no separate `stderr`
//!     under a TTY). Stream IDs are client-odd, in creation order.
//!   - stdout/stderr data arrive as DATA frames on their streams.
//!   - the exit code arrives as a `metav1.Status` on the `error` stream, closed
//!     with FIN (empty FIN = exit 0).
//!
//! WebSocket channel bytes (kubectl ↔ api-server): ch0 stdin, ch1 stdout,
//! ch2 stderr, ch3 error/status, ch4 resize.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use cri_server::streaming::spdy::{read_frame, Frame, SpdyWriter};
use futures::{SinkExt, StreamExt};
use rusternetes_common::resources::Pod;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

// ── SPDY upgrade handshake ─────────────────────────────────────────────

/// Connect to the kubelet streaming server and perform the HTTP/1.1 → SPDY/3.1
/// upgrade, returning the raw stream positioned right after the `101` response.
///
/// The SPDY transport tops out at `v4.channel.k8s.io` — `v5` is WebSocket-only
/// (it adds the stdin-CLOSE signal). kubectl↔api-server speaks `v5` over
/// WebSocket; this SPDY leg to the kubelet/CRI runtime must advertise `v4`, or
/// the runtime's SPDY streaming server refuses to negotiate and never sends the
/// `101` upgrade response.
async fn kubelet_spdy_connect(addr: &str, path: &str) -> anyhow::Result<TcpStream> {
    let mut sock = TcpStream::connect(addr)
        .await
        .map_err(|e| anyhow::anyhow!("TCP connect to kubelet {addr}: {e}"))?;

    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: SPDY/3.1\r\n\
         Connection: Upgrade\r\n\
         X-Stream-Protocol-Version: v4.channel.k8s.io\r\n\
         \r\n"
    );
    sock.write_all(req.as_bytes())
        .await
        .map_err(|e| anyhow::anyhow!("SPDY upgrade write: {e}"))?;

    // Read the 101 response one byte at a time so no SPDY frame bytes past the
    // blank line are consumed.
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

// ── exit-status helpers ─────────────────────────────────────────────────

/// Parse an exit code out of a `metav1.Status` written to the error stream
/// (v4 protocol). Non-zero codes live in `details.causes[]` with
/// `reason: "ExitCode"`; a `Failure` without one maps to 1.
fn parse_exit_code(payload: &[u8]) -> Option<i32> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    if let Some(causes) = v.pointer("/details/causes").and_then(|c| c.as_array()) {
        for cause in causes {
            if cause.get("reason").and_then(|r| r.as_str()) == Some("ExitCode") {
                if let Some(code) = cause
                    .get("message")
                    .and_then(|m| m.as_str())
                    .and_then(|s| s.parse::<i32>().ok())
                {
                    return Some(code);
                }
            }
        }
    }
    match v.get("status").and_then(|s| s.as_str()) {
        Some("Failure") => Some(1),
        _ => Some(0),
    }
}

fn success_status_json() -> String {
    r#"{"metadata":{},"status":"Success"}"#.to_string()
}

/// A WebSocket close frame with an explicit normal-closure (1000) status code.
/// kubectl reports a status-less close as `websocket: close 1005 (no status)`
/// and treats it as an error, so every stream must end with this.
fn normal_close() -> Message {
    Message::Close(Some(CloseFrame {
        code: 1000,
        reason: std::borrow::Cow::Borrowed(""),
    }))
}

fn failure_status_json(code: i32) -> String {
    format!(
        r#"{{"metadata":{{}},"status":"Failure","message":"command terminated with exit code {code}","reason":"NonZeroExitCode","details":{{"causes":[{{"reason":"ExitCode","message":"{code}"}}]}}}}"#
    )
}

/// Send a `Failure` status on WS channel 3 and close, used when the SPDY
/// upgrade itself fails before any streaming can start.
async fn fail_socket(mut socket: WebSocket, message: &str) {
    let mut err = vec![3u8];
    let json = format!(
        r#"{{"metadata":{{}},"status":"Failure","message":"{}"}}"#,
        message.replace('"', "'")
    );
    err.extend_from_slice(json.as_bytes());
    let _ = socket.send(Message::Binary(err)).await;
    let _ = socket.close().await;
}

// ── shared exec/attach remotecommand client ─────────────────────────────

/// Core exec/attach proxy: dial the kubelet over SPDY, create the
/// remotecommand streams, and shuttle bytes between the kubelet's SPDY streams
/// and the client's WebSocket channels until the error stream closes.
#[allow(clippy::too_many_arguments)]
async fn proxy_remotecommand(
    socket: WebSocket,
    kubelet_addr: String,
    path: String,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    is_v1_protocol: bool,
    label: &str,
) {
    let backend = match kubelet_spdy_connect(&kubelet_addr, &path).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS {label}: SPDY connect failed: {e}");
            fail_socket(socket, &e.to_string()).await;
            return;
        }
    };

    let (mut read_half, write_half) = tokio::io::split(backend);
    let writer = Arc::new(Mutex::new(SpdyWriter::new(write_half)));

    // Allocate client-odd stream IDs in creation order (error first).
    let mut next = 1u32;
    let mut alloc = || {
        let id = next;
        next += 2;
        id
    };
    let error_id = alloc();
    let stdin_id = stdin.then(&mut alloc);
    let stdout_id = stdout.then(&mut alloc);
    // Under a TTY there is no demultiplexed stderr; the client sends resize.
    let stderr_id = (stderr && !tty).then(&mut alloc);
    let resize_id = tty.then(&mut alloc);

    {
        let mut w = writer.lock().await;
        if let Err(e) = w.syn_stream(error_id, &[("streamtype", "error")]).await {
            drop(w);
            error!("WS {label}: failed to open error stream: {e}");
            fail_socket(socket, &e.to_string()).await;
            return;
        }
        if let Some(id) = stdin_id {
            let _ = w.syn_stream(id, &[("streamtype", "stdin")]).await;
        }
        if let Some(id) = stdout_id {
            let _ = w.syn_stream(id, &[("streamtype", "stdout")]).await;
        }
        if let Some(id) = stderr_id {
            let _ = w.syn_stream(id, &[("streamtype", "stderr")]).await;
        }
        if let Some(id) = resize_id {
            let _ = w.syn_stream(id, &[("streamtype", "resize")]).await;
        }
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Client → kubelet: WS ch0 (stdin) and ch4 (resize) → SPDY DATA frames.
    let stdin_task = {
        let writer = writer.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Binary(data)) if !data.is_empty() => {
                        let ch = data[0];
                        let payload = &data[1..];
                        match ch {
                            0 => {
                                if let Some(id) = stdin_id {
                                    let mut w = writer.lock().await;
                                    // Empty ch0 frame = v5 stdin-close signal.
                                    if payload.is_empty() {
                                        let _ = w.data(id, true, &[]).await;
                                    } else {
                                        let _ = w.data(id, false, payload).await;
                                    }
                                }
                            }
                            4 => {
                                if let Some(id) = resize_id {
                                    let mut w = writer.lock().await;
                                    let _ = w.data(id, false, payload).await;
                                }
                            }
                            255 => {
                                // v5.channel.k8s.io stream-close signal: the
                                // payload names the channel being closed. kubectl
                                // sends `[255, 0]` on stdin EOF — half-close the
                                // stdin SPDY stream so the process sees EOF.
                                if payload.first() == Some(&0) {
                                    if let Some(id) = stdin_id {
                                        let mut w = writer.lock().await;
                                        let _ = w.data(id, true, &[]).await;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => {
                        if let Some(id) = stdin_id {
                            let mut w = writer.lock().await;
                            let _ = w.data(id, true, &[]).await;
                        }
                        break;
                    }
                    _ => {}
                }
            }
        })
    };

    // Kubelet → client: SPDY DATA frames → WS channels; error stream FIN ends it.
    let mut exit_code: Option<i32> = None;
    loop {
        match read_frame(&mut read_half).await {
            Ok(Some(Frame::Data {
                stream_id,
                fin,
                payload,
            })) => {
                if Some(stream_id) == stdout_id {
                    if !payload.is_empty() {
                        let mut f = vec![1u8];
                        f.extend_from_slice(&payload);
                        if ws_sender.send(Message::Binary(f)).await.is_err() {
                            break;
                        }
                    }
                } else if Some(stream_id) == stderr_id {
                    if !payload.is_empty() {
                        let mut f = vec![2u8];
                        f.extend_from_slice(&payload);
                        if ws_sender.send(Message::Binary(f)).await.is_err() {
                            break;
                        }
                    }
                } else if stream_id == error_id {
                    if !payload.is_empty() {
                        exit_code = parse_exit_code(&payload);
                    }
                    if fin {
                        // The runtime writes the status last, after all output.
                        break;
                    }
                }
            }
            Ok(Some(Frame::GoAway)) | Ok(None) => break,
            // SYN_REPLY / PING / RST_STREAM / others: ignore.
            Ok(Some(_)) => continue,
            Err(_) => break,
        }
    }

    let code = exit_code.unwrap_or(0);
    debug!("WS {label}: done, exit_code={code}");
    if !is_v1_protocol || code != 0 {
        let status_json = if code == 0 {
            success_status_json()
        } else {
            failure_status_json(code)
        };
        let mut status = vec![3u8];
        status.extend_from_slice(status_json.as_bytes());
        let _ = ws_sender.send(Message::Binary(status)).await;
    }
    let _ = ws_sender.send(normal_close()).await;
    stdin_task.abort();
}

/// Extract a boolean query flag (`name=true`/`name=1`) from a raw query string.
fn query_flag(query: &str, name: &str) -> bool {
    query.split('&').any(|pair| {
        matches!(pair.split_once('='), Some((k, v)) if k == name && (v == "true" || v == "1"))
    })
}

// ── public handlers (invoked from pod_subresources) ─────────────────────

/// Handle a WebSocket `exec` by proxying to the kubelet streaming server.
#[allow(clippy::too_many_arguments)]
pub async fn handle_exec_websocket_via_kubelet(
    socket: WebSocket,
    kubelet_addr: String,
    pod: Pod,
    container_name: String,
    command: Vec<String>,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
    is_v1_protocol: bool,
) {
    let namespace = pod
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let mut query = String::new();
    for cmd in &command {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(&format!("command={}", urlencode(cmd)));
    }
    if stdin {
        query.push_str("&stdin=true");
    }
    if stdout {
        query.push_str("&stdout=true");
    }
    if stderr {
        query.push_str("&stderr=true");
    }
    if tty {
        query.push_str("&tty=true");
    }
    query.push_str(&format!("&container={}", urlencode(&container_name)));

    let path = format!(
        "/exec/{}/{}/{}?{}",
        namespace, pod.metadata.name, container_name, query
    );
    info!("WS exec → kubelet {kubelet_addr}{path} cmd={command:?}");

    proxy_remotecommand(
        socket,
        kubelet_addr,
        path,
        stdin,
        stdout,
        stderr,
        tty,
        is_v1_protocol,
        "exec",
    )
    .await;
}

/// Handle a WebSocket `attach` by proxying to the kubelet streaming server.
pub async fn handle_attach_websocket_via_kubelet(
    socket: WebSocket,
    kubelet_addr: String,
    kubelet_path: String,
) {
    // Attach flags live in the request query; default to stdout+stderr.
    let query = kubelet_path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let stdin = query_flag(query, "stdin") || query_flag(query, "input");
    let mut stdout = query_flag(query, "stdout");
    let stderr = query_flag(query, "stderr");
    let tty = query_flag(query, "tty");
    if !stdout && !stderr {
        stdout = true;
    }
    info!("WS attach → kubelet {kubelet_addr}{kubelet_path}");

    proxy_remotecommand(
        socket,
        kubelet_addr,
        kubelet_path,
        stdin,
        stdout,
        stderr,
        tty,
        false,
        "attach",
    )
    .await;
}

/// Handle a WebSocket `port-forward` by proxying to the kubelet streaming
/// server. Per forwarded port the client uses two WS channels — `2*i` (data)
/// and `2*i+1` (error) — and the runtime expects a SPDY `data`+`error` stream
/// pair carrying a `port` header.
pub async fn handle_portforward_websocket_via_kubelet(
    socket: WebSocket,
    kubelet_addr: String,
    kubelet_path: String,
) {
    let query = kubelet_path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let ports: Vec<u16> = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(k, _)| *k == "ports" || *k == "port")
        .flat_map(|(_, v)| v.split(','))
        .filter_map(|p| p.parse::<u16>().ok())
        .collect();

    info!("WS portforward → kubelet {kubelet_addr}{kubelet_path} ports={ports:?}");
    if ports.is_empty() {
        fail_socket(socket, "no ports specified").await;
        return;
    }

    let backend = match kubelet_spdy_connect(&kubelet_addr, &kubelet_path).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS portforward: SPDY connect failed: {e}");
            fail_socket(socket, &e.to_string()).await;
            return;
        }
    };

    let (mut read_half, write_half) = tokio::io::split(backend);
    let writer = Arc::new(Mutex::new(SpdyWriter::new(write_half)));
    let (ws_sender, mut ws_receiver) = socket.split();
    let ws_sender = Arc::new(Mutex::new(ws_sender));

    // Map SPDY data-stream id → WS data channel, and WS data channel → SPDY id.
    let mut spdy_to_ws: HashMap<u32, u8> = HashMap::new();
    let mut ws_to_spdy: HashMap<u8, u32> = HashMap::new();
    let mut next = 1u32;

    for (i, port) in ports.iter().enumerate() {
        let ws_data = (i * 2) as u8;
        let ws_err = (i * 2 + 1) as u8;
        let port_str = port.to_string();
        let error_id = next;
        next += 2;
        let data_id = next;
        next += 2;
        {
            let mut w = writer.lock().await;
            let _ = w
                .syn_stream(error_id, &[("streamtype", "error"), ("port", &port_str)])
                .await;
            let _ = w
                .syn_stream(data_id, &[("streamtype", "data"), ("port", &port_str)])
                .await;
        }
        spdy_to_ws.insert(data_id, ws_data);
        ws_to_spdy.insert(ws_data, data_id);
        // Initial port handshake to the client on both channels (2-byte LE).
        let pbytes = port.to_le_bytes();
        let mut s = ws_sender.lock().await;
        let mut df = vec![ws_data];
        df.extend_from_slice(&pbytes);
        let _ = s.send(Message::Binary(df)).await;
        let mut ef = vec![ws_err];
        ef.extend_from_slice(&pbytes);
        let _ = s.send(Message::Binary(ef)).await;
    }

    // Track how many leading bytes of the client's port handshake to strip per
    // data channel (kubectl repeats the 2-byte port as its first data frame).
    let mut pending_handshake: HashMap<u8, u8> = ws_to_spdy.keys().map(|&c| (c, 2u8)).collect();

    // Client → kubelet
    let c2k = {
        let writer = writer.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Binary(data)) if !data.is_empty() => {
                        let ch = data[0];
                        let mut payload = &data[1..];
                        if let Some(rem) = pending_handshake.get_mut(&ch) {
                            let skip = (*rem as usize).min(payload.len());
                            *rem -= skip as u8;
                            payload = &payload[skip..];
                            if payload.is_empty() {
                                continue;
                            }
                        }
                        if let Some(&data_id) = ws_to_spdy.get(&ch) {
                            let mut w = writer.lock().await;
                            let _ = w.data(data_id, false, payload).await;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        })
    };

    // Kubelet → client
    loop {
        match read_frame(&mut read_half).await {
            Ok(Some(Frame::Data {
                stream_id, payload, ..
            })) => {
                if let Some(&ws_ch) = spdy_to_ws.get(&stream_id) {
                    if !payload.is_empty() {
                        let mut f = vec![ws_ch];
                        f.extend_from_slice(&payload);
                        let mut s = ws_sender.lock().await;
                        if s.send(Message::Binary(f)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Ok(Some(Frame::GoAway)) | Ok(None) => break,
            Ok(Some(_)) => continue,
            Err(_) => break,
        }
    }

    c2k.abort();
    let mut s = ws_sender.lock().await;
    let _ = s.send(normal_close()).await;
}

// ── Backward-compatibility wrappers ─────────────────────────────────────

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
        socket,
        pod,
        container_name,
        command,
        stdin,
        stdout,
        stderr,
        tty,
        false,
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
    let kubelet_addr = match resolve_kubelet_for_pod(&pod).await {
        Ok(a) => a,
        Err(_) => {
            fail_socket(socket, "Failed to resolve kubelet address").await;
            return;
        }
    };
    handle_exec_websocket_via_kubelet(
        socket,
        kubelet_addr,
        pod,
        container_name,
        command,
        stdin,
        stdout,
        stderr,
        tty,
        is_v1_protocol,
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
    info!("WS attach (legacy): stub");
    fail_socket(socket, "Attach via kubelet: use via_kubelet endpoint").await;
}

/// Backward-compat alias for port-forward WebSocket.
pub async fn handle_portforward_websocket(socket: WebSocket, _pod: Pod, _ports: Vec<u16>) {
    info!("WS portforward (legacy): stub");
    fail_socket(socket, "Port-forward via kubelet: use via_kubelet endpoint").await;
}

// ── Helpers ─────────────────────────────────────────────────────────────

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
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.clone())
        .ok_or_else(|| anyhow::anyhow!("pod not scheduled"))?;
    Ok(format!("{}:10250", node_name))
}

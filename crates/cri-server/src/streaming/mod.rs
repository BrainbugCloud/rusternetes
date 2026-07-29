// SPDX-License-Identifier: Apache-2.0
//
// Forked from the aurae project's `auraed/src/cri/streaming.rs`
// (https://github.com/aurae-runtime/aurae, `cri` branch, Apache-2.0,
// Copyright 2022-2024 the aurae contributors), with the two aurae-specific
// call sites (SandboxCache exec/attach plumbing, netns dialing) cut behind
// the [`StreamingBackend`] trait.

//! CRI streaming server: the `Exec`, `Attach`, and `PortForward` RPCs return
//! an `http://<addr>/{exec,attach,portforward}/<token>` URL; the client
//! (kubelet / critest) opens that URL and upgrades the HTTP/1.1 connection to
//! SPDY/3.1 — the protocol the Kubernetes `remotecommand` and `portforward`
//! clients speak by default. WebSocket is out of scope for now (a future
//! `websocket.rs` can join [`spdy`] behind the same registry).
//!
//! This module implements just enough of SPDY/3.1 to serve those three RPCs:
//! the framing, the zlib header (de)compression with the fixed SPDY
//! dictionary, and the small `v4.channel.k8s.io` / `portforward.k8s.io`
//! stream protocols layered on top. The streams are always created by the
//! client; the server replies (`SYN_REPLY`) and splices bytes to/from the
//! container.

pub(crate) mod channels;
pub mod spdy;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::error::Result;
use spdy::SpdyWriter;

/// Combined async read+write, as returned by
/// [`StreamingBackend::dial_in_sandbox`].
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

pub type BoxedReader = Pin<Box<dyn AsyncRead + Send>>;
pub type BoxedWriter = Pin<Box<dyn AsyncWrite + Send>>;

/// I/O handles of a started exec process.
pub struct ExecStreams {
    /// Write side of the process stdin; dropping it closes the process's
    /// stdin. `None` when the exec was requested without stdin.
    pub stdin: Option<BoxedWriter>,
    pub stdout: Option<BoxedReader>,
    /// `None` for tty execs (no demultiplexed stderr).
    pub stderr: Option<BoxedReader>,
    /// Resolves to the process exit code. Senders that drop without sending
    /// surface as exit code 255.
    pub exit: tokio::sync::oneshot::Receiver<i32>,
}

/// I/O handles onto an already-running container's stdio.
pub struct AttachStreams {
    pub stdin: Option<BoxedWriter>,
    pub stdout: Option<BoxedReader>,
    pub stderr: Option<BoxedReader>,
}

/// The seam a runtime backend implements to serve exec/attach/portforward.
#[async_trait]
pub trait StreamingBackend: Send + Sync + 'static {
    /// Start `cmd` inside the container and return its stream handles.
    async fn exec_stream(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
    ) -> Result<ExecStreams>;

    /// Attach to the container's stdio.
    async fn attach_stream(
        &self,
        container_id: &str,
        tty: bool,
        stdin: bool,
    ) -> Result<AttachStreams>;

    /// Open a TCP connection to `port` as seen from inside the sandbox
    /// (Linux: setns into the sandbox netns then connect; macOS: connect to
    /// the sandbox VM's IP).
    async fn dial_in_sandbox(&self, sandbox_id: &str, port: i32)
        -> Result<Box<dyn AsyncReadWrite>>;
}

/// How long an unclaimed streaming token stays valid. The kubelet redials
/// the returned URL immediately; anything older is a leak.
const TOKEN_TTL: Duration = Duration::from_secs(300);

/// How long the server waits for the client to create every stream a given
/// exec/attach request needs before giving up.
pub(crate) const STREAM_SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// A pending streaming request, stored by token when the corresponding CRI
/// RPC returns a URL and consumed when the client connects to that URL.
#[derive(Debug, Clone)]
pub(crate) enum StreamSpec {
    Exec {
        container_id: String,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
        stdout: bool,
        stderr: bool,
    },
    Attach {
        container_id: String,
        tty: bool,
        stdin: bool,
        stdout: bool,
        stderr: bool,
    },
    PortForward {
        pod_sandbox_id: String,
    },
}

/// The token registry backing the streaming URLs plus the base URL the CRI
/// RPCs advertise. Shared (`Arc`) between [`CriService`](crate::CriService)
/// (which registers tokens) and the streaming server (which consumes them).
#[derive(Debug, Default)]
pub struct Streaming {
    tokens: StdMutex<HashMap<String, (Instant, StreamSpec)>>,
    base_url: OnceLock<String>,
}

impl Streaming {
    pub fn new() -> Arc<Self> {
        Arc::new(Streaming::default())
    }

    /// Record the address the streaming server bound to, so URLs can be
    /// handed out. Called once, before any RPC is served.
    pub fn set_base_url(&self, base_url: String) {
        let _ = self.base_url.set(base_url);
    }

    /// Register a request and return the URL the client should connect to,
    /// or `None` if the streaming server never started (no base URL).
    pub(crate) fn register(&self, route: &str, spec: StreamSpec) -> Option<String> {
        let base = self.base_url.get()?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut tokens = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
        tokens.retain(|_, (created, _)| created.elapsed() < TOKEN_TTL);
        let _ = tokens.insert(token.clone(), (Instant::now(), spec));
        Some(format!("{base}/{route}/{token}"))
    }

    fn take(&self, token: &str) -> Option<StreamSpec> {
        let (created, spec) = self
            .tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(token)?;
        (created.elapsed() < TOKEN_TTL).then_some(spec)
    }
}

/// Bind a TCP listener, record its base URL on a fresh registry, and spawn
/// the accept loop. The returned registry is what
/// [`CriService::with_streaming`](crate::CriService::with_streaming) takes.
pub async fn start<B: StreamingBackend>(
    bind_addr: &str,
    backend: Arc<B>,
) -> std::io::Result<Arc<Streaming>> {
    let listener = TcpListener::bind(bind_addr).await?;
    let streaming = Streaming::new();
    streaming.set_base_url(format!("http://{}", listener.local_addr()?));
    tokio::spawn(serve(listener, streaming.clone(), backend));
    Ok(streaming)
}

/// Accept SPDY streaming connections until the listener is dropped. Each
/// connection is handled on its own task; a failure never takes the loop
/// down.
pub async fn serve<B: StreamingBackend>(
    listener: TcpListener,
    streaming: Arc<Streaming>,
    backend: Arc<B>,
) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("CRI streaming accept failed: {e}");
                continue;
            }
        };
        let streaming = streaming.clone();
        let backend = backend.clone();
        let _task = tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, streaming, backend).await {
                debug!("CRI streaming connection ended: {e}");
            }
        });
    }
}

/// Parsed first line + headers of the client's HTTP/1.1 upgrade request.
struct UpgradeRequest {
    route: String,
    token: String,
    /// The `X-Stream-Protocol-Version` values the client offered.
    protocols: Vec<String>,
}

async fn handle_connection<B: StreamingBackend>(
    mut stream: tokio::net::TcpStream,
    streaming: Arc<Streaming>,
    backend: Arc<B>,
) -> std::result::Result<(), String> {
    let request = read_upgrade_request(&mut stream).await?;
    let Some(spec) = streaming.take(&request.token) else {
        let _ = write_http_error(&mut stream, "404 Not Found").await;
        return Err(format!("unknown streaming token '{}'", request.token));
    };

    // The upgrade protocol depends on the route: exec/attach negotiate one of
    // the remotecommand channel versions; portforward has its own.
    let negotiated = negotiate_protocol(&request);
    write_upgrade_response(&mut stream, &negotiated).await?;

    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(Mutex::new(SpdyWriter::new(write_half)));

    match (request.route.as_str(), spec) {
        ("exec", spec @ StreamSpec::Exec { .. }) | ("attach", spec @ StreamSpec::Attach { .. }) => {
            channels::run_stream_session(read_half, writer, spec, backend).await
        }
        ("portforward", StreamSpec::PortForward { pod_sandbox_id }) => {
            channels::run_portforward_session(read_half, writer, pod_sandbox_id, backend).await
        }
        (route, _) => Err(format!("route '{route}' does not match token spec")),
    }
}

/// Read (byte at a time, so the SPDY frames that follow the blank line stay
/// unconsumed in the socket) and parse the HTTP/1.1 request head.
async fn read_upgrade_request(
    stream: &mut tokio::net::TcpStream,
) -> std::result::Result<UpgradeRequest, String> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| format!("reading request: {e}"))?;
        if n == 0 {
            return Err("client closed before sending request".to_string());
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        if head.len() > 16 * 1024 {
            return Err("request head too large".to_string());
        }
    }

    let text = String::from_utf8_lossy(&head);
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let target = request_line
        .split(' ')
        .nth(1)
        .ok_or_else(|| format!("malformed request line: {request_line:?}"))?;
    // /exec/<token>[/][?query]
    let path = target.split('?').next().unwrap_or(target);
    let mut segments = path.trim_matches('/').split('/');
    let route = segments.next().unwrap_or_default().to_string();
    let token = segments.next().unwrap_or_default().to_string();

    let mut protocols = Vec::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name
            .trim()
            .eq_ignore_ascii_case("x-stream-protocol-version")
        {
            for part in value.split(',') {
                let part = part.trim();
                if !part.is_empty() {
                    protocols.push(part.to_string());
                }
            }
        }
    }

    if route.is_empty() || token.is_empty() {
        return Err(format!("unroutable request target: {target:?}"));
    }
    Ok(UpgradeRequest {
        route,
        token,
        protocols,
    })
}

/// Pick the stream protocol version to echo in the `101` response. For
/// exec/attach prefer `v4.channel.k8s.io` (the first version to carry the
/// exit status on the error stream); otherwise echo the client's first
/// offer.
fn negotiate_protocol(request: &UpgradeRequest) -> String {
    const V4: &str = "v4.channel.k8s.io";
    if request.protocols.iter().any(|p| p == V4) {
        return V4.to_string();
    }
    request
        .protocols
        .first()
        .cloned()
        .unwrap_or_else(|| V4.to_string())
}

async fn write_upgrade_response(
    stream: &mut tokio::net::TcpStream,
    protocol: &str,
) -> std::result::Result<(), String> {
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Connection: Upgrade\r\n\
         Upgrade: SPDY/3.1\r\n\
         X-Stream-Protocol-Version: {protocol}\r\n\
         \r\n"
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|e| format!("writing upgrade response: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flushing upgrade: {e}"))
}

async fn write_http_error(stream: &mut tokio::net::TcpStream, status: &str) -> std::io::Result<()> {
    let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(response.as_bytes()).await
}

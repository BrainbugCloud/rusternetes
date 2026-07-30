// SPDX-License-Identifier: Apache-2.0
//
// Forked from the aurae project's `auraed/src/cri/streaming.rs`
// (https://github.com/aurae-runtime/aurae, `cri` branch, Apache-2.0,
// Copyright 2022-2024 the aurae contributors); the container plumbing is
// replaced by [`StreamingBackend`] calls.

//! The `v4.channel.k8s.io` (exec/attach) and `portforward.k8s.io` stream
//! protocols, layered over the SPDY framing in [`super::spdy`].

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

use super::spdy::{header, read_frame, Frame, SpdyWriter, MAX_DATA_CHUNK};
use super::{BoxedReader, StreamSpec, StreamingBackend, STREAM_SETUP_TIMEOUT};

type Writer = Arc<Mutex<SpdyWriter<OwnedWriteHalf>>>;

/// One message routed from the reader loop to a stream's consumer.
enum StreamMsg {
    Data(Vec<u8>),
    Eof,
}

/// A stream the client created: its id, and the receiver of its inbound
/// data (empty for the streams the server only writes to).
struct IncomingStream {
    stream_type: String,
    stream_id: u32,
    data: mpsc::UnboundedReceiver<StreamMsg>,
}

/// Run the reader loop on its own task: reply to every `SYN_STREAM`, route
/// `DATA` frames to the matching stream, answer pings, and forward each new
/// stream to the controller.
fn spawn_reader<R: AsyncRead + Unpin + Send + 'static>(
    mut read: R,
    writer: Writer,
    events: mpsc::UnboundedSender<IncomingStream>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut inflater = super::spdy::HeaderInflater::new();
        let mut streams: HashMap<u32, mpsc::UnboundedSender<StreamMsg>> = HashMap::new();
        loop {
            match read_frame(&mut read).await {
                Ok(Some(Frame::SynStream { stream_id, headers })) => {
                    let pairs = match inflater.inflate(&headers) {
                        Ok(pairs) => pairs,
                        Err(e) => {
                            warn!("SPDY header inflate failed: {e}");
                            break;
                        }
                    };
                    let stream_type = header(&pairs, "streamtype").unwrap_or_default().to_string();
                    let port = header(&pairs, "port").map(str::to_string);
                    {
                        let mut w = writer.lock().await;
                        if let Err(e) = w.syn_reply(stream_id).await {
                            warn!("SPDY syn_reply failed: {e}");
                            break;
                        }
                    }
                    let (tx, rx) = mpsc::unbounded_channel();
                    let _ = streams.insert(stream_id, tx);
                    // Port-forward carries the port in a header; smuggle it into
                    // the stream_type suffix so the controller can read it
                    // without another channel.
                    let stream_type = match port {
                        Some(port) => format!("{stream_type}:{port}"),
                        None => stream_type,
                    };
                    if events
                        .send(IncomingStream {
                            stream_type,
                            stream_id,
                            data: rx,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Some(Frame::Data {
                    stream_id,
                    fin,
                    payload,
                })) => {
                    if let Some(tx) = streams.get(&stream_id) {
                        if !payload.is_empty() {
                            let _ = tx.send(StreamMsg::Data(payload));
                        }
                        if fin {
                            let _ = tx.send(StreamMsg::Eof);
                        }
                    }
                }
                Ok(Some(Frame::RstStream { stream_id })) => {
                    let _ = streams.remove(&stream_id);
                }
                Ok(Some(Frame::Ping { id })) => {
                    let mut w = writer.lock().await;
                    let _ = w.ping(id).await;
                }
                Ok(Some(Frame::Other)) => {}
                Ok(Some(Frame::GoAway)) | Ok(None) => break,
                Err(e) => {
                    debug!("SPDY read ended: {e}");
                    break;
                }
            }
        }
    })
}

pub(crate) async fn run_stream_session<B: StreamingBackend>(
    read: tokio::net::tcp::OwnedReadHalf,
    writer: Writer,
    spec: StreamSpec,
    backend: Arc<B>,
) -> Result<(), String> {
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let reader = spawn_reader(read, writer.clone(), ev_tx);

    let expected = expected_streams(&spec);
    let mut collected: HashMap<String, IncomingStream> = HashMap::new();
    while !expected.iter().all(|name| collected.contains_key(*name)) {
        match tokio::time::timeout(STREAM_SETUP_TIMEOUT, ev_rx.recv()).await {
            Ok(Some(stream)) => {
                let key = base_type(&stream.stream_type);
                let _ = collected.insert(key, stream);
            }
            Ok(None) => {
                reader.abort();
                return Err("client closed before creating streams".to_string());
            }
            Err(_) => {
                reader.abort();
                return Err("timed out waiting for client streams".to_string());
            }
        }
    }

    let result = match spec {
        StreamSpec::Exec {
            container_id,
            cmd,
            tty,
            stdin,
            ..
        } => drive_exec(&writer, collected, &*backend, container_id, cmd, tty, stdin).await,
        StreamSpec::Attach {
            container_id,
            tty,
            stdin,
            ..
        } => drive_attach(&writer, collected, &*backend, container_id, tty, stdin).await,
        StreamSpec::PortForward { .. } => unreachable!("routed elsewhere"),
    };

    {
        let mut w = writer.lock().await;
        let _ = w.goaway().await;
    }
    reader.abort();
    result
}

/// The header block's stream type without the `:port` smuggled suffix.
fn base_type(stream_type: &str) -> String {
    stream_type
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// The set of stream types a given exec/attach request needs the client to
/// create before the command can be driven.
fn expected_streams(spec: &StreamSpec) -> Vec<&'static str> {
    let (tty, stdin, stdout, stderr) = match spec {
        StreamSpec::Exec {
            tty,
            stdin,
            stdout,
            stderr,
            ..
        }
        | StreamSpec::Attach {
            tty,
            stdin,
            stdout,
            stderr,
            ..
        } => (*tty, *stdin, *stdout, *stderr),
        StreamSpec::PortForward { .. } => return Vec::new(),
    };
    let mut expected = vec!["error"];
    if stdin {
        expected.push("stdin");
    }
    if stdout {
        expected.push("stdout");
    }
    // With a TTY there is no demultiplexed stderr; the client sends a resize
    // stream instead.
    if stderr && !tty {
        expected.push("stderr");
    }
    if tty {
        expected.push("resize");
    }
    expected
}

/// A metav1.Status marking a non-zero exec exit, written to the error
/// stream. This is the shape the remotecommand v4 client parses to recover
/// the exit code.
fn exit_status_json(exit_code: i32) -> String {
    format!(
        "{{\"metadata\":{{}},\"status\":\"Failure\",\
         \"reason\":\"NonZeroExitCode\",\
         \"details\":{{\"causes\":[{{\"reason\":\"ExitCode\",\
         \"message\":\"{exit_code}\"}}]}},\
         \"message\":\"command terminated with exit code {exit_code}\"}}"
    )
}

async fn drive_exec<B: StreamingBackend>(
    writer: &Writer,
    mut streams: HashMap<String, IncomingStream>,
    backend: &B,
    container_id: String,
    cmd: Vec<String>,
    tty: bool,
    stdin: bool,
) -> Result<(), String> {
    let error_id = streams.get("error").map(|s| s.stream_id);

    let exec = match backend.exec_stream(&container_id, cmd, tty, stdin).await {
        Ok(exec) => exec,
        Err(e) => return fail_error_stream(writer, error_id, &e.to_string()).await,
    };

    // stdin: copy client stdin data into the process, closing its stdin on
    // EOF (drop of the writer).
    if stdin {
        if let (Some(mut proc_stdin), Some(stream)) = (exec.stdin, streams.remove("stdin")) {
            let mut rx = stream.data;
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        StreamMsg::Data(bytes) => {
                            if proc_stdin.write_all(&bytes).await.is_err() {
                                break;
                            }
                            if proc_stdin.flush().await.is_err() {
                                break;
                            }
                        }
                        StreamMsg::Eof => break,
                    }
                }
                let _ = proc_stdin.shutdown().await;
                // Dropping proc_stdin closes the process's stdin.
            });
        }
    }

    // resize: terminal resize events; no PTY plumbing in the trait yet, so
    // drain them.
    if let Some(stream) = streams.remove("resize") {
        drain_stream(stream.data);
    }

    // stdout / stderr: pump the process output to the client's streams.
    let stdout_task = match (exec.stdout, streams.remove("stdout")) {
        (Some(stdout), Some(stream)) => Some(pump_output(writer.clone(), stream.stream_id, stdout)),
        _ => None,
    };
    let stderr_task = match (exec.stderr, streams.remove("stderr")) {
        (Some(stderr), Some(stream)) => Some(pump_output(writer.clone(), stream.stream_id, stderr)),
        _ => None,
    };

    if let Some(task) = stdout_task {
        let _ = task.await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }

    let exit_code = exec.exit.await.unwrap_or(255);
    close_error_stream(writer, error_id, exit_code).await;
    Ok(())
}

async fn drive_attach<B: StreamingBackend>(
    writer: &Writer,
    mut streams: HashMap<String, IncomingStream>,
    backend: &B,
    container_id: String,
    tty: bool,
    stdin: bool,
) -> Result<(), String> {
    let error_id = streams.get("error").map(|s| s.stream_id);

    let attach = match backend.attach_stream(&container_id, tty, stdin).await {
        Ok(attach) => attach,
        Err(e) => return fail_error_stream(writer, error_id, &e.to_string()).await,
    };

    // stdin: forward client stdin to the container, closing it on EOF
    // (StdinOnce semantics: dropping the writer lets a shell exit).
    if stdin {
        if let (Some(mut cont_stdin), Some(stream)) = (attach.stdin, streams.remove("stdin")) {
            let mut rx = stream.data;
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        StreamMsg::Data(bytes) => {
                            if cont_stdin.write_all(&bytes).await.is_err() {
                                break;
                            }
                            if cont_stdin.flush().await.is_err() {
                                break;
                            }
                        }
                        StreamMsg::Eof => break,
                    }
                }
                let _ = cont_stdin.shutdown().await;
            });
        }
    }

    if let Some(stream) = streams.remove("resize") {
        drain_stream(stream.data);
    }

    let stdout_task = match (attach.stdout, streams.remove("stdout")) {
        (Some(stdout), Some(stream)) => Some(pump_output(writer.clone(), stream.stream_id, stdout)),
        _ => None,
    };
    let stderr_task = match (attach.stderr, streams.remove("stderr")) {
        (Some(stderr), Some(stream)) => Some(pump_output(writer.clone(), stream.stream_id, stderr)),
        _ => None,
    };

    if let Some(task) = stdout_task {
        let _ = task.await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }

    // Attach reports no exit code; close the error stream cleanly.
    close_error_stream(writer, error_id, 0).await;
    Ok(())
}

/// Copy a container output stream into a SPDY stream, terminating it with a
/// FIN once it hits EOF.
fn pump_output(
    writer: Writer,
    stream_id: u32,
    mut reader: BoxedReader,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buffer = [0u8; MAX_DATA_CHUNK];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    let mut w = writer.lock().await;
                    if w.data(stream_id, false, &buffer[..n]).await.is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let mut w = writer.lock().await;
        let _ = w.data(stream_id, true, &[]).await;
    })
}

fn drain_stream(mut rx: mpsc::UnboundedReceiver<StreamMsg>) {
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
}

/// Close the error stream to signal exec/attach completion: an empty FIN for
/// success, a `NonZeroExitCode` status otherwise.
async fn close_error_stream(writer: &Writer, error_id: Option<u32>, exit_code: i32) {
    let Some(error_id) = error_id else { return };
    let mut w = writer.lock().await;
    if exit_code == 0 {
        let _ = w.data(error_id, true, &[]).await;
    } else {
        let status = exit_status_json(exit_code);
        let _ = w.data(error_id, true, status.as_bytes()).await;
    }
}

/// Report a setup failure to the client via the error stream, then succeed
/// (the failure is the streamed payload, not a transport error).
async fn fail_error_stream(
    writer: &Writer,
    error_id: Option<u32>,
    message: &str,
) -> Result<(), String> {
    if let Some(error_id) = error_id {
        let status = format!(
            "{{\"metadata\":{{}},\"status\":\"Failure\",\"message\":\"{}\"}}",
            message.replace('"', "'")
        );
        let mut w = writer.lock().await;
        let _ = w.data(error_id, true, status.as_bytes()).await;
    }
    Err(message.to_string())
}

// --------------------------------------------------------------------------
// port-forward session
// --------------------------------------------------------------------------

pub(crate) async fn run_portforward_session<B: StreamingBackend>(
    read: tokio::net::tcp::OwnedReadHalf,
    writer: Writer,
    pod_sandbox_id: String,
    backend: Arc<B>,
) -> Result<(), String> {
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let reader = spawn_reader(read, writer.clone(), ev_tx);

    while let Some(stream) = ev_rx.recv().await {
        let base = base_type(&stream.stream_type);
        if base != "data" {
            // The error stream (and anything else) is accepted but unused;
            // draining keeps the reader's channel from filling.
            drain_stream(stream.data);
            continue;
        }
        let Some(port) = stream
            .stream_type
            .split(':')
            .nth(1)
            .and_then(|p| p.parse::<u16>().ok())
        else {
            let mut w = writer.lock().await;
            let _ = w.rst_stream(stream.stream_id, 1).await;
            drain_stream(stream.data);
            continue;
        };

        let writer = writer.clone();
        let backend = backend.clone();
        let sandbox_id = pod_sandbox_id.clone();
        tokio::spawn(async move {
            forward_port(
                writer,
                stream.stream_id,
                stream.data,
                &*backend,
                &sandbox_id,
                port,
            )
            .await;
        });
    }

    reader.abort();
    Ok(())
}

/// Splice one SPDY data stream to a TCP connection opened to `port` inside
/// the sandbox (via the backend's dialer).
async fn forward_port<B: StreamingBackend>(
    writer: Writer,
    stream_id: u32,
    mut inbound: mpsc::UnboundedReceiver<StreamMsg>,
    backend: &B,
    sandbox_id: &str,
    port: u16,
) {
    let conn = match backend.dial_in_sandbox(sandbox_id, port as i32).await {
        Ok(conn) => conn,
        Err(e) => {
            warn!("port-forward dial {port} in sandbox {sandbox_id} failed: {e}");
            let mut w = writer.lock().await;
            let _ = w.rst_stream(stream_id, 1).await;
            return;
        }
    };
    let (mut conn_read, mut conn_write) = tokio::io::split(conn);

    // SPDY -> TCP
    let to_conn = tokio::spawn(async move {
        while let Some(msg) = inbound.recv().await {
            match msg {
                StreamMsg::Data(bytes) => {
                    if conn_write.write_all(&bytes).await.is_err() {
                        break;
                    }
                    if conn_write.flush().await.is_err() {
                        break;
                    }
                }
                StreamMsg::Eof => break,
            }
        }
        let _ = conn_write.shutdown().await;
    });

    // TCP -> SPDY
    let mut buffer = [0u8; MAX_DATA_CHUNK];
    loop {
        match conn_read.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                let mut w = writer.lock().await;
                if w.data(stream_id, false, &buffer[..n]).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    {
        let mut w = writer.lock().await;
        let _ = w.data(stream_id, true, &[]).await;
    }
    to_conn.abort();
}

// SPDX-License-Identifier: Apache-2.0

//! Streaming exec/attach/portforward over the Docker Engine API (plan 03 B4).
//!
//! The SPDY server and stream protocols live in `cri-server`; this module
//! implements its [`StreamingBackend`] seam:
//!
//! - `exec_stream` ports cri-dockerd's `NativeExecHandler`: `create_exec` +
//!   `start_exec`, demultiplex the hijacked connection's `LogOutput` frames
//!   to stdout/stderr pipes, and recover the exit code by polling
//!   `inspect_exec` with bounded retries once the output stream closes.
//!   (Terminal resize is not plumbed: the cri-server session drains the
//!   client's resize stream, so there is no resize seam to implement yet.)
//! - `attach_stream` is `attach_container` with the same demultiplexer.
//! - `dial_in_sandbox` connects to `127.0.0.1:<port>` from *inside* the
//!   sandbox's network namespace — `setns(2)` on a throwaway OS thread, the
//!   aurae pattern — so port-forward reaches localhost-bound servers, which
//!   dialing the pod IP from the host cannot.

use async_trait::async_trait;
use bollard::container::{AttachContainerOptions, LogOutput};
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use cri_server::error::{Error, Result};
use cri_server::streaming::{AsyncReadWrite, BoxedReader, BoxedWriter};
use cri_server::{AttachStreams, ExecStreams, StreamingBackend};
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncWriteExt;

use crate::backend::{docker_err, BollardBackend};
use crate::container::exec_exit_code;

/// Per-stream pipe capacity between the Docker demultiplexer and the SPDY
/// session; the demultiplexer blocks (backpressure) when a client is slow.
const PIPE_CAPACITY: usize = 64 * 1024;

/// Demultiplex a hijacked Docker output stream (`LogOutput` frames) into
/// stdout/stderr pipe readers. The pump task ends at EOF; `on_close` then
/// resolves the exit code sent to the returned receiver (255 on failure, per
/// the [`ExecStreams::exit`] contract).
fn demux_output<S, F, Fut>(
    mut output: S,
    on_close: F,
) -> (
    BoxedReader,
    BoxedReader,
    tokio::sync::oneshot::Receiver<i32>,
)
where
    S: Stream<Item = std::result::Result<LogOutput, bollard::errors::Error>>
        + Send
        + Unpin
        + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = i32> + Send,
{
    let (mut stdout_writer, stdout_reader) = tokio::io::duplex(PIPE_CAPACITY);
    let (mut stderr_writer, stderr_reader) = tokio::io::duplex(PIPE_CAPACITY);
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        while let Some(frame) = output.next().await {
            let result = match frame {
                // Console frames are the tty (non-multiplexed) output.
                Ok(LogOutput::StdOut { message }) | Ok(LogOutput::Console { message }) => {
                    stdout_writer.write_all(&message).await
                }
                Ok(LogOutput::StdErr { message }) => stderr_writer.write_all(&message).await,
                Ok(LogOutput::StdIn { .. }) => Ok(()),
                Err(e) => {
                    tracing::debug!("docker stream ended: {e}");
                    break;
                }
            };
            if result.is_err() {
                // Both consumers hung up; no point draining Docker.
                break;
            }
        }
        // Dropping the writers signals EOF to the SPDY output pumps.
        drop(stdout_writer);
        drop(stderr_writer);
        let _ = exit_tx.send(on_close().await);
    });

    (Box::pin(stdout_reader), Box::pin(stderr_reader), exit_rx)
}

#[async_trait]
impl StreamingBackend for BollardBackend {
    async fn exec_stream(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
    ) -> Result<ExecStreams> {
        if cmd.is_empty() {
            return Err(Error::InvalidArgument("exec command is required".into()));
        }
        let exec = self
            .docker
            .create_exec(
                container_id,
                CreateExecOptions::<String> {
                    attach_stdin: Some(stdin),
                    attach_stdout: Some(true),
                    // With a tty stdout and stderr are one stream.
                    attach_stderr: Some(!tty),
                    tty: Some(tty),
                    cmd: Some(cmd),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| docker_err("create exec", e))?;

        let started = self
            .docker
            .start_exec(
                &exec.id,
                Some(StartExecOptions {
                    detach: false,
                    tty,
                    output_capacity: None,
                }),
            )
            .await
            .map_err(|e| docker_err("start exec", e))?;
        let StartExecResults::Attached { output, input } = started else {
            return Err(Error::Internal("exec unexpectedly started detached".into()));
        };

        let docker = self.docker.clone();
        let exec_id = exec.id.clone();
        let (stdout, stderr, exit) = demux_output(output, move || async move {
            exec_exit_code(&docker, &exec_id).await.unwrap_or(255)
        });

        Ok(ExecStreams {
            stdin: stdin.then(|| Box::pin(input) as BoxedWriter),
            stdout: Some(stdout),
            stderr: (!tty).then_some(stderr),
            exit,
        })
    }

    async fn attach_stream(
        &self,
        container_id: &str,
        tty: bool,
        stdin: bool,
    ) -> Result<AttachStreams> {
        let results = self
            .docker
            .attach_container(
                container_id,
                Some(AttachContainerOptions::<String> {
                    stdin: Some(stdin),
                    stdout: Some(true),
                    stderr: Some(!tty),
                    stream: Some(true),
                    logs: Some(false),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|e| docker_err("attach container", e))?;

        // Attach has no exit code; the demultiplexer's close callback is a
        // constant and the receiver is dropped.
        let (stdout, stderr, _exit) = demux_output(results.output, || async { 0 });

        Ok(AttachStreams {
            stdin: stdin.then(|| Box::pin(results.input) as BoxedWriter),
            stdout: Some(stdout),
            stderr: (!tty).then_some(stderr),
        })
    }

    async fn dial_in_sandbox(
        &self,
        sandbox_id: &str,
        port: i32,
    ) -> Result<Box<dyn AsyncReadWrite>> {
        let port = u16::try_from(port)
            .map_err(|_| Error::InvalidArgument(format!("invalid port {port}")))?;
        let inspect = self
            .docker
            .inspect_container(
                sandbox_id,
                None::<bollard::container::InspectContainerOptions>,
            )
            .await
            .map_err(|e| docker_err("inspect sandbox for port-forward", e))?;
        let state = inspect.state.unwrap_or_default();
        let pid = state
            .pid
            .filter(|&p| p > 0 && state.running.unwrap_or(false))
            .ok_or_else(|| {
                Error::FailedPrecondition(format!("sandbox {sandbox_id} is not running"))
            })?;

        let stream = dial_in_netns(pid as i32, port)
            .await
            .map_err(|e| Error::Unavailable(format!("dial 127.0.0.1:{port} in netns: {e}")))?;
        Ok(Box::new(stream))
    }
}

/// Connect to `127.0.0.1:<port>` from inside the network namespace of
/// `init_pid` (the sandbox's pause process). The `setns(2)` runs on a
/// throwaway OS thread so it never pins a tokio worker (or its blocking
/// pool, where DNS lives) into the wrong namespace; the connected socket
/// works from any thread afterwards.
#[cfg(target_os = "linux")]
async fn dial_in_netns(
    init_pid: i32,
    port: u16,
) -> std::result::Result<tokio::net::TcpStream, String> {
    let std_stream = tokio::task::spawn_blocking(move || {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = (|| {
                use std::os::fd::AsRawFd;
                let ns = std::fs::File::open(format!("/proc/{init_pid}/ns/net"))
                    .map_err(|e| format!("opening netns: {e}"))?;
                // SAFETY: plain setns(2) on an fd we own; it re-homes only
                // this throwaway thread.
                if unsafe { libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET) } != 0 {
                    return Err(format!("setns: {}", std::io::Error::last_os_error()));
                }
                std::net::TcpStream::connect(("127.0.0.1", port))
                    .map_err(|e| format!("connect: {e}"))
            })();
            let _ = tx.send(result);
        });
        let result = rx.recv().map_err(|e| format!("netns dial: {e}"))?;
        let _ = handle.join();
        result
    })
    .await
    .map_err(|e| format!("netns dial task: {e}"))??;

    std_stream
        .set_nonblocking(true)
        .map_err(|e| format!("set_nonblocking: {e}"))?;
    tokio::net::TcpStream::from_std(std_stream).map_err(|e| format!("from_std: {e}"))
}

#[cfg(not(target_os = "linux"))]
async fn dial_in_netns(
    _init_pid: i32,
    _port: u16,
) -> std::result::Result<tokio::net::TcpStream, String> {
    Err("port-forward requires Linux network namespaces".to_string())
}

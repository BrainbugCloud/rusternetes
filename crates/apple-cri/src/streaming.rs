// SPDX-License-Identifier: Apache-2.0

//! Exec / Attach / PortForward stream handles.
//!
//! Port-forward is the one CRI operation that is **simpler** on macOS than on
//! Linux. A Linux shim has to `setns(CLONE_NEWNET)` into the sandbox's network
//! namespace before connecting (bollard-cri does exactly that, which is why its
//! port-forward path cannot compile for Darwin). Here every container has a real
//! vmnet address that is routable from the host, so forwarding is a plain
//! `TcpStream::connect` — no namespace entering, no privileged syscall.
//!
//! `Exec` maps cleanly onto `container exec`, which gives separate stdin/stdout/
//! stderr pipes and propagates the process exit code.
//!
//! `Attach` reuses the log-relay supervisor rather than opening a second stream:
//! the container's stdio is already owned by `container start --attach` (see
//! [`crate::logs`]), and Apple offers no second attach to a running container.
//! Subscribing to that supervisor's fan-out keeps stdout and stderr separate and
//! — the part that actually matters — gives both streams a real EOF when the
//! container exits, which is what tells the CRI streaming protocol to close the
//! session. Attach stdin is wired to the supervisor's own stdin, so
//! `stdin: true` containers are genuinely interactive, and closing that stdin
//! reaches the guest (the `StdinOnce` shell-exits-on-EOF path).

use async_trait::async_trait;
use cri_server::error::{Error, Result};
use cri_server::streaming::{AsyncReadWrite, AttachStreams, ExecStreams, StreamingBackend};

use crate::backend::AppleBackend;

#[async_trait]
impl StreamingBackend for AppleBackend {
    async fn exec_stream(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
    ) -> Result<ExecStreams> {
        if self.store.container(container_id).is_none() {
            return Err(Error::NotFound(format!("container {container_id}")));
        }
        if cmd.is_empty() {
            return Err(Error::InvalidArgument("exec with no command".into()));
        }
        let (mut child, pty) = self.cli.spawn_exec(container_id, &cmd, tty, stdin)?;

        // With a pty, input and output share the master fd and there is no
        // separate stderr — which is exactly CRI's tty-exec contract.
        let (stdin_pipe, stdout, stderr) =
            match pty {
                Some(master) => {
                    let read_half = master
                        .try_clone()
                        .map_err(|e| Error::Internal(format!("dup pty master: {e}")))?;
                    (
                        Some(Box::pin(tokio::fs::File::from_std(master))
                            as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>),
                        Some(Box::pin(tokio::fs::File::from_std(read_half))
                            as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
                        None,
                    )
                }
                None => (
                    child.stdin.take().map(|s| {
                        Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>
                    }),
                    child.stdout.take().map(|s| {
                        Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>
                    }),
                    // A tty exec has no demultiplexed stderr either way.
                    if tty {
                        None
                    } else {
                        child.stderr.take().map(|s| {
                            Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>
                        })
                    },
                ),
            };

        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let id = container_id.to_string();
        tokio::spawn(async move {
            let code = match child.wait().await {
                Ok(status) => status.code().unwrap_or(255),
                Err(err) => {
                    tracing::warn!(container = %id, %err, "waiting on exec process");
                    255
                }
            };
            let _ = exit_tx.send(code);
        });

        Ok(ExecStreams {
            stdin: stdin_pipe,
            stdout,
            stderr,
            exit: exit_rx,
        })
    }

    async fn attach_stream(
        &self,
        container_id: &str,
        _tty: bool,
        stdin: bool,
    ) -> Result<AttachStreams> {
        if self.store.container(container_id).is_none() {
            return Err(Error::NotFound(format!("container {container_id}")));
        }

        // Read the live stdio straight off the supervisor's pipes rather than
        // starting a second `container logs --follow`. That keeps stdout and
        // stderr apart, and — critically — gives the streams a real EOF when the
        // container exits, which is how the CRI streaming protocol ends an
        // attach session. `None` means the container has already exited, so
        // there is nothing left to stream.
        let (stdout, stderr) = match self.log_relays.subscribe(container_id) {
            Some((out, err)) => (
                Some(Box::pin(out) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
                Some(Box::pin(err) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
            ),
            None => {
                tracing::debug!(
                    container = %container_id,
                    "attach to a container with no live relay; streams close immediately"
                );
                (None, None)
            }
        };

        let stdin_pipe = if stdin {
            self.log_relays.take_stdin(container_id).map(|s| match s {
                crate::logs::RelayStdin::Pipe(p) => {
                    Box::pin(p) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>
                }
                crate::logs::RelayStdin::Pty(f) => {
                    Box::pin(f) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>
                }
            })
        } else {
            None
        };
        if stdin && stdin_pipe.is_none() {
            tracing::warn!(
                container = %container_id,
                "attach requested stdin but no supervisor stdin pipe is available \
                 (container not created with stdin: true, or already attached)"
            );
        }

        Ok(AttachStreams {
            stdin: stdin_pipe,
            stdout,
            stderr,
        })
    }

    async fn dial_in_sandbox(
        &self,
        sandbox_id: &str,
        port: i32,
    ) -> Result<Box<dyn AsyncReadWrite>> {
        let Some(sandbox) = self.store.sandbox(sandbox_id) else {
            return Err(Error::NotFound(format!("sandbox {sandbox_id}")));
        };
        let want = u16::try_from(port)
            .map_err(|_| Error::InvalidArgument(format!("port {port} out of range")))?;

        // Preferred path: a plain host→guest TCP connect. The container's vmnet
        // address needs no namespace entering, unlike Linux.
        let mut first_error = None;
        if let Some(ip) = self.observe_sandbox_ip(sandbox_id).await {
            match tokio::net::TcpStream::connect((ip.as_str(), want)).await {
                Ok(stream) => return Ok(Box::new(stream)),
                Err(e) => {
                    tracing::debug!(%ip, port = want, %e, "direct port-forward connect failed");
                    first_error = Some(format!("{ip}:{want}: {e}"));
                }
            }
        }

        // Fallback: if the sandbox published this container port on the host,
        // go through that instead. Some hosts refuse to route from macOS into a
        // container's vmnet subnet at all (see README "Host↔container
        // connectivity"), and the published port is then the only way in.
        if let Some(pm) = sandbox
            .port_mappings
            .iter()
            .find(|pm| pm.container_port == port && pm.host_port > 0)
        {
            let host = if pm.host_ip.is_empty() {
                "127.0.0.1".to_string()
            } else {
                pm.host_ip.clone()
            };
            let host_port = u16::try_from(pm.host_port).map_err(|_| {
                Error::InvalidArgument(format!("host port {} out of range", pm.host_port))
            })?;
            match tokio::net::TcpStream::connect((host.as_str(), host_port)).await {
                Ok(stream) => {
                    tracing::debug!(%host, host_port, "port-forward via published host port");
                    return Ok(Box::new(stream));
                }
                Err(e) => {
                    return Err(Error::Internal(format!(
                        "port-forward to sandbox {sandbox_id} port {want} failed: \
                         direct ({}) and published {host}:{host_port}: {e}",
                        first_error.unwrap_or_else(|| "no sandbox address".into()),
                    )))
                }
            }
        }

        Err(match first_error {
            Some(err) => Error::Internal(format!(
                "port-forward to sandbox {sandbox_id} port {want} failed: {err}"
            )),
            None => Error::FailedPrecondition(format!(
                "sandbox {sandbox_id} has no address to forward to (no running container yet)"
            )),
        })
    }
}

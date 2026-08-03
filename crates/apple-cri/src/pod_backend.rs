// SPDX-License-Identifier: Apache-2.0

//! [`PodBackend`]: the CRI backend over **real pod sandboxes** — one microVM per
//! pod, N containers inside it.
//!
//! The sibling [`AppleBackend`](crate::backend::AppleBackend) drives Apple's
//! `container` CLI and gets one microVM per *container*; this one drives the VMM
//! broker, which runs Apple's `LinuxPod`. Selected with `--backend pod`.
//!
//! Like `AppleBackend`, this is pure delegation: everything interesting is in
//! [`crate::pod_runtime`]. What lives here is the shape CRI wants back —
//! statuses, filters, and the stream plumbing.
//!
//! # Streaming
//!
//! `cri-server` hands out `AsyncRead`/`AsyncWrite` halves; the broker speaks unix
//! sockets. **This side binds the listeners and the broker connects** — see
//! [`crate::pod_runtime::StreamPaths`] for why that direction and not the other.
//! `listen(2)` is called before the RPC, so the broker's `connect(2)` lands in
//! the backlog and is accepted straight after.
//!
//! # TODO — divergences carried into this path
//!
//! Each is accepted rather than fixed; see `STATUS.md` for the full reasoning.
//!
//! * **No restart checkpointing.** The CLI path has [`crate::state`], which
//!   survives a shim restart. Pod state lives only in memory here, so a restart
//!   loses every sandbox. `AppleBackend::reconcile` has no counterpart yet.
//! * **TTY `Attach` cannot be resized.** `LinuxPod` exposes `resize` only on the
//!   `LinuxProcess` from `execInContainer`, so an exec resizes and an attach does
//!   not.
//! * **No per-container IPC/UTS namespace, and no per-container OCI runtime.**
//!   `LinuxPod` gives each container a fresh `ipc`/`uts` and hardcodes
//!   `ociRuntimePath: nil`.
//! * **`RemoveContainer` leaves guest-side state** until the pod stops.
//! * **`UpdateContainerResources` is unimplemented.** A VM's memory is fixed at
//!   boot, so in-place resize can only ever grow within the boot ceiling; the
//!   sizing strategy that would make it meaningful is not decided.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::streaming::{AttachStreams, ExecStreams, StreamingBackend};
use cri_server::{ExecSyncResult, ImageBackend, RuntimeBackend};
use tokio::net::{UnixListener, UnixStream};

use crate::pod_runtime::{ContainerRecord, PodRuntime, PodRuntimeConfig, StreamPaths};

pub struct PodBackend {
    pods: Arc<PodRuntime>,
    /// Where per-exec stream sockets are bound. Kept short: a unix socket path
    /// over ~104 bytes is rejected by the kernel, and these nest a directory per
    /// exec under it.
    stream_dir: PathBuf,
}

impl PodBackend {
    pub fn new(broker_socket: PathBuf, config: PodRuntimeConfig, stream_dir: PathBuf) -> Self {
        Self {
            pods: Arc::new(PodRuntime::connect(broker_socket, config)),
            stream_dir,
        }
    }
}

/// A bound listener plus the path the broker should connect to.
struct Pending {
    listener: UnixListener,
    path: PathBuf,
}

fn bind(dir: &Path, name: &str) -> Result<Pending> {
    let path = dir.join(name);
    let listener = UnixListener::bind(&path)?;
    Ok(Pending { listener, path })
}

impl Pending {
    /// Take the connection the broker made during the RPC.
    async fn accept(self) -> Result<UnixStream> {
        let (stream, _) = self.listener.accept().await?;
        Ok(stream)
    }
}

/// Sockets for one exec/attach, in a directory removed when the call is torn
/// down.
struct Streams {
    dir: PathBuf,
    stdin: Option<Pending>,
    stdout: Option<Pending>,
    stderr: Option<Pending>,
}

impl Streams {
    fn bind_all(root: &Path, id: &str, tty: bool, stdin: bool) -> Result<Self> {
        let dir = root.join(id);
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            stdin: if stdin { Some(bind(&dir, "i")?) } else { None },
            stdout: Some(bind(&dir, "o")?),
            // A tty folds stderr into stdout; asking for a separate one would
            // bind a socket nothing ever connects to, and the accept below would
            // hang for the life of the exec.
            stderr: if tty { None } else { Some(bind(&dir, "e")?) },
            dir,
        })
    }

    fn paths(&self) -> StreamPaths {
        StreamPaths {
            stdin: self.stdin.as_ref().map(|p| p.path.clone()),
            stdout: self.stdout.as_ref().map(|p| p.path.clone()),
            stderr: self.stderr.as_ref().map(|p| p.path.clone()),
        }
    }

    /// Accept every connection the broker made, and hand back CRI's halves.
    async fn accept_all(
        self,
    ) -> Result<(
        Option<cri_server::streaming::BoxedWriter>,
        Option<cri_server::streaming::BoxedReader>,
        Option<cri_server::streaming::BoxedReader>,
    )> {
        let stdin = match self.stdin {
            Some(p) => Some(Box::pin(p.accept().await?) as cri_server::streaming::BoxedWriter),
            None => None,
        };
        let stdout = match self.stdout {
            Some(p) => Some(Box::pin(p.accept().await?) as cri_server::streaming::BoxedReader),
            None => None,
        };
        let stderr = match self.stderr {
            Some(p) => Some(Box::pin(p.accept().await?) as cri_server::streaming::BoxedReader),
            None => None,
        };
        // The sockets are connected; the directory entries are no longer needed.
        let _ = std::fs::remove_dir_all(&self.dir);
        Ok((stdin, stdout, stderr))
    }
}

#[async_trait]
impl RuntimeBackend for PodBackend {
    async fn version(&self) -> Result<VersionResponse> {
        Ok(VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "apple-cri-pod".to_string(),
            runtime_version: env!("CARGO_PKG_VERSION").to_string(),
            runtime_api_version: "v1".to_string(),
        })
    }

    async fn status(&self) -> Result<RuntimeStatus> {
        // Both conditions are reported true once the broker answers; a broker
        // that is not there surfaces as a connect error on the first real call
        // rather than as a cheerful "ready".
        Ok(RuntimeStatus {
            conditions: vec![
                RuntimeCondition {
                    r#type: "RuntimeReady".to_string(),
                    status: true,
                    ..Default::default()
                },
                RuntimeCondition {
                    r#type: "NetworkReady".to_string(),
                    status: true,
                    ..Default::default()
                },
            ],
        })
    }

    async fn update_runtime_config(&self, _pod_cidr: Option<String>) -> Result<()> {
        // The pod CIDR is the broker's: it allocates from its own vmnet subnet
        // (`--pod-subnet`), so accepting the kubelet's value here would claim an
        // addressing scheme nothing honours.
        Ok(())
    }

    async fn runtime_config(&self) -> Result<RuntimeConfigResponse> {
        Ok(RuntimeConfigResponse::default())
    }

    async fn run_pod_sandbox(
        &self,
        config: PodSandboxConfig,
        _runtime_handler: &str,
    ) -> Result<String> {
        let id = crate::naming::new_id();
        self.pods.run_pod_sandbox(&id, &config).await
    }

    async fn stop_pod_sandbox(&self, id: &str) -> Result<()> {
        self.pods.stop_pod_sandbox(id).await
    }

    async fn remove_pod_sandbox(&self, id: &str) -> Result<()> {
        self.pods.remove_pod_sandbox(id).await
    }

    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        let entry = self
            .pods
            .sandbox(id)
            .await
            .ok_or_else(|| Error::NotFound(format!("pod sandbox {id} not found")))?;
        Ok(PodSandboxStatus {
            id: entry.id,
            metadata: entry.metadata,
            state: if entry.ready {
                PodSandboxState::SandboxReady as i32
            } else {
                PodSandboxState::SandboxNotready as i32
            },
            created_at: entry.created_at,
            // One address for the pod: the VM *is* its network, so every
            // container shares it.
            network: Some(PodSandboxNetworkStatus {
                ip: entry.ip.unwrap_or_default(),
                ..Default::default()
            }),
            labels: entry.labels,
            annotations: entry.annotations,
            ..Default::default()
        })
    }

    async fn list_pod_sandbox(&self, filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>> {
        let mut out = Vec::new();
        for entry in self.pods.sandboxes().await {
            let state = if entry.ready {
                PodSandboxState::SandboxReady as i32
            } else {
                PodSandboxState::SandboxNotready as i32
            };
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != entry.id {
                    continue;
                }
                if let Some(s) = &f.state {
                    if s.state != state {
                        continue;
                    }
                }
                if !f
                    .label_selector
                    .iter()
                    .all(|(k, v)| entry.labels.get(k) == Some(v))
                {
                    continue;
                }
            }
            out.push(PodSandbox {
                id: entry.id,
                metadata: entry.metadata,
                state,
                created_at: entry.created_at,
                labels: entry.labels,
                annotations: entry.annotations,
                ..Default::default()
            });
        }
        Ok(out)
    }

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let id = crate::naming::new_id();
        self.pods
            .create_container(sandbox_id, &id, &config, &sandbox_config)
            .await
    }

    async fn start_container(&self, id: &str) -> Result<()> {
        self.pods.start_container(id).await
    }

    async fn stop_container(&self, id: &str, timeout_secs: i64) -> Result<()> {
        self.pods.stop_container(id, timeout_secs).await
    }

    async fn remove_container(&self, id: &str) -> Result<()> {
        self.pods.remove_container(id).await
    }

    async fn list_containers(&self, filter: Option<ContainerFilter>) -> Result<Vec<Container>> {
        let mut out = Vec::new();
        for entry in self.pods.container_entries().await {
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != entry.id {
                    continue;
                }
                if !f.pod_sandbox_id.is_empty() && f.pod_sandbox_id != entry.pod_id {
                    continue;
                }
                if !f
                    .label_selector
                    .iter()
                    .all(|(k, v)| entry.labels.get(k) == Some(v))
                {
                    continue;
                }
            }
            // Computed first: the literal below moves fields out of `entry`.
            let state = container_state(&entry) as i32;
            out.push(Container {
                id: entry.id,
                pod_sandbox_id: entry.pod_id,
                metadata: entry.metadata,
                image: Some(ImageSpec {
                    image: entry.image.clone(),
                    ..Default::default()
                }),
                image_ref: entry.image_ref.clone(),
                // "MUST always match PullImageResponse.image_ref when referring
                // to the same image" (CRI api.proto). Leaving it empty makes
                // critest skip the Image Identifier Consistency specs rather
                // than fail them, which reads as a pass and is not one.
                image_id: entry.image_ref,
                state,
                created_at: entry.created_at,
                labels: entry.labels,
                annotations: entry.annotations,
            });
        }
        Ok(out)
    }

    async fn container_status(&self, id: &str) -> Result<ContainerStatus> {
        let entry = self
            .pods
            .container_entry(id)
            .await
            .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
        let state = container_state(&entry) as i32;
        Ok(ContainerStatus {
            id: entry.id,
            metadata: entry.metadata,
            state,
            created_at: entry.created_at,
            started_at: entry.started_at,
            finished_at: entry.finished_at,
            exit_code: entry.exit_code,
            // CRI reports the *reason a container is in its current state*, and
            // only once it has one. Matching containerd's strings
            // (`internal/cri/server/container_status.go`) keeps `crictl ps` and
            // kubelet's event text familiar.
            reason: match (entry.finished_at, entry.exit_code) {
                (0, _) => String::new(),
                (_, 0) => "Completed".to_string(),
                _ => "Error".to_string(),
            },
            image: Some(ImageSpec {
                image: entry.image.clone(),
                ..Default::default()
            }),
            image_ref: entry.image_ref.clone(),
            image_id: entry.image_ref,
            labels: entry.labels,
            annotations: entry.annotations,
            ..Default::default()
        })
    }

    async fn update_container_resources(
        &self,
        _id: &str,
        _resources: LinuxContainerResources,
    ) -> Result<()> {
        Err(Error::Unimplemented(
            "UpdateContainerResources: a pod VM's memory is fixed at boot, so an \
             in-place resize can only grow within the boot ceiling"
                .to_string(),
        ))
    }

    async fn exec_sync(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        let exec_id = crate::naming::new_id();
        let result = self
            .pods
            .exec_sync(id, &exec_id, cmd, Vec::new(), timeout_secs)
            .await?;
        Ok(ExecSyncResult {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
        })
    }

    async fn container_stats(&self, id: &str) -> Result<ContainerStats> {
        self.pods
            .container_stats(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("no stats for container {id}")))
    }

    async fn list_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        let mut out = Vec::new();
        for entry in self.pods.container_entries().await {
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != entry.id {
                    continue;
                }
                if !f.pod_sandbox_id.is_empty() && f.pod_sandbox_id != entry.pod_id {
                    continue;
                }
                // `ListContainerStats` filters by label exactly as
                // `ListContainers` does; leaving it out returned every
                // container's stats for a selector that matched one.
                if !f
                    .label_selector
                    .iter()
                    .all(|(k, v)| entry.labels.get(k) == Some(v))
                {
                    continue;
                }
            }
            // A container that has gone away between listing and asking is
            // skipped, not an error for the whole call.
            if let Ok(Some(stats)) = self.pods.container_stats(&entry.id).await {
                out.push(stats);
            }
        }
        Ok(out)
    }

    async fn reopen_container_log(&self, id: &str) -> Result<()> {
        self.pods.reopen_container_log(id).await
    }
}

#[async_trait]
impl ImageBackend for PodBackend {
    async fn list_images(&self, _filter: Option<ImageFilter>) -> Result<Vec<Image>> {
        self.pods.list_images().await
    }

    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>> {
        self.pods.image_status(&image.image).await
    }

    async fn pull_image(
        &self,
        image: &ImageSpec,
        _auth: Option<AuthConfig>,
        _sandbox_config: Option<PodSandboxConfig>,
    ) -> Result<String> {
        self.pods.pull_image(&image.image).await
    }

    async fn remove_image(&self, image: &ImageSpec) -> Result<()> {
        self.pods.remove_image(&image.image).await
    }

    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>> {
        self.pods.image_fs_info().await
    }
}

#[async_trait]
impl StreamingBackend for PodBackend {
    async fn exec_stream(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
    ) -> Result<ExecStreams> {
        let exec_id = crate::naming::new_id();
        let streams = Streams::bind_all(&self.stream_dir, &exec_id, tty, stdin)?;

        // Bound before the RPC: the broker connects during it, so nothing the
        // process writes on the way up is lost.
        self.pods
            .exec_streaming(
                container_id,
                &exec_id,
                &cmd,
                Vec::new(),
                tty,
                streams.paths(),
            )
            .await?;
        let (stdin, stdout, stderr) = streams.accept_all().await?;

        let (tx, exit) = tokio::sync::oneshot::channel();
        let runtime = self.pods.clone();
        let container = container_id.to_string();
        tokio::spawn(async move {
            // A dropped sender surfaces as 255, which is what a reap we never
            // saw should look like.
            if let Ok(code) = runtime.wait_exec(&container, &exec_id).await {
                let _ = tx.send(code);
            }
        });

        Ok(ExecStreams {
            stdin,
            stdout,
            stderr,
            exit,
        })
    }

    async fn attach_stream(
        &self,
        container_id: &str,
        tty: bool,
        stdin: bool,
    ) -> Result<AttachStreams> {
        let token = crate::naming::new_id();
        let streams = Streams::bind_all(&self.stream_dir, &token, tty, stdin)?;
        self.pods.attach(container_id, streams.paths()).await?;
        let (stdin, stdout, stderr) = streams.accept_all().await?;
        Ok(AttachStreams {
            stdin,
            stdout,
            stderr,
        })
    }

    async fn dial_in_sandbox(
        &self,
        sandbox_id: &str,
        port: i32,
    ) -> Result<Box<dyn cri_server::streaming::AsyncReadWrite>> {
        let port = u16::try_from(port)
            .map_err(|_| Error::InvalidArgument(format!("port {port} out of range")))?;
        Ok(Box::new(self.pods.port_forward(sandbox_id, port).await?))
    }
}

/// A container's CRI state, from the timestamps `PodRuntime` records.
///
/// `LinuxPod` keeps its own per-container state private, so this is derived
/// rather than asked for. The order matters: a container that exited before it
/// was ever observed running must still report EXITED, which is why
/// `finished_at` is checked first.
fn container_state(entry: &ContainerRecord) -> ContainerState {
    if entry.finished_at != 0 {
        ContainerState::ContainerExited
    } else if entry.started_at != 0 {
        ContainerState::ContainerRunning
    } else {
        ContainerState::ContainerCreated
    }
}

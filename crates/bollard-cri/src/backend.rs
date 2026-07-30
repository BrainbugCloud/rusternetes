// SPDX-License-Identifier: Apache-2.0

//! `BollardBackend`: the CRI backend over the Docker Engine API.
//!
//! Stages B1–B3 (plan 03): runtime info, full image manager, the pod sandbox
//! lifecycle (see [`crate::sandbox`]), and the container lifecycle with the
//! CRI log relay (see [`crate::container`], [`crate::logs`]). Streaming
//! exec/attach/portforward land in B4; until then those RPCs answer
//! `Unimplemented`.

use std::path::PathBuf;

use async_trait::async_trait;
use bollard::Docker;
use cri_proto::v1::*;
use cri_server::checkpoint::CheckpointStore;
use cri_server::error::{Error, Result};
use cri_server::{ExecSyncResult, RuntimeBackend};
use tokio::sync::Mutex;

/// Runtime configuration from the command line.
pub struct Config {
    pub pod_infra_container_image: String,
    pub root_dir: PathBuf,
}

pub struct BollardBackend {
    pub(crate) docker: Docker,
    pub(crate) config: Config,
    /// Per-sandbox state Docker cannot hold (port mappings, host-network
    /// flag), rooted at `--root-dir/sandbox/`.
    pub(crate) checkpoints: CheckpointStore,
    /// Whether the daemon uses the systemd cgroup driver (cached from the
    /// first `docker info`); decides the cgroup-parent syntax.
    pub(crate) systemd_cgroup: tokio::sync::OnceCell<bool>,
    /// Live CRI log relays, one per started container.
    pub(crate) log_relays: crate::logs::LogRelays,
    pub(crate) pod_cidr: Mutex<Option<String>>,
    /// Writable-layer sizes for container stats, refreshed in the
    /// background (see [`crate::stats`], plan 03 B5).
    pub(crate) disk_usage: std::sync::Arc<crate::stats::DiskUsageCache>,
    /// The daemon's root directory (`docker info`), cached: it is the
    /// filesystem id stats and image-fs info report.
    pub(crate) docker_root: tokio::sync::OnceCell<String>,
    /// Serializes `RemoveImage`: dockerd 404s a concurrent delete as soon
    /// as the winner untags, before the image record is purged — a remover
    /// must not report success while the image still resolves by ID.
    pub(crate) image_remove_lock: Mutex<()>,
}

/// Connect to a Docker Engine API endpoint (unix socket path or `unix://`).
pub fn connect_docker(host: &str) -> Result<Docker> {
    let docker = if let Some(path) = host
        .strip_prefix("unix://")
        .or_else(|| host.strip_prefix("unix:"))
    {
        Docker::connect_with_unix(path, 120, bollard::API_DEFAULT_VERSION)
    } else if host.starts_with("tcp://") || host.starts_with("http://") {
        Docker::connect_with_http(host, 120, bollard::API_DEFAULT_VERSION)
    } else {
        Docker::connect_with_unix(host, 120, bollard::API_DEFAULT_VERSION)
    };
    docker.map_err(|e| Error::Unavailable(format!("connecting to Docker at {host}: {e}")))
}

/// Map a bollard error to a CRI-conventional error.
pub(crate) fn docker_err(context: &str, err: bollard::errors::Error) -> Error {
    match err {
        bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message,
        } => Error::NotFound(format!("{context}: {message}")),
        bollard::errors::Error::DockerResponseServerError {
            status_code,
            message,
        } => Error::Internal(format!("{context}: docker {status_code}: {message}")),
        other => Error::Unavailable(format!("{context}: {other}")),
    }
}

impl BollardBackend {
    pub fn new(docker: Docker, config: Config) -> Result<Self> {
        std::fs::create_dir_all(&config.root_dir)?;
        let checkpoints = CheckpointStore::open(config.root_dir.join("sandbox"))?;
        Ok(Self {
            docker,
            config,
            checkpoints,
            systemd_cgroup: tokio::sync::OnceCell::new(),
            log_relays: crate::logs::LogRelays::new(),
            pod_cidr: Mutex::new(None),
            disk_usage: std::sync::Arc::new(crate::stats::DiskUsageCache::default()),
            docker_root: tokio::sync::OnceCell::new(),
            image_remove_lock: Mutex::new(()),
        })
    }

    /// Start the background writable-layer size sweep (see [`crate::stats`]).
    pub(crate) fn spawn_disk_usage_refresh(&self) {
        self.disk_usage.spawn_refresh(self.docker.clone());
    }

    /// The daemon's root directory, fetched once; the conventional default
    /// when `docker info` is unavailable.
    pub(crate) async fn docker_root_dir(&self) -> String {
        self.docker_root
            .get_or_init(|| async {
                self.docker
                    .info()
                    .await
                    .ok()
                    .and_then(|info| info.docker_root_dir)
                    .unwrap_or_else(|| "/var/lib/docker".to_string())
            })
            .await
            .clone()
    }

    pub async fn docker_version_banner(&self) -> Result<String> {
        let version = self
            .docker
            .version()
            .await
            .map_err(|e| docker_err("version", e))?;
        Ok(format!(
            "{} {} (API {})",
            version.platform.map(|p| p.name).unwrap_or_default(),
            version.version.unwrap_or_default(),
            version.api_version.unwrap_or_default(),
        ))
    }

    pub(crate) async fn cgroup_driver(&self) -> Result<CgroupDriver> {
        let systemd = self
            .systemd_cgroup
            .get_or_try_init(|| async {
                let info = self
                    .docker
                    .info()
                    .await
                    .map_err(|e| docker_err("info", e))?;
                Ok::<_, Error>(
                    info.cgroup_driver
                        == Some(bollard::models::SystemInfoCgroupDriverEnum::SYSTEMD),
                )
            })
            .await?;
        if *systemd {
            Ok(CgroupDriver::Systemd)
        } else {
            Ok(CgroupDriver::Cgroupfs)
        }
    }
}

#[async_trait]
impl RuntimeBackend for BollardBackend {
    async fn version(&self) -> Result<VersionResponse> {
        let version = self
            .docker
            .version()
            .await
            .map_err(|e| docker_err("version", e))?;
        Ok(VersionResponse {
            // The CRI (kubelet API) version this server implements.
            version: "0.1.0".to_string(),
            runtime_name: "docker".to_string(),
            runtime_version: version.version.unwrap_or_default(),
            runtime_api_version: version.api_version.unwrap_or_default(),
        })
    }

    async fn status(&self) -> Result<RuntimeStatus> {
        let runtime_ready = self.docker.ping().await.is_ok();
        Ok(RuntimeStatus {
            conditions: vec![
                RuntimeCondition {
                    r#type: "RuntimeReady".to_string(),
                    status: runtime_ready,
                    reason: if runtime_ready {
                        String::new()
                    } else {
                        "DockerDaemonUnreachable".to_string()
                    },
                    ..Default::default()
                },
                RuntimeCondition {
                    // MVP: sandbox connectivity rides on the Docker/Podman
                    // network (plan 03), so network readiness follows the
                    // daemon's.
                    r#type: "NetworkReady".to_string(),
                    status: runtime_ready,
                    ..Default::default()
                },
            ],
        })
    }

    async fn update_runtime_config(&self, pod_cidr: Option<String>) -> Result<()> {
        if let Some(cidr) = &pod_cidr {
            tracing::info!(pod_cidr = %cidr, "runtime config updated");
        }
        *self.pod_cidr.lock().await = pod_cidr;
        Ok(())
    }

    async fn runtime_config(&self) -> Result<RuntimeConfigResponse> {
        let driver = self.cgroup_driver().await?;
        Ok(RuntimeConfigResponse {
            linux: Some(LinuxRuntimeConfiguration {
                cgroup_driver: driver as i32,
            }),
        })
    }

    // ---- sandbox lifecycle (B2, see sandbox.rs) ---------------------------

    async fn run_pod_sandbox(
        &self,
        config: PodSandboxConfig,
        runtime_handler: &str,
    ) -> Result<String> {
        self.run_sandbox(config, runtime_handler).await
    }

    async fn stop_pod_sandbox(&self, id: &str) -> Result<()> {
        self.stop_sandbox(id).await
    }

    async fn remove_pod_sandbox(&self, id: &str) -> Result<()> {
        self.remove_sandbox(id).await
    }

    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        self.sandbox_status(id).await
    }

    async fn list_pod_sandbox(&self, filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>> {
        self.list_sandboxes(filter).await
    }

    // ---- container lifecycle (B3, see container.rs) -----------------------

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        self.create_app_container(sandbox_id, config, sandbox_config)
            .await
    }

    async fn start_container(&self, id: &str) -> Result<()> {
        self.start_app_container(id).await
    }

    async fn stop_container(&self, id: &str, timeout_secs: i64) -> Result<()> {
        self.stop_app_container(id, timeout_secs).await
    }

    async fn remove_container(&self, id: &str) -> Result<()> {
        self.remove_app_container(id).await
    }

    async fn list_containers(&self, filter: Option<ContainerFilter>) -> Result<Vec<Container>> {
        self.list_app_containers(filter).await
    }

    async fn container_status(&self, id: &str) -> Result<ContainerStatus> {
        self.app_container_status(id).await
    }

    async fn update_container_resources(
        &self,
        id: &str,
        resources: LinuxContainerResources,
    ) -> Result<()> {
        self.update_app_container_resources(id, resources).await
    }

    async fn exec_sync(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        self.exec_sync_in_container(id, cmd, timeout_secs).await
    }

    // ---- stats (basic in B3; rootfs size cache in B5) ----------------------

    async fn container_stats(&self, id: &str) -> Result<ContainerStats> {
        self.app_container_stats(id).await
    }

    async fn list_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        self.list_app_container_stats(filter).await
    }

    async fn reopen_container_log(&self, id: &str) -> Result<()> {
        self.reopen_app_container_log(id).await
    }
}

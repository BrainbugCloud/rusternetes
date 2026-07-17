// SPDX-License-Identifier: Apache-2.0

//! `BollardBackend`: the CRI backend over the Docker Engine API.
//!
//! Stage B1 (plan 03): runtime info + full image manager. The sandbox and
//! container lifecycle land in B2/B3 and currently answer `Unimplemented`
//! (lists return empty so read-only clients keep working).

use std::path::PathBuf;

use async_trait::async_trait;
use bollard::Docker;
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::{ExecSyncResult, RuntimeBackend};
use tokio::sync::Mutex;

/// Runtime configuration from the command line.
pub struct Config {
    #[allow(dead_code)] // consumed from B2 on (pause container)
    pub pod_infra_container_image: String,
    pub root_dir: PathBuf,
}

pub struct BollardBackend {
    pub(crate) docker: Docker,
    #[allow(dead_code)] // used from B2 on (pause image, checkpoints)
    pub(crate) config: Config,
    pub(crate) pod_cidr: Mutex<Option<String>>,
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
        Ok(Self {
            docker,
            config,
            pod_cidr: Mutex::new(None),
        })
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

    fn unimplemented<T>(what: &str) -> Result<T> {
        Err(Error::Unimplemented(format!(
            "{what} is not implemented yet (bollard-cri plan 03 B2/B3)"
        )))
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

    // ---- sandbox lifecycle: B2 ------------------------------------------

    async fn run_pod_sandbox(
        &self,
        _config: PodSandboxConfig,
        _runtime_handler: &str,
    ) -> Result<String> {
        Self::unimplemented("RunPodSandbox")
    }

    async fn stop_pod_sandbox(&self, _id: &str) -> Result<()> {
        Self::unimplemented("StopPodSandbox")
    }

    async fn remove_pod_sandbox(&self, _id: &str) -> Result<()> {
        Self::unimplemented("RemovePodSandbox")
    }

    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        Err(Error::NotFound(format!("sandbox {id} not found")))
    }

    async fn list_pod_sandbox(&self, _filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>> {
        Ok(Vec::new())
    }

    // ---- container lifecycle: B3 ----------------------------------------

    async fn create_container(
        &self,
        _sandbox_id: &str,
        _config: ContainerConfig,
        _sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        Self::unimplemented("CreateContainer")
    }

    async fn start_container(&self, _id: &str) -> Result<()> {
        Self::unimplemented("StartContainer")
    }

    async fn stop_container(&self, _id: &str, _timeout_secs: i64) -> Result<()> {
        Self::unimplemented("StopContainer")
    }

    async fn remove_container(&self, _id: &str) -> Result<()> {
        Self::unimplemented("RemoveContainer")
    }

    async fn list_containers(&self, _filter: Option<ContainerFilter>) -> Result<Vec<Container>> {
        Ok(Vec::new())
    }

    async fn container_status(&self, id: &str) -> Result<ContainerStatus> {
        Err(Error::NotFound(format!("container {id} not found")))
    }

    async fn update_container_resources(
        &self,
        _id: &str,
        _resources: LinuxContainerResources,
    ) -> Result<()> {
        Self::unimplemented("UpdateContainerResources")
    }

    async fn exec_sync(
        &self,
        _id: &str,
        _cmd: &[String],
        _timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        Self::unimplemented("ExecSync")
    }

    // ---- stats: B5 --------------------------------------------------------

    async fn container_stats(&self, _id: &str) -> Result<ContainerStats> {
        Self::unimplemented("ContainerStats")
    }

    async fn list_container_stats(
        &self,
        _filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        Ok(Vec::new())
    }

    async fn reopen_container_log(&self, _id: &str) -> Result<()> {
        Self::unimplemented("ReopenContainerLog")
    }
}

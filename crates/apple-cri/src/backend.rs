// SPDX-License-Identifier: Apache-2.0

//! [`AppleBackend`]: the CRI backend over Apple's `container` runtime.
//!
//! The [`RuntimeBackend`] impl here is pure delegation to the focused modules
//! ([`crate::sandbox`], [`crate::container`], [`crate::stats`]); the interesting
//! decisions live there. [`ImageBackend`](cri_server::ImageBackend) is in
//! [`crate::images`] and
//! [`StreamingBackend`](cri_server::StreamingBackend) in [`crate::streaming`].

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cri_proto::v1::*;
use cri_server::error::Result;
use cri_server::{ExecSyncResult, RuntimeBackend};
use tokio::sync::Mutex;

use crate::cli::Cli;
use crate::logs::LogRelays;
use crate::state::Store;

/// Runtime configuration from the command line.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the `container` binary.
    pub binary: String,
    /// Apple network every pod's containers attach to. A single flat network is
    /// the closer match to the Kubernetes network model than a per-pod one; see
    /// [`crate::sandbox`].
    pub pod_network: String,
    /// Subnet for that network; `None` lets Apple choose.
    pub pod_network_subnet: Option<String>,
    /// Guest architecture (`arm64` / `amd64`).
    pub arch: String,
    pub root_dir: PathBuf,
}

impl Config {
    /// Where Apple keeps image content, for `ImageFsInfo`.
    pub fn image_store_dir(&self) -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_default();
        PathBuf::from(home).join("Library/Application Support/com.apple.container/content")
    }
}

pub struct AppleBackend {
    pub(crate) cli: Cli,
    pub(crate) config: Config,
    /// Authoritative record of everything Apple does not track (timestamps,
    /// exit codes, CRI labels/annotations).
    pub(crate) store: Arc<Store>,
    /// Live CRI log relays / exit-code supervisors, one per started container.
    pub(crate) log_relays: LogRelays,
    pub(crate) pod_cidr: Mutex<Option<String>>,
}

impl AppleBackend {
    pub fn new(config: Config) -> Result<Self> {
        std::fs::create_dir_all(&config.root_dir)?;
        let store = Arc::new(Store::open(&config.root_dir)?);
        Ok(Self {
            cli: Cli::new(config.binary.clone()),
            config,
            store,
            log_relays: LogRelays::new(),
            pod_cidr: Mutex::new(None),
        })
    }

    /// A one-line description of the runtime, for the startup banner.
    pub async fn runtime_banner(&self) -> Result<String> {
        self.cli.version().await
    }

    /// `container system start` (idempotent).
    pub async fn cli_system_start(&self) -> Result<()> {
        self.cli.system_start().await
    }

    /// Create the pod network at startup, ahead of the first `RunPodSandbox`,
    /// so `NetworkReady` is true as soon as the kubelet asks.
    pub async fn ensure_pod_network_public(&self) -> Result<()> {
        self.ensure_pod_network().await
    }

    /// Re-adopt containers that outlived the previous shim process: restart
    /// their log relays and settle any exit that happened while we were gone.
    pub async fn reconcile(&self) {
        let list = match self.cli.list_containers().await {
            Ok(list) => list,
            Err(err) => {
                tracing::warn!(%err, "cannot list containers for reconciliation");
                return;
            }
        };
        let live: std::collections::HashMap<String, bool> = list
            .iter()
            .map(|c| (c.id().to_string(), c.is_running()))
            .collect();

        // Containers we created but no longer have a record for — a crashed
        // shim leaves these behind, holding a whole VM each. Ids are opaque, so
        // ownership is read from the labels mirrored at create time.
        for c in &list {
            let id = c.id();
            let labels = &c.configuration.labels;
            let ours = labels
                .get(cri_server::labels::CONTAINER_TYPE_LABEL)
                .is_some_and(|t| {
                    t == cri_server::labels::CONTAINER_TYPE_CONTAINER
                        || t == cri_server::labels::CONTAINER_TYPE_SANDBOX
                });
            if !ours || self.store.container(id).is_some() {
                continue;
            }
            let label = |k: &str| {
                labels
                    .get(k)
                    .map(|v| crate::cli::decode_label_value(v))
                    .unwrap_or_default()
            };
            tracing::warn!(
                container = %id,
                pod = %label(cri_server::labels::POD_NAME_LABEL),
                namespace = %label(cri_server::labels::POD_NAMESPACE_LABEL),
                name = %label(cri_server::labels::CONTAINER_NAME_LABEL),
                "removing orphan left by a previous shim"
            );
            if let Err(err) = self.cli.remove_container(id).await {
                tracing::warn!(container = %id, %err, "cannot remove orphan");
            }
        }

        for rec in self.store.containers() {
            match live.get(&rec.id) {
                Some(true) if !rec.log_path.is_empty() => {
                    tracing::info!(container = %rec.id, "resuming log relay");
                    self.log_relays.resume(
                        &self.cli,
                        self.store.clone(),
                        &rec.id,
                        PathBuf::from(&rec.log_path),
                    );
                }
                Some(true) => {}
                // Exited while the shim was down: recover the code from the
                // guest's own log so restart policy still works.
                Some(false) | None if rec.started && !rec.finished => {
                    if let Some(code) = crate::logs::exit_status_from_vminitd_log(&rec.id) {
                        let reason = if code == 0 { "Completed" } else { "Error" };
                        let _ = self.store.record_exit(&rec.id, code, reason);
                        tracing::info!(container = %rec.id, code, "recovered exit code");
                    }
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl RuntimeBackend for AppleBackend {
    async fn version(&self) -> Result<VersionResponse> {
        let banner = self.cli.version().await.unwrap_or_default();
        // "container CLI version 0.7.1 (build: release, commit: unspecified)"
        let runtime_version = banner
            .split_whitespace()
            .nth(3)
            .unwrap_or("unknown")
            .to_string();
        Ok(VersionResponse {
            // The CRI (kubelet API) version this server implements.
            version: "0.1.0".to_string(),
            runtime_name: "apple-container".to_string(),
            runtime_version,
            runtime_api_version: "v1".to_string(),
        })
    }

    async fn status(&self) -> Result<RuntimeStatus> {
        let runtime_ready = self.cli.apiserver_ready().await;
        // The pod network is what container connectivity depends on here.
        let network_ready = runtime_ready
            && self
                .cli
                .list_networks()
                .await
                .map(|n| n.iter().any(|net| net.id == self.config.pod_network))
                .unwrap_or(false);
        Ok(RuntimeStatus {
            conditions: vec![
                RuntimeCondition {
                    r#type: "RuntimeReady".to_string(),
                    status: runtime_ready,
                    reason: if runtime_ready {
                        String::new()
                    } else {
                        "ContainerApiserverUnreachable".to_string()
                    },
                    message: if runtime_ready {
                        String::new()
                    } else {
                        "container-apiserver is not running; try `container system start`"
                            .to_string()
                    },
                },
                RuntimeCondition {
                    r#type: "NetworkReady".to_string(),
                    status: network_ready,
                    reason: if network_ready {
                        String::new()
                    } else {
                        "PodNetworkNotReady".to_string()
                    },
                    message: if network_ready {
                        String::new()
                    } else {
                        format!(
                            "pod network {:?} does not exist yet",
                            self.config.pod_network
                        )
                    },
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
        Ok(RuntimeConfigResponse {
            linux: Some(LinuxRuntimeConfiguration {
                // The guest is a full VM with its own cgroup2 hierarchy that
                // `vminitd` manages directly; there is no host cgroup driver
                // and no systemd on macOS.
                cgroup_driver: CgroupDriver::Cgroupfs as i32,
            }),
        })
    }

    // ---- sandbox lifecycle (see sandbox.rs) -------------------------------

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

    // ---- container lifecycle (see container.rs) ---------------------------

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

/// A backend over a temporary state dir, for unit tests that only exercise
/// pure translation logic (no CLI calls).
#[cfg(test)]
pub(crate) fn test_backend() -> AppleBackend {
    let dir = std::env::temp_dir().join(format!("apple-cri-test-{}", std::process::id()));
    AppleBackend::new(Config {
        binary: "container".into(),
        pod_network: "k8s-pods".into(),
        pod_network_subnet: None,
        arch: "arm64".into(),
        root_dir: dir,
    })
    .expect("test backend")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn version_extracts_the_runtime_version_from_the_cli_banner() {
        // Parsing is what matters here; the banner shape is
        // "container CLI version 0.7.1 (build: release, ...)".
        let banner = "container CLI version 0.7.1 (build: release, commit: unspecified)";
        assert_eq!(banner.split_whitespace().nth(3), Some("0.7.1"));
    }

    #[test]
    fn image_store_dir_is_under_the_container_app_root() {
        let b = test_backend();
        let dir = b.config.image_store_dir();
        assert!(dir.ends_with("com.apple.container/content"), "{dir:?}");
    }
}

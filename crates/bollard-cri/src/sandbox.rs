// SPDX-License-Identifier: Apache-2.0

//! Pod sandbox lifecycle over the Docker Engine API (plan 03 B2).
//!
//! Port of cri-dockerd's sandbox decisions (`core/sandbox_*.go`):
//!
//! - The sandbox is a **pause container** (`--pod-infra-container-image`,
//!   pulled only if absent) created with `IpcMode=shareable`, minimal CPU
//!   shares, and an OOM score that protects it from everything but the
//!   daemon/kubelet.
//! - RunPodSandbox step order is create → **checkpoint** → start, so a
//!   sandbox that dies mid-flight is still discoverable for teardown.
//! - Networking rides on the Docker/Podman network (MVP per plan 03): the
//!   pod IP is the pause container's IP; host-network pods get
//!   `network_mode: host`. There is no CNI setup/teardown.
//! - Docker owns the sandbox's resolv.conf; it is rewritten from the CRI
//!   `DnsConfig` after start (`rewriteResolvFile`).
//! - Stop/remove are idempotent; a create-name conflict removes the stale
//!   container and retries (`recoverFromCreationConflictIfNeeded`) — with
//!   one deviation: after successfully removing the stale container we
//!   retry the create instead of erroring for the client to retry.

use std::collections::{HashMap, HashSet};

use bollard::container::{
    Config as DockerConfig, CreateContainerOptions, InspectContainerOptions, ListContainersOptions,
    RemoveContainerOptions, StopContainerOptions,
};
use bollard::models::{
    ContainerInspectResponse, ContainerSummary, HostConfig, PortBinding, PortMap,
};
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::{labels, ImageBackend};
use serde::{Deserialize, Serialize};

use crate::backend::{docker_err, BollardBackend};
use crate::naming;

/// Minimal CPU shares for the pause container (cri-dockerd).
const SANDBOX_CPU_SHARES: i64 = 2;
/// The pause container should be OOM-killed only before the daemon/kubelet,
/// never before app containers (cri-dockerd `defaultSandboxOOMAdj`).
const SANDBOX_OOM_SCORE_ADJ: i64 = -998;
/// Termination grace for the pause container (cri-dockerd).
const SANDBOX_STOP_GRACE_SECS: i64 = 10;
/// Docker namespace-mode value for host namespaces.
pub(crate) const MODE_HOST: &str = "host";

/// Per-sandbox state Docker cannot hold for us, persisted at RunPodSandbox
/// (before start) and read back for list/teardown after a shim restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SandboxCheckpoint {
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: String,
    pub attempt: u32,
    #[serde(default)]
    pub port_mappings: Vec<CheckpointPortMapping>,
    #[serde(default)]
    pub host_network: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckpointPortMapping {
    pub protocol: i32,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

impl SandboxCheckpoint {
    fn from_config(config: &PodSandboxConfig, host_network: bool) -> Self {
        let meta = config.metadata.clone().unwrap_or_default();
        Self {
            pod_name: meta.name,
            pod_namespace: meta.namespace,
            pod_uid: meta.uid,
            attempt: meta.attempt,
            port_mappings: config
                .port_mappings
                .iter()
                .map(|pm| CheckpointPortMapping {
                    protocol: pm.protocol,
                    container_port: pm.container_port,
                    host_port: pm.host_port,
                    host_ip: pm.host_ip.clone(),
                })
                .collect(),
            host_network,
        }
    }

    /// A checkpoint-only sandbox (container already gone from Docker) is
    /// reported NOTREADY with just its metadata, so the kubelet can still
    /// call RemovePodSandbox on it.
    fn to_sandbox(&self, id: &str) -> PodSandbox {
        PodSandbox {
            id: id.to_string(),
            metadata: Some(PodSandboxMetadata {
                name: self.pod_name.clone(),
                namespace: self.pod_namespace.clone(),
                uid: self.pod_uid.clone(),
                attempt: self.attempt,
            }),
            state: PodSandboxState::SandboxNotready as i32,
            ..Default::default()
        }
    }
}

fn sandbox_host_network(config: &PodSandboxConfig) -> bool {
    config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|sc| sc.namespace_options.as_ref())
        .is_some_and(|ns| ns.network == NamespaceMode::Node as i32)
}

/// CRI port mappings → Docker `ExposedPorts` + `PortBindings`.
fn ports_and_bindings(mappings: &[PortMapping]) -> (HashMap<String, HashMap<(), ()>>, PortMap) {
    let mut exposed = HashMap::new();
    let mut bindings: PortMap = HashMap::new();
    for pm in mappings {
        if pm.container_port <= 0 || pm.container_port > 65535 {
            tracing::warn!(port = pm.container_port, "ignoring invalid container port");
            continue;
        }
        let proto = match pm.protocol {
            p if p == Protocol::Udp as i32 => "udp",
            p if p == Protocol::Sctp as i32 => "sctp",
            _ => "tcp",
        };
        let key = format!("{}/{proto}", pm.container_port);
        exposed.insert(key.clone(), HashMap::new());
        if pm.host_port > 0 {
            bindings
                .entry(key)
                .or_insert_with(|| Some(Vec::new()))
                .get_or_insert_with(Vec::new)
                .push(PortBinding {
                    host_ip: (!pm.host_ip.is_empty()).then(|| pm.host_ip.clone()),
                    host_port: Some(pm.host_port.to_string()),
                });
        }
    }
    (exposed, bindings)
}

/// Extract the stale container id from a Docker/Podman name-conflict message
/// (`… is already in use by container "<id>" …`).
fn conflicting_container_id(message: &str) -> Option<&str> {
    let idx = message.find("already in use by")?;
    message[idx + "already in use by".len()..]
        .split(|c: char| !c.is_ascii_hexdigit())
        .find(|token| token.len() >= 12)
}

pub(crate) fn rfc3339_to_nanos(ts: Option<&str>) -> i64 {
    ts.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .and_then(|dt| dt.timestamp_nanos_opt())
        .unwrap_or_default()
}

fn mode_is_host(mode: Option<&String>) -> bool {
    mode.map(String::as_str) == Some(MODE_HOST)
}

/// Split runtime labels into CRI `(labels, annotations)` for a sandbox,
/// additionally dropping the shim-added infra-container name label
/// (cri-dockerd `extractLabels`).
fn sandbox_cri_labels(
    merged: &HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut merged = merged.clone();
    merged.remove(labels::CONTAINER_NAME_LABEL);
    labels::split_labels(&merged)
}

/// Pod IPs from the pause container's network settings: legacy top-level
/// (default bridge) first, then per-network endpoints, IPv4 before IPv6.
fn sandbox_ips(inspect: &ContainerInspectResponse) -> Vec<String> {
    let Some(net) = &inspect.network_settings else {
        return Vec::new();
    };
    let mut ips: Vec<String> = Vec::new();
    let mut push = |ip: Option<&String>| {
        if let Some(ip) = ip.filter(|s| !s.is_empty()) {
            if !ips.contains(ip) {
                ips.push(ip.clone());
            }
        }
    };
    push(net.ip_address.as_ref());
    if let Some(networks) = &net.networks {
        let mut names: Vec<&String> = networks.keys().collect();
        names.sort();
        for name in names {
            push(networks[name].ip_address.as_ref());
        }
        for name in networks.keys() {
            push(networks[name].global_ipv6_address.as_ref());
        }
    }
    ips
}

fn summary_to_sandbox(c: &ContainerSummary) -> Result<PodSandbox> {
    let name = c
        .names
        .as_ref()
        .and_then(|n| n.first())
        .cloned()
        .unwrap_or_default();
    let metadata = naming::parse_sandbox_name(&name)?;
    let state = if c.state.as_deref() == Some("running") {
        PodSandboxState::SandboxReady
    } else {
        PodSandboxState::SandboxNotready
    };
    let (cri_labels, annotations) =
        sandbox_cri_labels(c.labels.as_ref().unwrap_or(&HashMap::new()));
    Ok(PodSandbox {
        id: c.id.clone().unwrap_or_default(),
        metadata: Some(metadata),
        state: state as i32,
        // The list API timestamp is in seconds.
        created_at: c.created.unwrap_or_default().saturating_mul(1_000_000_000),
        labels: cri_labels,
        annotations,
        ..Default::default()
    })
}

impl BollardBackend {
    pub(crate) async fn run_sandbox(
        &self,
        config: PodSandboxConfig,
        runtime_handler: &str,
    ) -> Result<String> {
        let metadata = config
            .metadata
            .clone()
            .ok_or_else(|| Error::InvalidArgument("sandbox metadata is required".into()))?;

        self.ensure_sandbox_image().await?;

        let host_network = sandbox_host_network(&config);
        let linux = config.linux.clone().unwrap_or_default();
        let ns = linux
            .security_context
            .as_ref()
            .and_then(|sc| sc.namespace_options.clone())
            .unwrap_or_default();

        let mut merged = labels::flatten_labels(&config.labels, &config.annotations);
        merged.insert(
            labels::CONTAINER_TYPE_LABEL.to_string(),
            labels::CONTAINER_TYPE_SANDBOX.to_string(),
        );
        // Infra-container name for the summary API; stripped back out of CRI
        // labels in status/list conversions.
        merged.insert(
            labels::CONTAINER_NAME_LABEL.to_string(),
            naming::SANDBOX_INFRA_NAME.to_string(),
        );

        let mut host_config = HostConfig {
            // "shareable" so app containers can join the IPC namespace.
            ipc_mode: Some(if ns.ipc == NamespaceMode::Node as i32 {
                MODE_HOST.to_string()
            } else {
                "shareable".to_string()
            }),
            network_mode: Some(if host_network {
                MODE_HOST.to_string()
            } else {
                "default".to_string()
            }),
            pid_mode: (ns.pid == NamespaceMode::Node as i32).then(|| MODE_HOST.to_string()),
            cpu_shares: Some(SANDBOX_CPU_SHARES),
            oom_score_adj: Some(SANDBOX_OOM_SCORE_ADJ),
            privileged: linux.security_context.as_ref().map(|sc| sc.privileged),
            sysctls: (!linux.sysctls.is_empty()).then(|| linux.sysctls.clone()),
            cgroup_parent: self.expected_cgroup_parent(&linux.cgroup_parent).await?,
            ..Default::default()
        };
        if !runtime_handler.is_empty() && runtime_handler != "docker" {
            self.ensure_runtime_configured(runtime_handler).await?;
            host_config.runtime = Some(runtime_handler.to_string());
        }

        let mut create_config = DockerConfig::<String> {
            // Docker rejects a custom hostname together with host networking.
            hostname: (!config.hostname.is_empty() && !host_network)
                .then(|| config.hostname.clone()),
            image: Some(self.config.pod_infra_container_image.clone()),
            labels: Some(merged),
            ..Default::default()
        };
        if !host_network {
            let (exposed, bindings) = ports_and_bindings(&config.port_mappings);
            if !exposed.is_empty() {
                create_config.exposed_ports = Some(exposed);
                host_config.port_bindings = Some(bindings);
            }
        }
        create_config.host_config = Some(host_config);

        let name = naming::sandbox_name(&metadata);
        let id = self
            .create_with_conflict_recovery(&name, create_config, "sandbox")
            .await?;

        // Order matters: create → checkpoint → start, so a crash between
        // checkpoint and start still leaves the sandbox discoverable.
        self.checkpoints
            .save(&id, &SandboxCheckpoint::from_config(&config, host_network))?;

        self.docker
            .start_container::<String>(&id, None)
            .await
            .map_err(|e| docker_err("start sandbox", e))?;

        // Docker owns the sandbox's resolv.conf; overwrite it with the CRI
        // DNS config. The file is shared by every container in the pod.
        if let Some(dns) = &config.dns_config {
            self.rewrite_resolv_conf(&id, dns).await?;
        }

        Ok(id)
    }

    pub(crate) async fn stop_sandbox(&self, id: &str) -> Result<()> {
        match self
            .docker
            .stop_container(
                id,
                Some(StopContainerOptions {
                    t: SANDBOX_STOP_GRACE_SECS,
                }),
            )
            .await
        {
            // An already-stopped container answers 304, which bollard
            // treats as success.
            Ok(()) => Ok(()),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                // Gone from Docker entirely: drop the checkpoint so list
                // stops reporting it (cri-dockerd).
                self.checkpoints.delete(id)?;
                Ok(())
            }
            Err(e) => Err(docker_err("stop sandbox", e)),
        }
    }

    pub(crate) async fn remove_sandbox(&self, id: &str) -> Result<()> {
        // Force-remove any app containers still in the sandbox first.
        let mut filters = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!("{}={}", labels::SANDBOX_ID_LABEL, id)],
        );
        let members = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(|e| docker_err("list sandbox containers", e))?;
        for member in members {
            if let Some(cid) = member.id {
                self.log_relays.stop(&cid);
                self.remove_container_force(&cid).await?;
            }
        }

        self.remove_container_force(id).await?;
        self.checkpoints.delete(id)?;
        Ok(())
    }

    pub(crate) async fn sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| docker_err("inspect sandbox", e))?;

        let metadata = naming::parse_sandbox_name(inspect.name.as_deref().unwrap_or_default())?;
        let running = inspect
            .state
            .as_ref()
            .and_then(|s| s.running)
            .unwrap_or(false);
        let state = if running {
            PodSandboxState::SandboxReady
        } else {
            PodSandboxState::SandboxNotready
        };

        let host_config = inspect.host_config.clone().unwrap_or_default();
        let host_network = mode_is_host(host_config.network_mode.as_ref());
        // Host-network pods use the node IP; the shim does not report one.
        let mut ips = if host_network {
            Vec::new()
        } else {
            sandbox_ips(&inspect)
        };
        let ip = if ips.is_empty() {
            String::new()
        } else {
            ips.remove(0)
        };

        let merged = inspect
            .config
            .as_ref()
            .and_then(|c| c.labels.clone())
            .unwrap_or_default();
        let (cri_labels, annotations) = sandbox_cri_labels(&merged);

        Ok(PodSandboxStatus {
            id: inspect.id.clone().unwrap_or_else(|| id.to_string()),
            metadata: Some(metadata),
            state: state as i32,
            created_at: rfc3339_to_nanos(inspect.created.as_deref()),
            network: Some(PodSandboxNetworkStatus {
                ip,
                additional_ips: ips.into_iter().map(|ip| PodIp { ip }).collect(),
            }),
            linux: Some(LinuxPodSandboxStatus {
                namespaces: Some(Namespace {
                    options: Some(NamespaceOption {
                        network: if host_network {
                            NamespaceMode::Node
                        } else {
                            NamespaceMode::Pod
                        } as i32,
                        pid: if mode_is_host(host_config.pid_mode.as_ref()) {
                            NamespaceMode::Node
                        } else {
                            NamespaceMode::Container
                        } as i32,
                        ipc: if mode_is_host(host_config.ipc_mode.as_ref()) {
                            NamespaceMode::Node
                        } else {
                            NamespaceMode::Pod
                        } as i32,
                        ..Default::default()
                    }),
                }),
            }),
            labels: cri_labels,
            annotations,
            runtime_handler: host_config.runtime.unwrap_or_default(),
        })
    }

    pub(crate) async fn list_sandboxes(
        &self,
        filter: Option<PodSandboxFilter>,
    ) -> Result<Vec<PodSandbox>> {
        let mut all = true;
        let mut filter_out_ready = false;
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!(
                "{}={}",
                labels::CONTAINER_TYPE_LABEL,
                labels::CONTAINER_TYPE_SANDBOX
            )],
        );

        let no_filter = filter
            .as_ref()
            .is_none_or(|f| f.id.is_empty() && f.state.is_none() && f.label_selector.is_empty());
        if let Some(f) = &filter {
            if !f.id.is_empty() {
                filters.insert("id".to_string(), vec![f.id.clone()]);
            }
            if let Some(state) = &f.state {
                if state.state == PodSandboxState::SandboxReady as i32 {
                    all = false;
                } else {
                    // Docker cannot filter for "not running" directly.
                    filter_out_ready = true;
                }
            }
            let label_filters = filters.get_mut("label").expect("inserted above");
            for (k, v) in &f.label_selector {
                label_filters.push(format!("{k}={v}"));
            }
        }

        // Snapshot checkpoints before listing containers so a sandbox being
        // created right now (create → checkpoint → start) is reported from
        // the authoritative container list, not as checkpoint-only.
        let checkpoint_ids = if no_filter {
            self.checkpoints.list()?
        } else {
            Vec::new()
        };

        let summaries = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(|e| docker_err("list sandboxes", e))?;

        let mut result = Vec::new();
        let mut seen = HashSet::new();
        for summary in &summaries {
            let sandbox = match summary_to_sandbox(summary) {
                Ok(sandbox) => sandbox,
                Err(err) => {
                    tracing::debug!(names = ?summary.names, %err, "skipping non-CRI container");
                    continue;
                }
            };
            if filter_out_ready && sandbox.state == PodSandboxState::SandboxReady as i32 {
                continue;
            }
            seen.insert(sandbox.id.clone());
            result.push(sandbox);
        }

        // Sandboxes whose container is gone but whose checkpoint survives
        // are still listed (NOTREADY) so the kubelet can remove them.
        for id in checkpoint_ids {
            if seen.contains(&id) {
                continue;
            }
            if let Some(checkpoint) = self.checkpoints.load::<SandboxCheckpoint>(&id)? {
                result.push(checkpoint.to_sandbox(&id));
            }
        }

        Ok(result)
    }

    // ---- helpers ----------------------------------------------------------

    /// Pull the pod-infra image only when it is not present (PullIfNotPresent).
    async fn ensure_sandbox_image(&self) -> Result<()> {
        let image = &self.config.pod_infra_container_image;
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(()),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                tracing::info!(image = %image, "pulling pod infra image");
                ImageBackend::pull_image(
                    self,
                    &ImageSpec {
                        image: image.clone(),
                        ..Default::default()
                    },
                    None,
                    None,
                )
                .await
                .map(|_| ())
            }
            Err(e) => Err(docker_err("inspect pod infra image", e)),
        }
    }

    /// Cgroup parent in the syntax the daemon's cgroup driver expects (port
    /// of cri-dockerd `GenerateExpectedCgroupParent`): the systemd driver
    /// wants a bare `*.slice` name, not a cgroupfs path like `/test.slice`.
    pub(crate) async fn expected_cgroup_parent(
        &self,
        cgroup_parent: &str,
    ) -> Result<Option<String>> {
        if cgroup_parent.is_empty() {
            return Ok(None);
        }
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
            // Pass only the last component of the cgroup path to systemd.
            Ok(Some(
                cgroup_parent
                    .rsplit('/')
                    .next()
                    .unwrap_or(cgroup_parent)
                    .to_string(),
            ))
        } else {
            Ok(Some(cgroup_parent.to_string()))
        }
    }

    /// `RuntimeClass.handler` must name a runtime the daemon actually has.
    async fn ensure_runtime_configured(&self, runtime: &str) -> Result<()> {
        let info = self
            .docker
            .info()
            .await
            .map_err(|e| docker_err("info", e))?;
        if info.runtimes.is_some_and(|r| r.contains_key(runtime)) {
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "no runtime {runtime:?} is configured in the Docker daemon"
            )))
        }
    }

    /// Create a container; on a name conflict remove the stale holder and
    /// retry, randomizing the name if Docker's name index is out of sync
    /// (port of cri-dockerd `recoverFromCreationConflictIfNeeded`).
    /// `what` labels errors ("sandbox" / "container").
    pub(crate) async fn create_with_conflict_recovery(
        &self,
        name: &str,
        config: DockerConfig<String>,
        what: &str,
    ) -> Result<String> {
        let create = |name: String, config: DockerConfig<String>| {
            let docker = self.docker.clone();
            async move {
                docker
                    .create_container(
                        Some(CreateContainerOptions {
                            name,
                            platform: None,
                        }),
                        config,
                    )
                    .await
            }
        };

        let message = match create(name.to_string(), config.clone()).await {
            Ok(resp) => return Ok(resp.id),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                message,
            }) => message,
            Err(e) => return Err(docker_err(&format!("create {what}"), e)),
        };

        let Some(stale) = conflicting_container_id(&message) else {
            return Err(Error::Internal(format!(
                "create {what}: docker 409: {message}"
            )));
        };
        tracing::warn!(container = %stale, name, "create conflict; removing stale container");
        let retry_name = match self
            .docker
            .remove_container(
                stale,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(()) => name.to_string(),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                // The container is gone but the name is still reserved
                // (Docker name-index bug): fall back to a randomized name.
                let randomized = naming::randomize_name(name);
                tracing::warn!(name = %randomized, "conflicting container already gone; randomizing name");
                randomized
            }
            Err(e) => return Err(docker_err("remove conflicting container", e)),
        };

        create(retry_name, config)
            .await
            .map(|resp| resp.id)
            .map_err(|e| docker_err(&format!("create {what} (retry)"), e))
    }

    pub(crate) async fn remove_container_force(&self, id: &str) -> Result<()> {
        match self
            .docker
            .remove_container(
                id,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            Err(e) => Err(docker_err("remove container", e)),
        }
    }

    /// Overwrite the resolv.conf Docker generated for the sandbox with the
    /// CRI DNS config (port of cri-dockerd `rewriteResolvFile`). Requires
    /// running on the daemon host, where `ResolvConfPath` is visible.
    async fn rewrite_resolv_conf(&self, id: &str, dns: &DnsConfig) -> Result<()> {
        let mut content = String::new();
        for server in &dns.servers {
            content.push_str(&format!("nameserver {server}\n"));
        }
        if !dns.searches.is_empty() {
            content.push_str(&format!("search {}\n", dns.searches.join(" ")));
        }
        if !dns.options.is_empty() {
            content.push_str(&format!("options {}\n", dns.options.join(" ")));
        }
        if content.is_empty() {
            return Ok(());
        }

        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| docker_err("inspect sandbox for resolv.conf", e))?;
        let path = inspect.resolv_conf_path.unwrap_or_default();
        if path.is_empty() {
            tracing::error!(
                sandbox = id,
                "sandbox has no resolv.conf path; skipping DNS rewrite"
            );
            return Ok(());
        }
        if !std::path::Path::new(&path).exists() {
            return Err(Error::Internal(format!(
                "resolv.conf {path:?} does not exist (bollard-cri must run on the Docker host)"
            )));
        }
        tokio::fs::write(&path, content).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_id_from_docker_message() {
        let msg = r#"Conflict. The container name "/k8s_POD_web_default_uid-1_0" is already in use by container "3d8b2f0c9a1e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f809a0b1c2". You have to remove (or rename) that container to be able to reuse that name."#;
        assert_eq!(
            conflicting_container_id(msg),
            Some("3d8b2f0c9a1e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f809a0b1c2")
        );
    }

    #[test]
    fn conflict_id_from_podman_message() {
        let msg = r#"creating container storage: the container name "k8s_POD_web_default_uid-1_0" is already in use by 51b6fdc6c8dd8be0eb0ba3b526e04b13e96622628cb04c07d53341b1a4620a1b. You have to remove that container to be able to reuse that name: that name is already in use"#;
        assert_eq!(
            conflicting_container_id(msg),
            Some("51b6fdc6c8dd8be0eb0ba3b526e04b13e96622628cb04c07d53341b1a4620a1b")
        );
    }

    #[test]
    fn conflict_id_absent() {
        assert_eq!(conflicting_container_id("some other 409"), None);
    }

    #[test]
    fn ports_and_bindings_mapping() {
        let (exposed, bindings) = ports_and_bindings(&[
            PortMapping {
                protocol: Protocol::Tcp as i32,
                container_port: 80,
                host_port: 8080,
                host_ip: "127.0.0.1".into(),
            },
            PortMapping {
                protocol: Protocol::Udp as i32,
                container_port: 53,
                host_port: 0,
                host_ip: String::new(),
            },
            PortMapping {
                protocol: Protocol::Tcp as i32,
                container_port: 0, // invalid, skipped
                host_port: 1,
                host_ip: String::new(),
            },
        ]);
        assert_eq!(exposed.len(), 2);
        assert!(exposed.contains_key("80/tcp"));
        assert!(exposed.contains_key("53/udp"));
        // Only the mapping with a host port gets a binding.
        assert_eq!(bindings.len(), 1);
        let binding = bindings["80/tcp"].as_ref().unwrap();
        assert_eq!(binding[0].host_port.as_deref(), Some("8080"));
        assert_eq!(binding[0].host_ip.as_deref(), Some("127.0.0.1"));
    }

    #[test]
    fn sandbox_labels_strip_shim_keys() {
        let merged = HashMap::from([
            ("app".to_string(), "web".to_string()),
            ("annotation.note".to_string(), "v".to_string()),
            (
                labels::CONTAINER_TYPE_LABEL.to_string(),
                labels::CONTAINER_TYPE_SANDBOX.to_string(),
            ),
            (
                labels::CONTAINER_NAME_LABEL.to_string(),
                naming::SANDBOX_INFRA_NAME.to_string(),
            ),
        ]);
        let (cri_labels, annotations) = sandbox_cri_labels(&merged);
        assert_eq!(
            cri_labels,
            HashMap::from([("app".to_string(), "web".to_string())])
        );
        assert_eq!(
            annotations,
            HashMap::from([("note".to_string(), "v".to_string())])
        );
    }

    #[test]
    fn host_network_detection() {
        let mut config = PodSandboxConfig::default();
        assert!(!sandbox_host_network(&config));
        config.linux = Some(LinuxPodSandboxConfig {
            security_context: Some(LinuxSandboxSecurityContext {
                namespace_options: Some(NamespaceOption {
                    network: NamespaceMode::Node as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(sandbox_host_network(&config));
    }

    #[test]
    fn rfc3339_parsing() {
        assert_eq!(
            rfc3339_to_nanos(Some("1970-01-01T00:00:01.000000005Z")),
            1_000_000_005
        );
        assert_eq!(rfc3339_to_nanos(Some("garbage")), 0);
        assert_eq!(rfc3339_to_nanos(None), 0);
    }
}

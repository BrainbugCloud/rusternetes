// SPDX-License-Identifier: Apache-2.0

//! App container lifecycle over the Docker Engine API (plan 03 B3).
//!
//! Port of cri-dockerd's container decisions (`core/container_*.go`,
//! `security_context.go`, `convert.go`):
//!
//! - App containers join their sandbox's network/IPC namespaces via
//!   `container:<sandbox-id>` modes; PID follows the CRI namespace option.
//! - Docker healthchecks are disabled (`NONE`) and the restart policy is
//!   forced to `no` — the kubelet owns restarts.
//! - The kubelet-desired log path is stored in a label and served by the log
//!   relay (see [`crate::logs`]) instead of cri-dockerd's symlink.
//! - `ExecSync` buffers output with a 16 MiB cap and polls `inspect_exec`
//!   for the exit code with bounded retries. One deviation: on timeout the
//!   exec process is SIGKILLed by host PID (cri-dockerd leaves it running,
//!   and skips the critest timeout spec because of it).
//! - Stats map cri-dockerd's fields: CPU `total_usage`, memory `usage`,
//!   stamped at read time. The rootfs/writable-layer size cache is B5.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use bollard::container::{
    Config as DockerConfig, InspectContainerOptions, ListContainersOptions, StatsOptions,
    StopContainerOptions, UpdateContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{
    ContainerSummary, DeviceMapping, DeviceRequest, HostConfig, Mount as DockerMount,
    MountBindOptions, MountBindOptionsPropagationEnum, MountTypeEnum, RestartPolicy,
    RestartPolicyNameEnum,
};
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::labels;
use cri_server::ExecSyncResult;
use futures_util::StreamExt;

use crate::backend::{docker_err, BollardBackend};
use crate::naming;
use crate::sandbox::rfc3339_to_nanos;
use crate::{logs, sandbox::MODE_HOST};

/// Exec output cap shared by stdout+stderr (cri-dockerd `maxMsgSize`).
const MAX_EXEC_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
/// `inspect_exec` may lag the stream close; poll a few times (cri-dockerd).
const EXEC_INSPECT_RETRIES: u32 = 5;
const EXEC_INSPECT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
/// Bounded fan-out for list-stats (cri-dockerd uses a worker pool).
const STATS_CONCURRENCY: usize = 8;

fn container_mode(sandbox_id: &str) -> String {
    format!("container:{sandbox_id}")
}

/// CRI env `KeyValue`s → Docker `K=V` strings (values are bytes on the wire
/// since CRI v1.36; Docker env wants UTF-8).
fn env_list(envs: &[KeyValue]) -> Vec<String> {
    envs.iter()
        .map(|kv| format!("{}={}", kv.key, String::from_utf8_lossy(&kv.value)))
        .collect()
}

/// CRI mounts → Docker mount API objects (port of cri-dockerd
/// `GenerateMountBindings`; SELinux relabeling is not handled — MVP).
fn mount_bindings(mounts: &[Mount]) -> Result<Vec<DockerMount>> {
    let mut result = Vec::with_capacity(mounts.len());
    for m in mounts {
        let mut bind_options = MountBindOptions {
            create_mountpoint: Some(true),
            ..Default::default()
        };
        if m.recursive_read_only {
            if m.propagation != MountPropagation::PropagationPrivate as i32 {
                return Err(Error::InvalidArgument(format!(
                    "recursive read-only mount needs private propagation (hostPath={:?})",
                    m.host_path
                )));
            }
            if !m.readonly {
                return Err(Error::InvalidArgument(format!(
                    "recursive read-only mount conflicts with RW mount (hostPath={:?})",
                    m.host_path
                )));
            }
        }
        if m.readonly {
            // Docker v25+ made read-only mounts recursive by default, which
            // broke Kubernetes expectations (cri-dockerd #309).
            bind_options.read_only_non_recursive = Some(!m.recursive_read_only);
        }
        match m.propagation {
            p if p == MountPropagation::PropagationBidirectional as i32 => {
                bind_options.propagation = Some(MountBindOptionsPropagationEnum::RSHARED);
            }
            p if p == MountPropagation::PropagationHostToContainer as i32 => {
                bind_options.propagation = Some(MountBindOptionsPropagationEnum::RSLAVE);
            }
            // PRIVATE: let dockerd decide (it defaults to rprivate, or rslave
            // when the source contains the daemon root).
            _ => {}
        }
        result.push(DockerMount {
            typ: Some(MountTypeEnum::BIND),
            source: Some(m.host_path.clone()),
            target: Some(m.container_path.clone()),
            read_only: m.readonly.then_some(true),
            bind_options: Some(bind_options),
            ..Default::default()
        });
    }
    Ok(result)
}

/// CRI devices → Docker device mappings.
fn device_mappings(devices: &[Device]) -> Vec<DeviceMapping> {
    devices
        .iter()
        .map(|d| DeviceMapping {
            path_on_host: Some(d.host_path.clone()),
            path_in_container: Some(d.container_path.clone()),
            cgroup_permissions: Some(d.permissions.clone()),
        })
        .collect()
}

/// Container user string from the security context (port of cri-dockerd
/// `modifyContainerConfig`).
fn container_user(sc: &LinuxContainerSecurityContext) -> Result<Option<String>> {
    let mut user = sc
        .run_as_user
        .as_ref()
        .map(|u| u.value.to_string())
        .unwrap_or_default();
    if !sc.run_as_username.is_empty() {
        user = sc.run_as_username.clone();
    }
    if let Some(group) = &sc.run_as_group {
        if user.is_empty() {
            return Err(Error::InvalidArgument(
                "runAsGroup is specified without a runAsUser".into(),
            ));
        }
        user = format!("{user}:{}", group.value);
    }
    Ok((!user.is_empty()).then_some(user))
}

/// Seccomp security option (port of cri-dockerd `getSeccompDockerOpts`):
/// unset/Unconfined → explicitly unconfined (the historical Kubernetes
/// default), RuntimeDefault → none (Docker's own default profile),
/// Localhost → the profile file's JSON.
fn seccomp_security_opt(
    seccomp: Option<&SecurityProfile>,
    privileged: bool,
) -> Result<Option<String>> {
    let Some(seccomp) = seccomp else {
        return Ok(Some("seccomp=unconfined".to_string()));
    };
    match seccomp.profile_type {
        t if t == security_profile::ProfileType::RuntimeDefault as i32 => Ok(None),
        t if t == security_profile::ProfileType::Localhost as i32 => {
            let path = Path::new(&seccomp.localhost_ref);
            if !path.is_absolute() {
                return Err(Error::InvalidArgument(format!(
                    "seccomp profile path must be absolute, got {:?}",
                    seccomp.localhost_ref
                )));
            }
            let raw = std::fs::read(path).map_err(|e| {
                Error::InvalidArgument(format!(
                    "cannot load seccomp profile {:?}: {e}",
                    seccomp.localhost_ref
                ))
            })?;
            let profile: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| {
                Error::InvalidArgument(format!(
                    "decoding seccomp profile {:?} failed: {e}",
                    seccomp.localhost_ref
                ))
            })?;
            let profile = filter_seccomp_profile(profile, privileged);
            Ok(Some(format!("seccomp={profile}")))
        }
        _ => Ok(Some("seccomp=unconfined".to_string())),
    }
}

/// Privileged containers must still be able to set the hostname (critest's
/// sysctls specs); drop seccomp rules that block `sethostname` for them, and
/// fall back to Docker's default profile when nothing is left.
fn filter_seccomp_profile(mut profile: serde_json::Value, privileged: bool) -> serde_json::Value {
    if !privileged {
        return profile;
    }
    if let Some(syscalls) = profile.get_mut("syscalls").and_then(|s| s.as_array_mut()) {
        for rule in syscalls.iter_mut() {
            if rule.get("action").and_then(|a| a.as_str()) != Some("SCMP_ACT_ERRNO") {
                continue;
            }
            if let Some(names) = rule.get_mut("names").and_then(|n| n.as_array_mut()) {
                names.retain(|n| n.as_str() != Some("sethostname"));
            }
        }
        syscalls.retain(|rule| {
            rule.get("names")
                .and_then(|n| n.as_array())
                .is_none_or(|names| !names.is_empty())
        });
    }
    profile
}

/// AppArmor security option: prefers the `SecurityProfile` field, falls back
/// to the deprecated profile string (port of cri-dockerd `getAppArmorOpts`).
fn apparmor_security_opt(sc: &LinuxContainerSecurityContext) -> Option<String> {
    if let Some(profile) = &sc.apparmor {
        return match profile.profile_type {
            t if t == security_profile::ProfileType::Unconfined as i32 => {
                Some("apparmor=unconfined".to_string())
            }
            t if t == security_profile::ProfileType::Localhost as i32 => {
                let name = profile
                    .localhost_ref
                    .strip_prefix("localhost/")
                    .unwrap_or(&profile.localhost_ref);
                Some(format!("apparmor={name}"))
            }
            _ => None, // RuntimeDefault: Docker applies its default profile.
        };
    }
    #[allow(deprecated)]
    let profile = sc.apparmor_profile.as_str();
    match profile {
        "" | "runtime/default" => None,
        "unconfined" => Some("apparmor=unconfined".to_string()),
        other => Some(format!(
            "apparmor={}",
            other.strip_prefix("localhost/").unwrap_or(other)
        )),
    }
}

/// Apply the container security context to the Docker host config (port of
/// cri-dockerd `modifyHostConfig` + `modifyContainerNamespaceOptions`).
fn apply_security_context(
    sc: &LinuxContainerSecurityContext,
    sandbox_id: &str,
    host_config: &mut HostConfig,
) -> Result<()> {
    if !sc.supplemental_groups.is_empty() {
        host_config.group_add = Some(
            sc.supplemental_groups
                .iter()
                .map(|g| g.to_string())
                .collect(),
        );
    }
    host_config.privileged = Some(sc.privileged);
    host_config.readonly_rootfs = Some(sc.readonly_rootfs);
    if let Some(caps) = &sc.capabilities {
        host_config.cap_add = Some(caps.add_capabilities.clone());
        host_config.cap_drop = Some(caps.drop_capabilities.clone());
    }

    let mut security_opt = Vec::new();
    if let Some(opt) = seccomp_security_opt(sc.seccomp.as_ref(), sc.privileged)? {
        security_opt.push(opt);
    }
    if let Some(opt) = apparmor_security_opt(sc) {
        security_opt.push(opt);
    }
    if sc.no_new_privs {
        security_opt.push("no-new-privileges".to_string());
    }
    if !security_opt.is_empty() {
        host_config.security_opt = Some(security_opt);
    }

    if !sc.privileged {
        if !sc.masked_paths.is_empty() {
            host_config.masked_paths = Some(sc.masked_paths.clone());
        }
        if !sc.readonly_paths.is_empty() {
            host_config.readonly_paths = Some(sc.readonly_paths.clone());
        }
    }

    // Namespaces: network and IPC always join the sandbox; UTS goes host
    // for host-network pods; PID follows the CRI option.
    let ns = sc.namespace_options.clone().unwrap_or_default();
    host_config.network_mode = Some(container_mode(sandbox_id));
    host_config.ipc_mode = Some(container_mode(sandbox_id));
    if ns.network == NamespaceMode::Node as i32 {
        host_config.uts_mode = Some(MODE_HOST.to_string());
    }
    host_config.pid_mode = match ns.pid {
        p if p == NamespaceMode::Node as i32 => Some(MODE_HOST.to_string()),
        p if p == NamespaceMode::Pod as i32 => Some(container_mode(sandbox_id)),
        p if p == NamespaceMode::Target as i32 => Some(container_mode(&ns.target_id)),
        _ => None, // CONTAINER: its own PID namespace.
    };
    Ok(())
}

/// Docker inspect mount propagation string → CRI enum (the inverse of
/// [`mount_bindings`]; critest asserts status echoes the requested mode).
fn propagation_from_docker(propagation: Option<&str>) -> MountPropagation {
    match propagation {
        Some("rshared") | Some("shared") => MountPropagation::PropagationBidirectional,
        Some("rslave") | Some("slave") => MountPropagation::PropagationHostToContainer,
        _ => MountPropagation::PropagationPrivate,
    }
}

fn summary_state(state: Option<&str>) -> ContainerState {
    match state {
        Some("created") => ContainerState::ContainerCreated,
        Some("running") | Some("paused") => ContainerState::ContainerRunning,
        Some("exited") => ContainerState::ContainerExited,
        _ => ContainerState::ContainerUnknown,
    }
}

fn cri_state_to_docker(state: i32) -> &'static str {
    match state {
        s if s == ContainerState::ContainerCreated as i32 => "created",
        s if s == ContainerState::ContainerRunning as i32 => "running",
        s if s == ContainerState::ContainerExited as i32 => "exited",
        _ => "unknown",
    }
}

fn summary_to_container(c: &ContainerSummary) -> Result<Container> {
    let name = c
        .names
        .as_ref()
        .and_then(|n| n.first())
        .cloned()
        .unwrap_or_default();
    let metadata = naming::parse_container_name(&name)?;
    let merged = c.labels.clone().unwrap_or_default();
    let pod_sandbox_id = merged
        .get(labels::SANDBOX_ID_LABEL)
        .cloned()
        .unwrap_or_default();
    let (cri_labels, annotations) = labels::split_labels(&merged);
    Ok(Container {
        id: c.id.clone().unwrap_or_default(),
        pod_sandbox_id,
        metadata: Some(metadata),
        image: Some(ImageSpec {
            image: c.image.clone().unwrap_or_default(),
            ..Default::default()
        }),
        image_ref: c.image_id.clone().unwrap_or_default(),
        image_id: c.image_id.clone().unwrap_or_default(),
        state: summary_state(c.state.as_deref()) as i32,
        // The list API timestamp is in seconds.
        created_at: c.created.unwrap_or_default().saturating_mul(1_000_000_000),
        labels: cri_labels,
        annotations,
    })
}

impl BollardBackend {
    pub(crate) async fn create_app_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let metadata = config
            .metadata
            .clone()
            .ok_or_else(|| Error::InvalidArgument("container metadata is required".into()))?;
        let sandbox_metadata = sandbox_config
            .metadata
            .clone()
            .ok_or_else(|| Error::InvalidArgument("sandbox metadata is required".into()))?;
        let image = config
            .image
            .as_ref()
            .map(|i| i.image.clone())
            .filter(|i| !i.is_empty())
            .ok_or_else(|| Error::InvalidArgument("container image is required".into()))?;

        // The sandbox's runtime carries over to its app containers.
        let sandbox_inspect = self
            .docker
            .inspect_container(sandbox_id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| docker_err("inspect sandbox for container create", e))?;
        let runtime = sandbox_inspect
            .host_config
            .as_ref()
            .and_then(|hc| hc.runtime.clone());

        let mut merged = labels::flatten_labels(&config.labels, &config.annotations);
        merged.insert(
            labels::CONTAINER_TYPE_LABEL.to_string(),
            labels::CONTAINER_TYPE_CONTAINER.to_string(),
        );
        merged.insert(labels::SANDBOX_ID_LABEL.to_string(), sandbox_id.to_string());
        if !config.log_path.is_empty() {
            let log_path = Path::new(&sandbox_config.log_directory).join(&config.log_path);
            merged.insert(
                labels::CONTAINER_LOG_PATH_LABEL.to_string(),
                log_path.to_string_lossy().into_owned(),
            );
        }

        let mut host_config = HostConfig {
            mounts: Some(mount_bindings(&config.mounts)?),
            // The kubelet owns restarts; Docker must never restart for us.
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::NO),
                ..Default::default()
            }),
            runtime,
            cgroup_parent: self
                .expected_cgroup_parent(
                    &sandbox_config
                        .linux
                        .as_ref()
                        .map(|l| l.cgroup_parent.clone())
                        .unwrap_or_default(),
                )
                .await?,
            ..Default::default()
        };
        if !config.devices.is_empty() {
            host_config.devices = Some(device_mappings(&config.devices));
        }
        if !config.cdi_devices.is_empty() {
            host_config.device_requests = Some(
                config
                    .cdi_devices
                    .iter()
                    .map(|d| DeviceRequest {
                        driver: Some("cdi".to_string()),
                        device_ids: Some(vec![d.name.clone()]),
                        ..Default::default()
                    })
                    .collect(),
            );
        }

        let mut user = None;
        if let Some(linux) = &config.linux {
            if let Some(resources) = &linux.resources {
                // Memory and swap get the same value: no swap for containers.
                host_config.memory = Some(resources.memory_limit_in_bytes);
                host_config.memory_swap = Some(resources.memory_limit_in_bytes);
                host_config.cpu_shares = Some(resources.cpu_shares);
                host_config.cpu_quota = Some(resources.cpu_quota);
                host_config.cpu_period = Some(resources.cpu_period);
                host_config.cpuset_cpus = Some(resources.cpuset_cpus.clone());
                host_config.cpuset_mems = Some(resources.cpuset_mems.clone());
                host_config.oom_score_adj = Some(resources.oom_score_adj);
            }
            if let Some(sc) = &linux.security_context {
                user = container_user(sc)?;
                apply_security_context(sc, sandbox_id, &mut host_config)?;
            }
        }
        if host_config.network_mode.is_none() {
            // No Linux security context at all: still join the sandbox.
            host_config.network_mode = Some(container_mode(sandbox_id));
            host_config.ipc_mode = Some(container_mode(sandbox_id));
        }

        let create_config = DockerConfig::<String> {
            entrypoint: (!config.command.is_empty()).then(|| config.command.clone()),
            cmd: (!config.args.is_empty()).then(|| config.args.clone()),
            env: (!config.envs.is_empty()).then(|| env_list(&config.envs)),
            image: Some(image),
            working_dir: (!config.working_dir.is_empty()).then(|| config.working_dir.clone()),
            labels: Some(merged),
            user,
            open_stdin: Some(config.stdin),
            stdin_once: Some(config.stdin_once),
            tty: Some(config.tty),
            // Never surface Docker healthchecks to CRI (dockershim lesson).
            healthcheck: Some(bollard::models::HealthConfig {
                test: Some(vec!["NONE".to_string()]),
                ..Default::default()
            }),
            host_config: Some(host_config),
            ..Default::default()
        };

        let name = naming::container_name(&metadata, &sandbox_metadata);
        self.create_with_conflict_recovery(&name, create_config, "container")
            .await
    }

    pub(crate) async fn start_app_container(&self, id: &str) -> Result<()> {
        let start_result = self
            .docker
            .start_container::<String>(id, None)
            .await
            .map_err(|e| docker_err("start container", e));

        // The log relay starts even when the start failed, so the CRI log
        // file exists for every started-or-attempted container (cri-dockerd
        // creates its symlink the same way).
        if let Ok(inspect) = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await
        {
            let full_id = inspect.id.unwrap_or_else(|| id.to_string());
            if let Some(path) = inspect
                .config
                .and_then(|c| c.labels)
                .as_ref()
                .and_then(|l| l.get(labels::CONTAINER_LOG_PATH_LABEL))
                .filter(|p| !p.is_empty())
            {
                self.log_relays
                    .start(self.docker.clone(), full_id, PathBuf::from(path), None);
            }
        }

        start_result
    }

    pub(crate) async fn stop_app_container(&self, id: &str, timeout_secs: i64) -> Result<()> {
        match self
            .docker
            .stop_container(id, Some(StopContainerOptions { t: timeout_secs }))
            .await
        {
            // 304 (already stopped) is success in bollard.
            Ok(()) => Ok(()),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            Err(e) => Err(docker_err("stop container", e)),
        }
    }

    pub(crate) async fn remove_app_container(&self, id: &str) -> Result<()> {
        self.log_relays.stop(id);
        self.remove_container_force(id).await
    }

    pub(crate) async fn list_app_containers(
        &self,
        filter: Option<ContainerFilter>,
    ) -> Result<Vec<Container>> {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!(
                "{}={}",
                labels::CONTAINER_TYPE_LABEL,
                labels::CONTAINER_TYPE_CONTAINER
            )],
        );
        if let Some(f) = &filter {
            if !f.id.is_empty() {
                filters.insert("id".to_string(), vec![f.id.clone()]);
            }
            if let Some(state) = &f.state {
                filters.insert(
                    "status".to_string(),
                    vec![cri_state_to_docker(state.state).to_string()],
                );
            }
            let label_filters = filters.get_mut("label").expect("inserted above");
            if !f.pod_sandbox_id.is_empty() {
                label_filters.push(format!("{}={}", labels::SANDBOX_ID_LABEL, f.pod_sandbox_id));
            }
            for (k, v) in &f.label_selector {
                label_filters.push(format!("{k}={v}"));
            }
        }

        let summaries = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(|e| docker_err("list containers", e))?;

        Ok(summaries
            .iter()
            .filter_map(|summary| match summary_to_container(summary) {
                Ok(container) => Some(container),
                Err(err) => {
                    tracing::debug!(names = ?summary.names, %err, "skipping non-CRI container");
                    None
                }
            })
            .collect())
    }

    pub(crate) async fn app_container_status(&self, id: &str) -> Result<ContainerStatus> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| docker_err("inspect container", e))?;

        let metadata = naming::parse_container_name(inspect.name.as_deref().unwrap_or_default())?;
        let state_info = inspect.state.clone().unwrap_or_default();
        let created_at = rfc3339_to_nanos(inspect.created.as_deref());
        let mut started_at = rfc3339_to_nanos(state_info.started_at.as_deref());
        let mut finished_at = rfc3339_to_nanos(state_info.finished_at.as_deref());
        let exit_code = state_info.exit_code.unwrap_or_default() as i32;

        // Interpret the Docker state (port of cri-dockerd ContainerStatus):
        // running; ran and exited; failed to start; or created-not-started.
        let (state, reason) = if state_info.running.unwrap_or(false) {
            (ContainerState::ContainerRunning, "")
        } else if finished_at != 0 {
            let reason = if state_info.oom_killed.unwrap_or(false) {
                "OOMKilled"
            } else if exit_code == 0 {
                "Completed"
            } else {
                "Error"
            };
            (ContainerState::ContainerExited, reason)
        } else if exit_code != 0 {
            // Failed to start: zero finishedAt but a non-zero exit code.
            started_at = created_at;
            finished_at = created_at;
            (ContainerState::ContainerExited, "ContainerCannotRun")
        } else {
            (ContainerState::ContainerCreated, "")
        };
        let message = state_info.error.clone().unwrap_or_default();

        let config = inspect.config.clone().unwrap_or_default();
        let merged = config.labels.clone().unwrap_or_default();
        let (cri_labels, annotations) = labels::split_labels(&merged);
        let log_path = merged
            .get(labels::CONTAINER_LOG_PATH_LABEL)
            .cloned()
            .unwrap_or_default();

        // Resolve the image id to kubelet-friendly refs. Unlike cri-dockerd
        // we skip the legacy docker:// prefixes: image_id must match what
        // PullImage returns (the Docker image ID digest).
        let image_id = inspect.image.clone().unwrap_or_default();
        let image_inspect = self.docker.inspect_image(&image_id).await.ok();
        let image_name = image_inspect
            .as_ref()
            .and_then(|i| i.repo_tags.as_ref())
            .and_then(|tags| tags.first().cloned())
            .or(config.image)
            .unwrap_or_default();
        let image_ref = image_inspect
            .as_ref()
            .and_then(|i| i.repo_digests.as_ref())
            .and_then(|digests| digests.first().cloned())
            .unwrap_or_else(|| image_id.clone());

        let mounts = inspect
            .mounts
            .as_ref()
            .map(|mounts| {
                mounts
                    .iter()
                    .map(|m| Mount {
                        host_path: m.source.clone().unwrap_or_default(),
                        container_path: m.destination.clone().unwrap_or_default(),
                        readonly: !m.rw.unwrap_or(true),
                        propagation: propagation_from_docker(m.propagation.as_deref()) as i32,
                        ..Default::default()
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(ContainerStatus {
            id: inspect.id.clone().unwrap_or_else(|| id.to_string()),
            metadata: Some(metadata),
            state: state as i32,
            created_at,
            started_at,
            finished_at,
            exit_code,
            image: Some(ImageSpec {
                image: image_name,
                ..Default::default()
            }),
            image_ref,
            image_id,
            reason: reason.to_string(),
            message,
            labels: cri_labels,
            annotations,
            mounts,
            log_path,
            ..Default::default()
        })
    }

    pub(crate) async fn update_app_container_resources(
        &self,
        id: &str,
        resources: LinuxContainerResources,
    ) -> Result<()> {
        self.docker
            .update_container(
                id,
                UpdateContainerOptions::<String> {
                    memory: Some(resources.memory_limit_in_bytes),
                    memory_swap: Some(resources.memory_limit_in_bytes),
                    cpu_shares: Some(resources.cpu_shares as isize),
                    cpu_quota: Some(resources.cpu_quota),
                    cpu_period: Some(resources.cpu_period),
                    cpuset_cpus: Some(resources.cpuset_cpus),
                    cpuset_mems: Some(resources.cpuset_mems),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| docker_err("update container resources", e))
    }

    // ---- exec --------------------------------------------------------------

    pub(crate) async fn exec_sync_in_container(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        if cmd.is_empty() {
            return Err(Error::InvalidArgument("exec command is required".into()));
        }
        let exec = self
            .docker
            .create_exec(
                id,
                CreateExecOptions::<String> {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(cmd.to_vec()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| docker_err("create exec", e))?;

        let collect = async {
            let started = self
                .docker
                .start_exec(&exec.id, None)
                .await
                .map_err(|e| docker_err("start exec", e))?;
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            if let StartExecResults::Attached { mut output, .. } = started {
                while let Some(frame) = output.next().await {
                    match frame.map_err(|e| docker_err("exec output", e))? {
                        bollard::container::LogOutput::StdOut { message }
                        | bollard::container::LogOutput::Console { message } => {
                            stdout.extend_from_slice(&message)
                        }
                        bollard::container::LogOutput::StdErr { message } => {
                            stderr.extend_from_slice(&message)
                        }
                        bollard::container::LogOutput::StdIn { .. } => {}
                    }
                    if stdout.len() + stderr.len() > MAX_EXEC_OUTPUT_BYTES {
                        tracing::warn!(container = %id, "exec output exceeds 16 MiB; truncating");
                        break;
                    }
                }
            }
            Ok::<_, Error>((stdout, stderr))
        };

        let (stdout, stderr) = if timeout_secs > 0 {
            let deadline = std::time::Duration::from_secs(timeout_secs as u64);
            match tokio::time::timeout(deadline, collect).await {
                Ok(result) => result?,
                Err(_) => {
                    // Deviation from cri-dockerd (which leaves the process
                    // running): kill the timed-out exec by its host PID.
                    self.kill_exec_process(&exec.id).await;
                    return Err(Error::DeadlineExceeded(format!(
                        "exec {cmd:?} timed out after {timeout_secs}s"
                    )));
                }
            }
        } else {
            collect.await?
        };

        let exit_code = self.exec_exit_code(&exec.id).await?;
        Ok(ExecSyncResult {
            stdout,
            stderr,
            exit_code,
        })
    }

    async fn kill_exec_process(&self, exec_id: &str) {
        let Ok(inspect) = self.docker.inspect_exec(exec_id).await else {
            return;
        };
        if inspect.running != Some(true) {
            return;
        }
        if let Some(pid) = inspect.pid.filter(|&p| p > 0) {
            tracing::warn!(pid, "killing timed-out exec process");
            // SAFETY: plain kill(2) on the exec's host PID.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        }
    }

    /// `inspect_exec` can briefly report `Running` after the stream closes;
    /// retry a few times, then give up with exit code 0 (cri-dockerd).
    async fn exec_exit_code(&self, exec_id: &str) -> Result<i32> {
        for attempt in 0..EXEC_INSPECT_RETRIES {
            let inspect = self
                .docker
                .inspect_exec(exec_id)
                .await
                .map_err(|e| docker_err("inspect exec", e))?;
            if inspect.running != Some(true) {
                return Ok(inspect.exit_code.unwrap_or_default() as i32);
            }
            if attempt + 1 < EXEC_INSPECT_RETRIES {
                tokio::time::sleep(EXEC_INSPECT_INTERVAL).await;
            }
        }
        tracing::error!(exec = %exec_id, "exec stream ended but process still running");
        Ok(0)
    }

    // ---- stats (basic; the rootfs size cache is plan 03 B5) ----------------

    pub(crate) async fn app_container_stats(&self, id: &str) -> Result<ContainerStats> {
        let containers = self
            .list_app_containers(Some(ContainerFilter {
                id: id.to_string(),
                ..Default::default()
            }))
            .await?;
        let container = containers
            .into_iter()
            .next()
            .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
        self.stats_for(&container).await
    }

    pub(crate) async fn list_app_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        let containers = self
            .list_app_containers(filter.map(|f| ContainerFilter {
                id: f.id,
                pod_sandbox_id: f.pod_sandbox_id,
                label_selector: f.label_selector,
                state: None,
            }))
            .await?;
        let stats = futures_util::stream::iter(containers)
            .map(|container| async move {
                match self.stats_for(&container).await {
                    Ok(stats) => Some(stats),
                    Err(err) => {
                        // Best effort: containers can vanish or be stopped
                        // between list and stats.
                        tracing::debug!(container = %container.id, %err, "skipping container stats");
                        None
                    }
                }
            })
            .buffer_unordered(STATS_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        Ok(stats.into_iter().flatten().collect())
    }

    async fn stats_for(&self, container: &Container) -> Result<ContainerStats> {
        let stats = self
            .docker
            .stats(
                &container.id,
                Some(StatsOptions {
                    stream: false,
                    one_shot: true,
                }),
            )
            .next()
            .await
            .ok_or_else(|| Error::Internal(format!("no stats for container {}", container.id)))?
            .map_err(|e| docker_err("container stats", e))?;

        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        Ok(ContainerStats {
            attributes: Some(ContainerAttributes {
                id: container.id.clone(),
                metadata: container.metadata.clone(),
                labels: container.labels.clone(),
                annotations: container.annotations.clone(),
            }),
            cpu: Some(CpuUsage {
                timestamp,
                usage_core_nano_seconds: Some(UInt64Value {
                    value: stats.cpu_stats.cpu_usage.total_usage,
                }),
                ..Default::default()
            }),
            memory: Some(MemoryUsage {
                timestamp,
                working_set_bytes: Some(UInt64Value {
                    value: stats.memory_stats.usage.unwrap_or_default(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    // ---- logs ---------------------------------------------------------------

    pub(crate) async fn reopen_app_container_log(&self, id: &str) -> Result<()> {
        let inspect = self
            .docker
            .inspect_container(id, None::<InspectContainerOptions>)
            .await
            .map_err(|e| docker_err("inspect container for log reopen", e))?;
        let full_id = inspect.id.unwrap_or_else(|| id.to_string());
        if self.log_relays.reopen(&full_id).await {
            return Ok(());
        }

        // No live relay (e.g. after a shim restart without resume): start
        // one from the file's last record instead of failing rotation.
        let running = inspect
            .state
            .as_ref()
            .and_then(|s| s.running)
            .unwrap_or(false);
        let Some(path) = inspect
            .config
            .and_then(|c| c.labels)
            .as_ref()
            .and_then(|l| l.get(labels::CONTAINER_LOG_PATH_LABEL))
            .filter(|p| !p.is_empty())
            .map(PathBuf::from)
        else {
            return Ok(()); // No log path was requested; nothing to rotate.
        };
        if !running {
            return Err(Error::FailedPrecondition(format!(
                "container {id} is not running"
            )));
        }
        let resume_after = logs::last_logged_time(&path).await;
        self.log_relays
            .start(self.docker.clone(), full_id, path, resume_after);
        Ok(())
    }

    /// Restart log relays for running CRI containers after a shim restart,
    /// resuming each from its file's last record.
    pub(crate) async fn resume_log_relays(&self) {
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!(
                "{}={}",
                labels::CONTAINER_TYPE_LABEL,
                labels::CONTAINER_TYPE_CONTAINER
            )],
        );
        filters.insert("status".to_string(), vec!["running".to_string()]);
        let summaries = match self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
        {
            Ok(summaries) => summaries,
            Err(err) => {
                tracing::warn!(%err, "cannot list containers to resume log relays");
                return;
            }
        };
        for summary in summaries {
            let Some(id) = summary.id else { continue };
            let Some(path) = summary
                .labels
                .as_ref()
                .and_then(|l| l.get(labels::CONTAINER_LOG_PATH_LABEL))
                .filter(|p| !p.is_empty())
                .map(PathBuf::from)
            else {
                continue;
            };
            let resume_after = logs::last_logged_time(&path).await;
            tracing::info!(container = %id, path = %path.display(), "resuming log relay");
            self.log_relays
                .start(self.docker.clone(), id, path, resume_after);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_list_formats_pairs() {
        let envs = vec![KeyValue {
            key: "PATH".into(),
            value: "/usr/bin".into(),
        }];
        assert_eq!(env_list(&envs), vec!["PATH=/usr/bin".to_string()]);
    }

    #[test]
    fn mounts_map_readonly_and_propagation() {
        let mounts = mount_bindings(&[
            Mount {
                host_path: "/data".into(),
                container_path: "/mnt".into(),
                readonly: true,
                propagation: MountPropagation::PropagationHostToContainer as i32,
                ..Default::default()
            },
            Mount {
                host_path: "/shared".into(),
                container_path: "/shared".into(),
                propagation: MountPropagation::PropagationBidirectional as i32,
                ..Default::default()
            },
        ])
        .unwrap();
        assert_eq!(mounts[0].read_only, Some(true));
        let bind = mounts[0].bind_options.as_ref().unwrap();
        assert_eq!(bind.read_only_non_recursive, Some(true));
        assert_eq!(
            bind.propagation,
            Some(MountBindOptionsPropagationEnum::RSLAVE)
        );
        assert_eq!(bind.create_mountpoint, Some(true));
        assert_eq!(
            mounts[1].bind_options.as_ref().unwrap().propagation,
            Some(MountBindOptionsPropagationEnum::RSHARED)
        );
    }

    #[test]
    fn rro_mount_requires_readonly_private() {
        let rw = mount_bindings(&[Mount {
            host_path: "/data".into(),
            container_path: "/mnt".into(),
            recursive_read_only: true,
            readonly: false,
            ..Default::default()
        }]);
        assert!(rw.is_err());

        let ok = mount_bindings(&[Mount {
            host_path: "/data".into(),
            container_path: "/mnt".into(),
            recursive_read_only: true,
            readonly: true,
            propagation: MountPropagation::PropagationPrivate as i32,
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(
            ok[0].bind_options.as_ref().unwrap().read_only_non_recursive,
            Some(false)
        );
    }

    #[test]
    fn user_mapping() {
        let mut sc = LinuxContainerSecurityContext {
            run_as_user: Some(Int64Value { value: 1000 }),
            ..Default::default()
        };
        assert_eq!(container_user(&sc).unwrap().as_deref(), Some("1000"));

        sc.run_as_group = Some(Int64Value { value: 2000 });
        assert_eq!(container_user(&sc).unwrap().as_deref(), Some("1000:2000"));

        sc.run_as_username = "www-data".into();
        assert_eq!(
            container_user(&sc).unwrap().as_deref(),
            Some("www-data:2000")
        );

        let group_only = LinuxContainerSecurityContext {
            run_as_group: Some(Int64Value { value: 2000 }),
            ..Default::default()
        };
        assert!(container_user(&group_only).is_err());
    }

    #[test]
    fn namespace_modes_join_sandbox() {
        let sc = LinuxContainerSecurityContext {
            namespace_options: Some(NamespaceOption {
                pid: NamespaceMode::Pod as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut hc = HostConfig::default();
        apply_security_context(&sc, "sbx1", &mut hc).unwrap();
        assert_eq!(hc.network_mode.as_deref(), Some("container:sbx1"));
        assert_eq!(hc.ipc_mode.as_deref(), Some("container:sbx1"));
        assert_eq!(hc.pid_mode.as_deref(), Some("container:sbx1"));
        assert_eq!(hc.uts_mode, None);

        let host_ns = LinuxContainerSecurityContext {
            namespace_options: Some(NamespaceOption {
                network: NamespaceMode::Node as i32,
                pid: NamespaceMode::Node as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut hc = HostConfig::default();
        apply_security_context(&host_ns, "sbx1", &mut hc).unwrap();
        assert_eq!(hc.uts_mode.as_deref(), Some("host"));
        assert_eq!(hc.pid_mode.as_deref(), Some("host"));
        // Network still joins the sandbox (which itself is host-network).
        assert_eq!(hc.network_mode.as_deref(), Some("container:sbx1"));
    }

    #[test]
    fn seccomp_defaults_to_unconfined() {
        assert_eq!(
            seccomp_security_opt(None, false).unwrap().as_deref(),
            Some("seccomp=unconfined")
        );
        let runtime_default = SecurityProfile {
            profile_type: security_profile::ProfileType::RuntimeDefault as i32,
            ..Default::default()
        };
        assert_eq!(
            seccomp_security_opt(Some(&runtime_default), false).unwrap(),
            None
        );
        let relative = SecurityProfile {
            profile_type: security_profile::ProfileType::Localhost as i32,
            localhost_ref: "relative/path.json".into(),
        };
        assert!(seccomp_security_opt(Some(&relative), false).is_err());
    }

    #[test]
    fn privileged_seccomp_drops_sethostname_block() {
        let profile: serde_json::Value = serde_json::json!({
            "defaultAction": "SCMP_ACT_ALLOW",
            "syscalls": [
                {"names": ["sethostname"], "action": "SCMP_ACT_ERRNO"},
                {"names": ["sethostname", "chroot"], "action": "SCMP_ACT_ERRNO"},
                {"names": ["read"], "action": "SCMP_ACT_ALLOW"}
            ]
        });
        let filtered = filter_seccomp_profile(profile.clone(), true);
        let syscalls = filtered["syscalls"].as_array().unwrap();
        assert_eq!(syscalls.len(), 2);
        assert_eq!(syscalls[0]["names"], serde_json::json!(["chroot"]));
        assert_eq!(syscalls[1]["names"], serde_json::json!(["read"]));

        // Unprivileged: untouched.
        assert_eq!(filter_seccomp_profile(profile.clone(), false), profile);
    }

    #[test]
    fn apparmor_profiles() {
        let unconfined = LinuxContainerSecurityContext {
            apparmor: Some(SecurityProfile {
                profile_type: security_profile::ProfileType::Unconfined as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            apparmor_security_opt(&unconfined).as_deref(),
            Some("apparmor=unconfined")
        );

        #[allow(deprecated)]
        let legacy = LinuxContainerSecurityContext {
            apparmor_profile: "localhost/my-profile".into(),
            ..Default::default()
        };
        assert_eq!(
            apparmor_security_opt(&legacy).as_deref(),
            Some("apparmor=my-profile")
        );
        assert_eq!(
            apparmor_security_opt(&LinuxContainerSecurityContext::default()),
            None
        );
    }

    #[test]
    fn propagation_round_trip() {
        assert_eq!(
            propagation_from_docker(Some("rshared")),
            MountPropagation::PropagationBidirectional
        );
        assert_eq!(
            propagation_from_docker(Some("rslave")),
            MountPropagation::PropagationHostToContainer
        );
        assert_eq!(
            propagation_from_docker(Some("rprivate")),
            MountPropagation::PropagationPrivate
        );
        assert_eq!(
            propagation_from_docker(None),
            MountPropagation::PropagationPrivate
        );
    }

    #[test]
    fn summary_state_mapping() {
        assert_eq!(
            summary_state(Some("created")),
            ContainerState::ContainerCreated
        );
        assert_eq!(
            summary_state(Some("running")),
            ContainerState::ContainerRunning
        );
        assert_eq!(
            summary_state(Some("paused")),
            ContainerState::ContainerRunning
        );
        assert_eq!(
            summary_state(Some("exited")),
            ContainerState::ContainerExited
        );
        assert_eq!(
            summary_state(Some("dead")),
            ContainerState::ContainerUnknown
        );
        assert_eq!(summary_state(None), ContainerState::ContainerUnknown);
    }
}

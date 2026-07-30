// SPDX-License-Identifier: Apache-2.0

//! Container lifecycle: create, start, stop, remove, list, status, exec.
//!
//! Each CRI container becomes one Apple container — that is, one microVM. The
//! interesting work is translating a `ContainerConfig` into `container create`
//! flags, because the CLI exposes a strict subset of what the guest OCI spec
//! supports. The unsupported request is never silently dropped: it is either
//! clamped with a warning (memory below the VM floor) or reported through
//! `ContainerStatus`/`Unimplemented` so a caller can tell.

use std::collections::HashMap;

use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::ExecSyncResult;

use crate::backend::AppleBackend;
use crate::cli::CreateSpec;
use crate::naming;
use crate::sandbox::matches_selector;
use crate::state::{now_nanos, ContainerRecord, MountRecord, ResourcesRecord};

/// The kernel plus a minimal userland needs headroom to boot; a CRI memory
/// limit below this cannot produce a working VM, so it is clamped (and logged)
/// rather than failing the pod.
const MIN_VM_MEMORY_BYTES: i64 = 128 * 1024 * 1024;

/// Resolve the effective argv the way the kubelet and the OCI spec do:
/// `command` overrides the image ENTRYPOINT, `args` overrides its CMD, and
/// supplying `command` alone drops the image CMD entirely.
///
/// Ported from upstream `pkg/kubelet/kuberuntime/kuberuntime_container.go`
/// and the OCI image-spec's ENTRYPOINT/CMD rules.
pub(crate) fn effective_argv(
    command: &[String],
    args: &[String],
    image_entrypoint: &[String],
    image_cmd: &[String],
) -> Vec<String> {
    let entrypoint = if command.is_empty() {
        image_entrypoint.to_vec()
    } else {
        command.to_vec()
    };
    let cmd = if !args.is_empty() {
        args.to_vec()
    } else if !command.is_empty() {
        // An explicit command with no args must not inherit the image CMD.
        Vec::new()
    } else {
        image_cmd.to_vec()
    };
    [entrypoint, cmd].concat()
}

/// CPU count for `--cpus` from a CRI CPU limit.
///
/// CRI expresses CPU as a cgroup quota over a period; Apple sizes the VM in
/// whole vCPUs, so the quota is rounded **up** — under-provisioning a VM would
/// throttle the workload below its request, which is worse than the
/// coarser-grained over-provisioning rounding up gives.
pub(crate) fn cpus_from_quota(quota: i64, period: i64) -> Option<i64> {
    if quota <= 0 {
        return None;
    }
    let period = if period > 0 { period } else { 100_000 };
    Some(((quota + period - 1) / period).max(1))
}

/// Memory for `--memory`, clamped to the VM floor.
pub(crate) fn memory_for_vm(limit: i64) -> Option<i64> {
    if limit <= 0 {
        return None;
    }
    if limit < MIN_VM_MEMORY_BYTES {
        tracing::warn!(
            requested = limit,
            clamped_to = MIN_VM_MEMORY_BYTES,
            "memory limit below the microVM floor; clamping (apple-cri deviation)"
        );
        return Some(MIN_VM_MEMORY_BYTES);
    }
    Some(limit)
}

/// `--user` value from a CRI security context.
pub(crate) fn user_arg(sc: Option<&LinuxContainerSecurityContext>) -> Option<String> {
    let sc = sc?;
    if !sc.run_as_username.is_empty() {
        return Some(sc.run_as_username.clone());
    }
    let uid = sc.run_as_user.as_ref().map(|v| v.value)?;
    match sc.run_as_group.as_ref().map(|v| v.value) {
        Some(gid) => Some(format!("{uid}:{gid}")),
        None => Some(uid.to_string()),
    }
}

/// `--publish` arguments for a sandbox's port mappings.
///
/// A mapping with no host port asks only that the container port be *exposed*,
/// not bound on the host: cri-dockerd's `portMappings` skips these ("No need to
/// do port binding when HostPort is not set"), and Apple rejects `--publish
/// 0:80` outright with `invalidArgument: "invalid publish host port range: 0"`.
/// Apple has no expose-without-publish concept, so skipping is all there is to
/// do — and it is what critest's "port mapping with only container port" spec
/// expects.
pub(crate) fn publish_specs(mappings: &[crate::state::PortMappingRecord]) -> Vec<String> {
    mappings
        .iter()
        .filter(|pm| pm.host_port > 0)
        .map(|pm| {
            let proto = if pm.protocol == Protocol::Udp as i32 {
                "udp"
            } else {
                "tcp"
            };
            let host_ip = if pm.host_ip.is_empty() {
                String::new()
            } else {
                format!("{}:", pm.host_ip)
            };
            format!("{host_ip}{}:{}/{proto}", pm.host_port, pm.container_port)
        })
        .collect()
}

/// The signal name `container stop --signal` should use.
fn stop_signal_name(signal: i32) -> String {
    match Signal::try_from(signal) {
        // RUNTIME_DEFAULT means "whatever the runtime normally uses".
        Ok(Signal::RuntimeDefault) | Err(_) => "SIGTERM".to_string(),
        Ok(other) => other.as_str_name().to_string(),
    }
}

impl AppleBackend {
    pub(crate) async fn create_app_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        _sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let sandbox = self
            .store
            .sandbox(sandbox_id)
            .ok_or_else(|| Error::NotFound(format!("sandbox {sandbox_id}")))?;
        if !sandbox.ready {
            return Err(Error::FailedPrecondition(format!(
                "sandbox {sandbox_id} is not ready"
            )));
        }
        let meta = config
            .metadata
            .clone()
            .ok_or_else(|| Error::InvalidArgument("container config has no metadata".into()))?;
        let image_spec = config
            .image
            .clone()
            .ok_or_else(|| Error::InvalidArgument("container config has no image".into()))?;

        let image = self
            .resolve_image(&image_spec.image)
            .await?
            .ok_or_else(|| Error::NotFound(format!("image {}", image_spec.image)))?;

        // Idempotent create: a retried CreateContainer for the same
        // (sandbox, name, attempt) must return the original id rather than
        // making a second container. Apple's duplicate-name rejection cannot
        // serve this any more, because ids are opaque (see crate::naming).
        if let Some(existing) = self
            .store
            .containers_in_sandbox(sandbox_id)
            .into_iter()
            .find(|c| c.name == meta.name && c.attempt == meta.attempt)
        {
            if self.cli.inspect_container(&existing.id).await?.is_some() {
                return Ok(existing.id);
            }
            // The record outlived the runtime object; drop it and re-create.
            tracing::warn!(container = %existing.id, "stale record with no container; recreating");
            self.store.delete_container(&existing.id)?;
        }

        let id = naming::new_id();
        let argv = effective_argv(&config.command, &config.args, &image.entrypoint, &image.cmd);

        let linux = config.linux.clone().unwrap_or_default();
        let resources = linux.resources.clone().unwrap_or_default();
        let security = linux.security_context.clone();

        // Report what this runtime cannot honour rather than pretending.
        warn_unsupported(&config, &resources, security.as_ref());
        if !sandbox.hostname.is_empty() {
            // Apple derives the guest hostname from the container name and
            // `container create` has no `--hostname`, so the pod's hostname
            // cannot be applied (README "Deviations").
            tracing::debug!(
                container = %id, requested_hostname = %sandbox.hostname,
                "guest hostname will be the container id: no --hostname flag exists"
            );
        }

        let mut spec = CreateSpec {
            name: id.clone(),
            image: image.reference.clone(),
            args: argv,
            // CRI 1.36 types `KeyValue.value` as `bytes` (matching upstream
            // `cri-api`), but an environment variable is a C string.
            env: config
                .envs
                .iter()
                .map(|kv| {
                    (
                        kv.key.clone(),
                        String::from_utf8_lossy(&kv.value).to_string(),
                    )
                })
                .collect(),
            labels: self.discovery_labels(sandbox_id, &sandbox, Some(&meta.name)),
            workdir: (!config.working_dir.is_empty()).then(|| config.working_dir.clone()),
            user: user_arg(security.as_ref()),
            network: Some(sandbox.network.clone()),
            dns_servers: sandbox.dns.servers.clone(),
            dns_searches: sandbox.dns.searches.clone(),
            dns_options: sandbox.dns.options.clone(),
            cpus: cpus_from_quota(resources.cpu_quota, resources.cpu_period),
            memory_bytes: memory_for_vm(resources.memory_limit_in_bytes),
            tty: config.tty,
            stdin: config.stdin,
            platform: Some(self.platform()),
            ..Default::default()
        };

        for m in &config.mounts {
            if m.host_path.is_empty() {
                // An image-volume mount (CRI 1.31 KEP-4639) has no host path;
                // Apple cannot mount an image as a volume.
                tracing::warn!(
                    container = %id, target = %m.container_path,
                    "skipping image-volume mount: unsupported by apple-cri"
                );
                continue;
            }
            spec.binds
                .push((m.host_path.clone(), m.container_path.clone(), m.readonly));
        }
        spec.publish = publish_specs(&sandbox.port_mappings);

        tracing::debug!(container = %id, argv = ?spec.to_args(), "creating container");
        let created = self.cli.create_container(&spec).await?;
        let log_path = self.resolve_log_path(&sandbox.log_directory, &config.log_path);

        let record = ContainerRecord {
            id: created.clone(),
            sandbox_id: sandbox_id.to_string(),
            name: meta.name.clone(),
            attempt: meta.attempt,
            labels: config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: config
                .annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            image_ref: image_spec.image.clone(),
            image_id: image.id.clone(),
            created_at: now_nanos(),
            log_path,
            tty: config.tty,
            stdin: config.stdin,
            mounts: config
                .mounts
                .iter()
                .map(|m| MountRecord {
                    container_path: m.container_path.clone(),
                    host_path: m.host_path.clone(),
                    readonly: m.readonly,
                    propagation: m.propagation,
                    selinux_relabel: m.selinux_relabel,
                    recursive_read_only: m.recursive_read_only,
                })
                .collect(),
            resources: ResourcesRecord {
                cpu_period: resources.cpu_period,
                cpu_quota: resources.cpu_quota,
                cpu_shares: resources.cpu_shares,
                memory_limit_in_bytes: resources.memory_limit_in_bytes,
                oom_score_adj: resources.oom_score_adj,
                cpuset_cpus: resources.cpuset_cpus.clone(),
                cpuset_mems: resources.cpuset_mems.clone(),
                memory_swap_limit_in_bytes: resources.memory_swap_limit_in_bytes,
            },
            stop_signal: config.stop_signal,
            run_as_uid: security
                .as_ref()
                .and_then(|s| s.run_as_user.as_ref().map(|v| v.value))
                .unwrap_or(0),
            run_as_gid: security
                .as_ref()
                .and_then(|s| s.run_as_group.as_ref().map(|v| v.value))
                .unwrap_or(0),
            ..Default::default()
        };
        self.store.put_container(record)?;
        Ok(created)
    }

    /// `{log_directory}/{log_path}`, per CRI.
    fn resolve_log_path(&self, log_directory: &str, log_path: &str) -> String {
        if log_path.is_empty() {
            return String::new();
        }
        if log_directory.is_empty() || log_path.starts_with('/') {
            return log_path.to_string();
        }
        std::path::Path::new(log_directory)
            .join(log_path)
            .to_string_lossy()
            .to_string()
    }

    pub(crate) async fn start_app_container(&self, id: &str) -> Result<()> {
        let rec = self
            .store
            .container(id)
            .ok_or_else(|| Error::NotFound(format!("container {id}")))?;

        // The relay owns `container start --attach`: it is both the start call
        // and the only live source of the exit code (see crate::logs).
        self.log_relays
            .start_attached(
                &self.cli,
                self.store.clone(),
                id,
                std::path::PathBuf::from(&rec.log_path),
                rec.tty,
                rec.stdin,
            )
            .await?;

        self.store.update_container(id, |c| {
            c.started = true;
            c.started_at = now_nanos();
        })?;
        Ok(())
    }

    pub(crate) async fn stop_app_container(&self, id: &str, timeout_secs: i64) -> Result<()> {
        // CRI: stopping an unknown container is Ok.
        let Some(rec) = self.store.container(id) else {
            return Ok(());
        };
        if timeout_secs <= 0 {
            // CRI: a zero grace period means kill now, without SIGTERM first.
            self.cli.kill_container(id, "SIGKILL").await?;
        } else {
            let signal = stop_signal_name(rec.stop_signal);
            self.cli.stop_container(id, timeout_secs, &signal).await?;
        }

        // `container stop` returns once the VM is down, but the attach
        // supervisor may not have recorded the exit yet; CRI callers read the
        // status straight after, so settle it here.
        self.settle_exit(id).await;
        Ok(())
    }

    /// Make sure a stopped container has an exit code recorded, falling back to
    /// the guest's own `vminitd` log when the supervisor has not reported yet.
    async fn settle_exit(&self, id: &str) {
        for _ in 0..20 {
            if self.store.container(id).is_some_and(|c| c.finished) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if let Some(code) = crate::logs::exit_status_from_vminitd_log(id) {
            let reason = if code == 0 { "Completed" } else { "Error" };
            let _ = self.store.record_exit(id, code, reason);
            return;
        }
        // Stopped on request, and neither source produced a code: record the
        // conventional SIGTERM exit rather than leaving finished_at unset.
        let _ = self.store.record_exit(id, 137, "Error");
    }

    pub(crate) async fn remove_app_container(&self, id: &str) -> Result<()> {
        self.log_relays.stop(id);
        self.cli.remove_container(id).await?;
        self.store.delete_container(id)?;
        Ok(())
    }

    pub(crate) async fn list_app_containers(
        &self,
        filter: Option<ContainerFilter>,
    ) -> Result<Vec<Container>> {
        // One CLI call for every container's live state, then join against our
        // records — N inspects would make the kubelet's relist quadratic.
        let live: HashMap<String, bool> = self
            .cli
            .list_containers()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| (c.configuration.id.clone(), c.is_running()))
            .collect();

        let mut out = Vec::new();
        for rec in self.store.containers() {
            let state = container_state(&rec, live.get(&rec.id).copied());
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != rec.id {
                    continue;
                }
                if !f.pod_sandbox_id.is_empty() && f.pod_sandbox_id != rec.sandbox_id {
                    continue;
                }
                if let Some(want) = &f.state {
                    if want.state != state as i32 {
                        continue;
                    }
                }
                if !matches_selector(&f.label_selector, &rec.labels) {
                    continue;
                }
            }
            out.push(Container {
                id: rec.id.clone(),
                pod_sandbox_id: rec.sandbox_id.clone(),
                metadata: Some(rec.metadata()),
                image: Some(ImageSpec {
                    image: rec.image_ref.clone(),
                    ..Default::default()
                }),
                image_ref: rec.image_id.clone(),
                state: state as i32,
                created_at: rec.created_at,
                labels: rec.labels.clone().into_iter().collect(),
                annotations: rec.annotations.clone().into_iter().collect(),
                image_id: rec.image_id.clone(),
            });
        }
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    pub(crate) async fn app_container_status(&self, id: &str) -> Result<ContainerStatus> {
        let rec = self
            .store
            .container(id)
            .ok_or_else(|| Error::NotFound(format!("container {id}")))?;
        let live = self.cli.inspect_container(id).await?;
        let running = live.as_ref().map(|c| c.is_running());

        // A container the runtime reports stopped, but whose exit we have not
        // recorded, has exited behind our back (e.g. shim restart).
        if running == Some(false) && rec.started && !rec.finished {
            if let Some(code) = crate::logs::exit_status_from_vminitd_log(id) {
                let reason = if code == 0 { "Completed" } else { "Error" };
                let _ = self.store.record_exit(id, code, reason);
            }
        }
        let rec = self.store.container(id).unwrap_or(rec);
        let state = container_state(&rec, running);

        Ok(ContainerStatus {
            id: rec.id.clone(),
            metadata: Some(rec.metadata()),
            state: state as i32,
            created_at: rec.created_at,
            started_at: rec.started_at,
            finished_at: rec.finished_at,
            exit_code: rec.exit_code,
            image: Some(ImageSpec {
                image: rec.image_ref.clone(),
                ..Default::default()
            }),
            image_ref: rec.image_id.clone(),
            image_id: rec.image_id.clone(),
            reason: rec.reason.clone(),
            message: rec.message.clone(),
            labels: rec.labels.clone().into_iter().collect(),
            annotations: rec.annotations.clone().into_iter().collect(),
            mounts: rec
                .mounts
                .iter()
                .map(|m| Mount {
                    container_path: m.container_path.clone(),
                    host_path: m.host_path.clone(),
                    readonly: m.readonly,
                    selinux_relabel: m.selinux_relabel,
                    propagation: m.propagation,
                    recursive_read_only: m.recursive_read_only,
                    ..Default::default()
                })
                .collect(),
            log_path: rec.log_path.clone(),
            resources: Some(ContainerResources {
                linux: Some(LinuxContainerResources {
                    cpu_period: rec.resources.cpu_period,
                    cpu_quota: rec.resources.cpu_quota,
                    cpu_shares: rec.resources.cpu_shares,
                    memory_limit_in_bytes: rec.resources.memory_limit_in_bytes,
                    oom_score_adj: rec.resources.oom_score_adj,
                    cpuset_cpus: rec.resources.cpuset_cpus.clone(),
                    cpuset_mems: rec.resources.cpuset_mems.clone(),
                    memory_swap_limit_in_bytes: rec.resources.memory_swap_limit_in_bytes,
                    ..Default::default()
                }),
                windows: None,
            }),
            user: Some(ContainerUser {
                linux: Some(LinuxContainerUser {
                    uid: rec.run_as_uid,
                    gid: rec.run_as_gid,
                    supplemental_groups: Vec::new(),
                }),
            }),
            stop_signal: rec.stop_signal,
        })
    }

    /// CRI `UpdateContainerResources`.
    ///
    /// A running microVM's vCPU count and memory size are fixed at boot, and
    /// the CLI has no update verb — so this can only be honoured for the *next*
    /// start. The request is recorded and reported as unimplemented rather than
    /// silently accepted, which would let a caller believe a limit took effect.
    pub(crate) async fn update_app_container_resources(
        &self,
        id: &str,
        resources: LinuxContainerResources,
    ) -> Result<()> {
        if self.store.container(id).is_none() {
            return Err(Error::NotFound(format!("container {id}")));
        }
        self.store.update_container(id, |c| {
            c.resources.cpu_period = resources.cpu_period;
            c.resources.cpu_quota = resources.cpu_quota;
            c.resources.cpu_shares = resources.cpu_shares;
            c.resources.memory_limit_in_bytes = resources.memory_limit_in_bytes;
            c.resources.oom_score_adj = resources.oom_score_adj;
        })?;
        Err(Error::Unimplemented(
            "apple-cri: a running microVM cannot be resized; \
             the new limits apply at the next container start"
                .into(),
        ))
    }

    pub(crate) async fn exec_sync_in_container(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        if self.store.container(id).is_none() {
            return Err(Error::NotFound(format!("container {id}")));
        }
        if cmd.is_empty() {
            return Err(Error::InvalidArgument("exec with no command".into()));
        }
        let deadline =
            (timeout_secs > 0).then(|| std::time::Duration::from_secs(timeout_secs as u64));
        let out = self.cli.exec(id, cmd, false, deadline).await?;

        // A CLI-level failure (container gone, not running) is an RPC error;
        // a non-zero exit *of the command* is a successful ExecSync.
        let stderr = out.stderr_str();
        if !out.status.success() && crate::cli::looks_like_cli_error(&stderr) {
            return Err(crate::cli::classify(&format!("exec in {id}"), &stderr));
        }
        Ok(ExecSyncResult {
            stdout: out.stdout,
            stderr: out.stderr,
            exit_code: out.status.code().unwrap_or(255),
        })
    }

    pub(crate) async fn reopen_app_container_log(&self, id: &str) -> Result<()> {
        let rec = self
            .store
            .container(id)
            .ok_or_else(|| Error::NotFound(format!("container {id}")))?;
        if self.log_relays.reopen(id).await {
            return Ok(());
        }
        // No live relay (the container already exited): recreate the file so
        // the caller's rotation still leaves a readable log path.
        if !rec.log_path.is_empty() {
            let path = std::path::Path::new(&rec.log_path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn app_container_stats(&self, id: &str) -> Result<ContainerStats> {
        let rec = self
            .store
            .container(id)
            .ok_or_else(|| Error::NotFound(format!("container {id}")))?;
        let snapshot = self.stats_snapshot().await;
        Ok(self.cri_stats(&rec, snapshot.get(id)))
    }

    pub(crate) async fn list_app_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        let snapshot = self.stats_snapshot().await;
        let mut out = Vec::new();
        for rec in self.store.containers() {
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != rec.id {
                    continue;
                }
                if !f.pod_sandbox_id.is_empty() && f.pod_sandbox_id != rec.sandbox_id {
                    continue;
                }
                if !matches_selector(&f.label_selector, &rec.labels) {
                    continue;
                }
            }
            out.push(self.cri_stats(&rec, snapshot.get(&rec.id)));
        }
        Ok(out)
    }
}

/// Map our record plus the runtime's live view onto a CRI container state.
///
/// Apple reports a never-started container and an exited one identically
/// (`stopped`), so the record's `started` flag is what separates CREATED from
/// EXITED.
pub(crate) fn container_state(rec: &ContainerRecord, running: Option<bool>) -> ContainerState {
    match running {
        Some(true) => ContainerState::ContainerRunning,
        Some(false) | None => {
            if rec.started || rec.finished {
                ContainerState::ContainerExited
            } else {
                ContainerState::ContainerCreated
            }
        }
    }
}

/// Log the parts of a `ContainerConfig` this runtime cannot honour, once per
/// container create. Silence here would look like support.
fn warn_unsupported(
    config: &ContainerConfig,
    resources: &LinuxContainerResources,
    security: Option<&LinuxContainerSecurityContext>,
) {
    let name = config
        .metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default();
    if !config.devices.is_empty() {
        tracing::warn!(container = %name, "device mappings are not supported by apple-cri");
    }
    if !resources.hugepage_limits.is_empty() {
        tracing::warn!(container = %name, "hugepage limits are not supported by apple-cri");
    }
    if !resources.cpuset_cpus.is_empty() || !resources.cpuset_mems.is_empty() {
        tracing::warn!(container = %name, "cpuset pinning is not supported by apple-cri");
    }
    let Some(sc) = security else { return };
    if sc.privileged {
        tracing::warn!(
            container = %name,
            "privileged is not expressible through the container CLI; \
             the workload already runs in its own VM"
        );
    }
    if sc.capabilities.is_some() {
        tracing::warn!(container = %name, "capability add/drop is not supported by apple-cri");
    }
    if sc.seccomp.is_some() {
        tracing::warn!(container = %name, "seccomp profiles are not supported by apple-cri");
    }
    if sc.readonly_rootfs {
        tracing::warn!(container = %name, "readonly rootfs is not supported by apple-cri");
    }
    if !sc.supplemental_groups.is_empty() {
        tracing::warn!(container = %name, "supplemental groups are not supported by apple-cri");
    }
    if sc
        .namespace_options
        .as_ref()
        .is_some_and(|n| n.pid == NamespaceMode::Pod as i32 || n.ipc == NamespaceMode::Pod as i32)
    {
        tracing::warn!(
            container = %name,
            "pod-shared PID/IPC namespaces cannot be provided: each container is its own VM"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn argv_follows_oci_entrypoint_cmd_rules() {
        let img_ep = s(&["/entry"]);
        let img_cmd = s(&["default-arg"]);

        // Nothing overridden: ENTRYPOINT + CMD.
        assert_eq!(
            effective_argv(&[], &[], &img_ep, &img_cmd),
            s(&["/entry", "default-arg"])
        );
        // args override CMD only.
        assert_eq!(
            effective_argv(&[], &s(&["mine"]), &img_ep, &img_cmd),
            s(&["/entry", "mine"])
        );
        // command overrides ENTRYPOINT *and* drops the image CMD.
        assert_eq!(
            effective_argv(&s(&["/bin/sh"]), &[], &img_ep, &img_cmd),
            s(&["/bin/sh"])
        );
        // Both overridden.
        assert_eq!(
            effective_argv(&s(&["/bin/sh"]), &s(&["-c", "true"]), &img_ep, &img_cmd),
            s(&["/bin/sh", "-c", "true"])
        );
        // An image with only a CMD.
        assert_eq!(effective_argv(&[], &[], &[], &img_cmd), s(&["default-arg"]));
        // Nothing anywhere: let the runtime resolve from the image.
        assert!(effective_argv(&[], &[], &[], &[]).is_empty());
    }

    #[test]
    fn cpu_quota_rounds_up_to_whole_vcpus() {
        // 0.5 CPU still needs a whole vCPU.
        assert_eq!(cpus_from_quota(50_000, 100_000), Some(1));
        assert_eq!(cpus_from_quota(100_000, 100_000), Some(1));
        assert_eq!(cpus_from_quota(150_000, 100_000), Some(2));
        assert_eq!(cpus_from_quota(400_000, 100_000), Some(4));
        // A missing period defaults to the cgroup default of 100ms.
        assert_eq!(cpus_from_quota(200_000, 0), Some(2));
        // Unlimited.
        assert_eq!(cpus_from_quota(0, 100_000), None);
        assert_eq!(cpus_from_quota(-1, 100_000), None);
    }

    #[test]
    fn memory_is_clamped_to_the_vm_floor() {
        assert_eq!(memory_for_vm(0), None);
        assert_eq!(memory_for_vm(512 * 1024 * 1024), Some(512 * 1024 * 1024));
        // Below the floor: clamped, not rejected.
        assert_eq!(memory_for_vm(16 * 1024 * 1024), Some(MIN_VM_MEMORY_BYTES));
        assert_eq!(
            memory_for_vm(MIN_VM_MEMORY_BYTES),
            Some(MIN_VM_MEMORY_BYTES)
        );
    }

    #[test]
    fn user_arg_prefers_username_then_uid_gid() {
        let with = |f: fn(&mut LinuxContainerSecurityContext)| {
            let mut sc = LinuxContainerSecurityContext::default();
            f(&mut sc);
            user_arg(Some(&sc))
        };
        assert_eq!(user_arg(None), None);
        assert_eq!(with(|_| {}), None);
        assert_eq!(
            with(|sc| sc.run_as_user = Some(Int64Value { value: 1000 })),
            Some("1000".into())
        );
        assert_eq!(
            with(|sc| {
                sc.run_as_user = Some(Int64Value { value: 1000 });
                sc.run_as_group = Some(Int64Value { value: 2000 });
            }),
            Some("1000:2000".into())
        );
        assert_eq!(
            with(|sc| {
                sc.run_as_username = "nobody".into();
                sc.run_as_user = Some(Int64Value { value: 1000 });
            }),
            Some("nobody".into())
        );
    }

    #[test]
    fn stop_signal_defaults_to_sigterm() {
        assert_eq!(stop_signal_name(Signal::RuntimeDefault as i32), "SIGTERM");
        assert_eq!(stop_signal_name(Signal::Sigkill as i32), "SIGKILL");
        assert_eq!(stop_signal_name(Signal::Sigint as i32), "SIGINT");
        // An unknown enum value must not panic.
        assert_eq!(stop_signal_name(9999), "SIGTERM");
    }

    #[test]
    fn created_and_exited_are_distinguished_by_the_started_flag() {
        let mut rec = ContainerRecord::default();
        // Apple reports both as "stopped"; only our flag separates them.
        assert_eq!(
            container_state(&rec, Some(false)),
            ContainerState::ContainerCreated
        );
        rec.started = true;
        assert_eq!(
            container_state(&rec, Some(false)),
            ContainerState::ContainerExited
        );
        assert_eq!(
            container_state(&rec, Some(true)),
            ContainerState::ContainerRunning
        );
        // Gone from the runtime entirely, but started once → exited.
        assert_eq!(container_state(&rec, None), ContainerState::ContainerExited);

        // Never started and gone → still CREATED, not EXITED.
        let fresh = ContainerRecord::default();
        assert_eq!(
            container_state(&fresh, None),
            ContainerState::ContainerCreated
        );
    }

    #[test]
    fn port_mappings_without_a_host_port_are_not_published() {
        use crate::state::PortMappingRecord;
        let pm = |host_port, container_port, protocol, host_ip: &str| PortMappingRecord {
            protocol,
            container_port,
            host_port,
            host_ip: host_ip.into(),
        };
        let tcp = Protocol::Tcp as i32;
        let udp = Protocol::Udp as i32;

        // host_port 0 means "expose only": Apple rejects `--publish 0:80`.
        assert!(publish_specs(&[pm(0, 80, tcp, "")]).is_empty());
        assert!(publish_specs(&[pm(-1, 80, tcp, "")]).is_empty());

        assert_eq!(publish_specs(&[pm(12000, 80, tcp, "")]), ["12000:80/tcp"]);
        assert_eq!(publish_specs(&[pm(53, 53, udp, "")]), ["53:53/udp"]);
        assert_eq!(
            publish_specs(&[pm(12001, 80, tcp, "127.0.0.1")]),
            ["127.0.0.1:12001:80/tcp"]
        );
        // A mixed set keeps only the bound ones, in order.
        assert_eq!(
            publish_specs(&[pm(0, 80, tcp, ""), pm(12000, 8080, tcp, "")]),
            ["12000:8080/tcp"]
        );
    }

    #[test]
    fn cli_failures_are_told_apart_from_command_exit_codes() {
        use crate::cli::looks_like_cli_error as f;
        assert!(f(r#"Error: notFound: "container x not found""#));
        assert!(f(r#"Error: invalidState: "not running""#));
        assert!(f(
            r#"Error: invalidArgument: "attach is currently unsupported on already running containers""#
        ));
        // A command writing to stderr and exiting non-zero is a *successful*
        // ExecSync, not an RPC failure.
        assert!(!f("ls: /nope: No such file or directory\n"));
        assert!(!f(""));
    }

    #[test]
    fn log_path_joins_directory_and_relative_path() {
        let b = crate::backend::test_backend();
        assert_eq!(
            b.resolve_log_path("/var/log/pods/ns_pod_uid", "ctr/0.log"),
            "/var/log/pods/ns_pod_uid/ctr/0.log"
        );
        // An absolute log_path wins over the directory.
        assert_eq!(
            b.resolve_log_path("/var/log/pods/x", "/abs/0.log"),
            "/abs/0.log"
        );
        assert_eq!(b.resolve_log_path("", "ctr/0.log"), "ctr/0.log");
        assert_eq!(b.resolve_log_path("/var/log/pods/x", ""), "");
    }
}

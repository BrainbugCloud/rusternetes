// SPDX-License-Identifier: Apache-2.0

//! CRI over **real pod sandboxes**: one microVM per pod, N containers inside it.
//!
//! The default path in this crate ([`crate::sandbox`], [`crate::container`])
//! drives Apple's `container` CLI, which gives one microVM per *container*. That
//! is enough for critest — every conformance spec it runs is single-container —
//! but it is not a Kubernetes pod: containers get no shared `localhost`, no
//! single pod IP, and no shared IPC.
//!
//! This module is the pod-shaped path. It translates CRI configuration onto
//! [`apple_containerization::Pod`], which speaks `SandboxContext` gRPC to
//! `vminitd` directly and hosts many containers in one VM. Everything about the
//! translation — namespace modes, resources, mounts, DNS — is here; the pod
//! semantics themselves live in `apple-containerization`.
//!
//! # Status
//!
//! Not yet the default backend. It needs two host-side capabilities that
//! Virtualization.framework only exposes to the process owning the VM, both
//! behind traits here: [`apple_containerization::Vmm`] for VM lifecycle, vsock
//! and block hotplug, and [`RootfsProvider`] for turning an image reference into
//! an ext4 block device. See `STATUS.md`.

use std::collections::HashMap;
use std::sync::Arc;

use apple_containerization::agent::{DnsConfig, HostsEntry, StatCategories, Stdio};
use apple_containerization::oci;
use apple_containerization::pod::{
    ContainerConfig as PodContainerConfig, NamespaceMode, Pod, PodConfig, ProcessConfig,
};
use apple_containerization::vmm::{BlockMount, Interface, Vmm};
use async_trait::async_trait;
use cri_proto::v1::*;
use tokio::sync::Mutex;

use cri_server::error::{Error, Result};

/// Turns a container image into a block device the guest can mount as a rootfs.
///
/// This is a host-side concern: the image layers have to be unpacked into an
/// ext4 image on the host before the VM can attach it (upstream does this with
/// `ContainerizationEXT4`'s `EXT4Unpacker`, writing a per-container
/// `rootfs.ext4`). Kept behind a trait so the VMM broker owns it and this module
/// stays free of image plumbing.
#[async_trait]
pub trait RootfsProvider: Send + Sync + std::fmt::Debug {
    /// Materialise `image` as a block device for `container_id`.
    async fn provision(&self, image: &str, container_id: &str) -> Result<BlockMount>;
    /// Discard whatever `provision` created.
    async fn release(&self, container_id: &str) -> Result<()>;
}

/// Defaults for pods this runtime creates.
#[derive(Debug, Clone)]
pub struct PodRuntimeConfig {
    /// CPUs for a pod's VM when the sandbox config does not say.
    pub default_cpus: u32,
    /// Memory for a pod's VM when the sandbox config does not say.
    pub default_memory_bytes: u64,
    /// OCI runtime inside the guest.
    ///
    /// `None` selects vminitd's built-in `vmexec`, which is what upstream's
    /// `LinuxPod` uses — it passes `ociRuntimePath: nil` at all three of its
    /// `createProcess` call sites (pause, member container, exec). vmexec
    /// implements the namespace model this pod layer depends on:
    /// `vminitd/Sources/vmexec/RunCommand.swift:287` `setupNamespaces()` calls
    /// `setns(fd, flag)` for a namespace carrying a path and `unshare` for one
    /// without, which is exactly join-by-path vs. create-new.
    ///
    /// `Some(path)` makes the guest shell out to that binary instead
    /// (`vminitd/Sources/VminitdCore/ManagedContainer.swift:71` builds
    /// `Runc(command: path, root: "/run/runc")`). The path must exist *inside the
    /// guest*; Apple's init image does not ship runc, so this is opt-in for hosts
    /// that provide one, not a default.
    pub oci_runtime_path: Option<String>,
}

impl Default for PodRuntimeConfig {
    fn default() -> Self {
        Self {
            // Matches LinuxPod.Configuration's own defaults.
            default_cpus: 4,
            default_memory_bytes: 1024 * 1024 * 1024,
            // vmexec, as upstream LinuxPod does. See the field docs.
            oci_runtime_path: None,
        }
    }
}

/// A container's bookkeeping, so CRI's flat container ids can be resolved back
/// to the pod that owns them.
#[derive(Debug, Clone)]
struct ContainerEntry {
    pod_id: String,
    image: String,
    metadata: Option<ContainerMetadata>,
    created_at: i64,
}

/// CRI runtime backed by pod sandboxes.
#[derive(Debug)]
pub struct PodRuntime {
    vmm: Arc<dyn Vmm>,
    rootfs: Arc<dyn RootfsProvider>,
    config: PodRuntimeConfig,
    pods: Mutex<HashMap<String, Arc<Pod>>>,
    containers: Mutex<HashMap<String, ContainerEntry>>,
}

impl PodRuntime {
    pub fn new(
        vmm: Arc<dyn Vmm>,
        rootfs: Arc<dyn RootfsProvider>,
        config: PodRuntimeConfig,
    ) -> Self {
        Self {
            vmm,
            rootfs,
            config,
            pods: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
        }
    }

    /// Create and boot a pod sandbox.
    pub async fn run_pod_sandbox(&self, id: &str, config: &PodSandboxConfig) -> Result<String> {
        let pod_config = pod_config_from_cri(config, &self.config);
        let pod = Pod::new(id, pod_config, self.vmm.clone()).map_err(pod_err)?;
        pod.create().await.map_err(pod_err)?;
        self.pods.lock().await.insert(id.to_string(), Arc::new(pod));
        Ok(id.to_string())
    }

    /// Stop every container in the pod and shut its VM down.
    pub async fn stop_pod_sandbox(&self, id: &str) -> Result<()> {
        let pod = self.pod(id).await?;
        pod.stop().await.map_err(pod_err)
    }

    /// Remove the pod and forget its containers.
    pub async fn remove_pod_sandbox(&self, id: &str) -> Result<()> {
        // Stopping is idempotent, so a remove of a running sandbox is safe.
        if let Ok(pod) = self.pod(id).await {
            pod.stop().await.map_err(pod_err)?;
        }
        self.pods.lock().await.remove(id);
        let mut containers = self.containers.lock().await;
        let owned: Vec<String> = containers
            .iter()
            .filter(|(_, e)| e.pod_id == id)
            .map(|(cid, _)| cid.clone())
            .collect();
        for cid in owned {
            containers.remove(&cid);
            let _ = self.rootfs.release(&cid).await;
        }
        Ok(())
    }

    /// Add a container to a running pod sandbox.
    pub async fn create_container(
        &self,
        pod_id: &str,
        container_id: &str,
        config: &ContainerConfig,
        sandbox_config: &PodSandboxConfig,
    ) -> Result<String> {
        let pod = self.pod(pod_id).await?;
        let image = config
            .image
            .as_ref()
            .map(|i| i.image.clone())
            .unwrap_or_default();

        let rootfs = self.rootfs.provision(&image, container_id).await?;
        let container_config =
            container_config_from_cri(config, sandbox_config, &self.config, &rootfs);

        if let Err(e) = pod
            .add_container(container_id, rootfs, container_config)
            .await
        {
            // Don't leak the rootfs image if the guest refused the container.
            let _ = self.rootfs.release(container_id).await;
            return Err(pod_err(e));
        }

        self.containers.lock().await.insert(
            container_id.to_string(),
            ContainerEntry {
                pod_id: pod_id.to_string(),
                image,
                metadata: config.metadata.clone(),
                created_at: now_nanos(),
            },
        );
        Ok(container_id.to_string())
    }

    pub async fn start_container(&self, container_id: &str) -> Result<()> {
        let (pod, _) = self.container(container_id).await?;
        pod.start_container(container_id).await.map_err(pod_err)?;
        Ok(())
    }

    /// Stop a container, escalating to SIGKILL when the grace period is zero.
    pub async fn stop_container(&self, container_id: &str, timeout_secs: i64) -> Result<()> {
        let (pod, _) = self.container(container_id).await?;
        // CRI passes a grace period; 0 means "kill now".
        let signal = if timeout_secs == 0 { 9 } else { 15 };
        match pod.stop_container(container_id, signal).await {
            Ok(_) => Ok(()),
            // Stopping an already-stopped container is success in CRI.
            Err(apple_containerization::Error::InvalidState(_)) => Ok(()),
            Err(e) => Err(pod_err(e)),
        }
    }

    pub async fn remove_container(&self, container_id: &str) -> Result<()> {
        let (pod, _) = self.container(container_id).await?;
        pod.remove_container(container_id).await.map_err(pod_err)?;
        self.containers.lock().await.remove(container_id);
        self.rootfs.release(container_id).await?;
        Ok(())
    }

    /// Exec a command in a container and collect its exit code.
    ///
    /// `exec_id` must differ from the container id — that is how
    /// `SandboxContext` distinguishes an exec from the container's init process.
    pub async fn exec(
        &self,
        container_id: &str,
        exec_id: &str,
        cmd: &[String],
        stdio: Stdio,
    ) -> Result<i32> {
        let (pod, _) = self.container(container_id).await?;
        pod.exec(
            container_id,
            exec_id,
            ProcessConfig {
                args: cmd.to_vec(),
                cwd: "/".to_string(),
                ..Default::default()
            },
            stdio,
        )
        .await
        .map_err(pod_err)
    }

    /// Per-container stats from the guest agent.
    pub async fn container_stats(&self, container_id: &str) -> Result<Option<ContainerStats>> {
        let (pod, entry) = self.container(container_id).await?;
        let stats = pod
            .statistics(vec![container_id.to_string()], StatCategories::ALL)
            .await
            .map_err(pod_err)?;
        let Some(s) = stats.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(ContainerStats {
            attributes: Some(ContainerAttributes {
                id: container_id.to_string(),
                metadata: entry.metadata.clone(),
                ..Default::default()
            }),
            cpu: s.cpu.map(|c| CpuUsage {
                timestamp: now_nanos(),
                // CRI wants nanoseconds; the guest reports microseconds.
                usage_core_nano_seconds: Some(UInt64Value {
                    value: c.usage_usec.saturating_mul(1_000),
                }),
                ..Default::default()
            }),
            memory: s.memory.map(|m| MemoryUsage {
                timestamp: now_nanos(),
                working_set_bytes: Some(UInt64Value {
                    // Working set is usage minus inactive file cache, as cAdvisor
                    // and the kubelet compute it.
                    value: m.usage_bytes.saturating_sub(m.inactive_file),
                }),
                usage_bytes: Some(UInt64Value {
                    value: m.usage_bytes,
                }),
                rss_bytes: Some(UInt64Value { value: m.anon }),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    /// Ids of the pods this runtime owns.
    pub async fn list_pods(&self) -> Vec<String> {
        self.pods.lock().await.keys().cloned().collect()
    }

    /// Container ids in a pod.
    pub async fn list_containers(&self, pod_id: &str) -> Vec<String> {
        self.containers
            .lock()
            .await
            .iter()
            .filter(|(_, e)| e.pod_id == pod_id)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub async fn container_image(&self, container_id: &str) -> Option<String> {
        self.containers
            .lock()
            .await
            .get(container_id)
            .map(|e| e.image.clone())
    }

    pub async fn container_created_at(&self, container_id: &str) -> Option<i64> {
        self.containers
            .lock()
            .await
            .get(container_id)
            .map(|e| e.created_at)
    }

    async fn pod(&self, id: &str) -> Result<Arc<Pod>> {
        self.pods
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("pod sandbox {id} not found")))
    }

    async fn container(&self, container_id: &str) -> Result<(Arc<Pod>, ContainerEntry)> {
        let entry = self
            .containers
            .lock()
            .await
            .get(container_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("container {container_id} not found")))?;
        let pod = self.pod(&entry.pod_id).await?;
        Ok((pod, entry))
    }
}

fn pod_err(e: apple_containerization::Error) -> Error {
    use apple_containerization::Error as E;
    match e {
        E::NotFound(m) => Error::NotFound(m),
        E::InvalidArgument(m) | E::InvalidState(m) => Error::InvalidArgument(m),
        E::Unsupported(m) => Error::Unimplemented(m),
        other => Error::Internal(other.to_string()),
    }
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}

/// Map a CRI [`NamespaceMode`](cri_proto::v1::NamespaceMode) onto the pod's.
///
/// `TARGET` means "join a specific container's namespace". The pod model anchors
/// pod-scoped namespaces on the infra process rather than on an arbitrary
/// container, so `TARGET` is treated as `POD`: it is what the kubelet uses for
/// ephemeral debug containers, and joining the pod is the useful approximation.
pub fn namespace_mode_from_cri(mode: i32) -> NamespaceMode {
    match NamespaceMode::try_from_cri(mode) {
        Some(m) => m,
        None => NamespaceMode::Pod,
    }
}

/// Extension so the mapping stays a total function over the CRI enum.
trait FromCriNamespaceMode {
    fn try_from_cri(mode: i32) -> Option<NamespaceMode>;
}

impl FromCriNamespaceMode for NamespaceMode {
    fn try_from_cri(mode: i32) -> Option<NamespaceMode> {
        // CRI: POD = 0, CONTAINER = 1, NODE = 2, TARGET = 3.
        match mode {
            0 => Some(NamespaceMode::Pod),
            1 => Some(NamespaceMode::Container),
            2 => Some(NamespaceMode::Node),
            3 => Some(NamespaceMode::Pod),
            _ => None,
        }
    }
}

/// Build a [`PodConfig`] from a CRI `PodSandboxConfig`.
pub fn pod_config_from_cri(config: &PodSandboxConfig, defaults: &PodRuntimeConfig) -> PodConfig {
    let namespace_options = config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|s| s.namespace_options.as_ref());

    // Kubernetes `shareProcessNamespace` reaches the runtime as pid == POD on the
    // *sandbox*. CRI's own default for pid is POD, but the kubelet sets it to
    // CONTAINER explicitly for v1 pods that do not share, so honouring the value
    // as sent is correct.
    let share_process_namespace = namespace_options
        .map(|n| namespace_mode_from_cri(n.pid) == NamespaceMode::Pod)
        .unwrap_or(false);

    let dns = config.dns_config.as_ref().map(|d| DnsConfig {
        nameservers: d.servers.clone(),
        search_domains: d.searches.clone(),
        options: d.options.clone(),
        domain: None,
    });

    // The sandbox's hostname is the pod's, and the pod has exactly one because
    // every container shares the infra UTS namespace.
    let hostname = if config.hostname.is_empty() {
        None
    } else {
        Some(config.hostname.clone())
    };

    PodConfig {
        cpus: defaults.default_cpus,
        memory_in_bytes: defaults.default_memory_bytes,
        interfaces: Vec::new(),
        nested_virtualization: false,
        hostname,
        dns,
        hosts: Vec::new(),
        volumes: Vec::new(),
        share_process_namespace,
        boot_log: None,
    }
}

/// Attach a pod IP to a [`PodConfig`], as the CNI/vmnet result would.
pub fn with_interface(mut config: PodConfig, address: &str, gateway: Option<&str>) -> PodConfig {
    config.interfaces.push(Interface {
        address: address.to_string(),
        gateway: gateway.map(|g| g.to_string()),
        mtu: None,
        mac_address: None,
    });
    config
}

/// Add `/etc/hosts` entries for the pod.
pub fn with_hosts(mut config: PodConfig, entries: Vec<HostsEntry>) -> PodConfig {
    config.hosts = entries;
    config
}

/// Build a container config from a CRI `ContainerConfig`.
pub fn container_config_from_cri(
    config: &ContainerConfig,
    sandbox_config: &PodSandboxConfig,
    defaults: &PodRuntimeConfig,
    rootfs: &BlockMount,
) -> PodContainerConfig {
    let linux = config.linux.as_ref();
    let security = linux.and_then(|l| l.security_context.as_ref());
    let namespace_options = security.and_then(|s| s.namespace_options.as_ref());

    // CRI splits the entrypoint across `command` and `args`.
    let mut args = config.command.clone();
    args.extend(config.args.clone());

    let env = config
        .envs
        .iter()
        // CRI models env values as `bytes` (api.proto KeyValue), so decode.
        .map(|e| format!("{}={}", e.key, String::from_utf8_lossy(&e.value)))
        .collect();

    let user = security
        .map(|s| oci::User {
            uid: s.run_as_user.as_ref().map(|v| v.value as u32).unwrap_or(0),
            gid: s.run_as_group.as_ref().map(|v| v.value as u32).unwrap_or(0),
            additional_gids: s.supplemental_groups.iter().map(|g| *g as u32).collect(),
            username: s.run_as_username.clone(),
            umask: None,
        })
        .unwrap_or_default();

    let mut mounts = oci::default_mounts();
    mounts.extend(config.mounts.iter().map(mount_from_cri));

    let resources = linux.and_then(|l| l.resources.as_ref());
    let memory_in_bytes = resources
        .map(|r| r.memory_limit_in_bytes)
        .filter(|m| *m > 0)
        .map(|m| m as u64);

    // The pod layer expresses CPU as a whole-core count and derives a quota over
    // a 100ms period. CRI gives quota/period directly, so convert back to cores
    // rather than losing the limit.
    let cpus = resources.and_then(|r| {
        if r.cpu_quota > 0 && r.cpu_period > 0 {
            let cores = r.cpu_quota / r.cpu_period;
            u32::try_from(cores.max(1)).ok()
        } else {
            None
        }
    });

    let sandbox_pid_mode = sandbox_config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|s| s.namespace_options.as_ref())
        .map(|n| namespace_mode_from_cri(n.pid));

    // A container inherits the sandbox's PID mode unless it states its own.
    let pid_namespace = namespace_options
        .map(|n| namespace_mode_from_cri(n.pid))
        .or(sandbox_pid_mode)
        .unwrap_or(NamespaceMode::Container);

    // IPC is pod-scoped in Kubernetes; only an explicit NODE request opts out.
    let ipc_namespace = namespace_options
        .map(|n| namespace_mode_from_cri(n.ipc))
        .unwrap_or(NamespaceMode::Pod);

    PodContainerConfig {
        process: ProcessConfig {
            args,
            env,
            cwd: if config.working_dir.is_empty() {
                "/".to_string()
            } else {
                config.working_dir.clone()
            },
            terminal: config.tty,
            user,
            capabilities: None,
            no_new_privileges: security.map(|s| s.no_new_privs).unwrap_or(false),
            rlimits: Vec::new(),
            oom_score_adj: None,
        },
        cpus,
        memory_in_bytes,
        sysctl: HashMap::new(),
        mounts,
        masked_paths: security
            .map(|s| s.masked_paths.clone())
            .filter(|p| !p.is_empty())
            .unwrap_or_else(oci::default_masked_paths),
        readonly_paths: security
            .map(|s| s.readonly_paths.clone())
            .filter(|p| !p.is_empty())
            .unwrap_or_else(oci::default_readonly_paths),
        volume_mounts: Vec::new(),
        readonly_rootfs: security.map(|s| s.readonly_rootfs).unwrap_or(false)
            || rootfs.is_readonly(),
        pid_namespace,
        ipc_namespace,
        oci_runtime_path: defaults.oci_runtime_path.clone(),
    }
}

/// Map a CRI mount onto an OCI bind mount.
///
/// Note the host-path dependency: CRI mounts name a path on the *host*, but the
/// container runs in a VM, so the path has to be shared in over virtiofs before
/// this bind can resolve. The shape here is what the guest needs once the VMM
/// broker attaches the share; without it the guest bind fails with ENOENT.
fn mount_from_cri(mount: &Mount) -> oci::Mount {
    let mut options = vec!["rbind".to_string()];
    if mount.readonly {
        options.push("ro".to_string());
    } else {
        options.push("rw".to_string());
    }
    match MountPropagation::try_from(mount.propagation)
        .unwrap_or(MountPropagation::PropagationPrivate)
    {
        MountPropagation::PropagationBidirectional => options.push("rshared".to_string()),
        MountPropagation::PropagationHostToContainer => options.push("rslave".to_string()),
        MountPropagation::PropagationPrivate => options.push("rprivate".to_string()),
    }
    oci::Mount::new(
        "bind",
        mount.host_path.clone(),
        mount.container_path.clone(),
        options,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    // The unqualified `DnsConfig` in this module is the guest agent's; alias the
    // CRI message so both are usable in tests.
    use cri_proto::v1::DnsConfig as CriDnsConfig;

    fn sandbox_with_pid_mode(mode: i32) -> PodSandboxConfig {
        PodSandboxConfig {
            linux: Some(LinuxPodSandboxConfig {
                security_context: Some(LinuxSandboxSecurityContext {
                    namespace_options: Some(NamespaceOption {
                        pid: mode,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn cri_namespace_modes_map_onto_pod_modes() {
        assert_eq!(namespace_mode_from_cri(0), NamespaceMode::Pod);
        assert_eq!(namespace_mode_from_cri(1), NamespaceMode::Container);
        assert_eq!(namespace_mode_from_cri(2), NamespaceMode::Node);
        // TARGET approximates to POD; see the doc comment.
        assert_eq!(namespace_mode_from_cri(3), NamespaceMode::Pod);
    }

    #[test]
    fn sandbox_pid_pod_mode_enables_share_process_namespace() {
        let config = pod_config_from_cri(&sandbox_with_pid_mode(0), &PodRuntimeConfig::default());
        assert!(config.share_process_namespace);
    }

    #[test]
    fn sandbox_pid_container_mode_leaves_process_namespaces_private() {
        let config = pod_config_from_cri(&sandbox_with_pid_mode(1), &PodRuntimeConfig::default());
        assert!(!config.share_process_namespace);
    }

    #[test]
    fn absent_namespace_options_do_not_share_process_namespace() {
        let config =
            pod_config_from_cri(&PodSandboxConfig::default(), &PodRuntimeConfig::default());
        assert!(!config.share_process_namespace);
    }

    #[test]
    fn sandbox_hostname_and_dns_reach_the_pod_config() {
        let cri = PodSandboxConfig {
            hostname: "my-pod".to_string(),
            dns_config: Some(CriDnsConfig {
                servers: vec!["10.96.0.10".to_string()],
                searches: vec!["default.svc.cluster.local".to_string()],
                options: vec!["ndots:5".to_string()],
            }),
            ..Default::default()
        };
        let config = pod_config_from_cri(&cri, &PodRuntimeConfig::default());
        assert_eq!(config.hostname.as_deref(), Some("my-pod"));
        let dns = config.dns.unwrap();
        assert_eq!(dns.nameservers, vec!["10.96.0.10"]);
        assert_eq!(dns.search_domains, vec!["default.svc.cluster.local"]);
        assert_eq!(dns.options, vec!["ndots:5"]);
    }

    #[test]
    fn empty_hostname_is_none_not_empty_string() {
        // An empty hostname must not become the pod's hostname, or the infra
        // container would set the hostname to "".
        let config =
            pod_config_from_cri(&PodSandboxConfig::default(), &PodRuntimeConfig::default());
        assert!(config.hostname.is_none());
    }

    fn container_config(config: ContainerConfig) -> PodContainerConfig {
        container_config_from_cri(
            &config,
            &PodSandboxConfig::default(),
            &PodRuntimeConfig::default(),
            &BlockMount::block("ext4", "/i.ext4"),
        )
    }

    #[test]
    fn command_and_args_concatenate_into_the_process_args() {
        let out = container_config(ContainerConfig {
            command: vec!["/bin/sh".to_string()],
            args: vec!["-c".to_string(), "echo hi".to_string()],
            ..Default::default()
        });
        assert_eq!(out.process.args, vec!["/bin/sh", "-c", "echo hi"]);
    }

    #[test]
    fn envs_become_key_equals_value() {
        let out = container_config(ContainerConfig {
            envs: vec![
                KeyValue {
                    key: "A".to_string(),
                    value: b"1".to_vec(),
                },
                // An empty value must still produce `B=`, not be dropped.
                KeyValue {
                    key: "B".to_string(),
                    value: Vec::new(),
                },
            ],
            ..Default::default()
        });
        assert_eq!(out.process.env, vec!["A=1", "B="]);
    }

    #[test]
    fn empty_working_dir_defaults_to_root() {
        let out = container_config(ContainerConfig::default());
        assert_eq!(out.process.cwd, "/");
    }

    #[test]
    fn cpu_quota_and_period_convert_back_to_whole_cores() {
        let out = container_config(ContainerConfig {
            linux: Some(LinuxContainerConfig {
                resources: Some(LinuxContainerResources {
                    cpu_quota: 200_000,
                    cpu_period: 100_000,
                    memory_limit_in_bytes: 512 * 1024 * 1024,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(out.cpus, Some(2));
        assert_eq!(out.memory_in_bytes, Some(512 * 1024 * 1024));
    }

    #[test]
    fn sub_core_cpu_quota_floors_to_one_core() {
        // 100m of CPU must not become a 0-core limit, which the pod layer would
        // then drop entirely.
        let out = container_config(ContainerConfig {
            linux: Some(LinuxContainerConfig {
                resources: Some(LinuxContainerResources {
                    cpu_quota: 10_000,
                    cpu_period: 100_000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(out.cpus, Some(1));
    }

    #[test]
    fn unset_resources_stay_unset() {
        let out = container_config(ContainerConfig::default());
        assert_eq!(out.cpus, None);
        assert_eq!(out.memory_in_bytes, None);
    }

    #[test]
    fn container_inherits_sandbox_pid_mode_when_it_states_none() {
        let out = container_config_from_cri(
            &ContainerConfig::default(),
            &sandbox_with_pid_mode(0),
            &PodRuntimeConfig::default(),
            &BlockMount::block("ext4", "/i.ext4"),
        );
        assert_eq!(out.pid_namespace, NamespaceMode::Pod);
    }

    #[test]
    fn container_pid_mode_overrides_the_sandbox() {
        let out = container_config_from_cri(
            &ContainerConfig {
                linux: Some(LinuxContainerConfig {
                    security_context: Some(LinuxContainerSecurityContext {
                        namespace_options: Some(NamespaceOption {
                            pid: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &sandbox_with_pid_mode(0),
            &PodRuntimeConfig::default(),
            &BlockMount::block("ext4", "/i.ext4"),
        );
        assert_eq!(out.pid_namespace, NamespaceMode::Container);
    }

    #[test]
    fn ipc_is_pod_scoped_by_default() {
        let out = container_config(ContainerConfig::default());
        assert_eq!(out.ipc_namespace, NamespaceMode::Pod);
    }

    #[test]
    fn readonly_rootfs_from_security_context_or_the_block_device() {
        let from_context = container_config(ContainerConfig {
            linux: Some(LinuxContainerConfig {
                security_context: Some(LinuxContainerSecurityContext {
                    readonly_rootfs: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(from_context.readonly_rootfs);

        let mut ro_block = BlockMount::block("ext4", "/i.ext4");
        ro_block.options.push("ro".to_string());
        let from_block = container_config_from_cri(
            &ContainerConfig::default(),
            &PodSandboxConfig::default(),
            &PodRuntimeConfig::default(),
            &ro_block,
        );
        assert!(from_block.readonly_rootfs);
    }

    #[test]
    fn run_as_user_and_group_map_onto_the_oci_user() {
        let out = container_config(ContainerConfig {
            linux: Some(LinuxContainerConfig {
                security_context: Some(LinuxContainerSecurityContext {
                    run_as_user: Some(Int64Value { value: 1000 }),
                    run_as_group: Some(Int64Value { value: 2000 }),
                    supplemental_groups: vec![3000],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(out.process.user.uid, 1000);
        assert_eq!(out.process.user.gid, 2000);
        assert_eq!(out.process.user.additional_gids, vec![3000]);
    }

    #[test]
    fn cri_mounts_become_bind_mounts_with_propagation_options() {
        let out = container_config(ContainerConfig {
            mounts: vec![
                Mount {
                    host_path: "/host/ro".to_string(),
                    container_path: "/ctr/ro".to_string(),
                    readonly: true,
                    ..Default::default()
                },
                Mount {
                    host_path: "/host/shared".to_string(),
                    container_path: "/ctr/shared".to_string(),
                    propagation: MountPropagation::PropagationBidirectional as i32,
                    ..Default::default()
                },
            ],
            ..Default::default()
        });

        let ro = out
            .mounts
            .iter()
            .find(|m| m.destination == "/ctr/ro")
            .expect("ro mount present");
        assert_eq!(ro.type_, "bind");
        assert_eq!(ro.source, "/host/ro");
        assert!(ro.options.contains(&"ro".to_string()));
        assert!(ro.options.contains(&"rprivate".to_string()));

        let shared = out
            .mounts
            .iter()
            .find(|m| m.destination == "/ctr/shared")
            .expect("shared mount present");
        assert!(shared.options.contains(&"rw".to_string()));
        assert!(shared.options.contains(&"rshared".to_string()));
    }

    #[test]
    fn default_mounts_are_present_alongside_cri_mounts() {
        let out = container_config(ContainerConfig {
            mounts: vec![Mount {
                host_path: "/host".to_string(),
                container_path: "/ctr".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let dests: Vec<&str> = out.mounts.iter().map(|m| m.destination.as_str()).collect();
        assert!(dests.contains(&"/proc"));
        assert!(dests.contains(&"/ctr"));
    }

    #[test]
    fn vmexec_is_the_default_guest_runtime_for_pods() {
        // Upstream's LinuxPod passes ociRuntimePath: nil at every createProcess
        // call site, and vmexec is what implements join-by-path namespaces
        // (vmexec/RunCommand.swift setupNamespaces). Apple's init image ships no
        // runc, so defaulting to a runc path would fail every container start.
        let out = container_config(ContainerConfig::default());
        assert_eq!(out.oci_runtime_path, None);
    }

    #[test]
    fn an_explicit_guest_runtime_is_passed_through() {
        // Opt-in for a guest that does provide a runc binary.
        let out = container_config_from_cri(
            &ContainerConfig::default(),
            &PodSandboxConfig::default(),
            &PodRuntimeConfig {
                oci_runtime_path: Some("/usr/bin/runc".to_string()),
                ..PodRuntimeConfig::default()
            },
            &BlockMount::block("ext4", "/i.ext4"),
        );
        assert_eq!(out.oci_runtime_path.as_deref(), Some("/usr/bin/runc"));
    }

    #[test]
    fn masked_and_readonly_paths_fall_back_to_the_oci_defaults() {
        let out = container_config(ContainerConfig::default());
        assert!(out.masked_paths.contains(&"/proc/kcore".to_string()));
        assert!(out.readonly_paths.contains(&"/proc/sys".to_string()));
    }

    // ---- end to end: CRI in, SandboxContext out -------------------------

    /// A [`RootfsProvider`] that hands out a distinct fake image per container.
    /// No ext4 is built — the mock VMM only records what it was asked to attach.
    #[derive(Debug, Default)]
    struct FakeRootfs {
        released: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl RootfsProvider for FakeRootfs {
        async fn provision(&self, _image: &str, container_id: &str) -> Result<BlockMount> {
            Ok(BlockMount::block(
                "ext4",
                format!("/images/{container_id}.ext4"),
            ))
        }

        async fn release(&self, container_id: &str) -> Result<()> {
            self.released
                .lock()
                .expect("poisoned")
                .push(container_id.to_string());
            Ok(())
        }
    }

    struct Harness {
        runtime: PodRuntime,
        guest: apple_containerization::testing::FakeGuest,
        vmm: Arc<apple_containerization::testing::MockVmm>,
        rootfs: Arc<FakeRootfs>,
    }

    async fn harness() -> Harness {
        let guest = apple_containerization::testing::FakeGuest::start()
            .await
            .expect("fake guest");
        let vmm = Arc::new(apple_containerization::testing::MockVmm::new(guest.clone()));
        let rootfs = Arc::new(FakeRootfs::default());
        let runtime = PodRuntime::new(vmm.clone(), rootfs.clone(), PodRuntimeConfig::default());
        Harness {
            runtime,
            guest,
            vmm,
            rootfs,
        }
    }

    fn container(name: &str) -> ContainerConfig {
        ContainerConfig {
            metadata: Some(ContainerMetadata {
                name: name.to_string(),
                attempt: 0,
            }),
            image: Some(ImageSpec {
                image: "busybox:1.29-2".to_string(),
                ..Default::default()
            }),
            command: vec!["/bin/sleep".to_string(), "1000".to_string()],
            ..Default::default()
        }
    }

    /// The namespace path a spec joins for `type_`, if any.
    fn joined_ns(
        spec: &apple_containerization::oci::Spec,
        type_: apple_containerization::oci::LinuxNamespaceType,
    ) -> Option<String> {
        spec.linux
            .as_ref()?
            .namespaces
            .iter()
            .find(|n| n.type_ == type_)
            .map(|n| n.path.clone())
    }

    #[tokio::test]
    async fn two_cri_containers_land_in_one_vm_sharing_pod_namespaces() {
        use apple_containerization::oci::LinuxNamespaceType;
        use apple_containerization::testing::VmmCall;

        let h = harness().await;
        let sandbox = PodSandboxConfig {
            hostname: "my-pod".to_string(),
            metadata: Some(PodSandboxMetadata {
                name: "my-pod".to_string(),
                namespace: "default".to_string(),
                uid: "uid-1".to_string(),
                attempt: 0,
            }),
            ..Default::default()
        };

        h.runtime
            .run_pod_sandbox("pod-1", &sandbox)
            .await
            .expect("run sandbox");

        for name in ["app", "sidecar"] {
            h.runtime
                .create_container("pod-1", name, &container(name), &sandbox)
                .await
                .expect("create container");
            h.runtime.start_container(name).await.expect("start");
        }

        // Exactly one VM for the pod, and one hotplug per container.
        let creates = h
            .vmm
            .calls()
            .iter()
            .filter(|c| matches!(c, VmmCall::Create { .. }))
            .count();
        assert_eq!(creates, 1, "one microVM per pod, not per container");
        let hotplugs = h
            .vmm
            .calls()
            .iter()
            .filter(|c| matches!(c, VmmCall::Hotplug { .. }))
            .count();
        assert_eq!(hotplugs, 2);

        // Both containers join the *same* infra IPC and UTS namespaces — this is
        // what the CLI-backed path cannot do.
        let app = h.guest.spec_for("app").expect("app spec");
        let sidecar = h.guest.spec_for("sidecar").expect("sidecar spec");
        for type_ in [LinuxNamespaceType::Ipc, LinuxNamespaceType::Uts] {
            let a = joined_ns(&app, type_).expect("namespace present");
            let b = joined_ns(&sidecar, type_).expect("namespace present");
            assert_eq!(a, b, "{type_:?} must be shared across the pod");
            assert!(
                a.starts_with("/proc/"),
                "{type_:?} must join the infra namespace by path, got {a:?}"
            );
        }

        // No network namespace declared, so both share the VM's netns: one pod IP
        // and a working localhost between containers.
        assert!(joined_ns(&app, LinuxNamespaceType::Network).is_none());
        assert!(joined_ns(&sidecar, LinuxNamespaceType::Network).is_none());

        // The pod hostname lives on the infra container, which owns the UTS ns.
        let infra = h.guest.spec_for("pause-pod-1").expect("infra spec");
        assert_eq!(infra.hostname, "my-pod");
        assert_eq!(app.hostname, "");

        // Each container still has its own rootfs and cgroup.
        assert_eq!(app.root.as_ref().unwrap().path, "/run/container/app/rootfs");
        assert_eq!(
            app.linux.as_ref().unwrap().cgroups_path,
            "/container/pod/pod-1/app"
        );
    }

    #[tokio::test]
    async fn share_process_namespace_pod_shares_pids_across_containers() {
        use apple_containerization::oci::LinuxNamespaceType;

        let h = harness().await;
        // pid == POD on the sandbox is how shareProcessNamespace arrives.
        let sandbox = sandbox_with_pid_mode(0);
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();

        for name in ["app", "sidecar"] {
            h.runtime
                .create_container("pod-1", name, &container(name), &sandbox)
                .await
                .unwrap();
            h.runtime.start_container(name).await.unwrap();
        }

        let app = h.guest.spec_for("app").unwrap();
        let sidecar = h.guest.spec_for("sidecar").unwrap();
        let a = joined_ns(&app, LinuxNamespaceType::Pid).unwrap();
        assert!(
            a.starts_with("/proc/"),
            "expected a joined pid ns, got {a:?}"
        );
        assert_eq!(a, joined_ns(&sidecar, LinuxNamespaceType::Pid).unwrap());
    }

    #[tokio::test]
    async fn container_lifecycle_round_trips_through_the_guest() {
        use apple_containerization::testing::Call;

        let h = harness().await;
        let sandbox = PodSandboxConfig::default();
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();
        h.runtime
            .create_container("pod-1", "app", &container("app"), &sandbox)
            .await
            .unwrap();
        h.runtime.start_container("app").await.unwrap();

        // A grace period of 0 must escalate straight to SIGKILL.
        h.runtime.stop_container("app", 0).await.expect("stop");
        assert!(h.guest.calls().contains(&Call::KillProcess {
            id: "app".to_string(),
            container_id: Some("app".to_string()),
            signal: 9,
        }));

        h.runtime.remove_container("app").await.expect("remove");
        assert!(h.guest.calls().contains(&Call::Umount {
            path: "/run/container/app/rootfs".to_string()
        }));
        // The rootfs image is reclaimed, not leaked.
        assert_eq!(*h.rootfs.released.lock().unwrap(), vec!["app".to_string()]);
        assert!(h.runtime.list_containers("pod-1").await.is_empty());
    }

    #[tokio::test]
    async fn stopping_an_already_stopped_container_succeeds() {
        // CRI requires StopContainer to be idempotent.
        let h = harness().await;
        let sandbox = PodSandboxConfig::default();
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();
        h.runtime
            .create_container("pod-1", "app", &container("app"), &sandbox)
            .await
            .unwrap();
        h.runtime.start_container("app").await.unwrap();

        h.runtime.stop_container("app", 30).await.expect("first");
        h.runtime
            .stop_container("app", 30)
            .await
            .expect("second stop must succeed");
    }

    #[tokio::test]
    async fn exec_runs_in_the_container_with_a_distinct_process_id() {
        use apple_containerization::testing::Call;

        let h = harness().await;
        let sandbox = PodSandboxConfig::default();
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();
        h.runtime
            .create_container("pod-1", "app", &container("app"), &sandbox)
            .await
            .unwrap();
        h.runtime.start_container("app").await.unwrap();

        h.runtime
            .exec(
                "app",
                "exec-1",
                &["/bin/echo".to_string(), "hi".to_string()],
                Stdio::default(),
            )
            .await
            .expect("exec");

        assert!(h.guest.calls().contains(&Call::StartProcess {
            id: "exec-1".to_string(),
            container_id: Some("app".to_string()),
        }));
    }

    #[tokio::test]
    async fn removing_a_sandbox_stops_the_vm_and_reclaims_rootfs_images() {
        use apple_containerization::testing::VmmCall;

        let h = harness().await;
        let sandbox = PodSandboxConfig::default();
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();
        for name in ["app", "sidecar"] {
            h.runtime
                .create_container("pod-1", name, &container(name), &sandbox)
                .await
                .unwrap();
            h.runtime.start_container(name).await.unwrap();
        }

        h.runtime
            .remove_pod_sandbox("pod-1")
            .await
            .expect("remove sandbox");

        assert!(h.vmm.calls().contains(&VmmCall::Stop));
        let mut released = h.rootfs.released.lock().unwrap().clone();
        released.sort();
        assert_eq!(released, vec!["app".to_string(), "sidecar".to_string()]);
        assert!(h.runtime.list_pods().await.is_empty());
    }

    #[tokio::test]
    async fn container_stats_convert_guest_units_to_cri_units() {
        let h = harness().await;
        let sandbox = PodSandboxConfig::default();
        h.runtime.run_pod_sandbox("pod-1", &sandbox).await.unwrap();
        h.runtime
            .create_container("pod-1", "app", &container("app"), &sandbox)
            .await
            .unwrap();
        h.runtime.start_container("app").await.unwrap();

        let stats = h
            .runtime
            .container_stats("app")
            .await
            .expect("stats")
            .expect("some stats");
        // The fake guest reports 2048 µs of CPU and 1024 bytes of memory.
        assert_eq!(
            stats.cpu.unwrap().usage_core_nano_seconds.unwrap().value,
            2_048_000,
            "guest microseconds must become CRI nanoseconds"
        );
        assert_eq!(stats.memory.unwrap().usage_bytes.unwrap().value, 1024);
    }

    #[tokio::test]
    async fn operations_against_an_unknown_sandbox_are_not_found() {
        let h = harness().await;
        let err = h
            .runtime
            .create_container(
                "nope",
                "app",
                &container("app"),
                &PodSandboxConfig::default(),
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[test]
    fn explicit_masked_paths_override_the_defaults() {
        let out = container_config(ContainerConfig {
            linux: Some(LinuxContainerConfig {
                security_context: Some(LinuxContainerSecurityContext {
                    masked_paths: vec!["/custom".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(out.masked_paths, vec!["/custom"]);
    }
}

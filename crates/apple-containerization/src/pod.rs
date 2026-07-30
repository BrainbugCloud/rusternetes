//! One microVM per **pod**, many containers inside it.
//!
//! Ported from `Sources/Containerization/LinuxPod.swift` (containerization
//! ff44a5b, v0.40.1). Upstream marks `LinuxPod` experimental; the state machine,
//! guest paths, pause-container trick and namespace wiring here follow it
//! closely, and every deliberate divergence is called out below.
//!
//! # How one VM holds many containers
//!
//! `SandboxContext`'s process RPCs all carry an optional `containerID`
//! (`CreateProcessRequest.containerID`), and the guest keys its container table
//! by it. So a single `vminitd` can host N containers, each with its own rootfs
//! and OCI runtime invocation, and `id == containerID` marks a container's init
//! process while a distinct `id` is an exec into it.
//!
//! # Divergences from `LinuxPod`, and why
//!
//! `LinuxPod` is not a Kubernetes pod. It gives every container a **fresh**
//! `ipc` and `uts` namespace (`LinuxPod.swift:926`) and only shares `pid` when
//! `shareProcessNamespace` is set. A Kubernetes pod shares IPC and UTS across
//! all its containers unconditionally, and shares PID only when the pod asks.
//! Three consequences:
//!
//! 1. **The infra process always exists.** Upstream creates the pause container
//!    only for `shareProcessNamespace`; we always create it, because it is the
//!    anchor whose `/proc/<pid>/ns/*` paths the member containers join, and
//!    because a CRI sandbox must outlive every container in it.
//! 2. **Members join the infra `ipc` and `uts` namespaces**, giving pod-scoped
//!    SysV IPC / POSIX message queues and one shared hostname.
//! 3. **A member container must not carry `hostname`.** The OCI runtime can only
//!    set a hostname in a UTS namespace it created; runc errors out if `hostname`
//!    is set while the UTS namespace is inherited. So the pod hostname goes on
//!    the infra spec, and member specs clear it. This is why upstream could set
//!    per-container hostnames and we cannot — they weren't sharing UTS.
//!
//! Networking needs no namespace entry at all: the VM *is* the pod's network, so
//! omitting the `network` namespace leaves every container in the VM's root
//! netns — one IP, shared `localhost`. Upstream relies on the same property.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::agent::{
    Agent, ContainerStatistics, DnsConfig, ExitStatus, HostsEntry, StatCategories, Stdio,
};
use crate::error::{Error, Result};
use crate::oci::{self, LinuxNamespace, LinuxNamespaceType};
use crate::vmm::{
    AttachedFilesystem, BlockMount, Interface, PortAllocator, VmConfig, VmInstance, Vmm,
};

/// Upstream's `LinuxPod.maxIDLength`.
pub const MAX_ID_LENGTH: usize = 64;

/// How a container gets a namespace, mirroring CRI's `NamespaceMode`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NamespaceMode {
    /// A fresh namespace for this container alone.
    #[default]
    Container,
    /// Join the pod's namespace (the infra container's).
    Pod,
    /// The VM's root namespace.
    ///
    /// There is no macOS host namespace to share — the pod's VM is the closest
    /// thing to a "node" a guest process can see — so `Node` means the VM root.
    /// Documented rather than silently treated as `Pod`.
    Node,
}

/// The process a container should run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessConfig {
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub cwd: String,
    pub terminal: bool,
    pub user: oci::User,
    pub capabilities: Option<oci::LinuxCapabilities>,
    pub no_new_privileges: bool,
    pub rlimits: Vec<oci::PosixRlimit>,
    pub oom_score_adj: Option<i64>,
}

impl ProcessConfig {
    fn to_oci(&self) -> oci::Process {
        oci::Process {
            args: self.args.clone(),
            env: self.env.clone(),
            cwd: if self.cwd.is_empty() {
                "/".to_string()
            } else {
                self.cwd.clone()
            },
            terminal: self.terminal,
            user: self.user.clone(),
            capabilities: self.capabilities.clone(),
            no_new_privileges: self.no_new_privileges,
            rlimits: self.rlimits.clone(),
            oom_score_adj: self.oom_score_adj,
            ..Default::default()
        }
    }
}

/// Per-container configuration, mirroring `LinuxPod.ContainerConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerConfig {
    pub process: ProcessConfig,
    pub cpus: Option<u32>,
    pub memory_in_bytes: Option<u64>,
    pub sysctl: HashMap<String, String>,
    /// Guest mounts for this container, on top of [`oci::default_mounts`].
    pub mounts: Vec<oci::Mount>,
    pub masked_paths: Vec<String>,
    pub readonly_paths: Vec<String>,
    /// Pod volumes to bind in, by volume name → destination.
    pub volume_mounts: Vec<VolumeMount>,
    pub readonly_rootfs: bool,
    pub pid_namespace: NamespaceMode,
    pub ipc_namespace: NamespaceMode,
    /// OCI runtime in the guest. `None` uses vminitd's built-in `vmexec`, which
    /// is what upstream's `LinuxPod` passes and which implements join-by-path
    /// namespaces (`vmexec/RunCommand.swift` `setupNamespaces`). `Some(path)`
    /// shells out to that binary, which must exist inside the guest.
    pub oci_runtime_path: Option<String>,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self {
            process: ProcessConfig::default(),
            cpus: None,
            memory_in_bytes: None,
            sysctl: HashMap::new(),
            mounts: oci::default_mounts(),
            masked_paths: oci::default_masked_paths(),
            readonly_paths: oci::default_readonly_paths(),
            volume_mounts: Vec::new(),
            readonly_rootfs: false,
            // CRI's default for a container is its own PID namespace; IPC is
            // pod-scoped. Matches kubelet's NamespaceOption defaults.
            pid_namespace: NamespaceMode::Container,
            ipc_namespace: NamespaceMode::Pod,
            oci_runtime_path: None,
        }
    }
}

/// A pod volume bound into a container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMount {
    /// Name of a volume declared in [`PodConfig::volumes`].
    pub name: String,
    pub destination: String,
    pub options: Vec<String>,
}

/// A pod-level volume, shared by containers that mount it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodVolume {
    pub name: String,
    pub source: BlockMount,
}

/// Pod-level configuration, mirroring `LinuxPod.Configuration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodConfig {
    pub cpus: u32,
    pub memory_in_bytes: u64,
    pub interfaces: Vec<Interface>,
    pub nested_virtualization: bool,
    pub hostname: Option<String>,
    pub dns: Option<DnsConfig>,
    pub hosts: Vec<HostsEntry>,
    pub volumes: Vec<PodVolume>,
    /// Kubernetes `shareProcessNamespace`. When set, containers whose
    /// `pid_namespace` is [`NamespaceMode::Pod`] join the infra PID namespace.
    pub share_process_namespace: bool,
    pub boot_log: Option<std::path::PathBuf>,
}

impl Default for PodConfig {
    fn default() -> Self {
        Self {
            cpus: 4,
            memory_in_bytes: 1024 * 1024 * 1024,
            interfaces: Vec::new(),
            nested_virtualization: false,
            hostname: None,
            dns: None,
            hosts: Vec::new(),
            volumes: Vec::new(),
            share_process_namespace: false,
            boot_log: None,
        }
    }
}

/// Lifecycle state of a container within the pod, mirroring upstream's
/// `PodContainer.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerState {
    /// Added before the pod was created; its rootfs attaches at boot.
    Registered,
    /// Rootfs attached and mounted, process not yet started.
    Created,
    Started,
    Stopped,
}

#[derive(Debug, Clone)]
struct PodContainer {
    id: String,
    rootfs: BlockMount,
    config: ContainerConfig,
    state: ContainerState,
    /// Guest PID once started.
    pid: Option<i32>,
}

#[derive(Debug)]
enum Phase {
    Initialized,
    Created(CreatedState),
    Stopped,
}

#[derive(Debug)]
struct CreatedState {
    vm: Arc<dyn VmInstance>,
    agent: Agent,
    /// PID of the infra process, whose namespaces members join.
    infra_pid: i32,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    containers: HashMap<String, PodContainer>,
}

/// A pod: one microVM, many containers.
#[derive(Debug)]
pub struct Pod {
    id: String,
    config: PodConfig,
    vmm: Arc<dyn Vmm>,
    state: Mutex<State>,
    stdio_ports: PortAllocator,
}

impl Pod {
    /// Guest path a container's rootfs is mounted at.
    ///
    /// `LinuxPod.guestRootfsPath` — `/run/container/<id>/rootfs`.
    pub fn guest_rootfs_path(container_id: &str) -> String {
        format!("/run/container/{container_id}/rootfs")
    }

    /// `LinuxPod.guestVolumePath` — `/run/volumes/<name>`.
    pub fn guest_volume_path(volume_name: &str) -> String {
        format!("/run/volumes/{volume_name}")
    }

    /// The infra ("pause") container's id for a pod.
    ///
    /// `LinuxPod.create()` uses `pause-<podID>`; we keep the same name so a
    /// guest inspected by hand looks the same as upstream's.
    pub fn infra_id(pod_id: &str) -> String {
        format!("pause-{pod_id}")
    }

    /// cgroup path for a container, `LinuxPod.createDefaultRuntimeSpec`.
    fn cgroups_path(pod_id: &str, container_id: &str) -> String {
        format!("/container/pod/{pod_id}/{container_id}")
    }

    pub fn new(id: impl Into<String>, config: PodConfig, vmm: Arc<dyn Vmm>) -> Result<Self> {
        let id = id.into();
        if id.len() > MAX_ID_LENGTH {
            return Err(Error::InvalidArgument(format!(
                "pod id length {} exceeds maximum of {MAX_ID_LENGTH} characters",
                id.len()
            )));
        }
        Ok(Self {
            id,
            config,
            vmm,
            state: Mutex::new(State {
                phase: Phase::Initialized,
                containers: HashMap::new(),
            }),
            stdio_ports: PortAllocator::default(),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn config(&self) -> &PodConfig {
        &self.config
    }

    /// The default runtime spec for a container, `LinuxPod.createDefaultRuntimeSpec`.
    fn default_runtime_spec(container_id: &str, pod_id: &str) -> oci::Spec {
        oci::Spec {
            process: Some(oci::Process::default()),
            hostname: container_id.to_string(),
            root: Some(oci::Root {
                path: Self::guest_rootfs_path(container_id),
                readonly: false,
            }),
            linux: Some(oci::Linux {
                resources: Some(oci::LinuxResources::default()),
                cgroups_path: Self::cgroups_path(pod_id, container_id),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Build a member container's spec, `LinuxPod.generateRuntimeSpec` plus the
    /// Kubernetes namespace handling described in the module docs.
    fn generate_runtime_spec(&self, container: &PodContainer, infra_pid: i32) -> oci::Spec {
        let mut spec = Self::default_runtime_spec(&container.id, &self.id);
        let config = &container.config;

        spec.process = Some(config.process.to_oci());

        let mut namespaces = vec![
            LinuxNamespace::new(LinuxNamespaceType::Cgroup),
            LinuxNamespace::new(LinuxNamespaceType::Mount),
        ];

        // IPC: pod-scoped by default in Kubernetes.
        match config.ipc_namespace {
            NamespaceMode::Pod => namespaces.push(LinuxNamespace::joining(
                LinuxNamespaceType::Ipc,
                Self::ns_path(infra_pid, LinuxNamespaceType::Ipc),
            )),
            NamespaceMode::Container => {
                namespaces.push(LinuxNamespace::new(LinuxNamespaceType::Ipc))
            }
            // The VM root IPC namespace: inherit by omitting the entry.
            NamespaceMode::Node => {}
        }

        // UTS is always pod-scoped, so the pod has exactly one hostname. Because
        // the namespace is inherited, `hostname` must be cleared or the OCI
        // runtime rejects the spec — see the module docs.
        namespaces.push(LinuxNamespace::joining(
            LinuxNamespaceType::Uts,
            Self::ns_path(infra_pid, LinuxNamespaceType::Uts),
        ));
        spec.hostname = String::new();

        // PID: shared only when the pod asked for it.
        match config.pid_namespace {
            NamespaceMode::Pod if self.config.share_process_namespace => {
                namespaces.push(LinuxNamespace::joining(
                    LinuxNamespaceType::Pid,
                    Self::ns_path(infra_pid, LinuxNamespaceType::Pid),
                ));
            }
            NamespaceMode::Node => {}
            // A `Pod` request without `share_process_namespace` degrades to a
            // private namespace rather than silently sharing.
            _ => namespaces.push(LinuxNamespace::new(LinuxNamespaceType::Pid)),
        }

        // Deliberately no `network` namespace: the VM's root netns is the pod
        // network, so inheriting it is what gives one pod IP and shared
        // localhost.

        let linux = spec.linux.as_mut().expect("default spec has linux");
        linux.namespaces = namespaces;
        linux.masked_paths = config.masked_paths.clone();
        linux.readonly_paths = config.readonly_paths.clone();
        if !config.sysctl.is_empty() {
            linux.sysctl = Some(config.sysctl.clone());
        }

        // Resource limits, `generateRuntimeSpec`. cpus → a quota over the
        // standard 100ms period.
        let resources = linux.resources.get_or_insert_with(Default::default);
        if let Some(cpus) = config.cpus.filter(|c| *c > 0) {
            resources.cpu = Some(oci::LinuxCpu {
                quota: Some(i64::from(cpus) * 100_000),
                period: Some(100_000),
                ..Default::default()
            });
        }
        if let Some(memory) = config.memory_in_bytes.filter(|m| *m > 0) {
            resources.memory = Some(oci::LinuxMemory {
                limit: Some(memory as i64),
                ..Default::default()
            });
        }

        // Upstream lets the OCI runtime remount ro rather than attaching the
        // block device read-only.
        if let Some(root) = spec.root.as_mut() {
            root.readonly = config.readonly_rootfs || container.rootfs.is_readonly();
        }

        spec
    }

    fn ns_path(pid: i32, type_: LinuxNamespaceType) -> String {
        format!("/proc/{pid}/ns/{}", type_.procfs_name())
    }

    /// The infra container's spec, ported from the pause block of
    /// `LinuxPod.create()`.
    ///
    /// It runs `/sbin/vminitd pause` — the guest agent's own pause mode — out of
    /// a rootfs that is nothing but a bind mount of the guest's `/sbin`, so no
    /// image is needed for it. Unlike upstream it *creates* the UTS namespace and
    /// carries the pod hostname, because member containers now join it.
    fn infra_spec(&self, infra_id: &str) -> oci::Spec {
        let mut spec = Self::default_runtime_spec(infra_id, &self.id);
        spec.process = Some(oci::Process::with_args(vec![
            "/sbin/vminitd".to_string(),
            "pause".to_string(),
        ]));
        spec.hostname = self.config.hostname.clone().unwrap_or_default();
        spec.mounts = oci::default_mounts();

        let linux = spec.linux.as_mut().expect("default spec has linux");
        linux.namespaces = vec![
            LinuxNamespace::new(LinuxNamespaceType::Cgroup),
            LinuxNamespace::new(LinuxNamespaceType::Ipc),
            LinuxNamespace::new(LinuxNamespaceType::Mount),
            LinuxNamespace::new(LinuxNamespaceType::Pid),
            LinuxNamespace::new(LinuxNamespaceType::Uts),
        ];
        spec
    }

    /// Register a container with the pod.
    ///
    /// Before [`Pod::create`] the rootfs is attached at boot; afterwards it is
    /// hotplugged into the running VM and mounted immediately — the path CRI
    /// always takes, since the kubelet creates containers after the sandbox is
    /// up. Ported from `LinuxPod.addContainer`.
    pub async fn add_container(
        &self,
        container_id: impl Into<String>,
        rootfs: BlockMount,
        config: ContainerConfig,
    ) -> Result<()> {
        let container_id = container_id.into();
        if container_id.len() > MAX_ID_LENGTH {
            return Err(Error::InvalidArgument(format!(
                "container id length {} exceeds maximum of {MAX_ID_LENGTH} characters",
                container_id.len()
            )));
        }

        for volume_mount in &config.volume_mounts {
            if !self
                .config
                .volumes
                .iter()
                .any(|v| v.name == volume_mount.name)
            {
                return Err(Error::InvalidArgument(format!(
                    "container {container_id} references unknown pod volume \"{}\"",
                    volume_mount.name
                )));
            }
        }

        let mut state = self.state.lock().await;
        if state.containers.contains_key(&container_id) {
            return Err(Error::InvalidArgument(format!(
                "container with id {container_id} already exists in pod"
            )));
        }

        match &state.phase {
            Phase::Initialized => {
                state.containers.insert(
                    container_id.clone(),
                    PodContainer {
                        id: container_id,
                        rootfs,
                        config,
                        state: ContainerState::Registered,
                        pid: None,
                    },
                );
                Ok(())
            }
            Phase::Created(created) => {
                // Strip `ro`: the block device attaches rw and the OCI runtime
                // remounts read-only from `root.readonly`. Keeping `ro` here
                // would make the guest mount fail before the runtime ever ran.
                let mut attach = rootfs.clone();
                attach.options.retain(|o| o != "ro");

                let attachment = created.vm.hotplug(attach, &container_id).await?;
                let mut mount = attachment.to_mount();
                mount.destination = Self::guest_rootfs_path(&container_id);

                if let Err(e) = created.agent.mount(&mount).await {
                    // Don't leak the block device if the guest mount failed.
                    let _ = created.vm.release_hotplug(&container_id).await;
                    return Err(e);
                }
                created
                    .vm
                    .register_mounts(&container_id, attachment, Vec::new())
                    .await?;

                state.containers.insert(
                    container_id.clone(),
                    PodContainer {
                        id: container_id,
                        rootfs,
                        config,
                        state: ContainerState::Created,
                        pid: None,
                    },
                );
                Ok(())
            }
            Phase::Stopped => Err(Error::InvalidState(
                "cannot add a container to a stopped pod".to_string(),
            )),
        }
    }

    /// Boot the pod's VM and bring the sandbox up.
    ///
    /// Ported from `LinuxPod.create()`: build `mountsByID`, create and start the
    /// VM, run the guest's standard setup, configure the network, start the infra
    /// process, then mount each boot-time container's rootfs.
    pub async fn create(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        match state.phase {
            Phase::Initialized => {}
            Phase::Created(_) => {
                return Err(Error::InvalidState("pod already created".to_string()))
            }
            Phase::Stopped => return Err(Error::InvalidState("pod is stopped".to_string())),
        }

        let mut names = std::collections::HashSet::new();
        for volume in &self.config.volumes {
            if !names.insert(&volume.name) {
                return Err(Error::InvalidArgument(format!(
                    "duplicate pod volume name \"{}\"",
                    volume.name
                )));
            }
        }

        let mut vm_config = VmConfig::new(self.id.clone());
        vm_config.cpus = self.config.cpus;
        vm_config.memory_in_bytes = self.config.memory_in_bytes;
        vm_config.interfaces = self.config.interfaces.clone();
        vm_config.nested_virtualization = self.config.nested_virtualization;
        vm_config.boot_log = self.config.boot_log.clone();

        for container in state.containers.values() {
            let mut rootfs = container.rootfs.clone();
            rootfs.options.retain(|o| o != "ro");
            vm_config
                .mounts_by_id
                .insert(container.id.clone(), vec![rootfs]);
        }
        if !self.config.volumes.is_empty() {
            // Pod-level volumes are owned by the pod id, as upstream keys them.
            vm_config.mounts_by_id.insert(
                self.id.clone(),
                self.config
                    .volumes
                    .iter()
                    .map(|v| v.source.clone())
                    .collect(),
            );
        }

        let vm = self.vmm.create(vm_config).await?;
        vm.start().await?;

        let agent = vm.dial_agent().await?;
        agent.standard_setup().await?;

        self.configure_network(&agent).await?;

        // The infra process must exist before any member container, since member
        // specs reference /proc/<infra_pid>/ns/*.
        let infra_id = Self::infra_id(&self.id);
        let infra_rootfs = Self::guest_rootfs_path(&infra_id);
        // `/sbin` is where the guest agent lives, and it is all the pause process
        // needs; bind it in rather than attaching an image.
        agent
            .mount(&oci::Mount::new(
                "",
                "/sbin",
                format!("{infra_rootfs}/sbin"),
                vec!["bind".to_string()],
            ))
            .await?;

        let infra_spec = self.infra_spec(&infra_id);
        agent
            .create_process(
                &infra_id,
                Some(&infra_id),
                Stdio::default(),
                None,
                &infra_spec,
                None,
            )
            .await?;
        let infra_pid = agent.start_process(&infra_id, Some(&infra_id)).await?;
        tracing::debug!(pod = %self.id, infra_pid, "pod infra process started");

        // Mount the rootfs of every container registered before boot.
        let attachments = vm.mounts().await;
        for container in state.containers.values_mut() {
            let attachment = attachments
                .get(&container.id)
                .and_then(|a| a.first())
                .ok_or_else(|| {
                    Error::NotFound(format!(
                        "rootfs mount not found for container {}",
                        container.id
                    ))
                })?;
            let mut mount = attachment.to_mount();
            mount.destination = Self::guest_rootfs_path(&container.id);
            agent.mount(&mount).await?;
            container.state = ContainerState::Created;
        }

        // Pod volumes are mounted once, at /run/volumes/<name>, and bind-mounted
        // into each container that asks for them.
        let pod_attachments = attachments.get(&self.id).cloned().unwrap_or_default();
        for (index, volume) in self.config.volumes.iter().enumerate() {
            let attachment = pod_attachments.get(index).ok_or_else(|| {
                Error::NotFound(format!(
                    "attached filesystem not found for pod volume \"{}\"",
                    volume.name
                ))
            })?;
            let mut mount = attachment.to_mount();
            mount.destination = Self::guest_volume_path(&volume.name);
            agent.mkdir(&mount.destination, true, 0o755).await?;
            agent.mount(&mount).await?;
        }

        state.phase = Phase::Created(CreatedState {
            vm,
            agent,
            infra_pid,
        });
        Ok(())
    }

    /// Configure the guest's network, DNS and hosts file.
    ///
    /// The VM has one netns shared by every container, so this runs once per pod
    /// rather than once per container — the pod IP *is* the VM's IP.
    async fn configure_network(&self, agent: &Agent) -> Result<()> {
        // eth0 is the first interface the guest kernel enumerates for the single
        // virtio-net device the broker attaches.
        for (index, interface) in self.config.interfaces.iter().enumerate() {
            let name = format!("eth{index}");
            agent.up(&name, interface.mtu).await?;
            agent.address_add(&name, &interface.address, None).await?;
            if let Some(gateway) = &interface.gateway {
                agent.route_add_default(&name, gateway).await?;
            }
        }
        if let Some(dns) = &self.config.dns {
            agent.configure_dns(dns, "/etc/resolv.conf").await?;
        }
        if !self.config.hosts.is_empty() {
            agent
                .configure_hosts(&self.config.hosts, "/etc/hosts")
                .await?;
        }
        Ok(())
    }

    /// Start a container's init process. Ported from `LinuxPod.startContainer`.
    pub async fn start_container(&self, container_id: &str) -> Result<i32> {
        let mut state = self.state.lock().await;
        let (vm, agent, infra_pid) = match &state.phase {
            Phase::Created(c) => (c.vm.clone(), c.agent.clone(), c.infra_pid),
            _ => {
                return Err(Error::InvalidState(
                    "startContainer requires a created pod".to_string(),
                ))
            }
        };

        let container = state
            .containers
            .get(container_id)
            .ok_or_else(|| Error::NotFound(format!("container {container_id} not found in pod")))?
            .clone();

        if container.state != ContainerState::Created {
            return Err(Error::InvalidState(format!(
                "container {container_id} must be in created state to start, is {:?}",
                container.state
            )));
        }

        let mut spec = self.generate_runtime_spec(&container, infra_pid);
        spec.mounts = self.assemble_mounts(&container, &vm.mounts().await).await;

        let stdio = Stdio {
            stdin: None,
            stdout: Some(self.stdio_ports.allocate()),
            stderr: Some(self.stdio_ports.allocate()),
        };

        agent
            .create_process(
                container_id,
                Some(container_id),
                stdio,
                container.config.oci_runtime_path.as_deref(),
                &spec,
                None,
            )
            .await?;
        let pid = agent
            .start_process(container_id, Some(container_id))
            .await?;

        if let Some(entry) = state.containers.get_mut(container_id) {
            entry.state = ContainerState::Started;
            entry.pid = Some(pid);
        }
        Ok(pid)
    }

    /// Assemble a container's mount list.
    ///
    /// Ported from `LinuxPod.startContainer`: drop the rootfs (element 0 — the
    /// OCI runtime gets it via `root.path`, and runtimes reject it as a mount),
    /// keep the rest, then add pod-volume bind mounts.
    async fn assemble_mounts(
        &self,
        container: &PodContainer,
        vm_mounts: &HashMap<String, Vec<AttachedFilesystem>>,
    ) -> Vec<oci::Mount> {
        let mut mounts = container.config.mounts.clone();

        if let Some(attached) = vm_mounts.get(&container.id) {
            mounts.extend(attached.iter().skip(1).map(|a| a.to_mount()));
        }

        for volume_mount in &container.config.volume_mounts {
            let mut options = vec!["bind".to_string()];
            options.extend(volume_mount.options.clone());
            mounts.push(oci::Mount::new(
                "none",
                Self::guest_volume_path(&volume_mount.name),
                volume_mount.destination.clone(),
                options,
            ));
        }

        oci::clean_and_sort_mounts(mounts)
    }

    /// Signal a container's init process.
    pub async fn kill_container(&self, container_id: &str, signal: i32) -> Result<()> {
        let agent = self.created_agent().await?;
        agent
            .signal_process(container_id, Some(container_id), signal)
            .await?;
        Ok(())
    }

    /// Wait for a container's init process to exit.
    pub async fn wait_container(&self, container_id: &str) -> Result<ExitStatus> {
        let agent = self.created_agent().await?;
        agent.wait_process(container_id, Some(container_id)).await
    }

    /// Stop a container: signal, wait, then delete its guest process.
    ///
    /// Ported from `LinuxPod.stopContainer`, which signals and waits before
    /// deleting so the guest can reap the process.
    pub async fn stop_container(&self, container_id: &str, signal: i32) -> Result<ExitStatus> {
        let agent = self.created_agent().await?;
        {
            let state = self.state.lock().await;
            let container = state.containers.get(container_id).ok_or_else(|| {
                Error::NotFound(format!("container {container_id} not found in pod"))
            })?;
            if container.state == ContainerState::Stopped {
                return Err(Error::InvalidState(format!(
                    "container {container_id} is already stopped"
                )));
            }
        }

        agent
            .signal_process(container_id, Some(container_id), signal)
            .await?;
        let status = agent.wait_process(container_id, Some(container_id)).await?;
        agent
            .delete_process(container_id, Some(container_id))
            .await?;

        let mut state = self.state.lock().await;
        if let Some(entry) = state.containers.get_mut(container_id) {
            entry.state = ContainerState::Stopped;
        }
        Ok(status)
    }

    /// Unmount and detach a container's rootfs, removing it from the pod.
    pub async fn remove_container(&self, container_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let (vm, agent) = match &state.phase {
            Phase::Created(c) => (c.vm.clone(), c.agent.clone()),
            _ => {
                return Err(Error::InvalidState(
                    "removeContainer requires a created pod".to_string(),
                ))
            }
        };
        if !state.containers.contains_key(container_id) {
            return Err(Error::NotFound(format!(
                "container {container_id} not found in pod"
            )));
        }

        // Best-effort: the guest may already have torn these down, and a failure
        // here must not strand the container in the pod's table.
        let _ = agent
            .umount(&Self::guest_rootfs_path(container_id), 0)
            .await;
        let _ = vm.release_hotplug(container_id).await;
        state.containers.remove(container_id);
        Ok(())
    }

    /// Exec a process inside a running container.
    ///
    /// `exec_id` must differ from `container_id`: same `containerID` with a
    /// different process `id` is exactly how `SandboxContext` models an exec.
    pub async fn exec(
        &self,
        container_id: &str,
        exec_id: &str,
        process: ProcessConfig,
        stdio: Stdio,
    ) -> Result<i32> {
        if exec_id == container_id {
            return Err(Error::InvalidArgument(
                "exec id must differ from the container id".to_string(),
            ));
        }
        let state = self.state.lock().await;
        let (agent, infra_pid) = match &state.phase {
            Phase::Created(c) => (c.agent.clone(), c.infra_pid),
            _ => {
                return Err(Error::InvalidState(
                    "exec requires a created pod".to_string(),
                ))
            }
        };
        let container = state
            .containers
            .get(container_id)
            .ok_or_else(|| Error::NotFound(format!("container {container_id} not found in pod")))?
            .clone();
        if container.state != ContainerState::Started {
            return Err(Error::InvalidState(format!(
                "container {container_id} must be running to exec"
            )));
        }
        drop(state);

        // An exec inherits the container's spec and only replaces the process,
        // so it lands in the same namespaces, cgroup and rootfs.
        let mut spec = self.generate_runtime_spec(&container, infra_pid);
        spec.process = Some(process.to_oci());

        let runtime = container.config.oci_runtime_path.clone();
        agent
            .create_process(
                exec_id,
                Some(container_id),
                stdio,
                runtime.as_deref(),
                &spec,
                None,
            )
            .await?;
        agent.start_process(exec_id, Some(container_id)).await
    }

    /// Resize an exec's or container's tty.
    pub async fn resize(
        &self,
        container_id: &str,
        process_id: &str,
        columns: u32,
        rows: u32,
    ) -> Result<()> {
        let agent = self.created_agent().await?;
        agent
            .resize_process(process_id, Some(container_id), columns, rows)
            .await
    }

    /// Close a process's stdin.
    pub async fn close_stdin(&self, container_id: &str, process_id: &str) -> Result<()> {
        let agent = self.created_agent().await?;
        agent
            .close_process_stdin(process_id, Some(container_id))
            .await
    }

    /// Statistics for some or all containers in the pod.
    pub async fn statistics(
        &self,
        container_ids: Vec<String>,
        categories: StatCategories,
    ) -> Result<Vec<ContainerStatistics>> {
        let agent = self.created_agent().await?;
        agent.container_statistics(container_ids, categories).await
    }

    /// Ids of every container in the pod, excluding the infra process.
    pub async fn list_containers(&self) -> Vec<String> {
        self.state.lock().await.containers.keys().cloned().collect()
    }

    pub async fn container_state(&self, container_id: &str) -> Option<ContainerState> {
        self.state
            .lock()
            .await
            .containers
            .get(container_id)
            .map(|c| c.state)
    }

    pub async fn container_pid(&self, container_id: &str) -> Option<i32> {
        self.state
            .lock()
            .await
            .containers
            .get(container_id)
            .and_then(|c| c.pid)
    }

    /// Dial a vsock port in the pod's VM — the transport port-forward rides on.
    pub async fn dial(&self, port: u32) -> Result<tokio::net::UnixStream> {
        let state = self.state.lock().await;
        match &state.phase {
            Phase::Created(c) => c.vm.dial(port).await,
            _ => Err(Error::InvalidState(
                "dial requires a created pod".to_string(),
            )),
        }
    }

    /// Tear the pod down: stop running containers, then stop the VM.
    ///
    /// Ported from `LinuxPod.stop()`. Container teardown is best-effort — the VM
    /// going away takes every guest process with it, so a container that will not
    /// stop cleanly must not block the pod from being reclaimed.
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let (vm, agent) = match &state.phase {
            Phase::Created(c) => (c.vm.clone(), c.agent.clone()),
            Phase::Stopped => return Ok(()),
            Phase::Initialized => {
                state.phase = Phase::Stopped;
                return Ok(());
            }
        };

        let running: Vec<String> = state
            .containers
            .iter()
            .filter(|(_, c)| c.state == ContainerState::Started)
            .map(|(id, _)| id.clone())
            .collect();

        for id in running {
            // SIGKILL: stop() is the terminal path, and the VM is about to go.
            let _ = agent.signal_process(&id, Some(&id), 9).await;
            let _ = agent.wait_process(&id, Some(&id)).await;
            if let Some(entry) = state.containers.get_mut(&id) {
                entry.state = ContainerState::Stopped;
            }
        }

        let infra_id = Self::infra_id(&self.id);
        let _ = agent.signal_process(&infra_id, Some(&infra_id), 9).await;

        vm.stop().await?;
        state.phase = Phase::Stopped;
        Ok(())
    }

    async fn created_agent(&self) -> Result<Agent> {
        let state = self.state.lock().await;
        match &state.phase {
            Phase::Created(c) => Ok(c.agent.clone()),
            _ => Err(Error::InvalidState(
                "operation requires a created pod".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_paths_match_upstream() {
        assert_eq!(Pod::guest_rootfs_path("c1"), "/run/container/c1/rootfs");
        assert_eq!(Pod::guest_volume_path("data"), "/run/volumes/data");
        assert_eq!(Pod::infra_id("pod-1"), "pause-pod-1");
        assert_eq!(Pod::cgroups_path("pod-1", "c1"), "/container/pod/pod-1/c1");
    }

    #[test]
    fn ns_path_uses_procfs_names() {
        assert_eq!(Pod::ns_path(42, LinuxNamespaceType::Pid), "/proc/42/ns/pid");
        // network → net is the one name that differs from the OCI spelling.
        assert_eq!(
            Pod::ns_path(42, LinuxNamespaceType::Network),
            "/proc/42/ns/net"
        );
        assert_eq!(
            Pod::ns_path(42, LinuxNamespaceType::Mount),
            "/proc/42/ns/mnt"
        );
    }
}

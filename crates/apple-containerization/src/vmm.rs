//! The VMM boundary: host-side operations that require Virtualization.framework.
//!
//! Ported from `VirtualMachineManager` / `VirtualMachineInstance`
//! (`Sources/Containerization/VirtualMachineManager.swift`,
//! `VirtualMachineInstance.swift`, containerization ff44a5b).
//!
//! # Why this is a trait and not an implementation
//!
//! Everything a pod does *inside* the guest is `SandboxContext` gRPC, which is
//! pure Rust ([`crate::agent`]). Four things are not, because macOS only exposes
//! them through Virtualization.framework, to the process that owns the
//! `VZVirtualMachine` object:
//!
//! 1. **VM lifecycle** — `VZVirtualMachine.start()/stop()`.
//! 2. **vsock** — a guest connection comes from `VZVirtioSocketDevice.connect(toPort:)`
//!    on the in-process VM object. No other process can dial that guest, which is
//!    why the agent channel has to come from here.
//! 3. **Block hotplug** — attaching a container's rootfs image to a *running* VM.
//!    This is what makes CRI's "add a container to a live sandbox" possible;
//!    `LinuxPod.addContainer` calls `vm.hotplug(rootfs, id:)` for exactly this.
//! 4. **virtiofs shares** — host directories into the guest.
//!
//! So a broker process owns the VM and exposes these four capabilities; the pod
//! logic, the OCI specs, the namespace wiring and the process lifecycle all stay
//! in Rust. Each vsock port is surfaced to us as a host unix socket, which is a
//! faithful shape: upstream's own `Vminitd.init(connection: FileHandle,…)` wraps
//! an already-connected socket fd rather than dialing anything itself.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

use crate::agent::{Agent, AGENT_VSOCK_PORT};
use crate::error::{Error, Result};
use crate::oci;

/// Runtime state of a VM, mirroring `VirtualMachineInstanceState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VmState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Unknown,
}

/// A filesystem to attach to the VM, mirroring containerization's `Mount` in its
/// host-side (pre-attach) form.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockMount {
    /// Filesystem format, e.g. `ext4`.
    pub format: String,
    /// Host path of the disk image.
    pub source: PathBuf,
    /// Where it should end up in the guest. The pod overwrites this with the
    /// canonical rootfs path before mounting, matching `addContainer`.
    pub destination: String,
    pub options: Vec<String>,
}

impl BlockMount {
    pub fn block(format: impl Into<String>, source: impl Into<PathBuf>) -> Self {
        Self {
            format: format.into(),
            source: source.into(),
            destination: String::new(),
            options: Vec::new(),
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.options.iter().any(|o| o == "ro")
    }
}

/// A filesystem that has been attached to the VM, mirroring `AttachedFilesystem`.
///
/// `source` is what the *guest* sees — a block device path such as `/dev/vdb`,
/// or a virtiofs tag — which is why the pod can only build a container's mount
/// list after the attach has happened.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachedFilesystem {
    pub type_: String,
    pub source: String,
    pub destination: String,
    pub options: Vec<String>,
}

impl AttachedFilesystem {
    /// The guest-side mount this attachment implies, mirroring upstream's
    /// `AttachedFilesystem.to`.
    pub fn to_mount(&self) -> oci::Mount {
        oci::Mount::new(
            self.type_.clone(),
            self.source.clone(),
            self.destination.clone(),
            self.options.clone(),
        )
    }
}

/// A network interface for the pod's VM, mirroring the `Interface` protocol.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Interface {
    /// CIDR form, e.g. `192.168.64.5/24`.
    pub address: String,
    pub gateway: Option<String>,
    pub mtu: Option<u32>,
    /// MAC address, if the broker should pin one.
    pub mac_address: Option<String>,
}

/// Configuration for creating a pod's VM, mirroring `VMConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VmConfig {
    /// Identifier the broker should use for the VM; the pod passes its own id.
    pub id: String,
    pub cpus: u32,
    pub memory_in_bytes: u64,
    pub interfaces: Vec<Interface>,
    pub nested_virtualization: bool,
    /// Filesystems to attach at boot, keyed by owner id (container id, or the
    /// pod id for pod-level volumes). Mirrors `VMConfiguration.mountsByID`.
    pub mounts_by_id: HashMap<String, Vec<BlockMount>>,
    /// Host path to write the guest's serial console to.
    pub boot_log: Option<PathBuf>,
}

impl VmConfig {
    /// `LinuxPod.Configuration` defaults: 4 cpus, 1024 MiB.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            cpus: 4,
            memory_in_bytes: 1024 * 1024 * 1024,
            interfaces: Vec::new(),
            nested_virtualization: false,
            mounts_by_id: HashMap::new(),
            boot_log: None,
        }
    }
}

/// Creates VMs. Mirrors `VirtualMachineManager`.
#[async_trait]
pub trait Vmm: Send + Sync + std::fmt::Debug {
    async fn create(&self, config: VmConfig) -> Result<Arc<dyn VmInstance>>;
}

/// A live VM. Mirrors `VirtualMachineInstance`.
#[async_trait]
pub trait VmInstance: Send + Sync + std::fmt::Debug {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn state(&self) -> VmState;

    /// Connect to a vsock port in the guest, as a host-side stream.
    async fn dial(&self, port: u32) -> Result<UnixStream>;

    /// Listen for guest-initiated connections to a host vsock port. Returns the
    /// host socket path the broker is accepting on; used for process stdin.
    async fn listen(&self, port: u32) -> Result<PathBuf>;

    /// Attach a block device to the running VM, returning what the guest sees.
    async fn hotplug(&self, block: BlockMount, id: &str) -> Result<AttachedFilesystem>;

    /// Release a previously hotplugged device.
    async fn release_hotplug(&self, id: &str) -> Result<()>;

    /// Every attachment the VM knows about, keyed by owner id. For a container,
    /// element 0 is its rootfs — the pod relies on that ordering, as upstream's
    /// `startContainer` does with `containerMounts.dropFirst()`.
    async fn mounts(&self) -> HashMap<String, Vec<AttachedFilesystem>>;

    /// Record a container's attachments after hotplug so `mounts()` reports them.
    async fn register_mounts(
        &self,
        id: &str,
        rootfs: AttachedFilesystem,
        additional: Vec<AttachedFilesystem>,
    ) -> Result<()>;

    /// Dial the guest agent on its well-known vsock port and wrap it in a gRPC
    /// client. Mirrors `VirtualMachineInstance.dialAgent()`.
    async fn dial_agent(&self) -> Result<Agent> {
        let stream = self.dial(AGENT_VSOCK_PORT).await?;
        agent_over_stream(stream).await
    }
}

/// Build an [`Agent`] from an already-connected stream to the guest's
/// `SandboxContext` server.
///
/// The stream is consumed by the first (and only) connection attempt: like
/// upstream's `HTTP2ClientTransport.WrappedChannel.withConnectedSocket`, this
/// channel cannot re-dial, because the fd it was handed is already connected. If
/// the guest drops the connection, callers dial a new agent.
pub async fn agent_over_stream(stream: UnixStream) -> Result<Agent> {
    let stream = std::sync::Mutex::new(Some(stream));
    // The authority is required by tonic but never resolved: the connector
    // ignores the Uri and returns the pre-connected stream.
    let channel = Endpoint::from_static("http://vminitd.vsock")
        .connect_with_connector(service_fn(move |_: Uri| {
            let taken = stream.lock().expect("agent stream mutex poisoned").take();
            async move {
                match taken {
                    Some(s) => Ok(TokioIo::new(s)),
                    None => Err(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "vminitd stream already consumed; dial a new agent",
                    )),
                }
            }
        }))
        .await?;
    Ok(Agent::new(channel))
}

/// Connect an [`Agent`] to a `SandboxContext` server listening on a unix socket.
///
/// Unlike [`agent_over_stream`] this one can reconnect, so it is what a broker
/// that exposes the agent as a long-lived socket path should use.
pub async fn agent_over_socket(path: impl Into<PathBuf>) -> Result<Agent> {
    let path = path.into();
    let channel = Endpoint::from_static("http://vminitd.vsock")
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?)) }
        }))
        .await?;
    Ok(Agent::new(channel))
}

/// Allocates vsock ports for process stdio.
///
/// `LinuxPod` seeds both its host and guest allocators at `0x1000_0000`
/// (`LinuxPod.swift:266`) and hands out one port per stream.
#[derive(Debug)]
pub struct PortAllocator {
    next: std::sync::atomic::AtomicU32,
}

/// The base vsock port `LinuxPod` allocates stdio ports from.
pub const STDIO_PORT_BASE: u32 = 0x1000_0000;

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new(STDIO_PORT_BASE)
    }
}

impl PortAllocator {
    pub fn new(base: u32) -> Self {
        Self {
            next: std::sync::atomic::AtomicU32::new(base),
        }
    }

    pub fn allocate(&self) -> u32 {
        self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

/// Helper for brokers: turn a `Vmm` error string into [`Error::Vmm`].
pub fn vmm_err(msg: impl std::fmt::Display) -> Error {
    Error::vmm(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_allocator_starts_at_upstream_base_and_increments() {
        let alloc = PortAllocator::default();
        assert_eq!(alloc.allocate(), 0x1000_0000);
        assert_eq!(alloc.allocate(), 0x1000_0001);
    }

    #[test]
    fn attached_filesystem_maps_to_guest_mount() {
        let attached = AttachedFilesystem {
            type_: "ext4".to_string(),
            source: "/dev/vdb".to_string(),
            destination: "/run/container/c1/rootfs".to_string(),
            options: vec!["rw".to_string()],
        };
        let mount = attached.to_mount();
        assert_eq!(mount.type_, "ext4");
        assert_eq!(mount.source, "/dev/vdb");
        assert_eq!(mount.destination, "/run/container/c1/rootfs");
    }

    #[test]
    fn block_mount_readonly_is_driven_by_options() {
        let mut m = BlockMount::block("ext4", "/tmp/rootfs.ext4");
        assert!(!m.is_readonly());
        m.options.push("ro".to_string());
        assert!(m.is_readonly());
    }

    #[test]
    fn vm_config_defaults_match_linux_pod() {
        let cfg = VmConfig::new("pod-1");
        assert_eq!(cfg.cpus, 4);
        assert_eq!(cfg.memory_in_bytes, 1024 * 1024 * 1024);
    }
}

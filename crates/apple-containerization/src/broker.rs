//! The VMM broker protocol, and a pod client that speaks it.
//!
//! # Why the broker owns pods, not just VMs
//!
//! This protocol used to be VM-shaped — `createVm`, `start`, `hotplug`, `dial` —
//! with [`crate::pod`] porting Apple's `LinuxPod` into Rust and driving a dumb VM
//! underneath. That port duplicated working Swift, did not receive Apple's fixes,
//! and its first live boot failed on a `LinuxPod.create()` precondition it had not
//! replicated. The broker now links Apple's `Containerization` and calls the real
//! `LinuxPod`, so the protocol is **pod-shaped**: `createPod`, `addContainer`,
//! `startContainer`, `waitContainer`.
//!
//! What stays on this side is the part that is genuinely ours — translating CRI
//! onto these calls (`apple-cri`'s `pod_runtime`).
//!
//! # Who is authoritative
//!
//! **`vmm-broker/Sources/rusternetes-vmm/Protocol.swift` is.** This module
//! mirrors it, and `tests` below pin the exact JSON both sides must agree on.
//! Two rules follow from Swift's `Codable`:
//!
//! 1. A field that is **non-optional in Swift must always be serialised here** —
//!    `Codable` fails to decode when it is absent. So no `skip_serializing_if` on
//!    anything but `Option`s.
//! 2. Swift's `JSONEncoder` omits `nil`, so `#[serde(default)]` is required on
//!    every struct we decode.
//!
//! Getting this wrong is not loud. The `container` 1.2.0 upgrade broke five
//! things and three failed *silently*, parsing into empty values rather than
//! erroring — hence the fixture tests rather than hand-checked field names.
//!
//! # Wire format
//!
//! Newline-delimited JSON over a unix socket, **one connection per request**:
//! connect, write one request line, read one response line, close. These calls
//! are infrequent (a handful per pod lifecycle), so a connection per call buys
//! simplicity — no request ids, no multiplexing, no head-of-line blocking, and a
//! crashed broker surfaces as a connect error rather than a hung stream.
//!
//! ```text
//! -> {"method":"createPod","params":{"config":{"id":"pod-1",…}}}
//! <- {"ok":{}}
//! -> {"method":"waitContainer","params":{"podId":"pod-1","containerId":"init"}}
//! <- {"ok":{"exitCode":0}}
//! ```
//!
//! Bulk data never crosses this socket. `dial` returns a *path* to a dedicated
//! unix socket that the broker relays to the guest vsock port, mirroring
//! upstream's `Vminitd.init(connection: FileHandle,…)` taking an already-connected
//! fd.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::{Error, Result};

/// A filesystem to attach to the VM, mirroring containerization's `Mount` in its
/// host-side (pre-attach) form.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockMount {
    /// Filesystem format, e.g. `ext4`.
    pub format: String,
    /// Host path of the disk image.
    pub source: PathBuf,
    /// Where it should end up in the guest. The broker overwrites this with the
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

/// A filesystem as the *guest* sees it, mirroring `AttachedFilesystem`.
///
/// `source` is a guest-side path — a block device such as `/dev/vdb`, or a
/// virtiofs tag — or, for a bind, the path inside the guest to bind from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachedFilesystem {
    /// `type` on the wire; `type_` here because `type` is a Rust keyword.
    pub type_: String,
    pub source: String,
    pub destination: String,
    pub options: Vec<String>,
}

/// A network interface for the pod's VM, mirroring the `Interface` protocol.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Interface {
    /// CIDR form, e.g. `192.168.64.5/24`.
    pub address: String,
    pub gateway: Option<String>,
    pub mtu: Option<u32>,
    /// MAC address, if the broker should pin one.
    pub mac_address: Option<String>,
}

/// A request to the broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub method: Method,
    pub params: Params,
}

/// The broker's method set, mirroring `Protocol.swift`'s `Method`.
///
/// Deliberately small: anything expressible as `SandboxContext` gRPC belongs in
/// [`crate::agent`], and anything expressible as `LinuxPod` belongs in the
/// broker's `PodService`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Method {
    /// Build a pod from a [`PodConfigWire`]. Does not boot its VM.
    CreatePod,
    /// Boot the pod's VM and bring the sandbox up.
    Create,
    /// Stop every container and shut the VM down.
    StopPod,
    /// Add a container. Before [`Method::Create`] it is attached at boot; after,
    /// its rootfs is hotplugged into the live VM — which is what makes CRI's
    /// "create a container in a running sandbox" possible.
    AddContainer,
    StartContainer,
    StopContainer,
    /// Signal a container's init process with a raw POSIX signal number.
    KillContainer,
    /// Wait for a container's init process; answers with its exit code.
    WaitContainer,
    ListContainers,
    /// Exec a process in a running container; answers with its guest pid.
    Exec,
    /// Join a client's unix sockets to a running container's stdio.
    Attach,
    /// Resize an exec'd process's pty.
    Resize,
    /// EOF a container's stdin — CRI's `stdinOnce`.
    CloseStdin,
    ReopenContainerLog,
    /// Wait for an exec'd process; answers with its exit code. `ExecSync` needs
    /// this — [`Method::Exec`]'s pid says nothing about how the command ended.
    WaitProcess,
    /// Signal an exec'd process, which is how `ExecSync` enforces its timeout.
    KillProcess,
    Statistics,
    /// CRI image management over the broker's own `ImageStore` — the store the
    /// pod path actually runs rootfs images out of.
    ListImages,
    ImageStatus,
    PullImage,
    RemoveImage,
    ImageFsInfo,
    /// Materialise a container image as an ext4 block device (see
    /// [`BrokerRootfs`]).
    ProvisionRootfs,
    ReleaseRootfs,
    /// Connect to a guest vsock port; answers with a relay socket path.
    Dial,
}

/// DNS configuration for the pod, mirroring `DnsConfigWire`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DnsConfigWire {
    pub nameservers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    pub search_domains: Vec<String>,
    pub options: Vec<String>,
}

/// Pod-level configuration, mirroring `LinuxPod.Configuration`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PodConfigWire {
    pub id: String,
    pub cpus: u32,
    pub memory_in_bytes: u64,
    pub interfaces: Vec<Interface>,
    pub share_process_namespace: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns: Option<DnsConfigWire>,
    /// Host path for the guest's boot log. Invaluable when a pod fails to come
    /// up, because the failure is otherwise invisible from this side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boot_log: Option<String>,
}

impl Default for PodConfigWire {
    fn default() -> Self {
        Self {
            id: String::new(),
            // Apple's own default per container; see STATUS.md on why sizing a
            // pod VM from `LinuxPodSandboxConfig.resources` is the real fix.
            cpus: 4,
            memory_in_bytes: 1024 * 1024 * 1024,
            interfaces: Vec::new(),
            share_process_namespace: false,
            hostname: None,
            dns: None,
            boot_log: None,
        }
    }
}

impl PodConfigWire {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ..Default::default()
        }
    }
}

/// Per-container configuration, mirroring `LinuxPod.ContainerConfiguration` plus
/// the rootfs block the broker provisioned.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerConfigWire {
    pub id: String,
    pub rootfs: BlockMount,
    /// Host path for the container's CRI log file. When set, the broker writes
    /// stdout/stderr there in the CRI format — `<RFC3339Nano> <stream> <F|P>
    /// <line>`.
    ///
    /// The runtime owning the log file is upstream's own arrangement:
    /// containerd's CRI plugin writes it in `pkg/cri/io/logger.go`. It is also
    /// the only shape available here, because the process's `stdout`/`stderr` are
    /// `Writer`s held by the broker — there is no stream for this side to read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_directory: String,
    pub terminal: bool,
    /// CRI `stdin`: the container gets an attachable stdin. Without it the guest
    /// process must get no stdin at all, rather than a stream that never EOFs.
    pub stdin: bool,
    /// CRI `stdin_once`: close the container's stdin once an attached client
    /// detaches, so a process reading stdin sees EOF and exits instead of
    /// hanging. `kubectl attach --stdin` sets it.
    pub stdin_once: bool,
    pub uid: u32,
    pub gid: u32,
    pub additional_gids: Vec<u32>,
    pub username: String,
    /// A pod-level hostname reaches each container through *its own* UTS
    /// namespace: Apple's `LinuxPod` gives every container a fresh `uts`, so this
    /// is the same string rather than the same namespace. See `PodService.swift`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpus: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_in_bytes: Option<u64>,
    pub sysctl: HashMap<String, String>,
    /// Extra mounts, on top of the defaults the container configuration seeds.
    pub mounts: Vec<AttachedFilesystem>,
    pub masked_paths: Vec<String>,
    pub readonly_paths: Vec<String>,
}

impl ContainerConfigWire {
    pub fn new(id: impl Into<String>, rootfs: BlockMount) -> Self {
        Self {
            id: id.into(),
            rootfs,
            ..Default::default()
        }
    }
}

/// What to exec, and where its output goes.
///
/// Output goes to **files**, not sockets. `ExecSync` is synchronous and bounded,
/// a file never blocks the guest when nothing is reading it, and there is no
/// connect race between `exec` returning and the caller attaching. Interactive
/// `Exec`/`Attach` will need a streaming transport; this is not it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecOptions {
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub terminal: bool,
    pub stdin_path: Option<PathBuf>,
    pub stdout_path: Option<PathBuf>,
    pub stderr_path: Option<PathBuf>,
    /// The paths above are unix sockets the caller is already listening on,
    /// rather than files the broker creates.
    pub sockets: bool,
}

impl ExecOptions {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            args,
            ..Default::default()
        }
    }

    /// Capture stdout and stderr into `dir`, as `ExecSync` needs.
    pub fn capturing_in(mut self, dir: &Path) -> Self {
        self.stdout_path = Some(dir.join("stdout"));
        self.stderr_path = Some(dir.join("stderr"));
        self.sockets = false;
        self
    }

    /// Stream stdio over unix sockets the caller is listening on, as an
    /// interactive `Exec` needs. `stdin` is `None` when the client sends none.
    pub fn streaming(
        mut self,
        stdin: Option<PathBuf>,
        stdout: Option<PathBuf>,
        stderr: Option<PathBuf>,
    ) -> Self {
        self.stdin_path = stdin;
        self.stdout_path = stdout;
        self.stderr_path = stderr;
        self.sockets = true;
        self
    }
}

/// One image in the broker's store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageWire {
    pub reference: String,
    pub digest: String,
    pub size_bytes: u64,
    /// The raw OCI `User` string. Splitting it into uid vs username is a CRI
    /// convention, so it is done on this side rather than in the broker.
    pub user: String,
    /// The image's own process configuration, reported verbatim.
    ///
    /// A CRI `ContainerConfig` routinely leaves `command`, `args`, `envs` and
    /// `working_dir` empty and expects the image's values to apply. Merging the
    /// two is a CRI convention, so the broker reports and this side decides.
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: String,
}

/// Per-container statistics from the guest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStatsWire {
    pub id: String,
    pub memory_usage_bytes: u64,
    pub memory_inactive_file_bytes: u64,
    pub memory_anon_bytes: u64,
    pub cpu_usage_usec: u64,
}

/// Union of every method's parameters. A flat, all-optional shape keeps the
/// Swift side's decoding trivial — it reads only the fields its method needs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Params {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<PodConfigWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<ContainerConfigWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_ids: Option<Vec<String>>,
    /// `exec` only: must differ from `container_id`. That is how the guest tells
    /// an exec from the container's init process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal: Option<bool>,
    /// `exec` / `attach`: where the process's stdio goes. Files for `ExecSync`,
    /// unix sockets when `stdio_sockets` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdio_sockets: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u16>,
    /// Raw POSIX signal number, as CRI uses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,
    /// Container id, or the pod id for pod-level volumes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

/// The broker's answer. Exactly one of `ok` / `error` is set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<Reply>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Payload of a successful response; every field is method-specific.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Reply {
    /// `dial`: the relay socket to connect to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
    /// `provisionRootfs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockMount>,
    /// `listContainers`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_ids: Option<Vec<String>>,
    /// `waitContainer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// `exec`: the guest pid of the spawned process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    /// `statistics`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Vec<ContainerStatsWire>>,
    /// `createPod`: the address the broker allocated for the pod.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<ImageWire>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<ImageWire>,
    /// `imageFsInfo`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_bytes: Option<u64>,
}

impl Response {
    pub fn ok(reply: Reply) -> Self {
        Self {
            ok: Some(reply),
            error: None,
        }
    }

    pub fn err(message: impl std::fmt::Display) -> Self {
        Self {
            ok: None,
            error: Some(message.to_string()),
        }
    }
}

/// Transport to a broker listening on a unix socket.
#[derive(Debug, Clone)]
pub struct BrokerClient {
    socket: PathBuf,
}

impl BrokerClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// One request, one response, one connection.
    pub async fn call(&self, method: Method, params: Params) -> Result<Reply> {
        let stream = UnixStream::connect(&self.socket).await.map_err(|e| {
            Error::vmm(format!(
                "connecting to vmm broker at {}: {e}",
                self.socket.display()
            ))
        })?;
        let mut stream = BufReader::new(stream);

        let mut line = serde_json::to_vec(&Request { method, params })
            .map_err(|e| Error::vmm(format!("encoding {method:?}: {e}")))?;
        line.push(b'\n');
        stream
            .get_mut()
            .write_all(&line)
            .await
            .map_err(|e| Error::vmm(format!("sending {method:?}: {e}")))?;
        stream
            .get_mut()
            .flush()
            .await
            .map_err(|e| Error::vmm(format!("flushing {method:?}: {e}")))?;

        let mut response = String::new();
        let read = stream
            .read_line(&mut response)
            .await
            .map_err(|e| Error::vmm(format!("reading reply to {method:?}: {e}")))?;
        if read == 0 {
            // A broker that dies mid-call must not look like a successful no-op.
            return Err(Error::vmm(format!(
                "vmm broker closed the connection without answering {method:?}"
            )));
        }

        let response: Response = serde_json::from_str(&response)
            .map_err(|e| Error::vmm(format!("decoding reply to {method:?}: {e}")))?;
        match (response.ok, response.error) {
            (_, Some(error)) => Err(Error::vmm(format!("{method:?}: {error}"))),
            (Some(reply), None) => Ok(reply),
            (None, None) => Err(Error::vmm(format!(
                "{method:?}: broker replied with neither ok nor error"
            ))),
        }
    }
}

/// A pod, addressed by id on every call.
///
/// This is the whole host-side pod surface: `apple-cri`'s `pod_runtime` maps CRI
/// onto exactly these calls. It deliberately mirrors the CRI verbs it serves —
/// `RunPodSandbox` is [`PodBroker::create_pod`] + [`PodBroker::create`],
/// `CreateContainer` is `provisionRootfs` + [`PodBroker::add_container`].
#[derive(Debug, Clone)]
pub struct PodBroker {
    client: BrokerClient,
}

impl PodBroker {
    pub fn new(client: BrokerClient) -> Self {
        Self { client }
    }

    /// Connect to a broker socket.
    pub fn connect(socket: impl Into<PathBuf>) -> Self {
        Self::new(BrokerClient::new(socket))
    }

    pub fn client(&self) -> &BrokerClient {
        &self.client
    }

    fn for_pod(pod_id: &str) -> Params {
        Params {
            pod_id: Some(pod_id.to_string()),
            ..Default::default()
        }
    }

    fn for_container(pod_id: &str, container_id: &str) -> Params {
        Params {
            container_id: Some(container_id.to_string()),
            ..Self::for_pod(pod_id)
        }
    }

    /// Build the pod without booting it. Containers added before [`Self::create`]
    /// are attached at boot; those added after are hotplugged.
    ///
    /// Answers with the address the broker allocated, so a caller does not need a
    /// second round trip for `PodSandboxStatus.network.ip`. `None` means the pod
    /// has no address — the caller supplied its own interfaces, or allocation is
    /// disabled.
    pub async fn create_pod(&self, config: &PodConfigWire) -> Result<Option<String>> {
        let reply = self
            .client
            .call(
                Method::CreatePod,
                Params {
                    pod_id: Some(config.id.clone()),
                    config: Some(config.clone()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(reply.ipv4)
    }

    /// Boot the pod's VM and bring the sandbox up.
    pub async fn create(&self, pod_id: &str) -> Result<()> {
        self.client
            .call(Method::Create, Self::for_pod(pod_id))
            .await?;
        Ok(())
    }

    /// Stop every container and shut the VM down. Idempotent: a pod the broker
    /// no longer knows about is already gone.
    pub async fn stop_pod(&self, pod_id: &str) -> Result<()> {
        self.client
            .call(Method::StopPod, Self::for_pod(pod_id))
            .await?;
        Ok(())
    }

    pub async fn add_container(&self, pod_id: &str, container: &ContainerConfigWire) -> Result<()> {
        self.client
            .call(
                Method::AddContainer,
                Params {
                    container: Some(container.clone()),
                    ..Self::for_pod(pod_id)
                },
            )
            .await?;
        Ok(())
    }

    pub async fn start_container(&self, pod_id: &str, container_id: &str) -> Result<()> {
        self.client
            .call(
                Method::StartContainer,
                Self::for_container(pod_id, container_id),
            )
            .await?;
        Ok(())
    }

    pub async fn stop_container(&self, pod_id: &str, container_id: &str) -> Result<()> {
        self.client
            .call(
                Method::StopContainer,
                Self::for_container(pod_id, container_id),
            )
            .await?;
        Ok(())
    }

    /// Signal a container's init process. `signal` is a raw POSIX number.
    pub async fn kill_container(
        &self,
        pod_id: &str,
        container_id: &str,
        signal: i32,
    ) -> Result<()> {
        self.client
            .call(
                Method::KillContainer,
                Params {
                    signal: Some(signal),
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        Ok(())
    }

    /// Wait for a container's init process and return its exit code.
    ///
    /// This is the primitive an **init container** needs: the kubelet only starts
    /// the next container once this one has exited, and it drives restart policy
    /// off the code. A missing `exitCode` is an error rather than a defaulted
    /// zero — "the container succeeded" is precisely the wrong thing to guess.
    pub async fn wait_container(&self, pod_id: &str, container_id: &str) -> Result<i32> {
        let reply = self
            .client
            .call(
                Method::WaitContainer,
                Self::for_container(pod_id, container_id),
            )
            .await?;
        reply.exit_code.ok_or_else(|| {
            Error::vmm(format!(
                "waitContainer({pod_id}/{container_id}): broker returned no exitCode"
            ))
        })
    }

    pub async fn list_containers(&self, pod_id: &str) -> Result<Vec<String>> {
        let reply = self
            .client
            .call(Method::ListContainers, Self::for_pod(pod_id))
            .await?;
        Ok(reply.container_ids.unwrap_or_default())
    }

    /// Exec a process in a running container; answers with its guest pid.
    ///
    /// `process_id` must differ from `container_id` — that is how the guest
    /// distinguishes an exec from the container's init process.
    pub async fn exec(
        &self,
        pod_id: &str,
        container_id: &str,
        process_id: &str,
        options: &ExecOptions,
    ) -> Result<i32> {
        let reply = self
            .client
            .call(
                Method::Exec,
                Params {
                    process_id: Some(process_id.to_string()),
                    args: Some(options.args.clone()),
                    env: Some(options.env.clone()),
                    terminal: Some(options.terminal),
                    stdin_path: options.stdin_path.clone(),
                    stdout_path: options.stdout_path.clone(),
                    stderr_path: options.stderr_path.clone(),
                    stdio_sockets: Some(options.sockets),
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        reply.pid.ok_or_else(|| {
            Error::vmm(format!(
                "exec({pod_id}/{container_id}/{process_id}): broker returned no pid"
            ))
        })
    }

    /// Join a client's unix sockets to a running container's stdio.
    ///
    /// The caller must already be listening on every path it passes: the broker
    /// connects, so anything the container writes before that would otherwise be
    /// lost.
    /// CRI's `ReopenContainerLog`: the broker owns the file, so only it can
    /// reopen the path after a rotation.
    pub async fn reopen_container_log(&self, pod_id: &str, container_id: &str) -> Result<()> {
        self.client
            .call(
                Method::ReopenContainerLog,
                Self::for_container(pod_id, container_id),
            )
            .await
            .map(|_| ())
    }

    pub async fn attach(
        &self,
        pod_id: &str,
        container_id: &str,
        stdin: Option<PathBuf>,
        stdout: Option<PathBuf>,
        stderr: Option<PathBuf>,
    ) -> Result<()> {
        self.client
            .call(
                Method::Attach,
                Params {
                    stdin_path: stdin,
                    stdout_path: stdout,
                    stderr_path: stderr,
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        Ok(())
    }

    /// Resize an exec'd process's pty.
    pub async fn resize(
        &self,
        pod_id: &str,
        container_id: &str,
        process_id: &str,
        width: u16,
        height: u16,
    ) -> Result<()> {
        self.client
            .call(
                Method::Resize,
                Params {
                    process_id: Some(process_id.to_string()),
                    width: Some(width),
                    height: Some(height),
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        Ok(())
    }

    /// EOF a container's stdin — CRI's `stdinOnce`.
    pub async fn close_stdin(&self, pod_id: &str, container_id: &str) -> Result<()> {
        self.client
            .call(
                Method::CloseStdin,
                Self::for_container(pod_id, container_id),
            )
            .await?;
        Ok(())
    }

    /// Wait for an exec'd process and return its exit code.
    pub async fn wait_process(
        &self,
        pod_id: &str,
        container_id: &str,
        process_id: &str,
    ) -> Result<i32> {
        let reply = self
            .client
            .call(
                Method::WaitProcess,
                Params {
                    process_id: Some(process_id.to_string()),
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        reply.exit_code.ok_or_else(|| {
            Error::vmm(format!(
                "waitProcess({pod_id}/{container_id}/{process_id}): broker returned no exitCode"
            ))
        })
    }

    /// Signal an exec'd process. `signal` is a raw POSIX number.
    pub async fn kill_process(
        &self,
        pod_id: &str,
        container_id: &str,
        process_id: &str,
        signal: i32,
    ) -> Result<()> {
        self.client
            .call(
                Method::KillProcess,
                Params {
                    process_id: Some(process_id.to_string()),
                    signal: Some(signal),
                    ..Self::for_container(pod_id, container_id)
                },
            )
            .await?;
        Ok(())
    }

    /// Per-container statistics. An empty `container_ids` means every container.
    pub async fn statistics(
        &self,
        pod_id: &str,
        container_ids: Vec<String>,
    ) -> Result<Vec<ContainerStatsWire>> {
        let reply = self
            .client
            .call(
                Method::Statistics,
                Params {
                    container_ids: Some(container_ids),
                    ..Self::for_pod(pod_id)
                },
            )
            .await?;
        Ok(reply.stats.unwrap_or_default())
    }

    /// Dial a guest vsock port through the broker's relay.
    pub async fn dial(&self, pod_id: &str, port: u32) -> Result<UnixStream> {
        let reply = self
            .client
            .call(
                Method::Dial,
                Params {
                    port: Some(port),
                    ..Self::for_pod(pod_id)
                },
            )
            .await?;
        let path = reply
            .socket_path
            .ok_or_else(|| Error::vmm(format!("dial({port}): broker returned no socketPath")))?;
        UnixStream::connect(&path).await.map_err(|e| {
            Error::vmm(format!(
                "connecting to vsock relay {} for port {port}: {e}",
                path.display()
            ))
        })
    }
}

/// A rootfs provider backed by the broker.
///
/// Turning an image reference into an ext4 block device is host-side work that
/// needs the image layers and an ext4 writer (upstream: `ContainerizationEXT4`'s
/// `EXT4Unpacker`, writing a per-container `rootfs.ext4`). The broker already
/// links that code, so it owns this too rather than duplicating an ext4
/// implementation in Rust.
#[derive(Debug, Clone)]
pub struct BrokerRootfs {
    client: BrokerClient,
}

impl BrokerRootfs {
    pub fn new(client: BrokerClient) -> Self {
        Self { client }
    }

    /// Every image in the broker's store.
    pub async fn list_images(&self) -> Result<Vec<ImageWire>> {
        let reply = self
            .client
            .call(Method::ListImages, Params::default())
            .await?;
        Ok(reply.images.unwrap_or_default())
    }

    /// `None` when the image is not present — absence, not failure, as CRI wants.
    pub async fn image_status(&self, image: &str) -> Result<Option<ImageWire>> {
        let reply = self
            .client
            .call(
                Method::ImageStatus,
                Params {
                    image: Some(image.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(reply.image)
    }

    pub async fn pull_image(&self, image: &str) -> Result<ImageWire> {
        let reply = self
            .client
            .call(
                Method::PullImage,
                Params {
                    image: Some(image.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        reply
            .image
            .ok_or_else(|| Error::vmm(format!("pullImage({image}): broker returned no image")))
    }

    pub async fn remove_image(&self, image: &str) -> Result<()> {
        self.client
            .call(
                Method::RemoveImage,
                Params {
                    image: Some(image.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Path and bytes used by the image store, for `ImageFsInfo`.
    pub async fn image_fs_info(&self) -> Result<(String, u64)> {
        let reply = self
            .client
            .call(Method::ImageFsInfo, Params::default())
            .await?;
        Ok((
            reply.fs_path.unwrap_or_default(),
            reply.fs_bytes.unwrap_or_default(),
        ))
    }

    /// Materialise `image` as a block device for `container_id`.
    pub async fn provision(&self, image: &str, container_id: &str) -> Result<BlockMount> {
        let reply = self
            .client
            .call(
                Method::ProvisionRootfs,
                Params {
                    image: Some(image.to_string()),
                    owner_id: Some(container_id.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        reply.block.ok_or_else(|| {
            Error::vmm(format!(
                "provisionRootfs({image}) for {container_id}: broker returned no block"
            ))
        })
    }

    /// Discard whatever `provision` created.
    pub async fn release(&self, container_id: &str) -> Result<()> {
        self.client
            .call(
                Method::ReleaseRootfs,
                Params {
                    owner_id: Some(container_id.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_round_trips_over_the_wire_format() {
        let request = Request {
            method: Method::Dial,
            params: Params {
                pod_id: Some("pod-1".to_string()),
                port: Some(1024),
                ..Default::default()
            },
        };
        let line = serde_json::to_string(&request).unwrap();
        // Unset fields are omitted, so the Swift side decodes a small object.
        assert!(!line.contains("config"), "{line}");
        assert!(line.contains(r#""method":"dial""#), "{line}");
        assert!(line.contains(r#""podId":"pod-1""#), "{line}");
        assert_eq!(serde_json::from_str::<Request>(&line).unwrap(), request);
    }

    #[test]
    fn every_method_serialises_as_the_name_protocol_swift_declares() {
        // One assertion per case, so adding a method to only one side fails here
        // rather than at runtime against a live broker.
        let name = |m: Method| serde_json::to_string(&m).unwrap();
        assert_eq!(name(Method::CreatePod), r#""createPod""#);
        assert_eq!(name(Method::Create), r#""create""#);
        assert_eq!(name(Method::StopPod), r#""stopPod""#);
        assert_eq!(name(Method::AddContainer), r#""addContainer""#);
        assert_eq!(name(Method::StartContainer), r#""startContainer""#);
        assert_eq!(name(Method::StopContainer), r#""stopContainer""#);
        assert_eq!(name(Method::KillContainer), r#""killContainer""#);
        assert_eq!(name(Method::WaitContainer), r#""waitContainer""#);
        assert_eq!(name(Method::ListContainers), r#""listContainers""#);
        assert_eq!(name(Method::Exec), r#""exec""#);
        assert_eq!(name(Method::Attach), r#""attach""#);
        assert_eq!(name(Method::Resize), r#""resize""#);
        assert_eq!(name(Method::CloseStdin), r#""closeStdin""#);
        assert_eq!(name(Method::WaitProcess), r#""waitProcess""#);
        assert_eq!(name(Method::KillProcess), r#""killProcess""#);
        assert_eq!(name(Method::Statistics), r#""statistics""#);
        assert_eq!(name(Method::ListImages), r#""listImages""#);
        assert_eq!(name(Method::ImageStatus), r#""imageStatus""#);
        assert_eq!(name(Method::PullImage), r#""pullImage""#);
        assert_eq!(name(Method::RemoveImage), r#""removeImage""#);
        assert_eq!(name(Method::ImageFsInfo), r#""imageFsInfo""#);
        assert_eq!(name(Method::ProvisionRootfs), r#""provisionRootfs""#);
        assert_eq!(name(Method::ReleaseRootfs), r#""releaseRootfs""#);
        assert_eq!(name(Method::Dial), r#""dial""#);
    }

    /// Swift's `Codable` fails to decode a non-optional field that is absent.
    /// Every field below is non-optional in `Protocol.swift`, so serialising a
    /// default must still emit all of them — a `skip_serializing_if` added here
    /// would break the broker at runtime, not at compile time.
    #[test]
    fn non_optional_swift_fields_are_always_emitted() {
        let json = serde_json::to_string(&PodConfigWire::new("pod-1")).unwrap();
        for field in [
            "id",
            "cpus",
            "memoryInBytes",
            "interfaces",
            "shareProcessNamespace",
        ] {
            assert!(
                json.contains(field),
                "PodConfigWire dropped {field}: {json}"
            );
        }
        // Optionals stay absent, which Swift decodes as nil.
        assert!(!json.contains("hostname"), "{json}");

        let json = serde_json::to_string(&ContainerConfigWire::new(
            "app",
            BlockMount::block("ext4", "/images/app.ext4"),
        ))
        .unwrap();
        for field in [
            "id",
            "rootfs",
            "args",
            "env",
            "workingDirectory",
            "terminal",
            "stdin",
            "uid",
            "gid",
            "additionalGids",
            "username",
            "sysctl",
            "mounts",
            "maskedPaths",
            "readonlyPaths",
        ] {
            assert!(
                json.contains(field),
                "ContainerConfigWire dropped {field}: {json}"
            );
        }
    }

    /// A verbatim capture of what the Swift side encodes/decodes. Asserting
    /// against a hand-written struct would go on agreeing with a stale model —
    /// which is exactly how the 1.2.0 breakages got through.
    #[test]
    fn a_container_config_matches_the_swift_field_names() {
        let fixture = r#"{
            "id": "app",
            "rootfs": {"format":"ext4","source":"/blocks/app.ext4","destination":"/","options":["ro"]},
            "args": ["/bin/sh","-c","sleep 1"],
            "env": ["PATH=/usr/bin"],
            "workingDirectory": "/work",
            "terminal": false,
            "stdin": true,
            "uid": 0,
            "gid": 0,
            "additionalGids": [10],
            "username": "root",
            "hostname": "my-pod",
            "cpus": 2,
            "memoryInBytes": 268435456,
            "sysctl": {"net.ipv4.ip_forward":"1"},
            "mounts": [{"type":"bind","source":"/host","destination":"/data","options":["rw"]}],
            "maskedPaths": ["/proc/kcore"],
            "readonlyPaths": ["/proc/sys"]
        }"#;
        let decoded: ContainerConfigWire = serde_json::from_str(fixture).unwrap();
        assert_eq!(decoded.id, "app");
        assert_eq!(decoded.working_directory, "/work");
        assert_eq!(decoded.additional_gids, vec![10]);
        assert_eq!(decoded.cpus, Some(2));
        assert_eq!(decoded.masked_paths, vec!["/proc/kcore"]);
        // `type` on the wire, `type_` in Rust: the rename must survive.
        assert_eq!(decoded.mounts[0].type_, "bind");
        assert_eq!(decoded.rootfs.format, "ext4");

        // And it survives a round trip back to the same field names.
        let reencoded = serde_json::to_string(&decoded).unwrap();
        assert_eq!(
            serde_json::from_str::<ContainerConfigWire>(&reencoded).unwrap(),
            decoded
        );
        assert!(reencoded.contains(r#""type":"bind""#), "{reencoded}");
    }

    #[test]
    fn a_pod_config_matches_the_swift_field_names() {
        let fixture = r#"{
            "id": "pod-1",
            "cpus": 4,
            "memoryInBytes": 1073741824,
            "interfaces": [{"address":"192.168.64.5/24","gateway":"192.168.64.1"}],
            "shareProcessNamespace": true,
            "hostname": "my-pod",
            "dns": {"nameservers":["10.96.0.10"],"searchDomains":["svc.cluster.local"],"options":["ndots:5"]},
            "bootLog": "/tmp/rk/pod-1/boot.log"
        }"#;
        let decoded: PodConfigWire = serde_json::from_str(fixture).unwrap();
        assert_eq!(decoded.memory_in_bytes, 1_073_741_824);
        assert!(decoded.share_process_namespace);
        assert_eq!(decoded.interfaces[0].address, "192.168.64.5/24");
        assert_eq!(
            decoded.dns.as_ref().unwrap().search_domains,
            vec!["svc.cluster.local"]
        );
        assert_eq!(decoded.boot_log.as_deref(), Some("/tmp/rk/pod-1/boot.log"));
    }

    #[test]
    fn a_reply_matches_the_swift_field_names() {
        let stats: Reply = serde_json::from_str(
            r#"{"stats":[{"id":"app","memoryUsageBytes":1024,"memoryInactiveFileBytes":8,
                "memoryAnonBytes":512,"cpuUsageUsec":99}]}"#,
        )
        .unwrap();
        let stats = stats.stats.unwrap();
        assert_eq!(stats[0].memory_usage_bytes, 1024);
        assert_eq!(stats[0].cpu_usage_usec, 99);

        let wait: Reply = serde_json::from_str(r#"{"exitCode":0}"#).unwrap();
        assert_eq!(wait.exit_code, Some(0));
        let list: Reply = serde_json::from_str(r#"{"containerIds":["a","b"]}"#).unwrap();
        assert_eq!(list.container_ids.unwrap(), vec!["a", "b"]);
        let exec: Reply = serde_json::from_str(r#"{"pid":42}"#).unwrap();
        assert_eq!(exec.pid, Some(42));
        let pod: Reply = serde_json::from_str(r#"{"ipv4":"192.168.64.200"}"#).unwrap();
        assert_eq!(pod.ipv4.as_deref(), Some("192.168.64.200"));
    }

    #[test]
    fn an_error_response_is_distinguishable_from_a_success() {
        let ok: Response = serde_json::from_str(r#"{"ok":{}}"#).unwrap();
        assert!(ok.ok.is_some() && ok.error.is_none());
        let err: Response = serde_json::from_str(r#"{"error":"no such vm"}"#).unwrap();
        assert!(err.ok.is_none() && err.error.as_deref() == Some("no such vm"));
    }
}

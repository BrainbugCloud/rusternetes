//! A fake guest and a mock VMM, so pod semantics can be tested without a
//! hypervisor.
//!
//! [`FakeGuest`] is a **real** `SandboxContext` gRPC server on a unix socket. The
//! pod talks to it through the same generated client, proto encoding and OCI-spec
//! JSON it would use against `vminitd`, and every RPC is recorded in order. That
//! makes assertions about *the call sequence and the exact spec bytes* — which is
//! where pod semantics actually live — rather than about a hand-rolled double.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::net::UnixStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

use crate::error::Result;
use crate::oci;
use crate::proto::sandbox_context_server::{SandboxContext, SandboxContextServer};
use crate::proto::{self as pb};
use crate::vmm::{AttachedFilesystem, BlockMount, VmConfig, VmInstance, VmState, Vmm};

/// One recorded RPC. Only the fields pod semantics depend on are captured.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    StandardSetupUp {
        interface: String,
    },
    Setenv {
        key: String,
        value: Option<String>,
    },
    Mount {
        type_: String,
        source: String,
        destination: String,
        options: Vec<String>,
    },
    Umount {
        path: String,
    },
    Mkdir {
        path: String,
        all: bool,
        perms: u32,
    },
    IpLinkSet {
        interface: String,
        up: bool,
        mtu: Option<u32>,
    },
    IpAddrAdd {
        interface: String,
        ipv4: String,
    },
    IpRouteAddDefault {
        interface: String,
        gateway: String,
    },
    ConfigureDns {
        location: String,
        nameservers: Vec<String>,
        search_domains: Vec<String>,
    },
    ConfigureHosts {
        location: String,
        entries: Vec<(String, Vec<String>)>,
    },
    CreateProcess {
        id: String,
        container_id: Option<String>,
        oci_runtime_path: Option<String>,
        stdout: Option<u32>,
        stderr: Option<u32>,
        /// Decoded from the JSON the client actually sent. Boxed because a spec
        /// dwarfs every other variant and `calls` holds one entry per RPC.
        spec: Box<oci::Spec>,
    },
    StartProcess {
        id: String,
        container_id: Option<String>,
    },
    KillProcess {
        id: String,
        container_id: Option<String>,
        signal: i32,
    },
    WaitProcess {
        id: String,
        container_id: Option<String>,
    },
    DeleteProcess {
        id: String,
        container_id: Option<String>,
    },
    ResizeProcess {
        id: String,
        container_id: Option<String>,
        columns: u32,
        rows: u32,
    },
    CloseProcessStdin {
        id: String,
        container_id: Option<String>,
    },
    ContainerStatistics {
        container_ids: Vec<String>,
    },
    Sysctl,
    Sync,
    WriteFile {
        path: String,
    },
}

/// Shared state between the server task and test assertions.
#[derive(Debug, Default)]
struct GuestState {
    calls: Vec<Call>,
    next_pid: i32,
    exit_code: i32,
}

/// A running fake `SandboxContext` server.
#[derive(Debug, Clone)]
pub struct FakeGuest {
    state: Arc<Mutex<GuestState>>,
    socket: PathBuf,
    _dir: Arc<tempfile::TempDir>,
}

impl FakeGuest {
    /// Start the server on a unix socket in a temp dir.
    pub async fn start() -> std::io::Result<Self> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("vminitd.sock");
        let state = Arc::new(Mutex::new(GuestState {
            calls: Vec::new(),
            // Guest PIDs start well above 1 so tests can tell a real pid from a
            // zero value, and so /proc/<pid>/ns paths look plausible.
            next_pid: 100,
            exit_code: 0,
        }));

        let listener = tokio::net::UnixListener::bind(&socket)?;
        let service = FakeGuestService {
            state: state.clone(),
        };
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(SandboxContextServer::new(service))
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await;
        });

        Ok(Self {
            state,
            socket,
            _dir: Arc::new(dir),
        })
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    /// Every RPC received, in order.
    pub fn calls(&self) -> Vec<Call> {
        self.state
            .lock()
            .expect("guest state poisoned")
            .calls
            .clone()
    }

    /// Set the exit code `WaitProcess` reports.
    pub fn set_exit_code(&self, code: i32) {
        self.state.lock().expect("guest state poisoned").exit_code = code;
    }

    /// Calls filtered to `CreateProcess`, which is where specs land.
    pub fn created_processes(&self) -> Vec<(String, oci::Spec)> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::CreateProcess { id, spec, .. } => Some((id, *spec)),
                _ => None,
            })
            .collect()
    }

    /// The spec for a given process id, if it was created.
    pub fn spec_for(&self, id: &str) -> Option<oci::Spec> {
        self.created_processes()
            .into_iter()
            .find(|(pid, _)| pid == id)
            .map(|(_, spec)| spec)
    }

    /// Mount destinations, in the order the guest received them.
    pub fn mount_destinations(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::Mount { destination, .. } => Some(destination),
                _ => None,
            })
            .collect()
    }
}

#[derive(Debug)]
struct FakeGuestService {
    state: Arc<Mutex<GuestState>>,
}

impl FakeGuestService {
    fn record(&self, call: Call) {
        self.state
            .lock()
            .expect("guest state poisoned")
            .calls
            .push(call);
    }

    fn next_pid(&self) -> i32 {
        let mut state = self.state.lock().expect("guest state poisoned");
        let pid = state.next_pid;
        state.next_pid += 1;
        pid
    }
}

#[tonic::async_trait]
impl SandboxContext for FakeGuestService {
    async fn mount(
        &self,
        request: Request<pb::MountRequest>,
    ) -> std::result::Result<Response<pb::MountResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::Mount {
            type_: r.r#type,
            source: r.source,
            destination: r.destination,
            options: r.options,
        });
        Ok(Response::new(pb::MountResponse {}))
    }

    async fn umount(
        &self,
        request: Request<pb::UmountRequest>,
    ) -> std::result::Result<Response<pb::UmountResponse>, Status> {
        self.record(Call::Umount {
            path: request.into_inner().path,
        });
        Ok(Response::new(pb::UmountResponse {}))
    }

    async fn setenv(
        &self,
        request: Request<pb::SetenvRequest>,
    ) -> std::result::Result<Response<pb::SetenvResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::Setenv {
            key: r.key,
            value: r.value,
        });
        Ok(Response::new(pb::SetenvResponse {}))
    }

    async fn getenv(
        &self,
        _request: Request<pb::GetenvRequest>,
    ) -> std::result::Result<Response<pb::GetenvResponse>, Status> {
        Ok(Response::new(pb::GetenvResponse { value: None }))
    }

    async fn mkdir(
        &self,
        request: Request<pb::MkdirRequest>,
    ) -> std::result::Result<Response<pb::MkdirResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::Mkdir {
            path: r.path,
            all: r.all,
            perms: r.perms,
        });
        Ok(Response::new(pb::MkdirResponse {}))
    }

    async fn sysctl(
        &self,
        _request: Request<pb::SysctlRequest>,
    ) -> std::result::Result<Response<pb::SysctlResponse>, Status> {
        self.record(Call::Sysctl);
        Ok(Response::new(pb::SysctlResponse {}))
    }

    async fn set_time(
        &self,
        _request: Request<pb::SetTimeRequest>,
    ) -> std::result::Result<Response<pb::SetTimeResponse>, Status> {
        Ok(Response::new(pb::SetTimeResponse {}))
    }

    async fn setup_emulator(
        &self,
        _request: Request<pb::SetupEmulatorRequest>,
    ) -> std::result::Result<Response<pb::SetupEmulatorResponse>, Status> {
        Ok(Response::new(pb::SetupEmulatorResponse {}))
    }

    async fn write_file(
        &self,
        request: Request<pb::WriteFileRequest>,
    ) -> std::result::Result<Response<pb::WriteFileResponse>, Status> {
        self.record(Call::WriteFile {
            path: request.into_inner().path,
        });
        Ok(Response::new(pb::WriteFileResponse {}))
    }

    type CopyStream =
        tokio_stream::wrappers::ReceiverStream<std::result::Result<pb::CopyResponse, Status>>;

    async fn copy(
        &self,
        _request: Request<pb::CopyRequest>,
    ) -> std::result::Result<Response<Self::CopyStream>, Status> {
        Err(Status::unimplemented("copy"))
    }

    async fn stat(
        &self,
        _request: Request<pb::StatRequest>,
    ) -> std::result::Result<Response<pb::StatResponse>, Status> {
        Err(Status::unimplemented("stat"))
    }

    async fn filesystem_operation(
        &self,
        _request: Request<pb::FilesystemOperationRequest>,
    ) -> std::result::Result<Response<pb::FilesystemOperationResponse>, Status> {
        Ok(Response::new(pb::FilesystemOperationResponse {
            result: None,
        }))
    }

    async fn create_process(
        &self,
        request: Request<pb::CreateProcessRequest>,
    ) -> std::result::Result<Response<pb::CreateProcessResponse>, Status> {
        let r = request.into_inner();
        // Decode the spec the way the guest does, so a malformed spec fails the
        // test rather than being silently accepted.
        let spec: oci::Spec = serde_json::from_slice(&r.configuration)
            .map_err(|e| Status::invalid_argument(format!("bad oci spec: {e}")))?;
        self.record(Call::CreateProcess {
            id: r.id,
            container_id: r.container_id,
            oci_runtime_path: r.oci_runtime_path,
            stdout: r.stdout,
            stderr: r.stderr,
            spec: Box::new(spec),
        });
        Ok(Response::new(pb::CreateProcessResponse {}))
    }

    async fn delete_process(
        &self,
        request: Request<pb::DeleteProcessRequest>,
    ) -> std::result::Result<Response<pb::DeleteProcessResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::DeleteProcess {
            id: r.id,
            container_id: r.container_id,
        });
        Ok(Response::new(pb::DeleteProcessResponse {}))
    }

    async fn start_process(
        &self,
        request: Request<pb::StartProcessRequest>,
    ) -> std::result::Result<Response<pb::StartProcessResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::StartProcess {
            id: r.id,
            container_id: r.container_id,
        });
        Ok(Response::new(pb::StartProcessResponse {
            pid: self.next_pid(),
        }))
    }

    async fn kill_process(
        &self,
        request: Request<pb::KillProcessRequest>,
    ) -> std::result::Result<Response<pb::KillProcessResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::KillProcess {
            id: r.id,
            container_id: r.container_id,
            signal: r.signal,
        });
        Ok(Response::new(pb::KillProcessResponse { result: 0 }))
    }

    async fn wait_process(
        &self,
        request: Request<pb::WaitProcessRequest>,
    ) -> std::result::Result<Response<pb::WaitProcessResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::WaitProcess {
            id: r.id,
            container_id: r.container_id,
        });
        let exit_code = self.state.lock().expect("guest state poisoned").exit_code;
        Ok(Response::new(pb::WaitProcessResponse {
            exit_code,
            exited_at: None,
        }))
    }

    async fn resize_process(
        &self,
        request: Request<pb::ResizeProcessRequest>,
    ) -> std::result::Result<Response<pb::ResizeProcessResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::ResizeProcess {
            id: r.id,
            container_id: r.container_id,
            columns: r.columns,
            rows: r.rows,
        });
        Ok(Response::new(pb::ResizeProcessResponse {}))
    }

    async fn close_process_stdin(
        &self,
        request: Request<pb::CloseProcessStdinRequest>,
    ) -> std::result::Result<Response<pb::CloseProcessStdinResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::CloseProcessStdin {
            id: r.id,
            container_id: r.container_id,
        });
        Ok(Response::new(pb::CloseProcessStdinResponse {}))
    }

    async fn container_statistics(
        &self,
        request: Request<pb::ContainerStatisticsRequest>,
    ) -> std::result::Result<Response<pb::ContainerStatisticsResponse>, Status> {
        let r = request.into_inner();
        let containers = r
            .container_ids
            .iter()
            .map(|id| pb::ContainerStats {
                container_id: id.clone(),
                memory: Some(pb::MemoryStats {
                    usage_bytes: 1024,
                    ..Default::default()
                }),
                cpu: Some(pb::CpuStats {
                    usage_usec: 2048,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
        self.record(Call::ContainerStatistics {
            container_ids: r.container_ids,
        });
        Ok(Response::new(pb::ContainerStatisticsResponse {
            containers,
        }))
    }

    async fn proxy_vsock(
        &self,
        _request: Request<pb::ProxyVsockRequest>,
    ) -> std::result::Result<Response<pb::ProxyVsockResponse>, Status> {
        Ok(Response::new(pb::ProxyVsockResponse {}))
    }

    async fn stop_vsock_proxy(
        &self,
        _request: Request<pb::StopVsockProxyRequest>,
    ) -> std::result::Result<Response<pb::StopVsockProxyResponse>, Status> {
        Ok(Response::new(pb::StopVsockProxyResponse {}))
    }

    async fn ip_link_set(
        &self,
        request: Request<pb::IpLinkSetRequest>,
    ) -> std::result::Result<Response<pb::IpLinkSetResponse>, Status> {
        let r = request.into_inner();
        // `standard_setup` brings up `lo` first; keep that distinguishable from
        // the pod's own interface configuration.
        if r.interface == "lo" && r.up {
            self.record(Call::StandardSetupUp {
                interface: r.interface,
            });
        } else {
            self.record(Call::IpLinkSet {
                interface: r.interface,
                up: r.up,
                mtu: r.mtu,
            });
        }
        Ok(Response::new(pb::IpLinkSetResponse {}))
    }

    async fn ip_addr_add(
        &self,
        request: Request<pb::IpAddrAddRequest>,
    ) -> std::result::Result<Response<pb::IpAddrAddResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::IpAddrAdd {
            interface: r.interface,
            ipv4: r.ipv4_address,
        });
        Ok(Response::new(pb::IpAddrAddResponse {}))
    }

    async fn ip_route_add_link(
        &self,
        _request: Request<pb::IpRouteAddLinkRequest>,
    ) -> std::result::Result<Response<pb::IpRouteAddLinkResponse>, Status> {
        Ok(Response::new(pb::IpRouteAddLinkResponse {}))
    }

    async fn ip_route_add_default(
        &self,
        request: Request<pb::IpRouteAddDefaultRequest>,
    ) -> std::result::Result<Response<pb::IpRouteAddDefaultResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::IpRouteAddDefault {
            interface: r.interface,
            gateway: r.ipv4_gateway,
        });
        Ok(Response::new(pb::IpRouteAddDefaultResponse {}))
    }

    async fn configure_dns(
        &self,
        request: Request<pb::ConfigureDnsRequest>,
    ) -> std::result::Result<Response<pb::ConfigureDnsResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::ConfigureDns {
            location: r.location,
            nameservers: r.nameservers,
            search_domains: r.search_domains,
        });
        Ok(Response::new(pb::ConfigureDnsResponse {}))
    }

    async fn configure_hosts(
        &self,
        request: Request<pb::ConfigureHostsRequest>,
    ) -> std::result::Result<Response<pb::ConfigureHostsResponse>, Status> {
        let r = request.into_inner();
        self.record(Call::ConfigureHosts {
            location: r.location,
            entries: r
                .entries
                .into_iter()
                .map(|e| (e.ip_address, e.hostnames))
                .collect(),
        });
        Ok(Response::new(pb::ConfigureHostsResponse {}))
    }

    async fn sync(
        &self,
        _request: Request<pb::SyncRequest>,
    ) -> std::result::Result<Response<pb::SyncResponse>, Status> {
        self.record(Call::Sync);
        Ok(Response::new(pb::SyncResponse {}))
    }

    async fn kill(
        &self,
        _request: Request<pb::KillRequest>,
    ) -> std::result::Result<Response<pb::KillResponse>, Status> {
        Ok(Response::new(pb::KillResponse { result: 0 }))
    }
}

/// What the mock VMM was asked to do.
#[derive(Debug, Clone, PartialEq)]
pub enum VmmCall {
    Create { config: VmConfig },
    Start,
    Stop,
    Hotplug { id: String, source: PathBuf },
    ReleaseHotplug { id: String },
    Dial { port: u32 },
}

#[derive(Debug, Default)]
struct MockVmState {
    calls: Vec<VmmCall>,
    mounts: HashMap<String, Vec<AttachedFilesystem>>,
    state: Option<VmState>,
    /// Index for handing out /dev/vdb, /dev/vdc, … as hotplug lands.
    next_device: usize,
}

/// A [`Vmm`] that records what it was asked for and routes every vsock dial to a
/// [`FakeGuest`]. No hypervisor involved.
#[derive(Debug, Clone)]
pub struct MockVmm {
    guest: FakeGuest,
    state: Arc<Mutex<MockVmState>>,
}

impl MockVmm {
    pub fn new(guest: FakeGuest) -> Self {
        Self {
            guest,
            state: Arc::new(Mutex::new(MockVmState {
                next_device: 1,
                ..Default::default()
            })),
        }
    }

    pub fn calls(&self) -> Vec<VmmCall> {
        self.state.lock().expect("vmm state poisoned").calls.clone()
    }

    /// Pre-seed the attachment table, as a boot-time attach would.
    pub fn attach_at_boot(&self, id: &str, attached: AttachedFilesystem) {
        self.state
            .lock()
            .expect("vmm state poisoned")
            .mounts
            .entry(id.to_string())
            .or_default()
            .push(attached);
    }
}

#[async_trait]
impl Vmm for MockVmm {
    async fn create(&self, config: VmConfig) -> Result<Arc<dyn VmInstance>> {
        {
            let mut state = self.state.lock().expect("vmm state poisoned");
            state.calls.push(VmmCall::Create {
                config: config.clone(),
            });
            state.state = Some(VmState::Stopped);
        }
        // Boot-time mounts become attachments immediately, mirroring the VMM
        // building `mountsByID` into the machine configuration before start.
        for (id, blocks) in &config.mounts_by_id {
            for block in blocks {
                let device = {
                    let mut state = self.state.lock().expect("vmm state poisoned");
                    let n = state.next_device;
                    state.next_device += 1;
                    format!("/dev/vd{}", (b'a' + n as u8) as char)
                };
                self.attach_at_boot(
                    id,
                    AttachedFilesystem {
                        type_: block.format.clone(),
                        source: device,
                        destination: block.destination.clone(),
                        options: block.options.clone(),
                    },
                );
            }
        }
        Ok(Arc::new(MockVm {
            guest: self.guest.clone(),
            state: self.state.clone(),
        }))
    }
}

#[derive(Debug)]
struct MockVm {
    guest: FakeGuest,
    state: Arc<Mutex<MockVmState>>,
}

impl MockVm {
    fn record(&self, call: VmmCall) {
        self.state
            .lock()
            .expect("vmm state poisoned")
            .calls
            .push(call);
    }
}

#[async_trait]
impl VmInstance for MockVm {
    async fn start(&self) -> Result<()> {
        self.record(VmmCall::Start);
        self.state.lock().expect("vmm state poisoned").state = Some(VmState::Running);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.record(VmmCall::Stop);
        self.state.lock().expect("vmm state poisoned").state = Some(VmState::Stopped);
        Ok(())
    }

    async fn state(&self) -> VmState {
        self.state
            .lock()
            .expect("vmm state poisoned")
            .state
            .unwrap_or(VmState::Unknown)
    }

    async fn dial(&self, port: u32) -> Result<UnixStream> {
        self.record(VmmCall::Dial { port });
        Ok(UnixStream::connect(self.guest.socket_path()).await?)
    }

    async fn listen(&self, _port: u32) -> Result<PathBuf> {
        Ok(self.guest.socket_path().to_path_buf())
    }

    async fn hotplug(&self, block: BlockMount, id: &str) -> Result<AttachedFilesystem> {
        let device = {
            let mut state = self.state.lock().expect("vmm state poisoned");
            state.calls.push(VmmCall::Hotplug {
                id: id.to_string(),
                source: block.source.clone(),
            });
            let n = state.next_device;
            state.next_device += 1;
            format!("/dev/vd{}", (b'a' + n as u8) as char)
        };
        Ok(AttachedFilesystem {
            type_: block.format,
            source: device,
            destination: block.destination,
            options: block.options,
        })
    }

    async fn release_hotplug(&self, id: &str) -> Result<()> {
        let mut state = self.state.lock().expect("vmm state poisoned");
        state
            .calls
            .push(VmmCall::ReleaseHotplug { id: id.to_string() });
        state.mounts.remove(id);
        Ok(())
    }

    async fn mounts(&self) -> HashMap<String, Vec<AttachedFilesystem>> {
        self.state
            .lock()
            .expect("vmm state poisoned")
            .mounts
            .clone()
    }

    async fn register_mounts(
        &self,
        id: &str,
        rootfs: AttachedFilesystem,
        additional: Vec<AttachedFilesystem>,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("vmm state poisoned");
        let entry = state.mounts.entry(id.to_string()).or_default();
        // Rootfs must be element 0: the pod drops the first entry when building
        // a container's mount list.
        entry.insert(0, rootfs);
        entry.extend(additional);
        Ok(())
    }
}

/// A broker that speaks the real [`crate::broker`] wire format, backed by
/// [`MockVmm`].
///
/// This exists to validate the protocol without a hypervisor: a [`crate::Pod`]
/// driven through [`crate::BrokerVmm`] against this server exercises the exact
/// JSON both sides will exchange, then the exact `SandboxContext` gRPC the guest
/// will serve. Anything the Swift broker gets wrong is a mismatch against a
/// contract that is already covered here.
#[derive(Debug, Clone)]
pub struct FakeBroker {
    socket: PathBuf,
    guest: FakeGuest,
    requests: Arc<Mutex<Vec<crate::broker::Method>>>,
    _dir: Arc<tempfile::TempDir>,
}

impl FakeBroker {
    /// Start the broker on a unix socket in a temp dir.
    pub async fn start(guest: FakeGuest) -> std::io::Result<Self> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("broker.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let requests = Arc::new(Mutex::new(Vec::new()));

        let vmm = MockVmm::new(guest.clone());
        let state: Arc<tokio::sync::Mutex<HashMap<String, Arc<dyn VmInstance>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let guest_socket = guest.socket_path().to_path_buf();
        let recorded = requests.clone();

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let vmm = vmm.clone();
                let state = state.clone();
                let guest_socket = guest_socket.clone();
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let response = match serde_json::from_str::<crate::broker::Request>(&line) {
                        Ok(request) => {
                            recorded
                                .lock()
                                .expect("broker request log poisoned")
                                .push(request.method);
                            Self::handle(&vmm, &state, &guest_socket, request).await
                        }
                        Err(e) => crate::broker::Response::err(format!("bad request: {e}")),
                    };
                    let mut out = serde_json::to_vec(&response).unwrap_or_default();
                    out.push(b'\n');
                    let _ = reader.get_mut().write_all(&out).await;
                    let _ = reader.get_mut().flush().await;
                });
            }
        });

        Ok(Self {
            socket,
            guest,
            requests,
            _dir: Arc::new(dir),
        })
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    pub fn guest(&self) -> &FakeGuest {
        &self.guest
    }

    /// Methods received, in order.
    pub fn requests(&self) -> Vec<crate::broker::Method> {
        self.requests
            .lock()
            .expect("broker request log poisoned")
            .clone()
    }

    async fn handle(
        vmm: &MockVmm,
        state: &Arc<tokio::sync::Mutex<HashMap<String, Arc<dyn VmInstance>>>>,
        guest_socket: &std::path::Path,
        request: crate::broker::Request,
    ) -> crate::broker::Response {
        use crate::broker::{Method, Reply, Response};

        let vm_id = request.params.vm_id.clone().unwrap_or_default();
        let lookup = || async {
            state
                .lock()
                .await
                .get(&vm_id)
                .cloned()
                .ok_or_else(|| format!("no such vm: {vm_id}"))
        };

        match request.method {
            Method::CreateVm => {
                let Some(config) = request.params.config else {
                    return Response::err("createVm requires config");
                };
                match vmm.create(config).await {
                    Ok(vm) => {
                        state.lock().await.insert(vm_id, vm);
                        Response::ok(Reply::default())
                    }
                    Err(e) => Response::err(e),
                }
            }
            Method::Start => match lookup().await {
                Ok(vm) => match vm.start().await {
                    Ok(()) => Response::ok(Reply::default()),
                    Err(e) => Response::err(e),
                },
                Err(e) => Response::err(e),
            },
            Method::Stop => match lookup().await {
                Ok(vm) => match vm.stop().await {
                    Ok(()) => Response::ok(Reply::default()),
                    Err(e) => Response::err(e),
                },
                Err(e) => Response::err(e),
            },
            Method::State => match lookup().await {
                Ok(vm) => Response::ok(Reply {
                    state: Some(vm.state().await),
                    ..Default::default()
                }),
                Err(e) => Response::err(e),
            },
            // The relay a real broker would stand up per dial; here the guest's
            // own socket already is one.
            Method::Dial | Method::Listen => Response::ok(Reply {
                socket_path: Some(guest_socket.to_path_buf()),
                ..Default::default()
            }),
            Method::Hotplug => {
                let (Some(block), Some(owner)) = (request.params.block, request.params.owner_id)
                else {
                    return Response::err("hotplug requires block and ownerId");
                };
                match lookup().await {
                    Ok(vm) => match vm.hotplug(block, &owner).await {
                        Ok(attached) => Response::ok(Reply {
                            attached: Some(attached),
                            ..Default::default()
                        }),
                        Err(e) => Response::err(e),
                    },
                    Err(e) => Response::err(e),
                }
            }
            Method::ReleaseHotplug => {
                let Some(owner) = request.params.owner_id else {
                    return Response::err("releaseHotplug requires ownerId");
                };
                match lookup().await {
                    Ok(vm) => match vm.release_hotplug(&owner).await {
                        Ok(()) => Response::ok(Reply::default()),
                        Err(e) => Response::err(e),
                    },
                    Err(e) => Response::err(e),
                }
            }
            Method::Mounts => match lookup().await {
                Ok(vm) => Response::ok(Reply {
                    mounts: Some(vm.mounts().await),
                    ..Default::default()
                }),
                Err(e) => Response::err(e),
            },
            Method::RegisterMounts => {
                let (Some(owner), Some(rootfs)) = (request.params.owner_id, request.params.rootfs)
                else {
                    return Response::err("registerMounts requires ownerId and rootfs");
                };
                let additional = request.params.additional.unwrap_or_default();
                match lookup().await {
                    Ok(vm) => match vm.register_mounts(&owner, rootfs, additional).await {
                        Ok(()) => Response::ok(Reply::default()),
                        Err(e) => Response::err(e),
                    },
                    Err(e) => Response::err(e),
                }
            }
            Method::ProvisionRootfs => {
                let Some(owner) = request.params.owner_id else {
                    return Response::err("provisionRootfs requires ownerId");
                };
                // A real broker unpacks the image into an ext4 file here.
                Response::ok(Reply {
                    block: Some(BlockMount::block("ext4", format!("/images/{owner}.ext4"))),
                    ..Default::default()
                })
            }
            Method::ReleaseRootfs => Response::ok(Reply::default()),
        }
    }
}

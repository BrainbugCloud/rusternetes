//! A fake VMM broker, so the CRI translation can be tested without a hypervisor.
//!
//! [`FakeBroker`] speaks the real [`crate::broker`] wire format over a unix
//! socket and records every call it receives.
//!
//! It used to be a fake *guest*: a real `SandboxContext` gRPC server that a Rust
//! `Pod` drove, asserting the exact OCI spec bytes. That went away with the Rust
//! `LinuxPod` port — pod semantics are the broker's now, and the broker is Swift
//! calling Apple's own `LinuxPod`. What remains assertable on this side is the
//! call sequence and the payloads, which is precisely what `apple-cri`'s
//! `pod_runtime` is responsible for.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::broker::{BlockMount, ImageWire};

/// A broker that speaks the real [`crate::broker`] wire format.
///
/// This exists to validate the protocol without a hypervisor. Since the broker
/// became pod-shaped — it owns Apple's `LinuxPod`, so pod semantics live in
/// Swift — what Rust can still hold itself to is the **call sequence and the
/// payload shapes** it sends. That is what this fake records.
///
/// It keeps just enough state to make the interesting errors real: an unknown
/// pod or container is reported rather than silently accepted, and it remembers
/// whether a container was added *before* the sandbox booted (attached at boot)
/// or *after* (hotplugged into a live VM) — the distinction an init container or
/// a sidecar actually exercises.
#[derive(Debug, Clone)]
pub struct FakeBroker {
    socket: PathBuf,
    state: Arc<Mutex<BrokerState>>,
    calls: Arc<Mutex<Vec<(crate::broker::Method, crate::broker::Params)>>>,
    _dir: Arc<tempfile::TempDir>,
}

#[derive(Debug, Default)]
struct BrokerState {
    pods: HashMap<String, PodEntry>,
    /// Containers added after their pod was booted, in order.
    hotplugged: Vec<String>,
    /// Overrides for `waitContainer` / `waitProcess`, keyed by container or
    /// process id.
    exit_codes: HashMap<String, i32>,
    /// Live exec'd process ids.
    processes: Vec<String>,
    /// Containers a client has attached to, in order.
    attached: Vec<String>,
    /// Addresses handed out by `createPod`, keyed by pod id.
    addresses: HashMap<String, String>,
    /// The broker's image store, keyed by reference.
    images: HashMap<String, ImageWire>,
    /// Canned `(stdout, stderr)` an exec should produce, keyed by process id.
    exec_output: HashMap<String, (Vec<u8>, Vec<u8>)>,
    next_pid: i32,
}

#[derive(Debug, Default)]
struct PodEntry {
    /// `create` has been called: the VM is up.
    booted: bool,
    /// Insertion order, which `listContainers` must preserve.
    containers: Vec<String>,
    running: Vec<String>,
}

impl FakeBroker {
    /// Start the broker on a unix socket in a temp dir.
    pub async fn start() -> std::io::Result<Self> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("broker.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let state = Arc::new(Mutex::new(BrokerState {
            next_pid: 200,
            ..Default::default()
        }));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let served_state = state.clone();
        let served_calls = calls.clone();
        let relay = dir.path().join("relay.sock");

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = served_state.clone();
                let calls = served_calls.clone();
                let relay = relay.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let response = match serde_json::from_str::<crate::broker::Request>(&line) {
                        Ok(request) => {
                            calls
                                .lock()
                                .expect("broker call log poisoned")
                                .push((request.method, request.params.clone()));
                            Self::handle(&state, &relay, request)
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
            state,
            calls,
            _dir: Arc::new(dir),
        })
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    /// Methods received, in order.
    pub fn requests(&self) -> Vec<crate::broker::Method> {
        self.calls
            .lock()
            .expect("broker call log poisoned")
            .iter()
            .map(|(method, _)| *method)
            .collect()
    }

    /// Methods *and* their parameters, in order — for asserting payloads, not
    /// just ordering.
    pub fn calls(&self) -> Vec<(crate::broker::Method, crate::broker::Params)> {
        self.calls.lock().expect("broker call log poisoned").clone()
    }

    /// Parameters of every call to `method`, in order.
    pub fn params_for(&self, method: crate::broker::Method) -> Vec<crate::broker::Params> {
        self.calls
            .lock()
            .expect("broker call log poisoned")
            .iter()
            .filter(|(m, _)| *m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }

    /// What `waitContainer` should answer for `container_id`. Defaults to 0.
    pub fn set_exit_code(&self, container_id: &str, code: i32) {
        self.state
            .lock()
            .expect("broker state poisoned")
            .exit_codes
            .insert(container_id.to_string(), code);
    }

    /// What an exec'd process should write to its stdout/stderr files.
    pub fn set_exec_output(&self, process_id: &str, stdout: &[u8], stderr: &[u8]) {
        self.state
            .lock()
            .expect("broker state poisoned")
            .exec_output
            .insert(process_id.to_string(), (stdout.to_vec(), stderr.to_vec()));
    }

    /// Containers added *after* their pod booted, i.e. hotplugged into a live
    /// sandbox. This is the CRI ordering a second container always takes.
    pub fn hotplugged(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("broker state poisoned")
            .hotplugged
            .clone()
    }

    /// Containers a client has attached to, in order.
    pub fn attached(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("broker state poisoned")
            .attached
            .clone()
    }

    /// Containers currently running in `pod_id`.
    pub fn running(&self, pod_id: &str) -> Vec<String> {
        self.state
            .lock()
            .expect("broker state poisoned")
            .pods
            .get(pod_id)
            .map(|p| p.running.clone())
            .unwrap_or_default()
    }

    fn handle(
        state: &Arc<Mutex<BrokerState>>,
        relay: &std::path::Path,
        request: crate::broker::Request,
    ) -> crate::broker::Response {
        use crate::broker::{ContainerStatsWire, Method, Reply, Response};

        let params = request.params;
        let mut state = state.lock().expect("broker state poisoned");

        // Every pod-scoped method needs a pod that exists; answering "no such
        // pod" rather than a cheerful `{}` is the whole point of keeping state.
        let pod_id = params.pod_id.clone().unwrap_or_default();
        macro_rules! pod {
            () => {
                match state.pods.get_mut(&pod_id) {
                    Some(pod) => pod,
                    None => return Response::err(format!("no such pod: {pod_id}")),
                }
            };
        }
        macro_rules! container_id {
            ($method:literal) => {
                match params.container_id.clone() {
                    Some(id) => id,
                    None => return Response::err(concat!($method, " requires containerId")),
                }
            };
        }

        match request.method {
            Method::CreatePod => {
                let Some(config) = params.config else {
                    return Response::err("createPod requires config");
                };
                state.pods.insert(config.id.clone(), PodEntry::default());
                // A real broker allocates from its vmnet subnet; the fake hands
                // out the same range deterministically so callers can assert on
                // the address reaching CRI.
                let host = 200 + state.pods.len() - 1;
                let address = format!("192.168.64.{host}");
                state.addresses.insert(config.id.clone(), address.clone());
                Response::ok(Reply {
                    ipv4: Some(address),
                    ..Default::default()
                })
            }
            Method::Create => {
                pod!().booted = true;
                Response::ok(Reply::default())
            }
            Method::StopPod => {
                // Idempotent, matching PodService: a pod we no longer know about
                // is already gone.
                state.pods.remove(&pod_id);
                state.addresses.remove(&pod_id);
                Response::ok(Reply::default())
            }
            Method::AddContainer => {
                let Some(container) = params.container.clone() else {
                    return Response::err("addContainer requires container");
                };
                let id = container.id.clone();
                let booted = {
                    let pod = pod!();
                    pod.containers.push(id.clone());
                    pod.booted
                };
                if booted {
                    state.hotplugged.push(id);
                }
                Response::ok(Reply::default())
            }
            Method::StartContainer => {
                let id = container_id!("startContainer");
                let pod = pod!();
                if !pod.containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                if !pod.running.contains(&id) {
                    pod.running.push(id);
                }
                Response::ok(Reply::default())
            }
            Method::StopContainer | Method::KillContainer => {
                let id = container_id!("stopContainer");
                let pod = pod!();
                if !pod.containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                pod.running.retain(|c| *c != id);
                Response::ok(Reply::default())
            }
            Method::WaitContainer => {
                let id = container_id!("waitContainer");
                if !pod!().containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                // A real `waitContainer` blocks until the process exits, which a
                // synchronous fake cannot do — so "no exit code was declared"
                // stands in for "still running", and the wait fails rather than
                // inventing a 0. Tests that want a container to have exited say
                // so with `set_exit_code`.
                let Some(code) = state.exit_codes.get(&id).copied() else {
                    return Response::err(format!("container {id} has not exited"));
                };
                // Waiting is what reaps it: an exited container is not running.
                pod!().running.retain(|c| *c != id);
                Response::ok(Reply {
                    exit_code: Some(code),
                    ..Default::default()
                })
            }
            Method::ListContainers => {
                let containers = pod!().containers.clone();
                Response::ok(Reply {
                    container_ids: Some(containers),
                    ..Default::default()
                })
            }
            Method::Exec => {
                let id = container_id!("exec");
                let Some(process_id) = params.process_id.clone() else {
                    return Response::err("exec requires processId");
                };
                if process_id == id {
                    // The guest keys a container's init process by the container
                    // id, so an exec reusing it would address the wrong process.
                    return Response::err("exec processId must differ from containerId");
                }
                if !pod!().running.contains(&id) {
                    return Response::err(format!("container is not running: {id}"));
                }
                state.next_pid += 1;
                let pid = state.next_pid;
                // A real broker attaches the process's Writers to these files.
                // Writing the canned output here is what lets an `ExecSync` be
                // tested end to end rather than only up to the exit code.
                let output = state.exec_output.get(&process_id).cloned();
                for (path, bytes) in [
                    (params.stdout_path.clone(), output.clone().map(|o| o.0)),
                    (params.stderr_path.clone(), output.map(|o| o.1)),
                ] {
                    if let Some(path) = path {
                        if let Some(parent) = path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        if let Err(e) = std::fs::write(&path, bytes.unwrap_or_default()) {
                            return Response::err(format!("writing {}: {e}", path.display()));
                        }
                    }
                }
                state.processes.push(process_id);
                Response::ok(Reply {
                    pid: Some(pid),
                    ..Default::default()
                })
            }
            Method::WaitProcess => {
                let _ = container_id!("waitProcess");
                let Some(process_id) = params.process_id.clone() else {
                    return Response::err("waitProcess requires processId");
                };
                if !state.processes.contains(&process_id) {
                    return Response::err(format!("no such process: {process_id}"));
                }
                let code = state.exit_codes.get(&process_id).copied().unwrap_or(0);
                state.processes.retain(|p| *p != process_id);
                Response::ok(Reply {
                    exit_code: Some(code),
                    ..Default::default()
                })
            }
            Method::KillProcess => {
                let _ = container_id!("killProcess");
                let Some(process_id) = params.process_id.clone() else {
                    return Response::err("killProcess requires processId");
                };
                if params.signal.is_none() {
                    return Response::err("killProcess requires signal");
                }
                if !state.processes.contains(&process_id) {
                    return Response::err(format!("no such process: {process_id}"));
                }
                Response::ok(Reply::default())
            }
            Method::Statistics => {
                let requested = params.container_ids.clone().unwrap_or_default();
                let pod = pod!();
                let ids = if requested.is_empty() {
                    pod.containers.clone()
                } else {
                    requested
                };
                Response::ok(Reply {
                    stats: Some(
                        ids.into_iter()
                            .map(|id| ContainerStatsWire {
                                id,
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                })
            }
            Method::ProvisionRootfs => {
                let Some(owner) = params.owner_id else {
                    return Response::err("provisionRootfs requires ownerId");
                };
                // A real broker unpacks the image into an ext4 file here.
                Response::ok(Reply {
                    block: Some(BlockMount::block("ext4", format!("/images/{owner}.ext4"))),
                    ..Default::default()
                })
            }
            Method::Attach => {
                let id = container_id!("attach");
                if !pod!().containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                // A real broker connects to each path; the fake records that it
                // was asked, which is what the CRI translation is responsible for.
                state.attached.push(id);
                Response::ok(Reply::default())
            }
            Method::Resize => {
                let _ = container_id!("resize");
                let Some(process_id) = params.process_id.clone() else {
                    return Response::err("resize requires processId");
                };
                if params.width.is_none() || params.height.is_none() {
                    return Response::err("resize requires width and height");
                }
                if !state.processes.contains(&process_id) {
                    return Response::err(format!("no such process: {process_id}"));
                }
                Response::ok(Reply::default())
            }
            Method::CloseStdin => {
                let id = container_id!("closeStdin");
                if !pod!().containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                Response::ok(Reply::default())
            }
            Method::ReopenContainerLog => {
                let id = container_id!("reopenContainerLog");
                if !pod!().containers.contains(&id) {
                    return Response::err(format!("no such container: {id}"));
                }
                Response::ok(Reply::default())
            }
            Method::ListImages => Response::ok(Reply {
                images: Some(state.images.values().cloned().collect()),
                ..Default::default()
            }),
            Method::ImageStatus => {
                let Some(reference) = params.image.clone() else {
                    return Response::err("imageStatus requires image");
                };
                // Absence is an empty answer, not an error — CRI reports "not
                // present" rather than failing.
                Response::ok(Reply {
                    image: state.images.get(&reference).cloned(),
                    ..Default::default()
                })
            }
            Method::PullImage => {
                let Some(reference) = params.image.clone() else {
                    return Response::err("pullImage requires image");
                };
                let image = ImageWire {
                    reference: reference.clone(),
                    digest: format!("sha256:{:064x}", state.images.len() + 1),
                    size_bytes: 1024,
                    ..Default::default()
                };
                state.images.insert(reference, image.clone());
                Response::ok(Reply {
                    image: Some(image),
                    ..Default::default()
                })
            }
            Method::RemoveImage => {
                let Some(reference) = params.image.clone() else {
                    return Response::err("removeImage requires image");
                };
                // Removing what is already gone is success, as CRI requires.
                state.images.remove(&reference);
                Response::ok(Reply::default())
            }
            Method::ImageFsInfo => Response::ok(Reply {
                fs_path: Some("/tmp/fake-broker/images".to_string()),
                fs_bytes: Some(state.images.values().map(|i| i.size_bytes).sum()),
                ..Default::default()
            }),
            Method::ReleaseRootfs => Response::ok(Reply::default()),
            // The relay socket a real broker stands up per dial.
            Method::Dial => Response::ok(Reply {
                socket_path: Some(relay.to_path_buf()),
                ..Default::default()
            }),
        }
    }
}

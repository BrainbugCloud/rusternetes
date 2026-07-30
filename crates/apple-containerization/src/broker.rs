//! The VMM broker protocol, and a [`Vmm`] implementation that speaks it.
//!
//! [`crate::vmm`] explains *why* a broker exists: four host-side operations —
//! VM lifecycle, vsock dial/listen, block hotplug and virtiofs shares — are only
//! reachable through Virtualization.framework, from the process that owns the
//! `VZVirtualMachine`. A Rust process cannot own one, so a helper does, and this
//! module is the client half.
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
//! -> {"method":"start","params":{"vmId":"pod-1"}}
//! <- {"ok":{}}
//! -> {"method":"dial","params":{"vmId":"pod-1","port":1024}}
//! <- {"ok":{"socketPath":"/tmp/broker/pod-1/vsock-1024-7"}}
//! ```
//!
//! Bulk data never crosses this socket. `dial` and `listen` return a *path* to a
//! dedicated unix socket that the broker relays to the guest vsock port, which is
//! the shape [`crate::vmm::VmInstance`] is defined in terms of and mirrors
//! upstream's `Vminitd.init(connection: FileHandle,…)` taking an already-connected
//! fd.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::{Error, Result};
use crate::vmm::{AttachedFilesystem, BlockMount, VmConfig, VmInstance, VmState, Vmm};

/// A request to the broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub method: Method,
    pub params: Params,
}

/// The broker's method set. Deliberately minimal: anything expressible as
/// `SandboxContext` gRPC belongs in [`crate::agent`], not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Method {
    /// Build a VM from a [`VmConfig`]. Does not boot it.
    CreateVm,
    Start,
    Stop,
    State,
    /// Connect to a guest vsock port; answers with a relay socket path.
    Dial,
    /// Accept guest-initiated connections on a host vsock port.
    Listen,
    /// Attach a block device to a running VM.
    Hotplug,
    ReleaseHotplug,
    /// The VM's attachment table.
    Mounts,
    RegisterMounts,
    /// Materialise a container image as a block device (see
    /// [`BrokerRootfs`]).
    ProvisionRootfs,
    ReleaseRootfs,
}

/// Union of every method's parameters. A flat, all-optional shape keeps the
/// Swift side's decoding trivial — it reads only the fields its method needs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Params {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<VmConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,
    /// Container id, or the pod id for pod-level volumes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockMount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rootfs: Option<AttachedFilesystem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional: Option<Vec<AttachedFilesystem>>,
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
    /// `dial` / `listen`: the relay socket to connect to or accept on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<VmState>,
    /// `hotplug` / `provisionRootfs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attached: Option<AttachedFilesystem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockMount>,
    /// `mounts`, keyed by owner id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounts: Option<HashMap<String, Vec<AttachedFilesystem>>>,
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

/// A [`Vmm`] backed by the broker.
#[derive(Debug, Clone)]
pub struct BrokerVmm {
    client: BrokerClient,
}

impl BrokerVmm {
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
}

#[async_trait]
impl Vmm for BrokerVmm {
    async fn create(&self, config: VmConfig) -> Result<Arc<dyn VmInstance>> {
        let vm_id = config.id.clone();
        self.client
            .call(
                Method::CreateVm,
                Params {
                    vm_id: Some(vm_id.clone()),
                    config: Some(config),
                    ..Default::default()
                },
            )
            .await?;
        Ok(Arc::new(BrokerVm {
            client: self.client.clone(),
            vm_id,
        }))
    }
}

/// One VM, addressed by id on every call.
#[derive(Debug, Clone)]
pub struct BrokerVm {
    client: BrokerClient,
    vm_id: String,
}

impl BrokerVm {
    fn params(&self) -> Params {
        Params {
            vm_id: Some(self.vm_id.clone()),
            ..Default::default()
        }
    }
}

#[async_trait]
impl VmInstance for BrokerVm {
    async fn start(&self) -> Result<()> {
        self.client.call(Method::Start, self.params()).await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.client.call(Method::Stop, self.params()).await?;
        Ok(())
    }

    async fn state(&self) -> VmState {
        // Infallible by signature; an unreachable broker is reported as Unknown
        // rather than papered over as Stopped, which callers treat as terminal.
        match self.client.call(Method::State, self.params()).await {
            Ok(reply) => reply.state.unwrap_or(VmState::Unknown),
            Err(e) => {
                tracing::warn!(vm = %self.vm_id, error = %e, "vmm broker state query failed");
                VmState::Unknown
            }
        }
    }

    async fn dial(&self, port: u32) -> Result<UnixStream> {
        let reply = self
            .client
            .call(
                Method::Dial,
                Params {
                    port: Some(port),
                    ..self.params()
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

    async fn listen(&self, port: u32) -> Result<PathBuf> {
        let reply = self
            .client
            .call(
                Method::Listen,
                Params {
                    port: Some(port),
                    ..self.params()
                },
            )
            .await?;
        reply
            .socket_path
            .ok_or_else(|| Error::vmm(format!("listen({port}): broker returned no socketPath")))
    }

    async fn hotplug(&self, block: BlockMount, id: &str) -> Result<AttachedFilesystem> {
        let reply = self
            .client
            .call(
                Method::Hotplug,
                Params {
                    owner_id: Some(id.to_string()),
                    block: Some(block),
                    ..self.params()
                },
            )
            .await?;
        reply
            .attached
            .ok_or_else(|| Error::vmm(format!("hotplug({id}): broker returned no attachment")))
    }

    async fn release_hotplug(&self, id: &str) -> Result<()> {
        self.client
            .call(
                Method::ReleaseHotplug,
                Params {
                    owner_id: Some(id.to_string()),
                    ..self.params()
                },
            )
            .await?;
        Ok(())
    }

    async fn mounts(&self) -> HashMap<String, Vec<AttachedFilesystem>> {
        match self.client.call(Method::Mounts, self.params()).await {
            Ok(reply) => reply.mounts.unwrap_or_default(),
            Err(e) => {
                tracing::warn!(vm = %self.vm_id, error = %e, "vmm broker mounts query failed");
                HashMap::new()
            }
        }
    }

    async fn register_mounts(
        &self,
        id: &str,
        rootfs: AttachedFilesystem,
        additional: Vec<AttachedFilesystem>,
    ) -> Result<()> {
        self.client
            .call(
                Method::RegisterMounts,
                Params {
                    owner_id: Some(id.to_string()),
                    rootfs: Some(rootfs),
                    additional: Some(additional),
                    ..self.params()
                },
            )
            .await?;
        Ok(())
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
                vm_id: Some("pod-1".to_string()),
                port: Some(1024),
                ..Default::default()
            },
        };
        let line = serde_json::to_string(&request).unwrap();
        // Unset fields are omitted, so the Swift side decodes a small object.
        assert!(!line.contains("config"), "{line}");
        assert!(line.contains(r#""method":"dial""#), "{line}");
        assert!(line.contains(r#""vmId":"pod-1""#), "{line}");
        assert_eq!(serde_json::from_str::<Request>(&line).unwrap(), request);
    }

    #[test]
    fn methods_serialise_as_camel_case_names() {
        let name = |m: Method| serde_json::to_string(&m).unwrap();
        assert_eq!(name(Method::CreateVm), r#""createVm""#);
        assert_eq!(name(Method::ReleaseHotplug), r#""releaseHotplug""#);
        assert_eq!(name(Method::ProvisionRootfs), r#""provisionRootfs""#);
    }

    #[test]
    fn vm_state_serialises_lowercase() {
        assert_eq!(
            serde_json::to_string(&VmState::Running).unwrap(),
            r#""running""#
        );
        assert_eq!(
            serde_json::from_str::<VmState>(r#""stopped""#).unwrap(),
            VmState::Stopped
        );
    }

    #[test]
    fn an_error_response_is_distinguishable_from_a_success() {
        let ok: Response = serde_json::from_str(r#"{"ok":{}}"#).unwrap();
        assert!(ok.ok.is_some() && ok.error.is_none());
        let err: Response = serde_json::from_str(r#"{"error":"no such vm"}"#).unwrap();
        assert!(err.ok.is_none() && err.error.as_deref() == Some("no such vm"));
    }

    #[test]
    fn a_vm_config_survives_the_wire() {
        let mut config = VmConfig::new("pod-1");
        config.cpus = 2;
        config.mounts_by_id.insert(
            "c1".to_string(),
            vec![BlockMount::block("ext4", "/images/c1.ext4")],
        );
        let json = serde_json::to_string(&config).unwrap();
        let back: VmConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, back);
        // camelCase, so the Swift side's Codable defaults line up.
        assert!(json.contains("memoryInBytes"), "{json}");
        assert!(json.contains("mountsById"), "{json}");
    }
}

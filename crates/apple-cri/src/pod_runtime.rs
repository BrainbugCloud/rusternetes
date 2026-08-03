// SPDX-License-Identifier: Apache-2.0

//! CRI over **real pod sandboxes**: one microVM per pod, N containers inside it.
//!
//! The default path in this crate ([`crate::sandbox`], [`crate::container`])
//! drives Apple's `container` CLI, which gives one microVM per *container*. That
//! is enough for critest — every conformance spec it runs is single-container —
//! but it is not a Kubernetes pod: containers get no shared `localhost`, no
//! single pod IP, and no shared IPC.
//!
//! This module is the pod-shaped path. It translates CRI onto the VMM broker's
//! pod protocol ([`apple_containerization::PodBroker`]), which the broker serves
//! with Apple's own `LinuxPod` — one VM, many containers.
//!
//! # What lives where
//!
//! Pod semantics (namespaces, OCI specs, process lifecycle) are the broker's,
//! because they are Apple's `LinuxPod`. **This module is the CRI translation and
//! nothing else** — CRI's `command`+`args` split, its `KeyValue` env encoding,
//! quota/period back to whole cores, log-path joining, mount propagation. That
//! translation is the part that is genuinely ours, and it is what the tests below
//! pin.
//!
//! # Accepted divergences
//!
//! Three things CRI asks for that Apple's `LinuxPod` does not offer. Each is
//! fixable only by forking Apple's package or dropping below `LinuxPod` to the
//! raw `SandboxContext` protocol, and **all three were accepted rather than paid
//! for** (2026-08-02). Recorded here rather than faked; see `STATUS.md` for the
//! follow-ups.
//!
//! * **No per-container OCI runtime.** `LinuxPod` passes `ociRuntimePath: nil` at
//!   all three of its `createProcess` call sites and its `ContainerConfiguration`
//!   has no field for it. Mixed-runtime pods — a wasm container beside a classic
//!   one — are therefore not expressible here. Dropped as a feature; the raw
//!   protocol underneath does carry the selector if it is ever revisited.
//! * **No `RemoveContainer`.** `LinuxPod` has no remove; a container's guest-side
//!   state lives until the pod stops. [`PodRuntime::remove_container`] therefore
//!   stops it, drops our bookkeeping and reclaims the rootfs image, which is
//!   everything CRI can observe. The residue is an entry in `LinuxPod`'s own
//!   container table, bounded by pod lifetime but unbounded within it.
//! * **No per-container IPC/UTS namespace choice.** `LinuxPod` gives each
//!   container a fresh `ipc` and `uts`, so a pod-scoped hostname is the same
//!   string rather than the same namespace, and System V IPC is not shared.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use apple_containerization::{
    AttachedFilesystem, BlockMount, BrokerClient, BrokerRootfs, ContainerConfigWire, DnsConfigWire,
    ExecOptions, ImageWire, Interface, PodBroker, PodConfigWire,
};
use cri_proto::v1::*;
use tokio::sync::Mutex;

use cri_server::error::{Error, Result};

/// Defaults for pods this runtime creates.
///
/// These are a floor, not a policy: CRI hands `RunPodSandbox` the pod's summed
/// container resources in `LinuxPodSandboxConfig.resources`, which is what should
/// size the VM. Until that is wired, a pod gets these.
#[derive(Debug, Clone)]
pub struct PodRuntimeConfig {
    /// CPUs for a pod's VM when the sandbox config does not say.
    pub default_cpus: u32,
    /// Memory for a pod's VM when the sandbox config does not say.
    pub default_memory_bytes: u64,
    /// Scratch directory for `ExecSync` output files. One subdirectory per exec,
    /// removed when the call returns.
    pub exec_dir: PathBuf,
}

impl Default for PodRuntimeConfig {
    fn default() -> Self {
        Self {
            // Matches LinuxPod.Configuration's own defaults.
            default_cpus: 4,
            default_memory_bytes: 1024 * 1024 * 1024,
            exec_dir: std::env::temp_dir().join("apple-cri-exec"),
        }
    }
}

/// The client's already-listening sockets for a streaming call.
///
/// Each is optional because CRI lets a client ask for any subset — `kubectl exec`
/// without `-i` sends no stdin, and a non-TTY exec keeps stderr separate while a
/// TTY one folds it into stdout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamPaths {
    pub stdin: Option<PathBuf>,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
}

/// The result of a CRI `ExecSync`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecSyncResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Removes a directory when dropped, so an early return or a panic does not
/// leave exec scratch files behind.
struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Translate a broker image into CRI's shape.
///
/// The id is the digest: CRI requires that `ImageStatus` by id, by tag and by
/// `name@digest` all resolve to the same record, and the digest is the only one
/// of those that is stable across tags.
/// Collapse the broker's per-*reference* view into CRI's per-*image* one.
///
/// The broker's store is keyed by reference, so `busybox:1`, `busybox:2` and
/// `busybox:3` of one image are three entries. CRI keys on the image id and
/// expects a single image carrying all three in `repo_tags` — critest's
/// "listImage should get exactly 3 repoTags in the result image" is that
/// assertion. Grouping is by digest, in first-seen order so the result is stable.
fn aggregate_images(wires: &[ImageWire]) -> Vec<Image> {
    let mut order: Vec<String> = Vec::new();
    let mut by_digest: HashMap<String, Image> = HashMap::new();

    for wire in wires {
        let image = by_digest.entry(wire.digest.clone()).or_insert_with(|| {
            order.push(wire.digest.clone());
            image_identity(wire)
        });
        // A reference is either a tag or a digest reference, never both, and a
        // repeated pull of the same tag must not list it twice.
        if wire.reference.contains('@') {
            if !image.repo_digests.contains(&wire.reference) {
                image.repo_digests.push(wire.reference.clone());
            }
        } else if !wire.reference.is_empty() {
            if !image.repo_tags.contains(&wire.reference) {
                image.repo_tags.push(wire.reference.clone());
            }
            let digested = format!("{}@{}", strip_tag(&wire.reference), wire.digest);
            if !image.repo_digests.contains(&digested) {
                image.repo_digests.push(digested);
            }
        }
    }

    order
        .into_iter()
        .filter_map(|digest| by_digest.remove(&digest))
        .collect()
}

/// The parts of a CRI `Image` that come from the image itself rather than from
/// the references pointing at it.
fn image_identity(wire: &ImageWire) -> Image {
    // CRI splits the OCI `User` string into a numeric uid or a username, and the
    // group half is not CRI's — `www-data:www-data` must report `www-data`.
    let user = wire.user.split(':').next().unwrap_or_default();
    let (uid, username) = match user.parse::<i64>() {
        Ok(value) => (Some(Int64Value { value }), String::new()),
        Err(_) => (None, user.to_string()),
    };
    Image {
        id: wire.digest.clone(),
        repo_tags: Vec::new(),
        repo_digests: Vec::new(),
        size: wire.size_bytes,
        uid,
        username,
        ..Default::default()
    }
}

/// Resolve one of CRI's interchangeable image references against `images`.
///
/// A caller may name an image by its id (what `PullImage` handed back), by a
/// `repo@digest`, or by any tag on it — and a tag may be written short
/// (`busybox`) or fully qualified. All of them must land on the same image.
fn find_image(images: &[Image], query: &str) -> Option<Image> {
    if let Some(found) = images.iter().find(|i| i.id == query) {
        return Some(found.clone());
    }
    if let Some(found) = images
        .iter()
        .find(|i| i.repo_digests.iter().any(|d| d == query))
    {
        return Some(found.clone());
    }
    let want = crate::images::normalize_reference(query);
    images
        .iter()
        .find(|i| {
            i.repo_tags
                .iter()
                .any(|t| crate::images::normalize_reference(t) == want)
        })
        .cloned()
}

/// The repository part of a reference, so a repo digest is `repo@sha256:…`
/// rather than `repo:tag@sha256:…`, which is not a resolvable reference.
fn strip_tag(reference: &str) -> &str {
    match reference.rfind(':') {
        // A colon before the last `/` is a registry port, not a tag.
        Some(colon) if !reference[colon..].contains('/') => &reference[..colon],
        _ => reference,
    }
}

/// A stream the process never wrote to is empty, not an error: the broker only
/// creates the file when the caller asked for capture.
fn read_or_empty(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// A container's bookkeeping, so CRI's flat container ids can be resolved back
/// to the pod that owns them.
///
/// The broker knows which containers a pod holds; it does not know CRI's
/// metadata, image reference or creation time, so those are kept here.
#[derive(Debug, Clone)]
struct ContainerEntry {
    pod_id: String,
    image: String,
    /// The image *id* (digest), which is what CRI's `image_ref` reports.
    /// `image` is the reference the caller asked for; the two are different
    /// fields in `ContainerStatus` and critest compares the id across APIs.
    image_ref: String,
    metadata: Option<ContainerMetadata>,
    labels: HashMap<String, String>,
    annotations: HashMap<String, String>,
    created_at: i64,
    /// Lifecycle, tracked here because `LinuxPod` keeps its own per-container
    /// state private and exposes no accessor — `listContainers` returns every
    /// container it knows, running or not. containerd's CRI plugin tracks the
    /// same way, in its own container store fed by an exit monitor.
    ///
    /// Zero means "not yet": `started_at == 0` is CREATED, a non-zero
    /// `started_at` with `finished_at == 0` is RUNNING, both non-zero is EXITED.
    started_at: i64,
    finished_at: i64,
    exit_code: i32,
}

/// A container's CRI bookkeeping, as the backend needs it.
#[derive(Debug, Clone)]
pub struct ContainerRecord {
    pub id: String,
    pub pod_id: String,
    pub image: String,
    /// The image id (digest); see [`ContainerEntry`].
    pub image_ref: String,
    pub metadata: Option<ContainerMetadata>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub created_at: i64,
    /// See [`ContainerEntry`]: zero means "not yet", so the CRI state is
    /// derivable from the pair without a second enum to keep in sync.
    pub started_at: i64,
    pub finished_at: i64,
    pub exit_code: i32,
}

/// A sandbox's CRI bookkeeping. The broker knows the pod; it does not know
/// CRI's metadata, labels or creation time.
#[derive(Debug, Clone)]
pub struct SandboxEntry {
    pub id: String,
    pub ip: Option<String>,
    pub metadata: Option<PodSandboxMetadata>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub created_at: i64,
    /// CRI has only READY / NOTREADY for a sandbox.
    pub ready: bool,
    /// One task per published host port. `Arc` only so `SandboxEntry` stays
    /// `Clone` for the read snapshots CRI status calls take; the listener is
    /// aborted when the last reference goes, which `stop_pod_sandbox` forces.
    #[allow(dead_code)]
    pub published: Vec<Arc<PublishedPort>>,
}

/// A host port forwarded into the pod, and the listener serving it.
#[derive(Debug)]
pub struct PublishedPort {
    pub host_port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PublishedPort {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
impl SandboxEntry {
    fn testing(id: &str, ip: Option<&str>) -> Self {
        Self {
            id: id.to_string(),
            ip: ip.map(str::to_string),
            metadata: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            created_at: 0,
            ready: true,
            published: Vec::new(),
        }
    }
}

/// CRI runtime backed by pod sandboxes.
#[derive(Debug)]
pub struct PodRuntime {
    pods: PodBroker,
    rootfs: BrokerRootfs,
    config: PodRuntimeConfig,
    /// Sandboxes we created, in creation order, with the address the broker
    /// allocated for each — `PodSandboxStatus.network.ip` needs it and there is
    /// no round trip that would return it later.
    sandboxes: Mutex<Vec<SandboxEntry>>,
    containers: Arc<Mutex<HashMap<String, ContainerEntry>>>,
}

impl PodRuntime {
    pub fn new(client: BrokerClient, config: PodRuntimeConfig) -> Self {
        Self {
            pods: PodBroker::new(client.clone()),
            rootfs: BrokerRootfs::new(client),
            config,
            sandboxes: Mutex::new(Vec::new()),
            containers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Connect to a broker on `socket`.
    pub fn connect(socket: impl Into<std::path::PathBuf>, config: PodRuntimeConfig) -> Self {
        Self::new(BrokerClient::new(socket), config)
    }

    /// `RunPodSandbox`: build the pod, then boot its VM.
    pub async fn run_pod_sandbox(&self, id: &str, config: &PodSandboxConfig) -> Result<String> {
        let pod_config = pod_config_from_cri(id, config, &self.config);
        let address = self.pods.create_pod(&pod_config).await.map_err(pod_err)?;
        self.pods.create(id).await.map_err(pod_err)?;
        let published = match address.as_deref() {
            Some(ip) => publish_ports(id, ip, &config.port_mappings).await,
            None => Vec::new(),
        };
        self.sandboxes.lock().await.push(SandboxEntry {
            id: id.to_string(),
            ip: address,
            metadata: config.metadata.clone(),
            labels: config.labels.clone(),
            annotations: config.annotations.clone(),
            created_at: now_nanos(),
            ready: true,
            published,
        });
        Ok(id.to_string())
    }

    /// Stop every container in the pod and shut its VM down.
    pub async fn stop_pod_sandbox(&self, id: &str) -> Result<()> {
        // "This call is idempotent, and must not return an error if all relevant
        // resources have already been reclaimed" (CRI api.proto). containerd
        // no-ops the same way — `internal/cri/server/sandbox_stop.go` logs a
        // warning on a missing sandbox rather than failing.
        if self.assert_sandbox(id).await.is_err() {
            return Ok(());
        }
        self.pods.stop_pod(id).await.map_err(pod_err)?;
        // The record outlives the pod: CRI expects a stopped sandbox to still be
        // listable and inspectable until it is removed.
        let mut sandboxes = self.sandboxes.lock().await;
        if let Some(entry) = sandboxes.iter_mut().find(|e| e.id == id) {
            entry.ready = false;
            // The pod is gone; a listener still bound to its host port would
            // accept connections it can no longer serve, and would keep the port
            // from being reused by the next sandbox.
            entry.published.clear();
        }
        Ok(())
    }

    /// A sandbox's CRI record, for `PodSandboxStatus` and `ListPodSandbox`.
    pub async fn sandbox(&self, id: &str) -> Option<SandboxEntry> {
        self.sandboxes
            .lock()
            .await
            .iter()
            .find(|e| e.id == id)
            .cloned()
    }

    pub async fn sandboxes(&self) -> Vec<SandboxEntry> {
        self.sandboxes.lock().await.clone()
    }

    /// A container's CRI record.
    pub async fn container_entry(&self, id: &str) -> Option<ContainerRecord> {
        self.containers
            .lock()
            .await
            .get(id)
            .cloned()
            .map(|e| ContainerRecord {
                id: id.to_string(),
                pod_id: e.pod_id,
                image: e.image,
                image_ref: e.image_ref,
                metadata: e.metadata,
                labels: e.labels,
                annotations: e.annotations,
                created_at: e.created_at,
                started_at: e.started_at,
                finished_at: e.finished_at,
                exit_code: e.exit_code,
            })
    }

    /// Every container, across every sandbox.
    pub async fn container_entries(&self) -> Vec<ContainerRecord> {
        self.containers
            .lock()
            .await
            .iter()
            .map(|(id, e)| ContainerRecord {
                id: id.clone(),
                pod_id: e.pod_id.clone(),
                image: e.image.clone(),
                image_ref: e.image_ref.clone(),
                metadata: e.metadata.clone(),
                labels: e.labels.clone(),
                annotations: e.annotations.clone(),
                created_at: e.created_at,
                started_at: e.started_at,
                finished_at: e.finished_at,
                exit_code: e.exit_code,
            })
            .collect()
    }

    /// Remove the pod and forget its containers.
    pub async fn remove_pod_sandbox(&self, id: &str) -> Result<()> {
        // Stopping is idempotent, so removing a running sandbox is safe.
        let _ = self.pods.stop_pod(id).await;
        self.sandboxes.lock().await.retain(|e| e.id != id);

        let mut containers = self.containers.lock().await;
        let owned: Vec<String> = containers
            .iter()
            .filter(|(_, e)| e.pod_id == id)
            .map(|(cid, _)| cid.clone())
            .collect();
        for cid in owned {
            containers.remove(&cid);
            // Best effort: the sandbox is going away either way, and a leaked
            // ext4 image must not fail the remove.
            let _ = self.rootfs.release(&cid).await;
        }
        Ok(())
    }

    /// `CreateContainer`: materialise the image, then add it to the sandbox.
    ///
    /// After the sandbox has booted — which is always, in CRI's ordering — the
    /// broker hotplugs the rootfs into the live VM.
    pub async fn create_container(
        &self,
        pod_id: &str,
        container_id: &str,
        config: &ContainerConfig,
        sandbox_config: &PodSandboxConfig,
    ) -> Result<String> {
        self.assert_sandbox(pod_id).await?;
        let image = config
            .image
            .as_ref()
            .map(|i| i.image.clone())
            .unwrap_or_default();

        // CRI may name the image by id, by `repo@digest`, or by tag — critest
        // creates a container straight from the id `PullImage` handed back. The
        // broker's store is keyed by reference, so resolve to one it knows first;
        // containerd likewise resolves against its image store before handing the
        // result to the snapshotter. An image not in the store yet is passed
        // through unchanged so `provision` pulls it.
        let reference = match self.image_wire(&image).await {
            Some(wire) => wire.reference,
            None => image.clone(),
        };

        let rootfs = self
            .rootfs
            .provision(&reference, container_id)
            .await
            .map_err(pod_err)?;

        // The image is in the store by now — `provision` pulls it — so this is a
        // local lookup. It carries the entrypoint/cmd/env/workingDir the CRI
        // config is allowed to omit, and the id `image_ref` must report.
        let image_config = self.image_wire(&reference).await;
        let wire = container_config_from_cri(
            container_id,
            config,
            sandbox_config,
            rootfs,
            image_config.as_ref(),
        );

        if let Err(e) = self.pods.add_container(pod_id, &wire).await {
            // Don't leak the rootfs image if the broker refused the container.
            let _ = self.rootfs.release(container_id).await;
            return Err(pod_err(e));
        }

        self.containers.lock().await.insert(
            container_id.to_string(),
            ContainerEntry {
                pod_id: pod_id.to_string(),
                image_ref: image_config
                    .as_ref()
                    .map(|w| w.digest.clone())
                    .unwrap_or_default(),
                image,
                metadata: config.metadata.clone(),
                labels: config.labels.clone(),
                annotations: config.annotations.clone(),
                created_at: now_nanos(),
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
            },
        );
        Ok(container_id.to_string())
    }

    pub async fn start_container(&self, container_id: &str) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .start_container(&entry.pod_id, container_id)
            .await
            .map_err(pod_err)?;

        {
            let mut containers = self.containers.lock().await;
            if let Some(e) = containers.get_mut(container_id) {
                e.started_at = now_nanos();
            }
        }
        self.watch_for_exit(&entry.pod_id, container_id);
        Ok(())
    }

    /// Record a container's exit as soon as it happens.
    ///
    /// CRI has no `WaitContainer`: the kubelet polls `ContainerStatus` and reads
    /// the state, so something has to notice the exit and write it down.
    /// containerd's CRI plugin spawns exactly this per started container
    /// (`internal/cri/server/container_start.go`, which launches the exit monitor
    /// that later updates the container store).
    ///
    /// This is the only waiter on a container's init process, so it cannot race
    /// another `waitContainer` for the same id.
    fn watch_for_exit(&self, pod_id: &str, container_id: &str) {
        let pods = self.pods.clone();
        let containers = self.containers.clone();
        let pod_id = pod_id.to_string();
        let container_id = container_id.to_string();
        tokio::spawn(async move {
            // A wait that fails says nothing about the container, so the state
            // is left alone rather than guessed at. The pod going away is the
            // realistic cause, and `remove_pod_sandbox` drops these entries.
            let exit_code = match pods.wait_container(&pod_id, &container_id).await {
                Ok(code) => code,
                Err(e) => {
                    tracing::debug!(container = %container_id, error = %e, "wait ended without an exit code");
                    return;
                }
            };
            if let Some(entry) = containers.lock().await.get_mut(&container_id) {
                entry.exit_code = exit_code;
                entry.finished_at = now_nanos();
            }
        });
    }

    /// Stop a container, escalating to SIGKILL when the grace period is zero.
    pub async fn stop_container(&self, container_id: &str, timeout_secs: i64) -> Result<()> {
        // Idempotent per CRI api.proto: stopping a container that is gone, or
        // that has already exited, is success. The exit monitor stamps
        // `finished_at`, so an already-stopped container needs no guest round
        // trip at all.
        let Some(entry) = self.containers.lock().await.get(container_id).cloned() else {
            return Ok(());
        };
        if entry.finished_at != 0 {
            return Ok(());
        }
        let result = if timeout_secs == 0 {
            // CRI's "kill now".
            self.pods
                .kill_container(&entry.pod_id, container_id, 9)
                .await
        } else {
            self.pods.stop_container(&entry.pod_id, container_id).await
        };
        match result {
            Ok(()) => Ok(()),
            // Stopping an already-stopped container is success in CRI.
            Err(apple_containerization::Error::InvalidState(_)) => Ok(()),
            Err(e) => Err(pod_err(e)),
        }
    }

    /// `RemoveContainer`. See the module docs: `LinuxPod` has no remove, so this
    /// stops the container and reclaims everything CRI can observe.
    pub async fn remove_container(&self, container_id: &str) -> Result<()> {
        // "This call is idempotent, and must not return an error if the container
        // has already been removed" — containerd returns an empty response for an
        // unknown id (`internal/cri/server/container_remove.go`).
        let Some(entry) = self.containers.lock().await.get(container_id).cloned() else {
            return Ok(());
        };
        let _ = self
            .pods
            .kill_container(&entry.pod_id, container_id, 9)
            .await;
        self.containers.lock().await.remove(container_id);
        self.rootfs.release(container_id).await.map_err(pod_err)
    }

    /// Wait for a container's init process and return its exit code.
    ///
    /// This is what makes an **init container** expressible: the kubelet does not
    /// create the next container until this returns, and it drives restart policy
    /// off the code.
    pub async fn wait_container(&self, container_id: &str) -> Result<i32> {
        let entry = self.container(container_id).await?;
        self.pods
            .wait_container(&entry.pod_id, container_id)
            .await
            .map_err(pod_err)
    }

    /// `ExecSync`: run a command to completion and collect its output.
    ///
    /// `exec_id` must differ from the container id — that is how the guest
    /// distinguishes an exec from the container's init process.
    ///
    /// Output is captured through files the broker writes: the process's
    /// `Writer`s live in the broker, so there is no stream for this side to read.
    /// A file also cannot block the guest when nothing is reading it, which a
    /// socket would if the caller were slow to attach.
    ///
    /// `timeout_secs` of 0 means no deadline, as CRI defines it. On timeout the
    /// process is killed *and reaped* — otherwise it would outlive the RPC — and
    /// the call reports `DeadlineExceeded` rather than an exit code, because a
    /// command that was shot did not "finish with" the status that killed it.
    pub async fn exec_sync(
        &self,
        container_id: &str,
        exec_id: &str,
        cmd: &[String],
        env: Vec<String>,
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        let entry = self.container(container_id).await?;
        let dir = self.config.exec_dir.join(exec_id);
        std::fs::create_dir_all(&dir)?;
        // Whatever happens below, the scratch files go.
        let _guard = DirGuard(dir.clone());

        let options = ExecOptions::new(cmd.to_vec()).capturing_in(&dir);
        let options = ExecOptions { env, ..options };

        self.pods
            .exec(&entry.pod_id, container_id, exec_id, &options)
            .await
            .map_err(pod_err)?;

        let wait = self.pods.wait_process(&entry.pod_id, container_id, exec_id);
        let exit_code = if timeout_secs > 0 {
            match tokio::time::timeout(Duration::from_secs(timeout_secs as u64), wait).await {
                Ok(code) => code.map_err(pod_err)?,
                Err(_) => {
                    let _ = self
                        .pods
                        .kill_process(&entry.pod_id, container_id, exec_id, 9)
                        .await;
                    let _ = self
                        .pods
                        .wait_process(&entry.pod_id, container_id, exec_id)
                        .await;
                    return Err(Error::DeadlineExceeded(format!(
                        "exec in {container_id} timed out after {timeout_secs}s"
                    )));
                }
            }
        } else {
            wait.await.map_err(pod_err)?
        };

        // Read after the wait: the broker closes the files as it reaps the
        // process, so anything written on the way out is on disk by now.
        Ok(ExecSyncResult {
            exit_code,
            stdout: read_or_empty(&dir.join("stdout")),
            stderr: read_or_empty(&dir.join("stderr")),
        })
    }

    /// `Exec`: run a command with its stdio streamed over unix sockets.
    ///
    /// The caller must already be listening on every path it passes. That is the
    /// opposite of `ExecSync`'s file capture and it is deliberate: with sockets,
    /// anything the process writes before the client attaches would be lost, so
    /// the client binds first and the broker connects.
    pub async fn exec_streaming(
        &self,
        container_id: &str,
        exec_id: &str,
        cmd: &[String],
        env: Vec<String>,
        tty: bool,
        streams: StreamPaths,
    ) -> Result<i32> {
        let entry = self.container(container_id).await?;
        let options = ExecOptions {
            args: cmd.to_vec(),
            env,
            terminal: tty,
            ..Default::default()
        }
        .streaming(streams.stdin, streams.stdout, streams.stderr);
        self.pods
            .exec(&entry.pod_id, container_id, exec_id, &options)
            .await
            .map_err(pod_err)
    }

    /// `Attach`: join a client's sockets to a running container's stdio.
    ///
    /// A container only has stdin if it was created with CRI's `stdin: true`;
    /// attaching stdin to one that was not is an error rather than a silent
    /// no-op, because the client would otherwise wait forever for input to land.
    pub async fn attach(&self, container_id: &str, streams: StreamPaths) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .attach(
                &entry.pod_id,
                container_id,
                streams.stdin,
                streams.stdout,
                streams.stderr,
            )
            .await
            .map_err(pod_err)
    }

    /// `ReopenContainerLog`: reopen the container's log file at its path.
    ///
    /// The kubelet calls this after rotating the file away, and CRI requires a
    /// *new* file to appear — the broker holds the handle, so only it can.
    pub async fn reopen_container_log(&self, container_id: &str) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .reopen_container_log(&entry.pod_id, container_id)
            .await
            .map_err(pod_err)
    }

    /// Resize an exec'd process's pty.
    pub async fn resize_exec(
        &self,
        container_id: &str,
        exec_id: &str,
        width: u16,
        height: u16,
    ) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .resize(&entry.pod_id, container_id, exec_id, width, height)
            .await
            .map_err(pod_err)
    }

    // -- images ------------------------------------------------------------
    //
    // Answered from the *broker's* store, not Apple's CLI store. The pod path
    // runs rootfs images out of the broker's, so pointing CRI at the CLI's would
    // let `PullImage` populate one while the pod pulls into the other, and
    // `RemoveImage` leave the image still resolvable — which is exactly what
    // critest's Image Consistency suite checks.

    pub async fn list_images(&self) -> Result<Vec<Image>> {
        Ok(aggregate_images(
            &self.rootfs.list_images().await.map_err(pod_err)?,
        ))
    }

    /// The broker's record for whatever form of reference CRI supplied, so a
    /// container created from `busybox` finds the image pulled as
    /// `docker.io/library/busybox:latest`.
    async fn image_wire(&self, reference: &str) -> Option<ImageWire> {
        let wires = self.rootfs.list_images().await.ok()?;
        let want = crate::images::normalize_reference(reference);
        wires
            .iter()
            .find(|w| {
                w.digest == reference
                    || w.reference == reference
                    || crate::images::normalize_reference(&w.reference) == want
            })
            .cloned()
    }

    /// `None` when the image is absent — CRI reports that, it does not fail.
    ///
    /// Resolves an image id, a `repo@digest`, or any tag, because CRI callers use
    /// all three interchangeably and critest's "all kinds of references" spec
    /// checks exactly that.
    pub async fn image_status(&self, image: &str) -> Result<Option<Image>> {
        let images = self.list_images().await?;
        Ok(find_image(&images, image))
    }

    /// Pull, and answer with the image id CRI keys everything else off.
    pub async fn pull_image(&self, image: &str) -> Result<String> {
        let pulled = self.rootfs.pull_image(image).await.map_err(pod_err)?;
        Ok(pulled.digest)
    }

    /// Remove the image `image` names, **and every other tag on it**.
    ///
    /// CRI removes an *image*, not a reference: "removing image by one tag should
    /// remove all tags" is a conformance spec. The broker's store is keyed by
    /// reference, so each of them has to go individually. Removing an image that
    /// is already gone is success.
    pub async fn remove_image(&self, image: &str) -> Result<()> {
        let wires = self.rootfs.list_images().await.map_err(pod_err)?;
        let Some(target) = find_image(&aggregate_images(&wires), image) else {
            return Ok(());
        };
        for wire in wires.iter().filter(|w| w.digest == target.id) {
            self.rootfs
                .remove_image(&wire.reference)
                .await
                .map_err(pod_err)?;
        }
        Ok(())
    }

    pub async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>> {
        let (path, bytes) = self.rootfs.image_fs_info().await.map_err(pod_err)?;
        Ok(vec![FilesystemUsage {
            timestamp: now_nanos(),
            fs_id: Some(FilesystemIdentifier { mountpoint: path }),
            used_bytes: Some(UInt64Value { value: bytes }),
            ..Default::default()
        }])
    }

    /// `PortForward`: a TCP connection to `port` inside the pod.
    ///
    /// No broker round trip: the pod's NAT'd address is reachable from the host
    /// directly, so this is a plain `connect(2)`. The guest transport the broker
    /// does offer — `ProxyVsock` — relays a vsock port to a guest *unix socket*
    /// path, which cannot reach a TCP listener, so the address is the only route.
    ///
    /// Note this needs the shim's own process to hold **macOS Local Network
    /// access**; without it every host-originated packet to the pod is dropped
    /// before egress. The same grant gates the kubelet's HTTP/TCP probes.
    pub async fn port_forward(&self, pod_id: &str, port: u16) -> Result<tokio::net::TcpStream> {
        let ip = self.pod_ip(pod_id).await.ok_or_else(|| {
            Error::FailedPrecondition(format!("pod sandbox {pod_id} has no address to forward to"))
        })?;
        tokio::net::TcpStream::connect((ip.as_str(), port))
            .await
            .map_err(|e| Error::Internal(format!("port-forward to {ip}:{port} in {pod_id}: {e}")))
    }

    /// EOF a container's stdin — CRI's `stdinOnce`.
    pub async fn close_stdin(&self, container_id: &str) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .close_stdin(&entry.pod_id, container_id)
            .await
            .map_err(pod_err)
    }

    /// Wait for a streaming exec and return its exit code.
    ///
    /// Separate from [`Self::exec_sync`] because a streaming exec has already
    /// returned its handles to the caller; only the reaper is still outstanding.
    pub async fn wait_exec(&self, container_id: &str, exec_id: &str) -> Result<i32> {
        let entry = self.container(container_id).await?;
        self.pods
            .wait_process(&entry.pod_id, container_id, exec_id)
            .await
            .map_err(pod_err)
    }

    /// Signal an exec'd process — how `ExecSync` enforces its timeout.
    pub async fn kill_exec(&self, container_id: &str, exec_id: &str, signal: i32) -> Result<()> {
        let entry = self.container(container_id).await?;
        self.pods
            .kill_process(&entry.pod_id, container_id, exec_id, signal)
            .await
            .map_err(pod_err)
    }

    /// Per-container stats from the guest.
    pub async fn container_stats(&self, container_id: &str) -> Result<Option<ContainerStats>> {
        let entry = self.container(container_id).await?;
        let stats = self
            .pods
            .statistics(&entry.pod_id, vec![container_id.to_string()])
            .await
            .map_err(pod_err)?;
        let Some(s) = stats.into_iter().find(|s| s.id == container_id) else {
            return Ok(None);
        };
        Ok(Some(ContainerStats {
            attributes: Some(ContainerAttributes {
                id: container_id.to_string(),
                metadata: entry.metadata.clone(),
                ..Default::default()
            }),
            cpu: Some(CpuUsage {
                timestamp: now_nanos(),
                // CRI wants nanoseconds; the guest reports microseconds.
                usage_core_nano_seconds: Some(UInt64Value {
                    value: s.cpu_usage_usec.saturating_mul(1_000),
                }),
                ..Default::default()
            }),
            memory: Some(MemoryUsage {
                timestamp: now_nanos(),
                working_set_bytes: Some(UInt64Value {
                    // Working set is usage minus inactive file cache, as cAdvisor
                    // and the kubelet compute it.
                    value: s
                        .memory_usage_bytes
                        .saturating_sub(s.memory_inactive_file_bytes),
                }),
                usage_bytes: Some(UInt64Value {
                    value: s.memory_usage_bytes,
                }),
                rss_bytes: Some(UInt64Value {
                    value: s.memory_anon_bytes,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    /// Ids of the pods this runtime owns.
    pub async fn list_pods(&self) -> Vec<String> {
        self.sandboxes
            .lock()
            .await
            .iter()
            .map(|e| e.id.clone())
            .collect()
    }

    /// The pod's IP, for `PodSandboxStatus.network.ip`.
    ///
    /// Every container in the pod shares it: the VM *is* the pod's network, so
    /// there is one address rather than one per container. That is the property
    /// the CLI-backed path cannot provide.
    pub async fn pod_ip(&self, pod_id: &str) -> Option<String> {
        self.sandboxes
            .lock()
            .await
            .iter()
            .find(|e| e.id == pod_id)
            .and_then(|e| e.ip.clone())
    }

    /// Container ids in a pod, in creation order as the broker reports them.
    pub async fn list_containers(&self, pod_id: &str) -> Vec<String> {
        self.pods.list_containers(pod_id).await.unwrap_or_default()
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

    async fn assert_sandbox(&self, id: &str) -> Result<()> {
        if self.sandboxes.lock().await.iter().any(|e| e.id == id) {
            Ok(())
        } else {
            Err(Error::NotFound(format!("pod sandbox {id} not found")))
        }
    }

    async fn container(&self, container_id: &str) -> Result<ContainerEntry> {
        self.containers
            .lock()
            .await
            .get(container_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("container {container_id} not found")))
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

/// Build a [`PodConfigWire`] from a CRI `PodSandboxConfig`.
pub fn pod_config_from_cri(
    id: &str,
    config: &PodSandboxConfig,
    defaults: &PodRuntimeConfig,
) -> PodConfigWire {
    let namespace_options = config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|s| s.namespace_options.as_ref());

    // Kubernetes `shareProcessNamespace` reaches the runtime as pid == POD on the
    // *sandbox*. CRI's own default for pid is POD, but the kubelet sets it to
    // CONTAINER explicitly for v1 pods that do not share, so honouring the value
    // as sent is correct — and absent namespace options mean "not shared".
    let share_process_namespace = namespace_options
        .map(|n| n.pid == NamespaceMode::Pod as i32)
        .unwrap_or(false);

    let dns = config.dns_config.as_ref().map(|d| DnsConfigWire {
        nameservers: d.servers.clone(),
        search_domains: d.searches.clone(),
        options: d.options.clone(),
        domain: None,
    });

    PodConfigWire {
        id: id.to_string(),
        cpus: defaults.default_cpus,
        memory_in_bytes: defaults.default_memory_bytes,
        interfaces: Vec::new(),
        share_process_namespace,
        hostname: non_empty(&config.hostname),
        dns,
        boot_log: None,
    }
}

/// Attach a pod IP to a [`PodConfigWire`], as the CNI/vmnet result would.
pub fn with_interface(
    mut config: PodConfigWire,
    address: &str,
    gateway: Option<&str>,
) -> PodConfigWire {
    config.interfaces.push(Interface {
        address: address.to_string(),
        gateway: gateway.map(|g| g.to_string()),
        mtu: None,
        mac_address: None,
    });
    config
}

/// Build a [`ContainerConfigWire`] from a CRI `ContainerConfig`.
/// Resolve the process argv from CRI's `command`/`args` and the image config.
///
/// A CRI container routinely specifies neither and expects the image's
/// `Entrypoint`/`Cmd` to run — every critest container built from the nginx
/// image does. Ported from containerd's CRI plugin, `WithProcessArgs` in
/// `internal/cri/opts/spec_opts.go:59`, including its guard against an image
/// whose entrypoint is a single empty string.
///
/// Returning empty is left to the caller to surface: the guest already rejects
/// it with "process args cannot be empty", which is the same failure containerd
/// reports as "no command specified".
fn process_args(command: &[String], args: &[String], image: Option<&ImageWire>) -> Vec<String> {
    let mut command = command.to_vec();
    let mut args = args.to_vec();

    if command.is_empty() {
        if args.is_empty() {
            args = image.map(|i| i.cmd.clone()).unwrap_or_default();
        }
        let entrypoint = image.map(|i| i.entrypoint.as_slice()).unwrap_or_default();
        if !(entrypoint.len() == 1 && entrypoint[0].is_empty()) {
            command = entrypoint.to_vec();
        }
    }

    command.extend(args);
    command
}

/// Publish each `host_port` in `mappings` as a host listener that forwards into
/// the pod.
///
/// A pod's VM sits on macOS's vmnet NAT, which forwards *outbound* only — there
/// is no API to ask it to publish an inbound port. containerd gets this from CNI
/// portmap, which writes iptables DNAT rules; there is no equivalent here, so the
/// shim carries the traffic itself. One `accept` loop per mapping, one bidirectional
/// copy per connection, all aborted when the sandbox stops.
///
/// A port that cannot be bound is logged and skipped rather than failing
/// `RunPodSandbox`: the pod is up and useful, and CRI has no way to report a
/// partially published sandbox.
async fn publish_ports(
    pod_id: &str,
    pod_ip: &str,
    mappings: &[PortMapping],
) -> Vec<Arc<PublishedPort>> {
    let mut published = Vec::new();
    for mapping in mappings {
        // `host_port == 0` means "expose only" — the container port is reachable
        // on the pod IP, and nothing should be bound on the host.
        let Ok(host_port) = u16::try_from(mapping.host_port) else {
            continue;
        };
        if host_port == 0 {
            continue;
        }
        if mapping.protocol == Protocol::Udp as i32 {
            tracing::warn!(pod = %pod_id, port = host_port, "UDP port mapping is not implemented");
            continue;
        }
        let Ok(container_port) = u16::try_from(mapping.container_port) else {
            continue;
        };
        // An empty `host_ip` means all interfaces, as in Kubernetes.
        let host_ip = if mapping.host_ip.is_empty() {
            "0.0.0.0"
        } else {
            mapping.host_ip.as_str()
        };

        let listener = match tokio::net::TcpListener::bind((host_ip, host_port)).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::warn!(pod = %pod_id, port = host_port, error = %e, "could not publish host port");
                continue;
            }
        };
        let target = format!("{pod_ip}:{container_port}");
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                let target = target.clone();
                tokio::spawn(async move {
                    let Ok(mut upstream) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                });
            }
        });
        tracing::info!(pod = %pod_id, host_port, %pod_ip, container_port, "published host port");
        published.push(Arc::new(PublishedPort { host_port, task }));
    }
    published
}

pub fn container_config_from_cri(
    container_id: &str,
    config: &ContainerConfig,
    sandbox_config: &PodSandboxConfig,
    mut rootfs: BlockMount,
    image: Option<&ImageWire>,
) -> ContainerConfigWire {
    let linux = config.linux.as_ref();
    let security = linux.and_then(|l| l.security_context.as_ref());

    let args = process_args(&config.command, &config.args, image);

    // The image's environment is the base; the container's overrides it key by
    // key. containerd does the same — `internal/cri/server/container_create.go`
    // seeds `env` from the image config before appending the CRI entries, and
    // `oci.WithEnv` replaces in place rather than appending duplicates.
    let mut env: Vec<String> = image.map(|i| i.env.clone()).unwrap_or_default();
    for e in &config.envs {
        // CRI models env values as `bytes` (api.proto KeyValue), so decode.
        let entry = format!("{}={}", e.key, String::from_utf8_lossy(&e.value));
        let prefix = format!("{}=", e.key);
        match env
            .iter()
            .position(|existing| existing.starts_with(&prefix))
        {
            Some(i) => env[i] = entry,
            None => env.push(entry),
        }
    }

    let resources = linux.and_then(|l| l.resources.as_ref());
    let memory_in_bytes = resources
        .map(|r| r.memory_limit_in_bytes)
        .filter(|m| *m > 0)
        .map(|m| m as u64);

    // Apple expresses CPU as a whole-core count. CRI gives quota/period, so
    // convert rather than losing the limit.
    let cpus = resources.and_then(|r| {
        if r.cpu_quota > 0 && r.cpu_period > 0 {
            u32::try_from((r.cpu_quota / r.cpu_period).max(1)).ok()
        } else {
            None
        }
    });

    // A read-only rootfs is expressed as a mount option, since the wire carries
    // the rootfs as a block mount rather than a flag.
    if security.map(|s| s.readonly_rootfs).unwrap_or(false) && !rootfs.is_readonly() {
        rootfs.options.push("ro".to_string());
    }

    ContainerConfigWire {
        id: container_id.to_string(),
        rootfs,
        log_path: log_path_from_cri(config, sandbox_config),
        args,
        env,
        // CRI's working dir wins; the image's is the fallback, and "/" only when
        // neither says anything.
        working_directory: [
            config.working_dir.as_str(),
            image.map(|i| i.working_dir.as_str()).unwrap_or_default(),
            "/",
        ]
        .into_iter()
        .find(|d| !d.is_empty())
        .unwrap_or("/")
        .to_string(),
        terminal: config.tty,
        stdin: config.stdin,
        stdin_once: config.stdin_once,
        uid: security
            .and_then(|s| s.run_as_user.as_ref())
            .map(|v| v.value as u32)
            .unwrap_or(0),
        gid: security
            .and_then(|s| s.run_as_group.as_ref())
            .map(|v| v.value as u32)
            .unwrap_or(0),
        additional_gids: security
            .map(|s| s.supplemental_groups.iter().map(|g| *g as u32).collect())
            .unwrap_or_default(),
        username: security
            .map(|s| s.run_as_username.clone())
            .unwrap_or_default(),
        // The pod's hostname reaches every container. `LinuxPod` gives each its
        // own UTS namespace, so this is the same string, not a shared namespace.
        hostname: non_empty(&sandbox_config.hostname),
        cpus,
        memory_in_bytes,
        // Pod-level sysctls apply to every container in the pod.
        sysctl: sandbox_config
            .linux
            .as_ref()
            .map(|l| l.sysctls.clone())
            .unwrap_or_default(),
        // Only the mounts CRI asked for: Apple's `ContainerConfiguration` already
        // seeds `LinuxContainer.defaultMounts()`, and the broker appends to it.
        mounts: config.mounts.iter().map(mount_from_cri).collect(),
        // Likewise, empty means "keep Apple's OCI-standard defaults"; the broker
        // only overrides when these are non-empty.
        masked_paths: security.map(|s| s.masked_paths.clone()).unwrap_or_default(),
        readonly_paths: security
            .map(|s| s.readonly_paths.clone())
            .unwrap_or_default(),
    }
}

/// Where the container's CRI log file goes.
///
/// CRI splits it: `PodSandboxConfig.log_directory` is absolute, and
/// `ContainerConfig.log_path` is relative to it. Joining them is what containerd
/// does (`pkg/cri/server/container_create.go`).
fn log_path_from_cri(
    config: &ContainerConfig,
    sandbox_config: &PodSandboxConfig,
) -> Option<String> {
    let relative = non_empty(&config.log_path)?;
    match non_empty(&sandbox_config.log_directory) {
        Some(dir) => Some(
            std::path::Path::new(&dir)
                .join(&relative)
                .to_string_lossy()
                .into_owned(),
        ),
        None => Some(relative),
    }
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Map a CRI mount onto the virtiofs share the broker will apply.
///
/// CRI mounts name a path on the **host**, but the container runs in a VM, so a
/// plain bind cannot resolve — there is nothing at that path inside the guest.
/// The path has to be shared in over virtiofs first, which is what
/// `Mount.share` does and what `FileMountContext.prepare` acts on.
///
/// CRI's propagation modes have no equivalent here: they describe how a *bind*
/// relates to its parent mount, and a virtiofs share is not a bind. Passing them
/// through as `rshared`/`rslave` would claim a guarantee nothing is enforcing, so
/// they are dropped and only the read-only flag survives.
fn mount_from_cri(mount: &Mount) -> AttachedFilesystem {
    AttachedFilesystem {
        type_: "virtiofs".to_string(),
        source: mount.host_path.clone(),
        destination: mount.container_path.clone(),
        options: vec![if mount.readonly { "ro" } else { "rw" }.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apple_containerization::broker::Method;
    use apple_containerization::testing::FakeBroker;

    const POD: &str = "pod-1";

    /// The temp dir is returned so it outlives the runtime: dropping it would
    /// delete the exec scratch space out from under a running test.
    async fn runtime() -> (FakeBroker, PodRuntime, tempfile::TempDir) {
        let broker = FakeBroker::start().await.expect("fake broker");
        let dir = tempfile::tempdir().expect("temp dir");
        let runtime = PodRuntime::new(
            BrokerClient::new(broker.socket_path()),
            PodRuntimeConfig {
                exec_dir: dir.path().join("exec"),
                ..Default::default()
            },
        );
        (broker, runtime, dir)
    }

    fn sandbox_config() -> PodSandboxConfig {
        PodSandboxConfig {
            hostname: "my-pod".to_string(),
            log_directory: "/var/log/pods/default_my-pod".to_string(),
            ..Default::default()
        }
    }

    fn container_config(name: &str) -> ContainerConfig {
        ContainerConfig {
            metadata: Some(ContainerMetadata {
                name: name.to_string(),
                attempt: 0,
            }),
            image: Some(ImageSpec {
                image: "busybox:1.29".to_string(),
                ..Default::default()
            }),
            log_path: format!("{name}/0.log"),
            ..Default::default()
        }
    }

    /// The wire config the broker was handed for `addContainer`.
    fn added(broker: &FakeBroker, nth: usize) -> ContainerConfigWire {
        broker.params_for(Method::AddContainer)[nth]
            .container
            .clone()
            .expect("addContainer carries a container")
    }

    // -- pod config ---------------------------------------------------------

    #[tokio::test]
    async fn run_pod_sandbox_builds_then_boots() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();

        let requests = broker.requests();
        assert_eq!(requests, vec![Method::CreatePod, Method::Create]);
        let config = broker.params_for(Method::CreatePod)[0]
            .config
            .clone()
            .unwrap();
        assert_eq!(config.id, POD);
        assert_eq!(config.hostname.as_deref(), Some("my-pod"));
        assert_eq!(runtime.list_pods().await, vec![POD]);
    }

    #[tokio::test]
    async fn an_absent_hostname_is_none_not_an_empty_string() {
        let config = pod_config_from_cri(POD, &PodSandboxConfig::default(), &Default::default());
        assert_eq!(config.hostname, None);
        assert_eq!(config.dns, None);
    }

    #[tokio::test]
    async fn share_process_namespace_comes_from_the_sandbox_pid_mode() {
        let with_mode = |pid: i32| {
            pod_config_from_cri(
                POD,
                &PodSandboxConfig {
                    linux: Some(LinuxPodSandboxConfig {
                        security_context: Some(LinuxSandboxSecurityContext {
                            namespace_options: Some(NamespaceOption {
                                pid,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &Default::default(),
            )
            .share_process_namespace
        };
        assert!(with_mode(NamespaceMode::Pod as i32), "POD shares");
        assert!(!with_mode(NamespaceMode::Container as i32));
        // Absent namespace options must not be read as "shared".
        assert!(
            !pod_config_from_cri(POD, &PodSandboxConfig::default(), &Default::default())
                .share_process_namespace
        );
    }

    #[tokio::test]
    async fn dns_config_is_translated() {
        let config = pod_config_from_cri(
            POD,
            &PodSandboxConfig {
                dns_config: Some(DnsConfig {
                    servers: vec!["10.96.0.10".into()],
                    searches: vec!["svc.cluster.local".into()],
                    options: vec!["ndots:5".into()],
                }),
                ..Default::default()
            },
            &Default::default(),
        );
        let dns = config.dns.unwrap();
        assert_eq!(dns.nameservers, vec!["10.96.0.10"]);
        assert_eq!(dns.search_domains, vec!["svc.cluster.local"]);
        assert_eq!(dns.options, vec!["ndots:5"]);
    }

    // -- container config ---------------------------------------------------

    #[tokio::test]
    async fn command_and_args_are_concatenated_in_cri_order() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        let mut config = container_config("app");
        config.command = vec!["/bin/sh".into()];
        config.args = vec!["-c".into(), "echo hi".into()];

        runtime
            .create_container(POD, "c1", &config, &sandbox_config())
            .await
            .unwrap();

        assert_eq!(added(&broker, 0).args, vec!["/bin/sh", "-c", "echo hi"]);
    }

    /// critest builds most of its containers from an image and sets neither
    /// `command` nor `args`, expecting the image's entrypoint to run. Before the
    /// image config was merged in, every one of those failed to start with
    /// "process args cannot be empty".
    ///
    /// The cases are containerd's, from `WithProcessArgs`
    /// (`internal/cri/opts/spec_opts.go:59`).
    #[test]
    fn the_image_entrypoint_applies_when_cri_omits_the_command() {
        let image = |entrypoint: &[&str], cmd: &[&str]| ImageWire {
            entrypoint: entrypoint.iter().map(|s| s.to_string()).collect(),
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        };
        let of = |command: &[&str], args: &[&str], image: Option<&ImageWire>| {
            let to_vec = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            process_args(&to_vec(command), &to_vec(args), image)
        };

        let nginx = image(&["nginx"], &["-g", "daemon off;"]);
        // Neither supplied: the image's entrypoint *and* cmd run.
        assert_eq!(of(&[], &[], Some(&nginx)), ["nginx", "-g", "daemon off;"]);
        // CRI args replace the image's cmd, keeping its entrypoint.
        assert_eq!(of(&[], &["-v"], Some(&nginx)), ["nginx", "-v"]);
        // A CRI command replaces the entrypoint, and suppresses the image's cmd.
        assert_eq!(of(&["/bin/sh"], &[], Some(&nginx)), ["/bin/sh"]);
        assert_eq!(
            of(&["/bin/sh"], &["-c", "x"], Some(&nginx)),
            ["/bin/sh", "-c", "x"]
        );
        // Cmd-only image: the cmd is the whole argv.
        assert_eq!(
            of(&[], &[], Some(&image(&[], &["/bin/sleep", "1"]))),
            ["/bin/sleep", "1"]
        );
        // containerd's guard: an entrypoint of exactly one empty string is not
        // an entrypoint, and must not prepend "" to the argv.
        assert_eq!(
            of(&[], &[], Some(&image(&[""], &["/bin/true"]))),
            ["/bin/true"]
        );
        // No image config at all falls back to whatever CRI gave.
        assert_eq!(of(&["/bin/true"], &[], None), ["/bin/true"]);
        assert!(of(&[], &[], None).is_empty());
    }

    /// The image's environment is the base and CRI's overrides it per key —
    /// appending both would leave two `PATH=` entries with the loser first.
    #[test]
    fn container_env_overrides_image_env_by_key() {
        let image = ImageWire {
            env: vec!["PATH=/usr/bin".into(), "NGINX_VERSION=1.14".into()],
            ..Default::default()
        };
        let mut config = ContainerConfig {
            envs: vec![
                KeyValue {
                    key: "PATH".into(),
                    value: b"/opt/bin".to_vec(),
                },
                KeyValue {
                    key: "EXTRA".into(),
                    value: b"1".to_vec(),
                },
            ],
            ..container_config("app")
        };
        config.command = vec!["/bin/true".into()];

        let wire = container_config_from_cri(
            "c1",
            &config,
            &sandbox_config(),
            BlockMount::block("ext4", "/i.ext4"),
            Some(&image),
        );

        assert_eq!(
            wire.env,
            vec!["PATH=/opt/bin", "NGINX_VERSION=1.14", "EXTRA=1"]
        );
    }

    /// CRI's working dir wins, the image's is the fallback, "/" is the floor.
    #[test]
    fn the_image_working_dir_applies_when_cri_omits_it() {
        let dir = |cri: &str, image_dir: &str| {
            let mut config = container_config("app");
            config.working_dir = cri.to_string();
            let image = ImageWire {
                working_dir: image_dir.to_string(),
                ..Default::default()
            };
            container_config_from_cri(
                "c1",
                &config,
                &sandbox_config(),
                BlockMount::block("ext4", "/i.ext4"),
                Some(&image),
            )
            .working_directory
        };
        assert_eq!(dir("/cri", "/image"), "/cri");
        assert_eq!(dir("", "/image"), "/image");
        assert_eq!(dir("", ""), "/");
    }

    #[tokio::test]
    async fn env_bytes_are_decoded_into_key_equals_value() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        let mut config = container_config("app");
        config.envs = vec![KeyValue {
            key: "PATH".into(),
            value: b"/usr/bin".to_vec(),
        }];

        runtime
            .create_container(POD, "c1", &config, &sandbox_config())
            .await
            .unwrap();

        assert_eq!(added(&broker, 0).env, vec!["PATH=/usr/bin"]);
    }

    #[tokio::test]
    async fn the_log_path_is_joined_onto_the_sandbox_log_directory() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();

        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();

        assert_eq!(
            added(&broker, 0).log_path.as_deref(),
            Some("/var/log/pods/default_my-pod/app/0.log")
        );
    }

    #[tokio::test]
    async fn no_log_path_means_no_log_file_rather_than_an_empty_one() {
        let mut config = container_config("app");
        config.log_path = String::new();
        let wire = container_config_from_cri(
            "c1",
            &config,
            &sandbox_config(),
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert_eq!(wire.log_path, None);
    }

    #[tokio::test]
    async fn cpu_quota_and_period_become_whole_cores() {
        let wire = |quota: i64, period: i64| {
            container_config_from_cri(
                "c1",
                &ContainerConfig {
                    linux: Some(LinuxContainerConfig {
                        resources: Some(LinuxContainerResources {
                            cpu_quota: quota,
                            cpu_period: period,
                            memory_limit_in_bytes: 268_435_456,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &PodSandboxConfig::default(),
                BlockMount::block("ext4", "/i.ext4"),
                None,
            )
        };
        assert_eq!(wire(200_000, 100_000).cpus, Some(2));
        // Sub-core limits must not floor to zero cpus.
        assert_eq!(wire(50_000, 100_000).cpus, Some(1));
        // No quota means no limit, not a zero one.
        assert_eq!(wire(0, 100_000).cpus, None);
        assert_eq!(wire(200_000, 100_000).memory_in_bytes, Some(268_435_456));
    }

    #[tokio::test]
    async fn a_readonly_rootfs_becomes_a_mount_option() {
        let config = ContainerConfig {
            linux: Some(LinuxContainerConfig {
                security_context: Some(LinuxContainerSecurityContext {
                    readonly_rootfs: true,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let wire = container_config_from_cri(
            "c1",
            &config,
            &PodSandboxConfig::default(),
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert!(wire.rootfs.is_readonly(), "{:?}", wire.rootfs);
    }

    #[tokio::test]
    async fn masked_and_readonly_paths_are_empty_unless_cri_supplies_them() {
        // Empty means "keep Apple's OCI-standard defaults" — sending our own
        // would silently replace them.
        let wire = container_config_from_cri(
            "c1",
            &container_config("app"),
            &PodSandboxConfig::default(),
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert!(wire.masked_paths.is_empty());
        assert!(wire.readonly_paths.is_empty());
        assert!(
            wire.mounts.is_empty(),
            "default mounts are the broker's to seed"
        );
    }

    #[tokio::test]
    async fn a_cri_mount_becomes_a_virtiofs_share_not_a_bind() {
        let share = |readonly: bool, propagation: MountPropagation| {
            mount_from_cri(&Mount {
                host_path: "/host/data".into(),
                container_path: "/data".into(),
                readonly,
                propagation: propagation as i32,
                ..Default::default()
            })
        };

        let rw = share(false, MountPropagation::PropagationPrivate);
        assert_eq!(
            rw.type_, "virtiofs",
            "a bind cannot resolve a host path in a VM"
        );
        assert_eq!(rw.source, "/host/data");
        assert_eq!(rw.destination, "/data");
        assert_eq!(rw.options, vec!["rw"]);
        assert_eq!(
            share(true, MountPropagation::PropagationPrivate).options,
            vec!["ro"]
        );

        // Propagation describes how a bind relates to its parent mount, which a
        // virtiofs share has no notion of. Claiming rshared/rslave would promise
        // a guarantee nothing enforces, so it must not appear.
        for propagation in [
            MountPropagation::PropagationBidirectional,
            MountPropagation::PropagationHostToContainer,
        ] {
            let options = share(false, propagation).options;
            assert_eq!(options, vec!["rw"], "propagation must not be faked");
        }
    }

    #[tokio::test]
    async fn pod_sysctls_reach_every_container() {
        let sandbox = PodSandboxConfig {
            linux: Some(LinuxPodSandboxConfig {
                sysctls: HashMap::from([("net.ipv4.ip_forward".to_string(), "1".to_string())]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let wire = container_config_from_cri(
            "c1",
            &container_config("app"),
            &sandbox,
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert_eq!(
            wire.sysctl.get("net.ipv4.ip_forward").map(String::as_str),
            Some("1")
        );
    }

    // -- lifecycle ----------------------------------------------------------

    #[tokio::test]
    async fn an_init_container_then_a_sidecar_share_one_sandbox() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();

        broker.set_exit_code("init", 0);
        for id in ["init", "app"] {
            runtime
                .create_container(POD, id, &container_config(id), &sandbox_config())
                .await
                .unwrap();
            runtime.start_container(id).await.unwrap();
            if id == "init" {
                assert_eq!(runtime.wait_container("init").await.unwrap(), 0);
            }
        }

        assert_eq!(
            broker
                .requests()
                .iter()
                .filter(|m| **m == Method::CreatePod)
                .count(),
            1,
            "one VM for the whole pod"
        );
        assert_eq!(runtime.list_containers(POD).await, vec!["init", "app"]);
        assert_eq!(broker.running(POD), vec!["app"], "init was reaped");
    }

    /// critest's "stopping container" spec waits 60s for the state to leave
    /// RUNNING. It was hardcoded to RUNNING, so a created container looked
    /// started and a stopped one never exited.
    #[tokio::test]
    async fn a_container_reports_created_then_running_then_exited() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();

        let state = |id: &'static str| {
            let runtime = &runtime;
            async move {
                let e = runtime.container_entry(id).await.expect("entry");
                (e.started_at != 0, e.finished_at != 0, e.exit_code)
            }
        };

        broker.set_exit_code("c1", 7);
        runtime
            .create_container(POD, "c1", &container_config("c1"), &sandbox_config())
            .await
            .unwrap();
        assert_eq!(state("c1").await, (false, false, 0), "created, not started");

        runtime.start_container("c1").await.unwrap();
        // The exit monitor is a spawned task, so let it observe the exit the
        // fake reports the moment it is waited on.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (started, finished, code) = state("c1").await;
        assert!(started, "start must stamp started_at");
        assert!(finished, "the exit monitor must stamp finished_at");
        assert_eq!(code, 7, "and record the exit code CRI reports");
    }

    #[tokio::test]
    async fn a_failing_init_container_reports_its_exit_code() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        broker.set_exit_code("init", 1);

        runtime
            .create_container(POD, "init", &container_config("init"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("init").await.unwrap();

        assert_eq!(runtime.wait_container("init").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn a_zero_grace_period_kills_rather_than_stops() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();

        runtime.stop_container("c1", 0).await.unwrap();
        assert!(broker.requests().contains(&Method::KillContainer));
        assert_eq!(broker.params_for(Method::KillContainer)[0].signal, Some(9));

        runtime.start_container("c1").await.unwrap();
        runtime.stop_container("c1", 30).await.unwrap();
        assert!(broker.requests().contains(&Method::StopContainer));
    }

    #[tokio::test]
    async fn exec_sync_collects_the_exit_code_and_both_streams() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();
        broker.set_exit_code("exec-1", 3);
        broker.set_exec_output("exec-1", b"on stdout\n", b"on stderr\n");

        let result = runtime
            .exec_sync("c1", "exec-1", &["/bin/false".to_string()], vec![], 0)
            .await
            .unwrap();

        assert_eq!(
            result.exit_code, 3,
            "ExecSync needs the code, not just a pid"
        );
        assert_eq!(result.stdout, b"on stdout\n");
        assert_eq!(result.stderr, b"on stderr\n", "streams stay separate");

        // exec must precede the wait, and the capture paths must have been sent —
        // without them the broker has nowhere to put the output.
        let requests = broker.requests();
        let exec = requests.iter().position(|m| *m == Method::Exec).unwrap();
        let wait = requests
            .iter()
            .position(|m| *m == Method::WaitProcess)
            .unwrap();
        assert!(exec < wait, "exec then wait: {requests:?}");
        let params = &broker.params_for(Method::Exec)[0];
        assert!(params.stdout_path.is_some() && params.stderr_path.is_some());
    }

    /// The socket path is the caller's listener, so the broker connects rather
    /// than creating it — the opposite of `ExecSync`'s file capture, and the only
    /// ordering in which nothing written before the client attaches is lost.
    #[tokio::test]
    async fn exec_streaming_sends_socket_paths_not_file_paths() {
        let (broker, runtime, dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();

        let out = dir.path().join("out.sock");
        runtime
            .exec_streaming(
                "c1",
                "exec-1",
                &["/bin/sh".to_string()],
                vec![],
                true,
                StreamPaths {
                    stdin: Some(dir.path().join("in.sock")),
                    stdout: Some(out.clone()),
                    stderr: None,
                },
            )
            .await
            .unwrap();

        let params = &broker.params_for(Method::Exec)[0];
        assert_eq!(params.stdio_sockets, Some(true), "must not be file capture");
        assert_eq!(params.stdout_path.as_deref(), Some(out.as_path()));
        assert!(params.stdin_path.is_some());
        assert_eq!(params.terminal, Some(true), "tty must reach the broker");
    }

    #[tokio::test]
    async fn cri_stdin_reaches_the_container_config() {
        // A container created without stdin must get none: an empty stream that
        // never EOFs would hang anything reading it in the guest.
        let plain = container_config_from_cri(
            "c1",
            &container_config("app"),
            &sandbox_config(),
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert!(!plain.stdin);

        let mut config = container_config("app");
        config.stdin = true;
        let interactive = container_config_from_cri(
            "c1",
            &config,
            &sandbox_config(),
            BlockMount::block("ext4", "/i.ext4"),
            None,
        );
        assert!(interactive.stdin);
    }

    #[tokio::test]
    async fn attach_and_close_stdin_address_the_right_container() {
        let (broker, runtime, dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();

        runtime
            .attach(
                "c1",
                StreamPaths {
                    stdout: Some(dir.path().join("out.sock")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(broker.attached(), vec!["c1"]);

        runtime.close_stdin("c1").await.unwrap();
        assert!(broker.requests().contains(&Method::CloseStdin));

        // An unknown container is reported, not silently accepted.
        assert!(matches!(
            runtime.attach("nope", StreamPaths::default()).await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn resize_targets_the_exec_process() {
        let (broker, runtime, dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();
        runtime
            .exec_streaming(
                "c1",
                "exec-1",
                &["/bin/sh".to_string()],
                vec![],
                true,
                StreamPaths {
                    stdout: Some(dir.path().join("out.sock")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        runtime.resize_exec("c1", "exec-1", 120, 40).await.unwrap();

        let params = &broker.params_for(Method::Resize)[0];
        assert_eq!((params.width, params.height), (Some(120), Some(40)));
        assert_eq!(params.process_id.as_deref(), Some("exec-1"));
        // Resizing a process that was never exec'd must fail rather than no-op.
        assert!(runtime.resize_exec("c1", "nope", 80, 24).await.is_err());
    }

    #[tokio::test]
    async fn exec_sync_leaves_no_scratch_files_behind() {
        let (_broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();

        runtime
            .exec_sync("c1", "exec-1", &["/bin/true".to_string()], vec![], 0)
            .await
            .unwrap();

        assert!(
            !runtime.config.exec_dir.join("exec-1").exists(),
            "the per-exec scratch dir must be removed"
        );
    }

    /// A command that produced no output is empty, not an error: the broker only
    /// creates a file when capture was asked for, and an absent one is normal.
    #[tokio::test]
    async fn exec_sync_reports_empty_output_rather_than_failing() {
        let (_broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();

        let result = runtime
            .exec_sync("c1", "exec-1", &["/bin/true".to_string()], vec![], 0)
            .await
            .unwrap();

        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.is_empty() && result.stderr.is_empty());
    }

    #[tokio::test]
    async fn a_failed_add_container_does_not_leak_the_rootfs_image() {
        let (broker, runtime, _dir) = runtime().await;
        // No sandbox created on the broker, so addContainer is refused — but
        // provisionRootfs will already have happened.
        runtime
            .sandboxes
            .lock()
            .await
            .push(SandboxEntry::testing(POD, None));

        let err = runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .expect_err("must fail");
        assert!(err.to_string().contains("no such pod"), "got: {err}");
        assert!(
            broker.requests().contains(&Method::ReleaseRootfs),
            "the provisioned image must be reclaimed: {:?}",
            broker.requests()
        );
    }

    #[tokio::test]
    async fn removing_a_sandbox_forgets_its_containers_and_reclaims_images() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();

        runtime.remove_pod_sandbox(POD).await.unwrap();

        assert!(runtime.list_pods().await.is_empty());
        assert_eq!(runtime.container_image("c1").await, None);
        assert!(broker.requests().contains(&Method::ReleaseRootfs));
    }

    #[tokio::test]
    async fn a_pod_gets_one_address_that_reaches_cri() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();

        let ip = runtime.pod_ip(POD).await.expect("the pod must have an IP");
        assert_eq!(ip, "192.168.64.200");
        // Allocation is the broker's: we send no interfaces and it fills them in.
        let config = broker.params_for(Method::CreatePod)[0]
            .config
            .clone()
            .unwrap();
        assert!(
            config.interfaces.is_empty(),
            "the broker allocates; sending our own would bypass its pool"
        );
    }

    #[tokio::test]
    async fn two_pods_get_different_addresses_and_removal_returns_them() {
        let (_broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .run_pod_sandbox("pod-2", &sandbox_config())
            .await
            .unwrap();

        let first = runtime.pod_ip(POD).await.unwrap();
        let second = runtime.pod_ip("pod-2").await.unwrap();
        assert_ne!(first, second, "two pods must not share an address");

        runtime.remove_pod_sandbox(POD).await.unwrap();
        assert_eq!(runtime.pod_ip(POD).await, None);
        assert_eq!(
            runtime.pod_ip("pod-2").await.as_deref(),
            Some(second.as_str()),
            "removing one pod must not disturb another's address"
        );
    }

    #[tokio::test]
    async fn port_forward_dials_the_pod_address() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (_broker, runtime, _dir) = runtime().await;
        // Stand in for the pod: a listener on an address we can actually reach.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        runtime
            .sandboxes
            .lock()
            .await
            .push(SandboxEntry::testing(POD, Some("127.0.0.1")));

        let accepted = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(b"pong").await.unwrap();
        });

        let mut stream = runtime.port_forward(POD, port).await.expect("port forward");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong", "the caller must get the pod's bytes");
        accepted.await.unwrap();
    }

    /// A sandbox with no address cannot be forwarded to. Reporting that beats
    /// dialling nothing and timing out inside the kubelet's stream.
    #[tokio::test]
    async fn port_forward_without_an_address_is_a_failed_precondition() {
        let (_broker, runtime, _dir) = runtime().await;
        runtime
            .sandboxes
            .lock()
            .await
            .push(SandboxEntry::testing(POD, None));

        assert!(matches!(
            runtime.port_forward(POD, 80).await,
            Err(Error::FailedPrecondition(_))
        ));
    }

    // -- images -------------------------------------------------------------

    #[tokio::test]
    async fn images_are_answered_from_the_brokers_store_not_the_cli_store() {
        let (_broker, runtime, _dir) = runtime().await;
        assert!(runtime.list_images().await.unwrap().is_empty());
        assert!(runtime
            .image_status("busybox:1.29")
            .await
            .unwrap()
            .is_none());

        let id = runtime.pull_image("busybox:1.29").await.unwrap();
        assert!(id.starts_with("sha256:"), "the id must be the digest: {id}");

        // Pull -> list -> status must agree; that consistency is the whole reason
        // these RPCs cannot be served from Apple's CLI store while the pod runs
        // rootfs images out of the broker's.
        let listed = runtime.list_images().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        let status = runtime.image_status("busybox:1.29").await.unwrap().unwrap();
        assert_eq!(status.id, id);

        runtime.remove_image("busybox:1.29").await.unwrap();
        assert!(runtime
            .image_status("busybox:1.29")
            .await
            .unwrap()
            .is_none());
        assert!(runtime.list_images().await.unwrap().is_empty());
        // Removing it again is success, as CRI requires.
        runtime.remove_image("busybox:1.29").await.unwrap();
    }

    #[tokio::test]
    async fn an_image_reports_a_resolvable_repo_digest() {
        let one = |wire: ImageWire| aggregate_images(&[wire]).remove(0);
        let image = one(ImageWire {
            reference: "registry.k8s.io/busybox:1.29".to_string(),
            digest: "sha256:abc".to_string(),
            size_bytes: 42,
            ..Default::default()
        });
        assert_eq!(image.repo_tags, vec!["registry.k8s.io/busybox:1.29"]);
        // The tag must be stripped: `repo:tag@sha256:…` is not resolvable.
        assert_eq!(
            image.repo_digests,
            vec!["registry.k8s.io/busybox@sha256:abc"]
        );
        assert_eq!(image.size, 42);

        // A registry port is not a tag.
        let ported = one(ImageWire {
            reference: "localhost:5000/busybox".to_string(),
            digest: "sha256:def".to_string(),
            ..Default::default()
        });
        assert_eq!(
            ported.repo_digests,
            vec!["localhost:5000/busybox@sha256:def"]
        );
    }

    /// The broker stores one entry per reference; CRI wants one per image. Three
    /// tags of the same image are one image with three `repo_tags` — critest's
    /// "listImage should get exactly 3 repoTags" spec.
    #[test]
    fn tags_of_one_image_collapse_into_a_single_cri_image() {
        let tagged = |reference: &str, digest: &str| ImageWire {
            reference: reference.to_string(),
            digest: digest.to_string(),
            size_bytes: 10,
            ..Default::default()
        };
        let images = aggregate_images(&[
            tagged("gcr.io/test/tags:1", "sha256:aaa"),
            tagged("gcr.io/test/tags:2", "sha256:aaa"),
            tagged("gcr.io/test/tags:3", "sha256:aaa"),
            tagged("gcr.io/test/other:1", "sha256:bbb"),
        ]);

        assert_eq!(images.len(), 2, "two digests, two images");
        assert_eq!(images[0].id, "sha256:aaa");
        assert_eq!(
            images[0].repo_tags,
            vec![
                "gcr.io/test/tags:1",
                "gcr.io/test/tags:2",
                "gcr.io/test/tags:3"
            ]
        );
        // One digest reference, not one per tag: they all name the same content.
        assert_eq!(images[0].repo_digests, vec!["gcr.io/test/tags@sha256:aaa"]);

        // A digest reference is not a tag.
        let by_digest = aggregate_images(&[tagged("gcr.io/test/x@sha256:ccc", "sha256:ccc")]);
        assert!(by_digest[0].repo_tags.is_empty());
        assert_eq!(by_digest[0].repo_digests, vec!["gcr.io/test/x@sha256:ccc"]);
    }

    /// CRI callers name an image by id, by `repo@digest`, or by any tag on it —
    /// short or fully qualified. All must resolve to the same image, which is
    /// critest's "image status should support all kinds of references".
    #[test]
    fn an_image_resolves_by_id_digest_or_any_tag() {
        let images = aggregate_images(&[
            ImageWire {
                reference: "docker.io/library/busybox:1.29".to_string(),
                digest: "sha256:aaa".to_string(),
                ..Default::default()
            },
            ImageWire {
                reference: "gcr.io/mirror/busybox:1.29".to_string(),
                digest: "sha256:aaa".to_string(),
                ..Default::default()
            },
        ]);
        let resolves = |q: &str| find_image(&images, q).map(|i| i.id);

        assert_eq!(resolves("sha256:aaa").as_deref(), Some("sha256:aaa"));
        assert_eq!(
            resolves("docker.io/library/busybox@sha256:aaa").as_deref(),
            Some("sha256:aaa")
        );
        assert_eq!(
            resolves("docker.io/library/busybox:1.29").as_deref(),
            Some("sha256:aaa")
        );
        // Short form, normalised to the same docker.io reference.
        assert_eq!(resolves("busybox:1.29").as_deref(), Some("sha256:aaa"));
        // A second registry's tag on the same content resolves too.
        assert_eq!(
            resolves("gcr.io/mirror/busybox:1.29").as_deref(),
            Some("sha256:aaa")
        );
        assert_eq!(resolves("busybox:nope"), None);
    }

    /// CRI wants a numeric uid *or* a username, and the group half of the OCI
    /// `User` string is neither — reporting `www-data:www-data` as the username
    /// is a bug this shim already had to fix once on the CLI path.
    #[tokio::test]
    async fn the_image_user_splits_into_uid_or_username() {
        let user = |raw: &str| {
            let image = image_identity(&ImageWire {
                user: raw.to_string(),
                ..Default::default()
            });
            (image.uid.map(|u| u.value), image.username)
        };
        assert_eq!(user("0"), (Some(0), String::new()));
        assert_eq!(user("1000:1000"), (Some(1000), String::new()));
        assert_eq!(user("www-data"), (None, "www-data".to_string()));
        assert_eq!(user("www-data:www-data"), (None, "www-data".to_string()));
        assert_eq!(user(""), (None, String::new()));
    }

    /// `host_port == 0` means "expose only" — the container port is reachable on
    /// the pod IP and nothing may be bound on the host. Binding it anyway would
    /// squat a random port, and on the CLI path this exact case once made
    /// container creation fail outright.
    #[tokio::test]
    async fn only_real_host_ports_are_published() {
        let mapping = |host: i32, container: i32, protocol: Protocol| PortMapping {
            host_port: host,
            container_port: container,
            protocol: protocol as i32,
            host_ip: "127.0.0.1".to_string(),
        };
        let published = publish_ports(
            POD,
            "127.0.0.1",
            &[
                mapping(0, 80, Protocol::Tcp),
                // UDP is not implemented; it must be skipped, not bound as TCP.
                mapping(0, 53, Protocol::Udp),
                mapping(0, 80, Protocol::Tcp),
            ],
        )
        .await;
        assert!(published.is_empty(), "expose-only mappings bind nothing");

        // A real host port is bound, and dropping the sandbox frees it.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let free_port = listener.local_addr().unwrap().port();
        drop(listener);

        let published = publish_ports(
            POD,
            "127.0.0.1",
            &[mapping(free_port as i32, 80, Protocol::Tcp)],
        )
        .await;
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].host_port, free_port);
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", free_port))
                .await
                .is_ok(),
            "the published port must be accepting"
        );

        drop(published);
        // The abort is asynchronous; give the runtime a moment to unbind.
        for _ in 0..50 {
            if tokio::net::TcpListener::bind(("127.0.0.1", free_port))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the host port was not released when the sandbox dropped");
    }

    /// CRI splits its RPCs into the idempotent ones — which must succeed for an
    /// id that never existed — and the rest, which must report `NotFound`.
    /// api.proto says so for `StopPodSandbox`, `StopContainer` and
    /// `RemoveContainer`; critest's Idempotence suite checks each one.
    #[tokio::test]
    async fn the_idempotent_rpcs_accept_an_unknown_id_and_the_rest_do_not() {
        let (_broker, runtime, _dir) = runtime().await;

        runtime.stop_pod_sandbox("nope").await.unwrap();
        runtime.remove_pod_sandbox("nope").await.unwrap();
        runtime.stop_container("nope", 0).await.unwrap();
        runtime.remove_container("nope").await.unwrap();
        runtime.remove_image("nope").await.unwrap();

        // Starting a container that does not exist is a real error: there is
        // nothing to be idempotent about.
        assert!(matches!(
            runtime.start_container("nope").await,
            Err(Error::NotFound(_))
        ));
    }

    /// Stopping a container that has already exited must not go near the guest,
    /// let alone fail — the kubelet stops containers it has already reaped.
    #[tokio::test]
    async fn stopping_an_already_exited_container_is_success() {
        let (broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        broker.set_exit_code("c1", 0);
        runtime
            .create_container(POD, "c1", &container_config("c1"), &sandbox_config())
            .await
            .unwrap();
        runtime.start_container("c1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        runtime.stop_container("c1", 0).await.unwrap();
        runtime.stop_container("c1", 0).await.unwrap();
    }

    #[tokio::test]
    async fn stats_convert_microseconds_to_nanoseconds_and_derive_working_set() {
        let (_broker, runtime, _dir) = runtime().await;
        runtime
            .run_pod_sandbox(POD, &sandbox_config())
            .await
            .unwrap();
        runtime
            .create_container(POD, "c1", &container_config("app"), &sandbox_config())
            .await
            .unwrap();

        let stats = runtime.container_stats("c1").await.unwrap().unwrap();
        assert_eq!(stats.attributes.unwrap().id, "c1");
        // The fake reports zeroes; what matters is that the shape is populated
        // rather than dropped, and the units are converted.
        assert_eq!(stats.cpu.unwrap().usage_core_nano_seconds.unwrap().value, 0);
        assert!(stats.memory.unwrap().working_set_bytes.is_some());
    }
}

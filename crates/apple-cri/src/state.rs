// SPDX-License-Identifier: Apache-2.0

//! Shim-side bookkeeping for everything Apple's runtime does not record.
//!
//! `container inspect` reports a container's `status` and nothing else about
//! its history: **no creation/start/finish timestamps and no exit code**
//! (verified against 0.7.1). CRI requires all of them — `ContainerStatus`
//! carries `created_at`/`started_at`/`finished_at`/`exit_code`, and the kubelet
//! drives restart policy off the exit code. So this shim keeps its own
//! authoritative record, persisted through [`CheckpointStore`] so it survives a
//! restart, with an in-memory index in front of it.
//!
//! The records are also the authority for CRI labels and annotations. Mirroring
//! them into Apple's container labels would be lossy: `--label key=value`
//! rejects any `=` in the *value*, and annotation values routinely contain one.
//! A small, safe subset is still mirrored (see [`crate::labels_for`]) so that
//! `container list` alone is enough to re-adopt orphans.

use std::collections::BTreeMap;
use std::sync::Mutex;

use cri_proto::v1::{ContainerMetadata, PodSandboxMetadata};
use cri_server::checkpoint::CheckpointStore;
use cri_server::error::Result;
use serde::{Deserialize, Serialize};

/// Wall-clock now, in Unix nanoseconds (the CRI timestamp unit).
pub fn now_nanos() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp() * 1_000_000_000)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PortMappingRecord {
    pub protocol: i32,
    pub container_port: i32,
    pub host_port: i32,
    #[serde(default)]
    pub host_ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DnsRecord {
    #[serde(default)]
    pub servers: Vec<String>,
    #[serde(default)]
    pub searches: Vec<String>,
    #[serde(default)]
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SandboxRecord {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub attempt: u32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    pub created_at: i64,
    /// CRI `SANDBOX_READY` while true; `StopPodSandbox` clears it.
    pub ready: bool,
    #[serde(default)]
    pub log_directory: String,
    #[serde(default)]
    pub hostname: String,
    /// The Apple network this sandbox's containers attach to.
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub port_mappings: Vec<PortMappingRecord>,
    #[serde(default)]
    pub dns: DnsRecord,
    #[serde(default)]
    pub runtime_handler: String,
    /// Last observed pod IP (the primary container's address). Cached so a
    /// stopped sandbox can still report the address it had.
    #[serde(default)]
    pub ip: String,
}

impl SandboxRecord {
    pub fn metadata(&self) -> PodSandboxMetadata {
        PodSandboxMetadata {
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            uid: self.uid.clone(),
            attempt: self.attempt,
        }
    }
}

/// A CRI mount, echoed back verbatim by `ContainerStatus`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MountRecord {
    pub container_path: String,
    #[serde(default)]
    pub host_path: String,
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub propagation: i32,
    #[serde(default)]
    pub selinux_relabel: bool,
    #[serde(default)]
    pub recursive_read_only: bool,
}

/// The Linux resource limits the container was created with. CRI requires
/// `ContainerStatus.resources` to reflect the request, whether or not the
/// runtime could honour every field.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourcesRecord {
    #[serde(default)]
    pub cpu_period: i64,
    #[serde(default)]
    pub cpu_quota: i64,
    #[serde(default)]
    pub cpu_shares: i64,
    #[serde(default)]
    pub memory_limit_in_bytes: i64,
    #[serde(default)]
    pub oom_score_adj: i64,
    #[serde(default)]
    pub cpuset_cpus: String,
    #[serde(default)]
    pub cpuset_mems: String,
    #[serde(default)]
    pub memory_swap_limit_in_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerRecord {
    pub id: String,
    pub sandbox_id: String,
    /// The container's name *within* the pod (CRI `ContainerMetadata.name`).
    pub name: String,
    pub attempt: u32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    /// The image as the caller asked for it (`ImageSpec.image`).
    #[serde(default)]
    pub image_ref: String,
    /// The resolved image id (index digest).
    #[serde(default)]
    pub image_id: String,
    pub created_at: i64,
    #[serde(default)]
    pub started_at: i64,
    #[serde(default)]
    pub finished_at: i64,
    #[serde(default)]
    pub exit_code: i32,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
    /// Absolute path of the CRI log file this container's stdio is relayed to.
    #[serde(default)]
    pub log_path: String,
    #[serde(default)]
    pub tty: bool,
    #[serde(default)]
    pub stdin: bool,
    /// Set once `StartContainer` has run; distinguishes CREATED from EXITED
    /// for a container Apple merely reports as `stopped`.
    #[serde(default)]
    pub started: bool,
    /// Set when the exit code is known (by the attach supervisor or by
    /// [`crate::logs::exit_status_from_vminitd_log`]).
    #[serde(default)]
    pub finished: bool,
    #[serde(default)]
    pub mounts: Vec<MountRecord>,
    #[serde(default)]
    pub resources: ResourcesRecord,
    /// CRI `Signal` enum value used by `StopContainer`.
    #[serde(default)]
    pub stop_signal: i32,
    /// The uid/gid the container's init process runs as, for
    /// `ContainerStatus.user`.
    #[serde(default)]
    pub run_as_uid: i64,
    #[serde(default)]
    pub run_as_gid: i64,
}

impl ContainerRecord {
    pub fn metadata(&self) -> ContainerMetadata {
        ContainerMetadata {
            name: self.name.clone(),
            attempt: self.attempt,
        }
    }
}

/// Persistent, in-memory-indexed store of sandbox and container records.
pub struct Store {
    sandbox_ckpt: CheckpointStore,
    container_ckpt: CheckpointStore,
    sandboxes: Mutex<BTreeMap<String, SandboxRecord>>,
    containers: Mutex<BTreeMap<String, ContainerRecord>>,
}

impl Store {
    /// Open the store under `root`, loading every record already on disk.
    pub fn open(root: &std::path::Path) -> Result<Self> {
        let sandbox_ckpt = CheckpointStore::open(root.join("sandbox"))?;
        let container_ckpt = CheckpointStore::open(root.join("container"))?;

        let mut sandboxes = BTreeMap::new();
        for key in sandbox_ckpt.list()? {
            if let Some(rec) = sandbox_ckpt.load::<SandboxRecord>(&key)? {
                sandboxes.insert(rec.id.clone(), rec);
            }
        }
        let mut containers = BTreeMap::new();
        for key in container_ckpt.list()? {
            if let Some(rec) = container_ckpt.load::<ContainerRecord>(&key)? {
                containers.insert(rec.id.clone(), rec);
            }
        }
        tracing::info!(
            sandboxes = sandboxes.len(),
            containers = containers.len(),
            "loaded checkpoints"
        );
        Ok(Self {
            sandbox_ckpt,
            container_ckpt,
            sandboxes: Mutex::new(sandboxes),
            containers: Mutex::new(containers),
        })
    }

    // ---- sandboxes -------------------------------------------------------

    pub fn put_sandbox(&self, rec: SandboxRecord) -> Result<()> {
        self.sandbox_ckpt.save(&rec.id, &rec)?;
        self.sandboxes.lock().unwrap().insert(rec.id.clone(), rec);
        Ok(())
    }

    pub fn sandbox(&self, id: &str) -> Option<SandboxRecord> {
        self.sandboxes.lock().unwrap().get(id).cloned()
    }

    pub fn sandboxes(&self) -> Vec<SandboxRecord> {
        self.sandboxes.lock().unwrap().values().cloned().collect()
    }

    pub fn delete_sandbox(&self, id: &str) -> Result<()> {
        self.sandboxes.lock().unwrap().remove(id);
        self.sandbox_ckpt.delete(id)
    }

    /// Apply `f` to a sandbox record and persist it. `Ok(false)` if absent.
    pub fn update_sandbox(&self, id: &str, f: impl FnOnce(&mut SandboxRecord)) -> Result<bool> {
        let updated = {
            let mut guard = self.sandboxes.lock().unwrap();
            match guard.get_mut(id) {
                Some(rec) => {
                    f(rec);
                    rec.clone()
                }
                None => return Ok(false),
            }
        };
        self.sandbox_ckpt.save(id, &updated)?;
        Ok(true)
    }

    // ---- containers ------------------------------------------------------

    pub fn put_container(&self, rec: ContainerRecord) -> Result<()> {
        self.container_ckpt.save(&rec.id, &rec)?;
        self.containers.lock().unwrap().insert(rec.id.clone(), rec);
        Ok(())
    }

    pub fn container(&self, id: &str) -> Option<ContainerRecord> {
        self.containers.lock().unwrap().get(id).cloned()
    }

    pub fn containers(&self) -> Vec<ContainerRecord> {
        self.containers.lock().unwrap().values().cloned().collect()
    }

    /// Every container belonging to `sandbox_id`.
    pub fn containers_in_sandbox(&self, sandbox_id: &str) -> Vec<ContainerRecord> {
        self.containers
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.sandbox_id == sandbox_id)
            .cloned()
            .collect()
    }

    pub fn delete_container(&self, id: &str) -> Result<()> {
        self.containers.lock().unwrap().remove(id);
        self.container_ckpt.delete(id)
    }

    pub fn update_container(&self, id: &str, f: impl FnOnce(&mut ContainerRecord)) -> Result<bool> {
        let updated = {
            let mut guard = self.containers.lock().unwrap();
            match guard.get_mut(id) {
                Some(rec) => {
                    f(rec);
                    rec.clone()
                }
                None => return Ok(false),
            }
        };
        self.container_ckpt.save(id, &updated)?;
        Ok(true)
    }

    /// Record a container's exit exactly once — the attach supervisor and the
    /// `vminitd.log` fallback race, and the first writer wins.
    pub fn record_exit(&self, id: &str, exit_code: i32, reason: &str) -> Result<()> {
        self.update_container(id, |rec| {
            if rec.finished {
                return;
            }
            rec.finished = true;
            rec.exit_code = exit_code;
            rec.finished_at = now_nanos();
            if rec.reason.is_empty() {
                rec.reason = reason.to_string();
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sandbox(id: &str) -> SandboxRecord {
        SandboxRecord {
            id: id.into(),
            name: "web".into(),
            namespace: "default".into(),
            uid: "uid-1".into(),
            created_at: 123,
            ready: true,
            log_directory: "/var/log/pods/x".into(),
            hostname: "web".into(),
            network: "k8s-pods".into(),
            ..Default::default()
        }
    }

    fn container(id: &str, sandbox_id: &str) -> ContainerRecord {
        ContainerRecord {
            id: id.into(),
            sandbox_id: sandbox_id.into(),
            name: "app".into(),
            image_ref: "alpine:latest".into(),
            image_id: "sha256:abc".into(),
            created_at: 1,
            ..Default::default()
        }
    }

    #[test]
    fn records_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path()).unwrap();
            store.put_sandbox(sandbox("sb1")).unwrap();
            store.put_container(container("c1", "sb1")).unwrap();
            store.put_container(container("c2", "sb1")).unwrap();
            store
                .update_container("c1", |c| {
                    c.started = true;
                    c.started_at = 500;
                })
                .unwrap();
        }
        // Reopen: the on-disk checkpoints must rebuild the index.
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.sandboxes().len(), 1);
        assert_eq!(store.containers_in_sandbox("sb1").len(), 2);
        let c1 = store.container("c1").unwrap();
        assert!(c1.started);
        assert_eq!(c1.started_at, 500);
        assert_eq!(
            store.sandbox("sb1").unwrap().log_directory,
            "/var/log/pods/x"
        );
    }

    #[test]
    fn record_exit_is_write_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put_container(container("c1", "sb1")).unwrap();

        store.record_exit("c1", 42, "Error").unwrap();
        let after = store.container("c1").unwrap();
        assert!(after.finished);
        assert_eq!(after.exit_code, 42);
        assert_eq!(after.reason, "Error");
        assert!(after.finished_at > 0);

        // The losing writer (attach supervisor vs vminitd.log) must not
        // overwrite the recorded code.
        store.record_exit("c1", 0, "Completed").unwrap();
        let again = store.container("c1").unwrap();
        assert_eq!(again.exit_code, 42);
        assert_eq!(again.reason, "Error");
        assert_eq!(again.finished_at, after.finished_at);
    }

    #[test]
    fn update_missing_record_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert!(!store.update_container("nope", |_| {}).unwrap());
        assert!(!store.update_sandbox("nope", |_| {}).unwrap());
        // Deleting an absent record is idempotent.
        store.delete_container("nope").unwrap();
        store.delete_sandbox("nope").unwrap();
    }

    #[test]
    fn delete_removes_from_index_and_disk() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path()).unwrap();
            store.put_sandbox(sandbox("sb1")).unwrap();
            store.put_container(container("c1", "sb1")).unwrap();
            store.delete_container("c1").unwrap();
            store.delete_sandbox("sb1").unwrap();
            assert!(store.container("c1").is_none());
        }
        let store = Store::open(dir.path()).unwrap();
        assert!(store.sandboxes().is_empty());
        assert!(store.containers().is_empty());
    }
}

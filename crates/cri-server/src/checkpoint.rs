// SPDX-License-Identifier: Apache-2.0

//! Versioned, checksummed JSON file store for shim bookkeeping.
//!
//! Port of the cri-dockerd `store/` pattern: shims that cannot store CRI
//! metadata inside their runtime (port mappings, host-network flags) persist
//! it here, keyed by sandbox/container id. Writes are atomic
//! (tempfile + rename); corrupt entries are deleted on read and reported as
//! absent, so a partially-written checkpoint never wedges recovery.

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const VERSION: &str = "v1";

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: String,
    checksum: u32,
    data: serde_json::Value,
}

pub struct CheckpointStore {
    dir: PathBuf,
}

impl CheckpointStore {
    /// Open (creating if needed) a store rooted at `dir`.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        if key.is_empty()
            || key.contains(std::path::MAIN_SEPARATOR)
            || key.contains('/')
            || key == "."
            || key == ".."
        {
            return Err(Error::InvalidArgument(format!(
                "invalid checkpoint key {key:?}"
            )));
        }
        Ok(self.dir.join(format!("{key}.json")))
    }

    /// Atomically persist `data` under `key`, replacing any previous value.
    pub fn save<T: Serialize>(&self, key: &str, data: &T) -> Result<()> {
        let path = self.path(key)?;
        let data = serde_json::to_value(data)
            .map_err(|e| Error::Internal(format!("checkpoint encode: {e}")))?;
        let payload = serde_json::to_vec(&data)
            .map_err(|e| Error::Internal(format!("checkpoint encode: {e}")))?;
        let envelope = Envelope {
            version: VERSION.to_string(),
            checksum: crc32fast::hash(&payload),
            data,
        };
        let bytes = serde_json::to_vec_pretty(&envelope)
            .map_err(|e| Error::Internal(format!("checkpoint encode: {e}")))?;

        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Load `key`. Returns `Ok(None)` when absent — or when corrupt, in which
    /// case the corrupt file is deleted.
    pub fn load<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let path = self.path(key)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match Self::decode(&bytes) {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                tracing::warn!(key, %err, "deleting corrupt checkpoint");
                let _ = std::fs::remove_file(&path);
                Ok(None)
            }
        }
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let envelope: Envelope = serde_json::from_slice(bytes)
            .map_err(|e| Error::Internal(format!("checkpoint parse: {e}")))?;
        if envelope.version != VERSION {
            return Err(Error::Internal(format!(
                "checkpoint version {:?} (want {VERSION:?})",
                envelope.version
            )));
        }
        let payload = serde_json::to_vec(&envelope.data)
            .map_err(|e| Error::Internal(format!("checkpoint re-encode: {e}")))?;
        let checksum = crc32fast::hash(&payload);
        if checksum != envelope.checksum {
            return Err(Error::Internal(format!(
                "checkpoint checksum mismatch (got {checksum}, want {})",
                envelope.checksum
            )));
        }
        serde_json::from_value(envelope.data)
            .map_err(|e| Error::Internal(format!("checkpoint decode: {e}")))
    }

    /// Delete `key`; absent keys are fine (idempotent).
    pub fn delete(&self, key: &str) -> Result<()> {
        match std::fs::remove_file(self.path(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// List stored keys (unordered).
    pub fn list(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    keys.push(stem.to_string());
                }
            }
        }
        Ok(keys)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct SandboxCheckpoint {
        port_mappings: Vec<(i32, i32)>,
        host_network: bool,
    }

    fn sample() -> SandboxCheckpoint {
        SandboxCheckpoint {
            port_mappings: vec![(8080, 80)],
            host_network: false,
        }
    }

    #[test]
    fn save_load_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(dir.path()).unwrap();

        store.save("sandbox-1", &sample()).unwrap();
        let loaded: Option<SandboxCheckpoint> = store.load("sandbox-1").unwrap();
        assert_eq!(loaded, Some(sample()));
        assert_eq!(store.list().unwrap(), vec!["sandbox-1".to_string()]);

        store.delete("sandbox-1").unwrap();
        store.delete("sandbox-1").unwrap(); // idempotent
        let gone: Option<SandboxCheckpoint> = store.load("sandbox-1").unwrap();
        assert_eq!(gone, None);
    }

    #[test]
    fn corrupt_file_is_deleted_and_reported_absent() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(dir.path()).unwrap();
        store.save("bad", &sample()).unwrap();

        // Corrupt the payload without breaking JSON: flip the checksum.
        let path = dir.path().join("bad.json");
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("\"checksum\":", "\"checksum\": 1234567, \"was\":");
        std::fs::write(&path, text).unwrap();

        let loaded: Option<SandboxCheckpoint> = store.load("bad").unwrap();
        assert_eq!(loaded, None);
        assert!(!path.exists(), "corrupt checkpoint should be deleted");

        // Truncated/garbage file too.
        std::fs::write(&path, b"{ not json").unwrap();
        let loaded: Option<SandboxCheckpoint> = store.load("bad").unwrap();
        assert_eq!(loaded, None);
        assert!(!path.exists());
    }

    #[test]
    fn rejects_path_escaping_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(dir.path()).unwrap();
        assert!(store.save("../evil", &sample()).is_err());
        assert!(store.save("", &sample()).is_err());
    }
}

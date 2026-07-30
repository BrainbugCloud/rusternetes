// SPDX-License-Identifier: Apache-2.0

//! Container stats from `container stats --format json --no-stream`.
//!
//! This is the one place where Apple's per-container VM is an *advantage*: the
//! numbers come from the guest's own cgroup accounting via `vminitd`, so the
//! shim never reads `/proc` or cgroupfs on the host — neither of which exists on
//! macOS. That is why this shim needs no cAdvisor equivalent.
//!
//! What Apple reports (0.7.1): `cpuUsageUsec`, `memoryUsageBytes`,
//! `memoryLimitBytes`, `networkRx/TxBytes`, `blockRead/WriteBytes`,
//! `numProcesses`. What it does not report, and CRI asks for: RSS, page faults,
//! working-set, and the writable-layer size. `working_set_bytes` is reported as
//! the usage (CRI's own convention when no separate figure exists) and the
//! writable-layer figure is measured from the container's `rootfs.ext4` on the
//! host.

use std::collections::HashMap;

use crate::backend::AppleBackend;
use crate::model::StatsJson;
use crate::state::{now_nanos, ContainerRecord};
use cri_proto::v1::*;

impl AppleBackend {
    /// One `container stats` sweep, indexed by container id.
    ///
    /// A single call covers every running container, so `ListContainerStats`
    /// costs one process spawn rather than one per container.
    pub(crate) async fn stats_snapshot(&self) -> HashMap<String, StatsJson> {
        match self
            .cli
            .json::<Vec<StatsJson>>(
                "container stats",
                &["stats", "--no-stream", "--format", "json"],
            )
            .await
        {
            Ok(list) => list.into_iter().map(|s| (s.id.clone(), s)).collect(),
            Err(err) => {
                // Stats are advisory: the kubelet must still get a container
                // list even when the runtime cannot account for them.
                tracing::debug!(%err, "container stats unavailable");
                HashMap::new()
            }
        }
    }

    /// Build CRI stats for one container from a sweep entry.
    pub(crate) fn cri_stats(
        &self,
        rec: &ContainerRecord,
        stats: Option<&StatsJson>,
    ) -> ContainerStats {
        let now = now_nanos();
        let attributes = ContainerAttributes {
            id: rec.id.clone(),
            metadata: Some(rec.metadata()),
            labels: rec.labels.clone().into_iter().collect(),
            annotations: rec.annotations.clone().into_iter().collect(),
        };

        let (cpu, memory) = match stats {
            Some(s) => (
                Some(CpuUsage {
                    timestamp: now,
                    // Apple reports microseconds; CRI wants nanoseconds.
                    usage_core_nano_seconds: Some(UInt64Value {
                        value: s.cpu_usage_usec.saturating_mul(1_000),
                    }),
                    usage_nano_cores: None,
                    psi: None,
                }),
                Some(MemoryUsage {
                    timestamp: now,
                    // No separate working-set figure exists; CRI consumers
                    // treat usage as the working set when that is all there is.
                    working_set_bytes: Some(UInt64Value {
                        value: s.memory_usage_bytes,
                    }),
                    available_bytes: Some(UInt64Value {
                        value: s.memory_limit_bytes.saturating_sub(s.memory_usage_bytes),
                    }),
                    usage_bytes: Some(UInt64Value {
                        value: s.memory_usage_bytes,
                    }),
                    rss_bytes: None,
                    page_faults: None,
                    major_page_faults: None,
                    psi: None,
                }),
            ),
            None => (None, None),
        };

        let writable_layer = writable_layer_usage(&rec.id, now);

        ContainerStats {
            attributes: Some(attributes),
            cpu,
            memory,
            writable_layer,
            swap: None,
            io: None,
        }
    }
}

/// The container's writable layer: the on-host size of its `rootfs.ext4`.
///
/// The image is a 512 GiB *sparse* file, so the apparent length is meaningless;
/// only the allocated block count reflects what the container has written.
fn writable_layer_usage(id: &str, timestamp: i64) -> Option<FilesystemUsage> {
    let dir = crate::logs::container_state_dir(id);
    let rootfs = dir.join("rootfs.ext4");
    let used = allocated_bytes(&rootfs)?;
    Some(FilesystemUsage {
        timestamp,
        fs_id: Some(FilesystemIdentifier {
            mountpoint: dir.to_string_lossy().to_string(),
        }),
        used_bytes: Some(UInt64Value { value: used }),
        inodes_used: Some(UInt64Value { value: 0 }),
    })
}

/// Blocks actually allocated to a file, in bytes (`st_blocks` × 512).
fn allocated_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.blocks()).saturating_mul(512))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> ContainerRecord {
        ContainerRecord {
            id: "k8s_app_web_default_uid_0".into(),
            sandbox_id: "k8s_POD_web_default_uid_0".into(),
            name: "app".into(),
            labels: std::collections::BTreeMap::from([("app".into(), "web".into())]),
            image_ref: "alpine:latest".into(),
            image_id: "sha256:abc".into(),
            created_at: 1,
            started_at: 2,
            started: true,
            ..Default::default()
        }
    }

    fn sample() -> StatsJson {
        StatsJson {
            id: "k8s_app_web_default_uid_0".into(),
            cpu_usage_usec: 32_916,
            memory_usage_bytes: 2_797_568,
            memory_limit_bytes: 1_073_741_824,
            network_rx_bytes: 17_122,
            network_tx_bytes: 816,
            block_read_bytes: 2_293_760,
            block_write_bytes: 0,
            num_processes: 2,
        }
    }

    #[test]
    fn converts_microseconds_to_nanoseconds() {
        let backend = crate::backend::test_backend();
        let stats = backend.cri_stats(&record(), Some(&sample()));
        let cpu = stats.cpu.unwrap();
        assert_eq!(cpu.usage_core_nano_seconds.unwrap().value, 32_916_000);
        assert!(cpu.timestamp > 0);
    }

    #[test]
    fn memory_available_is_limit_minus_usage() {
        let backend = crate::backend::test_backend();
        let stats = backend.cri_stats(&record(), Some(&sample()));
        let mem = stats.memory.unwrap();
        assert_eq!(mem.usage_bytes.unwrap().value, 2_797_568);
        assert_eq!(mem.working_set_bytes.unwrap().value, 2_797_568);
        assert_eq!(
            mem.available_bytes.unwrap().value,
            1_073_741_824 - 2_797_568
        );
    }

    #[test]
    fn usage_above_limit_saturates_instead_of_wrapping() {
        let backend = crate::backend::test_backend();
        let over = StatsJson {
            memory_usage_bytes: 2_000,
            memory_limit_bytes: 1_000,
            ..sample()
        };
        let stats = backend.cri_stats(&record(), Some(&over));
        assert_eq!(stats.memory.unwrap().available_bytes.unwrap().value, 0);
    }

    #[test]
    fn attributes_are_populated_even_without_stats() {
        let backend = crate::backend::test_backend();
        let stats = backend.cri_stats(&record(), None);
        let attrs = stats.attributes.unwrap();
        assert_eq!(attrs.id, "k8s_app_web_default_uid_0");
        assert_eq!(attrs.metadata.unwrap().name, "app");
        assert_eq!(attrs.labels.get("app").map(String::as_str), Some("web"));
        // A stats-less container still yields a well-formed record.
        assert!(stats.cpu.is_none());
        assert!(stats.memory.is_none());
    }

    #[test]
    fn allocated_bytes_ignores_apparent_length_of_sparse_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse");
        std::fs::write(&path, b"1234567890").unwrap();
        // A 10-byte file allocates at least one block, never 0.
        let used = allocated_bytes(&path).unwrap();
        assert!(used >= 512, "expected a whole block, got {used}");
        assert_eq!(allocated_bytes(&dir.path().join("missing")), None);
    }
}

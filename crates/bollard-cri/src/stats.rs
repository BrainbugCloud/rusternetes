// SPDX-License-Identifier: Apache-2.0

//! Writable-layer disk usage cache for container stats (plan 03 B5).
//!
//! Docker's size inspection (`SizeRw`) walks the container's writable layer
//! and is far too slow for the kubelet's stats cadence, so cri-dockerd
//! caches it and refreshes off the hot path; this is that pattern. A
//! background task lists all CRI containers with sizes on a fixed cadence,
//! backing off exponentially while the daemon is unreachable. Stats reads
//! come from the cache; a container created after the last sweep gets one
//! inline (slow) fetch which also primes its entry.

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use bollard::container::{InspectContainerOptions, ListContainersOptions};
use bollard::Docker;
use cri_server::labels;

/// Cadence of the background size sweep (cri-dockerd refreshes its cache
/// about once a minute).
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Backoff ceiling while the sweep keeps failing.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// One container's cached writable-layer usage, stamped when measured.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WritableLayer {
    pub used_bytes: u64,
    pub timestamp: i64,
}

fn now_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

#[derive(Debug, Default)]
pub(crate) struct DiskUsageCache {
    entries: StdMutex<HashMap<String, WritableLayer>>,
}

impl DiskUsageCache {
    pub(crate) fn get(&self, container_id: &str) -> Option<WritableLayer> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(container_id)
            .copied()
    }

    fn insert(&self, container_id: String, layer: WritableLayer) {
        let _ = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(container_id, layer);
    }

    /// Cache miss path: one inline size inspection, priming the entry so the
    /// next read is cheap again.
    pub(crate) async fn fetch(&self, docker: &Docker, container_id: &str) -> WritableLayer {
        let used_bytes = docker
            .inspect_container(container_id, Some(InspectContainerOptions { size: true }))
            .await
            .ok()
            .and_then(|inspect| inspect.size_rw)
            .unwrap_or_default()
            .max(0) as u64;
        let layer = WritableLayer {
            used_bytes,
            timestamp: now_nanos(),
        };
        self.insert(container_id.to_string(), layer);
        layer
    }

    /// Run the background sweep until the cache is dropped: one sized
    /// container list per interval, replacing the whole map; exponential
    /// backoff while the daemon errors.
    pub(crate) fn spawn_refresh(self: &std::sync::Arc<Self>, docker: Docker) {
        let cache = std::sync::Arc::downgrade(self);
        tokio::spawn(async move {
            let mut backoff = REFRESH_INTERVAL;
            loop {
                let Some(cache) = cache.upgrade() else { return };
                match sweep(&docker).await {
                    Ok(entries) => {
                        *cache.entries.lock().unwrap_or_else(|e| e.into_inner()) = entries;
                        backoff = REFRESH_INTERVAL;
                    }
                    Err(e) => {
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        tracing::debug!("disk usage sweep failed (next in {backoff:?}): {e}");
                    }
                }
                drop(cache);
                tokio::time::sleep(backoff).await;
            }
        });
    }
}

/// One `container list` with sizes, covering every CRI app container.
async fn sweep(docker: &Docker) -> Result<HashMap<String, WritableLayer>, bollard::errors::Error> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    filters.insert(
        "label".to_string(),
        vec![format!(
            "{}={}",
            labels::CONTAINER_TYPE_LABEL,
            labels::CONTAINER_TYPE_CONTAINER
        )],
    );
    let summaries = docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            size: true,
            filters,
            ..Default::default()
        }))
        .await?;
    let timestamp = now_nanos();
    Ok(summaries
        .into_iter()
        .filter_map(|summary| {
            Some((
                summary.id?,
                WritableLayer {
                    used_bytes: summary.size_rw.unwrap_or_default().max(0) as u64,
                    timestamp,
                },
            ))
        })
        .collect())
}

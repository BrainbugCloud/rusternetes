// SPDX-License-Identifier: Apache-2.0

//! Pod sandbox lifecycle.
//!
//! # Why there is no infra ("pause") container
//!
//! On Linux a sandbox is a real container holding the namespaces its siblings
//! join. Apple's runtime gives **each container its own microVM**, and the CLI
//! offers no way to share a network namespace between two of them (`--network`
//! takes a *network* name, never a container) — so an infra container could not
//! hold anything for the others to join. It would only cost a whole VM per pod
//! and, worse, would own an IP address that no app container listens on, so
//! traffic to the "pod IP" would go nowhere.
//!
//! This shim therefore models a sandbox as **a checkpoint record plus a shared
//! Apple network**, and reports the *primary container's* address as the pod IP.
//! A flat network shared by every pod is also the closer match to the
//! Kubernetes network model (every pod routable, no NAT between pods) than a
//! per-pod network would be.
//!
//! # Consequences (documented deviations, see `README.md`)
//!
//! - Containers in a multi-container pod get **distinct IPs and do not share
//!   `localhost`**. Single-container pods — the overwhelming majority — behave
//!   exactly like Linux.
//! - `PodSandboxStatus.network.ip` is empty between `RunPodSandbox` and the
//!   first container start, because no address exists yet.

use std::collections::BTreeMap;

use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::labels;

use crate::backend::AppleBackend;
use crate::naming;
use crate::state::{now_nanos, DnsRecord, PortMappingRecord, SandboxRecord};

impl AppleBackend {
    /// Create the shared pod network if it is not already present.
    pub(crate) async fn ensure_pod_network(&self) -> Result<()> {
        let name = &self.config.pod_network;
        let networks = self.cli.list_networks().await?;
        if networks.iter().any(|n| n.id == *name) {
            return Ok(());
        }
        let labels = BTreeMap::from([(
            "io.kubernetes.cri.managed".to_string(),
            "apple-cri".to_string(),
        )]);
        match self
            .cli
            .create_network(name, self.config.pod_network_subnet.as_deref(), &labels)
            .await
        {
            Ok(()) => {
                tracing::info!(network = %name, "created pod network");
                Ok(())
            }
            // A concurrent RunPodSandbox may have won the race.
            Err(err) => {
                let networks = self.cli.list_networks().await?;
                if networks.iter().any(|n| n.id == *name) {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    pub(crate) async fn run_sandbox(
        &self,
        config: PodSandboxConfig,
        runtime_handler: &str,
    ) -> Result<String> {
        let meta = config
            .metadata
            .clone()
            .ok_or_else(|| Error::InvalidArgument("sandbox config has no metadata".into()))?;

        // Idempotent: a retried RunPodSandbox must not duplicate state. Ids are
        // opaque (see crate::naming), so the match is on the CRI metadata.
        if let Some(existing) = self.store.sandboxes().into_iter().find(|s| {
            s.name == meta.name
                && s.namespace == meta.namespace
                && s.uid == meta.uid
                && s.attempt == meta.attempt
        }) {
            if existing.ready {
                return Ok(existing.id);
            }
        }
        let id = naming::new_id();

        self.ensure_pod_network().await?;

        let dns = config
            .dns_config
            .clone()
            .map(|d| DnsRecord {
                servers: d.servers,
                searches: d.searches,
                options: d.options,
            })
            .unwrap_or_default();

        let hostname = if config.hostname.is_empty() {
            naming::pod_hostname(&meta)
        } else {
            config.hostname.clone()
        };

        let record = SandboxRecord {
            id: id.clone(),
            name: meta.name.clone(),
            namespace: meta.namespace.clone(),
            uid: meta.uid.clone(),
            attempt: meta.attempt,
            labels: config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            annotations: config
                .annotations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            created_at: now_nanos(),
            ready: true,
            log_directory: config.log_directory.clone(),
            hostname,
            network: self.config.pod_network.clone(),
            port_mappings: config
                .port_mappings
                .iter()
                .map(|pm| PortMappingRecord {
                    protocol: pm.protocol,
                    container_port: pm.container_port,
                    host_port: pm.host_port,
                    host_ip: pm.host_ip.clone(),
                })
                .collect(),
            dns,
            runtime_handler: runtime_handler.to_string(),
            ip: String::new(),
        };
        self.store.put_sandbox(record)?;
        tracing::info!(sandbox = %id, pod = %meta.name, namespace = %meta.namespace,
                       "sandbox ready");
        Ok(id)
    }

    /// Stop every container in the sandbox and mark it NOTREADY.
    ///
    /// Idempotent, per CRI: an unknown id is `Ok`.
    pub(crate) async fn stop_sandbox(&self, id: &str) -> Result<()> {
        let Some(_) = self.store.sandbox(id) else {
            return Ok(());
        };
        // Cache the address before the containers go away, so a stopped
        // sandbox can still report the IP it had.
        if let Some(ip) = self.observe_sandbox_ip(id).await {
            let _ = self.store.update_sandbox(id, |s| s.ip = ip);
        }
        for c in self.store.containers_in_sandbox(id) {
            if let Err(err) = self.stop_app_container(&c.id, 0).await {
                tracing::warn!(container = %c.id, %err, "stopping container for sandbox stop");
            }
        }
        self.store.update_sandbox(id, |s| s.ready = false)?;
        Ok(())
    }

    /// Remove the sandbox and every container in it. Idempotent.
    pub(crate) async fn remove_sandbox(&self, id: &str) -> Result<()> {
        if self.store.sandbox(id).is_none() {
            return Ok(());
        }
        for c in self.store.containers_in_sandbox(id) {
            if let Err(err) = self.remove_app_container(&c.id).await {
                tracing::warn!(container = %c.id, %err, "removing container for sandbox remove");
            }
        }
        self.store.delete_sandbox(id)?;
        Ok(())
    }

    /// The pod IP: the primary container's address, as the runtime reports it.
    ///
    /// "Primary" is the earliest-created running container in the sandbox —
    /// a stable choice for a single-container pod, and a documented arbitrary
    /// one for a multi-container pod.
    pub(crate) async fn observe_sandbox_ip(&self, sandbox_id: &str) -> Option<String> {
        let mut containers = self.store.containers_in_sandbox(sandbox_id);
        containers.sort_by_key(|c| (c.created_at, c.id.clone()));
        for c in containers {
            if let Ok(Some(inspect)) = self.cli.inspect_container(&c.id).await {
                if inspect.is_running() {
                    if let Some(ip) = inspect.ipv4() {
                        return Some(ip);
                    }
                }
            }
        }
        None
    }

    pub(crate) async fn sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        let rec = self
            .store
            .sandbox(id)
            .ok_or_else(|| Error::NotFound(format!("sandbox {id}")))?;

        // Prefer a live address; fall back to the last one observed.
        let ip = match self.observe_sandbox_ip(id).await {
            Some(ip) => {
                if ip != rec.ip {
                    let cached = ip.clone();
                    let _ = self.store.update_sandbox(id, |s| s.ip = cached);
                }
                ip
            }
            None => rec.ip.clone(),
        };

        let state = if rec.ready {
            PodSandboxState::SandboxReady
        } else {
            PodSandboxState::SandboxNotready
        };

        Ok(PodSandboxStatus {
            id: rec.id.clone(),
            metadata: Some(rec.metadata()),
            state: state as i32,
            created_at: rec.created_at,
            network: Some(PodSandboxNetworkStatus {
                ip,
                additional_ips: Vec::new(),
            }),
            linux: Some(LinuxPodSandboxStatus {
                namespaces: Some(Namespace {
                    options: Some(NamespaceOption {
                        // Each container is its own VM: the network is the
                        // VM's, and pid/ipc are per-container, never shared
                        // across the pod.
                        network: NamespaceMode::Pod as i32,
                        pid: NamespaceMode::Container as i32,
                        ipc: NamespaceMode::Container as i32,
                        ..Default::default()
                    }),
                }),
            }),
            labels: rec.labels.into_iter().collect(),
            annotations: rec.annotations.into_iter().collect(),
            runtime_handler: rec.runtime_handler,
        })
    }

    pub(crate) async fn list_sandboxes(
        &self,
        filter: Option<PodSandboxFilter>,
    ) -> Result<Vec<PodSandbox>> {
        let mut out = Vec::new();
        for rec in self.store.sandboxes() {
            let state = if rec.ready {
                PodSandboxState::SandboxReady
            } else {
                PodSandboxState::SandboxNotready
            };
            if let Some(f) = &filter {
                if !f.id.is_empty() && f.id != rec.id {
                    continue;
                }
                if let Some(want) = &f.state {
                    if want.state != state as i32 {
                        continue;
                    }
                }
                if !matches_selector(&f.label_selector, &rec.labels) {
                    continue;
                }
            }
            out.push(PodSandbox {
                id: rec.id.clone(),
                metadata: Some(rec.metadata()),
                state: state as i32,
                created_at: rec.created_at,
                labels: rec.labels.clone().into_iter().collect(),
                annotations: rec.annotations.clone().into_iter().collect(),
                runtime_handler: rec.runtime_handler.clone(),
            });
        }
        // Newest first, matching containerd/cri-dockerd list ordering.
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    /// Labels mirrored onto Apple's container objects so an orphan left by a
    /// crashed shim can be identified from `container list` alone.
    pub(crate) fn discovery_labels(
        &self,
        sandbox_id: &str,
        rec: &SandboxRecord,
        container_name: Option<&str>,
    ) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        out.insert(labels::POD_NAME_LABEL.to_string(), rec.name.clone());
        out.insert(
            labels::POD_NAMESPACE_LABEL.to_string(),
            rec.namespace.clone(),
        );
        out.insert(labels::POD_UID_LABEL.to_string(), rec.uid.clone());
        out.insert(labels::SANDBOX_ID_LABEL.to_string(), sandbox_id.to_string());
        match container_name {
            Some(name) => {
                out.insert(labels::CONTAINER_NAME_LABEL.to_string(), name.to_string());
                out.insert(
                    labels::CONTAINER_TYPE_LABEL.to_string(),
                    labels::CONTAINER_TYPE_CONTAINER.to_string(),
                );
            }
            None => {
                out.insert(
                    labels::CONTAINER_TYPE_LABEL.to_string(),
                    labels::CONTAINER_TYPE_SANDBOX.to_string(),
                );
            }
        }
        out
    }
}

/// Whether `labels` contains every key/value in `selector` (CRI label filters
/// are exact-match conjunctions).
pub(crate) fn matches_selector(
    selector: &std::collections::HashMap<String, String>,
    labels: &BTreeMap<String, String>,
) -> bool {
    selector
        .iter()
        .all(|(k, v)| labels.get(k).map(String::as_str) == Some(v.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_is_a_conjunction_of_exact_matches() {
        let labels = BTreeMap::from([
            ("app".to_string(), "web".to_string()),
            ("tier".to_string(), "front".to_string()),
        ]);
        let sel = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<std::collections::HashMap<_, _>>()
        };
        assert!(matches_selector(&sel(&[]), &labels));
        assert!(matches_selector(&sel(&[("app", "web")]), &labels));
        assert!(matches_selector(
            &sel(&[("app", "web"), ("tier", "front")]),
            &labels
        ));
        assert!(!matches_selector(&sel(&[("app", "api")]), &labels));
        // A key the object does not carry fails the whole selector.
        assert!(!matches_selector(
            &sel(&[("app", "web"), ("zone", "a")]),
            &labels
        ));
    }
}

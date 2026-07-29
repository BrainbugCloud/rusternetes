//! CRI client wrapper for the kubelet.
//!
//! Wraps the tonic `RuntimeServiceClient` / `ImageServiceClient` from
//! `cri-proto` behind typed helpers used by `runtime.rs`. Channels are
//! created lazily so the kubelet can start before the runtime socket is
//! available (the channel connects lazily on first use); the first
//! RPC fails with a transport error if the socket never appears.

// K1 scaffolding: call sites land stage by stage (K2-K6); drop this once
// runtime.rs is fully migrated.
#![allow(dead_code)]

use anyhow::{Context, Result};
use cri_proto::v1;
use cri_proto::v1::image_service_client::ImageServiceClient;
use cri_proto::v1::runtime_service_client::RuntimeServiceClient;
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Container state label used by the kubelet in CRI labels.
pub mod labels {
    /// Pod name label (upstream kubelet convention).
    pub const POD_NAME: &str = "io.kubernetes.pod.name";
    /// Pod namespace label.
    pub const POD_NAMESPACE: &str = "io.kubernetes.pod.namespace";
    /// Pod UID label.
    pub const POD_UID: &str = "io.kubernetes.pod.uid";
    /// Container name label.
    pub const CONTAINER_NAME: &str = "io.kubernetes.container.name";
    /// Container type label: "init", "ephemeral", or "regular".
    pub const CONTAINER_TYPE: &str = "io.rusternetes.container.type";
    /// Restart count annotation on containers.
    pub const RESTART_COUNT: &str = "io.rusternetes.container.restart-count";
}

/// Label selector matching every sandbox/container of a pod, keyed by BOTH
/// namespace and name. Both labels are required: pod names are only unique
/// within a namespace, so a name-only selector would also match same-named
/// pods in other namespaces (the cross-namespace identity bug this guards).
fn pod_label_selector(
    namespace: &str,
    pod_name: &str,
) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        (labels::POD_NAMESPACE.to_string(), namespace.to_string()),
        (labels::POD_NAME.to_string(), pod_name.to_string()),
    ])
}

/// Like [`pod_label_selector`] but additionally pins the container name.
fn container_label_selector(
    namespace: &str,
    pod_name: &str,
    container_name: &str,
) -> std::collections::HashMap<String, String> {
    let mut selector = pod_label_selector(namespace, pod_name);
    selector.insert(
        labels::CONTAINER_NAME.to_string(),
        container_name.to_string(),
    );
    selector
}

/// Build a lazily-connecting tonic channel over a unix domain socket.
///
/// Accepts `unix:///path`, `unix:/path`, or a bare filesystem path.
fn lazy_uds_channel(endpoint: &str) -> Channel {
    let path: PathBuf = cri_proto::uds::socket_path(endpoint);
    // The URI is required by tonic but never resolved for UDS transports.
    Endpoint::from_static("http://cri.localhost").connect_with_connector_lazy(service_fn(
        move |_: Uri| {
            let path = path.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?)) }
        },
    ))
}

/// CRI runtime + image service clients.
///
/// Cloning is cheap (tonic channels are reference-counted).
#[derive(Clone)]
pub struct CriClient {
    runtime: RuntimeServiceClient<Channel>,
    image: ImageServiceClient<Channel>,
}

impl CriClient {
    /// Create clients for the given runtime/image endpoints.
    ///
    /// Connections are lazy: this never fails at construction time.
    pub fn new(runtime_endpoint: &str, image_endpoint: &str) -> Self {
        let runtime_channel = lazy_uds_channel(runtime_endpoint);
        let image_channel = if image_endpoint == runtime_endpoint {
            runtime_channel.clone()
        } else {
            lazy_uds_channel(image_endpoint)
        };
        Self {
            runtime: RuntimeServiceClient::new(runtime_channel)
                .max_decoding_message_size(16 * 1024 * 1024),
            image: ImageServiceClient::new(image_channel)
                .max_decoding_message_size(16 * 1024 * 1024),
        }
    }

    fn rt(&self) -> RuntimeServiceClient<Channel> {
        self.runtime.clone()
    }

    fn img(&self) -> ImageServiceClient<Channel> {
        self.image.clone()
    }

    /// Runtime name + version, used for containerID prefixes (`containerd://...`).
    pub async fn version(&self) -> Result<v1::VersionResponse> {
        Ok(self
            .rt()
            .version(v1::VersionRequest::default())
            .await
            .context("CRI Version")?
            .into_inner())
    }

    // --- sandbox ---

    pub async fn run_pod_sandbox(&self, config: v1::PodSandboxConfig) -> Result<String> {
        Ok(self
            .rt()
            .run_pod_sandbox(v1::RunPodSandboxRequest {
                config: Some(config),
                runtime_handler: String::new(),
            })
            .await
            .context("CRI RunPodSandbox")?
            .into_inner()
            .pod_sandbox_id)
    }

    pub async fn stop_pod_sandbox(&self, pod_sandbox_id: &str) -> Result<()> {
        self.rt()
            .stop_pod_sandbox(v1::StopPodSandboxRequest {
                pod_sandbox_id: pod_sandbox_id.to_string(),
            })
            .await
            .context("CRI StopPodSandbox")?;
        Ok(())
    }

    pub async fn remove_pod_sandbox(&self, pod_sandbox_id: &str) -> Result<()> {
        self.rt()
            .remove_pod_sandbox(v1::RemovePodSandboxRequest {
                pod_sandbox_id: pod_sandbox_id.to_string(),
            })
            .await
            .context("CRI RemovePodSandbox")?;
        Ok(())
    }

    pub async fn pod_sandbox_status(&self, pod_sandbox_id: &str) -> Result<v1::PodSandboxStatus> {
        self.rt()
            .pod_sandbox_status(v1::PodSandboxStatusRequest {
                pod_sandbox_id: pod_sandbox_id.to_string(),
                verbose: false,
            })
            .await
            .context("CRI PodSandboxStatus")?
            .into_inner()
            .status
            .context("CRI PodSandboxStatus returned no status")
    }

    pub async fn list_pod_sandbox(
        &self,
        filter: Option<v1::PodSandboxFilter>,
    ) -> Result<Vec<v1::PodSandbox>> {
        Ok(self
            .rt()
            .list_pod_sandbox(v1::ListPodSandboxRequest { filter })
            .await
            .context("CRI ListPodSandbox")?
            .into_inner()
            .items)
    }

    /// List sandboxes carrying the given namespace+pod-name labels (any state).
    ///
    /// Both labels are required: pod names are only unique within a namespace,
    /// so filtering by name alone would match same-named pods in OTHER
    /// namespaces and cause cross-namespace sandbox termination.
    pub async fn sandboxes_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Vec<v1::PodSandbox>> {
        self.list_pod_sandbox(Some(v1::PodSandboxFilter {
            id: String::new(),
            state: None,
            label_selector: pod_label_selector(namespace, pod_name),
        }))
        .await
    }

    // --- containers ---

    pub async fn create_container(
        &self,
        pod_sandbox_id: &str,
        config: v1::ContainerConfig,
        sandbox_config: v1::PodSandboxConfig,
    ) -> Result<String> {
        Ok(self
            .rt()
            .create_container(v1::CreateContainerRequest {
                pod_sandbox_id: pod_sandbox_id.to_string(),
                config: Some(config),
                sandbox_config: Some(sandbox_config),
            })
            .await
            .context("CRI CreateContainer")?
            .into_inner()
            .container_id)
    }

    pub async fn start_container(&self, container_id: &str) -> Result<()> {
        self.rt()
            .start_container(v1::StartContainerRequest {
                container_id: container_id.to_string(),
            })
            .await
            .context("CRI StartContainer")?;
        Ok(())
    }

    pub async fn stop_container(&self, container_id: &str, timeout_secs: i64) -> Result<()> {
        self.rt()
            .stop_container(v1::StopContainerRequest {
                container_id: container_id.to_string(),
                timeout: timeout_secs,
            })
            .await
            .context("CRI StopContainer")?;
        Ok(())
    }

    pub async fn remove_container(&self, container_id: &str) -> Result<()> {
        self.rt()
            .remove_container(v1::RemoveContainerRequest {
                container_id: container_id.to_string(),
            })
            .await
            .context("CRI RemoveContainer")?;
        Ok(())
    }

    pub async fn list_containers(
        &self,
        filter: Option<v1::ContainerFilter>,
    ) -> Result<Vec<v1::Container>> {
        Ok(self
            .rt()
            .list_containers(v1::ListContainersRequest { filter })
            .await
            .context("CRI ListContainers")?
            .into_inner()
            .containers)
    }

    /// List containers carrying the given namespace+pod-name labels (any state).
    ///
    /// Both labels are required — see `sandboxes_for_pod` for why name alone
    /// is unsafe across namespaces.
    pub async fn containers_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Vec<v1::Container>> {
        self.list_containers(Some(v1::ContainerFilter {
            id: String::new(),
            state: None,
            pod_sandbox_id: String::new(),
            label_selector: pod_label_selector(namespace, pod_name),
        }))
        .await
    }

    /// Find one container by namespace + pod-name + container-name labels,
    /// preferring the newest attempt. Namespace is required: pod names are not
    /// globally unique, so a name-only match can return a same-named container
    /// from another namespace.
    pub async fn find_container(
        &self,
        namespace: &str,
        pod_name: &str,
        container_name: &str,
    ) -> Result<Option<v1::Container>> {
        let mut found = self
            .list_containers(Some(v1::ContainerFilter {
                id: String::new(),
                state: None,
                pod_sandbox_id: String::new(),
                label_selector: container_label_selector(namespace, pod_name, container_name),
            }))
            .await?;
        found.sort_by_key(|c| std::cmp::Reverse(c.created_at));
        Ok(found.into_iter().next())
    }

    pub async fn container_status(&self, container_id: &str) -> Result<v1::ContainerStatus> {
        self.rt()
            .container_status(v1::ContainerStatusRequest {
                container_id: container_id.to_string(),
                verbose: false,
            })
            .await
            .context("CRI ContainerStatus")?
            .into_inner()
            .status
            .context("CRI ContainerStatus returned no status")
    }

    pub async fn update_container_resources(
        &self,
        container_id: &str,
        linux: v1::LinuxContainerResources,
    ) -> Result<()> {
        self.rt()
            .update_container_resources(v1::UpdateContainerResourcesRequest {
                container_id: container_id.to_string(),
                linux: Some(linux),
                windows: None,
                annotations: Default::default(),
            })
            .await
            .context("CRI UpdateContainerResources")?;
        Ok(())
    }

    // --- exec / streaming ---

    /// Run a command synchronously in a container; returns (exit_code, stdout, stderr).
    pub async fn exec_sync(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        timeout_secs: i64,
    ) -> Result<(i32, Vec<u8>, Vec<u8>)> {
        let resp = self
            .rt()
            .exec_sync(v1::ExecSyncRequest {
                container_id: container_id.to_string(),
                cmd,
                timeout: timeout_secs,
            })
            .await
            .context("CRI ExecSync")?
            .into_inner();
        Ok((resp.exit_code, resp.stdout, resp.stderr))
    }

    /// Request a streaming exec URL from the runtime.
    pub async fn exec(&self, req: v1::ExecRequest) -> Result<String> {
        Ok(self
            .rt()
            .exec(req)
            .await
            .context("CRI Exec")?
            .into_inner()
            .url)
    }

    /// Request a streaming attach URL from the runtime.
    pub async fn attach(&self, req: v1::AttachRequest) -> Result<String> {
        Ok(self
            .rt()
            .attach(req)
            .await
            .context("CRI Attach")?
            .into_inner()
            .url)
    }

    /// Request a streaming port-forward URL from the runtime.
    pub async fn port_forward(&self, req: v1::PortForwardRequest) -> Result<String> {
        Ok(self
            .rt()
            .port_forward(req)
            .await
            .context("CRI PortForward")?
            .into_inner()
            .url)
    }

    // --- stats ---

    pub async fn list_container_stats(
        &self,
        filter: Option<v1::ContainerStatsFilter>,
    ) -> Result<Vec<v1::ContainerStats>> {
        Ok(self
            .rt()
            .list_container_stats(v1::ListContainerStatsRequest { filter })
            .await
            .context("CRI ListContainerStats")?
            .into_inner()
            .stats)
    }

    pub async fn container_stats(&self, container_id: &str) -> Result<Option<v1::ContainerStats>> {
        Ok(self
            .rt()
            .container_stats(v1::ContainerStatsRequest {
                container_id: container_id.to_string(),
            })
            .await
            .context("CRI ContainerStats")?
            .into_inner()
            .stats)
    }

    pub async fn image_fs_info(&self) -> Result<Vec<v1::FilesystemUsage>> {
        Ok(self
            .img()
            .image_fs_info(v1::ImageFsInfoRequest::default())
            .await
            .context("CRI ImageFsInfo")?
            .into_inner()
            .image_filesystems)
    }

    // --- images ---

    pub async fn image_status(&self, image: &str) -> Result<Option<v1::Image>> {
        Ok(self
            .img()
            .image_status(v1::ImageStatusRequest {
                image: Some(v1::ImageSpec {
                    image: image.to_string(),
                    ..Default::default()
                }),
                verbose: false,
            })
            .await
            .context("CRI ImageStatus")?
            .into_inner()
            .image)
    }

    pub async fn pull_image(&self, image: &str) -> Result<String> {
        Ok(self
            .img()
            .pull_image(v1::PullImageRequest {
                image: Some(v1::ImageSpec {
                    image: image.to_string(),
                    ..Default::default()
                }),
                auth: None,
                sandbox_config: None,
            })
            .await
            .context("CRI PullImage")?
            .into_inner()
            .image_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the cross-namespace pod-identity bug: the pod-scoped
    /// CRI lookups (`sandboxes_for_pod`, `containers_for_pod`) MUST filter on BOTH
    /// the pod-namespace and pod-name labels. Filtering by name alone matched
    /// same-named pods in other namespaces and terminated the wrong pod.
    #[test]
    fn pod_label_selector_includes_namespace_and_name() {
        let selector = pod_label_selector("team-a", "web-0");
        assert_eq!(
            selector.get(labels::POD_NAMESPACE).map(String::as_str),
            Some("team-a")
        );
        assert_eq!(
            selector.get(labels::POD_NAME).map(String::as_str),
            Some("web-0")
        );
        // Exactly the two identity labels — no accidental extra/missing keys.
        assert_eq!(selector.len(), 2);
    }

    /// `find_container` additionally pins the container name, but must still carry
    /// the namespace label so it can never resolve a container of a same-named pod
    /// in another namespace.
    #[test]
    fn container_label_selector_includes_namespace_name_and_container() {
        let selector = container_label_selector("team-a", "web-0", "nginx");
        assert_eq!(
            selector.get(labels::POD_NAMESPACE).map(String::as_str),
            Some("team-a")
        );
        assert_eq!(
            selector.get(labels::POD_NAME).map(String::as_str),
            Some("web-0")
        );
        assert_eq!(
            selector.get(labels::CONTAINER_NAME).map(String::as_str),
            Some("nginx")
        );
        assert_eq!(selector.len(), 3);
    }

    /// Two pods sharing a name across namespaces produce distinct selectors,
    /// so their sandboxes/containers can never be confused.
    #[test]
    fn same_name_different_namespace_selectors_differ() {
        assert_ne!(
            pod_label_selector("team-a", "web-0"),
            pod_label_selector("team-b", "web-0"),
        );
    }
}

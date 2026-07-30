// SPDX-License-Identifier: Apache-2.0

//! `CriService<B>`: tonic `RuntimeService` + `ImageService` over a
//! [`RuntimeBackend`] + [`ImageBackend`].
//!
//! Streaming/metrics RPCs that neither the kubelet nor critest require
//! (`Stream*`, `GetContainerEvents`, `CheckpointContainer`, metrics) answer
//! `Status::unimplemented`.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use cri_proto::v1::image_service_server::{ImageService, ImageServiceServer};
use cri_proto::v1::runtime_service_server::{RuntimeService, RuntimeServiceServer};
use cri_proto::v1::*;

use crate::backend::{ImageBackend, RuntimeBackend};

type ServerStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn required_id(id: String, what: &str) -> Result<String, Status> {
    if id.is_empty() {
        return Err(Status::invalid_argument(format!("{what} is required")));
    }
    Ok(id)
}

fn unimplemented<T>(rpc: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!("{rpc} is not implemented")))
}

/// gRPC facade over a backend. Cheap to clone; register with
/// [`runtime_server`](CriService::runtime_server) and
/// [`image_server`](CriService::image_server), or serve both over a unix
/// socket with [`crate::uds::serve`].
pub struct CriService<B> {
    backend: Arc<B>,
    streaming: Option<Arc<crate::streaming::Streaming>>,
}

impl<B> Clone for CriService<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            streaming: self.streaming.clone(),
        }
    }
}

impl<B> CriService<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            streaming: None,
        }
    }

    /// Attach a streaming registry (see [`crate::streaming::start`]); the
    /// `Exec`/`Attach`/`PortForward` RPCs answer with its URLs. Without one
    /// they answer `Unimplemented`.
    pub fn with_streaming(mut self, streaming: Arc<crate::streaming::Streaming>) -> Self {
        self.streaming = Some(streaming);
        self
    }

    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    fn register_stream(
        &self,
        route: &str,
        spec: crate::streaming::StreamSpec,
    ) -> Result<String, Status> {
        self.streaming
            .as_deref()
            .and_then(|s| s.register(route, spec))
            .ok_or_else(|| Status::unimplemented("no streaming server configured"))
    }
}

impl<B: RuntimeBackend> CriService<B> {
    pub fn runtime_server(&self) -> RuntimeServiceServer<Self> {
        RuntimeServiceServer::new(self.clone())
    }
}

impl<B: ImageBackend> CriService<B> {
    pub fn image_server(&self) -> ImageServiceServer<Self> {
        ImageServiceServer::new(self.clone())
    }
}

#[tonic::async_trait]
impl<B: RuntimeBackend> RuntimeService for CriService<B> {
    async fn version(
        &self,
        _request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(self.backend.version().await?))
    }

    async fn run_pod_sandbox(
        &self,
        request: Request<RunPodSandboxRequest>,
    ) -> Result<Response<RunPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("config is required"))?;
        let pod_sandbox_id = self
            .backend
            .run_pod_sandbox(config, &req.runtime_handler)
            .await?;
        Ok(Response::new(RunPodSandboxResponse { pod_sandbox_id }))
    }

    async fn stop_pod_sandbox(
        &self,
        request: Request<StopPodSandboxRequest>,
    ) -> Result<Response<StopPodSandboxResponse>, Status> {
        let id = required_id(request.into_inner().pod_sandbox_id, "pod_sandbox_id")?;
        self.backend.stop_pod_sandbox(&id).await?;
        Ok(Response::new(StopPodSandboxResponse {}))
    }

    async fn remove_pod_sandbox(
        &self,
        request: Request<RemovePodSandboxRequest>,
    ) -> Result<Response<RemovePodSandboxResponse>, Status> {
        let id = required_id(request.into_inner().pod_sandbox_id, "pod_sandbox_id")?;
        self.backend.remove_pod_sandbox(&id).await?;
        Ok(Response::new(RemovePodSandboxResponse {}))
    }

    async fn pod_sandbox_status(
        &self,
        request: Request<PodSandboxStatusRequest>,
    ) -> Result<Response<PodSandboxStatusResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.pod_sandbox_id, "pod_sandbox_id")?;
        let status = self.backend.pod_sandbox_status(&id).await?;
        Ok(Response::new(PodSandboxStatusResponse {
            status: Some(status),
            ..Default::default()
        }))
    }

    async fn list_pod_sandbox(
        &self,
        request: Request<ListPodSandboxRequest>,
    ) -> Result<Response<ListPodSandboxResponse>, Status> {
        let items = self
            .backend
            .list_pod_sandbox(request.into_inner().filter)
            .await?;
        Ok(Response::new(ListPodSandboxResponse { items }))
    }

    type StreamPodSandboxesStream = ServerStream<StreamPodSandboxesResponse>;
    async fn stream_pod_sandboxes(
        &self,
        _request: Request<StreamPodSandboxesRequest>,
    ) -> Result<Response<Self::StreamPodSandboxesStream>, Status> {
        unimplemented("StreamPodSandboxes")
    }

    async fn create_container(
        &self,
        request: Request<CreateContainerRequest>,
    ) -> Result<Response<CreateContainerResponse>, Status> {
        let req = request.into_inner();
        let sandbox_id = required_id(req.pod_sandbox_id, "pod_sandbox_id")?;
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("config is required"))?;
        let sandbox_config = req
            .sandbox_config
            .ok_or_else(|| Status::invalid_argument("sandbox_config is required"))?;
        let container_id = self
            .backend
            .create_container(&sandbox_id, config, sandbox_config)
            .await?;
        Ok(Response::new(CreateContainerResponse { container_id }))
    }

    async fn start_container(
        &self,
        request: Request<StartContainerRequest>,
    ) -> Result<Response<StartContainerResponse>, Status> {
        let id = required_id(request.into_inner().container_id, "container_id")?;
        self.backend.start_container(&id).await?;
        Ok(Response::new(StartContainerResponse {}))
    }

    async fn stop_container(
        &self,
        request: Request<StopContainerRequest>,
    ) -> Result<Response<StopContainerResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        self.backend.stop_container(&id, req.timeout).await?;
        Ok(Response::new(StopContainerResponse {}))
    }

    async fn remove_container(
        &self,
        request: Request<RemoveContainerRequest>,
    ) -> Result<Response<RemoveContainerResponse>, Status> {
        let id = required_id(request.into_inner().container_id, "container_id")?;
        self.backend.remove_container(&id).await?;
        Ok(Response::new(RemoveContainerResponse {}))
    }

    async fn list_containers(
        &self,
        request: Request<ListContainersRequest>,
    ) -> Result<Response<ListContainersResponse>, Status> {
        let containers = self
            .backend
            .list_containers(request.into_inner().filter)
            .await?;
        Ok(Response::new(ListContainersResponse { containers }))
    }

    type StreamContainersStream = ServerStream<StreamContainersResponse>;
    async fn stream_containers(
        &self,
        _request: Request<StreamContainersRequest>,
    ) -> Result<Response<Self::StreamContainersStream>, Status> {
        unimplemented("StreamContainers")
    }

    async fn container_status(
        &self,
        request: Request<ContainerStatusRequest>,
    ) -> Result<Response<ContainerStatusResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        let status = self.backend.container_status(&id).await?;
        Ok(Response::new(ContainerStatusResponse {
            status: Some(status),
            ..Default::default()
        }))
    }

    async fn update_container_resources(
        &self,
        request: Request<UpdateContainerResourcesRequest>,
    ) -> Result<Response<UpdateContainerResourcesResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        self.backend
            .update_container_resources(&id, req.linux.unwrap_or_default())
            .await?;
        Ok(Response::new(UpdateContainerResourcesResponse {}))
    }

    async fn reopen_container_log(
        &self,
        request: Request<ReopenContainerLogRequest>,
    ) -> Result<Response<ReopenContainerLogResponse>, Status> {
        let id = required_id(request.into_inner().container_id, "container_id")?;
        self.backend.reopen_container_log(&id).await?;
        Ok(Response::new(ReopenContainerLogResponse {}))
    }

    async fn exec_sync(
        &self,
        request: Request<ExecSyncRequest>,
    ) -> Result<Response<ExecSyncResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        if req.cmd.is_empty() {
            return Err(Status::invalid_argument("cmd is required"));
        }
        let result = self.backend.exec_sync(&id, &req.cmd, req.timeout).await?;
        Ok(Response::new(ExecSyncResponse {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
        }))
    }

    async fn exec(&self, request: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        if req.cmd.is_empty() {
            return Err(Status::invalid_argument("cmd is required"));
        }
        if !req.stdout && !req.stderr && !req.stdin {
            return Err(Status::invalid_argument(
                "one of stdin, stdout, or stderr must be set",
            ));
        }
        if req.tty && req.stderr {
            return Err(Status::invalid_argument(
                "tty and stderr are mutually exclusive",
            ));
        }
        let status = self.backend.container_status(&id).await?;
        if status.state != ContainerState::ContainerRunning as i32 {
            return Err(Status::failed_precondition(format!(
                "container {id} is not running"
            )));
        }
        let url = self.register_stream(
            "exec",
            crate::streaming::StreamSpec::Exec {
                container_id: id,
                cmd: req.cmd,
                tty: req.tty,
                stdin: req.stdin,
                stdout: req.stdout,
                stderr: req.stderr,
            },
        )?;
        Ok(Response::new(ExecResponse { url }))
    }

    async fn attach(
        &self,
        request: Request<AttachRequest>,
    ) -> Result<Response<AttachResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.container_id, "container_id")?;
        let status = self.backend.container_status(&id).await?;
        if status.state != ContainerState::ContainerRunning as i32 {
            return Err(Status::failed_precondition(format!(
                "container {id} is not running"
            )));
        }
        let url = self.register_stream(
            "attach",
            crate::streaming::StreamSpec::Attach {
                container_id: id,
                tty: req.tty,
                stdin: req.stdin,
                stdout: req.stdout,
                stderr: req.stderr,
            },
        )?;
        Ok(Response::new(AttachResponse { url }))
    }

    async fn port_forward(
        &self,
        request: Request<PortForwardRequest>,
    ) -> Result<Response<PortForwardResponse>, Status> {
        let req = request.into_inner();
        let id = required_id(req.pod_sandbox_id, "pod_sandbox_id")?;
        let _ = self.backend.pod_sandbox_status(&id).await?;
        let url = self.register_stream(
            "portforward",
            crate::streaming::StreamSpec::PortForward { pod_sandbox_id: id },
        )?;
        Ok(Response::new(PortForwardResponse { url }))
    }

    async fn container_stats(
        &self,
        request: Request<ContainerStatsRequest>,
    ) -> Result<Response<ContainerStatsResponse>, Status> {
        let id = required_id(request.into_inner().container_id, "container_id")?;
        let stats = self.backend.container_stats(&id).await?;
        Ok(Response::new(ContainerStatsResponse { stats: Some(stats) }))
    }

    async fn list_container_stats(
        &self,
        request: Request<ListContainerStatsRequest>,
    ) -> Result<Response<ListContainerStatsResponse>, Status> {
        let stats = self
            .backend
            .list_container_stats(request.into_inner().filter)
            .await?;
        Ok(Response::new(ListContainerStatsResponse { stats }))
    }

    type StreamContainerStatsStream = ServerStream<StreamContainerStatsResponse>;
    async fn stream_container_stats(
        &self,
        _request: Request<StreamContainerStatsRequest>,
    ) -> Result<Response<Self::StreamContainerStatsStream>, Status> {
        unimplemented("StreamContainerStats")
    }

    async fn pod_sandbox_stats(
        &self,
        _request: Request<PodSandboxStatsRequest>,
    ) -> Result<Response<PodSandboxStatsResponse>, Status> {
        unimplemented("PodSandboxStats")
    }

    async fn list_pod_sandbox_stats(
        &self,
        _request: Request<ListPodSandboxStatsRequest>,
    ) -> Result<Response<ListPodSandboxStatsResponse>, Status> {
        unimplemented("ListPodSandboxStats")
    }

    type StreamPodSandboxStatsStream = ServerStream<StreamPodSandboxStatsResponse>;
    async fn stream_pod_sandbox_stats(
        &self,
        _request: Request<StreamPodSandboxStatsRequest>,
    ) -> Result<Response<Self::StreamPodSandboxStatsStream>, Status> {
        unimplemented("StreamPodSandboxStats")
    }

    async fn update_runtime_config(
        &self,
        request: Request<UpdateRuntimeConfigRequest>,
    ) -> Result<Response<UpdateRuntimeConfigResponse>, Status> {
        let pod_cidr = request
            .into_inner()
            .runtime_config
            .and_then(|c| c.network_config)
            .map(|n| n.pod_cidr)
            .filter(|cidr| !cidr.is_empty());
        self.backend.update_runtime_config(pod_cidr).await?;
        Ok(Response::new(UpdateRuntimeConfigResponse {}))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let status = self.backend.status().await?;
        Ok(Response::new(StatusResponse {
            status: Some(status),
            ..Default::default()
        }))
    }

    async fn checkpoint_container(
        &self,
        _request: Request<CheckpointContainerRequest>,
    ) -> Result<Response<CheckpointContainerResponse>, Status> {
        unimplemented("CheckpointContainer")
    }

    type GetContainerEventsStream = ServerStream<ContainerEventResponse>;
    async fn get_container_events(
        &self,
        _request: Request<GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        // The kubelet falls back to polling when events are unavailable.
        unimplemented("GetContainerEvents")
    }

    async fn list_metric_descriptors(
        &self,
        _request: Request<ListMetricDescriptorsRequest>,
    ) -> Result<Response<ListMetricDescriptorsResponse>, Status> {
        unimplemented("ListMetricDescriptors")
    }

    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<ListPodSandboxMetricsRequest>,
    ) -> Result<Response<ListPodSandboxMetricsResponse>, Status> {
        unimplemented("ListPodSandboxMetrics")
    }

    type StreamPodSandboxMetricsStream = ServerStream<StreamPodSandboxMetricsResponse>;
    async fn stream_pod_sandbox_metrics(
        &self,
        _request: Request<StreamPodSandboxMetricsRequest>,
    ) -> Result<Response<Self::StreamPodSandboxMetricsStream>, Status> {
        unimplemented("StreamPodSandboxMetrics")
    }

    async fn runtime_config(
        &self,
        _request: Request<RuntimeConfigRequest>,
    ) -> Result<Response<RuntimeConfigResponse>, Status> {
        let resp = self.backend.runtime_config().await?;
        Ok(Response::new(resp))
    }

    async fn update_pod_sandbox_resources(
        &self,
        _request: Request<UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<UpdatePodSandboxResourcesResponse>, Status> {
        unimplemented("UpdatePodSandboxResources")
    }
}

#[tonic::async_trait]
impl<B: ImageBackend> ImageService for CriService<B> {
    async fn list_images(
        &self,
        request: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let images = self
            .backend
            .list_images(request.into_inner().filter)
            .await?;
        Ok(Response::new(ListImagesResponse { images }))
    }

    type StreamImagesStream = ServerStream<StreamImagesResponse>;
    async fn stream_images(
        &self,
        _request: Request<StreamImagesRequest>,
    ) -> Result<Response<Self::StreamImagesStream>, Status> {
        unimplemented("StreamImages")
    }

    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let image = request
            .into_inner()
            .image
            .ok_or_else(|| Status::invalid_argument("image is required"))?;
        let image = self.backend.image_status(&image).await?;
        Ok(Response::new(ImageStatusResponse {
            image,
            ..Default::default()
        }))
    }

    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let req = request.into_inner();
        let image = req
            .image
            .ok_or_else(|| Status::invalid_argument("image is required"))?;
        let image_ref = self
            .backend
            .pull_image(&image, req.auth, req.sandbox_config)
            .await?;
        Ok(Response::new(PullImageResponse { image_ref }))
    }

    async fn remove_image(
        &self,
        request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        let image = request
            .into_inner()
            .image
            .ok_or_else(|| Status::invalid_argument("image is required"))?;
        self.backend.remove_image(&image).await?;
        Ok(Response::new(RemoveImageResponse {}))
    }

    async fn image_fs_info(
        &self,
        _request: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        let image_filesystems = self.backend.image_fs_info().await?;
        Ok(Response::new(ImageFsInfoResponse {
            image_filesystems,
            ..Default::default()
        }))
    }
}

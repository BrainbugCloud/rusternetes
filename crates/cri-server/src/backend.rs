// SPDX-License-Identifier: Apache-2.0

//! The seam every CRI runtime backend implements.
//!
//! Backends implement these two narrow traits instead of the ~40 tonic RPCs;
//! [`CriService`](crate::service::CriService) supplies the gRPC plumbing,
//! request validation, and `Status::unimplemented` defaults for the streaming
//! and metrics RPCs that neither the kubelet nor critest require.
//!
//! Types are the `cri-proto` generated types — no parallel type universe.

use async_trait::async_trait;
use cri_proto::v1::{
    AuthConfig, Container, ContainerConfig, ContainerFilter, ContainerStats, ContainerStatsFilter,
    ContainerStatus, FilesystemUsage, Image, ImageFilter, ImageSpec, LinuxContainerResources,
    PodSandbox, PodSandboxConfig, PodSandboxFilter, PodSandboxStatus, RuntimeConfigResponse,
    RuntimeStatus, VersionResponse,
};

use crate::error::Result;

/// Result of a buffered [`RuntimeBackend::exec_sync`] call.
#[derive(Debug, Clone, Default)]
pub struct ExecSyncResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

#[async_trait]
pub trait RuntimeBackend: Send + Sync + 'static {
    // -- sandbox lifecycle -------------------------------------------------
    /// Create and start a pod sandbox; returns the sandbox id.
    async fn run_pod_sandbox(
        &self,
        config: PodSandboxConfig,
        runtime_handler: &str,
    ) -> Result<String>;
    /// Idempotent: an unknown id returns `Ok`.
    async fn stop_pod_sandbox(&self, id: &str) -> Result<()>;
    /// Idempotent: an unknown id returns `Ok`.
    async fn remove_pod_sandbox(&self, id: &str) -> Result<()>;
    /// Unknown id returns [`Error::NotFound`](crate::error::Error::NotFound).
    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus>;
    async fn list_pod_sandbox(&self, filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>>;

    // -- container lifecycle -----------------------------------------------
    /// Create a container in a sandbox; returns the container id.
    async fn create_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String>;
    async fn start_container(&self, id: &str) -> Result<()>;
    /// Idempotent: an unknown id returns `Ok`.
    async fn stop_container(&self, id: &str, timeout_secs: i64) -> Result<()>;
    /// Idempotent: an unknown id returns `Ok`.
    async fn remove_container(&self, id: &str) -> Result<()>;
    async fn list_containers(&self, filter: Option<ContainerFilter>) -> Result<Vec<Container>>;
    /// Unknown id returns [`Error::NotFound`](crate::error::Error::NotFound).
    async fn container_status(&self, id: &str) -> Result<ContainerStatus>;
    async fn update_container_resources(
        &self,
        id: &str,
        resources: LinuxContainerResources,
    ) -> Result<()>;

    // -- exec --------------------------------------------------------------
    /// Run a command in the container and buffer its output.
    async fn exec_sync(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult>;

    // -- stats & info --------------------------------------------------------
    async fn container_stats(&self, id: &str) -> Result<ContainerStats>;
    async fn list_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>>;
    /// RuntimeReady / NetworkReady conditions.
    async fn status(&self) -> Result<RuntimeStatus>;
    async fn version(&self) -> Result<VersionResponse>;
    async fn update_runtime_config(&self, pod_cidr: Option<String>) -> Result<()>;
    async fn runtime_config(&self) -> Result<RuntimeConfigResponse>;
    async fn reopen_container_log(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ImageBackend: Send + Sync + 'static {
    async fn list_images(&self, filter: Option<ImageFilter>) -> Result<Vec<Image>>;
    /// `Ok(None)` when the image is not present (CRI: empty response, not an error).
    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>>;
    /// Pull an image; returns the image ref (digest or canonical name).
    async fn pull_image(
        &self,
        image: &ImageSpec,
        auth: Option<AuthConfig>,
        sandbox_config: Option<PodSandboxConfig>,
    ) -> Result<String>;
    /// Idempotent: removing an absent image returns `Ok`.
    async fn remove_image(&self, image: &ImageSpec) -> Result<()>;
    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>>;
}

// SPDX-License-Identifier: Apache-2.0

//! Full sandbox + container lifecycle against `CriService<MemoryBackend>`
//! over a real unix socket, exercised through the `cri-proto` client —
//! the same wire path crictl and the kubelet use.

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::sync::Arc;

use cri_proto::uds::connect_uds;
use cri_proto::v1::image_service_client::ImageServiceClient;
use cri_proto::v1::runtime_service_client::RuntimeServiceClient;
use cri_proto::v1::*;
use cri_server::testing::MemoryBackend;
use cri_server::CriService;
use tonic::transport::Channel;
use tonic::Code;

struct TestServer {
    endpoint: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = format!("unix://{}", dir.path().join("cri.sock").display());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let service = CriService::new(Arc::new(MemoryBackend::new()));
        let ep = endpoint.clone();
        tokio::spawn(async move {
            cri_server::uds::serve(&ep, service, async {
                let _ = rx.await;
            })
            .await
            .expect("serve");
        });
        // Wait for the socket to accept connections.
        let path = cri_proto::uds::socket_path(&endpoint);
        for _ in 0..100 {
            if tokio::net::UnixStream::connect(&path).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        TestServer {
            endpoint,
            shutdown: Some(tx),
            _dir: dir,
        }
    }

    async fn channel(&self) -> Channel {
        connect_uds(&self.endpoint).await.expect("connect")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

fn sandbox_config(name: &str, log_dir: &str) -> PodSandboxConfig {
    PodSandboxConfig {
        metadata: Some(PodSandboxMetadata {
            name: name.to_string(),
            namespace: "default".to_string(),
            uid: format!("uid-{name}"),
            attempt: 0,
        }),
        labels: HashMap::from([("app".to_string(), name.to_string())]),
        log_directory: log_dir.to_string(),
        ..Default::default()
    }
}

fn container_config(name: &str, image: &str) -> ContainerConfig {
    ContainerConfig {
        metadata: Some(ContainerMetadata {
            name: name.to_string(),
            attempt: 0,
        }),
        image: Some(ImageSpec {
            image: image.to_string(),
            ..Default::default()
        }),
        log_path: format!("{name}_0.log"),
        ..Default::default()
    }
}

#[tokio::test]
async fn full_lifecycle_over_uds() {
    let server = TestServer::start().await;
    let logs = tempfile::tempdir().unwrap();
    let mut runtime = RuntimeServiceClient::new(server.channel().await);
    let mut images = ImageServiceClient::new(server.channel().await);

    // Version + Status
    let version = runtime
        .version(VersionRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(version.runtime_name, "memory-cri");
    assert_eq!(version.runtime_api_version, "v1");
    let status = runtime
        .status(StatusRequest::default())
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert!(status.conditions.iter().all(|c| c.status));

    // Pull the image the container will use.
    let image_ref = images
        .pull_image(PullImageRequest {
            image: Some(ImageSpec {
                image: "busybox".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .image_ref;
    assert!(image_ref.starts_with("sha256:"));
    let listed = images
        .list_images(ListImagesRequest::default())
        .await
        .unwrap()
        .into_inner()
        .images;
    assert_eq!(listed.len(), 1);
    assert!(listed[0].repo_tags.contains(&"busybox:latest".to_string()));

    // Sandbox up
    let log_dir = logs.path().to_string_lossy().into_owned();
    let sandbox_id = runtime
        .run_pod_sandbox(RunPodSandboxRequest {
            config: Some(sandbox_config("web", &log_dir)),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .pod_sandbox_id;

    let sandbox = runtime
        .pod_sandbox_status(PodSandboxStatusRequest {
            pod_sandbox_id: sandbox_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(sandbox.state, PodSandboxState::SandboxReady as i32);
    assert!(!sandbox.network.unwrap().ip.is_empty());

    // Container lifecycle: create → start → status(Running) → stop → status(Exited)
    let container_id = runtime
        .create_container(CreateContainerRequest {
            pod_sandbox_id: sandbox_id.clone(),
            config: Some(container_config("app", "busybox")),
            sandbox_config: Some(sandbox_config("web", &log_dir)),
        })
        .await
        .unwrap()
        .into_inner()
        .container_id;

    runtime
        .start_container(StartContainerRequest {
            container_id: container_id.clone(),
        })
        .await
        .unwrap();

    let running = runtime
        .container_status(ContainerStatusRequest {
            container_id: container_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(running.state, ContainerState::ContainerRunning as i32);
    assert!(running.started_at > 0);
    assert!(std::path::Path::new(&running.log_path).exists());

    // ExecSync through the scripted shell.
    let exec = runtime
        .exec_sync(ExecSyncRequest {
            container_id: container_id.clone(),
            cmd: vec!["echo".to_string(), "hello".to_string()],
            timeout: 5,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exec.exit_code, 0);
    assert_eq!(exec.stdout, b"hello\n");

    // List with filters.
    let filtered = runtime
        .list_containers(ListContainersRequest {
            filter: Some(ContainerFilter {
                pod_sandbox_id: sandbox_id.clone(),
                state: Some(ContainerStateValue {
                    state: ContainerState::ContainerRunning as i32,
                }),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner()
        .containers;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, container_id);

    runtime
        .stop_container(StopContainerRequest {
            container_id: container_id.clone(),
            timeout: 5,
        })
        .await
        .unwrap();
    let exited = runtime
        .container_status(ContainerStatusRequest {
            container_id: container_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(exited.state, ContainerState::ContainerExited as i32);
    assert_eq!(exited.exit_code, 0);
    assert_eq!(exited.reason, "Completed");

    // Removing a ready sandbox is refused; stop then remove cascades.
    let err = runtime
        .remove_pod_sandbox(RemovePodSandboxRequest {
            pod_sandbox_id: sandbox_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    runtime
        .stop_pod_sandbox(StopPodSandboxRequest {
            pod_sandbox_id: sandbox_id.clone(),
        })
        .await
        .unwrap();
    runtime
        .remove_pod_sandbox(RemovePodSandboxRequest {
            pod_sandbox_id: sandbox_id.clone(),
        })
        .await
        .unwrap();
    assert!(runtime
        .list_containers(ListContainersRequest::default())
        .await
        .unwrap()
        .into_inner()
        .containers
        .is_empty());
}

#[tokio::test]
async fn idempotency_and_not_found_semantics() {
    let server = TestServer::start().await;
    let mut runtime = RuntimeServiceClient::new(server.channel().await);

    // stop/remove of unknown ids → OK (CRI idempotency, asserted by critest)
    runtime
        .stop_pod_sandbox(StopPodSandboxRequest {
            pod_sandbox_id: "no-such-sandbox".to_string(),
        })
        .await
        .unwrap();
    runtime
        .remove_pod_sandbox(RemovePodSandboxRequest {
            pod_sandbox_id: "no-such-sandbox".to_string(),
        })
        .await
        .unwrap();
    runtime
        .stop_container(StopContainerRequest {
            container_id: "no-such-container".to_string(),
            timeout: 0,
        })
        .await
        .unwrap();
    runtime
        .remove_container(RemoveContainerRequest {
            container_id: "no-such-container".to_string(),
        })
        .await
        .unwrap();

    // status of unknown ids → NotFound
    let err = runtime
        .pod_sandbox_status(PodSandboxStatusRequest {
            pod_sandbox_id: "no-such-sandbox".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let err = runtime
        .container_status(ContainerStatusRequest {
            container_id: "no-such-container".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // empty ids → InvalidArgument
    let err = runtime
        .pod_sandbox_status(PodSandboxStatusRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // unimplemented RPCs answer Unimplemented, not a crash
    let err = runtime
        .checkpoint_container(CheckpointContainerRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn image_lifecycle_and_idempotent_remove() {
    let server = TestServer::start().await;
    let mut images = ImageServiceClient::new(server.channel().await);

    // Absent image: empty status, not an error.
    let status = images
        .image_status(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "busybox".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(status.image.is_none());

    images
        .pull_image(PullImageRequest {
            image: Some(ImageSpec {
                image: "busybox".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    let status = images
        .image_status(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "busybox".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(status.image.is_some());

    let fs = images
        .image_fs_info(ImageFsInfoRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(!fs.image_filesystems.is_empty());

    // Remove twice: idempotent.
    for _ in 0..2 {
        images
            .remove_image(RemoveImageRequest {
                image: Some(ImageSpec {
                    image: "busybox".to_string(),
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
    }
}

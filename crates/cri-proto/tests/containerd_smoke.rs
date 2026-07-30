// SPDX-License-Identifier: Apache-2.0

//! Integration smoke test against a real containerd (plan/01-cri-crates.md, S1).
//!
//! Ignored by default; run inside a Linux VM with containerd:
//! `cargo test -p cri-proto -- --ignored`.
//!
//! Override the socket with `CONTAINERD_SOCK` (default
//! `/run/containerd/containerd.sock`).

use cri_proto::uds::connect_uds;
use cri_proto::v1::image_service_client::ImageServiceClient;
use cri_proto::v1::runtime_service_client::RuntimeServiceClient;
use cri_proto::v1::{ListImagesRequest, ListPodSandboxRequest, VersionRequest};

fn containerd_sock() -> String {
    std::env::var("CONTAINERD_SOCK")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string())
}

#[tokio::test]
#[ignore = "requires a running containerd (run in the lima VM)"]
async fn version_reports_containerd() {
    let channel = connect_uds(containerd_sock()).await.expect("connect");
    let mut client = RuntimeServiceClient::new(channel);
    let resp = client
        .version(VersionRequest::default())
        .await
        .expect("Version RPC")
        .into_inner();
    assert_eq!(resp.runtime_name, "containerd");
    assert!(!resp.runtime_api_version.is_empty());
}

#[tokio::test]
#[ignore = "requires a running containerd (run in the lima VM)"]
async fn list_pod_sandbox_succeeds() {
    let channel = connect_uds(containerd_sock()).await.expect("connect");
    let mut client = RuntimeServiceClient::new(channel);
    client
        .list_pod_sandbox(ListPodSandboxRequest::default())
        .await
        .expect("ListPodSandbox RPC");
}

#[tokio::test]
#[ignore = "requires a running containerd (run in the lima VM)"]
async fn list_images_succeeds() {
    let channel = connect_uds(containerd_sock()).await.expect("connect");
    let mut client = ImageServiceClient::new(channel);
    client
        .list_images(ListImagesRequest::default())
        .await
        .expect("ListImages RPC");
}

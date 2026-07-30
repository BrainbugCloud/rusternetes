// SPDX-License-Identifier: Apache-2.0

//! `bollard-cri`: a CRI server backed by the Docker Engine API through
//! bollard — the cri-dockerd shape in Rust (see `plan/03-bollard-cri.md`).

mod backend;
mod container;
mod images;
mod logs;
mod naming;
mod sandbox;
mod stats;
mod streaming;

use std::sync::Arc;

use clap::Parser;

use backend::BollardBackend;
use cri_server::CriService;

#[derive(Parser, Debug)]
#[command(name = "bollard-cri", version, about)]
struct Args {
    /// CRI unix socket to serve (RuntimeService + ImageService).
    #[arg(long, default_value = "unix:///var/run/bollard-cri.sock")]
    cri_listen: String,

    /// Docker Engine API socket (dockerd or podman).
    #[arg(long, default_value = "unix:///var/run/docker.sock")]
    docker_host: String,

    /// Pod infra ("pause") container image.
    #[arg(long, default_value = "registry.k8s.io/pause:3.10")]
    pod_infra_container_image: String,

    /// State directory (checkpoints).
    #[arg(long, default_value = "/var/lib/bollard-cri")]
    root_dir: String,

    /// Bind address for the exec/attach/portforward streaming server.
    #[arg(long, default_value = "127.0.0.1:0")]
    streaming_bind: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let docker = backend::connect_docker(&args.docker_host)?;
    let backend = Arc::new(BollardBackend::new(
        docker,
        backend::Config {
            pod_infra_container_image: args.pod_infra_container_image,
            root_dir: args.root_dir.into(),
        },
    )?);

    match backend.docker_version_banner().await {
        Ok(banner) => tracing::info!(docker = %banner, "connected to Docker Engine API"),
        Err(e) => tracing::warn!("Docker Engine API not reachable yet: {e}"),
    }

    // Log relays don't survive a shim restart; resume them for running
    // containers from each CRI log file's last record.
    backend.resume_log_relays().await;

    // Writable-layer sizes for container stats are refreshed off the hot
    // path (Docker's size inspection is slow — cri-dockerd caches too).
    backend.spawn_disk_usage_refresh();

    let streaming = cri_server::streaming::start(&args.streaming_bind, backend.clone()).await?;
    let service = CriService::new(backend).with_streaming(streaming);

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    };
    let result = cri_server::uds::serve(&args.cri_listen, service, shutdown).await;
    cri_server::uds::cleanup(&args.cri_listen);
    result
}

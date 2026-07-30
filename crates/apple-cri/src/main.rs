// SPDX-License-Identifier: Apache-2.0

//! `apple-cri`: a CRI server backed by Apple's `container` runtime, so a
//! kubelet can run Linux workloads natively on macOS — no Docker Desktop, no
//! Lima VM, no Linux node.
//!
//! Structural note: Apple runs **one microVM per container** while Kubernetes
//! expects the containers of a pod to share a network (and optionally IPC/PID)
//! namespace. That mismatch, and the deviations it forces, are documented in
//! [`sandbox`] and in the crate README.

mod backend;
mod cli;
mod container;
mod images;
mod logs;
mod model;
mod naming;
mod sandbox;
mod state;
mod stats;
mod streaming;

use std::sync::Arc;

use backend::{AppleBackend, Config};
use clap::Parser;
use cri_server::CriService;

#[derive(Parser, Debug)]
#[command(name = "apple-cri", version, about)]
struct Args {
    /// CRI unix socket to serve (RuntimeService + ImageService).
    #[arg(long, default_value = "unix:///var/run/apple-cri.sock")]
    cri_listen: String,

    /// Path to Apple's `container` binary.
    #[arg(long, default_value = "container", env = "CONTAINER_BINARY")]
    container_binary: String,

    /// Apple network that pod containers attach to.
    #[arg(long, default_value = "k8s-pods")]
    pod_network: String,

    /// Subnet for the pod network (e.g. `10.244.0.0/16`); Apple chooses when
    /// unset.
    #[arg(long)]
    pod_network_subnet: Option<String>,

    /// Guest architecture for pulled images.
    #[arg(long, default_value = default_arch())]
    arch: String,

    /// State directory (checkpoints).
    #[arg(long, default_value = "/var/lib/apple-cri")]
    root_dir: String,

    /// Bind address for the exec/attach/portforward streaming server.
    #[arg(long, default_value = "127.0.0.1:0")]
    streaming_bind: String,

    /// Run `container system start` before serving.
    #[arg(long)]
    start_runtime: bool,
}

/// The host architecture, in the naming Apple's `--arch` uses.
fn default_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "amd64"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let backend = Arc::new(AppleBackend::new(Config {
        binary: args.container_binary,
        pod_network: args.pod_network,
        pod_network_subnet: args.pod_network_subnet,
        arch: args.arch,
        root_dir: args.root_dir.into(),
    })?);

    if args.start_runtime {
        if let Err(err) = backend.cli_system_start().await {
            tracing::warn!(%err, "`container system start` failed");
        }
    }

    match backend.runtime_banner().await {
        Ok(banner) => tracing::info!(runtime = %banner, "connected to Apple container runtime"),
        Err(e) => tracing::warn!(
            "Apple container runtime not reachable yet: {e} \
             (is `container system start` done?)"
        ),
    }

    // The pod network must exist before the kubelet reports NetworkReady.
    if let Err(err) = backend.ensure_pod_network_public().await {
        tracing::warn!(%err, "cannot create the pod network yet");
    }

    // Re-adopt containers that outlived a previous shim process.
    backend.reconcile().await;

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

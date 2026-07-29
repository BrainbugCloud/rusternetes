#[allow(dead_code)]
pub mod cni;
pub mod config;
pub mod cri;
#[allow(dead_code)]
pub mod eviction;
pub mod kubelet;
pub mod runtime;
pub mod server;
pub mod streaming_server;

pub use kubelet::PodWorkerState;

use config::KubeletConfiguration;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use tracing::info;

/// Configuration for the kubelet component.
pub struct KubeletConfig {
    pub node_name: String,
    pub volume_dir: String,
    pub cluster_dns: String,
    pub cluster_domain: String,
    pub network: String,
    pub sync_interval: u64,
    pub metrics_port: u16,
    /// Port for the exec/attach/portForward SPDY streaming server. Advertised
    /// in the node's `DaemonEndpoints`; also serves as the front door that
    /// proxies plain HTTP requests (e.g. `/containerLogs`) to `metrics_port`.
    pub streaming_port: u16,
    pub kubernetes_service_host: String,
    pub container_runtime_endpoint: String,
    pub image_service_endpoint: String,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: "node-1".to_string(),
            volume_dir: "./volumes".to_string(),
            cluster_dns: "10.96.0.10".to_string(),
            cluster_domain: "cluster.local".to_string(),
            network: "rusternetes-network".to_string(),
            sync_interval: 3,
            metrics_port: 10250,
            streaming_port: 10251,
            kubernetes_service_host: "127.0.0.1".to_string(),
            container_runtime_endpoint: config::DEFAULT_CONTAINER_RUNTIME_ENDPOINT.to_string(),
            image_service_endpoint: config::DEFAULT_CONTAINER_RUNTIME_ENDPOINT.to_string(),
        }
    }
}

/// Run the kubelet component.
///
/// This is the main entry point for embedding the kubelet in the all-in-one binary.
/// Starts the kubelet sync loop and metrics server, blocks until shutdown.
pub async fn run(storage: Arc<StorageBackend>, config: KubeletConfig) -> anyhow::Result<()> {
    info!(
        "Starting Rusternetes Kubelet for node: {}",
        config.node_name
    );

    // Discover cluster DNS if not hardcoded
    let cluster_dns = {
        use rusternetes_common::resources::Service;
        match storage
            .get::<Service>("/registry/services/kube-system/kube-dns")
            .await
        {
            Ok(service) => {
                if let Some(ref cluster_ip) = service.spec.cluster_ip {
                    info!("Discovered cluster DNS IP: {}", cluster_ip);
                    cluster_ip.clone()
                } else {
                    config.cluster_dns.clone()
                }
            }
            Err(_) => config.cluster_dns.clone(),
        }
    };

    // Metrics server
    let metrics =
        Arc::new(rusternetes_common::observability::MetricsRegistry::new().with_kubelet_metrics()?);
    let metrics_clone = metrics.clone();

    let kubelet_config = KubeletConfiguration {
        api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
        kind: "KubeletConfiguration".to_string(),
        root_dir: None,
        volume_dir: Some(config.volume_dir.clone()),
        volume_plugin_dir: None,
        sync_frequency: Some(config.sync_interval),
        metrics_bind_port: Some(config.metrics_port),
        log_level: Some("info".to_string()),
        cluster_service_cidr: None,
        container_runtime_endpoint: Some(config.container_runtime_endpoint.clone()),
        image_service_endpoint: Some(config.image_service_endpoint.clone()),
    };
    let kubelet_config = Arc::new(kubelet_config);
    let kubelet_config_clone = kubelet_config.clone();

    let metrics_addr = format!("0.0.0.0:{}", config.metrics_port);
    info!(
        "Starting kubelet API server on {} (metrics + configz)",
        metrics_addr
    );

    let cri_client = cri::CriClient::new(
        &config.container_runtime_endpoint,
        &config.image_service_endpoint,
    );

    // Streaming server (exec/attach/portForward SPDY) on its own port; it also
    // proxies plain HTTP (e.g. /containerLogs) to the HTTP API on metrics_port.
    let streaming_cri = cri_client.clone();
    let streaming_port = config.streaming_port;
    let http_forward_port = config.metrics_port;
    tokio::spawn(async move {
        if let Err(e) =
            streaming_server::start(streaming_cri, streaming_port, http_forward_port).await
        {
            tracing::error!("streaming server failed: {:#}", e);
        }
    });

    tokio::spawn(async move {
        use axum::{routing::get, Json, Router};
        let app = Router::new()
            .route("/metrics", get(|| async move { metrics_clone.gather() }))
            .route(
                "/configz",
                get(|| async move { Json(kubelet_config_clone.as_ref().clone()) }),
            )
            .merge(server::router(cri_client));
        let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    let k = Arc::new(
        kubelet::Kubelet::new(
            config.node_name,
            storage,
            config.sync_interval,
            config.volume_dir,
            cluster_dns,
            config.cluster_domain,
            config.network,
            config.kubernetes_service_host,
            config.container_runtime_endpoint,
            config.streaming_port,
            config.image_service_endpoint,
        )
        .await?,
    );
    k.run().await?;

    Ok(())
}

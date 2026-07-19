#[allow(dead_code)]
mod cni;
mod config;
mod cri;
#[allow(dead_code)]
mod eviction;
mod kubelet;
mod runtime;
mod server;

use anyhow::Result;
use axum::{routing::get, Json, Router};
use clap::Parser;
use config::{KubeletConfiguration, RuntimeConfig};
use kubelet::Kubelet;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{info, warn, Level};

#[derive(Parser, Debug)]
#[command(name = "rusternetes-kubelet")]
#[command(about = "Rusternetes Kubelet - Node agent that manages containers", long_about = None)]
#[command(version)]
struct Args {
    /// Node name
    #[arg(long)]
    node_name: String,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Path to kubelet configuration file
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Root directory for managing kubelet files (volume data, plugin state, etc.)
    #[arg(long, value_name = "DIR")]
    root_dir: Option<String>,

    /// Directory path for managing volume data
    #[arg(long, value_name = "DIR")]
    volume_dir: Option<String>,

    /// Directory where volume plugins are installed
    #[arg(long, value_name = "DIR")]
    volume_plugin_dir: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long)]
    log_level: Option<String>,

    /// Sync interval in seconds
    #[arg(long)]
    sync_interval: Option<u64>,

    /// Metrics server port
    #[arg(long)]
    metrics_port: Option<u16>,

    /// Cluster DNS service IP address (dynamically discovered if not provided)
    #[arg(long)]
    cluster_dns: Option<String>,

    /// Cluster domain suffix
    #[arg(long, default_value = "cluster.local")]
    cluster_domain: String,

    /// Container network to connect pods to
    #[arg(long, default_value = "rusternetes-network")]
    network: String,

    /// CRI runtime service endpoint (e.g. unix:///run/containerd/containerd.sock)
    #[arg(long, value_name = "ENDPOINT")]
    container_runtime_endpoint: Option<String>,

    /// CRI image service endpoint (defaults to --container-runtime-endpoint)
    #[arg(long, value_name = "ENDPOINT")]
    image_service_endpoint: Option<String>,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration file if specified
    let config_file = if let Some(config_path) = &args.config {
        info!("Loading kubelet configuration from: {}", config_path);
        Some(KubeletConfiguration::from_file(config_path)?)
    } else {
        None
    };

    // Parse etcd endpoints
    let etcd_endpoints: Vec<String> = args
        .etcd_servers
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // Build runtime configuration with proper precedence
    let runtime_config = RuntimeConfig::build(
        args.root_dir,
        args.volume_dir,
        args.volume_plugin_dir,
        args.sync_interval,
        args.metrics_port,
        args.log_level,
        config_file,
        args.node_name,
        etcd_endpoints,
        args.container_runtime_endpoint,
        args.image_service_endpoint,
    )?;

    // Initialize tracing
    let level = match runtime_config.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt().with_max_level(level).init();

    info!("Starting Rusternetes Kubelet");
    info!("{}", runtime_config.display());

    // Initialize storage
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            info!("Connecting to etcd: {:?}", runtime_config.etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: runtime_config.etcd_endpoints.clone(),
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Discover cluster DNS IP if not provided
    let cluster_dns = match args.cluster_dns {
        Some(dns) => {
            info!("Using provided cluster DNS: {}", dns);
            dns
        }
        None => {
            info!("Discovering cluster DNS IP from kube-dns service...");
            use rusternetes_common::resources::Service;
            use rusternetes_storage::Storage;

            match storage
                .get::<Service>("/registry/services/kube-system/kube-dns")
                .await
            {
                Ok(service) => {
                    if let Some(ref cluster_ip) = service.spec.cluster_ip {
                        info!("Discovered cluster DNS IP: {}", cluster_ip);
                        cluster_ip.clone()
                    } else {
                        warn!("kube-dns service has no ClusterIP, falling back to 10.96.0.10");
                        "10.96.0.10".to_string()
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to discover cluster DNS IP: {}. Falling back to 10.96.0.10",
                        e
                    );
                    "10.96.0.10".to_string()
                }
            }
        }
    };

    // Initialize metrics
    let metrics = Arc::new(MetricsRegistry::new().with_kubelet_metrics()?);
    let metrics_clone = metrics.clone();

    // Convert RuntimeConfig to KubeletConfiguration for /configz endpoint
    let kubelet_config = KubeletConfiguration {
        api_version: "kubelet.config.k8s.io/v1beta1".to_string(),
        kind: "KubeletConfiguration".to_string(),
        root_dir: Some(runtime_config.root_dir.to_string_lossy().to_string()),
        volume_dir: Some(runtime_config.volume_dir.to_string_lossy().to_string()),
        volume_plugin_dir: Some(
            runtime_config
                .volume_plugin_dir
                .to_string_lossy()
                .to_string(),
        ),
        sync_frequency: Some(runtime_config.sync_frequency),
        metrics_bind_port: Some(runtime_config.metrics_bind_port),
        log_level: Some(runtime_config.log_level.clone()),
        cluster_service_cidr: None, // Not exposed in config endpoint
        container_runtime_endpoint: Some(runtime_config.container_runtime_endpoint.clone()),
        image_service_endpoint: Some(runtime_config.image_service_endpoint.clone()),
    };
    let kubelet_config = Arc::new(kubelet_config);
    let kubelet_config_clone = kubelet_config.clone();

    // Start metrics and config server
    let metrics_addr = format!("0.0.0.0:{}", runtime_config.metrics_bind_port);
    info!(
        "Starting kubelet API server on {} (metrics + configz)",
        metrics_addr
    );

    let cri_client = cri::CriClient::new(
        &runtime_config.container_runtime_endpoint,
        &runtime_config.image_service_endpoint,
    );
    tokio::spawn(async move {
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

    // Create and run kubelet
    let kubelet = Arc::new(
        Kubelet::new(
            runtime_config.node_name.clone(),
            storage,
            runtime_config.sync_frequency,
            runtime_config.volume_dir.to_string_lossy().to_string(),
            cluster_dns,
            args.cluster_domain,
            args.network,
            runtime_config.kubernetes_service_host.clone(),
            runtime_config.container_runtime_endpoint.clone(),
            runtime_config.image_service_endpoint.clone(),
        )
        .await?,
    );
    kubelet.run().await?;

    Ok(())
}

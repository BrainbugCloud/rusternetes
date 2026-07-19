//! SPDY proxy from api-server to kubelet streaming server.
//!
//! When the api-server receives an exec/attach/portforward request with
//! SPDY upgrade headers from kubectl, this module:
//! 1. Resolves the pod's node → kubelet streaming endpoint
//! 2. Opens a TCP connection to the kubelet
//! 3. Sends a reconstructed HTTP upgrade request
//! 4. Relays SPDY frames bidirectionally
//!
//! This mirrors upstream Kubernetes: the api-server acts as a transparent
//! SPDY proxy to the kubelet's streaming server.

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use rusternetes_common::resources::Pod;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

use crate::state::ApiServerState;
use rusternetes_storage::Storage;

/// The kubelet streaming server endpoint (IP:port) for a pod's node.
pub async fn kubelet_streaming_endpoint(
    state: &ApiServerState,
    pod: &Pod,
) -> Result<String> {
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.clone())
        .context("pod is not scheduled to a node")?;

    let node: rusternetes_common::resources::Node = state
        .storage
        .get(&rusternetes_storage::build_key("nodes", None, &node_name))
        .await
        .with_context(|| format!("failed to get node {node_name}"))?;

    let address = node
        .status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .and_then(|addrs| {
            addrs
                .iter()
                .find(|a| a.address_type == "InternalIP")
                .or_else(|| addrs.iter().find(|a| a.address_type == "ExternalIP"))
        })
        .map(|a| a.address.clone())
        .context("no address found for node")?;

    let port = node
        .status
        .as_ref()
        .and_then(|s| s.daemon_endpoints.as_ref())
        .and_then(|d| d.kubelet_endpoint.as_ref())
        .map(|k| k.port)
        .filter(|p| *p > 0)
        .unwrap_or(10250);

    Ok(format!("{address}:{port}"))
}

/// Route an SPDY upgrade request to the kubelet streaming server and relay
/// data bidirectionally. Returns `None` if the request doesn't have SPDY
/// upgrade headers (caller should fall back to another path).
pub fn spdy_proxy_response(
    upgrade: OnUpgrade,
    kubelet_addr: &str,
    kubelet_path: &str,
) -> Response {

    info!("SPDY proxy → kubelet at {kubelet_addr}{kubelet_path}");

    let kubelet_addr = kubelet_addr.to_string();
    let kubelet_path = kubelet_path.to_string();

    // Build the response: 101 Switching Protocols to SPDY/3.1
    let response = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("Upgrade", "SPDY/3.1")
        .header("Connection", "Upgrade")
        .body(Body::empty())
        .unwrap();

    // After sending the response, upgrade the client connection and relay
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let client = TokioIo::new(upgraded);
                if let Err(e) = relay_spdy(client, &kubelet_addr, &kubelet_path).await {
                    debug!("SPDY relay ended: {e:#}");
                }
            }
            Err(e) => {
                debug!("SPDY upgrade failed: {e}");
            }
        }
    });

    response
}

/// Check if the request headers indicate an SPDY upgrade.
#[allow(dead_code)]
fn is_spdy_upgrade(headers: &HeaderMap) -> bool {
    let connection_upgrade = headers
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_lowercase().contains("upgrade"))
        .unwrap_or(false);

    let upgrade_spdy = headers
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_lowercase().contains("spdy"))
        .unwrap_or(false);

    connection_upgrade && upgrade_spdy
}

/// Open a TCP connection to the kubelet, send an HTTP upgrade request,
/// and relay bytes bidirectionally between client and kubelet.
async fn relay_spdy(
    mut client: TokioIo<hyper::upgrade::Upgraded>,
    kubelet_addr: &str,
    kubelet_path: &str,
) -> Result<()> {
    // 1. Connect to kubelet
    let mut backend = TcpStream::connect(kubelet_addr)
        .await
        .context("failed to connect to kubelet streaming")?;

    // 2. Send HTTP upgrade request to kubelet
    let upgrade_req = format!(
        "GET {kubelet_path} HTTP/1.1\r\n\
         Host: {kubelet_addr}\r\n\
         Upgrade: SPDY/3.1\r\n\
         Connection: Upgrade\r\n\
         X-Stream-Protocol-Version: v4.channel.k8s.io\r\n\
         \r\n"
    );
    backend
        .write_all(upgrade_req.as_bytes())
        .await
        .context("failed to send upgrade to kubelet")?;

    // 3. Read the 101 response from kubelet (byte at a time to not consume SPDY frames)
    let mut resp_buf = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    loop {
        let n = backend.read(&mut byte).await?;
        if n == 0 {
            anyhow::bail!("kubelet closed before upgrade response");
        }
        resp_buf.push(byte[0]);
        if resp_buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if resp_buf.len() > 4096 {
            anyhow::bail!("kubelet response too large");
        }
    }

    let resp_str = String::from_utf8_lossy(&resp_buf);
    if !resp_str.contains("101") {
        // Forward error response to client and bail
        let _ = client.write_all(&resp_buf).await;
        anyhow::bail!("kubelet returned non-101: {resp_str}");
    }

    // 4. Read any SPDY tail bytes already buffered from kubelet
    let mut spdy_tail = Vec::new();
    let mut tail_buf = [0u8; 4096];
    loop {
        match backend.try_read(&mut tail_buf) {
            Ok(0) => break,
            Ok(n) => spdy_tail.extend_from_slice(&tail_buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e.into()),
        }
    }

    // 5. Relay bidirectionally
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut backend_read, mut backend_write) = backend.into_split();

    if !spdy_tail.is_empty() {
        let _ = client_write.write_all(&spdy_tail).await;
    }

    let c2b = tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match client_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if backend_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = backend_write.shutdown().await;
    });

    let b2c = tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match backend_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if client_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = client_write.shutdown().await;
    });

    tokio::select! {
        _ = c2b => {},
        _ = b2c => {},
    }

    Ok(())
}

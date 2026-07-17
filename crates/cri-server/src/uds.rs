// SPDX-License-Identifier: Apache-2.0

//! Unix-socket bootstrap for CRI servers.
//!
//! Filesystem permissions are the trust boundary (containerd convention; no
//! TLS on the CRI socket): the socket is created `0o660`.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use crate::backend::{ImageBackend, RuntimeBackend};
use crate::service::CriService;

/// Remove a stale socket, bind, and set `0o660`.
///
/// Accepts `unix://` URIs and bare paths (see [`cri_proto::uds::socket_path`]).
pub fn bind(endpoint: &str) -> std::io::Result<UnixListener> {
    let path = cri_proto::uds::socket_path(endpoint);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(&path) {
        Ok(()) => tracing::debug!(path = %path.display(), "removed stale CRI socket"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))?;
    Ok(listener)
}

/// Serve both CRI services on a unix socket until `shutdown` resolves.
pub async fn serve<B: RuntimeBackend + ImageBackend>(
    endpoint: &str,
    service: CriService<B>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = bind(endpoint)?;
    tracing::info!(endpoint, "CRI server listening");
    Server::builder()
        .add_service(service.runtime_server())
        .add_service(service.image_server())
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

/// Best-effort removal of the socket file after shutdown.
pub fn cleanup(endpoint: &str) {
    let path = cri_proto::uds::socket_path(endpoint);
    let _ = std::fs::remove_file(Path::new(&path));
}

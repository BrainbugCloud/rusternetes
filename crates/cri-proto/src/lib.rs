// SPDX-License-Identifier: Apache-2.0

//! Kubernetes Container Runtime Interface (CRI) v1 gRPC bindings.
//!
//! Generated with tonic from the vendored
//! [`proto/release-1.36.proto`](https://github.com/kubernetes/cri-api/blob/release-1.36/pkg/apis/runtime/v1/api.proto)
//! (Kubernetes Authors, Apache-2.0). Regenerate the vendored file with
//! `scripts/vendor-cri-proto.sh`.
//!
//! - `v1` — all `runtime.v1` messages plus client stubs
//!   (`runtime_service_client`, `image_service_client`; feature `client`) and
//!   server stubs (`runtime_service_server`, `image_service_server`; feature
//!   `server`).
//! - [`uds`] — helpers to open a tonic [`Channel`](tonic::transport::Channel)
//!   over a unix domain socket, accepting both `unix://` endpoint URIs and
//!   bare filesystem paths (feature `client`).

#![allow(clippy::doc_lazy_continuation)]

/// The `runtime.v1` CRI API: messages, clients, and servers.
pub mod v1 {
    #![allow(clippy::large_enum_variant)]
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(rustdoc::invalid_html_tags)]
    #![allow(rustdoc::bare_urls)]

    tonic::include_proto!("runtime.v1");
}

#[cfg(feature = "client")]
pub mod uds {
    //! Client-side unix-domain-socket helpers.

    use std::path::{Path, PathBuf};

    use hyper_util::rt::TokioIo;
    use tokio::net::UnixStream;
    use tonic::transport::{Channel, Endpoint, Error, Uri};
    use tower::service_fn;

    /// Normalize a kubelet-style runtime endpoint to a filesystem path.
    ///
    /// Accepts `unix:///run/foo.sock`, `unix:/run/foo.sock`, and bare paths
    /// like `/run/foo.sock` (kubelet `--container-runtime-endpoint`
    /// compatibility).
    pub fn socket_path(endpoint: &str) -> PathBuf {
        let path = endpoint
            .strip_prefix("unix://")
            .or_else(|| endpoint.strip_prefix("unix:"))
            .unwrap_or(endpoint);
        PathBuf::from(path)
    }

    /// Connect a tonic [`Channel`] over a unix domain socket.
    ///
    /// `path` may be a `unix://` URI or a bare filesystem path. The HTTP
    /// authority is a placeholder; gRPC over UDS ignores it.
    pub async fn connect_uds(path: impl AsRef<Path>) -> Result<Channel, Error> {
        let path = socket_path(&path.as_ref().to_string_lossy());
        // The URI is required by tonic but never resolved for UDS transports.
        Endpoint::from_static("http://cri.localhost")
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(path).await?))
                }
            }))
            .await
    }

    #[cfg(test)]
    mod tests {
        use super::socket_path;
        use std::path::Path;

        #[test]
        fn socket_path_accepts_uri_and_bare_forms() {
            assert_eq!(
                socket_path("unix:///run/cri.sock"),
                Path::new("/run/cri.sock")
            );
            assert_eq!(
                socket_path("unix:/run/cri.sock"),
                Path::new("/run/cri.sock")
            );
            assert_eq!(socket_path("/run/cri.sock"), Path::new("/run/cri.sock"));
        }
    }
}

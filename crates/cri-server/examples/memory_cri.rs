// SPDX-License-Identifier: Apache-2.0

//! A CRI server backed by the in-memory fake — the crictl/critest target for
//! plan 01-S2/S4 acceptance.
//!
//! ```bash
//! cargo run -p cri-server --features testing --example memory_cri -- \
//!     --listen unix:///tmp/memory-cri.sock
//! crictl --runtime-endpoint unix:///tmp/memory-cri.sock info
//! ```

use std::sync::Arc;

use cri_server::testing::MemoryBackend;
use cri_server::CriService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut listen = "unix:///tmp/memory-cri.sock".to_string();
    let mut streaming_bind = "127.0.0.1:0".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = args
                    .next()
                    .ok_or("--listen requires a value (e.g. unix:///tmp/memory-cri.sock)")?;
            }
            "--streaming-bind" => {
                streaming_bind = args
                    .next()
                    .ok_or("--streaming-bind requires a value (e.g. 127.0.0.1:0)")?;
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: memory_cri [--listen unix:///tmp/memory-cri.sock] \
                     [--streaming-bind 127.0.0.1:0]"
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }

    let backend = Arc::new(MemoryBackend::new());
    let streaming = cri_server::streaming::start(&streaming_bind, backend.clone()).await?;
    let service = CriService::new(backend).with_streaming(streaming);
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    };
    let result = cri_server::uds::serve(&listen, service, shutdown).await;
    cri_server::uds::cleanup(&listen);
    result
}

// SPDX-License-Identifier: Apache-2.0

//! Reusable Kubernetes CRI server harness (see `plan/01-cri-crates.md`).
//!
//! Everything a CRI server needs that is *not* backend-specific:
//!
//! - [`backend`] — the [`RuntimeBackend`](backend::RuntimeBackend) /
//!   [`ImageBackend`](backend::ImageBackend) traits every runtime implements
//! - [`service`] — [`CriService`](service::CriService), the tonic
//!   `RuntimeService`/`ImageService` plumbing over a backend
//! - [`logfmt`] — the CRI container log format (writer + reader)
//! - [`checkpoint`] — checksummed JSON file store for shim bookkeeping
//! - [`labels`] — Kubernetes label/annotation conventions
//! - [`uds`] — unix-socket bootstrap (stale-socket cleanup, permissions)
//! - [`testing`] — an in-memory backend fake (feature `testing`)
//!
//! Parts of this crate are forked from the aurae project's `cri` branch
//! (Apache-2.0); provenance is noted per module.

pub mod backend;
pub mod checkpoint;
pub mod error;
pub mod labels;
pub mod logfmt;
pub mod service;
pub mod streaming;
pub mod uds;

#[cfg(feature = "testing")]
pub mod testing;

pub use backend::{ExecSyncResult, ImageBackend, RuntimeBackend};
pub use error::{Error, Result};
pub use service::CriService;
pub use streaming::{AttachStreams, ExecStreams, Streaming, StreamingBackend};

/// Re-export of the CRI proto bindings this crate is built on.
pub use cri_proto;

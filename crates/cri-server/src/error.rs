// SPDX-License-Identifier: Apache-2.0

//! Backend error type mapping to CRI-conventional gRPC status codes.
//!
//! CRI conventions (asserted by critest):
//! - unknown id on a *status* call → `NotFound`
//! - stop/remove of an unknown id → `Ok` (idempotent; backends return `Ok(())`)

use tonic::Status;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unknown sandbox/container/image id on a status-like call.
    #[error("not found: {0}")]
    NotFound(String),
    /// Malformed or missing request data.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The operation is known but not supported by this backend.
    #[error("unimplemented: {0}")]
    Unimplemented(String),
    /// The underlying runtime is unreachable or not ready.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// The operation exceeded its deadline (e.g. `ExecSync` timeout).
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),
    /// A precondition failed (e.g. starting a container that already exited).
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// Any other backend failure.
    #[error("{0}")]
    Internal(String),
    /// I/O failure (log files, checkpoints, sockets).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound(msg) => Status::not_found(msg),
            Error::InvalidArgument(msg) => Status::invalid_argument(msg),
            Error::Unimplemented(msg) => Status::unimplemented(msg),
            Error::Unavailable(msg) => Status::unavailable(msg),
            Error::DeadlineExceeded(msg) => Status::deadline_exceeded(msg),
            Error::FailedPrecondition(msg) => Status::failed_precondition(msg),
            Error::Internal(msg) => Status::internal(msg),
            Error::Io(err) => Status::internal(err.to_string()),
        }
    }
}

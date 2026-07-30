use std::fmt;

/// Errors from the guest agent or the VMM broker.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `SandboxContext` RPC failed. Carries the gRPC status so callers can map
    /// `NOT_FOUND` / `UNIMPLEMENTED` onto their own error space.
    #[error("guest agent rpc `{rpc}` failed: {status}")]
    Rpc {
        rpc: &'static str,
        #[source]
        status: tonic::Status,
    },

    /// Could not establish or keep the gRPC transport to the guest agent.
    #[error("guest agent transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// The VMM broker (the host-side component that owns the VM) failed.
    #[error("vmm: {0}")]
    Vmm(String),

    /// The OCI spec could not be encoded for `CreateProcessRequest.configuration`.
    #[error("encode oci spec: {0}")]
    SpecEncode(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The caller asked for something the pod's state machine does not allow —
    /// e.g. starting a container in an un-created pod.
    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The guest agent reported the operation as unsupported. `VirtualMachineAgent`
    /// requires implementations to signal this rather than fail arbitrarily.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    pub(crate) fn rpc(rpc: &'static str, status: tonic::Status) -> Self {
        // The guest maps its own failures onto gRPC codes; surface the ones the
        // pod layer branches on as typed variants instead of opaque Rpc errors.
        match status.code() {
            tonic::Code::NotFound => Self::NotFound(format!("{rpc}: {}", status.message())),
            tonic::Code::InvalidArgument => {
                Self::InvalidArgument(format!("{rpc}: {}", status.message()))
            }
            tonic::Code::Unimplemented => Self::Unsupported(format!("{rpc}: {}", status.message())),
            _ => Self::Rpc { rpc, status },
        }
    }

    pub fn vmm(msg: impl fmt::Display) -> Self {
        Self::Vmm(msg.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

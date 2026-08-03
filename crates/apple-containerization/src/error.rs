use std::fmt;

/// Errors from the VMM broker.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The VMM broker (the host-side component that owns the VM) failed. Its
    /// message is the broker's own, which is where pod and container failures
    /// surface now that the broker owns `LinuxPod`.
    #[error("vmm: {0}")]
    Vmm(String),

    #[error("encode broker request: {0}")]
    Encode(#[from] serde_json::Error),

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

    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    pub fn vmm(msg: impl fmt::Display) -> Self {
        Self::Vmm(msg.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

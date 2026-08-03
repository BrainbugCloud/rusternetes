//! A host-side client for the rusternetes VMM broker's **pod** protocol.
//!
//! Apple's `container` CLI runs one microVM per container. Kubernetes needs one
//! sandbox per *pod*, with several containers sharing its network and one pod IP.
//! Apple's `Containerization` package has the primitive for that — `LinuxPod`,
//! one VM hosting N containers — but it is Swift, and the host-side operations it
//! needs (owning the `VZVirtualMachine`, vsock, admitting a rootfs into a running
//! VM, unpacking an image) are only reachable through Virtualization.framework
//! from the process that owns the VM.
//!
//! So a small Swift broker owns all of that (`vmm-broker/`), and this crate is
//! the client half: [`PodBroker`] is the entire host-side pod surface, spoken as
//! newline-delimited JSON over a unix socket.
//!
//! # What this crate used to be
//!
//! It was a Rust port of `LinuxPod` + `Vminitd` + the OCI spec types — ~2400
//! lines that spoke `SandboxContext` gRPC to the guest directly and drove a dumb
//! VM. That port duplicated working Swift, did not receive Apple's fixes, and its
//! first live boot failed on a `LinuxPod.create()` precondition it had not
//! replicated. The broker now calls the real `LinuxPod`, and this crate is the
//! protocol to it.
//!
//! What that leaves here is deliberately thin. Pod semantics are Apple's; the CRI
//! translation is `apple-cri`'s. This crate is the wire between them.
//!
//! # Layering
//!
//! This crate must not depend on any `rusternetes-*` crate, nor on `cri-proto` /
//! `cri-server`. It knows the broker protocol and nothing about CRI; `apple-cri`
//! is what joins the two.

pub mod broker;
pub mod error;

/// A fake broker, so the CRI translation can be tested without a hypervisor.
/// Enabled by the `testing` feature so `apple-cri` can use it too.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use broker::{
    AttachedFilesystem, BlockMount, BrokerClient, BrokerRootfs, ContainerConfigWire,
    ContainerStatsWire, DnsConfigWire, ExecOptions, ImageWire, Interface, PodBroker, PodConfigWire,
};
pub use error::{Error, Result};

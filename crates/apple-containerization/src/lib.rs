//! A low-level client for Apple's Containerization sandbox protocol.
//!
//! Apple's `container` CLI runs **one microVM per container**. Kubernetes needs
//! one sandbox per *pod*, with several containers sharing its network, IPC and
//! (optionally) PID namespaces. The layer that makes that possible is not the
//! CLI: it is `vminitd`, the guest agent, which serves the `SandboxContext` gRPC
//! service on vsock port 1024 and whose process RPCs all carry a `containerID`.
//! One VM, many containers, each with its own rootfs and OCI runtime invocation.
//!
//! This crate is the host side of that protocol, ported from Apple's
//! Containerization Swift package (`ff44a5b`, v0.40.1):
//!
//! | module | ported from |
//! |---|---|
//! | [`agent`] | `Sources/Containerization/Vminitd.swift` + `VirtualMachineAgent.swift` |
//! | [`oci`] | `Sources/ContainerizationOCI/Spec.swift` |
//! | [`pod`] | `Sources/Containerization/LinuxPod.swift` |
//! | [`vmm`] | `VirtualMachineManager.swift` / `VirtualMachineInstance.swift` |
//! | [`broker`] | no upstream equivalent — see below |
//! | [`proto`] | `Sources/Containerization/SandboxContext/SandboxContext.proto` |
//!
//! # What is Rust and what cannot be
//!
//! Everything inside the guest is gRPC, so it is all Rust. Four host-side
//! operations are only available through Virtualization.framework, in the process
//! that owns the `VZVirtualMachine`: VM lifecycle, vsock dial/listen, block
//! hotplug and virtiofs shares. Those sit behind the [`vmm::Vmm`] trait, so the
//! pod logic, OCI specs, namespace wiring and process lifecycle stay here. See
//! the [`vmm`] module docs for why that boundary falls exactly there.
//!
//! # Layering
//!
//! This crate must not depend on any `rusternetes-*` crate, nor on `cri-proto` /
//! `cri-server`. It knows about Apple's guest protocol and nothing about CRI;
//! `apple-cri` is what joins the two.

pub mod agent;
pub mod broker;
pub mod error;
pub mod oci;
pub mod pod;
pub mod vmm;

/// Generated `SandboxContext` v3 bindings.
///
/// From `proto/sandbox_context_v3.proto`, vendored verbatim from
/// containerization `ff44a5b`. Regenerate by updating that file; the proto
/// package is `com.apple.containerization.sandbox.v3`.
pub mod proto {
    tonic::include_proto!("com.apple.containerization.sandbox.v3");
}

/// An in-process fake guest agent and mock VMM, for testing pod semantics
/// without a hypervisor. Enabled by the `testing` feature so `apple-cri` can use
/// it too.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use agent::{Agent, AGENT_VSOCK_PORT};
pub use broker::{BrokerClient, BrokerRootfs, BrokerVmm};
pub use error::{Error, Result};
pub use pod::{ContainerConfig, ContainerState, NamespaceMode, Pod, PodConfig, ProcessConfig};
pub use vmm::{AttachedFilesystem, BlockMount, Interface, VmConfig, VmInstance, VmState, Vmm};

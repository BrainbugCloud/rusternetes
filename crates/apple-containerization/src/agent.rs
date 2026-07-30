//! The guest-agent client: a Rust port of the host side of
//! `Sources/Containerization/Vminitd.swift` (containerization ff44a5b, v0.40.1).
//!
//! `vminitd` runs as PID 1 in the microVM and serves `SandboxContext` (proto
//! package `com.apple.containerization.sandbox.v3`) as a gRPC-over-HTTP/2 server
//! on **vsock port 1024**. Every method here is one RPC, named after the
//! `VirtualMachineAgent` protocol method it implements so the two can be diffed.
//!
//! The transport is deliberately just a `tonic::transport::Channel`: upstream's
//! `Vminitd.init(connection: FileHandle, ...)` wraps an already-connected socket
//! fd, and we do the same over whatever byte stream the [`Vmm`](crate::vmm::Vmm)
//! broker hands back for that port.

use tonic::transport::Channel;

use crate::error::{Error, Result};
use crate::oci;
use crate::proto::sandbox_context_client::SandboxContextClient;
use crate::proto::{self as pb};

/// The vsock port `vminitd` serves `SandboxContext` on.
///
/// `Vminitd.swift:30` — `public static let port: UInt32 = 1024`.
pub const AGENT_VSOCK_PORT: u32 = 1024;

/// The guest `PATH` the agent seeds during `standard_setup`.
///
/// `LinuxProcessConfiguration.swift:363`.
pub const DEFAULT_GUEST_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Flags for [`Agent::write_file`], mirroring upstream's `WriteFileFlags`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteFileFlags {
    pub create_parent_directories: bool,
    pub append: bool,
    pub create: bool,
}

/// A filesystem operation on a mounted filesystem in the guest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilesystemOperation {
    Freeze,
    Thaw,
    Trim,
}

/// Which statistics categories to ask the guest for. Empty means "all", matching
/// `ContainerStatisticsRequest.categories`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatCategories {
    pub process: bool,
    pub memory: bool,
    pub cpu: bool,
    pub block_io: bool,
    pub network: bool,
    pub memory_events: bool,
}

impl StatCategories {
    pub const ALL: Self = Self {
        process: true,
        memory: true,
        cpu: true,
        block_io: true,
        network: true,
        memory_events: true,
    };

    fn to_proto(self) -> Vec<i32> {
        // An empty list means "every category" to the guest, so ALL sends nothing
        // rather than enumerating — same wire result, fewer bytes.
        if self == Self::ALL {
            return Vec::new();
        }
        let mut out = Vec::new();
        if self.process {
            out.push(pb::StatCategory::Process as i32);
        }
        if self.memory {
            out.push(pb::StatCategory::Memory as i32);
        }
        if self.cpu {
            out.push(pb::StatCategory::Cpu as i32);
        }
        if self.block_io {
            out.push(pb::StatCategory::BlockIo as i32);
        }
        if self.network {
            out.push(pb::StatCategory::Network as i32);
        }
        if self.memory_events {
            out.push(pb::StatCategory::MemoryEvents as i32);
        }
        out
    }
}

/// A DNS configuration to write into the guest, mirroring upstream's `DNS`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsConfig {
    pub nameservers: Vec<String>,
    pub domain: Option<String>,
    pub search_domains: Vec<String>,
    pub options: Vec<String>,
}

/// One `/etc/hosts` line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostsEntry {
    pub ip_address: String,
    pub hostnames: Vec<String>,
    pub comment: Option<String>,
}

/// Where a process's stdio is wired. Each field is a **host** vsock port that
/// the guest dials (stdout/stderr) or that the host listens on (stdin).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stdio {
    pub stdin: Option<u32>,
    pub stdout: Option<u32>,
    pub stderr: Option<u32>,
}

/// How a process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitStatus {
    pub exit_code: i32,
    /// Guest-side exit timestamp, as seconds/nanos since the epoch.
    pub exited_at: Option<(i64, i32)>,
}

/// Per-container statistics as reported by the guest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatistics {
    pub id: String,
    pub process: Option<ProcessStats>,
    pub memory: Option<MemoryStats>,
    pub cpu: Option<CpuStats>,
    pub networks: Vec<NetworkStats>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessStats {
    pub current: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryStats {
    pub usage_bytes: u64,
    pub limit_bytes: u64,
    pub cache_bytes: u64,
    pub inactive_file: u64,
    pub anon: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CpuStats {
    pub usage_usec: u64,
    pub user_usec: u64,
    pub system_usec: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkStats {
    pub interface: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
}

/// A connection to `vminitd` inside one microVM.
///
/// Cloning is cheap: `tonic::transport::Channel` is an `Arc`-backed multiplexed
/// HTTP/2 connection, so clones share the single vsock stream.
#[derive(Debug, Clone)]
pub struct Agent {
    client: SandboxContextClient<Channel>,
}

impl Agent {
    /// Wrap an established channel to the guest's `SandboxContext` server.
    pub fn new(channel: Channel) -> Self {
        Self {
            // The guest sends whole OCI specs and stat batches; the tonic default
            // decode cap (4 MiB) is ample, but specs grow with mount count, so be
            // explicit rather than surprised.
            client: SandboxContextClient::new(channel).max_decoding_message_size(16 * 1024 * 1024),
        }
    }

    /// Platform-standard guest setup, run once per VM before any container.
    ///
    /// Ported from `Vminitd.standardSetup()`: bring up `lo`, seed `PATH`, then
    /// mount `/tmp` and `/dev/pts`. vminitd already mounts `/proc`, `/sys`,
    /// `/sys/fs/cgroup` and `/run` itself.
    pub async fn standard_setup(&self) -> Result<()> {
        self.up("lo", None).await?;
        self.setenv("PATH", Some(DEFAULT_GUEST_PATH)).await?;

        for mount in [
            oci::Mount::new("tmpfs", "tmpfs", "/tmp", vec![]),
            oci::Mount::new(
                "devpts",
                "devpts",
                "/dev/pts",
                vec!["gid=5".into(), "mode=620".into(), "ptmxmode=666".into()],
            ),
        ] {
            self.mount(&mount).await?;
        }
        Ok(())
    }

    // ---- POSIX-y -----------------------------------------------------------

    pub async fn mount(&self, mount: &oci::Mount) -> Result<()> {
        let req = pb::MountRequest {
            r#type: mount.type_.clone(),
            source: mount.source.clone(),
            destination: mount.destination.clone(),
            options: mount.options.clone(),
        };
        self.client
            .clone()
            .mount(req)
            .await
            .map_err(|s| Error::rpc("Mount", s))?;
        Ok(())
    }

    pub async fn umount(&self, path: &str, flags: i32) -> Result<()> {
        self.client
            .clone()
            .umount(pb::UmountRequest {
                path: path.to_string(),
                flags,
            })
            .await
            .map_err(|s| Error::rpc("Umount", s))?;
        Ok(())
    }

    pub async fn mkdir(&self, path: &str, all: bool, perms: u32) -> Result<()> {
        self.client
            .clone()
            .mkdir(pb::MkdirRequest {
                path: path.to_string(),
                all,
                perms,
            })
            .await
            .map_err(|s| Error::rpc("Mkdir", s))?;
        Ok(())
    }

    /// `value: None` unsets the variable, matching the proto's `optional string`.
    pub async fn setenv(&self, key: &str, value: Option<&str>) -> Result<()> {
        self.client
            .clone()
            .setenv(pb::SetenvRequest {
                key: key.to_string(),
                value: value.map(|v| v.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("Setenv", s))?;
        Ok(())
    }

    pub async fn getenv(&self, key: &str) -> Result<Option<String>> {
        let resp = self
            .client
            .clone()
            .getenv(pb::GetenvRequest {
                key: key.to_string(),
            })
            .await
            .map_err(|s| Error::rpc("Getenv", s))?;
        Ok(resp.into_inner().value)
    }

    pub async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        flags: WriteFileFlags,
        mode: u32,
    ) -> Result<()> {
        self.client
            .clone()
            .write_file(pb::WriteFileRequest {
                path: path.to_string(),
                data,
                mode,
                flags: Some(pb::write_file_request::WriteFileFlags {
                    create_parent_dirs: flags.create_parent_directories,
                    append: flags.append,
                    create_if_missing: flags.create,
                }),
            })
            .await
            .map_err(|s| Error::rpc("WriteFile", s))?;
        Ok(())
    }

    /// Signal a guest process by PID (not by process id — see
    /// [`Agent::signal_process`] for that).
    pub async fn kill(&self, pid: i32, signal: i32) -> Result<i32> {
        let resp = self
            .client
            .clone()
            .kill(pb::KillRequest { pid, signal })
            .await
            .map_err(|s| Error::rpc("Kill", s))?;
        Ok(resp.into_inner().result)
    }

    pub async fn sync(&self) -> Result<()> {
        self.client
            .clone()
            .sync(pb::SyncRequest {})
            .await
            .map_err(|s| Error::rpc("Sync", s))?;
        Ok(())
    }

    pub async fn sysctl(&self, settings: std::collections::HashMap<String, String>) -> Result<()> {
        self.client
            .clone()
            .sysctl(pb::SysctlRequest { settings })
            .await
            .map_err(|s| Error::rpc("Sysctl", s))?;
        Ok(())
    }

    pub async fn filesystem_operation(&self, op: FilesystemOperation, path: &str) -> Result<()> {
        use pb::filesystem_operation_request::Operation;
        let operation = match op {
            FilesystemOperation::Freeze => Operation::Freeze(pb::FiFreezeParams {}),
            FilesystemOperation::Thaw => Operation::Thaw(pb::FiThawParams {}),
            FilesystemOperation::Trim => Operation::Trim(pb::FiTrimParams {
                schedule: Some(pb::fi_trim_params::Schedule::OneShot(
                    pb::fi_trim_params::OneShot {},
                )),
            }),
        };
        self.client
            .clone()
            .filesystem_operation(pb::FilesystemOperationRequest {
                path: path.to_string(),
                operation: Some(operation),
            })
            .await
            .map_err(|s| Error::rpc("FilesystemOperation", s))?;
        Ok(())
    }

    // ---- process lifecycle -------------------------------------------------

    /// Create (but do not start) a process.
    ///
    /// `container_id` is what makes one VM hold many containers: the guest keys
    /// its container table by it, and `id == container_id` is the container's
    /// init process while a distinct `id` is an exec into that container.
    ///
    /// `oci_runtime_path` selects the OCI runtime in the guest — `None` uses
    /// vminitd's built-in `vmexec`, `Some("/usr/bin/runc")` shells out to runc.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_process(
        &self,
        id: &str,
        container_id: Option<&str>,
        stdio: Stdio,
        oci_runtime_path: Option<&str>,
        spec: &oci::Spec,
        options: Option<Vec<u8>>,
    ) -> Result<()> {
        // The guest decodes this with JSONDecoder into ContainerizationOCI.Spec.
        let configuration = serde_json::to_vec(spec)?;
        self.client
            .clone()
            .create_process(pb::CreateProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
                stdin: stdio.stdin,
                stdout: stdio.stdout,
                stderr: stdio.stderr,
                oci_runtime_path: oci_runtime_path.map(|s| s.to_string()),
                configuration,
                options,
            })
            .await
            .map_err(|s| Error::rpc("CreateProcess", s))?;
        Ok(())
    }

    /// Start a created process, returning its guest PID.
    pub async fn start_process(&self, id: &str, container_id: Option<&str>) -> Result<i32> {
        let resp = self
            .client
            .clone()
            .start_process(pb::StartProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("StartProcess", s))?;
        Ok(resp.into_inner().pid)
    }

    pub async fn signal_process(
        &self,
        id: &str,
        container_id: Option<&str>,
        signal: i32,
    ) -> Result<i32> {
        let resp = self
            .client
            .clone()
            .kill_process(pb::KillProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
                signal,
            })
            .await
            .map_err(|s| Error::rpc("KillProcess", s))?;
        Ok(resp.into_inner().result)
    }

    pub async fn wait_process(&self, id: &str, container_id: Option<&str>) -> Result<ExitStatus> {
        let resp = self
            .client
            .clone()
            .wait_process(pb::WaitProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("WaitProcess", s))?
            .into_inner();
        Ok(ExitStatus {
            exit_code: resp.exit_code,
            exited_at: resp.exited_at.map(|t| (t.seconds, t.nanos)),
        })
    }

    pub async fn resize_process(
        &self,
        id: &str,
        container_id: Option<&str>,
        columns: u32,
        rows: u32,
    ) -> Result<()> {
        self.client
            .clone()
            .resize_process(pb::ResizeProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
                rows,
                columns,
            })
            .await
            .map_err(|s| Error::rpc("ResizeProcess", s))?;
        Ok(())
    }

    pub async fn delete_process(&self, id: &str, container_id: Option<&str>) -> Result<()> {
        self.client
            .clone()
            .delete_process(pb::DeleteProcessRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("DeleteProcess", s))?;
        Ok(())
    }

    pub async fn close_process_stdin(&self, id: &str, container_id: Option<&str>) -> Result<()> {
        self.client
            .clone()
            .close_process_stdin(pb::CloseProcessStdinRequest {
                id: id.to_string(),
                container_id: container_id.map(|s| s.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("CloseProcessStdin", s))?;
        Ok(())
    }

    // ---- statistics --------------------------------------------------------

    pub async fn container_statistics(
        &self,
        container_ids: Vec<String>,
        categories: StatCategories,
    ) -> Result<Vec<ContainerStatistics>> {
        let resp = self
            .client
            .clone()
            .container_statistics(pb::ContainerStatisticsRequest {
                container_ids,
                categories: categories.to_proto(),
            })
            .await
            .map_err(|s| Error::rpc("ContainerStatistics", s))?
            .into_inner();

        Ok(resp
            .containers
            .into_iter()
            .map(|c| ContainerStatistics {
                id: c.container_id,
                process: c.process.map(|p| ProcessStats {
                    current: p.current,
                    limit: p.limit,
                }),
                memory: c.memory.map(|m| MemoryStats {
                    usage_bytes: m.usage_bytes,
                    limit_bytes: m.limit_bytes,
                    cache_bytes: m.cache_bytes,
                    inactive_file: m.inactive_file,
                    anon: m.anon,
                }),
                cpu: c.cpu.map(|c| CpuStats {
                    usage_usec: c.usage_usec,
                    user_usec: c.user_usec,
                    system_usec: c.system_usec,
                }),
                networks: c
                    .networks
                    .into_iter()
                    .map(|n| NetworkStats {
                        interface: n.interface,
                        rx_bytes: n.received_bytes,
                        tx_bytes: n.transmitted_bytes,
                        rx_packets: n.received_packets,
                        tx_packets: n.transmitted_packets,
                        rx_errors: n.received_errors,
                        tx_errors: n.transmitted_errors,
                    })
                    .collect(),
            })
            .collect())
    }

    // ---- networking --------------------------------------------------------

    pub async fn up(&self, name: &str, mtu: Option<u32>) -> Result<()> {
        self.client
            .clone()
            .ip_link_set(pb::IpLinkSetRequest {
                interface: name.to_string(),
                up: true,
                mtu,
            })
            .await
            .map_err(|s| Error::rpc("IpLinkSet", s))?;
        Ok(())
    }

    pub async fn down(&self, name: &str) -> Result<()> {
        self.client
            .clone()
            .ip_link_set(pb::IpLinkSetRequest {
                interface: name.to_string(),
                up: false,
                mtu: None,
            })
            .await
            .map_err(|s| Error::rpc("IpLinkSet", s))?;
        Ok(())
    }

    pub async fn address_add(
        &self,
        name: &str,
        ipv4_cidr: &str,
        ipv6_cidr: Option<&str>,
    ) -> Result<()> {
        self.client
            .clone()
            .ip_addr_add(pb::IpAddrAddRequest {
                interface: name.to_string(),
                ipv4_address: ipv4_cidr.to_string(),
                ipv6_address: ipv6_cidr.map(|s| s.to_string()),
            })
            .await
            .map_err(|s| Error::rpc("IpAddrAdd", s))?;
        Ok(())
    }

    pub async fn route_add_default(&self, name: &str, ipv4_gateway: &str) -> Result<()> {
        self.client
            .clone()
            .ip_route_add_default(pb::IpRouteAddDefaultRequest {
                interface: name.to_string(),
                ipv4_gateway: ipv4_gateway.to_string(),
                ipv6_gateway: None,
            })
            .await
            .map_err(|s| Error::rpc("IpRouteAddDefault", s))?;
        Ok(())
    }

    pub async fn configure_dns(&self, config: &DnsConfig, location: &str) -> Result<()> {
        self.client
            .clone()
            .configure_dns(pb::ConfigureDnsRequest {
                location: location.to_string(),
                nameservers: config.nameservers.clone(),
                domain: config.domain.clone(),
                search_domains: config.search_domains.clone(),
                options: config.options.clone(),
            })
            .await
            .map_err(|s| Error::rpc("ConfigureDns", s))?;
        Ok(())
    }

    pub async fn configure_hosts(&self, entries: &[HostsEntry], location: &str) -> Result<()> {
        self.client
            .clone()
            .configure_hosts(pb::ConfigureHostsRequest {
                location: location.to_string(),
                entries: entries
                    .iter()
                    .map(|e| pb::configure_hosts_request::HostsEntry {
                        ip_address: e.ip_address.clone(),
                        hostnames: e.hostnames.clone(),
                        comment: e.comment.clone(),
                    })
                    .collect(),
                comment: None,
            })
            .await
            .map_err(|s| Error::rpc("ConfigureHosts", s))?;
        Ok(())
    }

    // ---- vsock proxying ----------------------------------------------------

    /// Proxy a guest unix socket to a vsock port, or the reverse.
    pub async fn proxy_vsock(
        &self,
        id: &str,
        vsock_port: u32,
        guest_path: &str,
        into_guest: bool,
        guest_socket_permissions: Option<u32>,
    ) -> Result<()> {
        let action = if into_guest {
            pb::proxy_vsock_request::Action::Into
        } else {
            pb::proxy_vsock_request::Action::OutOf
        };
        self.client
            .clone()
            .proxy_vsock(pb::ProxyVsockRequest {
                id: id.to_string(),
                vsock_port,
                guest_path: guest_path.to_string(),
                guest_socket_permissions,
                action: action as i32,
            })
            .await
            .map_err(|s| Error::rpc("ProxyVsock", s))?;
        Ok(())
    }

    pub async fn stop_vsock_proxy(&self, id: &str) -> Result<()> {
        self.client
            .clone()
            .stop_vsock_proxy(pb::StopVsockProxyRequest { id: id.to_string() })
            .await
            .map_err(|s| Error::rpc("StopVsockProxy", s))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_categories_sends_empty_list() {
        // Empty means "all" on the wire, so ALL must not enumerate.
        assert!(StatCategories::ALL.to_proto().is_empty());
    }

    #[test]
    fn selected_categories_map_to_enum_values() {
        let cats = StatCategories {
            cpu: true,
            memory: true,
            ..Default::default()
        };
        let proto = cats.to_proto();
        assert_eq!(
            proto,
            vec![
                pb::StatCategory::Memory as i32,
                pb::StatCategory::Cpu as i32
            ]
        );
    }

    #[test]
    fn agent_port_matches_upstream_constant() {
        assert_eq!(AGENT_VSOCK_PORT, 1024);
    }
}

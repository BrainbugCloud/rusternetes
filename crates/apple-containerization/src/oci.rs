//! OCI runtime-spec types, as the guest agent expects them.
//!
//! `CreateProcessRequest.configuration` is a **JSON-encoded OCI runtime spec**
//! (`Vminitd.swift:234` builds it with `JSONEncoder`), which the guest hands to
//! an OCI runtime — `vmexec` by default, or `runc` when `ociRuntimePath` is set.
//!
//! Ported from `Sources/ContainerizationOCI/Spec.swift` (containerization
//! ff44a5b, v0.40.1). That file is itself a partial port of runtime-spec v1.2.0:
//! Linux-only, with some fields omitted. We mirror *its* field set and JSON keys
//! so the bytes we send are the bytes Apple's guest agent already round-trips.
//!
//! Swift's synthesised `Codable` emits every non-optional property, so
//! non-`Option` fields here are serialised unconditionally; `Option` fields are
//! skipped when `None`. Keys are Swift property names (camelCase), except
//! `Spec::version`, which upstream maps to `ociVersion`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The OCI runtime spec (`config.json`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Spec {
    /// Upstream `CodingKeys` maps `version` to `ociVersion`.
    #[serde(rename = "ociVersion")]
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Hooks>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,
    pub hostname: String,
    pub domainname: String,
    pub mounts: Vec<Mount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<Root>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linux: Option<Linux>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Process {
    pub cwd: String,
    pub env: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub console_size: Option<Box_>,
    #[serde(rename = "selinuxLabel")]
    pub selinux_label: String,
    #[serde(rename = "noNewPrivileges")]
    pub no_new_privileges: bool,
    #[serde(rename = "commandLine")]
    pub command_line: String,
    #[serde(rename = "oomScoreAdj", skip_serializing_if = "Option::is_none")]
    pub oom_score_adj: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<LinuxCapabilities>,
    #[serde(rename = "apparmorProfile")]
    pub apparmor_profile: String,
    pub user: User,
    pub rlimits: Vec<PosixRlimit>,
    pub args: Vec<String>,
    pub terminal: bool,
}

impl Process {
    /// `consoleSize` is renamed explicitly rather than via `rename_all` because
    /// the surrounding struct mixes snake- and camel-cased Swift properties.
    pub fn with_args(args: Vec<String>) -> Self {
        Self {
            cwd: "/".to_string(),
            args,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename = "Box")]
pub struct Box_ {
    pub height: u64,
    pub width: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub uid: u32,
    pub gid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub umask: Option<u32>,
    #[serde(rename = "additionalGids")]
    pub additional_gids: Vec<u32>,
    pub username: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounding: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inheritable: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permitted: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ambient: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PosixRlimit {
    #[serde(rename = "type")]
    pub type_: String,
    pub hard: u64,
    pub soft: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Root {
    pub path: String,
    pub readonly: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    #[serde(rename = "type")]
    pub type_: String,
    pub source: String,
    pub destination: String,
    pub options: Vec<String>,
    #[serde(rename = "uidMappings", skip_serializing_if = "Option::is_none")]
    pub uid_mappings: Option<Vec<LinuxIdMapping>>,
    #[serde(rename = "gidMappings", skip_serializing_if = "Option::is_none")]
    pub gid_mappings: Option<Vec<LinuxIdMapping>>,
}

impl Mount {
    /// A `bind` mount, the shape used for every mount the pod assembles in the
    /// guest (rootfs bind-ins, pod volumes, virtiofs re-binds).
    pub fn bind(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            type_: "bind".to_string(),
            source: source.into(),
            destination: destination.into(),
            options: vec!["bind".to_string()],
            ..Default::default()
        }
    }

    pub fn new(
        type_: impl Into<String>,
        source: impl Into<String>,
        destination: impl Into<String>,
        options: Vec<String>,
    ) -> Self {
        Self {
            type_: type_.into(),
            source: source.into(),
            destination: destination.into(),
            options,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hooks {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prestart: Option<Vec<Hook>>,
    #[serde(rename = "createRuntime", skip_serializing_if = "Option::is_none")]
    pub create_runtime: Option<Vec<Hook>>,
    #[serde(rename = "createContainer", skip_serializing_if = "Option::is_none")]
    pub create_container: Option<Vec<Hook>>,
    #[serde(rename = "startContainer", skip_serializing_if = "Option::is_none")]
    pub start_container: Option<Vec<Hook>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poststart: Option<Vec<Hook>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poststop: Option<Vec<Hook>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hook {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Linux {
    #[serde(rename = "uidMappings")]
    pub uid_mappings: Vec<LinuxIdMapping>,
    #[serde(rename = "gidMappings")]
    pub gid_mappings: Vec<LinuxIdMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sysctl: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<LinuxResources>,
    #[serde(rename = "cgroupsPath")]
    pub cgroups_path: String,
    pub namespaces: Vec<LinuxNamespace>,
    pub devices: Vec<LinuxDevice>,
    #[serde(rename = "rootfsPropagation")]
    pub rootfs_propagation: String,
    #[serde(rename = "maskedPaths")]
    pub masked_paths: Vec<String>,
    #[serde(rename = "readonlyPaths")]
    pub readonly_paths: Vec<String>,
    #[serde(rename = "mountLabel")]
    pub mount_label: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxNamespace {
    #[serde(rename = "type")]
    pub type_: LinuxNamespaceType,
    /// Empty means "create a new namespace of this type"; a `/proc/<pid>/ns/<t>`
    /// path means "join the namespace that path refers to". This is the whole
    /// mechanism behind pod-shared namespaces.
    pub path: String,
}

impl LinuxNamespace {
    /// A fresh namespace of `type_`.
    pub fn new(type_: LinuxNamespaceType) -> Self {
        Self {
            type_,
            path: String::new(),
        }
    }

    /// Join an existing namespace by path.
    pub fn joining(type_: LinuxNamespaceType, path: impl Into<String>) -> Self {
        Self {
            type_,
            path: path.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinuxNamespaceType {
    #[default]
    Pid,
    Network,
    Uts,
    Mount,
    Ipc,
    User,
    Cgroup,
}

impl LinuxNamespaceType {
    /// The `/proc/<pid>/ns/<name>` component for this namespace type. `network`
    /// is `net` on the procfs side — the one place the OCI name and the kernel
    /// name differ.
    pub fn procfs_name(&self) -> &'static str {
        match self {
            Self::Pid => "pid",
            Self::Network => "net",
            Self::Uts => "uts",
            Self::Mount => "mnt",
            Self::Ipc => "ipc",
            Self::User => "user",
            Self::Cgroup => "cgroup",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxIdMapping {
    #[serde(rename = "containerID")]
    pub container_id: u32,
    #[serde(rename = "hostID")]
    pub host_id: u32,
    pub size: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LinuxResources {
    pub devices: Vec<LinuxDeviceCgroup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<LinuxMemory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<LinuxCpu>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pids: Option<LinuxPids>,
    #[serde(rename = "hugepageLimits")]
    pub hugepage_limits: Vec<LinuxHugepageLimit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unified: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxMemory {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kernel: Option<i64>,
    #[serde(rename = "kernelTCP", skip_serializing_if = "Option::is_none")]
    pub kernel_tcp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swappiness: Option<u64>,
    #[serde(rename = "disableOOMKiller", skip_serializing_if = "Option::is_none")]
    pub disable_oom_killer: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxCpu {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shares: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<u64>,
    #[serde(rename = "realtimeRuntime", skip_serializing_if = "Option::is_none")]
    pub realtime_runtime: Option<i64>,
    #[serde(rename = "realtimePeriod", skip_serializing_if = "Option::is_none")]
    pub realtime_period: Option<i64>,
    pub cpus: String,
    pub mems: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxPids {
    pub limit: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxHugepageLimit {
    #[serde(rename = "pageSize")]
    pub page_size: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxDevice {
    pub path: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub major: i64,
    pub minor: i64,
    #[serde(rename = "fileMode", skip_serializing_if = "Option::is_none")]
    pub file_mode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxDeviceCgroup {
    pub allow: bool,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub major: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
}

/// The default mount set every container gets, ported from
/// `LinuxContainer.defaultMounts()` (containerization
/// `Sources/Containerization/LinuxContainer.swift`).
pub fn default_mounts() -> Vec<Mount> {
    vec![
        Mount::new("proc", "proc", "/proc", vec![]),
        Mount::new(
            "tmpfs",
            "tmpfs",
            "/dev",
            vec![
                "nosuid".into(),
                "strictatime".into(),
                "mode=755".into(),
                "size=65536k".into(),
            ],
        ),
        Mount::new(
            "devpts",
            "devpts",
            "/dev/pts",
            vec![
                "nosuid".into(),
                "noexec".into(),
                "newinstance".into(),
                "ptmxmode=0666".into(),
                "mode=0620".into(),
            ],
        ),
        Mount::new(
            "tmpfs",
            "shm",
            "/dev/shm",
            vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "mode=1777".into(),
                "size=65536k".into(),
            ],
        ),
        Mount::new(
            "mqueue",
            "mqueue",
            "/dev/mqueue",
            vec!["nosuid".into(), "noexec".into(), "nodev".into()],
        ),
        Mount::new(
            "sysfs",
            "sysfs",
            "/sys",
            vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "ro".into(),
            ],
        ),
        Mount::new(
            "cgroup2",
            "cgroup",
            "/sys/fs/cgroup",
            vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "relatime".into(),
                "ro".into(),
            ],
        ),
    ]
}

/// Ported from `LinuxContainer.defaultMaskedPaths()`.
pub fn default_masked_paths() -> Vec<String> {
    [
        "/proc/acpi",
        "/proc/asound",
        "/proc/kcore",
        "/proc/keys",
        "/proc/latency_stats",
        "/proc/timer_list",
        "/proc/timer_stats",
        "/proc/sched_debug",
        "/proc/scsi",
        "/sys/firmware",
        "/sys/devices/virtual/powercap",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Ported from `LinuxContainer.defaultReadonlyPaths()`.
pub fn default_readonly_paths() -> Vec<String> {
    [
        "/proc/bus",
        "/proc/fs",
        "/proc/irq",
        "/proc/sys",
        "/proc/sysrq-trigger",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Deduplicate by destination (last wins) and sort parents before children, so
/// a mount never lands on a destination its parent has yet to create.
///
/// Ported from `cleanAndSortMounts` in containerization
/// (`Sources/Containerization/LinuxContainer.swift`), which `LinuxPod`
/// calls on the assembled mount list before handing the spec to the guest.
pub fn clean_and_sort_mounts(mounts: Vec<Mount>) -> Vec<Mount> {
    let mut by_destination: HashMap<String, Mount> = HashMap::new();
    for mount in mounts {
        by_destination.insert(mount.destination.clone(), mount);
    }
    let mut out: Vec<Mount> = by_destination.into_values().collect();
    out.sort_by(|a, b| {
        let depth = |p: &str| p.split('/').filter(|s| !s.is_empty()).count();
        depth(&a.destination)
            .cmp(&depth(&b.destination))
            .then_with(|| a.destination.cmp(&b.destination))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_serialises_version_as_oci_version() {
        let spec = Spec {
            version: "1.2.0".to_string(),
            ..Default::default()
        };
        let json: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["ociVersion"], "1.2.0");
        assert!(json.get("version").is_none());
    }

    #[test]
    fn namespace_with_empty_path_means_new_namespace() {
        let json = serde_json::to_value(LinuxNamespace::new(LinuxNamespaceType::Pid)).unwrap();
        assert_eq!(json["type"], "pid");
        assert_eq!(json["path"], "");
    }

    #[test]
    fn namespace_type_serialises_lowercase_but_procfs_maps_network_to_net() {
        let json = serde_json::to_value(LinuxNamespaceType::Network).unwrap();
        assert_eq!(json, "network");
        assert_eq!(LinuxNamespaceType::Network.procfs_name(), "net");
        assert_eq!(LinuxNamespaceType::Mount.procfs_name(), "mnt");
    }

    #[test]
    fn optional_fields_are_omitted_not_null() {
        let spec = Spec::default();
        let json: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert!(json.get("process").is_none());
        assert!(json.get("linux").is_none());
        assert!(json.get("root").is_none());
        // Non-optional in Swift, so always emitted.
        assert_eq!(json["hostname"], "");
        assert!(json["mounts"].is_array());
    }

    #[test]
    fn spec_round_trips() {
        let spec = Spec {
            version: "1.2.0".to_string(),
            hostname: "pod-1".to_string(),
            process: Some(Process::with_args(vec!["/bin/sh".into()])),
            root: Some(Root {
                path: "/run/container/c1/rootfs".to_string(),
                readonly: true,
            }),
            linux: Some(Linux {
                cgroups_path: "/container/pod/p1/c1".to_string(),
                namespaces: vec![LinuxNamespace::joining(
                    LinuxNamespaceType::Pid,
                    "/proc/42/ns/pid",
                )],
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&spec).unwrap();
        let back: Spec = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn clean_and_sort_dedupes_by_destination_and_orders_parents_first() {
        let mounts = vec![
            Mount::bind("/a", "/deep/nested/path"),
            Mount::bind("/b", "/deep"),
            Mount::bind("/c", "/deep/nested"),
            // Duplicate destination — last one wins.
            Mount::bind("/override", "/deep"),
        ];
        let sorted = clean_and_sort_mounts(mounts);
        let dests: Vec<&str> = sorted.iter().map(|m| m.destination.as_str()).collect();
        assert_eq!(dests, vec!["/deep", "/deep/nested", "/deep/nested/path"]);
        assert_eq!(sorted[0].source, "/override");
    }

    #[test]
    fn default_mounts_match_upstream_shape() {
        let mounts = default_mounts();
        let dests: Vec<&str> = mounts.iter().map(|m| m.destination.as_str()).collect();
        assert_eq!(
            dests,
            vec![
                "/proc",
                "/dev",
                "/dev/pts",
                "/dev/shm",
                "/dev/mqueue",
                "/sys",
                "/sys/fs/cgroup"
            ]
        );
        // cgroup2, not cgroup — the guest is cgroup-v2 only.
        assert_eq!(mounts.last().unwrap().type_, "cgroup2");
    }
}

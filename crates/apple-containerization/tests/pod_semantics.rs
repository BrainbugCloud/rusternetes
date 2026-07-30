//! Pod semantics, verified over real gRPC against an in-process `SandboxContext`
//! server.
//!
//! These are the tests that matter: they assert the *call sequence* and the exact
//! OCI specs the pod sends, which is where "is this really a Kubernetes pod"
//! is decided. Nothing is stubbed at the client boundary — the pod drives the
//! generated tonic client, the spec is JSON-encoded and decoded by the fake guest
//! exactly as `vminitd` would, and a malformed spec fails the test.
//!
//! The reference for every expectation is `LinuxPod.swift` in Apple's
//! Containerization (ff44a5b, v0.40.1), except where the module docs of
//! `apple_containerization::pod` record a deliberate Kubernetes divergence.

use std::collections::HashMap;
use std::sync::Arc;

use apple_containerization::agent::{DnsConfig, HostsEntry, StatCategories, Stdio};
use apple_containerization::oci::{self, LinuxNamespaceType};
use apple_containerization::pod::{
    ContainerConfig, ContainerState, NamespaceMode, Pod, PodConfig, PodVolume, ProcessConfig,
    VolumeMount,
};
use apple_containerization::testing::{Call, FakeGuest, MockVmm, VmmCall};
use apple_containerization::vmm::{BlockMount, Interface};

/// Build a pod backed by a fake guest, with `config`.
async fn pod_with(config: PodConfig) -> (Pod, FakeGuest, Arc<MockVmm>) {
    let guest = FakeGuest::start().await.expect("start fake guest");
    let vmm = Arc::new(MockVmm::new(guest.clone()));
    let pod = Pod::new("pod-1", config, vmm.clone()).expect("build pod");
    (pod, guest, vmm)
}

async fn created_pod() -> (Pod, FakeGuest, Arc<MockVmm>) {
    let (pod, guest, vmm) = pod_with(PodConfig::default()).await;
    pod.create().await.expect("create pod");
    (pod, guest, vmm)
}

fn container_with_args(args: &[&str]) -> ContainerConfig {
    ContainerConfig {
        process: ProcessConfig {
            args: args.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn rootfs(path: &str) -> BlockMount {
    BlockMount::block("ext4", path)
}

/// Namespace entries of a spec as (type, path) pairs.
fn namespaces(spec: &oci::Spec) -> Vec<(LinuxNamespaceType, String)> {
    spec.linux
        .as_ref()
        .expect("spec has linux")
        .namespaces
        .iter()
        .map(|n| (n.type_, n.path.clone()))
        .collect()
}

fn ns_path(spec: &oci::Spec, type_: LinuxNamespaceType) -> Option<String> {
    namespaces(spec)
        .into_iter()
        .find(|(t, _)| *t == type_)
        .map(|(_, p)| p)
}

// ---------------------------------------------------------------------------
// Sandbox bring-up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_boots_the_vm_then_sets_up_the_guest() {
    let (_pod, guest, vmm) = created_pod().await;

    // The VM is created and started before anything is asked of the guest.
    let vmm_calls = vmm.calls();
    assert!(matches!(vmm_calls[0], VmmCall::Create { .. }));
    assert_eq!(vmm_calls[1], VmmCall::Start);
    // The agent is dialed on vminitd's well-known port.
    assert!(vmm_calls.contains(&VmmCall::Dial { port: 1024 }));

    let calls = guest.calls();
    // standard_setup: lo up, then PATH, then /tmp and /dev/pts.
    assert_eq!(
        calls[0],
        Call::StandardSetupUp {
            interface: "lo".to_string()
        }
    );
    assert_eq!(
        calls[1],
        Call::Setenv {
            key: "PATH".to_string(),
            value: Some("/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string()),
        }
    );
    let setup_mounts: Vec<String> = calls
        .iter()
        .filter_map(|c| match c {
            Call::Mount { destination, .. } => Some(destination.clone()),
            _ => None,
        })
        .take(2)
        .collect();
    assert_eq!(setup_mounts, vec!["/tmp", "/dev/pts"]);
}

#[tokio::test]
async fn create_starts_an_infra_process_that_anchors_pod_namespaces() {
    let (_pod, guest, _vmm) = created_pod().await;

    // The pause rootfs is just a bind of the guest's /sbin — no image needed.
    assert!(guest.calls().contains(&Call::Mount {
        type_: String::new(),
        source: "/sbin".to_string(),
        destination: "/run/container/pause-pod-1/rootfs/sbin".to_string(),
        options: vec!["bind".to_string()],
    }));

    let spec = guest
        .spec_for("pause-pod-1")
        .expect("infra process was created");
    assert_eq!(
        spec.process.as_ref().unwrap().args,
        vec!["/sbin/vminitd", "pause"]
    );

    // It *creates* all five namespaces, so members have something to join.
    let mut types: Vec<LinuxNamespaceType> =
        namespaces(&spec).into_iter().map(|(t, _)| t).collect();
    types.sort_by_key(|t| format!("{t:?}"));
    assert_eq!(
        types,
        vec![
            LinuxNamespaceType::Cgroup,
            LinuxNamespaceType::Ipc,
            LinuxNamespaceType::Mount,
            LinuxNamespaceType::Pid,
            LinuxNamespaceType::Uts,
        ]
    );
    // All fresh: an empty path means "create".
    assert!(namespaces(&spec).iter().all(|(_, p)| p.is_empty()));

    // The infra process runs under vmexec, not a container OCI runtime.
    let created = guest
        .calls()
        .into_iter()
        .find_map(|c| match c {
            Call::CreateProcess {
                id,
                oci_runtime_path,
                ..
            } if id == "pause-pod-1" => Some(oci_runtime_path),
            _ => None,
        })
        .unwrap();
    assert_eq!(created, None);
}

#[tokio::test]
async fn create_configures_pod_networking_once_for_the_whole_vm() {
    let config = PodConfig {
        interfaces: vec![Interface {
            address: "192.168.64.7/24".to_string(),
            gateway: Some("192.168.64.1".to_string()),
            mtu: Some(1500),
            mac_address: None,
        }],
        dns: Some(DnsConfig {
            nameservers: vec!["10.96.0.10".to_string()],
            search_domains: vec!["default.svc.cluster.local".to_string()],
            ..Default::default()
        }),
        hosts: vec![HostsEntry {
            ip_address: "192.168.64.7".to_string(),
            hostnames: vec!["my-pod".to_string()],
            comment: None,
        }],
        hostname: Some("my-pod".to_string()),
        ..Default::default()
    };
    let (pod, guest, _vmm) = pod_with(config).await;
    pod.create().await.expect("create pod");

    let calls = guest.calls();
    assert!(calls.contains(&Call::IpLinkSet {
        interface: "eth0".to_string(),
        up: true,
        mtu: Some(1500),
    }));
    assert!(calls.contains(&Call::IpAddrAdd {
        interface: "eth0".to_string(),
        ipv4: "192.168.64.7/24".to_string(),
    }));
    assert!(calls.contains(&Call::IpRouteAddDefault {
        interface: "eth0".to_string(),
        gateway: "192.168.64.1".to_string(),
    }));
    assert!(calls.contains(&Call::ConfigureDns {
        location: "/etc/resolv.conf".to_string(),
        nameservers: vec!["10.96.0.10".to_string()],
        search_domains: vec!["default.svc.cluster.local".to_string()],
    }));
    assert!(calls.contains(&Call::ConfigureHosts {
        location: "/etc/hosts".to_string(),
        entries: vec![("192.168.64.7".to_string(), vec!["my-pod".to_string()])],
    }));

    // Exactly one address configuration for the pod, however many containers
    // later join it — the VM is the pod's network.
    let addr_calls = calls
        .iter()
        .filter(|c| matches!(c, Call::IpAddrAdd { .. }))
        .count();
    assert_eq!(addr_calls, 1);
}

// ---------------------------------------------------------------------------
// Adding containers to a live sandbox — the CRI path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_container_after_create_hotplugs_and_mounts_the_rootfs() {
    let (pod, guest, vmm) = created_pod().await;

    pod.add_container("c1", rootfs("/images/c1.ext4"), ContainerConfig::default())
        .await
        .expect("add container");

    // Hotplug, because the VM is already running. This is what makes CRI's
    // "create a container in a live sandbox" possible at all.
    assert!(vmm.calls().contains(&VmmCall::Hotplug {
        id: "c1".to_string(),
        source: "/images/c1.ext4".into(),
    }));

    // Then mounted at the canonical guest path.
    assert!(guest
        .mount_destinations()
        .contains(&"/run/container/c1/rootfs".to_string()));
    assert_eq!(
        pod.container_state("c1").await,
        Some(ContainerState::Created)
    );
}

#[tokio::test]
async fn hotplug_attaches_read_write_even_for_a_readonly_rootfs() {
    // `ro` is stripped before attach: the block device mounts rw in the guest and
    // the OCI runtime remounts read-only from root.readonly. Attaching `ro` would
    // fail the guest mount before the runtime ever ran.
    let (pod, _guest, _vmm) = created_pod().await;
    let mut ro = rootfs("/images/c1.ext4");
    ro.options.push("ro".to_string());

    pod.add_container("c1", ro, ContainerConfig::default())
        .await
        .expect("add container");
    pod.start_container("c1").await.expect("start");

    let guest_spec = _guest.spec_for("c1").unwrap();
    assert!(
        guest_spec.root.as_ref().unwrap().readonly,
        "readonly must be expressed in the OCI spec"
    );
}

#[tokio::test]
async fn multiple_containers_share_one_vm() {
    let (pod, guest, vmm) = created_pod().await;

    for id in ["c1", "c2", "c3"] {
        pod.add_container(
            id,
            rootfs(&format!("/images/{id}.ext4")),
            container_with_args(&["/bin/sleep", "1000"]),
        )
        .await
        .expect("add container");
        pod.start_container(id).await.expect("start container");
    }

    // One VM created, three rootfs hotplugs.
    let creates = vmm
        .calls()
        .iter()
        .filter(|c| matches!(c, VmmCall::Create { .. }))
        .count();
    assert_eq!(creates, 1, "one VM per pod, not per container");
    let hotplugs = vmm
        .calls()
        .iter()
        .filter(|c| matches!(c, VmmCall::Hotplug { .. }))
        .count();
    assert_eq!(hotplugs, 3);

    // Each container is its own container in the guest's table, addressed by
    // containerID, with its own rootfs.
    for id in ["c1", "c2", "c3"] {
        let spec = guest.spec_for(id).expect("container spec");
        assert_eq!(
            spec.root.as_ref().unwrap().path,
            format!("/run/container/{id}/rootfs")
        );
        assert!(guest.calls().contains(&Call::StartProcess {
            id: id.to_string(),
            container_id: Some(id.to_string()),
        }));
    }

    let mut listed = pod.list_containers().await;
    listed.sort();
    assert_eq!(listed, vec!["c1", "c2", "c3"]);
}

// ---------------------------------------------------------------------------
// Namespace sharing — the actual pod semantics
// ---------------------------------------------------------------------------

/// The infra PID the fake guest handed out for `pause-pod-1`. PIDs start at 100
/// and the infra process is always started first.
const INFRA_PID: i32 = 100;

#[tokio::test]
async fn containers_share_the_pod_ipc_namespace() {
    let (pod, guest, _vmm) = created_pod().await;
    for id in ["c1", "c2"] {
        pod.add_container(id, rootfs("/i.ext4"), ContainerConfig::default())
            .await
            .unwrap();
        pod.start_container(id).await.unwrap();
    }

    // Both join the *same* namespace path — the infra process's.
    let expected = format!("/proc/{INFRA_PID}/ns/ipc");
    for id in ["c1", "c2"] {
        let spec = guest.spec_for(id).unwrap();
        assert_eq!(
            ns_path(&spec, LinuxNamespaceType::Ipc),
            Some(expected.clone()),
            "{id} must share the pod IPC namespace"
        );
    }
}

#[tokio::test]
async fn containers_share_the_pod_uts_namespace_and_carry_no_hostname() {
    let (pod, guest, _vmm) = pod_with(PodConfig {
        hostname: Some("my-pod".to_string()),
        ..Default::default()
    })
    .await;
    pod.create().await.unwrap();
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    // The pod hostname lives on the infra spec, which owns the UTS namespace.
    let infra = guest.spec_for("pause-pod-1").unwrap();
    assert_eq!(infra.hostname, "my-pod");

    let spec = guest.spec_for("c1").unwrap();
    assert_eq!(
        ns_path(&spec, LinuxNamespaceType::Uts),
        Some(format!("/proc/{INFRA_PID}/ns/uts"))
    );
    // Critical: an OCI runtime can only set a hostname in a UTS namespace it
    // created. Carrying one while inheriting the namespace makes runc fail.
    assert_eq!(
        spec.hostname, "",
        "a container joining the pod UTS namespace must not carry a hostname"
    );
}

#[tokio::test]
async fn pid_namespace_is_private_unless_the_pod_shares_it() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            pid_namespace: NamespaceMode::Container,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    // Empty path = a fresh PID namespace for this container alone.
    assert_eq!(
        ns_path(&guest.spec_for("c1").unwrap(), LinuxNamespaceType::Pid),
        Some(String::new())
    );
}

#[tokio::test]
async fn share_process_namespace_joins_containers_to_the_infra_pid_namespace() {
    let (pod, guest, _vmm) = pod_with(PodConfig {
        share_process_namespace: true,
        ..Default::default()
    })
    .await;
    pod.create().await.unwrap();

    for id in ["c1", "c2"] {
        pod.add_container(
            id,
            rootfs("/i.ext4"),
            ContainerConfig {
                pid_namespace: NamespaceMode::Pod,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pod.start_container(id).await.unwrap();
    }

    let expected = format!("/proc/{INFRA_PID}/ns/pid");
    for id in ["c1", "c2"] {
        assert_eq!(
            ns_path(&guest.spec_for(id).unwrap(), LinuxNamespaceType::Pid),
            Some(expected.clone()),
            "{id} must share the pod PID namespace"
        );
    }
}

#[tokio::test]
async fn pod_mode_without_share_process_namespace_degrades_to_private() {
    // A container asking for POD pid mode in a pod that never enabled sharing
    // must not silently end up in the infra namespace.
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            pid_namespace: NamespaceMode::Pod,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    assert_eq!(
        ns_path(&guest.spec_for("c1").unwrap(), LinuxNamespaceType::Pid),
        Some(String::new())
    );
}

#[tokio::test]
async fn no_network_namespace_is_declared_so_the_vm_netns_is_the_pod_network() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    let spec = guest.spec_for("c1").unwrap();
    assert!(
        ns_path(&spec, LinuxNamespaceType::Network).is_none(),
        "declaring a network namespace would break the shared pod IP and localhost"
    );
}

#[tokio::test]
async fn node_namespace_mode_inherits_the_vm_namespace() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            ipc_namespace: NamespaceMode::Node,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    // No entry at all — the container inherits the VM's root IPC namespace.
    assert!(ns_path(&guest.spec_for("c1").unwrap(), LinuxNamespaceType::Ipc).is_none());
}

// ---------------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn container_spec_carries_pod_scoped_cgroup_path_and_resources() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            cpus: Some(2),
            memory_in_bytes: Some(256 * 1024 * 1024),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    let spec = guest.spec_for("c1").unwrap();
    let linux = spec.linux.as_ref().unwrap();
    assert_eq!(linux.cgroups_path, "/container/pod/pod-1/c1");

    let resources = linux.resources.as_ref().unwrap();
    // 2 cpus over the standard 100ms period.
    let cpu = resources.cpu.as_ref().unwrap();
    assert_eq!(cpu.quota, Some(200_000));
    assert_eq!(cpu.period, Some(100_000));
    assert_eq!(
        resources.memory.as_ref().unwrap().limit,
        Some(256 * 1024 * 1024)
    );
}

#[tokio::test]
async fn rootfs_is_not_included_in_the_container_mount_list() {
    // OCI runtimes get the rootfs via root.path and reject it as a mount, which
    // is why upstream drops element 0 of the attachment list.
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    let spec = guest.spec_for("c1").unwrap();
    assert!(
        !spec
            .mounts
            .iter()
            .any(|m| m.destination == "/run/container/c1/rootfs"),
        "the rootfs must not appear in spec.mounts"
    );
    assert_eq!(spec.root.as_ref().unwrap().path, "/run/container/c1/rootfs");
}

#[tokio::test]
async fn container_spec_includes_default_mounts_sorted_parents_first() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    let spec = guest.spec_for("c1").unwrap();
    let dests: Vec<&str> = spec.mounts.iter().map(|m| m.destination.as_str()).collect();
    assert!(dests.contains(&"/proc"));
    assert!(dests.contains(&"/sys/fs/cgroup"));
    // /sys must precede /sys/fs/cgroup, or the child mount has no parent.
    let sys = dests.iter().position(|d| *d == "/sys").unwrap();
    let cgroup = dests.iter().position(|d| *d == "/sys/fs/cgroup").unwrap();
    assert!(sys < cgroup);
}

#[tokio::test]
async fn sysctls_and_path_masking_reach_the_spec() {
    let (pod, guest, _vmm) = created_pod().await;
    let mut sysctl = HashMap::new();
    sysctl.insert("net.ipv4.ip_forward".to_string(), "1".to_string());
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            sysctl,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    let spec = guest.spec_for("c1").unwrap();
    let linux = spec.linux.as_ref().unwrap();
    assert_eq!(
        linux.sysctl.as_ref().unwrap().get("net.ipv4.ip_forward"),
        Some(&"1".to_string())
    );
    assert!(linux.masked_paths.contains(&"/proc/kcore".to_string()));
    assert!(linux.readonly_paths.contains(&"/proc/sys".to_string()));
}

#[tokio::test]
async fn runc_is_selected_per_container_via_oci_runtime_path() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container(
        "c1",
        rootfs("/i.ext4"),
        ContainerConfig {
            oci_runtime_path: Some("/usr/bin/runc".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    pod.start_container("c1").await.unwrap();

    let runtime = guest
        .calls()
        .into_iter()
        .find_map(|c| match c {
            Call::CreateProcess {
                id,
                oci_runtime_path,
                ..
            } if id == "c1" => Some(oci_runtime_path),
            _ => None,
        })
        .unwrap();
    assert_eq!(runtime, Some("/usr/bin/runc".to_string()));
}

#[tokio::test]
async fn container_stdio_gets_distinct_vsock_ports() {
    let (pod, guest, _vmm) = created_pod().await;
    for id in ["c1", "c2"] {
        pod.add_container(id, rootfs("/i.ext4"), ContainerConfig::default())
            .await
            .unwrap();
        pod.start_container(id).await.unwrap();
    }

    let ports: Vec<(Option<u32>, Option<u32>)> = guest
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            Call::CreateProcess {
                id, stdout, stderr, ..
            } if id.starts_with('c') => Some((stdout, stderr)),
            _ => None,
        })
        .collect();
    assert_eq!(ports.len(), 2);
    // Allocated from upstream's base, and never reused.
    let mut all: Vec<u32> = ports
        .iter()
        .flat_map(|(a, b)| [a.unwrap(), b.unwrap()])
        .collect();
    let count = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), count, "stdio ports must be unique");
    assert!(all[0] >= 0x1000_0000);
}

// ---------------------------------------------------------------------------
// Pod volumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pod_volumes_mount_once_and_bind_into_each_container() {
    let config = PodConfig {
        volumes: vec![PodVolume {
            name: "shared".to_string(),
            source: BlockMount::block("ext4", "/volumes/shared.ext4"),
        }],
        ..Default::default()
    };
    let (pod, guest, _vmm) = pod_with(config).await;
    pod.create().await.unwrap();

    // Mounted once at the pod-level path.
    assert!(guest
        .mount_destinations()
        .contains(&"/run/volumes/shared".to_string()));

    for id in ["c1", "c2"] {
        pod.add_container(
            id,
            rootfs("/i.ext4"),
            ContainerConfig {
                volume_mounts: vec![VolumeMount {
                    name: "shared".to_string(),
                    destination: "/data".to_string(),
                    options: vec!["rw".to_string()],
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pod.start_container(id).await.unwrap();
    }

    // Bound into both containers from the single pod-level mount.
    for id in ["c1", "c2"] {
        let spec = guest.spec_for(id).unwrap();
        let mount = spec
            .mounts
            .iter()
            .find(|m| m.destination == "/data")
            .expect("volume bound in");
        assert_eq!(mount.source, "/run/volumes/shared");
        assert!(mount.options.contains(&"bind".to_string()));
    }

    // Still only one guest mount of the volume itself.
    let volume_mounts = guest
        .mount_destinations()
        .into_iter()
        .filter(|d| d == "/run/volumes/shared")
        .count();
    assert_eq!(volume_mounts, 1);
}

#[tokio::test]
async fn unknown_pod_volume_is_rejected() {
    let (pod, _guest, _vmm) = created_pod().await;
    let err = pod
        .add_container(
            "c1",
            rootfs("/i.ext4"),
            ContainerConfig {
                volume_mounts: vec![VolumeMount {
                    name: "nope".to_string(),
                    destination: "/data".to_string(),
                    options: vec![],
                }],
                ..Default::default()
            },
        )
        .await
        .expect_err("unknown volume must fail");
    assert!(err.to_string().contains("unknown pod volume"));
}

#[tokio::test]
async fn duplicate_pod_volume_names_are_rejected() {
    let volume = |name: &str| PodVolume {
        name: name.to_string(),
        source: BlockMount::block("ext4", "/v.ext4"),
    };
    let (pod, _guest, _vmm) = pod_with(PodConfig {
        volumes: vec![volume("dup"), volume("dup")],
        ..Default::default()
    })
    .await;
    let err = pod.create().await.expect_err("duplicate must fail");
    assert!(err.to_string().contains("duplicate pod volume"));
}

// ---------------------------------------------------------------------------
// Exec
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exec_reuses_the_container_id_with_a_distinct_process_id() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    pod.exec(
        "c1",
        "exec-1",
        ProcessConfig {
            args: vec!["/bin/echo".to_string(), "hi".to_string()],
            ..Default::default()
        },
        Stdio {
            stdin: None,
            stdout: Some(0x2000_0000),
            stderr: Some(0x2000_0001),
        },
    )
    .await
    .expect("exec");

    // Same containerID, different process id — that is how SandboxContext models
    // an exec, and it is what puts the exec in the container's namespaces.
    let created = guest
        .calls()
        .into_iter()
        .find_map(|c| match c {
            Call::CreateProcess {
                id, container_id, ..
            } if id == "exec-1" => Some(container_id),
            _ => None,
        })
        .unwrap();
    assert_eq!(created, Some("c1".to_string()));
    assert!(guest.calls().contains(&Call::StartProcess {
        id: "exec-1".to_string(),
        container_id: Some("c1".to_string()),
    }));
}

#[tokio::test]
async fn exec_inherits_the_container_namespaces_and_rootfs() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();
    pod.exec(
        "c1",
        "exec-1",
        ProcessConfig {
            args: vec!["/bin/true".to_string()],
            ..Default::default()
        },
        Stdio::default(),
    )
    .await
    .unwrap();

    let container = guest.spec_for("c1").unwrap();
    let exec = guest.spec_for("exec-1").unwrap();
    assert_eq!(exec.root, container.root);
    assert_eq!(namespaces(&exec), namespaces(&container));
    assert_eq!(exec.process.as_ref().unwrap().args, vec!["/bin/true"]);
}

#[tokio::test]
async fn exec_id_must_differ_from_the_container_id() {
    let (pod, _guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();

    let err = pod
        .exec("c1", "c1", ProcessConfig::default(), Stdio::default())
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("must differ"));
}

#[tokio::test]
async fn exec_requires_a_running_container() {
    let (pod, _guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    // Created but never started.
    let err = pod
        .exec("c1", "e1", ProcessConfig::default(), Stdio::default())
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("must be running"));
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stop_container_signals_waits_then_deletes() {
    let (pod, guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();
    guest.set_exit_code(0);

    let status = pod.stop_container("c1", 15).await.expect("stop");
    assert_eq!(status.exit_code, 0);

    // Order matters: the guest can only reap a process it has waited on.
    let seq: Vec<Call> = guest
        .calls()
        .into_iter()
        .filter(|c| {
            matches!(
                c,
                Call::KillProcess { .. } | Call::WaitProcess { .. } | Call::DeleteProcess { .. }
            )
        })
        .collect();
    assert_eq!(
        seq,
        vec![
            Call::KillProcess {
                id: "c1".to_string(),
                container_id: Some("c1".to_string()),
                signal: 15,
            },
            Call::WaitProcess {
                id: "c1".to_string(),
                container_id: Some("c1".to_string()),
            },
            Call::DeleteProcess {
                id: "c1".to_string(),
                container_id: Some("c1".to_string()),
            },
        ]
    );
    assert_eq!(
        pod.container_state("c1").await,
        Some(ContainerState::Stopped)
    );
}

#[tokio::test]
async fn remove_container_unmounts_and_releases_the_block_device() {
    let (pod, guest, vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.start_container("c1").await.unwrap();
    pod.stop_container("c1", 15).await.unwrap();

    pod.remove_container("c1").await.expect("remove");

    assert!(guest.calls().contains(&Call::Umount {
        path: "/run/container/c1/rootfs".to_string()
    }));
    assert!(vmm.calls().contains(&VmmCall::ReleaseHotplug {
        id: "c1".to_string()
    }));
    assert!(pod.list_containers().await.is_empty());
}

#[tokio::test]
async fn stop_pod_kills_running_containers_then_stops_the_vm() {
    let (pod, guest, vmm) = created_pod().await;
    for id in ["c1", "c2"] {
        pod.add_container(id, rootfs("/i.ext4"), ContainerConfig::default())
            .await
            .unwrap();
        pod.start_container(id).await.unwrap();
    }

    pod.stop().await.expect("stop pod");

    // SIGKILL to each container, then the infra process, then the VM.
    for id in ["c1", "c2"] {
        assert!(guest.calls().contains(&Call::KillProcess {
            id: id.to_string(),
            container_id: Some(id.to_string()),
            signal: 9,
        }));
    }
    assert!(guest.calls().contains(&Call::KillProcess {
        id: "pause-pod-1".to_string(),
        container_id: Some("pause-pod-1".to_string()),
        signal: 9,
    }));
    assert_eq!(*vmm.calls().last().unwrap(), VmmCall::Stop);
}

#[tokio::test]
async fn stop_is_idempotent() {
    let (pod, _guest, _vmm) = created_pod().await;
    pod.stop().await.expect("first stop");
    pod.stop().await.expect("second stop must be a no-op");
}

#[tokio::test]
async fn operations_on_an_uncreated_pod_are_rejected() {
    let (pod, _guest, _vmm) = pod_with(PodConfig::default()).await;
    // Registered before create, so it exists but the VM does not.
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    let err = pod.start_container("c1").await.expect_err("must reject");
    assert!(err.to_string().contains("requires a created pod"));
}

#[tokio::test]
async fn adding_a_container_to_a_stopped_pod_is_rejected() {
    let (pod, _guest, _vmm) = created_pod().await;
    pod.stop().await.unwrap();
    let err = pod
        .add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("stopped pod"));
}

#[tokio::test]
async fn duplicate_container_ids_are_rejected() {
    let (pod, _guest, _vmm) = created_pod().await;
    pod.add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    let err = pod
        .add_container("c1", rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn oversized_ids_are_rejected() {
    let long = "x".repeat(65);
    let vmm = {
        let guest = FakeGuest::start().await.unwrap();
        Arc::new(MockVmm::new(guest))
    };
    assert!(Pod::new(long.clone(), PodConfig::default(), vmm.clone()).is_err());

    let (pod, _guest, _vmm) = created_pod().await;
    let err = pod
        .add_container(long, rootfs("/i.ext4"), ContainerConfig::default())
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("exceeds maximum"));
}

// ---------------------------------------------------------------------------
// Boot-time containers (added before create)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn containers_added_before_create_attach_at_boot_not_by_hotplug() {
    let (pod, guest, vmm) = pod_with(PodConfig::default()).await;
    pod.add_container("c1", rootfs("/images/c1.ext4"), ContainerConfig::default())
        .await
        .unwrap();
    pod.create().await.unwrap();

    // The rootfs was in the machine configuration, so no hotplug was needed.
    assert!(
        !vmm.calls()
            .iter()
            .any(|c| matches!(c, VmmCall::Hotplug { .. })),
        "a boot-time rootfs must not be hotplugged"
    );
    let create = vmm
        .calls()
        .into_iter()
        .find_map(|c| match c {
            VmmCall::Create { config } => Some(config),
            _ => None,
        })
        .unwrap();
    assert!(create.mounts_by_id.contains_key("c1"));

    assert!(guest
        .mount_destinations()
        .contains(&"/run/container/c1/rootfs".to_string()));
    assert_eq!(
        pod.container_state("c1").await,
        Some(ContainerState::Created)
    );
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn statistics_are_reported_per_container() {
    let (pod, _guest, _vmm) = created_pod().await;
    for id in ["c1", "c2"] {
        pod.add_container(id, rootfs("/i.ext4"), ContainerConfig::default())
            .await
            .unwrap();
        pod.start_container(id).await.unwrap();
    }

    let stats = pod
        .statistics(
            vec!["c1".to_string(), "c2".to_string()],
            StatCategories::ALL,
        )
        .await
        .expect("stats");
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].memory.unwrap().usage_bytes, 1024);
    assert_eq!(stats[0].cpu.unwrap().usage_usec, 2048);
}

//! A multi-container pod driven over the real broker wire format.
//!
//! Every other pod test talks to a `Vmm` in-process. These go through
//! [`apple_containerization::BrokerVmm`] to a broker on a unix socket, so the
//! full stack under test is:
//!
//! ```text
//! Pod  ->  BrokerVmm  -> JSON/unix -> broker -> VM
//!      \-> SandboxContext gRPC ----------------> vminitd
//! ```
//!
//! Both hops are the real encodings the Swift broker and Apple's guest agent will
//! see. That makes this the contract the broker has to satisfy: if it answers
//! these methods with these shapes, the pod semantics above it already work.

use std::sync::Arc;

use apple_containerization::broker::Method;
use apple_containerization::oci::LinuxNamespaceType;
use apple_containerization::pod::{ContainerConfig, NamespaceMode, Pod, PodConfig};
use apple_containerization::testing::{Call, FakeBroker, FakeGuest};
use apple_containerization::vmm::BlockMount;
use apple_containerization::{BrokerRootfs, BrokerVmm};

/// The infra PID the fake guest hands out; it is started first and PIDs begin
/// at 100.
const INFRA_PID: i32 = 100;

async fn broker() -> FakeBroker {
    let guest = FakeGuest::start().await.expect("fake guest");
    FakeBroker::start(guest).await.expect("fake broker")
}

fn joined_ns(
    spec: &apple_containerization::oci::Spec,
    type_: LinuxNamespaceType,
) -> Option<String> {
    spec.linux
        .as_ref()?
        .namespaces
        .iter()
        .find(|n| n.type_ == type_)
        .map(|n| n.path.clone())
}

#[tokio::test]
async fn a_pod_boots_over_the_broker_protocol() {
    let broker = broker().await;
    let vmm = Arc::new(BrokerVmm::connect(broker.socket_path()));
    let pod = Pod::new("pod-1", PodConfig::default(), vmm).unwrap();

    pod.create().await.expect("create pod over the broker");

    // The broker saw the VM built, booted, and its agent dialed — in that order.
    let requests = broker.requests();
    let first = requests
        .iter()
        .position(|m| *m == Method::CreateVm)
        .expect("createVm");
    let start = requests
        .iter()
        .position(|m| *m == Method::Start)
        .expect("start");
    let dial = requests
        .iter()
        .position(|m| *m == Method::Dial)
        .expect("dial");
    assert!(first < start, "the VM must exist before it is started");
    assert!(
        start < dial,
        "the VM must be running before the agent is dialed"
    );

    // And the guest was set up through the relayed vsock connection.
    assert!(broker.guest().calls().contains(&Call::StandardSetupUp {
        interface: "lo".to_string()
    }));
    assert!(broker.guest().spec_for("pause-pod-1").is_some());
}

#[tokio::test]
async fn two_containers_share_one_vm_and_the_pod_namespaces() {
    let broker = broker().await;
    let vmm = Arc::new(BrokerVmm::connect(broker.socket_path()));
    let pod = Pod::new(
        "pod-1",
        PodConfig {
            hostname: Some("my-pod".to_string()),
            share_process_namespace: true,
            ..Default::default()
        },
        vmm,
    )
    .unwrap();
    pod.create().await.unwrap();

    for id in ["app", "sidecar"] {
        pod.add_container(
            id,
            BlockMount::block("ext4", format!("/images/{id}.ext4")),
            ContainerConfig {
                pid_namespace: NamespaceMode::Pod,
                ..Default::default()
            },
        )
        .await
        .expect("add container over the broker");
        pod.start_container(id).await.expect("start container");
    }

    // One VM for the pod; a hotplug per container, since the VM is already up.
    let requests = broker.requests();
    assert_eq!(
        requests.iter().filter(|m| **m == Method::CreateVm).count(),
        1,
        "one microVM per pod"
    );
    assert_eq!(
        requests.iter().filter(|m| **m == Method::Hotplug).count(),
        2,
        "each container's rootfs is hotplugged into the live VM"
    );

    // The payoff: both containers join the *same* infra namespaces.
    let app = broker.guest().spec_for("app").expect("app spec");
    let sidecar = broker.guest().spec_for("sidecar").expect("sidecar spec");
    for type_ in [
        LinuxNamespaceType::Ipc,
        LinuxNamespaceType::Uts,
        LinuxNamespaceType::Pid,
    ] {
        let a = joined_ns(&app, type_).expect("namespace present");
        assert_eq!(
            a,
            format!("/proc/{INFRA_PID}/ns/{}", type_.procfs_name()),
            "{type_:?} must join the infra namespace"
        );
        assert_eq!(a, joined_ns(&sidecar, type_).unwrap(), "{type_:?} shared");
    }

    // No network namespace, so the VM's netns is the pod network: one IP, shared
    // localhost. This is what the CLI-backed path structurally cannot do.
    assert!(joined_ns(&app, LinuxNamespaceType::Network).is_none());
    assert!(joined_ns(&sidecar, LinuxNamespaceType::Network).is_none());

    // The pod hostname lives on the infra container, which owns the UTS ns.
    assert_eq!(
        broker.guest().spec_for("pause-pod-1").unwrap().hostname,
        "my-pod"
    );
    assert_eq!(app.hostname, "");

    // Each container still has its own rootfs and cgroup.
    assert_eq!(app.root.as_ref().unwrap().path, "/run/container/app/rootfs");
    assert_eq!(
        sidecar.linux.as_ref().unwrap().cgroups_path,
        "/container/pod/pod-1/sidecar"
    );
}

#[tokio::test]
async fn each_container_gets_its_own_block_device() {
    // The guest distinguishes containers by the device their rootfs came in on,
    // so two hotplugs must not collide.
    let broker = broker().await;
    let vmm = Arc::new(BrokerVmm::connect(broker.socket_path()));
    let pod = Pod::new("pod-1", PodConfig::default(), vmm).unwrap();
    pod.create().await.unwrap();

    for id in ["a", "b", "c"] {
        pod.add_container(
            id,
            BlockMount::block("ext4", format!("/images/{id}.ext4")),
            ContainerConfig::default(),
        )
        .await
        .unwrap();
    }

    let mounted: Vec<String> = broker
        .guest()
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            Call::Mount {
                source,
                destination,
                ..
            } if destination.starts_with("/run/container/") => Some(source),
            _ => None,
        })
        .collect();
    let unique: std::collections::HashSet<&String> = mounted.iter().collect();
    assert_eq!(
        unique.len(),
        mounted.len(),
        "each rootfs must arrive on a distinct device, got {mounted:?}"
    );
}

#[tokio::test]
async fn the_broker_provisions_and_reclaims_rootfs_images() {
    let broker = broker().await;
    let rootfs = BrokerRootfs::new(apple_containerization::BrokerClient::new(
        broker.socket_path(),
    ));

    let block = rootfs
        .provision("registry.k8s.io/e2e-test-images/busybox:1.29-2", "app")
        .await
        .expect("provision");
    assert_eq!(block.format, "ext4");
    assert!(
        block.source.to_string_lossy().contains("app"),
        "the image should be materialised per container, got {block:?}"
    );

    rootfs.release("app").await.expect("release");
    assert!(broker.requests().contains(&Method::ReleaseRootfs));
}

#[tokio::test]
async fn stopping_the_pod_tears_the_vm_down_through_the_broker() {
    let broker = broker().await;
    let vmm = Arc::new(BrokerVmm::connect(broker.socket_path()));
    let pod = Pod::new("pod-1", PodConfig::default(), vmm).unwrap();
    pod.create().await.unwrap();
    pod.add_container(
        "app",
        BlockMount::block("ext4", "/i.ext4"),
        ContainerConfig::default(),
    )
    .await
    .unwrap();
    pod.start_container("app").await.unwrap();

    pod.stop().await.expect("stop pod");

    // The container is killed in the guest, then the VM is stopped — not the
    // other way round, or the guest would never see the signal.
    let guest_killed =
        broker.guest().calls().iter().any(
            |c| matches!(c, Call::KillProcess { id, signal, .. } if id == "app" && *signal == 9),
        );
    assert!(guest_killed, "the container must be signalled in the guest");
    assert_eq!(
        *broker.requests().last().unwrap(),
        Method::Stop,
        "the VM stop must come last"
    );
}

#[tokio::test]
async fn a_broker_error_surfaces_as_a_pod_error_not_a_silent_success() {
    // Point at a socket nothing is listening on: the pod must fail loudly.
    let vmm = Arc::new(BrokerVmm::connect("/tmp/definitely-not-a-broker.sock"));
    let pod = Pod::new("pod-1", PodConfig::default(), vmm).unwrap();
    let err = pod.create().await.expect_err("must fail");
    assert!(
        err.to_string().contains("vmm"),
        "expected a vmm error, got: {err}"
    );
}

#[tokio::test]
async fn an_unknown_vm_id_is_reported_rather_than_ignored() {
    let broker = broker().await;
    let client = apple_containerization::BrokerClient::new(broker.socket_path());
    let err = client
        .call(
            Method::Start,
            apple_containerization::broker::Params {
                vm_id: Some("never-created".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("no such vm"), "got: {err}");
}

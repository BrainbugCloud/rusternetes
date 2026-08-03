//! A multi-container pod driven over the real broker wire format.
//!
//! ```text
//! PodBroker  -> JSON/unix -> broker -> LinuxPod -> vminitd
//! ```
//!
//! The broker owns Apple's `LinuxPod`, so the *semantics* (namespaces, OCI specs)
//! are asserted on the Swift side. What is ours, and what these tests pin, is the
//! **call sequence and the payloads** — which is where CRI's pod model actually
//! shows up:
//!
//! * one `createPod` per pod, however many containers it holds;
//! * containers added after `create` land in a **live** sandbox (hotplug), which
//!   is the ordering every container after the first always takes;
//! * `waitContainer` reports a real exit code, which is what makes an **init
//!   container** expressible at all;
//! * a **sidecar** is the same shape without the wait — two containers running
//!   at once in one pod.
//!
//! Neither init containers nor sidecars exist in CRI; they are kubelet ordering
//! concepts. These are their CRI-level translations, and they are the coverage
//! critest cannot give us: its own multi-container specs live in
//! `pkg/validate/multi_container_linux.go`, and the `_linux.go` suffix is an
//! implicit Go build constraint, so they are not in a darwin critest binary.

use apple_containerization::broker::Method;
use apple_containerization::testing::FakeBroker;
use apple_containerization::{
    BrokerClient, BrokerRootfs, ContainerConfigWire, ExecOptions, PodBroker, PodConfigWire,
};

const POD: &str = "pod-1";

struct Harness {
    broker: FakeBroker,
    pods: PodBroker,
    rootfs: BrokerRootfs,
}

impl Harness {
    async fn start() -> Self {
        let broker = FakeBroker::start().await.expect("fake broker");
        let client = BrokerClient::new(broker.socket_path());
        Self {
            pods: PodBroker::new(client.clone()),
            rootfs: BrokerRootfs::new(client),
            broker,
        }
    }

    /// `RunPodSandbox`: build the pod, then boot it.
    async fn run_sandbox(&self, config: PodConfigWire) {
        self.pods.create_pod(&config).await.expect("createPod");
        self.pods.create(&config.id).await.expect("create");
    }

    /// `CreateContainer`: materialise the image, then add it to the sandbox.
    async fn create_container(&self, id: &str) {
        let block = self
            .rootfs
            .provision(&format!("registry.example/{id}:latest"), id)
            .await
            .expect("provisionRootfs");
        self.pods
            .add_container(POD, &ContainerConfigWire::new(id, block))
            .await
            .expect("addContainer");
    }

    async fn create_and_start(&self, id: &str) {
        self.create_container(id).await;
        self.pods
            .start_container(POD, id)
            .await
            .expect("startContainer");
    }

    /// Index of the first call to `method`, or a failure naming what was seen.
    fn first(&self, method: Method) -> usize {
        let requests = self.broker.requests();
        requests
            .iter()
            .position(|m| *m == method)
            .unwrap_or_else(|| panic!("no {method:?} in {requests:?}"))
    }

    fn count(&self, method: Method) -> usize {
        self.broker
            .requests()
            .iter()
            .filter(|m| **m == method)
            .count()
    }
}

#[tokio::test]
async fn a_pod_is_built_before_it_is_booted() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;

    assert!(
        h.first(Method::CreatePod) < h.first(Method::Create),
        "the pod must exist before its VM is booted"
    );
    // The config the broker was handed is the one it must build LinuxPod from.
    let config = h.broker.params_for(Method::CreatePod)[0]
        .config
        .clone()
        .expect("createPod carries a config");
    assert_eq!(config.id, POD);
}

#[tokio::test]
async fn two_containers_share_one_pod_and_therefore_one_vm() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;

    h.create_and_start("app").await;
    h.create_and_start("sidecar").await;

    assert_eq!(h.count(Method::CreatePod), 1, "one microVM per pod");
    assert_eq!(h.count(Method::AddContainer), 2);
    assert_eq!(
        h.pods.list_containers(POD).await.unwrap(),
        vec!["app", "sidecar"],
        "listContainers must preserve creation order"
    );
}

/// The CRI translation of an **init container**: the next container is not even
/// *created* until this one has exited, and the decision is driven by its exit
/// code. Ordering is the whole assertion.
#[tokio::test]
async fn an_init_container_completes_before_the_next_is_created() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;

    // An init container is one that exits; the fake only lets you wait on a
    // container that has said it will.
    h.broker.set_exit_code("init", 0);
    h.create_and_start("init").await;
    let code = h.pods.wait_container(POD, "init").await.expect("wait");
    assert_eq!(code, 0);
    assert!(
        !h.broker.running(POD).contains(&"init".to_string()),
        "a container that has been waited on is no longer running"
    );

    h.create_and_start("app").await;

    // The ordering that *is* the init-container contract.
    let requests = h.broker.requests();
    let waited = h.first(Method::WaitContainer);
    let added_app = requests
        .iter()
        .enumerate()
        .filter(|(_, m)| **m == Method::AddContainer)
        .nth(1)
        .expect("a second addContainer")
        .0;
    assert!(
        waited < added_app,
        "the init container must be reaped before the next is created: {requests:?}"
    );

    // Still one pod, and the app landed in the already-booted sandbox.
    assert_eq!(h.count(Method::CreatePod), 1);
    assert_eq!(
        h.broker.hotplugged(),
        vec!["init", "app"],
        "every container is added after the sandbox booted, so both hotplug"
    );
}

/// A non-zero init container must surface as a non-zero code. The kubelet drives
/// restart policy off this, so defaulting a missing code to 0 would silently
/// report "succeeded" — the one wrong answer.
#[tokio::test]
async fn a_failing_init_container_reports_its_real_exit_code() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    h.broker.set_exit_code("init", 1);

    h.create_and_start("init").await;

    assert_eq!(h.pods.wait_container(POD, "init").await.unwrap(), 1);
}

/// The CRI translation of a **sidecar** (a restartable init container, KEP-753):
/// the same sequence *without* the wait, so both containers run at once inside
/// one VM. This is what the CLI-backed path structurally cannot do.
#[tokio::test]
async fn a_sidecar_keeps_running_while_the_next_container_starts() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;

    h.create_and_start("sidecar").await;
    h.create_and_start("app").await;

    let mut running = h.broker.running(POD);
    running.sort();
    assert_eq!(
        running,
        vec!["app", "sidecar"],
        "both must be running concurrently in the same pod"
    );
    assert_eq!(h.count(Method::CreatePod), 1, "in one VM");
    assert_eq!(
        h.count(Method::WaitContainer),
        0,
        "a sidecar is precisely the case where the kubelet does not wait"
    );

    // And an exec reaches a container that is running alongside another.
    let pid = h
        .pods
        .exec(
            POD,
            "sidecar",
            "exec-1",
            &ExecOptions::new(vec!["/bin/true".into()]),
        )
        .await
        .expect("exec into the sidecar");
    assert!(pid > 0, "exec must report a guest pid, got {pid}");
}

/// `ExecSync` needs a code, not a pid. `exec` starts the process and `waitProcess`
/// reaps it; a runtime that only reported the pid could never answer "did the
/// command succeed?".
#[tokio::test]
async fn an_exec_can_be_waited_on_for_its_exit_code() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    h.create_and_start("app").await;
    h.broker.set_exit_code("exec-1", 7);

    let pid = h
        .pods
        .exec(
            POD,
            "app",
            "exec-1",
            &ExecOptions::new(vec!["/bin/false".into()]),
        )
        .await
        .expect("exec");
    assert!(pid > 0);
    assert_eq!(
        h.pods.wait_process(POD, "app", "exec-1").await.unwrap(),
        7,
        "the exec's own exit code, not the container's"
    );
    // Reaped: waiting twice is an error rather than a second phantom success.
    assert!(h.pods.wait_process(POD, "app", "exec-1").await.is_err());
}

#[tokio::test]
async fn an_exec_can_be_signalled_for_its_timeout() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    h.create_and_start("app").await;
    h.pods
        .exec(
            POD,
            "app",
            "exec-1",
            &ExecOptions::new(vec!["/bin/sleep".into()]),
        )
        .await
        .expect("exec");

    h.pods
        .kill_process(POD, "app", "exec-1", 15)
        .await
        .expect("killProcess");
    assert_eq!(h.broker.params_for(Method::KillProcess)[0].signal, Some(15));
    // An unknown process must be reported, not silently accepted.
    assert!(h.pods.kill_process(POD, "app", "nope", 15).await.is_err());
}

/// The guest keys a container's init process by the container id, so an exec
/// reusing it would address the wrong process.
#[tokio::test]
async fn an_exec_id_must_differ_from_the_container_id() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    h.create_and_start("app").await;

    let err = h
        .pods
        .exec(
            POD,
            "app",
            "app",
            &ExecOptions::new(vec!["/bin/true".into()]),
        )
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("must differ"), "got: {err}");
}

#[tokio::test]
async fn each_container_gets_its_own_rootfs_block() {
    // The guest distinguishes containers by the device their rootfs arrived on,
    // so two provisions must not collide.
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;

    for id in ["a", "b", "c"] {
        h.create_container(id).await;
    }

    let sources: Vec<String> = h
        .broker
        .params_for(Method::AddContainer)
        .into_iter()
        .map(|p| {
            p.container
                .expect("addContainer carries a container")
                .rootfs
                .source
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let unique: std::collections::HashSet<&String> = sources.iter().collect();
    assert_eq!(
        unique.len(),
        sources.len(),
        "each rootfs must be a distinct block, got {sources:?}"
    );
}

#[tokio::test]
async fn the_broker_provisions_and_reclaims_rootfs_images() {
    let h = Harness::start().await;

    let block = h
        .rootfs
        .provision("registry.k8s.io/e2e-test-images/busybox:1.29-2", "app")
        .await
        .expect("provision");
    assert_eq!(block.format, "ext4");
    assert!(
        block.source.to_string_lossy().contains("app"),
        "the image should be materialised per container, got {block:?}"
    );

    h.rootfs.release("app").await.expect("release");
    assert!(h.broker.requests().contains(&Method::ReleaseRootfs));
}

#[tokio::test]
async fn stopping_the_pod_comes_after_everything_else() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    h.create_and_start("app").await;

    h.pods.stop_pod(POD).await.expect("stopPod");

    assert_eq!(
        *h.broker.requests().last().unwrap(),
        Method::StopPod,
        "the pod teardown must be last; the broker stops containers inside it"
    );
    // Idempotent: a pod the broker has forgotten is already gone.
    h.pods.stop_pod(POD).await.expect("stopPod is idempotent");
}

#[tokio::test]
async fn a_broker_error_surfaces_rather_than_a_silent_success() {
    // Point at a socket nothing is listening on: the call must fail loudly.
    let pods = PodBroker::connect("/tmp/definitely-not-a-broker.sock");
    let err = pods
        .create_pod(&PodConfigWire::new(POD))
        .await
        .expect_err("must fail");
    assert!(
        err.to_string().contains("vmm"),
        "expected a vmm error, got: {err}"
    );
}

#[tokio::test]
async fn an_unknown_pod_is_reported_rather_than_ignored() {
    let h = Harness::start().await;
    let err = h.pods.create("never-created").await.expect_err("must fail");
    assert!(err.to_string().contains("no such pod"), "got: {err}");
}

#[tokio::test]
async fn an_unknown_container_is_reported_rather_than_ignored() {
    let h = Harness::start().await;
    h.run_sandbox(PodConfigWire::new(POD)).await;
    let err = h
        .pods
        .start_container(POD, "never-added")
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("no such container"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Against a live broker
// ---------------------------------------------------------------------------

/// Drive a real pod against a running `rusternetes-vmm`, booting an actual VM.
///
/// Ignored by default: it needs the broker built, signed and listening. Run with
///
/// ```bash
/// bash scripts/build-vmm-broker.sh
/// ./vmm-broker/.build/debug/rusternetes-vmm --listen /tmp/rkv/vmm.sock \
///     --kernel "$HOME/Library/Application Support/com.apple.container/kernels/default.kernel-arm64" \
///     --runtime-dir /tmp/rkv/vmm &
/// VMM_SOCKET=/tmp/rkv/vmm.sock cargo test -p apple-containerization --all-features \
///     --test broker_pod -- --ignored --nocapture
/// ```
///
/// Everything above checks the protocol against a fake. This is the one that
/// checks it against Apple's actual `LinuxPod` and `vminitd`.
#[tokio::test]
#[ignore = "requires a running rusternetes-vmm broker; set VMM_SOCKET"]
async fn a_two_container_pod_boots_against_a_live_broker() {
    let socket = std::env::var("VMM_SOCKET").expect("VMM_SOCKET must point at the broker");
    let client = BrokerClient::new(&socket);
    let pods = PodBroker::new(client.clone());
    let rootfs = BrokerRootfs::new(client);

    let config = PodConfigWire {
        cpus: 2,
        memory_in_bytes: 1024 * 1024 * 1024,
        hostname: Some("live-pod".to_string()),
        boot_log: Some("/tmp/rkv/live-pod-boot.log".to_string()),
        ..PodConfigWire::new("live-pod")
    };
    pods.create_pod(&config).await.expect("createPod");
    pods.create("live-pod").await.expect("boot a real pod");
    eprintln!("pod created; the VM is up and the sandbox is running");

    // An init container: run to completion, then a long-lived one alongside.
    let image = "registry.k8s.io/e2e-test-images/busybox:1.29-2";
    for (id, args) in [
        (
            "init",
            vec!["/bin/sh".to_string(), "-c".into(), "true".into()],
        ),
        (
            "app",
            vec!["/bin/sh".to_string(), "-c".into(), "sleep 300".into()],
        ),
    ] {
        let block = rootfs.provision(image, id).await.expect("provisionRootfs");
        let mut container = ContainerConfigWire::new(id, block);
        container.args = args;
        pods.add_container("live-pod", &container)
            .await
            .expect("addContainer");
        pods.start_container("live-pod", id)
            .await
            .expect("startContainer");
        eprintln!("started {id}");
        if id == "init" {
            let code = pods
                .wait_container("live-pod", "init")
                .await
                .expect("waitContainer");
            assert_eq!(code, 0, "the init container must succeed");
            eprintln!("init exited {code}");
        }
    }

    assert!(pods
        .list_containers("live-pod")
        .await
        .expect("listContainers")
        .contains(&"app".to_string()));

    let stopped = pods.stop_pod("live-pod").await;
    eprintln!("stop: {stopped:?}");
    stopped.expect("stop the pod");
}

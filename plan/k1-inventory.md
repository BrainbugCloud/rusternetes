# K1 inventory — bollard call sites → CRI replacement

Generated for [plan/02-kubelet-cri-only.md](02-kubelet-cri-only.md) K1.
Every bollard reference in `crates/kubelet/` mapped to its CRI disposition.
Line numbers refer to the tree at the time of the K1 commit.

Conventions used by the CRI rewrite:

- Sandbox/containers carry labels (`crates/kubelet/src/cri.rs::labels`):
  `io.kubernetes.pod.name`, `io.kubernetes.pod.namespace`,
  `io.kubernetes.pod.uid`, `io.kubernetes.container.name`, plus
  `io.rusternetes.container.type` (`init`/`ephemeral`/`regular`) and the
  restart-count annotation `io.rusternetes.container.restart-count`.
  Today identity is name-prefix only (`{pod}_pause`, `{pod}_{container}`);
  labels replace all name-prefix filters.
- `CriClient` (new module `crates/kubelet/src/cri.rs`) wraps
  `RuntimeServiceClient`/`ImageServiceClient` over a lazy UDS channel.
- containerID prefix comes from CRI `Version().runtime_name`
  (`containerd://{id}`), cached in `ContainerRuntime.runtime_name`.

## runtime.rs

| Line(s) | bollard call | Purpose | CRI replacement |
|---|---|---|---|
| 189 | `Docker::connect_with_local_defaults` | client construction | `CriClient::new(runtime_endpoint, image_endpoint)` (done in K1) |
| 453 | `inspect_image` | `check_image_exists` | `ImageStatus(image)` → `Some(_)` |
| 492 | `create_image` (stream) | `pull_image_with_retry` | `PullImage(image)`; keep `docker.io/library` normalization |
| 546-583 | `list_containers` + `remove_container` | remove old exited containers before start | `ListContainers(label: pod.name, state != RUNNING)` → `RemoveContainer` |
| 891-901 | `remove_container` | init-container retry recreate | `RemoveContainer(container_id)` from label lookup |
| 1193-1568 | `start_pause_container` (inspect/create/start/list/stop/remove) | pause container owns netns/IPC, ports, DNS, sysctls, hostname; IP from `network_settings` | `RunPodSandbox(PodSandboxConfig{metadata, hostname, log_directory, dns_config, port_mappings, labels, linux.sysctls, linux.security_context.namespace_options})`; IP from `PodSandboxStatus.network.ip`. Conflict/retry dance deleted — CRI ids are unique; stale sandboxes removed via `ListPodSandbox` + `StopPodSandbox`/`RemovePodSandbox` |
| 1620-1623 | `inspect_container` | init completion polling | `ContainerStatus(id).state == EXITED && exit_code == 0` |
| 3360-3410 | `inspect_container`/`remove_container` | start_container idempotency pre-check | `find_container(pod, name)` + `ContainerStatus`; remove exited via `RemoveContainer` |
| 4549, 4707, 4739 | `inspect_image` | dead umask-wrapper code (`if false`) | deleted |
| 4776-4842 | `create_container` (+409 retry) | app container creation | `CreateContainer(sandbox_id, ContainerConfig, sandbox_config)`; 409-name-conflict retry deleted (runtime-assigned ids) |
| 4846-4858 | `start_container` | start | `StartContainer(id)` |
| 4860-4895 | `create_exec`/`start_exec` | rewrite /etc/hosts post-start | kubelet writes the hosts file on the host; bind-mounted via `ContainerConfig.mounts` (runtime honors mounts; no post-start rewrite needed). Fallback: `ExecSync` |
| 4946-4970 | `list_containers`/`stop_container`/`remove_container` | `stop_and_remove_pod` | `ListContainers(label)` → `StopContainer(0)`/`RemoveContainer`; then `StopPodSandbox` + `RemovePodSandbox` |
| 4994-5045 | `list_containers`/`stop_container` | `stop_pod_with_grace_period` | `StopContainer(id, grace)` per app container, then `StopPodSandbox` last; containers kept for logs |
| 5079-5088 | `list_volumes`/`remove_volume` | emptyDir named-volume cleanup | deleted — volumes are kubelet-side host dirs only |
| 5097-5122 | `inspect_container` | `is_container_running`/`container_exists` | `find_container` + `ContainerStatus.state == RUNNING` |
| 5124-5186 | `inspect_container`/`list_containers` | `has_terminated_containers`/`is_pod_running` | `containers_for_pod(pod)` states |
| 5189-5228 | `list_containers` | `has_any_app_container` | `containers_for_pod` minus init/ephemeral labels |
| 5231-5560 | `inspect_container` per init container | `get_init_container_statuses` | `find_container` + `ContainerStatus`; restart count from `io.rusternetes.container.restart-count` annotation |
| 5561-5634 | `inspect_container` | `compute_init_container_actions` | `ContainerStatus.state`/`exit_code` |
| 5635-5734 | `inspect_container` | `get_ephemeral_container_statuses` | same via labels |
| 5735-6034 | `inspect_container` | `get_container_statuses` | `ContainerStatus`: `state`, `exit_code`, `started_at`/`finished_at` (nanos → RFC3339), `image_ref`; `docker://` → `{runtime_name}://`; restart count from annotation (monotonic guard kept) |
| 6035-6192 | `inspect_container` | probe gating (`initial_delay`, startup) | `ContainerStatus.started_at` |
| 6260-6331 | `download_from_container` | termination-message fallback (tar) | deleted — host bind-mount termination file is the only source (kubelet-owned path); CRI has no download API |
| 6333-6395 | `docker.logs` (tail) | `FallbackToLogsOnError` termination message | read CRI log file (`/var/log/pods/...`) via `cri-server::logfmt::read_log_file` with `tail_lines` |
| 6397-6428 | `inspect_container` | `get_effective_container_ip` | `PodSandboxStatus.network.ip` via sandbox lookup |
| 6545-6560 | `create_exec`/`start_exec`/`inspect_exec` | exec probe | `ExecSync(id, cmd, timeout)` exit code |
| 6668-6705 | `create_exec`/`start_exec`/`inspect_exec` | lifecycle exec handler (postStart/preStop) | `ExecSync(id, cmd, 30s)` |
| 6998-7180 | `list_containers`/`stop_container` | `stop_pod_for` (preStop + graceful stop) | `containers_for_pod` → hooks via `ExecSync` → `StopContainer(remaining_grace)` → `StopPodSandbox` last |
| 7201-7207 | `inspect_container` | `get_container_exit_code` | `ContainerStatus.exit_code` |
| 7210-7232 | `inspect_container`/`remove_container` | `remove_terminated_container` | `ContainerStatus.state == EXITED` → `RemoveContainer` |
| 7396-7443 | `list_containers`/`inspect_container` | `get_pod_ip` | `sandboxes_for_pod` → `PodSandboxStatus.network.ip` |
| 7698-7723 | `update_container` | in-place resize | `UpdateContainerResources(LinuxContainerResources)` |
| 7733-7752 | `list_containers` | `list_running_pods` | `ListContainers(state=RUNNING)` group by pod label |
| 7763-7916 | `list_containers`/`stop_container`/`remove_container` | `garbage_collect_containers` | `ListContainers` + `ListPodSandbox` by labels; exited via `RemoveContainer`; orphan sandboxes via `StopPodSandbox`+`RemovePodSandbox` |
| 7921-7944 | `list_containers` | `list_all_pods` | `ListPodSandbox` pod-name labels |
| 7948-7966 | `inspect_container` (pause) | `get_container_age` | `PodSandboxStatus.created_at` |
| 7977-8086 | `list_containers` + `docker.stats` | `collect_node_metrics` | `ListContainerStats`: `cpu.usage_core_nano_seconds` (rate over cached prev sample), `memory.working_set_bytes` |

## main.rs

| Line(s) | bollard call | Purpose | CRI replacement |
|---|---|---|---|
| 270-328 | `Docker::connect_with_local_defaults`, `create_exec`, `start_exec`, `inspect_exec` | `POST /exec/:container_id` ad-hoc endpoint | deleted (K5). Kubelet serves `/exec/{ns}/{pod}/{container}` by calling CRI `Exec` and proxying the runtime's SPDY streaming URL; api-server translates websocket ⇄ SPDY |

## eviction.rs

| Line(s) | bollard call | Purpose | CRI replacement |
|---|---|---|---|
| 594-657 | `Docker::connect_with_local_defaults` | own Docker connection for pod stats | share the runtime's `CriClient` (no second connection) |
| 660-715 | `list_containers` + `docker.stats` | per-container memory (`memory_stats.usage`) + disk (`blkio` bytes) | `ListContainerStats(filter: pod labels)`: memory = `memory.working_set_bytes`; disk = `writable_layer.used_bytes` (+ `ImageFsInfo` for node-level). CRI has no blkio-throughput equivalent — disk-pressure signal is re-based on filesystem usage (weaker; documented at the call site) |

## Notes / per-site design decisions

- **`download_from_container`** (single use, termination-message fallback for
  pre-bind-mount containers): removed outright. The kubelet has written the
  termination file to a host path bind-mount since before this migration; the
  tar-download path only served containers created by older kubelets.
- **Docker named volumes** (`rusternetes-emptydir-*`): the create path never
  used them (host dirs only); the cleanup path is deleted with no replacement.
- **CNI**: on the CRI path the kubelet no longer drives CNI; sandbox
  networking (and the pod IP) is the runtime's job. `cni/` stays in-tree,
  unused, removal is a follow-up.
- **gRPC health probe**: not bollard, but the tonic 0.12 → 0.14 bump moved
  `ProstCodec` to `tonic-prost` (done in K1).

# apple-cri

A Kubernetes CRI server over **Apple's `container` runtime** (apple/container),
so a kubelet can run Linux workloads natively on macOS — no Docker Desktop, no
Lima VM, no Linux node.

Built on the reusable `cri-server` harness: this crate is a backend implementing
`RuntimeBackend`, `ImageBackend` and `streaming::StreamingBackend`, and nothing
else. It depends only on `cri-proto` / `cri-server` — never on the kubelet or
other `rusternetes-*` crates (the same rule `bollard-cri` follows).

```bash
container system start                 # Apple's runtime daemon
make build-fast ARGS="-p apple-cri --bin apple-cri"
./target/release-fast/apple-cri --cri-listen unix:///var/run/apple-cri.sock

crictl -r unix:///var/run/apple-cri.sock info
bash scripts/apple-cri-critest.sh      # the conformance harness
```

## The architectural mismatch, stated plainly

**Kubernetes' sandbox is a pod. Apple's sandbox is a container.**

Apple's runtime gives every container **its own microVM** — an intentional
isolation choice, and the opposite of what a pod needs. Kubernetes expects the
containers of a pod to share a network namespace (and optionally IPC and PID), so
they reach each other over `localhost` behind one pod IP.

Kata Containers solves this by making the sandbox *be* the VM: `RunPodSandbox`
boots one VM per pod, and `kata-agent` inside the guest then creates each
container as namespaced processes *within* that VM. Apple's `vminitd` is the
structural analogue of `kata-agent` — Swift init, first process in the guest, a
gRPC API over vsock — but its job is mounting the rootfs and launching *the*
process. It does not create per-container namespaces inside the VM, and the CLI
exposes no way to share a netns (`--network` takes a *network* name, never a
container) or to attach a second image's rootfs to a running VM.

So a Kata-shaped pod is reachable on this stack only *below* the CLI, by
extending `vminitd` or running our own agent in a pod VM via the
`Containerization` Swift package. That is the real fix, and it is not what this
crate does. This crate takes the achievable path — one VM per container, with the
consequences documented below — and keeps the pod model behind
`RuntimeBackend` so the Kata-shaped implementation can replace it later.

### What this means for a sandbox

There is **no infra ("pause") container**. It could not hold namespaces for
anything to join, it would cost a whole VM per pod, and it would own an IP that
no app container listens on — so traffic to that "pod IP" would go nowhere.
Instead a sandbox is *a checkpoint record plus a shared Apple network*, and the
pod IP is the **primary (earliest-created running) container's** address. A single
flat network shared by all pods is also closer to the Kubernetes network model
(every pod routable, no NAT between pods) than a per-pod network would be.

## What the runtime does *not* tell us, and how that is handled

`container inspect` reports a container's `status` and nothing else about its
history — **no timestamps and no exit code at all** — and it reports a
never-started container and an exited one identically (`stopped`). CRI requires
`created_at`/`started_at`/`finished_at`/`exit_code`, and the kubelet drives
restart policy off the exit code. Three mechanisms close that gap:

| Need | Mechanism |
|---|---|
| timestamps, labels, annotations, CREATED vs EXITED | the shim's own checkpoint store (`state.rs`), authoritative and restart-safe |
| exit code (live) | `container start --attach` **is** the start call; its own exit status is the container's (`logs.rs`) |
| exit code (after a shim restart) | parsed from the guest's `vminitd.log`: `id=<id> status=<n> [vminitd] managed process exit` |

The attach supervisor is safe to rely on: killing it with SIGKILL leaves the
container `running` — it is a pure observer, not the container's parent.

Per-stream logs need the same trick. Apple's own `stdio.log` **interleaves stdout
and stderr onto one stream, unordered** (a program writing stderr then stdout
lands in the file in the opposite order), and `container logs` has the same
limitation — neither can produce CRI log records with correct stream tags.
`start --attach` delivers the two as separate pipes, so that is the live path.

`Attach` subscribes to that same supervisor rather than opening a second stream,
which keeps stdout and stderr apart and — the part that actually matters — gives
both streams a real EOF when the container exits. Without that EOF the CRI
streaming protocol never closes the session and `Attach` hangs forever.

Three behaviours worth recording, all measured against 0.7.1 and all
counterintuitive:

- **Signals to the CLI are proxied into the guest — SIGTERM only.** SIGTERM to
  `container exec` kills the executed process inside the container; **SIGKILL
  kills only the CLI and orphans the guest process forever.** So the ExecSync
  timeout path sends SIGTERM and `kill_on_drop` is deliberately *off* for exec.
  (This is what critest's "timeout exec process should be gone" checks.)
- **A tty plus stdin needs a real PTY.** `container exec --tty --interactive` and
  `container start --attach --interactive` on a tty container both reject a pipe
  on stdin (`internalError: "the provided fd is not a pty"`). A tty *without*
  stdin is fine on pipes, and stdin *without* a tty is fine on pipes — only the
  combination needs `openpty(3)`, and with it input and output share the master
  fd, so there is no separate stderr (exactly CRI's tty contract).
- **Container ids must be short.** Apple uses `--name` *as* the id and embeds it
  in a length-limited system name. Past ~64 characters `start --attach` fails
  with `internalError: … NSPOSIXErrorDomain Code=22 "Invalid argument"` — 64
  works, 66 does not, with a 12-character `$HOME`, and the budget shrinks as the
  home path grows. A cri-dockerd-style `k8s_<ctr>_<pod>_<ns>_<uid>_<attempt>`
  name exceeds 230 characters for a real pod, so **every** container would fail
  to start. Ids here are therefore opaque 32-hex strings, and identity lives in
  the checkpoint store plus a few mirrored labels.

Port-forward is *structurally* simpler than on Linux: every container has a real
vmnet address, so `dial_in_sandbox` is a plain `TcpStream::connect` — no
`setns(CLONE_NEWNET)`, no privileged syscall. (That `setns` call is exactly what
stops `bollard-cri` compiling for Darwin.) Whether it *works* depends on the
host — see the next section.

Stats need no cAdvisor equivalent: `container stats --format json` reports the
guest's own cgroup accounting, so the shim never reads `/proc` or cgroupfs —
neither of which exists on macOS.

## Host↔container connectivity (an environment gate, not a shim limitation)

Container-to-container traffic works, and so does everything riding Apple's own
control plane (`exec`, `logs`, `attach`, stats). Traffic **from macOS into a
container** is a separate matter, and on some hosts it does not work at all.
Measured here (macOS 26.5.1, `container` 0.7.1):

| Path | Result |
|---|---|
| container → container, by IP | works (HTTP 200 from nginx) |
| host → container IP | `No route to host`, with an **incomplete ARP entry** on `bridge101` — even though the host holds `192.168.64.1/24` on that same bridge and pings its own gateway fine |
| host → `--publish`ed port on `127.0.0.1` | the proxy **accepts** the TCP connection, then **resets** it |

Neither path is fixed by restarting the runtime, and neither is caused by the
`vmenet` interface leak that accumulates across container churn (68 interfaces
survived a `container system stop` here). The likely cause is host policy —
macOS's Local Network privacy gate and/or the enabled application firewall —
rather than anything this shim controls. The fix is granting the connecting
process Local Network access and allowing `container` through the firewall; both
are user actions in System Settings.

`dial_in_sandbox` therefore tries the container IP first and falls back to a
published host port when the sandbox declares one, so port-forward works wherever
either path is open. The three critest specs that require host→container
connectivity are skipped by default and can be re-enabled with
`INCLUDE_NETWORK=1`.

## Deviations from CRI

Every item is a runtime limitation, not a shortcut. Where a request cannot be
honoured it is logged at create time rather than silently dropped.

| Area | Deviation | Cause |
|---|---|---|
| **pod networking** | containers in a multi-container pod get **distinct IPs and no shared `localhost`** | one VM per container; no netns sharing in the CLI |
| **pod IP** | `PodSandboxStatus.network.ip` is empty between `RunPodSandbox` and the first container start | the address does not exist until a VM boots |
| **hostname** | the guest hostname is the container id, not the pod name | `container create` has no `--hostname` |
| **PID / IPC** | never shared across a pod | separate VMs |
| **`UpdateContainerResources`** | returns `Unimplemented`; the new limits are recorded and apply at the next start | a booted microVM's vCPU count and memory size are fixed |
| **memory limits** | limits below 128 MiB are clamped upward (logged) | a VM needs headroom to boot a kernel |
| **CPU limits** | `cpu_quota/cpu_period` is rounded **up** to whole vCPUs | Apple sizes VMs in whole vCPUs |
| **sysctls, capabilities, seccomp, AppArmor/SELinux, read-only rootfs, devices, supplemental groups** | not applied | the *CLI* exposes no flags for them, though the guest OCI spec supports several — a native XPC transport could close this |
| **image-volume mounts** (KEP-4639) | skipped with a warning | cannot mount an image as a volume |
| **pull credentials** | applied via `container registry login`, which is **global and keychain-backed** | the CLI has no per-pull auth; token/basic-auth `AuthConfig` returns `Unimplemented` |
| **`Attach` / `Exec`** | no TTY resize | no CLI surface for it |
| **logs and `Attach` after a shim restart** | relayed via `container logs --follow`, so stdout and stderr arrive **merged as stdout** | `start --attach` cannot re-attach to an already-running container |
| **container stats** | no RSS, page faults, or separate working-set figure | not reported by `container stats` |

## critest coverage

Run the harness; it builds the shim, starts it on a throwaway socket and state
directory, and tears down every `k8s_*` object it created:

```bash
bash scripts/apple-cri-critest.sh              # the supported set
bash scripts/apple-cri-critest.sh --all        # no skips, warts and all
FOCUS='Streaming' bash scripts/apple-cri-critest.sh
INCLUDE_NETWORK=1 bash scripts/apple-cri-critest.sh   # also the host->container specs
KEEP=1 bash scripts/apple-cri-critest.sh       # leave the shim up to poke at
GINKGO_TIMEOUT=5m bash scripts/apple-cri-critest.sh   # surface hangs quickly
```

critest **1.36.0** runs natively on darwin/arm64, and its own build already
self-skips the Linux-only suites (hostNetwork, sysctls, seccomp/AppArmor/SELinux,
capabilities, OOM, `NamespaceOption`, devices) — 5 of its 59 specs. Of the 54 it
does offer here, the harness skips these:

| Skipped spec | Why |
|---|---|
| `runtime should support set hostname` | no `--hostname` flag exists |
| `should return the same image identifier when pulled from different registries` | needs the same image in two registries |
| `removing image from one registry should remove all tags from other registries` | same |
| `runtime should support portforward` | needs host→container connectivity (see above); re-enable with `INCLUDE_NETWORK=1` |
| `runtime should support port mapping` (both specs) | same |

The last three are *environment*-gated, not runtime-gated: they are expected to
pass on a host where macOS will route into a container.

See `STATUS.md` for the current pass count.

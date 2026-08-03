# Replacing the guest: kata-agent instead of LinuxPod + vminitd

**Status:** spike step 1 **passes**, 2026-08-03. `GetGuestDetails` answers over
the relay from a real Kata guest booted under Virtualization.framework. One
specific blocker remains before step 2 — see §7.

The proposal is a *guest* replacement, not a hypervisor one. Virtualization.framework
stays, the broker stays, `ImageService` and `DirectoryUnpacker` stay, and the
share-mutation mechanism stays. What goes is `LinuxPod` + `vminitd` — the layer
that owns every divergence recorded in `crates/apple-cri/STATUS.md`.

Reference: `kata-containers` v4.0.0, cloned to `../kata-containers`
(sparse: `src/libs/protocols`, `src/libs/kata-types`, `src/agent/src`,
`src/runtime-rs`, `tools/packaging`).

---

## 1. The two kill criteria, both answered

### 1.1 ttrpc-rust on darwin — **works**

`ttrpc = "0.9.0"` (the version Kata pins, `Cargo.toml:219`) builds on
darwin/arm64 with the `async` feature, and its client connects over a unix
socket. Verified by compiling and running, not by reading:

```rust
ttrpc::asynchronous::Client::connect("unix:///…/agent.sock").await  // Ok
```

Only ttrpc's *vsock* transport is Linux-only, and we do not need it: the guest
side is Linux (the agent), and the host side connects to a **unix socket the
broker relays to the guest vsock**. That is exactly what `VsockRelay.swift`
already produces — "dial the guest in Swift, splice to a unix socket, Rust
consumes it once."

### 1.2 VZ booting Kata's arm64 kernel — **same format we already boot**

Kata installs two artifacts per build (`tools/packaging/kernel/build-kernel.sh:600-613`):
`vmlinuz-<ver>` is `Image.gz`, and on arm64 `vmlinux-<ver>` is
`arch/arm64/boot/Image` — **uncompressed**. The kernel VZ boots today is the same
thing:

```
vmlinux-6.12.28-153: Linux kernel ARM64 boot executable Image, little-endian, 4K pages
offset 0x38: ARMd            # arm64 Image magic
```

So `VZLinuxBootLoader` takes Kata's `vmlinux-*` unchanged. The initrd
(`kata-containers-initrd.img`, gzipped cpio) goes to `initialRamdiskURL`, which is
the standard Linux path — no block device and therefore no hotplug needed to
boot, which sidesteps the constraint that shaped the current design.

### 1.3 Two facts that fell out, both favourable

**The agent listens on vsock port 1024** (`DEFAULT_AGENT_VSOCK_PORT`,
`kata-types/src/config/default.rs:26`) — the *same port* `vminitd` uses.
`VsockRelay.swift` and the broker's `dial` need no change at all.

**Stdio rides the ttrpc connection**, not separate vsock ports. runtime-rs pumps
it through `read_stdout` / `read_stderr` / `write_stdin` RPCs
(`virt_container/src/container_manager/io/container_io.rs`). One relay socket
carries everything. This is a bigger simplification than it looks — see §3.

---

## 2. What the API buys, item by item

`SandboxContext` is a *process* API we translate CRI onto. `agent.proto` is
already a *pod* API, so the translation collapses rather than moving.

| Open item | Closed by |
|---|---|
| shared IPC / UTS dropped (STATUS.md divergence table) | the agent builds sandbox namespaces; `CreateSandboxRequest.sandbox_pidns` (agent.proto:317) gives shared PID too |
| no `RemoveContainer` — guest state lives until the pod stops | `RemoveContainer` RPC |
| no node/pod accounting | `StatsContainer`, `GetMetrics` — real cgroup stats, in-guest |
| no eviction signal | `GetOOMEvent` |
| `UpdateContainerResources` unimplemented | `UpdateContainer` resizes cgroups within the VM's boot ceiling — most of KEP-1287 without needing VZ memory resize |
| host-path mounts / configmaps / secrets | `CopyFile` sidesteps the share entirely for small files |
| `Memory`-medium `emptyDir` | the agent creates the tmpfs in-guest |
| pod IP plumbing | `UpdateInterface` / `UpdateRoutes` / `ListInterfaces` — host synthesises endpoints, agent configures the guest netns |
| rootfs metadata fidelity (uid/gid, device nodes) | **not closed** — see §5 |

**The critest self-skips are the underrated part.** Five specs currently
self-skip on darwin as Linux-only — namespaces, capabilities, sysctls,
seccomp/AppArmor/SELinux, OOM, devices. With a real rustjail-managed container in
the guest those stop being architecturally unreachable and become work items. The
current 54/54 is 54 *of 59*; that denominator can grow.

---

## 3. What changes in this tree

### Stays, unchanged

- **VZ VM lifecycle** — `VZVirtualMachineInstance` via `VZVirtualMachineManager`.
- **`VsockRelay.swift`** — same port, same shape.
- **`ImageService` + `DirectoryUnpacker`** — Kata's default `shared_fs` is
  `virtio-fs` (`DEFAULT_SHARED_FS_TYPE`, `kata-types`), and its
  `disable_block_device_use` model passes container rootfs over a shared
  directory. That is the same trade `DirectoryUnpacker` already makes, for the
  same reason.
- **`NetworkService`** — vmnet subnet discovery is independent of the guest.
- **The share-mutation mechanism** — mutating the live `VZMultipleDirectoryShare`
  is still how a rootfs reaches a running VM.

### Changes shape

- **`VZHotplugProvider.swift`** — the *mechanism* survives verbatim; the
  `HotplugProvider` conformance and `VZHotplugInstaller` do not. Those exist only
  because `LinuxPod` calls `vm.hotplug`. Without LinuxPod the broker calls its own
  `addShare(tag:source:)` directly, and `/run/rk-virtiofs` disappears — the agent
  mounts what we declare in `CreateContainerRequest.storages` with driver
  `virtio-fs` (`DRIVER_VIRTIOFS_TYPE`, `kata-types/src/device.rs:37`), so
  guest-side mounting becomes protocol rather than a private mountpoint we
  negotiate around.
- **`apple-cri/src/pod_runtime.rs`** — shrinks. It currently translates CRI onto
  a process API; it would translate onto a pod API that already has the verbs.

### Deleted

- **`PodService.swift`** — the LinuxPod calls, ~420 lines. The single biggest
  casualty and the point of the exercise.
- **`Streaming.swift`** and **`LogWriter.swift`** — stdio arrives in Rust over
  ttrpc, so the unix-socket fan-out, `ContainerStdio`, `TeeWriter` and the CRI log
  writer all move to Rust, next to the CRI layer that consumes them. **This
  deletes the class of bug that dominated this session**: `stdin_once`, the attach
  teardown chain, and the fan-out subscriber leak were all consequences of stdio
  living in Swift behind a protocol that could not express CRI's semantics.

### New

- **A `kata-agent-client` crate** — `agent.proto` through `ttrpc-codegen 0.6`,
  plus a thin client. Mechanical.
- **Host-side sandbox orchestration** in `apple-cri` — the call sequence ported
  from `runtime-rs/crates/runtimes/virt_container/src/sandbox.rs` and
  `container_manager/`. Take the *sequence*, not Kata's host crates: they lean on
  `nix`/cgroups and will fight darwin the way containerd-shim-wasm did.
- **OCI spec construction, in Rust, again.** `CreateContainerRequest.OCI` is a
  full OCI spec (agent.proto:90). Retiring the Rust LinuxPod port deleted
  `crates/apple-containerization/src/oci.rs`; this brings it back. Worth saying
  plainly. The difference is that it is now a data structure we serialise, not a
  runtime we drive — the thing that made the old port unmaintainable was
  replicating `LinuxPod`'s *behaviour*, not its types.
- **A guest image pipeline** — pinned aarch64 kernel + initrd, built or fetched,
  CI'd. See §5.

---

## 4. Variants

**(a) Keep Containerization below LinuxPod.** `VZVirtualMachineInstance` already
gives VM lifecycle, the unified virtiofs device, vsock dial and boot log. Stop
calling `LinuxPod`; boot Kata's initrd and speak ttrpc from Rust.

**(b) Full port** — implement VZ behind Kata's own hypervisor trait in runtime-rs.
More work, no additional capability today.

**Do (a).** It reuses more of this tree, and (b) can still follow.

---

## 5. Costs — state these before committing

1. **You own a guest image forever.** Kernel + initrd for aarch64, pinned,
   patched, CI'd. Kata ships arm64 artifacts and build tooling, but the
   maintenance is yours. This is the largest new cost, and it is precisely what
   `PodService.swift` was written to avoid taking on.
2. **An upstream swap, not an upstream removal.** You stop receiving Apple's
   `vminitd` fixes and start tracking Kata's cadence. Probably favourable —
   Apache-2.0 Rust with a public release process versus an experimental Swift
   primitive — but it is a trade, not a win.
3. **Mixed-runtime pods do not return for free.** `CreateContainerRequest` has no
   `ociRuntimePath` equivalent; the agent creates containers with its embedded
   rustjail. That is a patch to Apache-2.0 Rust that is plausibly upstreamable
   rather than a fork of Apple's Swift package, so the price drops from "no" to
   "maybe later" — not to zero.
4. **Rosetta is recoverable but manual** (virtiofs share + `binfmt_misc`
   registration in the guest). Verify before assuming amd64 images keep working.
5. **Rootfs metadata fidelity is unchanged.** Still a host directory over
   virtio-fs, so uid/gid and device nodes are still lost. Kata has the same
   constraint under `disable_block_device_use`; the agent does not fix it.

---

## 6. Spike, in order

1. ~~Boot Kata's aarch64 initrd, dial 1024, get `GetGuestDetails` to answer over
   the relay.~~ **Done — see §7.**
2. `CreateSandbox` + two `CreateContainer`s over the live virtiofs share, then
   assert what `LinuxPod` cannot: same IPC namespace, same UTS namespace, then
   `RemoveContainer`. **Blocked on §7.2.**
3. Re-run critest. The bar is 54/54; anything less is a regression, and the
   Linux-only self-skips become the new frontier.

---

## 7. Step 1 result

Run it with `rusternetes-vmm --kata-probe` (see `KataProbe.swift`) plus the
host-side ttrpc client. Artifacts: `kata-static-4.0.0-arm64.tar.zst`,
`vmlinux-6.18.35-200` + `kata-ubuntu-noble.initrd`.

### 7.1 What worked

**Kata's arm64 kernel boots under VZ**, unmodified, from kernel + initrd. The
boot loader is replaced through `VZInstanceExtension.configureVZ`, which runs
after `toVZ` on the finished configuration — so Containerization's hardcoded
`VZLinuxBootLoader` and its insistence on a block rootfs are both worked around
without a fork.

**The agent serves ttrpc over the relay.** `GetGuestDetails` returned:

```
mem block size:   134217728 bytes
agent api:        4.0.0
storage handlers: nvdimm, scsi, ephemeral, image_guest_pull,
                  erofs.multi-layer, overlayfs, blk, mmioblk,
                  virtio-fs, watchable-bind, local
```

Three things that were assumptions in §1 are now measurements: the agent listens
on **vsock 1024** so `VsockRelay` needed no change; `ttrpc-codegen 0.6` generates
from `agent.proto` on darwin (it needs an `async-trait` dependency, or the
generated server trait fails object-safety); and **`virtio-fs` is in the agent's
storage handlers**, which is the rootfs delivery model §3 depends on.

### 7.2 The blocker: the agent cannot run as PID 1 yet

`GetGuestDetails` above was answered by an agent running as a **child** process.
As PID 1 — which is how a real deployment boots it — the agent fails during early
init and the VM disappears.

The failure is invisible by construction, which is worth knowing before anyone
else debugs it:

* `main()` calls `reboot(RB_POWER_OFF)` on any `real_main` error when
  `init_mode` (`src/agent/src/main.rs:354`), so the guest vanishes rather than
  reporting.
* The early logger writes into a pipe that `create_logger_task` only starts
  draining at `main.rs:231` — *after* `general_mount` and `init_agent_as_init`.
  Anything that fails before that is written and then lost.
* The agent logs to stdout by default and PID 1 has no stdout, so the boot log
  ends at `Run /init as init process` and says nothing.

Replaying `init_agent_as_init`'s steps by hand in the guest (`cgroup2` mount,
`/dev/ptmx` remove + symlink, `setsid`, `/etc/hostname`) shows **all of them
succeeding**, so the fault is elsewhere in that path — likely `general_mount` or
`cgroups_mount`. Narrowing it needs either a debug build of the agent or an
incremental bisect against a clean guest; the diagnostic wrapper used here
pre-mounts filesystems and so contaminates the test.

This is a tractable bug, not a new kill criterion — but it is unresolved, and
step 2 cannot start until the agent survives its own init.

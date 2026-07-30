# apple-cri status

Host: macOS 26.5.1, Apple silicon (arm64), `container` CLI **0.7.1**,
critest/crictl **1.36.0** (native darwin/arm64 builds).

## critest — 2026-07-30

```bash
bash scripts/apple-cri-critest.sh
```

```
Ran 48 of 59 Specs in 199.456 seconds
SUCCESS! -- 48 Passed | 0 Failed | 0 Pending | 11 Skipped
```

**48/48 of the supported set pass.** Of critest's 59 specs, 5 self-skip on
darwin (the Linux-only suites: hostNetwork, sysctls, seccomp/AppArmor/SELinux,
capabilities, OOM, `NamespaceOption`, devices) and the harness skips 6 more —
each justified in `README.md`:

| Skipped | Reason | Kind |
|---|---|---|
| `runtime should support set hostname` | `container create` has no `--hostname` | runtime |
| `…image identifier when pulled from different registries` | needs one image in two registries | fixture |
| `removing image from one registry should remove all tags from other registries` | same | fixture |
| `runtime should support portforward` | needs host→container connectivity | **environment** |
| `runtime should support port mapping` ×2 | same | **environment** |

The last three are expected to pass on a host where macOS routes into a
container; re-enable with `INCLUDE_NETWORK=1`. On this host neither the direct
vmnet address nor `--publish`'s own proxy is reachable from macOS — see
README "Host↔container connectivity" for the measurements.

Suites fully green: Runtime info (2), PodSandbox (4), Container runtime (18,
incl. volumes, logs, `ReopenContainerLog`, execSync + timeout, stats),
Streaming (exec tty/non-tty, attach), Networking (DNS config), Image Manager
(11), Image Consistency (3), Image Identifier Consistency (1), Idempotence (7).

## Unit tests

```
cargo test -p apple-cri
test result: ok. 52 passed; 0 failed
```

`cargo clippy -p apple-cri --all-targets -- -D warnings` and
`cargo fmt --all -- --check` are clean.

## Bugs this shim had to solve (each found by a failing spec)

| Symptom | Cause |
|---|---|
| every container reported EXITED right after start | container ids over ~64 chars make `start --attach` fail with `EINVAL`; the cri-dockerd-style name is ~230 chars |
| `StartContainer` failed for critest's idempotence specs | an empty CRI `log_path` was treated as a fatal open error |
| a timed-out `ExecSync` left its process running in the guest | SIGKILL to `container exec` orphans the guest process; only SIGTERM is proxied |
| exec with `tty=true, stdin=true` failed | the CLI requires a real PTY on stdin for that combination |
| `Attach` hung for the full suite timeout | attach output came from a second `container logs --follow` that never EOF'd; and the fan-out `Sender` held in the relay handle kept the channel open, so an explicit `Eof` was needed |
| attach saw nothing for `echo -n hello` | the stdio pump was line-oriented and blocked on a newline that never came |
| container create failed for port-mapped sandboxes | `host_port = 0` means "expose only"; Apple rejects `--publish 0:80` |
| one image with 3 tags reported as 3 images | CRI reports one `Image` per id with all its tags; Apple lists one row per reference |
| `RemoveImage` left the image resolvable | only tags were untagged, not a real `name@digest` store entry |
| `Username` reported as `www-data:group` | the group must be split off the OCI `User` field |

## Not yet done

- The Kata-shaped pod model (one VM per *pod*, containers as namespaced
  processes inside it) — the only way to get shared `localhost`, a single pod IP,
  and shared IPC/PID. Needs a transport below the CLI (`Containerization` +
  `vminitd`), which is why the CLI is isolated behind `cli.rs`.
- End-to-end rusternetes-on-macOS bring-up: this crate is the CRI half; the
  kubelet also needs a macOS story for kube-proxy (iptables) and for
  `Memory`-medium `emptyDir` (`mount -t tmpfs`).
- `apple-cri` is not yet wired into CI; the harness requires a macOS runner with
  Apple's runtime installed.

// SPDX-License-Identifier: Apache-2.0

//! The `container(1)` transport.
//!
//! Every call this shim makes into Apple's runtime goes through [`Cli`]. That
//! is deliberate: the CLI is a *lossy* funnel over `container-apiserver`'s XPC
//! API (the guest OCI spec plumbs sysctls, capabilities, seccomp and a
//! read-only rootfs that `container run` has no flags for), so the day this
//! shim grows a native XPC or `Containerization`-framework transport, only this
//! module changes. Nothing above it spawns a process.
//!
//! Measured CLI round-trip on an M-series host (0.7.1): `list ~21 ms`,
//! `inspect ~17 ms`, `exec true ~81 ms` — fast enough for the kubelet's
//! per-second relist and for exec probes.
//!
//! ## Error mapping
//!
//! The CLI reports failures on stderr as `Error: <kind>: "<message>"` with
//! kinds `notFound`, `invalidState`, `invalidArgument`, … — parsed by
//! [`classify`] into CRI-conventional errors. Two quirks matter:
//! `inspect` answers a missing object with an empty JSON array and exit 0,
//! while `stop`/`kill` of a missing container are already silent no-ops.

use std::collections::BTreeMap;
use std::process::{ExitStatus, Stdio};

use cri_server::error::{Error, Result};
use serde::de::DeserializeOwned;
use tokio::process::{Child, Command};

use crate::model::*;

/// Wrapper around the `container` binary.
#[derive(Debug, Clone)]
pub struct Cli {
    binary: String,
}

/// Captured output of a completed CLI invocation.
#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Output {
    pub fn stdout_str(&self) -> String {
        String::from_utf8_lossy(&self.stdout).to_string()
    }
    pub fn stderr_str(&self) -> String {
        String::from_utf8_lossy(&self.stderr).to_string()
    }
}

/// Whether some captured output is the CLI reporting a failure, rather than a
/// container's own stderr. The CLI's format is `Error: <kind>: "<message>"`.
pub fn looks_like_cli_error(text: &str) -> bool {
    text.contains("notFound:")
        || text.contains("invalidState:")
        || text.contains("invalidArgument:")
        || text.contains("internalError:")
}

/// Map a CLI failure onto a CRI-conventional error, keyed on the
/// `Error: <kind>:` prefix the CLI prints to stderr.
pub fn classify(context: &str, stderr: &str) -> Error {
    let msg = stderr.trim();
    let detail = if msg.is_empty() { "no output" } else { msg };
    if msg.contains("notFound:") {
        Error::NotFound(format!("{context}: {detail}"))
    } else if msg.contains("invalidArgument:") {
        Error::InvalidArgument(format!("{context}: {detail}"))
    } else if msg.contains("invalidState:") {
        // e.g. "container X is not running" — a precondition, not a bad request.
        Error::FailedPrecondition(format!("{context}: {detail}"))
    } else {
        Error::Internal(format!("{context}: {detail}"))
    }
}

impl Cli {
    pub fn new(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(&self.binary);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // The CLI colours/animates progress output when it thinks it has a
        // terminal; keep it machine-readable regardless of how the shim was
        // started.
        cmd.env("NO_COLOR", "1");
        cmd.kill_on_drop(true);
        cmd
    }

    /// Run to completion, capturing output; does not inspect the exit status.
    pub async fn raw(&self, args: &[&str]) -> Result<Output> {
        tracing::trace!(args = ?args, "container CLI");
        let out = self.command(args).output().await.map_err(|e| {
            Error::Unavailable(format!("spawning `{} {:?}`: {e}", self.binary, args))
        })?;
        Ok(Output {
            status: out.status,
            stdout: out.stdout,
            stderr: out.stderr,
        })
    }

    /// Run to completion, failing on a non-zero exit status.
    pub async fn checked(&self, context: &str, args: &[&str]) -> Result<Output> {
        let out = self.raw(args).await?;
        if !out.status.success() {
            return Err(classify(context, &out.stderr_str()));
        }
        Ok(out)
    }

    /// Run and deserialize stdout as JSON.
    pub async fn json<T: DeserializeOwned>(&self, context: &str, args: &[&str]) -> Result<T> {
        let out = self.checked(context, args).await?;
        serde_json::from_slice(&out.stdout).map_err(|e| {
            Error::Internal(format!(
                "{context}: parsing `container {:?}` JSON: {e}: {}",
                args,
                String::from_utf8_lossy(&out.stdout)
                    .chars()
                    .take(400)
                    .collect::<String>()
            ))
        })
    }

    // ---- system ----------------------------------------------------------

    /// `container --version` → the CLI's version banner.
    pub async fn version(&self) -> Result<String> {
        let out = self.checked("version", &["--version"]).await?;
        Ok(out.stdout_str().trim().to_string())
    }

    /// Whether `container-apiserver` answers. Used for `RuntimeReady`.
    pub async fn apiserver_ready(&self) -> bool {
        // `system status` exits non-zero when the daemon is not running.
        matches!(self.raw(&["system", "status"]).await, Ok(o) if o.status.success())
    }

    /// `container system start` — idempotent ("Verifying apiserver is running").
    pub async fn system_start(&self) -> Result<()> {
        self.checked("system start", &["system", "start"]).await?;
        Ok(())
    }

    // ---- containers ------------------------------------------------------

    /// `container list --all --format json`.
    pub async fn list_containers(&self) -> Result<Vec<ContainerJson>> {
        self.json("list containers", &["list", "--all", "--format", "json"])
            .await
    }

    /// `container inspect <id>`; `Ok(None)` when absent (the CLI answers `[]`).
    pub async fn inspect_container(&self, id: &str) -> Result<Option<ContainerJson>> {
        let out = self.raw(&["inspect", id]).await?;
        if !out.status.success() {
            let err = classify("inspect", &out.stderr_str());
            // A missing container is not an error at this layer.
            return if matches!(err, Error::NotFound(_)) {
                Ok(None)
            } else {
                Err(err)
            };
        }
        let list: Vec<ContainerJson> = serde_json::from_slice(&out.stdout)
            .map_err(|e| Error::Internal(format!("inspect {id}: parsing JSON: {e}")))?;
        Ok(list.into_iter().next())
    }

    /// `container create …` → the container id (Apple uses the name as id).
    pub async fn create_container(&self, spec: &CreateSpec) -> Result<String> {
        // Guard the name-length ceiling here rather than letting it surface much
        // later as `container start --attach` failing with a bare EINVAL (see
        // [`crate::naming::MAX_CONTAINER_ID_LEN`]).
        if spec.name.len() > crate::naming::MAX_CONTAINER_ID_LEN {
            return Err(Error::InvalidArgument(format!(
                "container id {:?} is {} chars; Apple's runtime cannot attach to \
                 an id longer than {} (see crate::naming)",
                spec.name,
                spec.name.len(),
                crate::naming::MAX_CONTAINER_ID_LEN,
            )));
        }
        let args = spec.to_args();
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = self.checked("create container", &argv).await?;
        let id = out.stdout_str().trim().to_string();
        Ok(if id.is_empty() { spec.name.clone() } else { id })
    }

    /// Spawn `container start --attach [--interactive] <id>`.
    ///
    /// The attach process is a pure *observer*: killing it leaves the
    /// container running (verified against 0.7.1), and its exit status is the
    /// container's exit code — the only way to obtain it live, since
    /// `container inspect` reports no exit code at all.
    pub fn spawn_start_attached(
        &self,
        id: &str,
        tty: bool,
        stdin: bool,
    ) -> Result<(Child, Option<std::fs::File>)> {
        let mut cmd = Command::new(&self.binary);
        cmd.arg("start").arg("--attach");
        let mut master = None;
        if tty && stdin {
            // A tty container with stdin needs a real terminal (see [`Pty`]).
            let pty = open_pty()?;
            let err = |e: std::io::Error| Error::Internal(format!("dup pty slave: {e}"));
            cmd.arg("--interactive")
                .stdin(Stdio::from(pty.slave.try_clone().map_err(err)?))
                .stdout(Stdio::from(pty.slave.try_clone().map_err(err)?))
                .stderr(Stdio::from(pty.slave.try_clone().map_err(err)?));
            master = Some(pty.master);
        } else {
            if stdin {
                cmd.arg("--interactive").stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        cmd.arg(id)
            .env("NO_COLOR", "1")
            // Do NOT kill_on_drop: the relay owns the handle for the
            // container's whole life and must not die with a transient future.
            .kill_on_drop(false);
        let child = cmd
            .spawn()
            .map_err(|e| Error::Unavailable(format!("spawning `container start -a {id}`: {e}")))?;
        Ok((child, master))
    }

    /// `container stop --signal <sig> --time <secs> <id>` — already a no-op
    /// for a missing or stopped container.
    pub async fn stop_container(&self, id: &str, timeout_secs: i64, signal: &str) -> Result<()> {
        let secs = timeout_secs.max(0).to_string();
        self.checked(
            "stop container",
            &["stop", "--signal", signal, "--time", &secs, id],
        )
        .await?;
        Ok(())
    }

    /// `container kill --signal <sig> <id>`.
    pub async fn kill_container(&self, id: &str, signal: &str) -> Result<()> {
        self.checked("kill container", &["kill", "--signal", signal, id])
            .await?;
        Ok(())
    }

    /// `container rm --force <id>`; a missing container is `Ok` (CRI wants
    /// `RemoveContainer` idempotent).
    pub async fn remove_container(&self, id: &str) -> Result<()> {
        let out = self.raw(&["rm", "--force", id]).await?;
        if out.status.success() {
            return Ok(());
        }
        match classify("remove container", &out.stderr_str()) {
            Error::NotFound(_) => Ok(()),
            other => Err(other),
        }
    }

    /// `container exec …` buffered to completion, with an optional deadline.
    ///
    /// On timeout the CLI child is signalled with **SIGTERM**, never SIGKILL.
    /// `container exec` proxies SIGTERM into the guest, so the executed process
    /// actually dies; SIGKILL kills only the CLI and leaves the guest process
    /// running forever (both verified against 0.7.1). That distinction is what
    /// critest's "timeout exec process should be gone" assertion checks — and it
    /// is why [`Self::spawn_exec`] must not use `kill_on_drop`.
    pub async fn exec(
        &self,
        id: &str,
        cmd: &[String],
        tty: bool,
        deadline: Option<std::time::Duration>,
    ) -> Result<Output> {
        let (mut child, _pty) = self.spawn_exec(id, cmd, tty, false)?;
        let pid = child.id();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let out_task = tokio::spawn(read_to_end(stdout));
        let err_task = tokio::spawn(read_to_end(stderr));

        let status = match deadline {
            Some(d) => match tokio::time::timeout(d, child.wait()).await {
                Ok(status) => status,
                Err(_) => {
                    if let Some(pid) = pid {
                        // SAFETY: `pid` is this child's, still unreaped, so the
                        // signal cannot land on an unrelated process.
                        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                    }
                    // Reap before answering, so the guest process is provably
                    // gone by the time the caller sees DeadlineExceeded.
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;
                    return Err(Error::DeadlineExceeded(format!(
                        "exec in {id} exceeded {}s",
                        d.as_secs()
                    )));
                }
            },
            None => child.wait().await,
        };
        let status = status.map_err(|e| Error::Internal(format!("exec in {id}: {e}")))?;
        Ok(Output {
            status,
            stdout: out_task.await.unwrap_or_default(),
            stderr: err_task.await.unwrap_or_default(),
        })
    }

    /// Spawn `container exec [-i] [-t] <id> <cmd…>`.
    ///
    /// A tty *and* stdin means the child gets a real pty (see [`Pty`]); the
    /// returned master carries both directions in that case. Otherwise stdio is
    /// piped as usual.
    pub fn spawn_exec(
        &self,
        id: &str,
        cmd: &[String],
        tty: bool,
        stdin: bool,
    ) -> Result<(Child, Option<std::fs::File>)> {
        let mut c = Command::new(&self.binary);
        c.arg("exec");
        if tty {
            c.arg("--tty");
        }
        let mut master = None;
        if tty && stdin {
            let pty = open_pty()?;
            let err = |e: std::io::Error| Error::Internal(format!("dup pty slave: {e}"));
            c.arg("--interactive")
                .stdin(Stdio::from(pty.slave.try_clone().map_err(err)?))
                .stdout(Stdio::from(pty.slave.try_clone().map_err(err)?))
                .stderr(Stdio::from(pty.slave.try_clone().map_err(err)?));
            master = Some(pty.master);
        } else {
            if stdin {
                c.arg("--interactive").stdin(Stdio::piped());
            } else {
                c.stdin(Stdio::null());
            }
            c.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        c.arg(id)
            .args(cmd)
            .env("NO_COLOR", "1")
            // Deliberately NOT kill_on_drop: that sends SIGKILL, which reaps the
            // CLI but orphans the process inside the guest. Cancellation has to
            // go through SIGTERM instead (see [`Self::exec`]).
            .kill_on_drop(false);
        let child = c
            .spawn()
            .map_err(|e| Error::Unavailable(format!("spawning `container exec {id}`: {e}")))?;
        Ok((child, master))
    }

    /// Spawn `container logs --follow <id>`.
    ///
    /// Used only to re-establish a log relay after a shim restart: the CLI
    /// merges the guest's stdout and stderr onto one stream here, so relayed
    /// lines are tagged `stdout`. The live path ([`Self::spawn_start_attached`])
    /// keeps the streams separate.
    /// `tail` bounds the backlog replayed before following: `Some(0)` streams
    /// only new output (CRI `Attach` semantics), `None` replays everything
    /// (log-relay catch-up after a restart).
    pub fn spawn_logs_follow(&self, id: &str, tail: Option<u32>) -> Result<Child> {
        let mut c = Command::new(&self.binary);
        c.arg("logs").arg("--follow");
        if let Some(n) = tail {
            c.arg("-n").arg(n.to_string());
        }
        c.arg(id)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NO_COLOR", "1")
            .kill_on_drop(false);
        c.spawn()
            .map_err(|e| Error::Unavailable(format!("spawning `container logs -f {id}`: {e}")))
    }

    // ---- networks --------------------------------------------------------

    /// `container network create [--subnet …] [--label …] <name>`.
    pub async fn create_network(
        &self,
        name: &str,
        subnet: Option<&str>,
        labels: &BTreeMap<String, String>,
    ) -> Result<()> {
        let mut args: Vec<String> = vec!["network".into(), "create".into()];
        if let Some(s) = subnet {
            args.push("--subnet".into());
            args.push(s.to_string());
        }
        for (k, v) in labels {
            args.push("--label".into());
            args.push(format!("{k}={}", encode_label_value(v)));
        }
        args.push(name.to_string());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        self.checked("create network", &argv).await?;
        Ok(())
    }

    pub async fn list_networks(&self) -> Result<Vec<NetworkJson>> {
        self.json("list networks", &["network", "list", "--format", "json"])
            .await
    }

    // ---- images ----------------------------------------------------------

    pub async fn list_images(&self) -> Result<Vec<ImageListEntry>> {
        self.json("list images", &["image", "list", "--format", "json"])
            .await
    }

    /// `container image inspect <ref>`; `Ok(None)` when absent.
    pub async fn inspect_image(&self, reference: &str) -> Result<Option<ImageInspect>> {
        let out = self.raw(&["image", "inspect", reference]).await?;
        if !out.status.success() {
            return match classify("inspect image", &out.stderr_str()) {
                Error::NotFound(_) => Ok(None),
                other => Err(other),
            };
        }
        let list: Vec<ImageInspect> = serde_json::from_slice(&out.stdout).map_err(|e| {
            Error::Internal(format!("inspect image {reference}: parsing JSON: {e}"))
        })?;
        Ok(list.into_iter().next())
    }

    /// `container image pull --platform <p> --progress none <ref>`.
    pub async fn pull_image(&self, reference: &str, platform: Option<&str>) -> Result<()> {
        let mut args: Vec<String> = vec!["image".into(), "pull".into()];
        args.push("--progress".into());
        args.push("none".into());
        if let Some(p) = platform {
            args.push("--platform".into());
            args.push(p.to_string());
        }
        args.push(reference.to_string());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        self.checked("pull image", &argv).await?;
        Ok(())
    }

    /// Idempotent image delete.
    ///
    /// CRI requires `RemoveImage` to succeed when the image is already gone,
    /// *including* when a concurrent `RemoveImage` is what removed it — critest's
    /// "should not fail on simultaneous RemoveImage calls" fires five at once.
    /// Apple's delete is not atomic against itself: the loser of that race exits
    /// non-zero with `failed to delete one or more images: ["<ref>"]`, which is
    /// indistinguishable by message from a genuine failure.
    ///
    /// So the outcome is checked rather than the message: if the reference is gone
    /// from the store, the delete achieved what was asked, whoever performed it.
    /// Matching on the error text instead would either mask real failures (an
    /// image still in use) or keep failing this spec.
    pub async fn remove_image(&self, reference: &str) -> Result<()> {
        let out = self.raw(&["image", "delete", reference]).await?;
        if out.status.success() {
            return Ok(());
        }
        let err = classify("remove image", &out.stderr_str());
        if matches!(err, Error::NotFound(_)) {
            return Ok(());
        }
        // Re-read the store: a concurrent delete may have won.
        if let Ok(images) = self.list_images().await {
            if !images.iter().any(|i| i.reference() == reference) {
                return Ok(());
            }
        }
        Err(err)
    }

    /// `container registry login --password-stdin -u <user> <server>`.
    ///
    /// CRI passes credentials per pull, but the CLI only has a *global*,
    /// keychain-backed login — so an authenticated pull mutates host state.
    /// See `README.md` (Deviations).
    pub async fn registry_login(&self, server: &str, username: &str, password: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut child = Command::new(&self.binary)
            .args([
                "registry",
                "login",
                "--password-stdin",
                "-u",
                username,
                server,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Unavailable(format!("spawning `container registry login`: {e}")))?;
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(password.as_bytes()).await;
            let _ = stdin.write_all(b"\n").await;
            let _ = stdin.shutdown().await;
        }
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| Error::Internal(format!("registry login: {e}")))?;
        if !out.status.success() {
            return Err(classify(
                "registry login",
                &String::from_utf8_lossy(&out.stderr),
            ));
        }
        Ok(())
    }
}

/// A pseudo-terminal pair.
///
/// Apple's CLI rejects a pipe on stdin whenever a tty is involved:
/// `container exec --tty --interactive` fails with
/// `internalError: "failed to exec process the provided fd is not a pty"`, and
/// `container start --attach --interactive` on a tty container with
/// `"failed to start container: the provided fd is not a pty"` (both verified on
/// 0.7.1). A tty *without* stdin is fine on pipes, and stdin *without* a tty is
/// fine on pipes — only the combination needs a real terminal.
///
/// With a tty there is one bidirectional stream: input is written to the master
/// and the process's output is read back from it, so there is no separate
/// stderr (which is what CRI expects for a tty exec).
pub struct Pty {
    pub master: std::fs::File,
    slave: std::fs::File,
}

/// Allocate a pty pair via `openpty(3)`.
pub fn open_pty() -> Result<Pty> {
    use std::os::fd::FromRawFd;
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    // SAFETY: both out-params point at live locals; passing null for the name,
    // termios and winsize selects the system defaults.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if rc != 0 {
        return Err(Error::Internal(format!(
            "openpty: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: openpty returned two fresh fds this process owns.
    Ok(Pty {
        master: unsafe { std::fs::File::from_raw_fd(master) },
        slave: unsafe { std::fs::File::from_raw_fd(slave) },
    })
}

/// Drain an optional child pipe to a buffer.
async fn read_to_end<R: tokio::io::AsyncRead + Unpin>(reader: Option<R>) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    if let Some(mut r) = reader {
        let _ = r.read_to_end(&mut buf).await;
    }
    buf
}

/// Arguments for `container create`.
#[derive(Debug, Default, Clone)]
pub struct CreateSpec {
    pub name: String,
    pub image: String,
    /// Overrides the image entrypoint when set.
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub labels: BTreeMap<String, String>,
    pub workdir: Option<String>,
    pub user: Option<String>,
    pub network: Option<String>,
    pub dns_servers: Vec<String>,
    pub dns_searches: Vec<String>,
    pub dns_options: Vec<String>,
    /// `(source, target, readonly)` bind mounts.
    pub binds: Vec<(String, String, bool)>,
    pub tmpfs: Vec<String>,
    /// `[host-ip:]host-port:container-port[/proto]`
    pub publish: Vec<String>,
    pub cpus: Option<i64>,
    pub memory_bytes: Option<i64>,
    pub tty: bool,
    pub stdin: bool,
    pub platform: Option<String>,
}

impl CreateSpec {
    pub fn to_args(&self) -> Vec<String> {
        let mut a: Vec<String> = vec!["create".into(), "--name".into(), self.name.clone()];
        if let Some(e) = &self.entrypoint {
            a.push("--entrypoint".into());
            a.push(e.clone());
        }
        for (k, v) in &self.env {
            a.push("--env".into());
            a.push(format!("{k}={v}"));
        }
        for (k, v) in &self.labels {
            a.push("--label".into());
            a.push(format!("{k}={}", encode_label_value(v)));
        }
        if let Some(w) = &self.workdir {
            a.push("--workdir".into());
            a.push(w.clone());
        }
        if let Some(u) = &self.user {
            a.push("--user".into());
            a.push(u.clone());
        }
        if let Some(n) = &self.network {
            a.push("--network".into());
            a.push(n.clone());
        }
        for s in &self.dns_servers {
            a.push("--dns".into());
            a.push(s.clone());
        }
        for s in &self.dns_searches {
            a.push("--dns-search".into());
            a.push(s.clone());
        }
        for o in &self.dns_options {
            a.push("--dns-option".into());
            a.push(o.clone());
        }
        for (src, dst, ro) in &self.binds {
            // A virtiofs share is directory-only: a single-file source fails with
            // `path '<src>' is not a directory`. That is not a corner case — the
            // kubelet bind-mounts two *files* into every container it creates
            // (`/dev/termination-log` and `/etc/hosts`), so without this every
            // CreateContainer fails and no pod can ever start.
            //
            // `-v` does handle file binds, including `:ro` (measured on 1.2.0:
            // the file is readable in the guest and writes to a `:ro` bind fail
            // with EROFS). It is only used for files; directories keep the
            // virtiofs path, which is what critest exercises.
            //
            // `-v` is colon-delimited, so a path containing `:` is ambiguous.
            // Fall through to `--mount` in that case rather than emit an argument
            // Apple would misparse — for a file that fails loudly, which beats
            // mounting the wrong thing.
            let is_file = std::fs::metadata(src).map(|m| m.is_file()).unwrap_or(false);
            let colon_safe = !src.contains(':') && !dst.contains(':');
            if is_file && colon_safe {
                a.push("-v".into());
                let mut m = format!("{src}:{dst}");
                if *ro {
                    m.push_str(":ro");
                }
                a.push(m);
                continue;
            }
            if is_file && !colon_safe {
                tracing::warn!(
                    source = %src,
                    target = %dst,
                    "file bind mount contains ':' and cannot be expressed with -v"
                );
            }
            // `--mount` takes an explicit readonly flag; `-v` does not.
            a.push("--mount".into());
            let mut m = format!("type=virtiofs,source={src},target={dst}");
            if *ro {
                m.push_str(",readonly");
            }
            a.push(m);
        }
        for t in &self.tmpfs {
            a.push("--tmpfs".into());
            a.push(t.clone());
        }
        for p in &self.publish {
            a.push("--publish".into());
            a.push(p.clone());
        }
        if let Some(c) = self.cpus {
            a.push("--cpus".into());
            a.push(c.to_string());
        }
        if let Some(m) = self.memory_bytes {
            a.push("--memory".into());
            a.push(m.to_string());
        }
        if self.tty {
            a.push("--tty".into());
        }
        if self.stdin {
            a.push("--interactive".into());
        }
        if let Some(p) = &self.platform {
            a.push("--platform".into());
            a.push(p.clone());
        }
        a.push(self.image.clone());
        a.extend(self.args.iter().cloned());
        a
    }
}

/// Percent-encode the one byte Apple's `--label key=value` parser rejects.
///
/// Measured against 0.7.1: values may contain slashes, spaces, commas, colons,
/// JSON braces and quotes, and may be empty — but any `=` in the value is
/// rejected outright (`invalidArgument: "invalid label format …"`). `%` is
/// encoded too so the transform stays reversible.
pub fn encode_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '=' => out.push_str("%3D"),
            c => out.push(c),
        }
    }
    out
}

/// Inverse of [`encode_label_value`].
pub fn decode_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let bytes = v.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            match &v[i..i + 3] {
                "%3D" | "%3d" => {
                    out.push('=');
                    i += 3;
                    continue;
                }
                "%25" => {
                    out.push('%');
                    i += 3;
                    continue;
                }
                _ => {}
            }
        }
        // Push a whole UTF-8 char, not a byte.
        let ch = v[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_values_round_trip() {
        for v in [
            "simple",
            "",
            "/var/log/pods/ns_pod_uid/ctr/0.log",
            r#"{"a":1,"b":["x","y"]}"#,
            "a=b=c",
            "YWJj==",
            "100%=done",
            "a,b c:d.e-f",
            "ünïcøde=ok",
        ] {
            let enc = encode_label_value(v);
            assert!(
                !enc.contains('='),
                "encoded {v:?} still contains '=': {enc}"
            );
            assert_eq!(decode_label_value(&enc), v, "round-trip failed for {v:?}");
        }
    }

    #[test]
    fn decode_leaves_other_percent_escapes_alone() {
        // Only %3D and %25 are ours; a literal "%2F" in a CRI annotation
        // value must survive untouched.
        assert_eq!(decode_label_value("a%2Fb"), "a%2Fb");
        assert_eq!(decode_label_value("trailing%"), "trailing%");
        assert_eq!(decode_label_value("%3"), "%3");
    }

    #[test]
    fn classify_maps_cli_error_kinds() {
        assert!(matches!(
            classify(
                "ctx",
                r#"Error: notFound: "get failed: container x not found""#
            ),
            Error::NotFound(_)
        ));
        assert!(matches!(
            classify(
                "ctx",
                r#"Error: invalidArgument: "invalid label format k=a=b""#
            ),
            Error::InvalidArgument(_)
        ));
        assert!(matches!(
            classify(
                "ctx",
                r#"Error: invalidState: "container x is not running""#
            ),
            Error::FailedPrecondition(_)
        ));
        assert!(matches!(classify("ctx", ""), Error::Internal(_)));
    }

    #[test]
    fn a_file_bind_uses_dash_v_because_virtiofs_is_directory_only() {
        // The kubelet bind-mounts /dev/termination-log and /etc/hosts — both
        // *files* — into every container. A virtiofs share rejects a file source
        // with "is not a directory", so those must go out as `-v`.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("termination-log");
        std::fs::write(&file, b"").unwrap();

        let spec = CreateSpec {
            name: "c".into(),
            image: "img".into(),
            binds: vec![(
                file.to_string_lossy().into_owned(),
                "/dev/termination-log".into(),
                false,
            )],
            ..Default::default()
        };
        let joined = spec.to_args().join(" ");
        assert!(
            joined.contains(&format!("-v {}:/dev/termination-log", file.display())),
            "expected a -v file bind, got: {joined}"
        );
        assert!(
            !joined.contains("type=virtiofs"),
            "a file must not be sent as a virtiofs share: {joined}"
        );
    }

    #[test]
    fn a_readonly_file_bind_gets_the_ro_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hosts");
        std::fs::write(&file, b"127.0.0.1 localhost\n").unwrap();

        let spec = CreateSpec {
            name: "c".into(),
            image: "img".into(),
            binds: vec![(
                file.to_string_lossy().into_owned(),
                "/etc/hosts".into(),
                true,
            )],
            ..Default::default()
        };
        let joined = spec.to_args().join(" ");
        assert!(
            joined.contains(&format!("-v {}:/etc/hosts:ro", file.display())),
            "expected :ro on the -v bind, got: {joined}"
        );
    }

    #[test]
    fn a_directory_bind_still_uses_virtiofs() {
        // Directories keep the proven path; only files changed.
        let dir = tempfile::tempdir().unwrap();
        let spec = CreateSpec {
            name: "c".into(),
            image: "img".into(),
            binds: vec![(
                dir.path().to_string_lossy().into_owned(),
                "/data".into(),
                false,
            )],
            ..Default::default()
        };
        let joined = spec.to_args().join(" ");
        assert!(
            joined.contains(&format!(
                "--mount type=virtiofs,source={},target=/data",
                dir.path().display()
            )),
            "expected a virtiofs share for a directory, got: {joined}"
        );
        assert!(!joined.contains(" -v "), "got: {joined}");
    }

    #[test]
    fn a_file_bind_with_a_colon_falls_back_to_mount_rather_than_misparsing() {
        // `-v` is colon-delimited, so `a:b:c` is ambiguous. Emitting `--mount`
        // instead fails loudly for a file rather than mounting the wrong path.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("odd:name");
        std::fs::write(&file, b"").unwrap();

        let spec = CreateSpec {
            name: "c".into(),
            image: "img".into(),
            binds: vec![(file.to_string_lossy().into_owned(), "/dst".into(), false)],
            ..Default::default()
        };
        let joined = spec.to_args().join(" ");
        assert!(joined.contains("type=virtiofs"), "got: {joined}");
        assert!(!joined.contains(" -v "), "got: {joined}");
    }

    #[test]
    fn create_spec_renders_expected_argv() {
        let spec = CreateSpec {
            name: "k8s_app_web_default_uid_0".into(),
            image: "docker.io/library/alpine:latest".into(),
            entrypoint: Some("/bin/sh".into()),
            args: vec!["-c".into(), "sleep 1".into()],
            env: vec![("FOO".into(), "bar=baz".into())],
            labels: BTreeMap::from([("io.kubernetes.cri.sandbox-id".into(), "sb1".into())]),
            binds: vec![("/host".into(), "/data".into(), true)],
            tmpfs: vec!["/scratch".into()],
            cpus: Some(2),
            memory_bytes: Some(268435456),
            network: Some("k8s-pods".into()),
            tty: true,
            ..Default::default()
        };
        let args = spec.to_args();
        let joined = args.join(" ");
        assert!(joined.starts_with("create --name k8s_app_web_default_uid_0 --entrypoint /bin/sh"));
        // Env values keep their '=' (only *labels* are restricted).
        assert!(args.contains(&"FOO=bar=baz".to_string()));
        assert!(joined.contains("--mount type=virtiofs,source=/host,target=/data,readonly"));
        assert!(joined.contains("--tmpfs /scratch"));
        assert!(joined.contains("--cpus 2 --memory 268435456"));
        assert!(joined.contains("--network k8s-pods"));
        assert!(joined.contains("--tty"));
        // The image and its args come last, in order.
        assert_eq!(
            &args[args.len() - 3..],
            &["docker.io/library/alpine:latest", "-c", "sleep 1"]
        );
    }

    #[test]
    fn create_spec_omits_absent_options() {
        let spec = CreateSpec {
            name: "c".into(),
            image: "alpine".into(),
            ..Default::default()
        };
        assert_eq!(spec.to_args(), vec!["create", "--name", "c", "alpine"]);
    }
}

// SPDX-License-Identifier: Apache-2.0

//! CRI log relay and the container exit-code supervisor.
//!
//! Apple's runtime writes each container's stdio to
//! `~/Library/Application Support/com.apple.container/containers/<id>/stdio.log`,
//! but that file is useless for CRI: it **interleaves stdout and stderr onto one
//! stream, unordered** (verified — a program writing stderr then stdout lands in
//! the file in the opposite order), and CRI log records must be tagged per
//! stream. `container logs` has the same limitation.
//!
//! `container start --attach` does not: it delivers stdout and stderr as two
//! separate pipes *and* exits with the container's own exit status. Since
//! `container inspect` reports no exit code at all, that attach process is the
//! only live source for one — so this shim starts every container through it and
//! keeps it as a supervisor for the container's lifetime. Two properties make
//! that safe, both verified against 0.7.1:
//!
//! - killing the attach process leaves the container `running` (it is a pure
//!   observer, not the container's parent), and
//! - `vminitd` durably logs the exit status to the container's `vminitd.log`
//!   (`id=<id> status=<n> [vminitd] managed process exit`), which
//!   [`exit_status_from_vminitd_log`] reads as a restart-safe fallback.
//!
//! The supervisor's pipes are also the source `Attach` reads, fanned out through
//! [`LogRelays::subscribe`] — one stream of bytes, two consumers.
//!
//! After a shim restart the attach path is unavailable for an
//! already-running container, so [`LogRelays::resume`] falls back to
//! `container logs --follow`, whose merged output is tagged `stdout`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cri_server::error::{Error, Result};
use cri_server::logfmt::{CriLogWriter, LogStream};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStdin};

use crate::cli::Cli;
use crate::state::Store;

/// How much of the attach process's stderr to retain for diagnostics. When
/// `container start --attach` fails (rather than the container exiting), this is
/// the only explanation available — its stderr would otherwise be relayed into
/// the container's log file as if the container had written it.
const DIAG_HEAD_BYTES: usize = 4096;

/// A reopen request carrying an ack channel: `ReopenContainerLog` must not
/// return before the file exists again (critest checks straight after).
type ReopenRequest = tokio::sync::oneshot::Sender<()>;

/// The write side of a container's stdin, as the supervisor holds it: an
/// ordinary pipe, or the pty master for a tty container.
pub enum RelayStdin {
    Pipe(ChildStdin),
    Pty(tokio::fs::File),
}

/// A boxed async reader over a pty master.
type PtyReader = std::pin::Pin<Box<tokio::fs::File>>;

/// One event in a container's live stdio fan-out.
#[derive(Clone, Debug)]
enum OutputEvent {
    Chunk(LogStream, Arc<Vec<u8>>),
    /// The container produced its last byte.
    ///
    /// An explicit sentinel, rather than relying on the channel closing: the
    /// [`RelayHandle`] holds a `Sender` for the container's whole life so that
    /// `Attach` can subscribe at any time, which means the channel never closes
    /// on its own. Without this, an attach client's streams would never reach
    /// EOF and the CRI streaming session would hang forever.
    Eof,
}

/// Live stdio, fanned out from the supervisor to the log file and to any
/// `Attach` client. Bounded: a client that cannot keep up is lagged, never
/// allowed to stall the container's own output.
type OutputTx = tokio::sync::broadcast::Sender<OutputEvent>;

/// Chunks buffered per subscriber before it is considered lagged.
const OUTPUT_FANOUT_CAPACITY: usize = 512;

/// In-flight bytes between the fan-out task and an attach client.
const FANOUT_BUFFER_BYTES: usize = 64 * 1024;

/// Read size for the stdio pumps.
const PUMP_CHUNK_BYTES: usize = 8 * 1024;

struct RelayHandle {
    reopen: tokio::sync::mpsc::UnboundedSender<ReopenRequest>,
    task: tokio::task::JoinHandle<()>,
    /// The supervisor's stdin, when the container was created with `stdin:
    /// true`. `Attach` takes it to feed the container's stdin.
    stdin: Arc<Mutex<Option<RelayStdin>>>,
    /// Live stdio fan-out for `Attach` (see [`LogRelays::subscribe`]).
    output: OutputTx,
}

/// The registry of live log relays, keyed by container id.
pub struct LogRelays {
    inner: Mutex<HashMap<String, RelayHandle>>,
}

impl Default for LogRelays {
    fn default() -> Self {
        Self::new()
    }
}

impl LogRelays {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Start `id` via `container start --attach` and relay its stdio to `path`.
    ///
    /// Returns once the container has left the created state — Apple's
    /// `start` is asynchronous from the attach process's point of view, and
    /// `StartContainer` must not return before the container is actually
    /// running (or has already exited, for a short-lived command).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_attached(
        &self,
        cli: &Cli,
        store: Arc<Store>,
        id: &str,
        path: PathBuf,
        tty: bool,
        stdin: bool,
    ) -> Result<()> {
        pre_create_log_file(id, &path);

        let (mut child, pty) = cli.spawn_start_attached(id, tty, stdin)?;
        // With a pty there is a single bidirectional stream: the container's
        // output is read back from the master, and there is no separate stderr.
        let (stdout, stderr, pty_stdin) = match pty {
            Some(master) => {
                let reader = master
                    .try_clone()
                    .map(|f| Box::pin(tokio::fs::File::from_std(f)) as PtyReader)
                    .map_err(|e| Error::Internal(format!("dup pty master: {e}")))?;
                (
                    None,
                    None,
                    Some((reader, tokio::fs::File::from_std(master))),
                )
            }
            None => (child.stdout.take(), child.stderr.take(), None),
        };
        let (pty_reader, pty_writer) = match pty_stdin {
            Some((r, w)) => (Some(r), Some(w)),
            None => (None, None),
        };
        let stdin_handle = Arc::new(Mutex::new(match pty_writer {
            Some(w) => Some(RelayStdin::Pty(w)),
            None => child.stdin.take().map(RelayStdin::Pipe),
        }));

        let (reopen, reopen_rx) = tokio::sync::mpsc::unbounded_channel();
        // `started` fires as soon as the runtime reports the container out of
        // the created state, or the supervisor exits first.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (output, _) = tokio::sync::broadcast::channel(OUTPUT_FANOUT_CAPACITY);

        let task = tokio::spawn(run_attach_relay(
            cli.clone(),
            store,
            id.to_string(),
            path.clone(),
            child,
            stdout,
            stderr,
            pty_reader,
            reopen_rx,
            started_tx,
            output.clone(),
        ));

        {
            let mut inner = self.inner.lock().expect("log relay lock poisoned");
            if let Some(old) = inner.insert(
                id.to_string(),
                RelayHandle {
                    reopen,
                    task,
                    stdin: stdin_handle,
                    output,
                },
            ) {
                old.task.abort();
            }
        }

        // Bounded: a wedged VM boot must surface as a StartContainer error
        // rather than hanging the kubelet's sync loop.
        match tokio::time::timeout(std::time::Duration::from_secs(60), started_rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(err))) => Err(err),
            // The task died without reporting; surface it rather than
            // pretending the container started.
            Ok(Err(_)) => Err(Error::Internal(format!(
                "starting {id}: supervisor exited before the container started"
            ))),
            Err(_) => Err(Error::Internal(format!(
                "starting {id}: timed out waiting for the container to run"
            ))),
        }
    }

    /// Re-adopt an already-running container's logs after a shim restart.
    pub fn resume(&self, cli: &Cli, store: Arc<Store>, id: &str, path: PathBuf) {
        {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            if inner.get(id).is_some_and(|h| !h.task.is_finished()) {
                return;
            }
        }
        pre_create_log_file(id, &path);
        let child = match cli.spawn_logs_follow(id, None) {
            Ok(child) => child,
            Err(err) => {
                tracing::warn!(container = %id, %err, "cannot resume log relay");
                return;
            }
        };
        let (reopen, reopen_rx) = tokio::sync::mpsc::unbounded_channel();
        let (output, _) = tokio::sync::broadcast::channel(OUTPUT_FANOUT_CAPACITY);
        let task = tokio::spawn(run_follow_relay(
            cli.clone(),
            store,
            id.to_string(),
            path,
            child,
            reopen_rx,
            output.clone(),
        ));
        self.inner.lock().expect("log relay lock poisoned").insert(
            id.to_string(),
            RelayHandle {
                reopen,
                task,
                stdin: Arc::new(Mutex::new(None)),
                output,
            },
        );
    }

    /// Ask a live relay to reopen its log file, waiting until it has;
    /// `false` when no relay is live for `id`.
    pub async fn reopen(&self, id: &str) -> bool {
        let sender = {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            let Some(handle) = inner.get(id) else {
                return false;
            };
            handle.reopen.clone()
        };
        let (ack, done) = tokio::sync::oneshot::channel();
        if sender.send(ack).is_err() {
            return false;
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), done)
            .await
            .is_ok_and(|recv| recv.is_ok())
    }

    /// Take the supervisor's stdin pipe for an `Attach` request. Only one
    /// attacher can hold it at a time.
    pub fn take_stdin(&self, id: &str) -> Option<RelayStdin> {
        let slot = {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            inner.get(id)?.stdin.clone()
        };
        let mut guard = slot.lock().expect("relay stdin lock poisoned");
        guard.take()
    }

    /// Subscribe to a running container's live stdio, as two readers
    /// (`stdout`, `stderr`).
    ///
    /// This is what `Attach` reads. Taking the bytes from the supervisor's own
    /// pipes — rather than starting a second `container logs --follow` — is what
    /// makes attach terminate correctly: when the container exits, the pumps end,
    /// the fan-out senders drop, and both readers hit EOF, which is the signal
    /// the CRI streaming protocol needs to close the session. It also preserves
    /// the stdout/stderr split that `container logs` throws away.
    ///
    /// `None` when no relay is live (the container has already exited), in which
    /// case attach has nothing to stream and completes immediately.
    pub fn subscribe(
        &self,
        id: &str,
    ) -> Option<(tokio::io::DuplexStream, tokio::io::DuplexStream)> {
        let tx = {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            let handle = inner.get(id)?;
            if handle.task.is_finished() {
                return None;
            }
            handle.output.clone()
        };
        let mut rx = tx.subscribe();
        // Subscribe first, then re-check: if the relay finished in between it
        // already sent `Eof` and this subscription would never see one, so the
        // streams are closed immediately instead of hanging.
        {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            match inner.get(id) {
                Some(handle) if handle.task.is_finished() => return None,
                None => return None,
                Some(_) => {}
            }
        }
        let (out_sink, out_reader) = tokio::io::duplex(FANOUT_BUFFER_BYTES);
        let (err_sink, err_reader) = tokio::io::duplex(FANOUT_BUFFER_BYTES);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut out_sink = out_sink;
            let mut err_sink = err_sink;
            loop {
                match rx.recv().await {
                    Ok(OutputEvent::Chunk(LogStream::Stdout, chunk)) => {
                        if out_sink.write_all(&chunk).await.is_err() {
                            break;
                        }
                    }
                    Ok(OutputEvent::Chunk(LogStream::Stderr, chunk)) => {
                        if err_sink.write_all(&chunk).await.is_err() {
                            break;
                        }
                    }
                    Ok(OutputEvent::Eof) => break,
                    // A slow client missed chunks; keep streaming rather than
                    // dropping the attach.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "attach client lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            // Dropping the sinks EOFs the reader halves, ending the attach.
        });
        Some((out_reader, err_reader))
    }

    /// Abort the relay for a removed container.
    pub fn stop(&self, id: &str) {
        let mut inner = self.inner.lock().expect("log relay lock poisoned");
        if let Some(handle) = inner.remove(id) {
            handle.task.abort();
        }
    }
}

/// The CRI log file must exist the moment `StartContainer` returns: critest and
/// the kubelet open it immediately, ahead of the relay's first write.
fn pre_create_log_file(id: &str, path: &Path) {
    if path.as_os_str().is_empty() {
        return;
    }
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(err) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        tracing::warn!(container = %id, path = %path.display(), %err,
                       "cannot pre-create CRI log file");
    }
}

/// A log file shared by the stdout and stderr reader tasks; `reopen` swaps the
/// handle underneath them.
///
/// Optional throughout: CRI permits an empty `log_path` (critest's idempotence
/// specs use one), and a container without a log file must still be supervised
/// for its exit code, so its stdio is drained rather than recorded.
type SharedWriter = Arc<tokio::sync::Mutex<CriLogWriter<tokio::fs::File>>>;

async fn open_writer(path: &Path) -> std::io::Result<CriLogWriter<tokio::fs::File>> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    Ok(CriLogWriter::new(file))
}

/// Pump one pipe into the shared CRI log file, tagging every record `stream`.
///
/// With no writer the pipe is still drained: an unread pipe fills its buffer and
/// would block the container's own writes.
/// Deliberately byte-oriented, not line-oriented.
///
/// A container may write output with no trailing newline and expect a reader to
/// see it immediately — `echo -n hello` is exactly critest's attach probe. Line
/// buffering would block forever waiting for a newline that never comes, and
/// synthesising one would corrupt the bytes. Raw chunks are also what
/// [`CriLogWriter::write`] wants: it already frames a chunk into one full (`F`)
/// record per newline plus a trailing partial (`P`) record.
async fn pump<R: AsyncRead + Unpin>(
    mut reader: R,
    writer: Option<SharedWriter>,
    stream: LogStream,
    head: Option<Arc<Mutex<Vec<u8>>>>,
    output: Option<OutputTx>,
) {
    let mut buf = vec![0u8; PUMP_CHUNK_BYTES];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                let chunk = &buf[..n];
                if let Some(head) = head.as_ref() {
                    let mut diag = head.lock().expect("head buffer lock poisoned");
                    let room = DIAG_HEAD_BYTES.saturating_sub(diag.len());
                    if room > 0 {
                        diag.extend_from_slice(&chunk[..chunk.len().min(room)]);
                    }
                }
                if let Some(output) = output.as_ref() {
                    // Feed any live `Attach` client. No subscribers is the
                    // normal case, so a send error is expected and ignored.
                    let _ = output.send(OutputEvent::Chunk(stream, Arc::new(chunk.to_vec())));
                }
                if let Some(writer) = writer.as_ref() {
                    let mut guard = writer.lock().await;
                    if let Err(err) = guard.write(stream, chunk).await {
                        tracing::error!(%err, "CRI log write failed; stopping pump");
                        return;
                    }
                }
            }
            Err(err) => {
                // A pty master reports EIO when the slave side closes; that is
                // the normal end of a tty stream, not a failure.
                tracing::debug!(%err, "output stream closed");
                return;
            }
        }
    }
}

/// Open the CRI log file for `path`, or `None` when CRI supplied no log path.
async fn open_optional_writer(
    id: &str,
    path: &Path,
) -> std::result::Result<Option<SharedWriter>, std::io::Error> {
    if path.as_os_str().is_empty() {
        tracing::debug!(container = %id, "no CRI log path; stdio will be drained");
        return Ok(None);
    }
    Ok(Some(Arc::new(tokio::sync::Mutex::new(
        open_writer(path).await?,
    ))))
}

#[allow(clippy::too_many_arguments)]
async fn run_attach_relay(
    cli: Cli,
    store: Arc<Store>,
    id: String,
    path: PathBuf,
    mut child: Child,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
    pty: Option<PtyReader>,
    mut reopen_rx: tokio::sync::mpsc::UnboundedReceiver<ReopenRequest>,
    started_tx: tokio::sync::oneshot::Sender<Result<()>>,
    output: OutputTx,
) {
    let writer = match open_optional_writer(&id, &path).await {
        Ok(w) => w,
        Err(err) => {
            tracing::error!(container = %id, path = %path.display(), %err,
                            "cannot open CRI log file");
            let _ = started_tx.send(Err(Error::Io(err)));
            return;
        }
    };

    let stderr_head = Arc::new(Mutex::new(Vec::new()));
    let mut pumps = Vec::new();
    if let Some(out) = stdout {
        pumps.push(tokio::spawn(pump(
            out,
            writer.clone(),
            LogStream::Stdout,
            None,
            Some(output.clone()),
        )));
    }
    if let Some(err) = stderr {
        pumps.push(tokio::spawn(pump(
            err,
            writer.clone(),
            LogStream::Stderr,
            Some(stderr_head.clone()),
            Some(output.clone()),
        )));
    }
    if let Some(master) = pty {
        // A tty container has no demultiplexed stderr; CRI folds console output
        // into stdout.
        pumps.push(tokio::spawn(pump(
            master,
            writer.clone(),
            LogStream::Stdout,
            None,
            Some(output.clone()),
        )));
    }

    // Confirm the container actually left the created state.
    let confirm = tokio::spawn({
        let cli = cli.clone();
        let id = id.clone();
        async move { wait_until_running(&cli, &id).await }
    });

    let mut started_tx = Some(started_tx);
    let mut confirm = Some(confirm);
    let mut confirmed_running = false;
    let exit_status;

    loop {
        tokio::select! {
            Some(ack) = reopen_rx.recv() => {
                match (&writer, open_writer(&path).await) {
                    // No log path: nothing to rotate, but the RPC still
                    // succeeds.
                    (None, _) => { let _ = ack.send(()); }
                    (Some(current), Ok(w)) => {
                        *current.lock().await = w;
                        let _ = ack.send(());
                    }
                    (Some(_), Err(err)) => tracing::error!(container = %id, %err,
                                                "cannot reopen CRI log file"),
                }
            }
            res = async { confirm.as_mut().unwrap().await }, if confirm.is_some() => {
                confirm = None;
                let outcome = match res {
                    Ok(inner) => inner,
                    Err(e) => Err(Error::Internal(format!("start confirm for {id}: {e}"))),
                };
                confirmed_running = outcome.is_ok();
                if let Some(tx) = started_tx.take() {
                    let _ = tx.send(outcome);
                }
            }
            status = child.wait() => {
                exit_status = status;
                break;
            }
        }
    }

    // Drain both pipes before recording the exit so the log file holds every
    // line the container produced.
    for p in pumps {
        let _ = p.await;
    }
    // Tell any attach client that no more output is coming.
    let _ = output.send(OutputEvent::Eof);
    let cli_failed = matches!(&exit_status, Ok(s) if !s.success());
    let diag = String::from_utf8_lossy(
        &stderr_head
            .lock()
            .expect("head buffer lock poisoned")
            .clone(),
    )
    .trim()
    .to_string();

    // `container start --attach` failing is not the container exiting: the
    // container may never have run at all. Report it instead of recording a
    // fabricated exit code (e.g. "attach is currently unsupported on already
    // running containers", which would otherwise look like exit 1).
    if cli_failed && !confirmed_running && crate::cli::looks_like_cli_error(&diag) {
        tracing::error!(container = %id, diagnostic = %diag, "attach failed to start container");
        if let Some(tx) = started_tx.take() {
            let _ = tx.send(Err(crate::cli::classify(&format!("starting {id}"), &diag)));
        }
        return;
    }
    if let Some(tx) = started_tx.take() {
        // The container exited before the confirm poll saw it running — that
        // is a successful start of a short-lived command.
        let _ = tx.send(Ok(()));
    }
    if cli_failed && !diag.is_empty() {
        tracing::warn!(container = %id, diagnostic = %diag, "attach supervisor stderr");
    }

    let code = match exit_status {
        Ok(status) => status.code().unwrap_or_else(|| {
            // The attach process was signalled; fall back to the guest's own
            // record before inventing a code.
            exit_status_from_vminitd_log(&id).unwrap_or(255)
        }),
        Err(err) => {
            tracing::warn!(container = %id, %err, "waiting on attach supervisor");
            exit_status_from_vminitd_log(&id).unwrap_or(255)
        }
    };
    let reason = if code == 0 { "Completed" } else { "Error" };
    if let Err(err) = store.record_exit(&id, code, reason) {
        tracing::warn!(container = %id, %err, "recording container exit");
    }
    tracing::debug!(container = %id, code, "container exited");
}

async fn run_follow_relay(
    _cli: Cli,
    store: Arc<Store>,
    id: String,
    path: PathBuf,
    mut child: Child,
    mut reopen_rx: tokio::sync::mpsc::UnboundedReceiver<ReopenRequest>,
    output: OutputTx,
) {
    let writer = match open_optional_writer(&id, &path).await {
        Ok(w) => w,
        Err(err) => {
            tracing::error!(container = %id, %err, "cannot open CRI log file");
            return;
        }
    };
    let mut pumps = Vec::new();
    if let Some(out) = child.stdout.take() {
        pumps.push(tokio::spawn(pump(
            out,
            writer.clone(),
            LogStream::Stdout,
            None,
            Some(output.clone()),
        )));
    }
    // `container logs` prints its own diagnostics on stderr; those are not
    // container output, so they are not relayed.
    loop {
        tokio::select! {
            Some(ack) = reopen_rx.recv() => {
                match (&writer, open_writer(&path).await) {
                    (None, _) => { let _ = ack.send(()); }
                    (Some(current), Ok(w)) => { *current.lock().await = w; let _ = ack.send(()); }
                    (Some(_), Err(err)) => tracing::error!(container = %id, %err,
                                                "cannot reopen CRI log file"),
                }
            }
            _ = child.wait() => break,
        }
    }
    for p in pumps {
        let _ = p.await;
    }
    // Tell any attach client that no more output is coming.
    let _ = output.send(OutputEvent::Eof);
    // `logs --follow` cannot report the exit code; read the guest's record.
    if let Some(code) = exit_status_from_vminitd_log(&id) {
        let reason = if code == 0 { "Completed" } else { "Error" };
        let _ = store.record_exit(&id, code, reason);
    }
}

/// Poll until the runtime reports `id` running, or it has clearly finished.
async fn wait_until_running(cli: &Cli, id: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(55);
    let mut backoff = std::time::Duration::from_millis(10);
    loop {
        match cli.inspect_container(id).await {
            Ok(Some(c)) if c.is_running() => return Ok(()),
            // A container that already left `created` and is not running has
            // exited — a legitimate start for `true`, `echo`, and friends.
            Ok(Some(_)) => {}
            Ok(None) => return Err(Error::NotFound(format!("container {id} disappeared"))),
            Err(err) => return Err(err),
        }
        if std::time::Instant::now() >= deadline {
            // Not fatal on its own: the supervisor may still be mid-boot.
            return Ok(());
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_millis(250));
    }
}

/// Apple's per-container state directory.
pub fn container_state_dir(id: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    PathBuf::from(home)
        .join("Library/Application Support/com.apple.container/containers")
        .join(id)
}

/// The init process's exit status as `vminitd` recorded it in the guest log.
///
/// The line looks like:
/// ```text
/// 2026-07-30T10:14:12+0000 info vminitd : id=ec3 status=42 [vminitd] managed process exit
/// ```
/// Only the *init* process carries `id=<container id>`; exec'd processes use
/// their own exec id, so they cannot be mistaken for the container's exit.
pub fn exit_status_from_vminitd_log(id: &str) -> Option<i32> {
    let path = container_state_dir(id).join("vminitd.log");
    let text = std::fs::read_to_string(path).ok()?;
    parse_vminitd_exit(&text, id)
}

/// Pure form of [`exit_status_from_vminitd_log`], for tests.
fn parse_vminitd_exit(text: &str, id: &str) -> Option<i32> {
    let want_id = format!("id={id} ");
    text.lines()
        .rev()
        .filter(|l| l.contains("managed process exit") && l.contains(&want_id))
        .find_map(|l| {
            let rest = l.split("status=").nth(1)?;
            let digits: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            digits.parse().ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim tail of a real `vminitd.log` for a container that exited 42.
    const VMINITD_TAIL: &str = "\
2026-07-30T10:14:12+0000 debug vminitd : [vminitd] received SIGCHLD, reaping processes
2026-07-30T10:14:12+0000 debug vminitd : exits=[92: 0] processes=1 [vminitd] checking for exit of managed process
2026-07-30T10:14:12+0000 debug vminitd : count=0 pid=93 status=42 [vminitd] managed process exited
2026-07-30T10:14:12+0000 info vminitd : id=ec3 status=42 [vminitd] managed process exit
2026-07-30T10:14:12+0000 debug vminitd : containerID=ec3 id=ec3 [vminitd] waitProcess
";

    #[test]
    fn parses_exit_status_from_vminitd_log() {
        assert_eq!(parse_vminitd_exit(VMINITD_TAIL, "ec3"), Some(42));
        // A different container's log must not be mined for this id.
        assert_eq!(parse_vminitd_exit(VMINITD_TAIL, "other"), None);
    }

    #[test]
    fn parses_zero_exit() {
        let log = "2026-07-30T10:14:12+0000 info vminitd : id=c1 status=0 [vminitd] managed process exit\n";
        assert_eq!(parse_vminitd_exit(log, "c1"), Some(0));
    }

    #[test]
    fn ignores_exec_process_exits() {
        // An exec'd process reports under its own exec id, not the container's.
        let log = "\
2026-07-30T10:14:12+0000 info vminitd : id=exec-abc status=7 [vminitd] managed process exit
";
        assert_eq!(parse_vminitd_exit(log, "c1"), None);
        assert_eq!(parse_vminitd_exit(log, "exec-abc"), Some(7));
    }

    #[test]
    fn takes_the_last_exit_for_a_restarted_id() {
        let log = "\
2026-07-30T10:14:12+0000 info vminitd : id=c1 status=1 [vminitd] managed process exit
2026-07-30T10:20:00+0000 info vminitd : id=c1 status=0 [vminitd] managed process exit
";
        assert_eq!(parse_vminitd_exit(log, "c1"), Some(0));
    }

    #[test]
    fn missing_or_partial_log_yields_none() {
        assert_eq!(parse_vminitd_exit("", "c1"), None);
        assert_eq!(
            parse_vminitd_exit(
                "info vminitd : id=c1 [vminitd] managed process exit\n",
                "c1"
            ),
            None
        );
    }

    #[tokio::test]
    async fn pump_tags_records_per_stream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        let writer = Arc::new(tokio::sync::Mutex::new(open_writer(&path).await.unwrap()));

        // Two independent pipes, as `container start -a` hands us.
        pump(
            &b"out-one\nout-two\n"[..],
            Some(writer.clone()),
            LogStream::Stdout,
            None,
            None,
        )
        .await;
        pump(
            &b"err-one\n"[..],
            Some(writer.clone()),
            LogStream::Stderr,
            None,
            None,
        )
        .await;
        drop(writer);

        let data = tokio::fs::read(&path).await.unwrap();
        let text = String::from_utf8(data).unwrap();
        let streams: Vec<&str> = text
            .lines()
            .filter_map(|l| l.split_whitespace().nth(1))
            .collect();
        assert_eq!(streams, ["stdout", "stdout", "stderr"]);
        assert!(text.contains("out-one"));
        assert!(text.contains("err-one"));
    }

    #[tokio::test]
    async fn pump_terminates_unterminated_tail() {
        // A final line without a trailing newline must still be recorded —
        // critest's log parser reads whole records.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        let writer = Arc::new(tokio::sync::Mutex::new(open_writer(&path).await.unwrap()));
        pump(
            &b"no-newline"[..],
            Some(writer.clone()),
            LogStream::Stdout,
            None,
            None,
        )
        .await;
        drop(writer);
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(text.contains("no-newline"), "got {text:?}");
    }

    #[tokio::test]
    async fn output_without_a_trailing_newline_reaches_attach_verbatim() {
        // critest's attach probe is `echo -n hello` and asserts the attach
        // stdout equals exactly "hello". A line-buffered pump would block
        // forever on the missing newline, and appending one would corrupt the
        // bytes — so the fan-out must carry raw chunks.
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        pump(&b"hello"[..], None, LogStream::Stdout, None, Some(tx)).await;
        match rx.try_recv().expect("chunk was published") {
            OutputEvent::Chunk(stream, chunk) => {
                assert_eq!(stream, LogStream::Stdout);
                assert_eq!(&*chunk, b"hello", "no newline may be added");
            }
            other => panic!("expected a chunk, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_empty_log_path_yields_no_writer_and_still_drains() {
        // CRI permits an empty log_path (critest's idempotence specs use one).
        // That must not fail StartContainer — the container still needs its
        // exit code supervised, so its stdio is drained instead of recorded.
        assert!(open_optional_writer("c1", Path::new(""))
            .await
            .unwrap()
            .is_none());
        // Draining with no writer must terminate rather than block.
        pump(
            &b"ignored-one\nignored-two\n"[..],
            None,
            LogStream::Stdout,
            None,
            None,
        )
        .await;
    }

    #[test]
    fn state_dir_is_under_the_container_app_root() {
        let dir = container_state_dir("k8s_app_web_default_uid_0");
        assert!(dir.ends_with("containers/k8s_app_web_default_uid_0"));
        assert!(dir.to_string_lossy().contains("com.apple.container"));
    }
}

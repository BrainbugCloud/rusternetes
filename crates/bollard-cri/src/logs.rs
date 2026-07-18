// SPDX-License-Identifier: Apache-2.0

//! CRI log relay (plan 03 B3, design (a)).
//!
//! CRI runtimes must write per-container log files in the CRI format at
//! `{log_directory}/{log_path}`; Docker only writes its own json-file logs.
//! cri-dockerd symlinks the CRI path to Docker's json log and pushes format
//! detection onto the consumer — our kubelet reads CRI format, so instead a
//! tokio task per started container follows `docker logs` (with daemon-side
//! timestamps) and rewrites the stream through
//! [`cri_server::logfmt::CriLogWriter`].
//!
//! - Docker's per-line timestamp prefix is parsed off and becomes the CRI
//!   record time, which makes resuming exact: after a shim restart the relay
//!   restarts from the file's last record time (`since` + line-level dedupe).
//! - `ReopenContainerLog` signals the relay to reopen its file handle at the
//!   same path (kubelet log rotation) — something the symlink design cannot
//!   support at all (cri-dockerd skips that critest spec).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bollard::container::{LogOutput, LogsOptions};
use bollard::Docker;
use chrono::{DateTime, Utc};
use cri_server::logfmt::{CriLogWriter, LogEntry, LogStream};
use futures_util::StreamExt;
use tokio::fs::File;

/// A timestamp prefix longer than this is not a timestamp (RFC3339Nano is
/// ~30 bytes); treat the buffered bytes as content instead of stalling.
const MAX_TIMESTAMP_LEN: usize = 64;

/// Strips the per-line timestamp prefix Docker adds with `timestamps=true`
/// and splits arbitrary stream chunks into `(time, bytes)` segments for
/// [`CriLogWriter`]: segments ending in `\n` become `F` records, unterminated
/// tails become `P` records. Lines at or before `resume_after` are dropped
/// (restart dedupe — `docker logs since` only has second granularity).
struct TimestampStripper {
    resume_after: Option<DateTime<Utc>>,
    at_line_start: bool,
    prefix: Vec<u8>,
    time: DateTime<Utc>,
    skip_line: bool,
}

impl TimestampStripper {
    fn new(resume_after: Option<DateTime<Utc>>) -> Self {
        Self {
            resume_after,
            at_line_start: true,
            prefix: Vec::new(),
            time: DateTime::<Utc>::MIN_UTC,
            skip_line: false,
        }
    }

    fn feed(&mut self, mut chunk: &[u8]) -> Vec<(DateTime<Utc>, Vec<u8>)> {
        let mut out = Vec::new();
        while !chunk.is_empty() {
            if self.at_line_start {
                let Some(space) = chunk.iter().position(|&b| b == b' ') else {
                    self.prefix.extend_from_slice(chunk);
                    if self.prefix.len() > MAX_TIMESTAMP_LEN {
                        // Not a timestamp after all; flush as content.
                        self.begin_line(Utc::now());
                        out.push((self.time, std::mem::take(&mut self.prefix)));
                    }
                    return out;
                };
                self.prefix.extend_from_slice(&chunk[..space]);
                chunk = &chunk[space + 1..];
                let time = std::str::from_utf8(&self.prefix)
                    .ok()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(Utc::now);
                self.prefix.clear();
                self.begin_line(time);
            } else {
                match chunk.iter().position(|&b| b == b'\n') {
                    Some(nl) => {
                        if !self.skip_line {
                            out.push((self.time, chunk[..=nl].to_vec()));
                        }
                        chunk = &chunk[nl + 1..];
                        self.at_line_start = true;
                    }
                    None => {
                        if !self.skip_line {
                            out.push((self.time, chunk.to_vec()));
                        }
                        chunk = &[];
                    }
                }
            }
        }
        out
    }

    fn begin_line(&mut self, time: DateTime<Utc>) {
        self.time = time;
        self.skip_line = self.resume_after.is_some_and(|after| time <= after);
        self.at_line_start = false;
    }
}

/// A reopen request carrying an ack channel: `ReopenContainerLog` must not
/// return before the file exists again at the log path (critest checks
/// immediately after the RPC).
type ReopenRequest = tokio::sync::oneshot::Sender<()>;

/// The registry of live log relays, keyed by full container id.
pub(crate) struct LogRelays {
    inner: Mutex<HashMap<String, RelayHandle>>,
}

struct RelayHandle {
    reopen: tokio::sync::mpsc::UnboundedSender<ReopenRequest>,
    task: tokio::task::JoinHandle<()>,
}

impl LogRelays {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Start a relay for `id` writing to `path`, unless one is already live.
    /// `resume_after` skips records already in the file (restart resume).
    pub(crate) fn start(
        &self,
        docker: Docker,
        id: String,
        path: PathBuf,
        resume_after: Option<DateTime<Utc>>,
    ) {
        let mut inner = self.inner.lock().expect("log relay lock poisoned");
        if let Some(handle) = inner.get(&id) {
            if !handle.task.is_finished() {
                return;
            }
        }
        let (reopen, reopen_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(run_relay(docker, id.clone(), path, resume_after, reopen_rx));
        inner.insert(id, RelayHandle { reopen, task });
    }

    /// Ask a live relay to reopen its log file and wait until it has done
    /// so; false when no relay is live for `id`.
    pub(crate) async fn reopen(&self, id: &str) -> bool {
        let sender = {
            let inner = self.inner.lock().expect("log relay lock poisoned");
            let Some(handle) = inner.get(id) else {
                return false;
            };
            handle.reopen.clone()
        };
        let (ack, done) = tokio::sync::oneshot::channel();
        if sender.send(ack).is_err() {
            return false; // The relay task already ended.
        }
        // The relay acks right after recreating the file; the timeout only
        // guards against a wedged Docker log stream.
        tokio::time::timeout(std::time::Duration::from_secs(10), done)
            .await
            .is_ok_and(|recv| recv.is_ok())
    }

    /// Abort the relay for a removed container.
    pub(crate) fn stop(&self, id: &str) {
        let mut inner = self.inner.lock().expect("log relay lock poisoned");
        if let Some(handle) = inner.remove(id) {
            handle.task.abort();
        }
    }
}

/// Time of the last intact record in an existing CRI log file (the resume
/// point after a shim restart), read from the file's tail.
pub(crate) async fn last_logged_time(path: &Path) -> Option<DateTime<Utc>> {
    const TAIL_BYTES: u64 = 64 * 1024;
    let mut file = File::open(path).await.ok()?;
    let len = file.metadata().await.ok()?.len();
    if len > TAIL_BYTES {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::End(-(TAIL_BYTES as i64)))
            .await
            .ok()?;
    }
    let mut data = Vec::new();
    use tokio::io::AsyncReadExt;
    file.read_to_end(&mut data).await.ok()?;
    data.split(|&b| b == b'\n')
        .rev()
        .filter(|line| !line.is_empty())
        .find_map(|line| LogEntry::parse(line).ok())
        .map(|entry| entry.time)
}

async fn open_log_file(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn run_relay(
    docker: Docker,
    id: String,
    path: PathBuf,
    resume_after: Option<DateTime<Utc>>,
    mut reopen_rx: tokio::sync::mpsc::UnboundedReceiver<ReopenRequest>,
) {
    let file = match open_log_file(&path).await {
        Ok(file) => file,
        Err(err) => {
            tracing::error!(container = %id, path = %path.display(), %err, "cannot open CRI log file");
            return;
        }
    };
    let mut writer = CriLogWriter::new(file);
    let mut stdout = TimestampStripper::new(resume_after);
    let mut stderr = TimestampStripper::new(resume_after);

    let mut stream = docker.logs(
        &id,
        Some(LogsOptions::<String> {
            follow: true,
            stdout: true,
            stderr: true,
            timestamps: true,
            // Second granularity only; the strippers dedupe exactly.
            since: resume_after.map(|t| t.timestamp()).unwrap_or_default(),
            tail: "all".to_string(),
            ..Default::default()
        }),
    );

    loop {
        tokio::select! {
            Some(ack) = reopen_rx.recv() => {
                // Kubelet log rotation moved the file away; recreate it,
                // then ack so ReopenContainerLog can return.
                match open_log_file(&path).await {
                    Ok(file) => {
                        writer = CriLogWriter::new(file);
                        let _ = ack.send(());
                    }
                    Err(err) => {
                        tracing::error!(container = %id, path = %path.display(), %err, "cannot reopen CRI log file");
                    }
                }
            }
            chunk = stream.next() => {
                let (stream_kind, message) = match chunk {
                    Some(Ok(LogOutput::StdOut { message })) => (LogStream::Stdout, message),
                    // Console = tty container output; CRI folds it into stdout.
                    Some(Ok(LogOutput::Console { message })) => (LogStream::Stdout, message),
                    Some(Ok(LogOutput::StdErr { message })) => (LogStream::Stderr, message),
                    Some(Ok(LogOutput::StdIn { .. })) => continue,
                    Some(Err(err)) => {
                        tracing::warn!(container = %id, %err, "log stream error; stopping relay");
                        return;
                    }
                    // Stream end: the container exited and everything is
                    // written; the stale registry entry is replaced on the
                    // next start.
                    None => return,
                };
                let stripper = match stream_kind {
                    LogStream::Stdout => &mut stdout,
                    LogStream::Stderr => &mut stderr,
                };
                for (time, segment) in stripper.feed(&message) {
                    if let Err(err) = writer.write_at(stream_kind, &segment, time).await {
                        tracing::error!(container = %id, path = %path.display(), %err, "CRI log write failed; stopping relay");
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn docker_line(secs: i64, content: &str) -> Vec<u8> {
        format!(
            "{} {content}",
            ts(secs).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        )
        .into_bytes()
    }

    #[test]
    fn strips_timestamps_and_splits_lines() {
        let mut stripper = TimestampStripper::new(None);
        let mut chunk = docker_line(0, "hello\n");
        chunk.extend_from_slice(&docker_line(1, "wor"));
        let segments = stripper.feed(&chunk);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], (ts(0), b"hello\n".to_vec()));
        assert_eq!(segments[1], (ts(1), b"wor".to_vec()));

        // The rest of the second line arrives in a later chunk.
        let segments = stripper.feed(b"ld\n");
        assert_eq!(segments, vec![(ts(1), b"ld\n".to_vec())]);
    }

    #[test]
    fn timestamp_split_across_chunks() {
        let mut stripper = TimestampStripper::new(None);
        let line = docker_line(0, "split\n");
        let (a, b) = line.split_at(10); // mid-timestamp
        assert!(stripper.feed(a).is_empty());
        assert_eq!(stripper.feed(b), vec![(ts(0), b"split\n".to_vec())]);
    }

    #[test]
    fn resume_dedupe_skips_old_lines() {
        let mut stripper = TimestampStripper::new(Some(ts(1)));
        let mut chunk = docker_line(0, "old\n");
        chunk.extend_from_slice(&docker_line(1, "boundary\n"));
        chunk.extend_from_slice(&docker_line(2, "new\n"));
        let segments = stripper.feed(&chunk);
        assert_eq!(segments, vec![(ts(2), b"new\n".to_vec())]);
    }

    #[test]
    fn skip_state_spans_chunks() {
        let mut stripper = TimestampStripper::new(Some(ts(5)));
        assert!(stripper.feed(&docker_line(0, "old start ")).is_empty());
        // Continuation and terminator of the skipped line, then a fresh one.
        let mut chunk = b"old end\n".to_vec();
        chunk.extend_from_slice(&docker_line(6, "fresh\n"));
        assert_eq!(stripper.feed(&chunk), vec![(ts(6), b"fresh\n".to_vec())]);
    }

    #[test]
    fn oversized_prefix_is_flushed_as_content() {
        let mut stripper = TimestampStripper::new(None);
        let blob = vec![b'x'; MAX_TIMESTAMP_LEN + 10];
        let segments = stripper.feed(&blob);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].1, blob);
    }

    #[tokio::test]
    async fn relay_segments_render_as_cri_records() {
        // End-to-end through CriLogWriter: docker chunks → CRI file bytes.
        let mut stripper = TimestampStripper::new(None);
        let mut writer = CriLogWriter::new(Vec::new());
        let mut chunk = docker_line(0, "hello World\n");
        chunk.extend_from_slice(&docker_line(1, "partial"));
        for (time, segment) in stripper.feed(&chunk) {
            writer
                .write_at(LogStream::Stdout, &segment, time)
                .await
                .unwrap();
        }
        let lines = cri_server::logfmt::reassemble(
            &writer.into_inner(),
            &cri_server::logfmt::ReadOptions::default(),
        );
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].message, b"hello World");
        assert!(!lines[0].truncated);
        assert_eq!(lines[1].message, b"partial");
        assert!(lines[1].truncated);
    }

    #[tokio::test]
    async fn last_logged_time_finds_tail_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.log");
        let mut writer = CriLogWriter::new(File::create(&path).await.unwrap());
        writer
            .write_at(LogStream::Stdout, b"one\ntwo\n", ts(7))
            .await
            .unwrap();
        assert_eq!(last_logged_time(&path).await, Some(ts(7)));
        assert_eq!(last_logged_time(&dir.path().join("nope")).await, None);
    }
}

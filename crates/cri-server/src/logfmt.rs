// SPDX-License-Identifier: Apache-2.0

//! The CRI container log format.
//!
//! CRI runtimes write per-container log files that the kubelet reads and
//! serves. One record per line:
//!
//! ```text
//! 2026-07-17T10:00:00.123456789Z stdout F full line
//! 2026-07-17T10:00:00.123456789Z stdout P partial…
//! ```
//!
//! `F` terminates a logical line; `P` records accumulate until the next `F`
//! on the same stream. [`CriLogWriter`] relays runtime-native output into
//! this format; [`CriLogReader`] parses it back with since/tail filtering.

use std::path::Path;

use chrono::{DateTime, SecondsFormat, Utc};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

impl LogStream {
    fn as_str(self) -> &'static str {
        match self {
            LogStream::Stdout => "stdout",
            LogStream::Stderr => "stderr",
        }
    }
}

impl std::str::FromStr for LogStream {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "stdout" => Ok(LogStream::Stdout),
            "stderr" => Ok(LogStream::Stderr),
            other => Err(Error::Internal(format!("unknown log stream {other:?}"))),
        }
    }
}

/// One parsed CRI log record (a single file line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub time: DateTime<Utc>,
    pub stream: LogStream,
    /// `false` = `F` (line complete), `true` = `P` (partial).
    pub partial: bool,
    pub message: Vec<u8>,
}

impl LogEntry {
    /// Parse one file line (without the trailing `\n`).
    pub fn parse(line: &[u8]) -> Result<Self> {
        let bad = |what: &str| Error::Internal(format!("malformed CRI log line ({what})"));
        let mut fields = line.splitn(4, |&b| b == b' ');
        let time = fields.next().ok_or_else(|| bad("missing timestamp"))?;
        let stream = fields.next().ok_or_else(|| bad("missing stream"))?;
        let tag = fields.next().ok_or_else(|| bad("missing tag"))?;
        let message = fields.next().unwrap_or_default();

        let time = std::str::from_utf8(time)
            .ok()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .ok_or_else(|| bad("bad timestamp"))?
            .with_timezone(&Utc);
        let stream: LogStream = std::str::from_utf8(stream)
            .map_err(|_| bad("bad stream"))?
            .parse()?;
        // The tag may carry sub-tags (`P:extra`); only the first byte matters.
        let partial = match tag.first() {
            Some(b'F') => false,
            Some(b'P') => true,
            _ => return Err(bad("bad tag")),
        };
        Ok(LogEntry {
            time,
            stream,
            partial,
            message: message.to_vec(),
        })
    }

    /// Render as one file line (with trailing `\n`).
    pub fn render(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.message.len() + 48);
        out.extend_from_slice(
            self.time
                .to_rfc3339_opts(SecondsFormat::Nanos, true)
                .as_bytes(),
        );
        out.push(b' ');
        out.extend_from_slice(self.stream.as_str().as_bytes());
        out.extend_from_slice(if self.partial { b" P " } else { b" F " });
        out.extend_from_slice(&self.message);
        out.push(b'\n');
        out
    }
}

/// Writes runtime-native output chunks as CRI log records.
///
/// Feed arbitrary chunks via [`write`](Self::write); complete lines become
/// `F` records and a chunk's unterminated tail is flushed immediately as a
/// `P` record, so `crictl logs`/`kubectl logs` see output promptly.
pub struct CriLogWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> CriLogWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Write one chunk of raw output from `stream`.
    pub async fn write(&mut self, stream: LogStream, chunk: &[u8]) -> Result<()> {
        self.write_at(stream, chunk, Utc::now()).await
    }

    /// Like [`write`](Self::write) with an explicit timestamp (for tests and
    /// replays).
    pub async fn write_at(
        &mut self,
        stream: LogStream,
        chunk: &[u8],
        time: DateTime<Utc>,
    ) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(chunk.len() + 64);
        let mut rest = chunk;
        while let Some(pos) = rest.iter().position(|&b| b == b'\n') {
            let (line, tail) = rest.split_at(pos);
            buf.extend_from_slice(
                &LogEntry {
                    time,
                    stream,
                    partial: false,
                    message: line.to_vec(),
                }
                .render(),
            );
            rest = &tail[1..];
        }
        if !rest.is_empty() {
            buf.extend_from_slice(
                &LogEntry {
                    time,
                    stream,
                    partial: true,
                    message: rest.to_vec(),
                }
                .render(),
            );
        }
        self.inner.write_all(&buf).await?;
        self.inner.flush().await?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// Filtering options for [`read_log_file`].
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Only records at or after this time.
    pub since: Option<DateTime<Utc>>,
    /// Only the last N *logical* lines (after partial-line reassembly).
    pub tail_lines: Option<usize>,
}

/// A reassembled logical log line (one or more `P` records + an `F` record).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// Timestamp of the first record of the line.
    pub time: DateTime<Utc>,
    pub stream: LogStream,
    pub message: Vec<u8>,
    /// True when the file ended before the line's `F` record.
    pub truncated: bool,
}

/// Read and reassemble a CRI log file.
///
/// Malformed lines are skipped (matching kubelet behavior of tolerating
/// partially-written records after a crash).
pub async fn read_log_file(path: impl AsRef<Path>, opts: &ReadOptions) -> Result<Vec<LogLine>> {
    let data = tokio::fs::read(path).await?;
    Ok(reassemble(&data, opts))
}

/// Reassemble raw CRI log file contents into logical lines.
pub fn reassemble(data: &[u8], opts: &ReadOptions) -> Vec<LogLine> {
    let mut lines: Vec<LogLine> = Vec::new();
    // Per-stream accumulation of partial records.
    let mut pending: [Option<LogLine>; 2] = [None, None];
    let idx = |s: LogStream| match s {
        LogStream::Stdout => 0,
        LogStream::Stderr => 1,
    };

    for raw in data.split(|&b| b == b'\n') {
        if raw.is_empty() {
            continue;
        }
        let Ok(entry) = LogEntry::parse(raw) else {
            continue;
        };
        let slot = &mut pending[idx(entry.stream)];
        match slot {
            Some(acc) => acc.message.extend_from_slice(&entry.message),
            None => {
                *slot = Some(LogLine {
                    time: entry.time,
                    stream: entry.stream,
                    message: entry.message,
                    truncated: false,
                });
            }
        }
        if !entry.partial {
            lines.push(slot.take().expect("slot was just filled"));
        }
    }
    for slot in pending.into_iter().flatten() {
        let mut line = slot;
        line.truncated = true;
        lines.push(line);
    }
    // File order is append order, but a trailing unterminated line sorts last
    // already; records are chronological per CRI semantics.

    if let Some(since) = opts.since {
        lines.retain(|l| l.time >= since);
    }
    if let Some(tail) = opts.tail_lines {
        if lines.len() > tail {
            lines.drain(..lines.len() - tail);
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 123_456_789)
            .unwrap()
    }

    #[tokio::test]
    async fn writer_reader_round_trip_with_partials() {
        let mut writer = CriLogWriter::new(Vec::new());
        writer
            .write_at(LogStream::Stdout, b"hello\nwor", t(0))
            .await
            .unwrap();
        writer
            .write_at(LogStream::Stderr, b"oops\n", t(1))
            .await
            .unwrap();
        writer
            .write_at(LogStream::Stdout, b"ld\n", t(2))
            .await
            .unwrap();
        let data = writer.into_inner();

        let lines = reassemble(&data, &ReadOptions::default());
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].message, b"hello");
        assert_eq!(lines[1].message, b"oops");
        assert_eq!(lines[1].stream, LogStream::Stderr);
        assert_eq!(lines[2].message, b"world");
        assert_eq!(lines[2].time, t(0)); // line started at the P record
        assert!(lines.iter().all(|l| !l.truncated));
    }

    #[tokio::test]
    async fn unterminated_tail_is_reported_truncated() {
        let mut writer = CriLogWriter::new(Vec::new());
        writer
            .write_at(LogStream::Stdout, b"no newline", t(0))
            .await
            .unwrap();
        let lines = reassemble(&writer.into_inner(), &ReadOptions::default());
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].message, b"no newline");
        assert!(lines[0].truncated);
    }

    #[tokio::test]
    async fn since_and_tail_filters() {
        let mut writer = CriLogWriter::new(Vec::new());
        for i in 0..5 {
            writer
                .write_at(LogStream::Stdout, format!("line{i}\n").as_bytes(), t(i))
                .await
                .unwrap();
        }
        let data = writer.into_inner();

        let since = reassemble(
            &data,
            &ReadOptions {
                since: Some(t(3)),
                tail_lines: None,
            },
        );
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].message, b"line3");

        let tail = reassemble(
            &data,
            &ReadOptions {
                since: None,
                tail_lines: Some(2),
            },
        );
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].message, b"line3");
        assert_eq!(tail[1].message, b"line4");
    }

    #[test]
    fn parse_tolerates_sub_tags_and_rejects_garbage() {
        let entry =
            LogEntry::parse(b"2026-07-17T10:00:00.123456789Z stdout P:extra partial").unwrap();
        assert!(entry.partial);
        assert_eq!(entry.message, b"partial");

        assert!(LogEntry::parse(b"garbage").is_err());
        assert!(LogEntry::parse(b"2026-07-17T10:00:00Z bogus F x").is_err());
    }

    #[test]
    fn render_matches_cri_format() {
        let entry = LogEntry {
            time: t(0),
            stream: LogStream::Stdout,
            partial: false,
            message: b"full line".to_vec(),
        };
        let rendered = String::from_utf8(entry.render()).unwrap();
        assert_eq!(
            rendered,
            "2023-11-14T22:13:20.123456789Z stdout F full line\n"
        );
    }
}

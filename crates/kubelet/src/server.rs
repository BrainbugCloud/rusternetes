//! Kubelet HTTP API endpoints beyond metrics/configz.
//!
//! Serves `GET /containerLogs/{namespace}/{pod}/{container}` by reading the
//! CRI-format log files the runtime writes (the api-server's pod `log`
//! subresource proxies here). Supports follow/tailLines/sinceSeconds/
//! sinceTime/timestamps/previous/limitBytes.

use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::{DateTime, SecondsFormat, Utc};
use cri_proto::v1;
use cri_server::logfmt::{self, LogEntry, LogLine, ReadOptions};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::debug;

use crate::cri::CriClient;

/// Poll interval for `follow` mode (the CRI log file has no inotify
/// guarantee across container filesystems, so we poll).
const FOLLOW_POLL: Duration = Duration::from_millis(250);
/// Idle polls between container-state checks in `follow` mode (~2s).
const FOLLOW_STATE_CHECK_EVERY: u32 = 8;

#[derive(Clone)]
struct ServerState {
    cri: CriClient,
}

/// Routes served by the kubelet API server (merged with metrics/configz).
pub fn router(cri: CriClient) -> Router {
    Router::new()
        .route(
            "/containerLogs/:namespace/:pod/:container",
            get(container_logs),
        )
        .with_state(ServerState { cri })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogsQuery {
    #[serde(default)]
    follow: bool,
    #[serde(default)]
    previous: bool,
    #[serde(default)]
    timestamps: bool,
    tail_lines: Option<usize>,
    limit_bytes: Option<usize>,
    since_seconds: Option<i64>,
    since_time: Option<String>,
}

async fn container_logs(
    State(state): State<ServerState>,
    UrlPath((namespace, pod, container)): UrlPath<(String, String, String)>,
    Query(query): Query<LogsQuery>,
) -> Response {
    match logs_response(&state, &namespace, &pod, &container, query).await {
        Ok(resp) => resp,
        Err(e) => (StatusCode::NOT_FOUND, format!("{:#}", e)).into_response(),
    }
}

async fn logs_response(
    state: &ServerState,
    namespace: &str,
    pod: &str,
    container: &str,
    query: LogsQuery,
) -> Result<Response> {
    let cri_container = state
        .cri
        .find_container_scoped(namespace, pod, container)
        .await?
        .with_context(|| {
            format!(
                "container {} not found for pod {}/{}",
                container, namespace, pod
            )
        })?;
    let status = state.cri.container_status(&cri_container.id).await?;
    if status.log_path.is_empty() {
        anyhow::bail!(
            "no log path recorded for container {} in pod {}/{}",
            container,
            namespace,
            pod
        );
    }
    let mut log_path = PathBuf::from(&status.log_path);

    if query.previous {
        // Log files are {log_directory}/{container}/{attempt}.log; the
        // previous attempt's file survives container removal.
        let attempt = cri_container
            .metadata
            .as_ref()
            .map(|m| m.attempt)
            .unwrap_or(0);
        if attempt == 0 {
            anyhow::bail!(
                "previous terminated container {} in pod {}/{} not found",
                container,
                namespace,
                pod
            );
        }
        log_path.set_file_name(format!("{}.log", attempt - 1));
    }

    let since = since_filter(&query);
    let opts = ReadOptions {
        since,
        tail_lines: query.tail_lines,
    };

    if !query.follow {
        let data = tokio::fs::read(&log_path)
            .await
            .with_context(|| format!("failed to read log file {}", log_path.display()))?;
        let mut body = Vec::new();
        for line in logfmt::reassemble(&data, &opts) {
            render_line(&line, query.timestamps, &mut body);
            if let Some(limit) = query.limit_bytes {
                if body.len() >= limit {
                    body.truncate(limit);
                    break;
                }
            }
        }
        return Ok((
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            body,
        )
            .into_response());
    }

    // follow: initial batch honoring since/tail, then stream file growth
    // until the container stops producing output and is no longer running.
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(16);
    let cri = state.cri.clone();
    let container_id = cri_container.id.clone();
    let timestamps = query.timestamps;
    let limit_bytes = query.limit_bytes;
    tokio::spawn(async move {
        follow_logs(cri, container_id, log_path, opts, timestamps, limit_bytes, tx).await;
    });
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from_stream(stream))?)
}

fn since_filter(query: &LogsQuery) -> Option<DateTime<Utc>> {
    if let Some(ref since_time) = query.since_time {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(since_time) {
            return Some(parsed.with_timezone(&Utc));
        }
    }
    query
        .since_seconds
        .map(|s| Utc::now() - chrono::Duration::seconds(s))
}

fn render_line(line: &LogLine, timestamps: bool, out: &mut Vec<u8>) {
    if timestamps {
        out.extend_from_slice(
            line.time
                .to_rfc3339_opts(SecondsFormat::Nanos, true)
                .as_bytes(),
        );
        out.push(b' ');
    }
    out.extend_from_slice(&line.message);
    out.push(b'\n');
}

async fn follow_logs(
    cri: CriClient,
    container_id: String,
    path: PathBuf,
    opts: ReadOptions,
    timestamps: bool,
    limit_bytes: Option<usize>,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Bytes, std::io::Error>>,
) {
    let mut sent: usize = 0;
    let mut offset: u64 = 0;

    if let Ok(data) = tokio::fs::read(&path).await {
        offset = data.len() as u64;
        let mut buf = Vec::new();
        for line in logfmt::reassemble(&data, &opts) {
            render_line(&line, timestamps, &mut buf);
        }
        if !send_capped(&tx, buf, &mut sent, limit_bytes).await {
            return;
        }
    }

    let mut idle_polls: u32 = 0;
    loop {
        tokio::time::sleep(FOLLOW_POLL).await;

        let len = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(_) => break, // log file removed (container GC'd)
        };
        if len < offset {
            offset = 0; // file truncated/rotated — re-read from the start
        }

        if len > offset {
            idle_polls = 0;
            let Some(raw) = read_complete_lines(&path, &mut offset).await else {
                break;
            };
            let mut buf = Vec::new();
            for raw_line in raw.split(|&b| b == b'\n') {
                if raw_line.is_empty() {
                    continue;
                }
                let Ok(entry) = LogEntry::parse(raw_line) else {
                    continue;
                };
                if timestamps {
                    buf.extend_from_slice(
                        entry
                            .time
                            .to_rfc3339_opts(SecondsFormat::Nanos, true)
                            .as_bytes(),
                    );
                    buf.push(b' ');
                }
                buf.extend_from_slice(&entry.message);
                // P (partial) records continue the logical line; only F
                // records terminate it.
                if !entry.partial {
                    buf.push(b'\n');
                }
            }
            if !send_capped(&tx, buf, &mut sent, limit_bytes).await {
                return;
            }
        } else {
            idle_polls += 1;
            if idle_polls >= FOLLOW_STATE_CHECK_EVERY {
                idle_polls = 0;
                // Stop following once the container has exited and the file
                // stopped growing (matches kubelet behavior).
                match cri.container_status(&container_id).await {
                    Ok(s) if s.state == v1::ContainerState::ContainerRunning as i32 => {}
                    _ => {
                        debug!("follow: container {} no longer running", container_id);
                        break;
                    }
                }
            }
        }
    }
}

/// Read newly appended *complete* lines from `offset`, leaving any
/// unterminated tail for the next poll.
async fn read_complete_lines(path: &PathBuf, offset: &mut u64) -> Option<Vec<u8>> {
    let mut file = tokio::fs::File::open(path).await.ok()?;
    file.seek(SeekFrom::Start(*offset)).await.ok()?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await.ok()?;
    match data.iter().rposition(|&b| b == b'\n') {
        Some(pos) => {
            let consumed = pos + 1;
            *offset += consumed as u64;
            data.truncate(consumed);
            Some(data)
        }
        None => Some(Vec::new()), // no complete line yet
    }
}

/// Send `buf`, enforcing limitBytes across the whole stream. Returns false
/// when the stream should stop (limit reached or receiver gone).
async fn send_capped(
    tx: &tokio::sync::mpsc::Sender<std::result::Result<Bytes, std::io::Error>>,
    mut buf: Vec<u8>,
    sent: &mut usize,
    limit_bytes: Option<usize>,
) -> bool {
    if buf.is_empty() {
        return true;
    }
    if let Some(limit) = limit_bytes {
        if *sent + buf.len() >= limit {
            buf.truncate(limit - *sent);
            let _ = tx.send(Ok(Bytes::from(buf))).await;
            return false;
        }
    }
    *sent += buf.len();
    tx.send(Ok(Bytes::from(buf))).await.is_ok()
}

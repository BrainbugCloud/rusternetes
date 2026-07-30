// SPDX-License-Identifier: Apache-2.0

//! `MemoryBackend`: an in-memory state-machine CRI backend for tests,
//! examples, and backend contract testing (plans 01-S2/S4, 05).
//!
//! Sandboxes and containers are plain records driven through the CRI state
//! machine; images are a set. No real workloads run. When a sandbox declares
//! a `log_directory` and a container a `log_path`, starting the container
//! writes a small real CRI-format log file so `crictl logs` and critest log
//! assertions have something true to check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cri_proto::v1::*;

use crate::backend::{ExecSyncResult, ImageBackend, RuntimeBackend};
use crate::error::{Error, Result};
use crate::logfmt::{CriLogWriter, LogStream};

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}

fn matches_labels(selector: &HashMap<String, String>, labels: &HashMap<String, String>) -> bool {
    selector
        .iter()
        .all(|(k, v)| labels.get(k).is_some_and(|actual| actual == v))
}

#[derive(Debug, Clone)]
struct SandboxRecord {
    id: String,
    config: PodSandboxConfig,
    runtime_handler: String,
    state: PodSandboxState,
    created_at: i64,
    ip: String,
}

impl SandboxRecord {
    fn to_pod_sandbox(&self) -> PodSandbox {
        PodSandbox {
            id: self.id.clone(),
            metadata: self.config.metadata.clone(),
            state: self.state as i32,
            created_at: self.created_at,
            labels: self.config.labels.clone(),
            annotations: self.config.annotations.clone(),
            runtime_handler: self.runtime_handler.clone(),
        }
    }

    fn to_status(&self) -> PodSandboxStatus {
        PodSandboxStatus {
            id: self.id.clone(),
            metadata: self.config.metadata.clone(),
            state: self.state as i32,
            created_at: self.created_at,
            network: Some(PodSandboxNetworkStatus {
                ip: self.ip.clone(),
                ..Default::default()
            }),
            labels: self.config.labels.clone(),
            annotations: self.config.annotations.clone(),
            runtime_handler: self.runtime_handler.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
struct ContainerRecord {
    id: String,
    sandbox_id: String,
    config: ContainerConfig,
    state: ContainerState,
    created_at: i64,
    started_at: i64,
    finished_at: i64,
    exit_code: i32,
    image_ref: String,
    log_path: String,
}

impl ContainerRecord {
    fn to_container(&self) -> Container {
        Container {
            id: self.id.clone(),
            pod_sandbox_id: self.sandbox_id.clone(),
            metadata: self.config.metadata.clone(),
            image: self.config.image.clone(),
            image_ref: self.image_ref.clone(),
            state: self.state as i32,
            created_at: self.created_at,
            labels: self.config.labels.clone(),
            annotations: self.config.annotations.clone(),
            ..Default::default()
        }
    }

    fn to_status(&self) -> ContainerStatus {
        let (reason, message) = match self.state {
            ContainerState::ContainerExited if self.exit_code == 0 => {
                ("Completed".to_string(), String::new())
            }
            ContainerState::ContainerExited => (
                "Error".to_string(),
                format!("exited with code {}", self.exit_code),
            ),
            _ => (String::new(), String::new()),
        };
        ContainerStatus {
            id: self.id.clone(),
            metadata: self.config.metadata.clone(),
            state: self.state as i32,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            exit_code: self.exit_code,
            image: self.config.image.clone(),
            image_ref: self.image_ref.clone(),
            reason,
            message,
            labels: self.config.labels.clone(),
            annotations: self.config.annotations.clone(),
            log_path: self.log_path.clone(),
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct State {
    sandboxes: HashMap<String, SandboxRecord>,
    containers: HashMap<String, ContainerRecord>,
    images: HashMap<String, Image>,
    next_id: u64,
    pod_cidr: Option<String>,
}

impl State {
    fn next_id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}{:016x}", self.next_id)
    }
}

/// What a container "runs" — a tiny scripted command language so critest's
/// exec/log assertions can be honored without real workloads.
enum Workload {
    /// Long-running process (`top`, `sleep`, pause default): stays Running.
    Idle,
    /// `echo …`: writes its output to the CRI log and exits 0.
    OneShot(Vec<u8>),
    /// `echo …; sleep N`: writes its output to the CRI log, stays Running.
    EchoThenIdle(Vec<u8>),
    /// `while true; do echo <line>; sleep 1; done`: appends `<line>` to the
    /// CRI log periodically until the container stops.
    LogLoop(String),
}

/// Interpret an effective container command (`command` ++ `args`).
fn interpret_command(cmd: &[String]) -> Workload {
    match cmd.first().map(String::as_str) {
        Some("echo") => Workload::OneShot(echo_output(&cmd[1..])),
        Some("sh") | Some("/bin/sh") if cmd.get(1).map(String::as_str) == Some("-c") => {
            let script = cmd.get(2).map(String::as_str).unwrap_or_default();
            if script.contains("while true") && script.contains("echo ") {
                let line = script
                    .split("echo ")
                    .nth(1)
                    .and_then(|rest| rest.split([';', '&']).next())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                Workload::LogLoop(line)
            } else if let Some(rest) = script.strip_prefix("echo ") {
                let first_statement = rest.split([';', '&']).next().unwrap_or("");
                let args: Vec<String> = first_statement
                    .split_whitespace()
                    .map(String::from)
                    .collect();
                let output = echo_output(&args);
                if rest.contains("sleep") || rest.contains("top") {
                    Workload::EchoThenIdle(output)
                } else {
                    Workload::OneShot(output)
                }
            } else {
                Workload::Idle
            }
        }
        _ => Workload::Idle,
    }
}

/// `echo` semantics: `-n`/`-e`/`-ne` flags are consumed, remaining args are
/// joined; a trailing newline unless `-n` was given.
fn echo_output(args: &[String]) -> Vec<u8> {
    let mut newline = true;
    let mut rest = args;
    while let Some(first) = rest.first() {
        match first.as_str() {
            "-n" | "-ne" | "-en" => {
                newline = false;
                rest = &rest[1..];
            }
            "-e" => rest = &rest[1..],
            _ => break,
        }
    }
    let mut out = rest.join(" ").into_bytes();
    if newline {
        out.push(b'\n');
    }
    out
}

async fn append_log_line(log_path: &str, line: &[u8]) -> Result<()> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await?;
    let mut writer = CriLogWriter::new(file);
    writer.write(LogStream::Stdout, line).await
}

/// In-memory CRI backend. See the module docs.
#[derive(Default)]
pub struct MemoryBackend {
    state: Arc<Mutex<State>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deterministic fake digest for an image name.
    fn fake_digest(name: &str) -> String {
        let crc = crc32fast::hash(name.as_bytes());
        let block = format!("{crc:08x}");
        format!("sha256:{}", block.repeat(8))
    }

    fn find_image(state: &State, spec: &ImageSpec) -> Option<Image> {
        let wanted = &spec.image;
        state
            .images
            .values()
            .find(|img| {
                &img.id == wanted
                    || img
                        .repo_tags
                        .iter()
                        .any(|t| t == wanted || t.strip_suffix(":latest") == Some(wanted.as_str()))
                    || img.repo_digests.iter().any(|d| d == wanted)
            })
            .cloned()
    }

    /// Canonicalize a pulled tag the way runtimes do (`busybox` →
    /// `busybox:latest`); leaves digests and tagged refs alone.
    fn canonical_tag(name: &str) -> String {
        let has_tag = name
            .rsplit('/')
            .next()
            .is_some_and(|last| last.contains(':'));
        if name.contains('@') || has_tag {
            name.to_string()
        } else {
            format!("{name}:latest")
        }
    }
}

#[async_trait]
impl RuntimeBackend for MemoryBackend {
    async fn run_pod_sandbox(
        &self,
        config: PodSandboxConfig,
        runtime_handler: &str,
    ) -> Result<String> {
        let meta = config
            .metadata
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("sandbox config.metadata is required".into()))?;
        if meta.name.is_empty() || meta.namespace.is_empty() || meta.uid.is_empty() {
            return Err(Error::InvalidArgument(
                "sandbox metadata requires name, namespace, and uid".into(),
            ));
        }
        if !config.log_directory.is_empty() {
            tokio::fs::create_dir_all(&config.log_directory).await?;
        }
        let mut state = self.state.lock().unwrap();
        let id = state.next_id("sb-");
        let n = state.next_id;
        state.sandboxes.insert(
            id.clone(),
            SandboxRecord {
                id: id.clone(),
                config,
                runtime_handler: runtime_handler.to_string(),
                state: PodSandboxState::SandboxReady,
                created_at: now_nanos(),
                ip: format!("10.88.{}.{}", (n >> 8) & 0xff, n & 0xff),
            },
        );
        Ok(id)
    }

    async fn stop_pod_sandbox(&self, id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(sandbox) = state.sandboxes.get_mut(id) {
            sandbox.state = PodSandboxState::SandboxNotready;
        }
        let now = now_nanos();
        for container in state.containers.values_mut() {
            if container.sandbox_id == id && container.state == ContainerState::ContainerRunning {
                container.state = ContainerState::ContainerExited;
                container.finished_at = now;
                container.exit_code = 137;
            }
        }
        Ok(())
    }

    async fn remove_pod_sandbox(&self, id: &str) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(sandbox) = state.sandboxes.get(id) {
            if sandbox.state == PodSandboxState::SandboxReady {
                return Err(Error::FailedPrecondition(format!(
                    "sandbox {id} must be stopped before removal"
                )));
            }
        }
        state.sandboxes.remove(id);
        state.containers.retain(|_, c| c.sandbox_id != id);
        Ok(())
    }

    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus> {
        let state = self.state.lock().unwrap();
        state
            .sandboxes
            .get(id)
            .map(SandboxRecord::to_status)
            .ok_or_else(|| Error::NotFound(format!("sandbox {id} not found")))
    }

    async fn list_pod_sandbox(&self, filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>> {
        let state = self.state.lock().unwrap();
        let mut items: Vec<_> = state
            .sandboxes
            .values()
            .filter(|s| {
                let Some(filter) = &filter else { return true };
                if !filter.id.is_empty() && s.id != filter.id {
                    return false;
                }
                if let Some(want) = &filter.state {
                    if s.state as i32 != want.state {
                        return false;
                    }
                }
                matches_labels(&filter.label_selector, &s.config.labels)
            })
            .map(SandboxRecord::to_pod_sandbox)
            .collect();
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(items)
    }

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let meta = config.metadata.as_ref().ok_or_else(|| {
            Error::InvalidArgument("container config.metadata is required".into())
        })?;
        if meta.name.is_empty() {
            return Err(Error::InvalidArgument(
                "container metadata requires name".into(),
            ));
        }
        let image = config
            .image
            .as_ref()
            .map(|spec| spec.image.clone())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| Error::InvalidArgument("container config.image is required".into()))?;

        let log_path = if !sandbox_config.log_directory.is_empty() && !config.log_path.is_empty() {
            std::path::Path::new(&sandbox_config.log_directory)
                .join(&config.log_path)
                .to_string_lossy()
                .into_owned()
        } else {
            String::new()
        };

        let mut state = self.state.lock().unwrap();
        if !state.sandboxes.contains_key(sandbox_id) {
            return Err(Error::NotFound(format!("sandbox {sandbox_id} not found")));
        }
        let id = state.next_id("ctr-");
        state.containers.insert(
            id.clone(),
            ContainerRecord {
                id: id.clone(),
                sandbox_id: sandbox_id.to_string(),
                config,
                state: ContainerState::ContainerCreated,
                created_at: now_nanos(),
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
                image_ref: Self::fake_digest(&image),
                log_path,
            },
        );
        Ok(id)
    }

    async fn start_container(&self, id: &str) -> Result<()> {
        let (log_path, effective_cmd) = {
            let mut state = self.state.lock().unwrap();
            let container = state
                .containers
                .get_mut(id)
                .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
            if container.state != ContainerState::ContainerCreated {
                return Err(Error::FailedPrecondition(format!(
                    "container {id} is not in created state"
                )));
            }
            container.state = ContainerState::ContainerRunning;
            container.started_at = now_nanos();
            let mut cmd = container.config.command.clone();
            cmd.extend(container.config.args.iter().cloned());
            (container.log_path.clone(), cmd)
        };
        if !log_path.is_empty() {
            if let Some(parent) = std::path::Path::new(&log_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            drop(tokio::fs::File::create(&log_path).await?);
        }
        match interpret_command(&effective_cmd) {
            Workload::Idle => {}
            Workload::EchoThenIdle(output) => {
                if !log_path.is_empty() {
                    append_log_line(&log_path, &output).await?;
                }
            }
            Workload::OneShot(output) => {
                if !log_path.is_empty() {
                    append_log_line(&log_path, &output).await?;
                }
                let mut state = self.state.lock().unwrap();
                if let Some(container) = state.containers.get_mut(id) {
                    container.state = ContainerState::ContainerExited;
                    container.exit_code = 0;
                    container.finished_at = now_nanos();
                }
            }
            Workload::LogLoop(line) => {
                let state = self.state.clone();
                let id = id.to_string();
                tokio::spawn(async move {
                    loop {
                        let log_path = {
                            let state = state.lock().unwrap();
                            match state.containers.get(&id) {
                                Some(c) if c.state == ContainerState::ContainerRunning => {
                                    c.log_path.clone()
                                }
                                _ => break,
                            }
                        };
                        if !log_path.is_empty() {
                            let _ =
                                append_log_line(&log_path, format!("{line}\n").as_bytes()).await;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                });
            }
        }
        Ok(())
    }

    async fn stop_container(&self, id: &str, _timeout_secs: i64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(container) = state.containers.get_mut(id) {
            if container.state == ContainerState::ContainerRunning {
                container.exit_code = 0;
            }
            if container.state != ContainerState::ContainerExited {
                container.state = ContainerState::ContainerExited;
                container.finished_at = now_nanos();
            }
        }
        Ok(())
    }

    async fn remove_container(&self, id: &str) -> Result<()> {
        // CRI: RemoveContainer forcibly removes even running containers
        // (asserted by critest "removing running container").
        let mut state = self.state.lock().unwrap();
        state.containers.remove(id);
        Ok(())
    }

    async fn list_containers(&self, filter: Option<ContainerFilter>) -> Result<Vec<Container>> {
        let state = self.state.lock().unwrap();
        let mut items: Vec<_> = state
            .containers
            .values()
            .filter(|c| {
                let Some(filter) = &filter else { return true };
                if !filter.id.is_empty() && c.id != filter.id {
                    return false;
                }
                if !filter.pod_sandbox_id.is_empty() && c.sandbox_id != filter.pod_sandbox_id {
                    return false;
                }
                if let Some(want) = &filter.state {
                    if c.state as i32 != want.state {
                        return false;
                    }
                }
                matches_labels(&filter.label_selector, &c.config.labels)
            })
            .map(ContainerRecord::to_container)
            .collect();
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(items)
    }

    async fn container_status(&self, id: &str) -> Result<ContainerStatus> {
        let state = self.state.lock().unwrap();
        state
            .containers
            .get(id)
            .map(ContainerRecord::to_status)
            .ok_or_else(|| Error::NotFound(format!("container {id} not found")))
    }

    async fn update_container_resources(
        &self,
        id: &str,
        resources: LinuxContainerResources,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let container = state
            .containers
            .get_mut(id)
            .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
        let linux = container.config.linux.get_or_insert_with(Default::default);
        linux.resources = Some(resources);
        Ok(())
    }

    async fn exec_sync(
        &self,
        id: &str,
        cmd: &[String],
        timeout_secs: i64,
    ) -> Result<ExecSyncResult> {
        {
            let state = self.state.lock().unwrap();
            let container = state
                .containers
                .get(id)
                .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
            if container.state != ContainerState::ContainerRunning {
                return Err(Error::FailedPrecondition(format!(
                    "container {id} is not running"
                )));
            }
        }
        // Scripted shell: enough for lifecycle and critest exec assertions,
        // not a real exec.
        Ok(match cmd.first().map(String::as_str) {
            Some("echo") => ExecSyncResult {
                stdout: echo_output(&cmd[1..]),
                stderr: Vec::new(),
                exit_code: 0,
            },
            Some("sleep") => {
                let secs: u64 = cmd.get(1).and_then(|s| s.parse().ok()).unwrap_or_default();
                if timeout_secs > 0 && secs > timeout_secs as u64 {
                    tokio::time::sleep(std::time::Duration::from_secs(timeout_secs as u64)).await;
                    return Err(Error::DeadlineExceeded(format!(
                        "sleep {secs} exceeded timeout of {timeout_secs}s"
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                ExecSyncResult::default()
            }
            Some("false") => ExecSyncResult {
                exit_code: 1,
                ..Default::default()
            },
            _ => ExecSyncResult::default(),
        })
    }

    async fn container_stats(&self, id: &str) -> Result<ContainerStats> {
        let state = self.state.lock().unwrap();
        let container = state
            .containers
            .get(id)
            .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?;
        Ok(fake_stats(container))
    }

    async fn list_container_stats(
        &self,
        filter: Option<ContainerStatsFilter>,
    ) -> Result<Vec<ContainerStats>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .containers
            .values()
            .filter(|c| {
                let Some(filter) = &filter else { return true };
                if !filter.id.is_empty() && c.id != filter.id {
                    return false;
                }
                if !filter.pod_sandbox_id.is_empty() && c.sandbox_id != filter.pod_sandbox_id {
                    return false;
                }
                matches_labels(&filter.label_selector, &c.config.labels)
            })
            .map(fake_stats)
            .collect())
    }

    async fn status(&self) -> Result<RuntimeStatus> {
        Ok(RuntimeStatus {
            conditions: vec![
                RuntimeCondition {
                    r#type: "RuntimeReady".to_string(),
                    status: true,
                    ..Default::default()
                },
                RuntimeCondition {
                    r#type: "NetworkReady".to_string(),
                    status: true,
                    ..Default::default()
                },
            ],
        })
    }

    async fn version(&self) -> Result<VersionResponse> {
        Ok(VersionResponse {
            version: "0.1.0".to_string(),
            runtime_name: "memory-cri".to_string(),
            runtime_version: env!("CARGO_PKG_VERSION").to_string(),
            runtime_api_version: "v1".to_string(),
        })
    }

    async fn update_runtime_config(&self, pod_cidr: Option<String>) -> Result<()> {
        self.state.lock().unwrap().pod_cidr = pod_cidr;
        Ok(())
    }

    async fn runtime_config(&self) -> Result<RuntimeConfigResponse> {
        Ok(RuntimeConfigResponse {
            linux: Some(LinuxRuntimeConfiguration {
                cgroup_driver: CgroupDriver::Cgroupfs as i32,
            }),
        })
    }

    async fn reopen_container_log(&self, id: &str) -> Result<()> {
        let log_path = {
            let state = self.state.lock().unwrap();
            state
                .containers
                .get(id)
                .map(|c| c.log_path.clone())
                .ok_or_else(|| Error::NotFound(format!("container {id} not found")))?
        };
        // Rotate: create a fresh file at the CRI log path. The log-loop
        // workload appends per line, so subsequent writes land here while a
        // renamed old file stays untouched.
        if !log_path.is_empty() {
            drop(tokio::fs::File::create(&log_path).await?);
        }
        Ok(())
    }
}

fn fake_stats(container: &ContainerRecord) -> ContainerStats {
    let now = now_nanos();
    ContainerStats {
        attributes: Some(ContainerAttributes {
            id: container.id.clone(),
            metadata: container.config.metadata.clone(),
            labels: container.config.labels.clone(),
            annotations: container.config.annotations.clone(),
        }),
        cpu: Some(CpuUsage {
            timestamp: now,
            usage_core_nano_seconds: Some(UInt64Value { value: 0 }),
            ..Default::default()
        }),
        memory: Some(MemoryUsage {
            timestamp: now,
            working_set_bytes: Some(UInt64Value { value: 0 }),
            ..Default::default()
        }),
        writable_layer: Some(FilesystemUsage {
            timestamp: now,
            used_bytes: Some(UInt64Value { value: 0 }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[async_trait]
impl ImageBackend for MemoryBackend {
    async fn list_images(&self, filter: Option<ImageFilter>) -> Result<Vec<Image>> {
        let state = self.state.lock().unwrap();
        if let Some(spec) = filter.and_then(|f| f.image).filter(|s| !s.image.is_empty()) {
            return Ok(Self::find_image(&state, &spec).into_iter().collect());
        }
        Ok(state.images.values().cloned().collect())
    }

    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>> {
        let state = self.state.lock().unwrap();
        Ok(Self::find_image(&state, image))
    }

    async fn pull_image(
        &self,
        image: &ImageSpec,
        _auth: Option<AuthConfig>,
        _sandbox_config: Option<PodSandboxConfig>,
    ) -> Result<String> {
        if image.image.is_empty() {
            return Err(Error::InvalidArgument("image name is required".into()));
        }
        let tag = Self::canonical_tag(&image.image);
        let id = Self::fake_digest(&tag);
        let mut state = self.state.lock().unwrap();
        state.images.insert(
            id.clone(),
            Image {
                id: id.clone(),
                repo_tags: vec![tag.clone()],
                repo_digests: vec![format!("{}@{id}", tag.split(':').next().unwrap_or(&tag))],
                size: 1024,
                spec: Some(image.clone()),
                ..Default::default()
            },
        );
        Ok(id)
    }

    async fn remove_image(&self, image: &ImageSpec) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(found) = Self::find_image(&state, image) {
            state.images.remove(&found.id);
        }
        Ok(())
    }

    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>> {
        let state = self.state.lock().unwrap();
        let used: u64 = state.images.values().map(|i| i.size).sum();
        Ok(vec![FilesystemUsage {
            timestamp: now_nanos(),
            fs_id: Some(FilesystemIdentifier {
                mountpoint: "/var/lib/memory-cri".to_string(),
            }),
            used_bytes: Some(UInt64Value { value: used }),
            inodes_used: Some(UInt64Value {
                value: state.images.len() as u64,
            }),
        }])
    }
}

#[async_trait]
impl crate::streaming::StreamingBackend for MemoryBackend {
    /// Scripted exec: `echo <args>` prints to stdout; `stderr <args>` prints
    /// to stderr; `cat` echoes stdin to stdout until EOF (works as the
    /// interactive/tty case); `false` exits 1; anything else exits 0 silently.
    async fn exec_stream(
        &self,
        container_id: &str,
        cmd: Vec<String>,
        tty: bool,
        stdin: bool,
    ) -> Result<crate::streaming::ExecStreams> {
        {
            let state = self.state.lock().unwrap();
            let container = state
                .containers
                .get(container_id)
                .ok_or_else(|| Error::NotFound(format!("container {container_id} not found")))?;
            if container.state != ContainerState::ContainerRunning {
                return Err(Error::FailedPrecondition(format!(
                    "container {container_id} is not running"
                )));
            }
        }

        let (stdin_client, mut stdin_task) = tokio::io::duplex(64 * 1024);
        let (mut stdout_task, stdout_client) = tokio::io::duplex(64 * 1024);
        let (mut stderr_task, stderr_client) = tokio::io::duplex(64 * 1024);
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let code = match cmd.first().map(String::as_str) {
                Some("echo") => {
                    let _ = stdout_task.write_all(&echo_output(&cmd[1..])).await;
                    0
                }
                Some("stderr") => {
                    let _ = stderr_task
                        .write_all(format!("{}\n", cmd[1..].join(" ")).as_bytes())
                        .await;
                    0
                }
                Some("cat") => {
                    let mut buf = [0u8; 4096];
                    loop {
                        match stdin_task.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stdout_task.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    0
                }
                Some("false") => 1,
                _ => 0,
            };
            // Dropping the duplex halves signals EOF on stdout/stderr.
            let _ = exit_tx.send(code);
        });

        Ok(crate::streaming::ExecStreams {
            stdin: stdin.then(|| Box::pin(stdin_client) as crate::streaming::BoxedWriter),
            stdout: Some(Box::pin(stdout_client) as crate::streaming::BoxedReader),
            stderr: (!tty).then(|| Box::pin(stderr_client) as crate::streaming::BoxedReader),
            exit: exit_rx,
        })
    }

    /// Scripted attach: behaves like an attached `/bin/sh` — each stdin line
    /// is interpreted by the scripted shell (`echo …` prints, anything else
    /// is silent). Without stdin it greets once and stays quiet.
    async fn attach_stream(
        &self,
        container_id: &str,
        _tty: bool,
        stdin: bool,
    ) -> Result<crate::streaming::AttachStreams> {
        {
            let state = self.state.lock().unwrap();
            let container = state
                .containers
                .get(container_id)
                .ok_or_else(|| Error::NotFound(format!("container {container_id} not found")))?;
            if container.state != ContainerState::ContainerRunning {
                return Err(Error::FailedPrecondition(format!(
                    "container {container_id} is not running"
                )));
            }
        }

        let (stdin_client, mut stdin_task) = tokio::io::duplex(64 * 1024);
        let (mut stdout_task, stdout_client) = tokio::io::duplex(64 * 1024);
        let id = container_id.to_string();
        let with_stdin = stdin;

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let _ = stdout_task
                .write_all(format!("attached to {id}\n").as_bytes())
                .await;
            if with_stdin {
                let mut buf = [0u8; 4096];
                let mut pending = Vec::new();
                loop {
                    match stdin_task.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            pending.extend_from_slice(&buf[..n]);
                            while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
                                let line: Vec<u8> = pending.drain(..=pos).collect();
                                let line = String::from_utf8_lossy(&line[..line.len() - 1])
                                    .trim()
                                    .to_string();
                                let tokens: Vec<String> =
                                    line.split_whitespace().map(String::from).collect();
                                if tokens.first().map(String::as_str) == Some("echo")
                                    && stdout_task
                                        .write_all(&echo_output(&tokens[1..]))
                                        .await
                                        .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(crate::streaming::AttachStreams {
            stdin: stdin.then(|| Box::pin(stdin_client) as crate::streaming::BoxedWriter),
            stdout: Some(Box::pin(stdout_client) as crate::streaming::BoxedReader),
            stderr: None,
        })
    }

    /// The fake sandbox shares the host network namespace: dial localhost.
    async fn dial_in_sandbox(
        &self,
        sandbox_id: &str,
        port: i32,
    ) -> Result<Box<dyn crate::streaming::AsyncReadWrite>> {
        {
            let state = self.state.lock().unwrap();
            if !state.sandboxes.contains_key(sandbox_id) {
                return Err(Error::NotFound(format!("sandbox {sandbox_id} not found")));
            }
        }
        let port = u16::try_from(port)
            .map_err(|_| Error::InvalidArgument(format!("invalid port {port}")))?;
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| Error::Unavailable(format!("dial 127.0.0.1:{port}: {e}")))?;
        Ok(Box::new(stream))
    }
}

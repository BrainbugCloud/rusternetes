//! Pod subresource handlers
//!
//! Implements pod subresources required for Kubernetes conformance:
//! - /logs - Get container logs
//! - /exec - Execute commands in containers (SPDY and WebSocket)
//! - /attach - Attach to running containers (SPDY and WebSocket)
//! - /portforward - Forward ports to pods (SPDY and WebSocket)

use crate::{kubelet_proxy, middleware::AuthContext, spdy, state::ApiServerState, streaming};
use axum::{
    body::Body,
    extract::{ws::WebSocketUpgrade, Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    Error, Result,
};
use rusternetes_storage::Storage;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info};

/// Simple percent-decoding for URL query parameters
fn percent_decode_str(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(s) = std::str::from_utf8(&hex) {
                if let Ok(val) = u8::from_str_radix(s, 16) {
                    result.push(val as char);
                    continue;
                }
            }
            result.push('%');
            result.push(hi as char);
            result.push(lo as char);
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// The container for which to stream logs
    #[serde(default)]
    pub container: Option<String>,
    /// Follow the log stream
    #[serde(default)]
    #[allow(dead_code)]
    pub follow: bool,
    /// Return previous terminated container logs
    #[serde(default)]
    #[allow(dead_code)]
    pub previous: bool,
    /// Show timestamps
    #[serde(default)]
    pub timestamps: bool,
    /// If set, the number of lines from the end of the logs to show
    #[serde(rename = "tailLines")]
    pub tail_lines: Option<i32>,
    /// If set, the number of bytes to read from the server before terminating
    #[serde(rename = "limitBytes")]
    pub limit_bytes: Option<i64>,
    /// Relative time in seconds before the current time from which to show logs
    #[serde(rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    /// RFC3339 timestamp from which to show logs
    #[serde(rename = "sinceTime")]
    pub since_time: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExecQuery {
    /// Container in which to execute the command
    pub container: Option<String>,
    /// Command to execute
    pub command: Vec<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

#[derive(Debug, Deserialize)]
pub struct AttachQuery {
    /// Container to attach to
    pub container: Option<String>,
    /// Redirect stdin
    #[serde(default)]
    pub stdin: bool,
    /// Redirect stdout
    #[serde(default)]
    pub stdout: bool,
    /// Redirect stderr
    #[serde(default)]
    pub stderr: bool,
    /// Use TTY
    #[serde(default)]
    pub tty: bool,
}

#[derive(Debug, Deserialize)]
pub struct PortForwardQuery {
    /// Ports to forward
    pub ports: Option<String>,
}

/// GET /api/v1/namespaces/{namespace}/pods/{name}/log
/// Stream logs from a pod (supports both HTTP and WebSocket)
pub async fn get_logs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<LogsQuery>,
    ws: Option<WebSocketUpgrade>,
) -> Result<Response> {
    debug!("Getting logs for pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("log");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod to verify it exists and get container information
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to get logs from
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in the pod (check containers, init containers, ephemeral containers)
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // Proxy to the kubelet on the pod's node. Errors surface as errors —
    // there is no fallback.
    let resp = fetch_kubelet_logs(&state, &pod, &container_name, &query)
        .await
        .map_err(|e| Error::Internal(format!("failed to get logs from kubelet: {:#}", e)))?;

    // If WebSocket upgrade requested, forward log chunks over WebSocket
    if let Some(ws) = ws {
        Ok(ws.on_upgrade(move |mut socket| async move {
            use axum::extract::ws::Message;
            use futures::StreamExt;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let Ok(chunk) = chunk else { break };
                let text = String::from_utf8_lossy(&chunk).to_string();
                if socket.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
            let _ = socket.close().await;
        }))
    } else {
        // Stream the kubelet response body through (supports follow)
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Body::from_stream(resp.bytes_stream()))
            .unwrap())
    }
}

/// Base URL (`http://{address}:{port}`) of the kubelet serving the pod's node.
pub(crate) async fn kubelet_base_url(
    state: &ApiServerState,
    pod: &rusternetes_common::resources::Pod,
) -> anyhow::Result<String> {
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.clone())
        .ok_or_else(|| anyhow::anyhow!("pod {} is not scheduled to a node", pod.metadata.name))?;
    let node_key = rusternetes_storage::build_key("nodes", None, &node_name);
    let node: rusternetes_common::resources::Node = state
        .storage
        .get(&node_key)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get node {}: {}", node_name, e))?;

    let address = node
        .status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .and_then(|addrs| {
            addrs
                .iter()
                .find(|a| a.address_type == "InternalIP")
                .or_else(|| addrs.iter().find(|a| a.address_type == "ExternalIP"))
        })
        .map(|a| a.address.clone())
        .ok_or_else(|| anyhow::anyhow!("no address found for node {}", node_name))?;
    let port = node
        .status
        .as_ref()
        .and_then(|s| s.daemon_endpoints.as_ref())
        .and_then(|d| d.kubelet_endpoint.as_ref())
        .map(|k| k.port)
        .filter(|p| *p > 0)
        .unwrap_or(10250);
    Ok(format!("http://{}:{}", address, port))
}

/// Fetch container logs from the kubelet's /containerLogs endpoint.
async fn fetch_kubelet_logs(
    state: &ApiServerState,
    pod: &rusternetes_common::resources::Pod,
    container_name: &str,
    query: &LogsQuery,
) -> anyhow::Result<reqwest::Response> {
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
    let base = kubelet_base_url(state, pod).await?;
    let url = format!(
        "{}/containerLogs/{}/{}/{}",
        base, namespace, pod.metadata.name, container_name
    );

    let mut params: Vec<(&str, String)> = Vec::new();
    if query.follow {
        params.push(("follow", "true".to_string()));
    }
    if query.previous {
        params.push(("previous", "true".to_string()));
    }
    if query.timestamps {
        params.push(("timestamps", "true".to_string()));
    }
    if let Some(tail) = query.tail_lines {
        params.push(("tailLines", tail.to_string()));
    }
    if let Some(limit) = query.limit_bytes {
        params.push(("limitBytes", limit.to_string()));
    }
    if let Some(since) = query.since_seconds {
        params.push(("sinceSeconds", since.to_string()));
    }
    if let Some(ref since_time) = query.since_time {
        params.push(("sinceTime", since_time.clone()));
    }

    // No overall timeout: follow streams stay open until the client goes away.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()?;
    let resp = client.get(&url).query(&params).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("kubelet returned {}: {}", status, body);
    }
    Ok(resp)
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/exec
/// Execute a command in a container (supports both SPDY and WebSocket)
pub async fn exec(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    // Parse query params manually because `command` can appear multiple times
    // (e.g., ?command=/bin/sh&command=-c&command=echo+hello) which serde's
    // query deserializer can't handle as Vec<String>.
    let raw_query = req.uri().query().unwrap_or("");
    let query = {
        let mut command = Vec::new();
        let mut container = None;
        let mut stdin = false;
        let mut stdout = false;
        let mut stderr = false;
        let mut tty = false;
        for pair in raw_query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                let value = percent_decode_str(value);
                match key {
                    "command" => command.push(value),
                    "container" => container = Some(value),
                    "stdin" => stdin = value == "true" || value == "1",
                    "stdout" => stdout = value == "true" || value == "1",
                    "stderr" => stderr = value == "true" || value == "1",
                    "tty" => tty = value == "true" || value == "1",
                    _ => {}
                }
            }
        }
        ExecQuery {
            container,
            command,
            stdin,
            stdout,
            stderr,
            tty,
        }
    };

    // Log request headers for debugging exec protocol
    let upgrade_header = req
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    let connection_header = req
        .headers()
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    let sec_ws_protocol = req
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    info!(
        "Exec {}/{}: cmd={:?} upgrade={} connection={} ws-protocol={}",
        namespace, name, query.command, upgrade_header, connection_header, sec_ws_protocol
    );

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("exec");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Run admission webhooks for Connect operation (exec)
    // K8s passes PodExecOptions as the admission object so webhooks can
    // inspect what command is being run and what streams are requested.
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodExecOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/exec".to_string(),
        };
        // Build PodExecOptions object matching K8s schema
        let exec_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodExecOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or(""),
            "command": query.command
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(exec_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to exec into
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in regular, init, or ephemeral containers
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // ── Proxy to kubelet streaming server ──

    // Resolve kubelet streaming endpoint from Node status
    let kubelet_addr = kubelet_proxy::kubelet_streaming_endpoint(&state, &pod)
        .await
        .map_err(|e| Error::Internal(format!("Failed to resolve kubelet: {e}")))?;

    // Build the kubelet streaming path
    let kubelet_path = if raw_query.is_empty() {
        format!("/exec/{namespace}/{name}/{container_name}")
    } else {
        format!("/exec/{namespace}/{name}/{container_name}?{raw_query}")
    };

    // Check for SPDY upgrade first (kubectl\'s default protocol)
    let is_spdy = spdy::is_spdy_upgrade(req.headers());
    if is_spdy {
        info!("SPDY exec → kubelet at {kubelet_addr}{kubelet_path}");
        let upgrade = hyper::upgrade::on(req);
        return Ok(kubelet_proxy::spdy_proxy_response(
            upgrade,
            &kubelet_addr,
            &kubelet_path,
        ));
    }

    // Handle WebSocket upgrade
    if let Some(ws) = ws {
        info!("WebSocket exec → kubelet at {kubelet_addr}");
        let is_v1_protocol = sec_ws_protocol == "channel.k8s.io"
            || (!sec_ws_protocol.contains("v4.channel") && !sec_ws_protocol.contains("v5.channel"));
        return Ok(ws
            .protocols(["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"])
            .on_upgrade(move |socket| {
                streaming::handle_exec_websocket_via_kubelet(
                    socket,
                    kubelet_addr,
                    pod,
                    container_name,
                    query.command,
                    query.stdin,
                    query.stdout,
                    query.stderr,
                    query.tty,
                    is_v1_protocol,
                )
            })
            .into_response());
    }

    // Plain HTTP: reverse-proxy to kubelet
    info!("HTTP exec proxy → kubelet at {kubelet_addr}{kubelet_path}");
    let target = format!("http://{kubelet_addr}{kubelet_path}");
    match reqwest::Client::new().get(&target).send().await {
        Ok(resp) => {
            let status = resp.status();
            let body_bytes = resp.bytes().await.unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK))
                .header("Content-Type", "text/plain")
                .body(Body::from(body_bytes))
                .unwrap())
        }
        Err(e) => Err(Error::Internal(format!("kubelet proxy error: {e}"))),
    }
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/attach
/// Attach to a running container (supports both SPDY and WebSocket)
pub async fn attach(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<AttachQuery>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    info!("Attaching to pod {}/{}", namespace, name);

    // Save user info for webhook check before auth moves ownership
    let webhook_user_info = rusternetes_common::admission::UserInfo {
        username: auth_ctx.user.username.clone(),
        uid: auth_ctx.user.uid.clone(),
        groups: auth_ctx.user.groups.clone(),
    };

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("attach");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Determine which container to attach to
    let container_name = if let Some(ref container) = query.container {
        container.clone()
    } else {
        // If no container specified, use the first container
        pod.spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .map(|c| c.name.clone())
            .ok_or_else(|| Error::InvalidResource("Pod has no containers".to_string()))?
    };

    // Verify the container exists in regular, init, or ephemeral containers
    let container_exists = pod
        .spec
        .as_ref()
        .map(|spec| {
            spec.containers.iter().any(|c| c.name == container_name)
                || spec
                    .init_containers
                    .as_ref()
                    .map(|ics| ics.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
                || spec
                    .ephemeral_containers
                    .as_ref()
                    .map(|ecs| ecs.iter().any(|c| c.name == container_name))
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    if !container_exists {
        return Err(Error::NotFound(format!(
            "Container {} not found in pod {}/{}",
            container_name, namespace, name
        )));
    }

    // Run admission webhooks for Connect operation (attach)
    {
        use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
        let gvk = GroupVersionKind {
            group: "".to_string(),
            version: "v1".to_string(),
            kind: "PodAttachOptions".to_string(),
        };
        let gvr = GroupVersionResource {
            group: "".to_string(),
            version: "v1".to_string(),
            resource: "pods/attach".to_string(),
        };
        let attach_options = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PodAttachOptions",
            "stdin": query.stdin,
            "stdout": query.stdout,
            "stderr": query.stderr,
            "tty": query.tty,
            "container": query.container.as_deref().unwrap_or("")
        });
        if let rusternetes_common::admission::AdmissionResponse::Deny(reason) = state
            .webhook_manager
            .run_validating_webhooks(
                &Operation::Connect,
                &gvk,
                &gvr,
                Some(&namespace),
                &name,
                Some(attach_options),
                None,
                &webhook_user_info,
            )
            .await?
        {
            return Err(Error::Forbidden(format!(
                "admission webhook denied the request: {}",
                reason
            )));
        }
    }

    // ── Proxy to kubelet streaming server ──

    // Resolve kubelet streaming endpoint from Node status
    let kubelet_addr = kubelet_proxy::kubelet_streaming_endpoint(&state, &pod)
        .await
        .map_err(|e| Error::Internal(format!("Failed to resolve kubelet: {e}")))?;

    // Build the kubelet streaming path
    let raw_query = req.uri().query().unwrap_or("");
    let kubelet_path = if raw_query.is_empty() {
        format!("/attach/{namespace}/{name}/{container_name}")
    } else {
        format!("/attach/{namespace}/{name}/{container_name}?{raw_query}")
    };

    // Check for SPDY upgrade first (kubectl's default protocol)
    let is_spdy = spdy::is_spdy_upgrade(req.headers());
    if is_spdy {
        info!("SPDY attach → kubelet at {kubelet_addr}{kubelet_path}");
        let upgrade = hyper::upgrade::on(req);
        return Ok(kubelet_proxy::spdy_proxy_response(
            upgrade,
            &kubelet_addr,
            &kubelet_path,
        ));
    }

    // Handle WebSocket upgrade
    if let Some(ws) = ws {
        info!("WebSocket attach → kubelet at {kubelet_addr}");
        return Ok(ws
            .protocols(["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"])
            .on_upgrade(move |socket| {
                streaming::handle_attach_websocket_via_kubelet(socket, kubelet_addr, kubelet_path)
            })
            .into_response());
    }

    // Plain HTTP: reverse-proxy to kubelet
    info!("HTTP attach proxy → kubelet at {kubelet_addr}{kubelet_path}");
    let target = format!("http://{kubelet_addr}{kubelet_path}");
    match reqwest::Client::new().get(&target).send().await {
        Ok(resp) => {
            let status = resp.status();
            let body_bytes = resp.bytes().await.unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK))
                .header("Content-Type", "text/plain")
                .body(Body::from(body_bytes))
                .unwrap())
        }
        Err(e) => Err(Error::Internal(format!("kubelet proxy error: {e}"))),
    }
}

/// GET/POST /api/v1/namespaces/{namespace}/pods/{name}/portforward
/// Forward ports to a pod (supports both SPDY and WebSocket)
pub async fn portforward(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<PortForwardQuery>,
    ws: Option<WebSocketUpgrade>,
    req: Request,
) -> Result<Response> {
    info!("Port forwarding to pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("portforward");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Get the pod
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Parse ports from query parameter
    if let Some(ref ports_str) = query.ports {
        if ports_str
            .split(',')
            .filter_map(|p| p.trim().parse::<u16>().ok())
            .next()
            .is_none()
        {
            return Err(Error::InvalidResource(
                "No valid ports specified for port forwarding".to_string(),
            ));
        }
    } else {
        return Err(Error::InvalidResource(
            "No ports specified for port forwarding".to_string(),
        ));
    }

    // ── Proxy to kubelet streaming server ──

    // Resolve kubelet streaming endpoint from Node status
    let kubelet_addr = kubelet_proxy::kubelet_streaming_endpoint(&state, &pod)
        .await
        .map_err(|e| Error::Internal(format!("Failed to resolve kubelet: {e}")))?;

    // Build the kubelet streaming path for port-forward
    let raw_query = req.uri().query().unwrap_or("");
    let kubelet_path = if raw_query.is_empty() {
        format!("/portForward/{namespace}/{name}")
    } else {
        format!("/portForward/{namespace}/{name}?{raw_query}")
    };

    // Check for SPDY upgrade first (kubectl's default protocol)
    let is_spdy = spdy::is_spdy_upgrade(req.headers());
    if is_spdy {
        info!("SPDY portforward → kubelet at {kubelet_addr}{kubelet_path}");
        let upgrade = hyper::upgrade::on(req);
        return Ok(kubelet_proxy::spdy_proxy_response(
            upgrade,
            &kubelet_addr,
            &kubelet_path,
        ));
    }

    // Handle WebSocket upgrade
    if let Some(ws) = ws {
        info!("WebSocket portforward → kubelet at {kubelet_addr}");
        return Ok(ws
            .protocols(["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"])
            .on_upgrade(move |socket| {
                streaming::handle_portforward_websocket_via_kubelet(
                    socket,
                    kubelet_addr,
                    kubelet_path,
                )
            })
            .into_response());
    }

    // Plain HTTP: reverse-proxy to kubelet
    info!("HTTP portforward proxy → kubelet at {kubelet_addr}{kubelet_path}");
    let target = format!("http://{kubelet_addr}{kubelet_path}");
    match reqwest::Client::new().get(&target).send().await {
        Ok(resp) => {
            let status = resp.status();
            let body_bytes = resp.bytes().await.unwrap_or_default();
            Ok(Response::builder()
                .status(StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK))
                .header("Content-Type", "text/plain")
                .body(Body::from(body_bytes))
                .unwrap())
        }
        Err(e) => Err(Error::Internal(format!("kubelet proxy error: {e}"))),
    }
}

/// POST /api/v1/namespaces/{namespace}/pods/{name}/binding
/// Bind a pod to a node
pub async fn create_binding(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Response> {
    info!("Creating binding for pod {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("binding");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Parse binding request
    let binding: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid binding format: {}", e)))?;

    // Extract target node from binding
    let node_name = binding
        .get("target")
        .and_then(|t: &serde_json::Value| t.get("name"))
        .and_then(|n: &serde_json::Value| n.as_str())
        .ok_or_else(|| Error::InvalidResource("Missing target.name in binding".to_string()))?;

    // Update pod's spec.nodeName to bind it to the node
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let mut pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Set the nodeName in spec
    if let Some(ref mut spec) = pod.spec {
        spec.node_name = Some(node_name.to_string());
    } else {
        return Err(Error::InvalidResource("Pod has no spec".to_string()));
    }

    // Update the pod in the storage
    state.storage.update(&pod_key, &pod).await?;

    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Binding",
                "metadata": {
                    "name": name,
                    "namespace": namespace
                },
                "target": {
                    "kind": "Node",
                    "name": node_name
                }
            })
            .to_string(),
        ))
        .unwrap())
}

/// POST /api/v1/namespaces/{namespace}/pods/{name}/eviction
/// Evict a pod
pub async fn create_eviction(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    body: String,
) -> Result<Response> {
    info!("Creating eviction for pod {}/{}", namespace, name);

    // Check authorization - eviction requires special permission
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "pods")
        .with_namespace(&namespace)
        .with_name(&name)
        .with_subresource("eviction");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(Error::Forbidden(reason));
        }
    }

    // Parse eviction request
    let eviction: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::InvalidResource(format!("Invalid eviction format: {}", e)))?;

    // Check if pod exists
    let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &name);
    let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;

    // Check PodDisruptionBudget constraints
    let pdb_prefix = rusternetes_storage::build_prefix("poddisruptionbudgets", Some(&namespace));
    let pdbs: Vec<rusternetes_common::resources::PodDisruptionBudget> =
        state.storage.list(&pdb_prefix).await.unwrap_or_default();

    // Get pod labels for matching
    let pod_labels = pod.metadata.labels.clone().unwrap_or_default();

    // Check if any PDB applies to this pod
    for pdb in &pdbs {
        // Check if PDB selector matches the pod
        let selector = &pdb.spec.selector;

        // Check if all match_labels are present in pod labels
        let matches = if let Some(ref match_labels) = selector.match_labels {
            match_labels
                .iter()
                .all(|(k, v)| pod_labels.get(k).map(|pv| pv == v).unwrap_or(false))
        } else if selector.match_expressions.is_some() {
            // TODO: Implement match_expressions support for more complex selectors
            // For now, treat match_expressions as non-matching
            false
        } else {
            // Empty selector (no match_labels or match_expressions) matches nothing
            false
        };

        if matches {
            // This PDB applies to our pod - compute disruptions_allowed inline
            // by counting matching healthy pods (don't rely solely on pre-computed status)
            let disruptions_allowed =
                compute_pdb_disruptions_allowed(state.storage.as_ref(), pdb, &namespace).await;

            if disruptions_allowed <= 0 {
                let current_healthy =
                    compute_pdb_healthy_count(state.storage.as_ref(), pdb, &namespace).await;
                let desired_healthy = compute_pdb_desired_healthy(pdb, current_healthy);
                let detail_msg = format!(
                    "Cannot evict pod as it would violate the pod's disruption budget. \
                    The disruption budget {} needs {} healthy pods and has {}, but we can only tolerate {} pod disruptions",
                    pdb.metadata.name,
                    desired_healthy,
                    current_healthy,
                    disruptions_allowed.max(0)
                );
                // Return 429 with details.causes containing DisruptionBudget
                let status_body = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "metadata": {},
                    "status": "Failure",
                    "message": detail_msg,
                    "reason": "TooManyRequests",
                    "details": {
                        "causes": [{
                            "reason": "DisruptionBudget",
                            "message": detail_msg
                        }]
                    },
                    "code": 429
                });
                return Ok(axum::response::Response::builder()
                    .status(axum::http::StatusCode::TOO_MANY_REQUESTS)
                    .header("Content-Type", "application/json")
                    .header("Retry-After", "10")
                    .body(axum::body::Body::from(
                        serde_json::to_string(&status_body).unwrap(),
                    ))
                    .unwrap()
                    .into_response());
            }

            info!(
                "Eviction for pod {}/{} passes PDB {} check (disruptions_allowed = {})",
                namespace, name, pdb.metadata.name, disruptions_allowed
            );

            // Update PDB's disruptedPods to record this eviction
            let mut updated_pdb = pdb.clone();
            let disrupted_pods = updated_pdb
                .status
                .get_or_insert(rusternetes_common::resources::PodDisruptionBudgetStatus {
                    current_healthy: 0,
                    desired_healthy: 0,
                    disruptions_allowed: 0,
                    expected_pods: 0,
                    observed_generation: None,
                    conditions: None,
                    disrupted_pods: None,
                })
                .disrupted_pods
                .get_or_insert_with(std::collections::HashMap::new);
            disrupted_pods.insert(name.clone(), chrono::Utc::now());

            // Also update disruptions_allowed in status
            if let Some(ref mut status) = updated_pdb.status {
                status.disruptions_allowed = disruptions_allowed - 1;
            }

            let pdb_key = rusternetes_storage::build_key(
                "poddisruptionbudgets",
                Some(&namespace),
                &pdb.metadata.name,
            );
            let _ = state.storage.update(&pdb_key, &updated_pdb).await;
        }
    }

    // Extract grace period if specified
    let grace_period_seconds = eviction
        .get("deleteOptions")
        .and_then(|opts: &serde_json::Value| opts.get("gracePeriodSeconds"))
        .and_then(|gp: &serde_json::Value| gp.as_i64());

    // Delete the pod (eviction is essentially a controlled delete)
    state.storage.delete(&pod_key).await?;

    info!(
        "Evicted pod {}/{} with grace period {:?}",
        namespace, name, grace_period_seconds
    );

    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {
                    "name": name,
                    "namespace": namespace
                }
            })
            .to_string(),
        ))
        .unwrap())
}

/// Check if a pod matches a PDB's label selector
fn pod_matches_pdb_selector(
    pod: &rusternetes_common::resources::Pod,
    selector: &rusternetes_common::types::LabelSelector,
) -> bool {
    let pod_labels = match &pod.metadata.labels {
        Some(labels) => labels,
        None => return false,
    };

    if let Some(ref match_labels) = selector.match_labels {
        for (key, value) in match_labels {
            if pod_labels.get(key) != Some(value) {
                return false;
            }
        }
    }

    true
}

/// Check if a pod is healthy (Running phase)
fn is_pod_healthy(pod: &rusternetes_common::resources::Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_ref())
        .map(|p| matches!(p, rusternetes_common::types::Phase::Running))
        .unwrap_or(false)
}

/// Compute the number of healthy pods matching a PDB's selector
async fn compute_pdb_healthy_count<S: Storage>(
    storage: &S,
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    namespace: &str,
) -> i32 {
    let pods_prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
    let all_pods: Vec<rusternetes_common::resources::Pod> =
        storage.list(&pods_prefix).await.unwrap_or_default();

    let matching_pods: Vec<&rusternetes_common::resources::Pod> = all_pods
        .iter()
        .filter(|p| pod_matches_pdb_selector(p, &pdb.spec.selector))
        .collect();

    matching_pods.iter().filter(|p| is_pod_healthy(p)).count() as i32
}

/// Compute the desired healthy count from a PDB spec
fn compute_pdb_desired_healthy(
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    total_pods: i32,
) -> i32 {
    if let Some(ref min_available) = pdb.spec.min_available {
        match min_available {
            rusternetes_common::resources::IntOrString::Int(value) => *value,
            rusternetes_common::resources::IntOrString::String(s) => {
                if let Some(stripped) = s.strip_suffix('%') {
                    if let Ok(percentage) = stripped.parse::<f64>() {
                        ((total_pods as f64) * (percentage / 100.0)).ceil() as i32
                    } else {
                        total_pods
                    }
                } else {
                    total_pods
                }
            }
        }
    } else if let Some(ref max_unavailable) = pdb.spec.max_unavailable {
        let max_unavailable_count = match max_unavailable {
            rusternetes_common::resources::IntOrString::Int(value) => *value,
            rusternetes_common::resources::IntOrString::String(s) => {
                if let Some(stripped) = s.strip_suffix('%') {
                    if let Ok(percentage) = stripped.parse::<f64>() {
                        ((total_pods as f64) * (percentage / 100.0)).floor() as i32
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
        };
        total_pods - max_unavailable_count
    } else {
        // No min_available or max_unavailable - default to requiring all pods
        total_pods
    }
}

/// Compute disruptions_allowed for a PDB by counting matching healthy pods
async fn compute_pdb_disruptions_allowed<S: Storage>(
    storage: &S,
    pdb: &rusternetes_common::resources::PodDisruptionBudget,
    namespace: &str,
) -> i32 {
    let pods_prefix = rusternetes_storage::build_prefix("pods", Some(namespace));
    let all_pods: Vec<rusternetes_common::resources::Pod> =
        storage.list(&pods_prefix).await.unwrap_or_default();

    let matching_pods: Vec<&rusternetes_common::resources::Pod> = all_pods
        .iter()
        .filter(|p| pod_matches_pdb_selector(p, &pdb.spec.selector))
        .collect();

    let total_pods = matching_pods.len() as i32;
    let healthy_pods = matching_pods.iter().filter(|p| is_pod_healthy(p)).count() as i32;
    let desired_healthy = compute_pdb_desired_healthy(pdb, total_pods);

    healthy_pods - desired_healthy
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec,
    };
    use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn make_pod(
        name: &str,
        namespace: &str,
        labels: HashMap<String, String>,
        running: bool,
    ) -> Pod {
        let phase = if running { "Running" } else { "Pending" };
        let labels_json = serde_json::to_value(&labels).unwrap();
        let json = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels_json
            },
            "spec": {
                "containers": [{
                    "name": "test",
                    "image": "nginx"
                }]
            },
            "status": {
                "phase": phase
            }
        });
        serde_json::from_value(json).unwrap()
    }

    fn make_pdb(
        name: &str,
        namespace: &str,
        min_available: i32,
        match_labels: HashMap<String, String>,
    ) -> PodDisruptionBudget {
        PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(min_available)),
                max_unavailable: None,
                selector: LabelSelector {
                    match_labels: Some(match_labels),
                    match_expressions: None,
                },
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        }
    }

    #[tokio::test]
    async fn test_pdb_blocks_eviction_then_allows_after_update() {
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-eviction-ns";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create a single running pod matching the PDB
        let pod = make_pod("test-pod-1", ns, labels.clone(), true);
        let pod_key = rusternetes_storage::build_key("pods", Some(ns), "test-pod-1");
        storage.create(&pod_key, &pod).await.unwrap();

        // Create a PDB with minAvailable=1 (so with 1 healthy pod, disruptions_allowed=0)
        let pdb = make_pdb("test-pdb", ns, 1, labels.clone());
        let pdb_key = rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "test-pdb");
        storage.create(&pdb_key, &pdb).await.unwrap();

        // Compute disruptions_allowed - should be 0 (1 healthy - 1 desired = 0)
        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 0,
            "Should not allow any disruptions with minAvailable=1 and 1 pod"
        );

        // Verify that the desired_healthy calculation is correct
        let healthy = compute_pdb_healthy_count(&*storage, &pdb_stored, ns).await;
        assert_eq!(healthy, 1);
        let desired = compute_pdb_desired_healthy(&pdb_stored, 1);
        assert_eq!(desired, 1);

        // Now update PDB to minAvailable=0 (allowing eviction)
        let mut updated_pdb = pdb_stored.clone();
        updated_pdb.spec.min_available = Some(IntOrString::Int(0));
        storage.update(&pdb_key, &updated_pdb).await.unwrap();

        // Now disruptions should be allowed (1 healthy - 0 desired = 1)
        let pdb_updated: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions_after = compute_pdb_disruptions_allowed(&*storage, &pdb_updated, ns).await;
        assert_eq!(
            disruptions_after, 1,
            "Should allow 1 disruption after lowering minAvailable to 0"
        );
    }

    #[tokio::test]
    async fn test_pdb_allows_eviction_with_extra_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-eviction-ns2";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create 3 running pods
        for i in 1..=3 {
            let name = format!("pod-{}", i);
            let pod = make_pod(&name, ns, labels.clone(), true);
            let key = rusternetes_storage::build_key("pods", Some(ns), &name);
            storage.create(&key, &pod).await.unwrap();
        }

        // PDB with minAvailable=2 and 3 healthy pods => disruptions_allowed=1
        let pdb = make_pdb("pdb-extra", ns, 2, labels.clone());
        let pdb_key = rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "pdb-extra");
        storage.create(&pdb_key, &pdb).await.unwrap();

        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 1,
            "Should allow 1 disruption with 3 healthy pods and minAvailable=2"
        );
    }

    #[tokio::test]
    async fn test_pdb_no_status_still_blocks() {
        // This tests the key bug fix: PDB with no status should still block evictions
        let storage = Arc::new(MemoryStorage::new());
        let ns = "test-no-status-ns";

        let labels = HashMap::from([("app".to_string(), "web".to_string())]);

        // Create 2 running pods
        for i in 1..=2 {
            let name = format!("pod-{}", i);
            let pod = make_pod(&name, ns, labels.clone(), true);
            let key = rusternetes_storage::build_key("pods", Some(ns), &name);
            storage.create(&key, &pod).await.unwrap();
        }

        // PDB with minAvailable=2 and NO status set (freshly created, controller hasn't reconciled)
        let pdb = make_pdb("pdb-no-status", ns, 2, labels.clone());
        assert!(pdb.status.is_none(), "PDB should have no status initially");

        let pdb_key =
            rusternetes_storage::build_key("poddisruptionbudgets", Some(ns), "pdb-no-status");
        storage.create(&pdb_key, &pdb).await.unwrap();

        let pdb_stored: PodDisruptionBudget = storage.get(&pdb_key).await.unwrap();
        let disruptions = compute_pdb_disruptions_allowed(&*storage, &pdb_stored, ns).await;
        assert_eq!(
            disruptions, 0,
            "PDB with no status should still block when minAvailable equals pod count"
        );
    }

    #[test]
    fn test_pod_matches_pdb_selector_basic() {
        let labels = HashMap::from([("app".to_string(), "web".to_string())]);
        let pod = make_pod("p1", "default", labels, true);
        let selector = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
            match_expressions: None,
        };
        assert!(pod_matches_pdb_selector(&pod, &selector));

        let wrong_selector = LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "api".to_string())])),
            match_expressions: None,
        };
        assert!(!pod_matches_pdb_selector(&pod, &wrong_selector));
    }

    #[test]
    fn test_is_pod_healthy_checks_phase() {
        let labels = HashMap::new();
        let running_pod = make_pod("p1", "default", labels.clone(), true);
        assert!(is_pod_healthy(&running_pod));

        let pending_pod = make_pod("p2", "default", labels, false);
        assert!(!is_pod_healthy(&pending_pod));
    }

    #[test]
    fn test_compute_desired_healthy_min_available() {
        let labels = HashMap::from([("app".to_string(), "web".to_string())]);
        let pdb = make_pdb("pdb", "default", 3, labels);
        assert_eq!(compute_pdb_desired_healthy(&pdb, 5), 3);
    }

    #[test]
    fn test_compute_desired_healthy_percentage() {
        let pdb = PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta::new("pdb").with_namespace("default"),
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::String("50%".to_string())),
                max_unavailable: None,
                selector: LabelSelector {
                    match_labels: Some(HashMap::new()),
                    match_expressions: None,
                },
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        };
        // 50% of 10 = 5
        assert_eq!(compute_pdb_desired_healthy(&pdb, 10), 5);
    }
}

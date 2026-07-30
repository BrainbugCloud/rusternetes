// SPDX-License-Identifier: Apache-2.0

//! Serde models for `container … --format json` output.
//!
//! These mirror Apple's `ContainerConfiguration` / `NetworkState` /
//! `ClientImage` JSON encodings (apple/container `Sources/ContainerClient`).
//! Every field is optional-with-default on purpose: the CLI's JSON is not a
//! stability contract, and a shim must not fail a whole `ListContainers`
//! because one new key appeared. [`crate::cli`] is the only module that
//! constructs these.

use std::collections::BTreeMap;

use serde::Deserialize;

/// `container list --format json` / `container inspect` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerJson {
    /// `"running"` | `"stopped"` | `"created"` (see [`Self::is_running`]).
    pub status: String,
    pub configuration: ContainerConfiguration,
    /// Live per-network attachment state; empty before the container starts.
    pub networks: Vec<NetworkAttachment>,
}

impl ContainerJson {
    pub fn id(&self) -> &str {
        &self.configuration.id
    }

    pub fn is_running(&self) -> bool {
        self.status.eq_ignore_ascii_case("running")
    }

    /// The container's first IPv4 address with the CIDR suffix stripped.
    pub fn ipv4(&self) -> Option<String> {
        self.networks
            .iter()
            .filter_map(|n| n.address.split('/').next())
            .find(|a| !a.is_empty() && a.contains('.'))
            .map(str::to_string)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerConfiguration {
    /// Apple uses the `--name` as the container id; we set it to the CRI
    /// name (see [`crate::naming`]).
    pub id: String,
    pub labels: BTreeMap<String, String>,
    pub image: ImageReference,
    pub init_process: InitProcess,
    pub resources: Resources,
    pub networks: Vec<NetworkRequest>,
    pub mounts: Vec<MountJson>,
    pub platform: Platform,
    pub runtime_handler: String,
    pub sysctls: BTreeMap<String, String>,
    pub dns: Option<DnsConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct DnsConfig {
    pub nameservers: Vec<String>,
    pub search_domains: Vec<String>,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageReference {
    pub reference: String,
    pub descriptor: Descriptor,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct Descriptor {
    pub digest: String,
    pub media_type: String,
    pub size: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct InitProcess {
    pub executable: String,
    pub arguments: Vec<String>,
    pub environment: Vec<String>,
    pub working_directory: String,
    pub terminal: bool,
    pub user: UserSpec,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct UserSpec {
    pub id: UserId,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct UserId {
    pub uid: i64,
    pub gid: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct Resources {
    pub cpus: i64,
    pub memory_in_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkRequest {
    pub network: String,
    pub options: BTreeMap<String, String>,
}

/// Live attachment state (top-level `networks` of a running container).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkAttachment {
    pub network: String,
    /// `192.168.77.3/24`
    pub address: String,
    pub gateway: String,
    pub hostname: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct MountJson {
    pub source: String,
    pub destination: String,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct Platform {
    pub os: String,
    pub architecture: String,
}

// ---- networks -------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkJson {
    pub id: String,
    pub state: String,
    pub config: NetworkConfig,
    pub status: Option<NetworkStatus>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkConfig {
    pub id: String,
    pub subnet: Option<String>,
    pub mode: String,
    pub labels: BTreeMap<String, String>,
    /// Core Foundation absolute time — seconds since 2001-01-01T00:00:00Z,
    /// *not* the Unix epoch. Parsed for completeness; sandbox timestamps come
    /// from the shim's own records (see [`crate::state`]).
    pub creation_date: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkStatus {
    pub address: String,
    pub gateway: String,
}

// ---- images ---------------------------------------------------------------

/// `container image list --format json` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageListEntry {
    pub reference: String,
    pub descriptor: Descriptor,
}

/// `container image inspect` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageInspect {
    pub name: String,
    pub variants: Vec<ImageVariant>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageVariant {
    pub platform: Platform,
    pub size: i64,
    pub config: Option<OciImageConfig>,
}

/// The OCI image config blob (`application/vnd.oci.image.config.v1+json`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct OciImageConfig {
    pub architecture: String,
    pub os: String,
    /// The nested `config` object, which uses Go-style capitalised keys.
    pub config: Option<OciConfigInner>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct OciConfigInner {
    #[serde(rename = "User")]
    pub user: String,
    #[serde(rename = "Env")]
    pub env: Vec<String>,
    #[serde(rename = "Cmd")]
    pub cmd: Vec<String>,
    #[serde(rename = "Entrypoint")]
    pub entrypoint: Vec<String>,
    #[serde(rename = "WorkingDir")]
    pub working_dir: String,
    #[serde(rename = "Labels")]
    pub labels: BTreeMap<String, String>,
}

// ---- stats ----------------------------------------------------------------

/// `container stats --format json --no-stream` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StatsJson {
    pub id: String,
    pub cpu_usage_usec: u64,
    pub memory_usage_bytes: u64,
    pub memory_limit_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
    pub num_processes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim `container list --format json` element (0.7.1).
    const LIST_JSON: &str = r#"[{
      "status": "running",
      "configuration": {
        "id": "podc2",
        "runtimeHandler": "container-runtime-linux",
        "resources": { "memoryInBytes": 1073741824, "cpus": 4 },
        "mounts": [],
        "dns": { "nameservers": [], "options": [], "searchDomains": [] },
        "initProcess": {
          "environment": ["PATH=/usr/bin"],
          "arguments": ["-c", "sleep 1"],
          "workingDirectory": "/",
          "user": { "id": { "uid": 0, "gid": 0 } },
          "executable": "sh",
          "terminal": false
        },
        "image": {
          "descriptor": { "mediaType": "application/vnd.oci.image.index.v1+json",
                          "digest": "sha256:28bd", "size": 9218 },
          "reference": "docker.io/library/alpine:latest"
        },
        "sysctls": {},
        "networks": [{ "options": { "hostname": "podc2" }, "network": "podtest1" }],
        "labels": { "io.kubernetes.pod.name": "p1" },
        "platform": { "os": "linux", "architecture": "arm64" }
      },
      "networks": [{ "hostname": "podc2", "address": "192.168.77.3/24",
                     "network": "podtest1", "gateway": "192.168.77.1" }]
    }]"#;

    #[test]
    fn parses_container_list() {
        let list: Vec<ContainerJson> = serde_json::from_str(LIST_JSON).unwrap();
        let c = &list[0];
        assert_eq!(c.id(), "podc2");
        assert!(c.is_running());
        assert_eq!(c.ipv4().as_deref(), Some("192.168.77.3"));
        assert_eq!(
            c.configuration.image.reference,
            "docker.io/library/alpine:latest"
        );
        assert_eq!(c.configuration.init_process.executable, "sh");
        assert_eq!(c.configuration.resources.cpus, 4);
        assert_eq!(
            c.configuration
                .labels
                .get("io.kubernetes.pod.name")
                .map(String::as_str),
            Some("p1")
        );
    }

    #[test]
    fn unknown_fields_and_missing_fields_are_tolerated() {
        // A stopped container has no live `networks`, and the CLI may grow keys.
        let json = r#"[{"status":"stopped","brandNewKey":42,
                        "configuration":{"id":"x","somethingElse":{"a":1}}}]"#;
        let list: Vec<ContainerJson> = serde_json::from_str(json).unwrap();
        assert_eq!(list[0].id(), "x");
        assert!(!list[0].is_running());
        assert_eq!(list[0].ipv4(), None);
    }

    #[test]
    fn parses_network_list() {
        let json = r#"[{"state":"running","id":"default",
            "config":{"id":"default","mode":"nat","labels":{},"creationDate":-978307200},
            "status":{"address":"192.168.64.0/24","gateway":"192.168.64.1"}}]"#;
        let nets: Vec<NetworkJson> = serde_json::from_str(json).unwrap();
        assert_eq!(nets[0].id, "default");
        assert_eq!(nets[0].status.as_ref().unwrap().gateway, "192.168.64.1");
        assert_eq!(nets[0].config.creation_date, Some(-978_307_200.0));
    }

    #[test]
    fn parses_image_inspect_variants() {
        let json = r#"[{"name":"docker.io/library/alpine:latest","variants":[
            {"platform":{"architecture":"arm64","os":"linux"},"size":3550000,
             "config":{"architecture":"arm64","os":"linux",
                       "config":{"Cmd":["/bin/sh"],"Env":["PATH=/usr/bin"],
                                 "WorkingDir":"/","User":"1000:1000"}}},
            {"platform":{"architecture":"amd64","os":"linux"},"size":3848024}]}]"#;
        let insp: Vec<ImageInspect> = serde_json::from_str(json).unwrap();
        assert_eq!(insp[0].variants.len(), 2);
        let arm = &insp[0].variants[0];
        assert_eq!(arm.size, 3550000);
        let inner = arm.config.as_ref().unwrap().config.as_ref().unwrap();
        assert_eq!(inner.cmd, vec!["/bin/sh"]);
        assert_eq!(inner.user, "1000:1000");
        // The amd64 variant carries no config blob — must not panic.
        assert!(insp[0].variants[1].config.is_none());
    }

    #[test]
    fn parses_stats() {
        let json = r#"[{"id":"podc1","cpuUsageUsec":32916,"memoryUsageBytes":2797568,
                        "memoryLimitBytes":1073741824,"networkRxBytes":17122,
                        "networkTxBytes":816,"blockReadBytes":2293760,
                        "blockWriteBytes":0,"numProcesses":2}]"#;
        let s: Vec<StatsJson> = serde_json::from_str(json).unwrap();
        assert_eq!(s[0].cpu_usage_usec, 32916);
        assert_eq!(s[0].memory_limit_bytes, 1073741824);
    }
}

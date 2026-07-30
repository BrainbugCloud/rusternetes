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
    pub configuration: ContainerConfiguration,
    /// Run state plus live network attachments.
    ///
    /// `container` **1.2.0** turned `status` from a bare string into an object
    /// (`{state, startedDate, networks}`) and moved the network list inside it.
    /// Parsing the old shape fails outright with `invalid type: map, expected a
    /// string`, which took out every spec that inspects a container.
    pub status: ContainerStatusJson,
}

/// The `status` object of `container inspect` (1.2.0+).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ContainerStatusJson {
    /// `"running"` | `"stopped"` | `"created"` (see [`ContainerJson::is_running`]).
    pub state: String,
    /// When the container was started, e.g. `2026-07-30T18:01:23Z`.
    ///
    /// New in 1.2.0 — 0.7.x reported no timestamps at all, which is why the shim
    /// keeps its own checkpoint store. Parsed but not yet used as a source of
    /// truth; see STATUS.md.
    pub started_date: Option<String>,
    /// Live per-network attachment state; empty before the container starts.
    pub networks: Vec<NetworkAttachment>,
}

impl ContainerJson {
    pub fn id(&self) -> &str {
        &self.configuration.id
    }

    pub fn is_running(&self) -> bool {
        self.status.state.eq_ignore_ascii_case("running")
    }

    /// The container's first IPv4 address with the CIDR suffix stripped.
    pub fn ipv4(&self) -> Option<String> {
        self.status
            .networks
            .iter()
            .filter_map(|n| n.ipv4_address.split('/').next())
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
    /// Heterogeneous by value type: `container` 1.2.0 reports
    /// `{"hostname": "<id>", "mtu": 1280}` — a string beside an integer — so this
    /// cannot be a `BTreeMap<String, String>`. Typing it as one made *every*
    /// `container inspect` fail with `invalid type: integer 1280, expected a
    /// string`, which took out the whole Container runtime suite. Nothing reads
    /// these options; they are parsed only so the surrounding object does.
    pub options: BTreeMap<String, serde_json::Value>,
}

/// Live attachment state (top-level `networks` of a running container).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkAttachment {
    pub network: String,
    /// `192.168.77.3/24`. Named `address` in 0.7.x; 1.2.0 splits the families
    /// into `ipv4Address` / `ipv6Address`.
    #[serde(alias = "address")]
    pub ipv4_address: String,
    #[serde(alias = "gateway")]
    pub ipv4_gateway: String,
    pub ipv6_address: String,
    pub mac_address: String,
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
    /// 0.7.x called this `config`; 1.2.0 renamed it to `configuration`.
    #[serde(alias = "config")]
    pub configuration: NetworkConfig,
    pub status: Option<NetworkStatus>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkConfig {
    /// 0.7.x called this `id`.
    #[serde(alias = "id")]
    pub name: String,
    pub mode: String,
    pub labels: BTreeMap<String, String>,
    /// The network plugin, e.g. `container-network-vmnet`. 1.2.0+.
    pub plugin: String,
    /// Left untyped on purpose: 1.2.0 reports ISO-8601 (`2026-07-30T17:28:25Z`)
    /// where 0.7.x reported a Core Foundation absolute time (a float, seconds
    /// since 2001-01-01Z). Nothing reads it — sandbox timestamps come from the
    /// shim's own records (see [`crate::state`]) — so accepting either keeps a
    /// runtime upgrade from failing the parse.
    pub creation_date: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkStatus {
    /// 0.7.x called these `address` and `gateway`.
    #[serde(alias = "address")]
    pub ipv4_subnet: String,
    #[serde(alias = "gateway")]
    pub ipv4_gateway: String,
    pub ipv6_subnet: String,
}

// ---- images ---------------------------------------------------------------

/// The `configuration` object both `image list` and `image inspect` wrap an
/// image's identity in.
///
/// `container` **1.2.0** moved `name` and `descriptor` here from the top level,
/// where 0.7.x reported them as `reference` and `descriptor`. Parsing the old
/// shape against 1.2.0 silently yields empty references, which surfaces as
/// "pulled X but it is not in the image store" — every Image Manager spec fails.
/// `reference` is kept as an alias so an older runtime still parses.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageConfiguration {
    #[serde(alias = "reference")]
    pub name: String,
    pub descriptor: Descriptor,
}

/// `container image list --format json` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageListEntry {
    pub id: String,
    pub configuration: ImageConfiguration,
}

impl ImageListEntry {
    /// The image reference (`name:tag` or `name@digest`).
    pub fn reference(&self) -> &str {
        &self.configuration.name
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.configuration.descriptor
    }
}

/// `container image inspect` element.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ImageInspect {
    pub id: String,
    pub configuration: ImageConfiguration,
    pub variants: Vec<ImageVariant>,
}

impl ImageInspect {
    pub fn name(&self) -> &str {
        &self.configuration.name
    }
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

    /// A real `container inspect` element from `container` **1.2.0**, captured
    /// verbatim into `testdata/`. Using the runtime's own output as the fixture is
    /// the point: the 1.2.0 upgrade silently changed four shapes at once, and a
    /// hand-written fixture would have kept agreeing with the old model.
    const INSPECT_JSON: &str = include_str!("../testdata/container-inspect-1.2.0.json");

    #[test]
    fn parses_container_inspect_1_2_0() {
        let c: ContainerJson = serde_json::from_str(INSPECT_JSON).unwrap();
        assert_eq!(c.id(), "fc51e1198130435bb3e0a682be082fe3");
        // `status` is an object in 1.2.0, so `state` is what decides running.
        assert!(c.is_running());
        assert_eq!(c.ipv4().as_deref(), Some("192.168.65.3"));
        assert_eq!(
            c.status.started_date.as_deref(),
            Some("2026-07-30T18:01:23Z")
        );
        assert_eq!(
            c.configuration.image.reference,
            "registry.k8s.io/e2e-test-images/busybox:1.29-2"
        );
        assert_eq!(c.configuration.resources.cpus, 4);
        assert_eq!(c.configuration.platform.os, "linux");
    }

    #[test]
    fn network_options_may_mix_value_types() {
        // 1.2.0 reports `{"hostname": "<id>", "mtu": 1280}` — a string beside an
        // integer. Typing the map as String->String failed every inspect.
        let c: ContainerJson = serde_json::from_str(INSPECT_JSON).unwrap();
        let opts = &c.configuration.networks[0].options;
        assert_eq!(opts.get("mtu").and_then(|v| v.as_u64()), Some(1280));
        assert_eq!(
            opts.get("hostname").and_then(|v| v.as_str()),
            Some("fc51e1198130435bb3e0a682be082fe3")
        );
    }

    #[test]
    fn parses_container_list() {
        // The list form is the same element in an array.
        let json = format!("[{INSPECT_JSON}]");
        let list: Vec<ContainerJson> = serde_json::from_str(&json).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].is_running());
    }

    #[test]
    fn zero_seven_network_attachment_field_names_still_parse() {
        // The 0.7.x names are kept as serde aliases, so an older runtime works.
        let json = r#"[{"status":{"state":"running","networks":[
            {"hostname":"podc2","address":"192.168.77.3/24",
             "network":"podtest1","gateway":"192.168.77.1"}]},
            "configuration":{"id":"podc2"}}]"#;
        let list: Vec<ContainerJson> = serde_json::from_str(json).unwrap();
        assert_eq!(list[0].ipv4().as_deref(), Some("192.168.77.3"));
    }

    #[test]
    fn unknown_fields_and_missing_fields_are_tolerated() {
        // A stopped container has no live `networks`, and the CLI may grow keys.
        let json = r#"[{"status":{"state":"stopped"},"brandNewKey":42,
                        "configuration":{"id":"x","somethingElse":{"a":1}}}]"#;
        let list: Vec<ContainerJson> = serde_json::from_str(json).unwrap();
        assert_eq!(list[0].id(), "x");
        assert!(!list[0].is_running());
        assert_eq!(list[0].ipv4(), None);
    }

    #[test]
    fn a_container_with_no_status_object_is_not_running() {
        let json = r#"[{"configuration":{"id":"x"}}]"#;
        let list: Vec<ContainerJson> = serde_json::from_str(json).unwrap();
        assert!(!list[0].is_running());
    }

    #[test]
    fn parses_network_list_1_2_0() {
        let json = r#"[{"id":"default",
            "configuration":{"name":"default","mode":"nat","labels":{},
                             "plugin":"container-network-vmnet",
                             "creationDate":"2026-07-30T17:28:25Z","options":{}},
            "status":{"ipv4Gateway":"192.168.64.1","ipv4Subnet":"192.168.64.0/24",
                      "ipv6Subnet":"fdfe:2925:eca4:95c3::/64"}}]"#;
        let nets: Vec<NetworkJson> = serde_json::from_str(json).unwrap();
        assert_eq!(nets[0].id, "default");
        assert_eq!(nets[0].configuration.name, "default");
        assert_eq!(nets[0].configuration.plugin, "container-network-vmnet");
        let status = nets[0].status.as_ref().unwrap();
        assert_eq!(status.ipv4_gateway, "192.168.64.1");
        assert_eq!(status.ipv4_subnet, "192.168.64.0/24");
    }

    #[test]
    fn parses_network_list_zero_seven_shape() {
        // `config`/`id`/`address`/`gateway` are aliases; the float creationDate
        // parses because the field is deliberately untyped.
        let json = r#"[{"state":"running","id":"default",
            "config":{"id":"default","mode":"nat","labels":{},"creationDate":-978307200},
            "status":{"address":"192.168.64.0/24","gateway":"192.168.64.1"}}]"#;
        let nets: Vec<NetworkJson> = serde_json::from_str(json).unwrap();
        assert_eq!(nets[0].id, "default");
        assert_eq!(nets[0].configuration.name, "default");
        assert_eq!(
            nets[0].status.as_ref().unwrap().ipv4_gateway,
            "192.168.64.1"
        );
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

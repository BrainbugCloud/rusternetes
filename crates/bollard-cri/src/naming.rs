// SPDX-License-Identifier: Apache-2.0

//! Container naming (port of cri-dockerd `naming.go`).
//!
//! Docker rejects duplicate names, which gives us create-idempotency for
//! free; the name also encodes the CRI metadata so state can be recovered
//! from Docker alone:
//!
//! ```text
//! k8s_POD_<podname>_<namespace>_<uid>_<attempt>          (sandbox)
//! k8s_<container>_<podname>_<namespace>_<uid>_<attempt>  (app container)
//! ```
//!
//! Kubernetes object names cannot contain `_`, so splitting on it is safe.
//! A 7th part is tolerated when parsing: [`randomize_name`] appends a random
//! suffix to work around Docker's stale-name-index bug (cri-dockerd
//! `randomizeName`).

use cri_proto::v1::{ContainerMetadata, PodSandboxMetadata};
use cri_server::error::{Error, Result};

const PREFIX: &str = "k8s";
pub const SANDBOX_INFRA_NAME: &str = "POD";

pub fn sandbox_name(meta: &PodSandboxMetadata) -> String {
    format!(
        "{PREFIX}_{SANDBOX_INFRA_NAME}_{}_{}_{}_{}",
        meta.name, meta.namespace, meta.uid, meta.attempt
    )
}

#[allow(dead_code)] // used from B3 on (app containers)
pub fn container_name(meta: &ContainerMetadata, sandbox_meta: &PodSandboxMetadata) -> String {
    format!(
        "{PREFIX}_{}_{}_{}_{}_{}",
        meta.name, sandbox_meta.name, sandbox_meta.namespace, sandbox_meta.uid, meta.attempt
    )
}

/// Randomize a container name after a create conflict against a container
/// that no longer exists (Docker name-index bug).
pub fn randomize_name(name: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{name}_{nonce:08x}")
}

fn parts(name: &str) -> Result<Vec<&str>> {
    // Docker inspect/list prefixes names with '/'.
    let name = name.strip_prefix('/').unwrap_or(name);
    let parts: Vec<&str> = name.split('_').collect();
    // 7 parts = randomized suffix (tolerated, see randomize_name).
    if !(parts.len() == 6 || parts.len() == 7) || parts[0] != PREFIX {
        return Err(Error::Internal(format!(
            "container name {name:?} is not CRI-managed"
        )));
    }
    Ok(parts)
}

pub fn parse_sandbox_name(name: &str) -> Result<PodSandboxMetadata> {
    let parts = parts(name)?;
    if parts[1] != SANDBOX_INFRA_NAME {
        return Err(Error::Internal(format!(
            "container name {name:?} is not a sandbox"
        )));
    }
    Ok(PodSandboxMetadata {
        name: parts[2].to_string(),
        namespace: parts[3].to_string(),
        uid: parts[4].to_string(),
        attempt: parts[5].parse().unwrap_or(0),
    })
}

#[allow(dead_code)] // used from B3 on (app containers)
pub fn parse_container_name(name: &str) -> Result<ContainerMetadata> {
    let parts = parts(name)?;
    if parts[1] == SANDBOX_INFRA_NAME {
        return Err(Error::Internal(format!(
            "container name {name:?} is a sandbox, not an app container"
        )));
    }
    Ok(ContainerMetadata {
        name: parts[1].to_string(),
        attempt: parts[5].parse().unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sandbox_meta() -> PodSandboxMetadata {
        PodSandboxMetadata {
            name: "web".into(),
            namespace: "default".into(),
            uid: "uid-1234".into(),
            attempt: 2,
        }
    }

    #[test]
    fn sandbox_name_round_trip() {
        let name = sandbox_name(&sandbox_meta());
        assert_eq!(name, "k8s_POD_web_default_uid-1234_2");
        assert_eq!(parse_sandbox_name(&name).unwrap(), sandbox_meta());
        assert_eq!(
            parse_sandbox_name("/k8s_POD_web_default_uid-1234_2").unwrap(),
            sandbox_meta()
        );
    }

    #[test]
    fn container_name_round_trip() {
        let meta = ContainerMetadata {
            name: "app".into(),
            attempt: 0,
        };
        let name = container_name(&meta, &sandbox_meta());
        assert_eq!(name, "k8s_app_web_default_uid-1234_0");
        assert_eq!(parse_container_name(&name).unwrap(), meta);
        assert!(parse_sandbox_name(&name).is_err());
    }

    #[test]
    fn tolerates_randomized_suffix() {
        let name = randomize_name(&sandbox_name(&sandbox_meta()));
        assert_eq!(parse_sandbox_name(&name).unwrap(), sandbox_meta());
    }

    #[test]
    fn rejects_foreign_names() {
        assert!(parse_sandbox_name("random-container").is_err());
        assert!(parse_container_name("k8s_POD_web_default_uid_0").is_err());
    }
}

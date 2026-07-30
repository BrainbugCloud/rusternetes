// SPDX-License-Identifier: Apache-2.0

//! Object identity.
//!
//! # Why ids are short and opaque
//!
//! cri-dockerd and `bollard-cri` encode the CRI metadata into the runtime's
//! container *name*
//! (`k8s_<container>_<pod>_<namespace>_<uid>_<attempt>`), which makes Docker's
//! duplicate-name rejection double as create-idempotency. That scheme cannot be
//! used here.
//!
//! Apple's runtime uses `--name` **as the container id** and embeds it in a
//! length-limited system name (the per-container launchd/XPC endpoint). Past
//! roughly 64 characters, `container start --attach` fails with
//! `internalError: Error Domain=NSPOSIXErrorDomain Code=22 "Invalid argument"`
//! — measured on 0.7.1 with a 12-character `$HOME`: 64 chars work, 66 do not,
//! and the budget shrinks as the home-directory path grows. A cri-dockerd-style
//! name built from a real pod is far longer than that: critest's own pod names
//! push it past 230 characters, so *every* container would fail to start.
//!
//! So ids here are opaque 32-hex-character strings, as containerd's are (just
//! shorter), and identity lives in two places instead of the name:
//!
//! - the checkpoint [`Store`](crate::state::Store) — authoritative, and what
//!   create-idempotency is resolved against, and
//! - a few Apple container labels ([`AppleBackend::discovery_labels`]) — enough
//!   to recognise and clean up an orphan from `container list` alone.
//!
//! [`AppleBackend::discovery_labels`]: crate::backend::AppleBackend::discovery_labels

use cri_proto::v1::PodSandboxMetadata;

/// Longest container id that reliably works with `container start --attach`.
///
/// Empirical, and deliberately conservative: the true ceiling was 64–65 on a
/// host with `$HOME=/Users/e28b0` (12 chars), and it falls by one for every
/// extra character in the home path. [`new_id`] emits 32 characters, leaving
/// room for a home path some 30 characters longer before this bound bites.
pub const MAX_CONTAINER_ID_LEN: usize = 64;

/// A fresh opaque object id: 32 lowercase hex characters (128 random bits).
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// The guest hostname for a pod, following the kubelet's convention: the pod
/// name truncated to the 63-character DNS label limit.
///
/// Recorded for completeness only — `container create` has no `--hostname`, so
/// Apple derives the guest hostname from the container id instead (see the
/// README's Deviations table).
pub fn pod_hostname(meta: &PodSandboxMetadata) -> String {
    meta.name.chars().take(63).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_short_unique_hex() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
        assert!(
            a.len() <= MAX_CONTAINER_ID_LEN,
            "id must fit Apple's name-length budget"
        );
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "unexpected id charset: {a}"
        );
    }

    #[test]
    fn ids_leave_headroom_for_long_home_paths() {
        // The measured ceiling was ~64 chars with a 12-char $HOME. A 32-char id
        // must still fit for a substantially longer home path.
        let headroom = MAX_CONTAINER_ID_LEN - new_id().len();
        assert!(headroom >= 30, "only {headroom} chars of headroom");
    }

    #[test]
    fn hostname_truncates_to_dns_label_limit() {
        let long = PodSandboxMetadata {
            name: "a".repeat(80),
            ..Default::default()
        };
        assert_eq!(pod_hostname(&long).len(), 63);
        assert_eq!(
            pod_hostname(&PodSandboxMetadata {
                name: "web".into(),
                ..Default::default()
            }),
            "web"
        );
    }
}

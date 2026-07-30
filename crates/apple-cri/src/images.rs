// SPDX-License-Identifier: Apache-2.0

//! The CRI image service over `container image …`.
//!
//! Apple's image store is content-addressed and multi-platform: `image list`
//! reports only a reference plus the *index* descriptor, and `image inspect`
//! expands each reference into per-platform variants carrying the size and the
//! OCI config blob. The index digest is used as the CRI image id, and only the
//! variant matching the node's platform is reported — so `Image.size` is the
//! size of the image that would actually run here, not the sum of all
//! architectures.
//!
//! Infra images Apple ships for itself (`ghcr.io/apple/containerization/vminit`,
//! the builder shim) are filtered out: they are not pullable workloads and must
//! not appear in the kubelet's image list, which drives image garbage
//! collection.

use async_trait::async_trait;
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::ImageBackend;

use crate::backend::AppleBackend;
use crate::model::{ImageInspect, ImageVariant};

/// References Apple manages for its own runtime, never CRI workloads.
const INFRA_IMAGE_PREFIXES: &[&str] = &[
    "ghcr.io/apple/containerization/vminit",
    "ghcr.io/apple/container-builder-shim/",
];

fn is_infra_image(reference: &str) -> bool {
    INFRA_IMAGE_PREFIXES
        .iter()
        .any(|p| reference.starts_with(p))
}

/// What the shim needs to know about an image to create a container from it.
#[derive(Debug, Clone, Default)]
pub struct ResolvedImage {
    /// CRI image id — the index digest.
    pub id: String,
    /// Fully-qualified reference as Apple stores it.
    pub reference: String,
    /// The image ENTRYPOINT and CMD, needed to compute the effective argv
    /// (see [`crate::container::effective_argv`]). The image's env, workdir
    /// and user are *not* carried here: Apple applies those from the image
    /// config itself, and this shim only ever overrides them.
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
}

/// Normalise a CRI image reference the way a registry client would, so
/// `alpine`, `alpine:latest` and `docker.io/library/alpine:latest` all match
/// the same stored image.
pub fn normalize_reference(reference: &str) -> String {
    if reference.starts_with("sha256:") {
        return reference.to_string();
    }
    let (name, tag_part) = split_tag(reference);
    let has_registry = name
        .split('/')
        .next()
        .is_some_and(|first| first.contains('.') || first.contains(':') || first == "localhost");
    let qualified = if has_registry {
        name.to_string()
    } else if name.contains('/') {
        format!("docker.io/{name}")
    } else {
        format!("docker.io/library/{name}")
    };
    match tag_part {
        Some(t) => format!("{qualified}{t}"),
        None => format!("{qualified}:latest"),
    }
}

/// Split off a `:tag` or `@digest` suffix, ignoring a registry port colon.
fn split_tag(reference: &str) -> (&str, Option<&str>) {
    if let Some(at) = reference.find('@') {
        return (&reference[..at], Some(&reference[at..]));
    }
    match reference.rfind(':') {
        // A colon before the last '/' is a registry port, not a tag.
        Some(colon) if !reference[colon..].contains('/') => {
            (&reference[..colon], Some(&reference[colon..]))
        }
        _ => (reference, None),
    }
}

impl AppleBackend {
    /// The `os/arch` this node runs containers as.
    pub(crate) fn platform(&self) -> String {
        format!("linux/{}", self.config.arch)
    }

    /// Pick the variant matching this node, else the only one, else none.
    fn best_variant<'a>(&self, insp: &'a ImageInspect) -> Option<&'a ImageVariant> {
        let want = &self.config.arch;
        insp.variants
            .iter()
            .find(|v| {
                v.platform.os == "linux" && &v.platform.architecture == want && v.config.is_some()
            })
            .or_else(|| {
                insp.variants
                    .iter()
                    .find(|v| v.platform.os == "linux" && &v.platform.architecture == want)
            })
            .or_else(|| insp.variants.iter().find(|v| v.config.is_some()))
            .or_else(|| insp.variants.first())
    }

    /// Every image in the store, keyed by CRI image id (the index digest).
    ///
    /// This grouping *is* the CRI model and it is not optional: CRI reports one
    /// `Image` per id carrying all of its `repo_tags`, whereas Apple's
    /// `image list` reports one row per reference. Three tags on one image must
    /// therefore collapse into a single entry with three tags — which is exactly
    /// what critest's "should get exactly 3 repoTags in the result image" and
    /// "removing image by one tag should remove all tags" specs assert.
    pub(crate) async fn image_groups(&self) -> Result<Vec<ImageGroup>> {
        let mut groups: std::collections::BTreeMap<String, ImageGroup> =
            std::collections::BTreeMap::new();
        for entry in self.cli.list_images().await? {
            if is_infra_image(entry.reference()) {
                continue;
            }
            groups
                .entry(entry.descriptor().digest.clone())
                .or_insert_with(|| ImageGroup {
                    id: entry.descriptor().digest.clone(),
                    listed: Vec::new(),
                })
                .listed
                .push(entry.configuration.name);
        }
        Ok(groups.into_values().collect())
    }

    /// The group a CRI [`ImageSpec`] refers to, by tag, `name@digest`, or id.
    pub(crate) async fn find_image_group(&self, want: &str) -> Result<Option<ImageGroup>> {
        Ok(self
            .image_groups()
            .await?
            .into_iter()
            .find(|g| g.matches(want)))
    }

    /// Resolve `reference` from the local store; `Ok(None)` when absent.
    ///
    /// Accepts anything CRI may pass: a short name, a fully-qualified tag, a
    /// `name@digest`, or a bare image id (`sha256:…`).
    pub(crate) async fn resolve_image(&self, reference: &str) -> Result<Option<ResolvedImage>> {
        let Some(group) = self.find_image_group(reference).await? else {
            return Ok(None);
        };
        // Apple's `image inspect` is reference-based, so probe through one of
        // the group's references rather than its id.
        let Some(probe) = group.probe_reference() else {
            return Ok(None);
        };
        let Some(insp) = self.cli.inspect_image(probe).await? else {
            return Ok(None);
        };
        let inner = self
            .best_variant(&insp)
            .and_then(|v| v.config.as_ref())
            .and_then(|c| c.config.as_ref());
        Ok(Some(ResolvedImage {
            id: group.id.clone(),
            reference: insp.name().to_string(),
            entrypoint: inner.map(|c| c.entrypoint.clone()).unwrap_or_default(),
            cmd: inner.map(|c| c.cmd.clone()).unwrap_or_default(),
        }))
    }

    /// Build the CRI [`Image`] for a group.
    async fn cri_image_for_group(&self, group: &ImageGroup) -> Result<Option<Image>> {
        let Some(probe) = group.probe_reference() else {
            return Ok(None);
        };
        let Some(insp) = self.cli.inspect_image(probe).await? else {
            return Ok(None);
        };
        let variant = self.best_variant(&insp);
        let inner = variant
            .and_then(|v| v.config.as_ref())
            .and_then(|c| c.config.as_ref());
        let (uid, username) = parse_image_user(inner.map(|c| c.user.as_str()).unwrap_or(""));
        Ok(Some(Image {
            id: group.id.clone(),
            repo_tags: group.tags(),
            repo_digests: group.repo_digests(),
            size: variant.map(|v| v.size.max(0) as u64).unwrap_or(0),
            uid,
            username,
            spec: Some(ImageSpec {
                image: probe.clone(),
                ..Default::default()
            }),
            pinned: false,
        }))
    }
}

/// One image as CRI sees it: an id (the index digest) plus every stored
/// reference that resolves to it.
///
/// Only [`Self::listed`] holds real store entries. Everything else is derived,
/// which matters for removal: deleting a *synthesized* digest reference would be
/// a no-op, and forgetting a real one would leave the image resolvable after
/// `RemoveImage` claimed success.
#[derive(Debug, Clone, Default)]
pub(crate) struct ImageGroup {
    /// The CRI image id — Apple's index digest.
    pub id: String,
    /// Every reference `container image list` reported for this digest.
    pub listed: Vec<String>,
}

impl ImageGroup {
    /// The listed references that are tags (`name:tag`), in listing order.
    pub fn tags(&self) -> Vec<String> {
        self.listed
            .iter()
            .filter(|r| !r.contains('@'))
            .cloned()
            .collect()
    }

    /// CRI `repo_digests`: the listed digest references, plus a synthesized
    /// `name@digest` for each distinct tagged repository (containerd reports
    /// these too, so consumers can address a tagged image by digest).
    pub fn repo_digests(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .listed
            .iter()
            .filter(|r| r.contains('@'))
            .cloned()
            .collect();
        if self.id.is_empty() {
            return out;
        }
        for tag in self.tags() {
            let synthesized = format!("{}@{}", split_tag(&tag).0, self.id);
            if !out.contains(&synthesized) {
                out.push(synthesized);
            }
        }
        out
    }

    /// A reference `container image inspect` will accept; a tag by preference.
    pub fn probe_reference(&self) -> Option<&String> {
        self.listed
            .iter()
            .find(|r| !r.contains('@'))
            .or_else(|| self.listed.first())
    }

    /// Every real store entry that has to be deleted to remove this image.
    pub fn all_references(&self) -> &[String] {
        &self.listed
    }

    /// Whether `want` names this image, in any of the spellings CRI uses.
    pub fn matches(&self, want: &str) -> bool {
        if want.is_empty() {
            return false;
        }
        // A bare digest is the image id.
        if want == self.id {
            return true;
        }
        if self.listed.iter().any(|r| r == want) {
            return true;
        }
        // `name@digest` matches on the digest, whatever the repository spelling.
        if let Some((_, digest)) = want.split_once('@') {
            if digest == self.id {
                return true;
            }
        }
        let normalized = normalize_reference(want);
        self.listed.contains(&normalized) || self.repo_digests().contains(&normalized)
    }
}

/// Split an OCI image `User` into CRI's `(uid, username)`.
///
/// The field may be `uid`, `uid:gid`, `name`, or `name:group`. CRI wants a
/// numeric uid when it is numeric and a username otherwise — and only the part
/// before the group separator, so `"www-data:group"` is the username
/// `www-data` (asserted by critest's "image status get image fields should not
/// have Uid|Username empty").
fn parse_image_user(user: &str) -> (Option<Int64Value>, String) {
    let first = user.split(':').next().unwrap_or("");
    if first.is_empty() {
        return (None, String::new());
    }
    match first.parse::<i64>() {
        Ok(n) => (Some(Int64Value { value: n }), String::new()),
        Err(_) => (None, first.to_string()),
    }
}

#[async_trait]
impl ImageBackend for AppleBackend {
    async fn list_images(&self, filter: Option<ImageFilter>) -> Result<Vec<Image>> {
        let want = filter.and_then(|f| f.image).map(|s| s.image);
        let mut out = Vec::new();
        for group in self.image_groups().await? {
            if let Some(want) = &want {
                if !want.is_empty() && !group.matches(want) {
                    continue;
                }
            }
            match self.cri_image_for_group(&group).await {
                Ok(Some(img)) => out.push(img),
                Ok(None) => {}
                // One unreadable image must not fail the whole listing — the
                // kubelet's image GC calls this every sync.
                Err(err) => tracing::warn!(image = %group.id, %err, "skipping image"),
            }
        }
        Ok(out)
    }

    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>> {
        let Some(group) = self.find_image_group(&image.image).await? else {
            return Ok(None);
        };
        self.cri_image_for_group(&group).await
    }

    async fn pull_image(
        &self,
        image: &ImageSpec,
        auth: Option<AuthConfig>,
        _sandbox_config: Option<PodSandboxConfig>,
    ) -> Result<String> {
        let reference = normalize_reference(&image.image);

        // CRI passes credentials per pull; Apple only has a global,
        // keychain-backed `registry login`. Authenticated pulls therefore
        // mutate host state — see README "Deviations".
        if let Some(auth) = auth {
            if !auth.username.is_empty() {
                let server = reference.split('/').next().unwrap_or("docker.io");
                tracing::warn!(
                    server,
                    "CRI per-pull credentials applied via `container registry login` \
                     (global, keychain-backed)"
                );
                self.cli
                    .registry_login(server, &auth.username, &auth.password)
                    .await?;
            } else if !auth.identity_token.is_empty() || !auth.auth.is_empty() {
                return Err(Error::Unimplemented(
                    "apple-cri: token/basic-auth pull credentials are not supported; \
                     use `container registry login`"
                        .into(),
                ));
            }
        }

        let platform = self.platform();
        self.cli.pull_image(&reference, Some(&platform)).await?;

        // CRI wants the image *ref* back — the id the kubelet will match
        // against `ContainerStatus.image_ref`.
        match self.resolve_image(&reference).await? {
            Some(r) if !r.id.is_empty() => Ok(r.id),
            Some(r) => Ok(r.reference),
            None => Err(Error::Internal(format!(
                "pulled {reference} but it is not in the image store"
            ))),
        }
    }

    async fn remove_image(&self, image: &ImageSpec) -> Result<()> {
        // Removing an absent image is Ok (CRI idempotency).
        let Some(group) = self.find_image_group(&image.image).await? else {
            return Ok(());
        };
        // `RemoveImage` takes an image *id*, and removing an id must remove
        // every tag pointing at it — critest asserts exactly this ("removing
        // image by one tag should remove all tags"). Apple's `image delete` only
        // untags one reference, so every reference in the group is deleted.
        for reference in group.all_references() {
            self.cli.remove_image(reference).await?;
        }
        Ok(())
    }

    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>> {
        let dir = self.config.image_store_dir();
        let used = dir_size(&dir);
        Ok(vec![FilesystemUsage {
            timestamp: crate::state::now_nanos(),
            fs_id: Some(FilesystemIdentifier {
                mountpoint: dir.to_string_lossy().to_string(),
            }),
            used_bytes: Some(UInt64Value { value: used }),
            inodes_used: Some(UInt64Value { value: 0 }),
        }])
    }
}

/// Recursive apparent size of a directory tree; 0 if unreadable.
fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_size(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_docker_hub_short_names() {
        assert_eq!(
            normalize_reference("alpine"),
            "docker.io/library/alpine:latest"
        );
        assert_eq!(
            normalize_reference("alpine:3.22"),
            "docker.io/library/alpine:3.22"
        );
        assert_eq!(
            normalize_reference("myorg/app"),
            "docker.io/myorg/app:latest"
        );
        assert_eq!(
            normalize_reference("docker.io/library/alpine:latest"),
            "docker.io/library/alpine:latest"
        );
    }

    #[test]
    fn normalizes_registries_ports_and_digests() {
        assert_eq!(
            normalize_reference("registry.k8s.io/pause:3.10"),
            "registry.k8s.io/pause:3.10"
        );
        // A port colon is not a tag separator.
        assert_eq!(
            normalize_reference("localhost:5000/app"),
            "localhost:5000/app:latest"
        );
        assert_eq!(
            normalize_reference("localhost:5000/app:v1"),
            "localhost:5000/app:v1"
        );
        // Digest references keep their form.
        assert_eq!(
            normalize_reference("alpine@sha256:abc"),
            "docker.io/library/alpine@sha256:abc"
        );
        assert_eq!(normalize_reference("sha256:abc"), "sha256:abc");
    }

    #[test]
    fn splits_tags_from_digests() {
        assert_eq!(split_tag("alpine:3.22"), ("alpine", Some(":3.22")));
        assert_eq!(split_tag("alpine"), ("alpine", None));
        assert_eq!(split_tag("a@sha256:x"), ("a", Some("@sha256:x")));
        assert_eq!(split_tag("host:5000/a"), ("host:5000/a", None));
    }

    /// Three tags on one image, as critest's tag specs create.
    fn three_tag_group() -> ImageGroup {
        ImageGroup {
            id: "sha256:73e6".into(),
            listed: vec![
                "gcr.io/k8s-staging-cri-tools/test-image-tags:1".into(),
                "gcr.io/k8s-staging-cri-tools/test-image-tags:2".into(),
                "gcr.io/k8s-staging-cri-tools/test-image-tags:3".into(),
            ],
        }
    }

    #[test]
    fn a_group_matches_every_spelling_cri_uses() {
        let g = three_tag_group();
        // By id (a bare digest).
        assert!(g.matches("sha256:73e6"));
        // By exact tag.
        assert!(g.matches("gcr.io/k8s-staging-cri-tools/test-image-tags:2"));
        // By a synthesized digest reference.
        assert!(g.matches("gcr.io/k8s-staging-cri-tools/test-image-tags@sha256:73e6"));
        // By a digest reference under a *different* repository spelling: the
        // digest is what identifies the image.
        assert!(g.matches("gcr.io/other/name@sha256:73e6"));

        assert!(!g.matches("sha256:dead"));
        assert!(!g.matches("gcr.io/k8s-staging-cri-tools/test-image-tags:4"));
        assert!(!g.matches(""));
    }

    #[test]
    fn a_tagged_group_reports_all_tags_and_one_synthesized_digest() {
        let g = three_tag_group();
        assert_eq!(g.tags().len(), 3, "CRI reports one image with all its tags");
        // One synthesized digest for the single repository.
        assert_eq!(
            g.repo_digests(),
            ["gcr.io/k8s-staging-cri-tools/test-image-tags@sha256:73e6"]
        );
        // `image inspect` needs a reference, never the bare id.
        assert_eq!(
            g.probe_reference().map(String::as_str),
            Some("gcr.io/k8s-staging-cri-tools/test-image-tags:1")
        );
    }

    #[test]
    fn removal_covers_real_digest_references_but_not_synthesized_ones() {
        // An image pulled by digest has a real `name@digest` store entry.
        // Removing the image must delete it — leaving it behind would keep the
        // image resolvable after RemoveImage reported success (critest's
        // "public image with digest should be pulled and removed").
        let by_digest = ImageGroup {
            id: "sha256:9700".into(),
            listed: vec!["gcr.io/k8s-staging-cri-tools/test-image-digest@sha256:9700".into()],
        };
        assert_eq!(by_digest.all_references(), by_digest.listed);
        assert!(by_digest.tags().is_empty());
        assert_eq!(
            by_digest.probe_reference().map(String::as_str),
            Some("gcr.io/k8s-staging-cri-tools/test-image-digest@sha256:9700")
        );

        // A tagged image's synthesized digest is NOT a store entry, so it must
        // not appear in the deletion set.
        let tagged = three_tag_group();
        assert_eq!(tagged.all_references().len(), 3);
        assert!(!tagged.all_references().iter().any(|r| r.contains('@')));
    }

    #[test]
    fn image_user_splits_uid_and_username_before_the_group() {
        // Numeric uid, with and without a group.
        assert_eq!(
            parse_image_user("1002"),
            (Some(Int64Value { value: 1002 }), String::new())
        );
        assert_eq!(
            parse_image_user("1003:2000"),
            (Some(Int64Value { value: 1003 }), String::new())
        );
        // A username, with and without a group — the group is not part of it.
        assert_eq!(parse_image_user("www-data"), (None, "www-data".to_string()));
        assert_eq!(
            parse_image_user("www-data:group"),
            (None, "www-data".to_string())
        );
        // Unset.
        assert_eq!(parse_image_user(""), (None, String::new()));
        assert_eq!(parse_image_user(":group"), (None, String::new()));
    }

    #[test]
    fn infra_images_are_hidden_from_cri() {
        assert!(is_infra_image(
            "ghcr.io/apple/containerization/vminit:0.2.0"
        ));
        assert!(is_infra_image(
            "ghcr.io/apple/container-builder-shim/builder:0.2.1"
        ));
        assert!(!is_infra_image("docker.io/library/alpine:latest"));
        assert!(!is_infra_image("ghcr.io/someone/app:latest"));
    }
}

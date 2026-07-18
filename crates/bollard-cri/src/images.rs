// SPDX-License-Identifier: Apache-2.0

//! `ImageBackend` over the Docker Engine API (plan 03 B1).
//!
//! Mapping decisions ported from cri-dockerd `core/image.go`: image id =
//! Docker image ID digest, uid/username parsed from the image config `User`,
//! pulls stream (progress discarded), removal is idempotent.

use async_trait::async_trait;
use bollard::auth::DockerCredentials;
use bollard::image::{CreateImageOptions, ListImagesOptions, RemoveImageOptions};
use cri_proto::v1::*;
use cri_server::error::{Error, Result};
use cri_server::ImageBackend;
use futures_util::TryStreamExt;

use crate::backend::{docker_err, BollardBackend};

/// Split an image ref into the `fromImage`/`tag` pair the Docker pull API
/// wants: `busybox` → (`busybox`, `latest`); `busybox:1.36` →
/// (`busybox`, `1.36`); `repo@sha256:…` → (`repo`, `sha256:…`).
fn split_pull_ref(image: &str) -> (String, String) {
    if let Some((repo, digest)) = image.split_once('@') {
        return (repo.to_string(), digest.to_string());
    }
    // A ':' after the last '/' is a tag separator; earlier ones belong to a
    // registry host:port.
    let tag_split = image.rfind(':').filter(|&i| !image[i..].contains('/'));
    match tag_split {
        Some(i) => (image[..i].to_string(), image[i + 1..].to_string()),
        None => (image.to_string(), "latest".to_string()),
    }
}

/// Parse a Docker image config `User` field (`1002`, `1002:1003`,
/// `www-data`, `www-data:www-data`) into CRI `(uid, username)`.
fn parse_image_user(user: &str) -> (Option<Int64Value>, String) {
    let user = user.split(':').next().unwrap_or("").trim();
    if user.is_empty() {
        return (None, String::new());
    }
    match user.parse::<i64>() {
        Ok(value) => (Some(Int64Value { value }), String::new()),
        Err(_) => (None, user.to_string()),
    }
}

fn real_repo_tags(tags: Vec<String>) -> Vec<String> {
    tags.into_iter().filter(|t| !t.contains("<none>")).collect()
}

impl BollardBackend {
    async fn inspect_to_cri_image(&self, reference: &str) -> Result<Option<Image>> {
        let inspect = match self.docker.inspect_image(reference).await {
            Ok(inspect) => inspect,
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => return Ok(None),
            Err(e) => return Err(docker_err("inspect image", e)),
        };
        let (uid, username) = inspect
            .config
            .as_ref()
            .and_then(|c| c.user.as_deref())
            .map(parse_image_user)
            .unwrap_or((None, String::new()));
        Ok(Some(Image {
            id: inspect.id.unwrap_or_default(),
            repo_tags: real_repo_tags(inspect.repo_tags.unwrap_or_default()),
            repo_digests: inspect.repo_digests.unwrap_or_default(),
            size: inspect.size.unwrap_or_default().max(0) as u64,
            uid,
            username,
            ..Default::default()
        }))
    }
}

#[async_trait]
impl ImageBackend for BollardBackend {
    async fn list_images(&self, filter: Option<ImageFilter>) -> Result<Vec<Image>> {
        // A name filter is served through inspect so the result carries
        // uid/username like ImageStatus does.
        if let Some(spec) = filter.and_then(|f| f.image).filter(|s| !s.image.is_empty()) {
            return Ok(self
                .inspect_to_cri_image(&spec.image)
                .await?
                .into_iter()
                .collect());
        }
        let summaries = self
            .docker
            .list_images(Some(ListImagesOptions::<String> {
                all: false,
                ..Default::default()
            }))
            .await
            .map_err(|e| docker_err("list images", e))?;
        Ok(summaries
            .into_iter()
            .map(|s| Image {
                id: s.id,
                repo_tags: real_repo_tags(s.repo_tags),
                repo_digests: s.repo_digests,
                size: s.size.max(0) as u64,
                ..Default::default()
            })
            .collect())
    }

    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>> {
        self.inspect_to_cri_image(&image.image).await
    }

    async fn pull_image(
        &self,
        image: &ImageSpec,
        auth: Option<AuthConfig>,
        _sandbox_config: Option<PodSandboxConfig>,
    ) -> Result<String> {
        if image.image.is_empty() {
            return Err(Error::InvalidArgument("image name is required".into()));
        }
        let (from_image, tag) = split_pull_ref(&image.image);
        let credentials = auth.map(|a| DockerCredentials {
            username: (!a.username.is_empty()).then_some(a.username),
            password: (!a.password.is_empty()).then_some(a.password),
            auth: (!a.auth.is_empty()).then_some(a.auth),
            serveraddress: (!a.server_address.is_empty()).then_some(a.server_address),
            identitytoken: (!a.identity_token.is_empty()).then_some(a.identity_token),
            registrytoken: (!a.registry_token.is_empty()).then_some(a.registry_token),
            ..Default::default()
        });

        self.docker
            .create_image(
                Some(CreateImageOptions {
                    from_image: from_image.clone(),
                    tag: tag.clone(),
                    ..Default::default()
                }),
                None,
                credentials,
            )
            .try_for_each(|progress| async {
                if let Some(err) = progress.error {
                    tracing::warn!(image = %from_image, "pull progress error: {err}");
                }
                Ok(())
            })
            .await
            .map_err(|e| docker_err("pull image", e))?;

        // CRI wants the canonical ref of what was pulled; use the image ID.
        let pulled = self
            .inspect_to_cri_image(&image.image)
            .await?
            .ok_or_else(|| Error::Internal(format!("image {} missing after pull", image.image)))?;
        Ok(pulled.id)
    }

    async fn remove_image(&self, image: &ImageSpec) -> Result<()> {
        // cri-dockerd behavior: untag every repo tag (no force — an image in
        // use by a container must stay an error for the kubelet's image GC),
        // falling back to the given ref when the image carries no tags.
        let inspect = match self.docker.inspect_image(&image.image).await {
            Ok(inspect) => inspect,
            // Idempotent: absent image is a success.
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => return Ok(()),
            Err(e) => return Err(docker_err("inspect image for removal", e)),
        };

        let mut refs = real_repo_tags(inspect.repo_tags.unwrap_or_default());
        if refs.is_empty() {
            refs.push(image.image.clone());
        }
        for reference in refs {
            match self
                .docker
                .remove_image(&reference, None::<RemoveImageOptions>, None)
                .await
            {
                Ok(_) => {}
                Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                }) => {}
                Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 409,
                    message,
                }) => {
                    return Err(Error::FailedPrecondition(format!(
                        "image {reference} is in use: {message}"
                    )))
                }
                Err(e) => return Err(docker_err("remove image", e)),
            }
        }
        Ok(())
    }

    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>> {
        // Best effort (plan 03): total image bytes from the daemon's data
        // usage, mountpoint from the daemon's root dir.
        let info = self
            .docker
            .info()
            .await
            .map_err(|e| docker_err("info", e))?;
        let mountpoint = info
            .docker_root_dir
            .unwrap_or_else(|| "/var/lib/docker".to_string());
        let df = self.docker.df().await.map_err(|e| docker_err("df", e))?;
        let (used, inodes) = df
            .images
            .map(|images| {
                let used: i64 = images.iter().map(|i| i.size.max(0)).sum();
                (used as u64, images.len() as u64)
            })
            .unwrap_or((0, 0));
        Ok(vec![FilesystemUsage {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or_default(),
            fs_id: Some(FilesystemIdentifier { mountpoint }),
            used_bytes: Some(UInt64Value { value: used }),
            inodes_used: Some(UInt64Value { value: inodes }),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_pull_ref_handles_tags_digests_and_ports() {
        assert_eq!(
            split_pull_ref("busybox"),
            ("busybox".into(), "latest".into())
        );
        assert_eq!(
            split_pull_ref("busybox:1.36"),
            ("busybox".into(), "1.36".into())
        );
        assert_eq!(
            split_pull_ref("localhost:5000/app"),
            ("localhost:5000/app".into(), "latest".into())
        );
        assert_eq!(
            split_pull_ref("localhost:5000/app:v1"),
            ("localhost:5000/app".into(), "v1".into())
        );
        assert_eq!(
            split_pull_ref("repo@sha256:abcd"),
            ("repo".into(), "sha256:abcd".into())
        );
    }

    #[test]
    fn parse_image_user_variants() {
        assert_eq!(parse_image_user("1002").0.unwrap().value, 1002);
        assert_eq!(parse_image_user("1002:1003").0.unwrap().value, 1002);
        assert_eq!(parse_image_user("www-data").1, "www-data");
        assert_eq!(parse_image_user("www-data:www-data").1, "www-data");
        assert_eq!(parse_image_user(""), (None, String::new()));
    }
}

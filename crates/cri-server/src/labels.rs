// SPDX-License-Identifier: Apache-2.0

//! Kubernetes label/annotation conventions for CRI shims.
//!
//! Shims that store CRI metadata in their runtime's flat label map (Docker
//! labels, apple/container labels) use these helpers to flatten CRI labels +
//! annotations into one map and split them back apart, plus the well-known
//! `io.kubernetes.*` keys the kubelet ecosystem expects.

use std::collections::HashMap;

/// Pod name, set on sandboxes and containers.
pub const POD_NAME_LABEL: &str = "io.kubernetes.pod.name";
/// Pod namespace, set on sandboxes and containers.
pub const POD_NAMESPACE_LABEL: &str = "io.kubernetes.pod.namespace";
/// Pod UID, set on sandboxes and containers.
pub const POD_UID_LABEL: &str = "io.kubernetes.pod.uid";
/// Container name within the pod, set on containers.
pub const CONTAINER_NAME_LABEL: &str = "io.kubernetes.container.name";

/// Internal label marking a runtime object as CRI-managed and typed.
/// Values: [`CONTAINER_TYPE_SANDBOX`], [`CONTAINER_TYPE_CONTAINER`].
pub const CONTAINER_TYPE_LABEL: &str = "io.kubernetes.cri.container-type";
pub const CONTAINER_TYPE_SANDBOX: &str = "podsandbox";
pub const CONTAINER_TYPE_CONTAINER: &str = "container";

/// Internal label holding the owning sandbox id (on app containers).
pub const SANDBOX_ID_LABEL: &str = "io.kubernetes.cri.sandbox-id";
/// Internal label holding the kubelet-desired container log path.
pub const CONTAINER_LOG_PATH_LABEL: &str = "io.kubernetes.cri.container-logpath";

/// Prefix distinguishing flattened CRI annotations from CRI labels.
pub const ANNOTATION_PREFIX: &str = "annotation.";

/// Flatten CRI `labels` + `annotations` into a single runtime label map.
/// Annotations get [`ANNOTATION_PREFIX`] so [`split_labels`] can undo this.
pub fn flatten_labels(
    labels: &HashMap<String, String>,
    annotations: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = labels.clone();
    for (k, v) in annotations {
        merged.insert(format!("{ANNOTATION_PREFIX}{k}"), v.clone());
    }
    merged
}

/// Undo [`flatten_labels`]: split a runtime label map back into CRI
/// `(labels, annotations)`, dropping internal `io.kubernetes.cri.*` keys.
pub fn split_labels(
    merged: &HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut labels = HashMap::new();
    let mut annotations = HashMap::new();
    for (k, v) in merged {
        if let Some(key) = k.strip_prefix(ANNOTATION_PREFIX) {
            annotations.insert(key.to_string(), v.clone());
        } else if !is_internal_label(k) {
            labels.insert(k.clone(), v.clone());
        }
    }
    (labels, annotations)
}

/// True for labels the shim added for its own bookkeeping, which must not
/// leak back into CRI `labels`.
pub fn is_internal_label(key: &str) -> bool {
    key.starts_with("io.kubernetes.cri.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_and_split_round_trip() {
        let labels = HashMap::from([("app".to_string(), "web".to_string())]);
        let annotations = HashMap::from([("k8s.io/some-note".to_string(), "value".to_string())]);
        let mut merged = flatten_labels(&labels, &annotations);
        merged.insert(
            CONTAINER_TYPE_LABEL.to_string(),
            CONTAINER_TYPE_SANDBOX.to_string(),
        );

        let (got_labels, got_annotations) = split_labels(&merged);
        assert_eq!(got_labels, labels);
        assert_eq!(got_annotations, annotations);
    }
}

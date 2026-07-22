// Repro: e2e subpath pod JSON decode
#[test]
fn decode_subpath_e2e_pod() {
    let json = r#"{
  "metadata": {"name": "pod-subpath-test-configmap", "namespace": "subpath-5471"},
  "spec": {
    "securityContext": {"seLinuxOptions": {"level": "s0:c0,c1"}},
    "initContainers": [
      {
        "name": "init-volume-configmap",
        "image": "registry.k8s.io/e2e-test-images/agnhost:2.55",
        "securityContext": {"privileged": false},
        "volumeMounts": [
          {"name": "test-volume", "mountPath": "/test-volume"},
          {"name": "liveness-probe-volume", "mountPath": "/probe-volume"}
        ]
      },
      {
        "name": "test-init-subpath-configmap",
        "image": "registry.k8s.io/e2e-test-images/agnhost:2.55",
        "args": ["mounttest"],
        "securityContext": {"privileged": false},
        "volumeMounts": [
          {"name": "test-volume", "mountPath": "/test-volume", "subPath": "configmap-key"},
          {"name": "liveness-probe-volume", "mountPath": "/probe-volume"}
        ]
      }
    ],
    "containers": [
      {
        "name": "test-container-subpath-configmap",
        "image": "registry.k8s.io/e2e-test-images/agnhost:2.55",
        "args": ["mounttest"],
        "securityContext": {"privileged": false},
        "volumeMounts": [
          {"name": "test-volume", "mountPath": "/test-volume", "subPath": "configmap-key"},
          {"name": "liveness-probe-volume", "mountPath": "/probe-volume"}
        ]
      },
      {
        "name": "test-container-volume-configmap",
        "image": "registry.k8s.io/e2e-test-images/agnhost:2.55",
        "args": ["mounttest"],
        "securityContext": {"privileged": false},
        "volumeMounts": [
          {"name": "test-volume", "mountPath": "/test-volume"},
          {"name": "liveness-probe-volume", "mountPath": "/probe-volume"}
        ]
      }
    ],
    "restartPolicy": "Never",
    "terminationGracePeriodSeconds": 1,
    "volumes": [
      {"name": "test-volume", "configMap": {"name": "my-configmap"}},
      {"name": "liveness-probe-volume", "emptyDir": {}}
    ]
  }
}"#;
    let pod: rusternetes_common::resources::Pod = serde_json::from_str(json).unwrap();
    assert_eq!(pod.metadata.name, "pod-subpath-test-configmap");
}

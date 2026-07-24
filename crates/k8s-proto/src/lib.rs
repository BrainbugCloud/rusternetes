//! Generic Kubernetes protobuf-to-JSON decoder.
//!
//! Kubernetes wraps all protobuf-encoded resources in an `Unknown` envelope:
//!   k8s\0 + proto(Unknown { typeMeta, raw, contentEncoding, contentType })
//!
//! The `raw` field contains the native protobuf encoding of the resource
//! (e.g., apps/v1.Deployment). This module decodes native protobuf into
//! JSON using field number → name mappings extracted from the K8s .proto
//! schema files.
//!
//! The Go API server uses generated .pb.go Unmarshal methods. We achieve
//! the same result by maintaining a registry of proto schemas and using
//! a generic recursive decoder.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use tracing::{debug, warn};

// Registry entries generated at build time from the vendored k8s.io/api protos.
// Defines `fn generated_schemas(&mut HashMap<String, MessageSchema>)`.
include!(concat!(env!("OUT_DIR"), "/generated_registry.rs"));

/// Wire types in protobuf encoding
const WIRE_VARINT: u8 = 0;
const WIRE_64BIT: u8 = 1;
const WIRE_LENGTH_DELIMITED: u8 = 2;
const WIRE_32BIT: u8 = 5;

/// Describes how a protobuf field should be decoded to JSON
#[derive(Debug, Clone)]
pub enum FieldType {
    /// Scalar string field
    String,
    /// Scalar integer field (int32, int64, uint32, uint64)
    Int,
    /// Scalar boolean field
    Bool,
    /// Nested message — value is the message type name for schema lookup
    Message(String),
    /// map<string, string> — encoded as repeated MapEntry messages
    StringMap,
    /// Repeated field — value is the element type
    Repeated(Box<FieldType>),
    /// Bytes field — base64 encode
    Bytes,
    /// IntOrString — K8s special type, try string first then int
    IntOrString,
    /// map<string, Message> — encoded as repeated MapEntry with key=string, value=message
    MessageMap(String),
    /// K8s JSON type — a message with a single `raw` bytes field containing JSON
    JsonRaw,
    /// Inlined nested message — the named message is decoded and its keys are
    /// merged directly into the PARENT object (used for K8s `VolumeSource`,
    /// which is a nested proto message but flattened in rusternetes' JSON structs).
    Inlined(String),
    /// K8s `resource.Quantity`. On the wire it is a submessage
    /// `message Quantity { optional string string = 1; }`, but K8s JSON marshals
    /// it as the bare string. Decode by unwrapping field 1, NOT by reading the
    /// submessage bytes as a raw string.
    Quantity,
    /// map<string, Quantity> — repeated MapEntry where each value is a Quantity
    /// submessage (see `Quantity`).
    QuantityMap,
}

/// Schema for a single protobuf message type
#[derive(Debug, Clone)]
pub struct MessageSchema {
    /// Map of field number → (json_field_name, field_type)
    pub fields: HashMap<u32, (String, FieldType)>,
}

/// Registry of all known K8s protobuf message schemas
pub struct ProtoRegistry {
    /// Map of message type name → schema
    schemas: HashMap<String, MessageSchema>,
}

impl Default for ProtoRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtoRegistry {
    /// Build the registry with all known K8s proto schemas.
    /// Field numbers are from the generated.proto files in k8s.io/api.
    pub fn new() -> Self {
        let mut schemas = HashMap::new();

        // ========== apimachinery types ==========

        schemas.insert("ObjectMeta".into(), Self::object_meta_schema());
        schemas.insert("LabelSelector".into(), Self::label_selector_schema());
        schemas.insert(
            "LabelSelectorRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (
                        3,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert("OwnerReference".into(), Self::owner_reference_schema());
        schemas.insert(
            "Time".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("seconds".into(), FieldType::Int)),
                    (2, ("nanos".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ManagedFieldsEntry".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("manager".into(), FieldType::String)),
                    (2, ("operation".into(), FieldType::String)),
                    (3, ("apiVersion".into(), FieldType::String)),
                    (4, ("time".into(), FieldType::Message("Time".into()))),
                    (6, ("fieldsType".into(), FieldType::String)),
                    (7, ("fieldsV1".into(), FieldType::Bytes)),
                    (8, ("subresource".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "DeleteOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("gracePeriodSeconds".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "preconditions".into(),
                            FieldType::Message("Preconditions".into()),
                        ),
                    ),
                    (3, ("orphanDependents".into(), FieldType::Bool)),
                    (4, ("propagationPolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        6,
                        (
                            "ignoreStoreReadErrorWithClusterBreakingPotential".into(),
                            FieldType::Bool,
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "Preconditions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("uid".into(), FieldType::String)),
                    (2, ("resourceVersion".into(), FieldType::String)),
                ]),
            },
        );

        // ========== apps/v1 types ==========

        schemas.insert("Deployment".into(), Self::deployment_schema());
        schemas.insert("DeploymentSpec".into(), Self::deployment_spec_schema());
        schemas.insert("DeploymentStatus".into(), Self::deployment_status_schema());
        schemas.insert(
            "DeploymentCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                    (
                        6,
                        ("lastUpdateTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        7,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DeploymentStrategy".into(),
            Self::deployment_strategy_schema(),
        );
        schemas.insert(
            "RollingUpdateDeployment".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxUnavailable".into(), FieldType::IntOrString)),
                    (2, ("maxSurge".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("ReplicaSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ReplicaSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("fullyLabeledReplicas".into(), FieldType::Int)),
                    (3, ("observedGeneration".into(), FieldType::Int)),
                    (4, ("readyReplicas".into(), FieldType::Int)),
                    (5, ("availableReplicas".into(), FieldType::Int)),
                    (
                        6,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ReplicaSetCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("StatefulSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("StatefulSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "volumeClaimTemplates".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PersistentVolumeClaim".into(),
                            ))),
                        ),
                    ),
                    (5, ("serviceName".into(), FieldType::String)),
                    (6, ("podManagementPolicy".into(), FieldType::String)),
                    (
                        7,
                        (
                            "updateStrategy".into(),
                            FieldType::Message("StatefulSetUpdateStrategy".into()),
                        ),
                    ),
                    (8, ("revisionHistoryLimit".into(), FieldType::Int)),
                    (9, ("minReadySeconds".into(), FieldType::Int)),
                    (
                        10,
                        (
                            "persistentVolumeClaimRetentionPolicy".into(),
                            FieldType::Message(
                                "StatefulSetPersistentVolumeClaimRetentionPolicy".into(),
                            ),
                        ),
                    ),
                    (
                        11,
                        (
                            "ordinals".into(),
                            FieldType::Message("StatefulSetOrdinals".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetUpdateStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "rollingUpdate".into(),
                            FieldType::Message("RollingUpdateStatefulSetStrategy".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "RollingUpdateStatefulSetStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("partition".into(), FieldType::Int)),
                    (2, ("maxUnavailable".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetPersistentVolumeClaimRetentionPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("whenDeleted".into(), FieldType::String)),
                    (2, ("whenScaled".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetOrdinals".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("start".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "StatefulSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (2, ("replicas".into(), FieldType::Int)),
                    (3, ("readyReplicas".into(), FieldType::Int)),
                    (4, ("currentReplicas".into(), FieldType::Int)),
                    (5, ("updatedReplicas".into(), FieldType::Int)),
                    (6, ("currentRevision".into(), FieldType::String)),
                    (7, ("updateRevision".into(), FieldType::String)),
                    (8, ("collisionCount".into(), FieldType::Int)),
                    (
                        9,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "StatefulSetCondition".into(),
                            ))),
                        ),
                    ),
                    (10, ("availableReplicas".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("DaemonSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("DaemonSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "updateStrategy".into(),
                            FieldType::Message("DaemonSetUpdateStrategy".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                    // field 5 (templateGeneration) was removed in apps/v1;
                    // revisionHistoryLimit is field 6 per generated.proto.
                    (6, ("revisionHistoryLimit".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetUpdateStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "rollingUpdate".into(),
                            FieldType::Message("RollingUpdateDaemonSet".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "RollingUpdateDaemonSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxUnavailable".into(), FieldType::IntOrString)),
                    (2, ("maxSurge".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("currentNumberScheduled".into(), FieldType::Int)),
                    (2, ("numberMisscheduled".into(), FieldType::Int)),
                    (3, ("desiredNumberScheduled".into(), FieldType::Int)),
                    (4, ("numberReady".into(), FieldType::Int)),
                    (5, ("observedGeneration".into(), FieldType::Int)),
                    (6, ("updatedNumberScheduled".into(), FieldType::Int)),
                    (7, ("numberAvailable".into(), FieldType::Int)),
                    (8, ("numberUnavailable".into(), FieldType::Int)),
                    (9, ("collisionCount".into(), FieldType::Int)),
                    (
                        10,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DaemonSetCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // ========== core/v1 types ==========

        schemas.insert("PodTemplateSpec".into(), Self::pod_template_spec_schema());
        // core/v1 PodTemplate { metadata=1, template=2 }. Without this,
        // protobuf-encoded PodTemplate creates fell through to the best-effort
        // decoder; the [sig-api-machinery] chunking test bulk-creates
        // PodTemplates and its failure panicked the whole e2e suite (0 results).
        schemas.insert(
            "PodTemplate".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert("PodSpec".into(), Self::pod_spec_schema());
        schemas.insert("Container".into(), Self::container_schema());
        schemas.insert("ContainerPort".into(), Self::container_port_schema());
        schemas.insert("SecurityContext".into(), Self::security_context_schema());
        schemas.insert(
            "ResourceRequirements".into(),
            Self::resource_requirements_schema(),
        );
        schemas.insert("Volume".into(), Self::volume_schema());
        schemas.insert("VolumeSource".into(), Self::volume_source_schema());
        schemas.insert("VolumeMount".into(), Self::volume_mount_schema());
        // Volume source submessages (K8s core/v1). Without these, the generic
        // decoder emits `{}` for each source and required fields (e.g.
        // HostPathVolumeSource.path, ConfigMapVolumeSource keys) go missing,
        // causing pod-create JSON decode failures.
        schemas.insert(
            "HostPathVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("type".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "EmptyDirVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("medium".into(), FieldType::String)),
                    (2, ("sizeLimit".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "KeyToPath".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("mode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    // localObjectReference{name=1} is embedded; rusternetes JSON
                    // flattens `name` to the top level, so inline it.
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::Inlined("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (3, ("defaultMode".into(), FieldType::Int)),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("secretName".into(), FieldType::String)),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (3, ("defaultMode".into(), FieldType::Int)),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "LocalObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ProjectedVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "sources".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "VolumeProjection".into(),
                            ))),
                        ),
                    ),
                    (2, ("defaultMode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "VolumeProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "secret".into(),
                            FieldType::Message("SecretProjection".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "downwardAPI".into(),
                            FieldType::Message("DownwardAPIProjection".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "configMap".into(),
                            FieldType::Message("ConfigMapProjection".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "serviceAccountToken".into(),
                            FieldType::Message("ServiceAccountTokenProjection".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::Inlined("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::Inlined("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "ServiceAccountTokenProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("audience".into(), FieldType::String)),
                    (2, ("expirationSeconds".into(), FieldType::Int)),
                    (3, ("path".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "DownwardAPIProjection".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "items".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "DownwardAPIVolumeFile".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "DownwardAPIVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DownwardAPIVolumeFile".into(),
                            ))),
                        ),
                    ),
                    (2, ("defaultMode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DownwardAPIVolumeFile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (
                        2,
                        (
                            "fieldRef".into(),
                            FieldType::Message("ObjectFieldSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "resourceFieldRef".into(),
                            FieldType::Message("ResourceFieldSelector".into()),
                        ),
                    ),
                    (4, ("mode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert("EnvVar".into(), Self::env_var_schema());
        schemas.insert("EnvVarSource".into(), Self::env_var_source_schema());
        schemas.insert(
            "ObjectFieldSelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiVersion".into(), FieldType::String)),
                    (2, ("fieldPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ResourceFieldSelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("containerName".into(), FieldType::String)),
                    (2, ("resource".into(), FieldType::String)),
                    (3, ("divisor".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("key".into(), FieldType::String)),
                    (3, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("key".into(), FieldType::String)),
                    (3, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert("Probe".into(), Self::probe_schema());
        schemas.insert("ProbeHandler".into(), Self::probe_handler_schema());
        schemas.insert(
            "ExecAction".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "command".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );
        schemas.insert(
            "HTTPGetAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("port".into(), FieldType::IntOrString)),
                    (3, ("host".into(), FieldType::String)),
                    (4, ("scheme".into(), FieldType::String)),
                    (
                        5,
                        (
                            "httpHeaders".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("HTTPHeader".into()))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "HTTPHeader".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "TCPSocketAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::IntOrString)),
                    (2, ("host".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "GRPCAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::Int)),
                    (2, ("service".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "Lifecycle".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "postStart".into(),
                            FieldType::Message("LifecycleHandler".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "preStop".into(),
                            FieldType::Message("LifecycleHandler".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LifecycleHandler".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("exec".into(), FieldType::Message("ExecAction".into()))),
                    (
                        2,
                        ("httpGet".into(), FieldType::Message("HTTPGetAction".into())),
                    ),
                    (
                        3,
                        (
                            "tcpSocket".into(),
                            FieldType::Message("TCPSocketAction".into()),
                        ),
                    ),
                    (
                        4,
                        ("sleep".into(), FieldType::Message("SleepAction".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SleepAction".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("seconds".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "Capabilities".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "add".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "drop".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SELinuxOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("user".into(), FieldType::String)),
                    (2, ("role".into(), FieldType::String)),
                    (3, ("type".into(), FieldType::String)),
                    (4, ("level".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SeccompProfile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("localhostProfile".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "AppArmorProfile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("localhostProfile".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodSecurityContext".into(),
            Self::pod_security_context_schema(),
        );
        schemas.insert(
            "Toleration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (3, ("value".into(), FieldType::String)),
                    (4, ("effect".into(), FieldType::String)),
                    (5, ("tolerationSeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "PodDNSConfig".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "nameservers".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "searches".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "options".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodDNSConfigOption".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodDNSConfigOption".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "LocalObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "Affinity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "nodeAffinity".into(),
                            FieldType::Message("NodeAffinity".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "podAffinity".into(),
                            FieldType::Message("PodAffinity".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "podAntiAffinity".into(),
                            FieldType::Message("PodAntiAffinity".into()),
                        ),
                    ),
                ]),
            },
        );
        // Affinity sub-types are complex — decode as opaque messages
        schemas.insert(
            "NodeAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodAntiAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "TopologySpreadConstraint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxSkew".into(), FieldType::Int)),
                    (2, ("topologyKey".into(), FieldType::String)),
                    (3, ("whenUnsatisfiable".into(), FieldType::String)),
                    (
                        4,
                        (
                            "labelSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("minDomains".into(), FieldType::Int)),
                    (6, ("nodeAffinityPolicy".into(), FieldType::String)),
                    (7, ("nodeTaintsPolicy".into(), FieldType::String)),
                    (
                        8,
                        (
                            "matchLabelKeys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        // Service, ConfigMap, Secret, etc. — common pattern
        schemas.insert(
            "Service".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("ServiceSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("ServiceStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ServiceSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("ServicePort".into()))),
                        ),
                    ),
                    (2, ("selector".into(), FieldType::StringMap)),
                    (3, ("clusterIP".into(), FieldType::String)),
                    (4, ("type".into(), FieldType::String)),
                    (
                        5,
                        (
                            "externalIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (7, ("sessionAffinity".into(), FieldType::String)),
                    (8, ("loadBalancerIP".into(), FieldType::String)),
                    (
                        9,
                        (
                            "loadBalancerSourceRanges".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (10, ("externalName".into(), FieldType::String)),
                    (11, ("externalTrafficPolicy".into(), FieldType::String)),
                    (12, ("healthCheckNodePort".into(), FieldType::Int)),
                    (13, ("publishNotReadyAddresses".into(), FieldType::Bool)),
                    (
                        14,
                        (
                            "sessionAffinityConfig".into(),
                            FieldType::Message("SessionAffinityConfig".into()),
                        ),
                    ),
                    (
                        17,
                        (
                            "ipFamilies".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (18, ("ipFamilyPolicy".into(), FieldType::String)),
                    (
                        19,
                        (
                            "clusterIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (20, ("internalTrafficPolicy".into(), FieldType::String)),
                    (
                        21,
                        ("allocateLoadBalancerNodePorts".into(), FieldType::Bool),
                    ),
                    (22, ("loadBalancerClass".into(), FieldType::String)),
                    (23, ("trafficDistribution".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ServicePort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("protocol".into(), FieldType::String)),
                    (3, ("port".into(), FieldType::Int)),
                    (4, ("targetPort".into(), FieldType::IntOrString)),
                    (5, ("nodePort".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ServiceStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "SessionAffinityConfig".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Batch types
        schemas.insert(
            "Job".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("JobSpec".into()))),
                    (3, ("status".into(), FieldType::Message("JobStatus".into()))),
                ]),
            },
        );
        schemas.insert(
            "JobSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("parallelism".into(), FieldType::Int)),
                    (2, ("completions".into(), FieldType::Int)),
                    (3, ("activeDeadlineSeconds".into(), FieldType::Int)),
                    (
                        4,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("manualSelector".into(), FieldType::Bool)),
                    (
                        6,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (7, ("backoffLimit".into(), FieldType::Int)),
                    (8, ("ttlSecondsAfterFinished".into(), FieldType::Int)),
                    (9, ("completionMode".into(), FieldType::String)),
                    (10, ("suspend".into(), FieldType::Bool)),
                    // Field numbers 11-15 per k8s.io/api/batch/v1 generated.proto.
                    // (Previously mis-assigned, silently corrupting these fields.)
                    (
                        11,
                        (
                            "podFailurePolicy".into(),
                            FieldType::Message("PodFailurePolicy".into()),
                        ),
                    ),
                    (12, ("backoffLimitPerIndex".into(), FieldType::Int)),
                    (13, ("maxFailedIndexes".into(), FieldType::Int)),
                    (14, ("podReplacementPolicy".into(), FieldType::String)),
                    (15, ("managedBy".into(), FieldType::String)),
                    (
                        16,
                        (
                            "successPolicy".into(),
                            FieldType::Message("SuccessPolicy".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JobStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodFailurePolicy".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "SuccessPolicy".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        // batch/v1 CronJob (field numbers per k8s.io/api/batch/v1 generated.proto).
        // Without these, protobuf-encoded CronJob creates fell through to the
        // best-effort decoder and lost spec.schedule → "missing field `schedule`".
        schemas.insert(
            "CronJob".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("CronJobSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("CronJobStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CronJobSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("schedule".into(), FieldType::String)),
                    (2, ("startingDeadlineSeconds".into(), FieldType::Int)),
                    (3, ("concurrencyPolicy".into(), FieldType::String)),
                    (4, ("suspend".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "jobTemplate".into(),
                            FieldType::Message("JobTemplateSpec".into()),
                        ),
                    ),
                    (6, ("successfulJobsHistoryLimit".into(), FieldType::Int)),
                    (7, ("failedJobsHistoryLimit".into(), FieldType::Int)),
                    (8, ("timeZone".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CronJobStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "active".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ObjectReference".into(),
                            ))),
                        ),
                    ),
                    (
                        4,
                        ("lastScheduleTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        5,
                        (
                            "lastSuccessfulTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JobTemplateSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("JobSpec".into()))),
                ]),
            },
        );

        // Pod (standalone)
        schemas.insert(
            "Pod".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("PodSpec".into()))),
                    (3, ("status".into(), FieldType::Message("PodStatus".into()))),
                ]),
            },
        );
        schemas.insert(
            "PodStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ConfigMap & Secret
        schemas.insert(
            "ConfigMap".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::StringMap)),
                    (3, ("binaryData".into(), FieldType::StringMap)),
                    (4, ("immutable".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "Secret".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::StringMap)),
                    (3, ("type".into(), FieldType::String)),
                    (4, ("stringData".into(), FieldType::StringMap)),
                    (5, ("immutable".into(), FieldType::Bool)),
                ]),
            },
        );

        // Namespace
        schemas.insert(
            "Namespace".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("NamespaceSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("NamespaceStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NamespaceSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "finalizers".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );
        schemas.insert(
            "NamespaceStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("phase".into(), FieldType::String)),
                    (
                        2,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NamespaceCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NamespaceCondition".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ServiceAccount
        schemas.insert(
            "ServiceAccount".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "secrets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ObjectReference".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "imagePullSecrets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "LocalObjectReference".into(),
                            ))),
                        ),
                    ),
                    (4, ("automountServiceAccountToken".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "ObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("namespace".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("uid".into(), FieldType::String)),
                    (5, ("apiVersion".into(), FieldType::String)),
                    (6, ("resourceVersion".into(), FieldType::String)),
                    (7, ("fieldPath".into(), FieldType::String)),
                ]),
            },
        );

        // PersistentVolumeClaim (used by StatefulSet volumeClaimTemplates)
        schemas.insert(
            "PersistentVolumeClaim".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("PersistentVolumeClaimSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("PersistentVolumeClaimStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeClaimSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "accessModes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "resources".into(),
                            FieldType::Message("VolumeResourceRequirements".into()),
                        ),
                    ),
                    (3, ("volumeName".into(), FieldType::String)),
                    (
                        4,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("storageClassName".into(), FieldType::String)),
                    (6, ("volumeMode".into(), FieldType::String)),
                    (
                        7,
                        (
                            "dataSource".into(),
                            FieldType::Message("TypedLocalObjectReference".into()),
                        ),
                    ),
                    (
                        8,
                        (
                            "dataSourceRef".into(),
                            FieldType::Message("TypedObjectReference".into()),
                        ),
                    ),
                    (9, ("volumeAttributesClassName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeClaimStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "VolumeResourceRequirements".into(),
            MessageSchema {
                fields: HashMap::from([
                    // ResourceList values are Quantity submessages, not raw strings.
                    (1, ("limits".into(), FieldType::QuantityMap)),
                    (2, ("requests".into(), FieldType::QuantityMap)),
                ]),
            },
        );
        schemas.insert(
            "TypedLocalObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "TypedObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("namespace".into(), FieldType::String)),
                ]),
            },
        );

        // ReplicationController (core/v1)
        schemas.insert(
            "ReplicationController".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("ReplicationControllerSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ReplicationControllerStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicationControllerSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("selector".into(), FieldType::StringMap)),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ReplicationControllerStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Endpoints
        schemas.insert(
            "Endpoints".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "subsets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "EndpointSubset".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "EndpointSubset".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Node
        schemas.insert(
            "Node".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("NodeSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("NodeStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NodeSpec".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "NodeStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ========== apiextensions types (CRDs) ==========

        schemas.insert(
            "CustomResourceDefinition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("CustomResourceDefinitionSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("CustomResourceDefinitionStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (
                        3,
                        (
                            "names".into(),
                            FieldType::Message("CustomResourceDefinitionNames".into()),
                        ),
                    ),
                    (4, ("scope".into(), FieldType::String)),
                    (
                        7,
                        (
                            "versions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceDefinitionVersion".into(),
                            ))),
                        ),
                    ),
                    (
                        9,
                        (
                            "conversion".into(),
                            FieldType::Message("CustomResourceConversion".into()),
                        ),
                    ),
                    (10, ("preserveUnknownFields".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionNames".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("plural".into(), FieldType::String)),
                    (2, ("singular".into(), FieldType::String)),
                    (
                        3,
                        (
                            "shortNames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (4, ("kind".into(), FieldType::String)),
                    (5, ("listKind".into(), FieldType::String)),
                    (
                        6,
                        (
                            "categories".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionVersion".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("served".into(), FieldType::Bool)),
                    (3, ("storage".into(), FieldType::Bool)),
                    (
                        4,
                        (
                            "schema".into(),
                            FieldType::Message("CustomResourceValidation".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "subresources".into(),
                            FieldType::Message("CustomResourceSubresources".into()),
                        ),
                    ),
                    (
                        6,
                        (
                            "additionalPrinterColumns".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceColumnDefinition".into(),
                            ))),
                        ),
                    ),
                    (7, ("deprecated".into(), FieldType::Bool)),
                    (8, ("deprecationWarning".into(), FieldType::String)),
                    (
                        9,
                        (
                            "selectableFields".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "SelectableField".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceValidation".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "openAPIV3Schema".into(),
                        FieldType::Message("JSONSchemaProps".into()),
                    ),
                )]),
            },
        );
        schemas.insert(
            "JSONSchemaProps".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("id".into(), FieldType::String)),
                    (2, ("$schema".into(), FieldType::String)),
                    (3, ("$ref".into(), FieldType::String)),
                    (4, ("description".into(), FieldType::String)),
                    (5, ("type".into(), FieldType::String)),
                    (6, ("format".into(), FieldType::String)),
                    (7, ("title".into(), FieldType::String)),
                    (8, ("default".into(), FieldType::JsonRaw)),
                    (9, ("maximum".into(), FieldType::Int)),
                    (10, ("exclusiveMaximum".into(), FieldType::Bool)),
                    (11, ("minimum".into(), FieldType::Int)),
                    (12, ("exclusiveMinimum".into(), FieldType::Bool)),
                    (13, ("maxLength".into(), FieldType::Int)),
                    (14, ("minLength".into(), FieldType::Int)),
                    (15, ("pattern".into(), FieldType::String)),
                    (16, ("maxItems".into(), FieldType::Int)),
                    (17, ("minItems".into(), FieldType::Int)),
                    (18, ("uniqueItems".into(), FieldType::Bool)),
                    (19, ("multipleOf".into(), FieldType::Int)), // double, but Int works for decode
                    (
                        20,
                        (
                            "enum".into(),
                            FieldType::Repeated(Box::new(FieldType::JsonRaw)),
                        ),
                    ),
                    (21, ("maxProperties".into(), FieldType::Int)),
                    (22, ("minProperties".into(), FieldType::Int)),
                    (
                        23,
                        (
                            "required".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        24,
                        (
                            "items".into(),
                            FieldType::Message("JSONSchemaPropsOrArray".into()),
                        ),
                    ),
                    (
                        25,
                        (
                            "allOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        26,
                        (
                            "oneOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        27,
                        (
                            "anyOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        28,
                        ("not".into(), FieldType::Message("JSONSchemaProps".into())),
                    ),
                    // field 29: properties — map<string, JSONSchemaProps>
                    // Protobuf maps are encoded as repeated MapEntry messages.
                    // We handle this as a special StringMap-like type but with Message values.
                    // For now, decode properties entries manually.
                    (
                        29,
                        (
                            "properties".into(),
                            FieldType::MessageMap("JSONSchemaProps".into()),
                        ),
                    ),
                    (
                        30,
                        (
                            "additionalProperties".into(),
                            FieldType::Message("JSONSchemaPropsOrBool".into()),
                        ),
                    ),
                    (37, ("nullable".into(), FieldType::Bool)),
                    (
                        38,
                        (
                            "x-kubernetes-preserve-unknown-fields".into(),
                            FieldType::Bool,
                        ),
                    ),
                    (
                        39,
                        ("x-kubernetes-embedded-resource".into(), FieldType::Bool),
                    ),
                    (40, ("x-kubernetes-int-or-string".into(), FieldType::Bool)),
                    (
                        41,
                        (
                            "x-kubernetes-list-map-keys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (42, ("x-kubernetes-list-type".into(), FieldType::String)),
                    (43, ("x-kubernetes-map-type".into(), FieldType::String)),
                    (
                        44,
                        (
                            "x-kubernetes-validations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ValidationRule".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        // JSONSchemaPropsOrArray: field 1 = schema (JSONSchemaProps), field 2 = jsonSchemas (repeated JSONSchemaProps)
        schemas.insert(
            "JSONSchemaPropsOrArray".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "schema".into(),
                            FieldType::Message("JSONSchemaProps".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "jsonSchemas".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JSONSchemaPropsOrBool".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("allows".into(), FieldType::Bool)),
                    (
                        2,
                        (
                            "schema".into(),
                            FieldType::Message("JSONSchemaProps".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceSubresources".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "status".into(),
                            FieldType::Message("CustomResourceSubresourceStatus".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "scale".into(),
                            FieldType::Message("CustomResourceSubresourceScale".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceSubresourceStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "CustomResourceSubresourceScale".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("specReplicasPath".into(), FieldType::String)),
                    (2, ("statusReplicasPath".into(), FieldType::String)),
                    (3, ("labelSelectorPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceConversion".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("strategy".into(), FieldType::String)),
                    (
                        2,
                        (
                            "webhook".into(),
                            FieldType::Message("WebhookConversion".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "WebhookConversion".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceDefinitionCondition".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "acceptedNames".into(),
                            FieldType::Message("CustomResourceDefinitionNames".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "storedVersions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceColumnDefinition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("type".into(), FieldType::String)),
                    (3, ("format".into(), FieldType::String)),
                    (4, ("description".into(), FieldType::String)),
                    (5, ("priority".into(), FieldType::Int)),
                    (6, ("jsonPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SelectableField".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("jsonPath".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ValidationRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("rule".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (4, ("messageExpression".into(), FieldType::String)),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("fieldPath".into(), FieldType::String)),
                    (7, ("optionalOldSelf".into(), FieldType::Bool)),
                ]),
            },
        );

        // events.k8s.io/v1 Event. This is a DIFFERENT message from core/v1
        // Event (kind "Event" in both groups) with different wire field
        // numbers, so it needs its own schema keyed distinctly; decode_k8s_resource
        // dispatches to it by apiVersion. Without this, an events.k8s.io/v1 Event
        // decoded with the core/v1 field map mis-places every field (its
        // eventTime MicroTime lands in involvedObject → deserialize fails with
        // "involvedObject.kind: invalid type: integer, expected a string").
        // Field numbers per k8s.io/api/events/v1/generated.proto.
        schemas.insert(
            "EventsV1Event".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("metadata".into(), FieldType::Message("ObjectMeta".into()))),
                    (2, ("eventTime".into(), FieldType::Message("MicroTime".into()))),
                    (3, ("series".into(), FieldType::Message("EventSeries".into()))),
                    (4, ("reportingController".into(), FieldType::String)),
                    (5, ("reportingInstance".into(), FieldType::String)),
                    (6, ("action".into(), FieldType::String)),
                    (7, ("reason".into(), FieldType::String)),
                    (
                        8,
                        ("regarding".into(), FieldType::Message("ObjectReference".into())),
                    ),
                    (
                        9,
                        ("related".into(), FieldType::Message("ObjectReference".into())),
                    ),
                    (10, ("note".into(), FieldType::String)),
                    (11, ("type".into(), FieldType::String)),
                    (
                        12,
                        (
                            "deprecatedSource".into(),
                            FieldType::Message("EventSource".into()),
                        ),
                    ),
                    (
                        13,
                        (
                            "deprecatedFirstTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (
                        14,
                        (
                            "deprecatedLastTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (15, ("deprecatedCount".into(), FieldType::Int)),
                ]),
            },
        );

        // Overlay schemas generated at build time from the vendored upstream
        // k8s.io/api protos (see build.rs). Generated entries replace the
        // hand-written ones above for the vendored groups (core/apps/batch +
        // apimachinery), so their field numbers come straight from protoc and
        // can't drift. The hand-written base still covers groups not yet
        // vendored (e.g. apiextensions CRD types).
        generated_schemas(&mut schemas);

        ProtoRegistry { schemas }
    }

    fn object_meta_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("generateName".into(), FieldType::String)),
                (3, ("namespace".into(), FieldType::String)),
                (5, ("uid".into(), FieldType::String)),
                (6, ("resourceVersion".into(), FieldType::String)),
                (7, ("generation".into(), FieldType::Int)),
                (
                    8,
                    (
                        "creationTimestamp".into(),
                        FieldType::Message("Time".into()),
                    ),
                ),
                (
                    9,
                    (
                        "deletionTimestamp".into(),
                        FieldType::Message("Time".into()),
                    ),
                ),
                (10, ("deletionGracePeriodSeconds".into(), FieldType::Int)),
                (11, ("labels".into(), FieldType::StringMap)),
                (12, ("annotations".into(), FieldType::StringMap)),
                (
                    13,
                    (
                        "ownerReferences".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("OwnerReference".into()))),
                    ),
                ),
                (
                    14,
                    (
                        "finalizers".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (
                    17,
                    (
                        "managedFields".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ManagedFieldsEntry".into(),
                        ))),
                    ),
                ),
            ]),
        }
    }

    fn owner_reference_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("apiVersion".into(), FieldType::String)),
                (2, ("kind".into(), FieldType::String)),
                (3, ("name".into(), FieldType::String)),
                (4, ("uid".into(), FieldType::String)),
                (6, ("controller".into(), FieldType::Bool)),
                (7, ("blockOwnerDeletion".into(), FieldType::Bool)),
            ]),
        }
    }

    fn label_selector_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("matchLabels".into(), FieldType::StringMap)),
                (
                    2,
                    (
                        "matchExpressions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "LabelSelectorRequirement".into(),
                        ))),
                    ),
                ),
            ]),
        }
    }

    fn deployment_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                ),
                (
                    2,
                    ("spec".into(), FieldType::Message("DeploymentSpec".into())),
                ),
                (
                    3,
                    (
                        "status".into(),
                        FieldType::Message("DeploymentStatus".into()),
                    ),
                ),
            ]),
        }
    }

    fn deployment_spec_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("replicas".into(), FieldType::Int)),
                (
                    2,
                    (
                        "selector".into(),
                        FieldType::Message("LabelSelector".into()),
                    ),
                ),
                (
                    3,
                    (
                        "template".into(),
                        FieldType::Message("PodTemplateSpec".into()),
                    ),
                ),
                (
                    4,
                    (
                        "strategy".into(),
                        FieldType::Message("DeploymentStrategy".into()),
                    ),
                ),
                (5, ("minReadySeconds".into(), FieldType::Int)),
                (6, ("revisionHistoryLimit".into(), FieldType::Int)),
                (7, ("paused".into(), FieldType::Bool)),
                (9, ("progressDeadlineSeconds".into(), FieldType::Int)),
            ]),
        }
    }

    fn deployment_status_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("observedGeneration".into(), FieldType::Int)),
                (2, ("replicas".into(), FieldType::Int)),
                (3, ("updatedReplicas".into(), FieldType::Int)),
                (4, ("unavailableReplicas".into(), FieldType::Int)),
                (5, ("availableReplicas".into(), FieldType::Int)),
                (
                    6,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "DeploymentCondition".into(),
                        ))),
                    ),
                ),
                (7, ("readyReplicas".into(), FieldType::Int)),
                (8, ("collisionCount".into(), FieldType::Int)),
            ]),
        }
    }

    fn deployment_strategy_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("type".into(), FieldType::String)),
                (
                    2,
                    (
                        "rollingUpdate".into(),
                        FieldType::Message("RollingUpdateDeployment".into()),
                    ),
                ),
            ]),
        }
    }

    fn pod_template_spec_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                ),
                (2, ("spec".into(), FieldType::Message("PodSpec".into()))),
            ]),
        }
    }

    fn pod_spec_schema() -> MessageSchema {
        // From core/v1/generated.proto — PodSpec has MANY fields
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "volumes".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Volume".into()))),
                    ),
                ),
                (
                    2,
                    (
                        "containers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (3, ("restartPolicy".into(), FieldType::String)),
                (4, ("terminationGracePeriodSeconds".into(), FieldType::Int)),
                (5, ("activeDeadlineSeconds".into(), FieldType::Int)),
                (6, ("dnsPolicy".into(), FieldType::String)),
                (7, ("nodeSelector".into(), FieldType::StringMap)),
                (8, ("serviceAccountName".into(), FieldType::String)),
                (9, ("serviceAccount".into(), FieldType::String)),
                (10, ("nodeName".into(), FieldType::String)),
                (11, ("hostNetwork".into(), FieldType::Bool)),
                (12, ("hostPID".into(), FieldType::Bool)),
                (13, ("hostIPC".into(), FieldType::Bool)),
                (
                    14,
                    (
                        "securityContext".into(),
                        FieldType::Message("PodSecurityContext".into()),
                    ),
                ),
                (
                    15,
                    (
                        "imagePullSecrets".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "LocalObjectReference".into(),
                        ))),
                    ),
                ),
                (16, ("hostname".into(), FieldType::String)),
                (17, ("subdomain".into(), FieldType::String)),
                (
                    18,
                    ("affinity".into(), FieldType::Message("Affinity".into())),
                ),
                (19, ("schedulerName".into(), FieldType::String)),
                (
                    20,
                    (
                        "initContainers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (21, ("automountServiceAccountToken".into(), FieldType::Bool)),
                (
                    22,
                    (
                        "tolerations".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Toleration".into()))),
                    ),
                ),
                // Field numbers 23-40 per k8s.io/api/core/v1 generated.proto.
                // Previously field 23 (hostAliases) was skipped, shifting every
                // field from here up by one: upstream `priority` (int32, field
                // 25, defaulted to 0 by admission) landed on our String
                // `priorityClassName` → "invalid type: integer 0, expected a
                // string" on every scheduled-pod update.
                (
                    23,
                    (
                        "hostAliases".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("HostAlias".into()))),
                    ),
                ),
                (24, ("priorityClassName".into(), FieldType::String)),
                (25, ("priority".into(), FieldType::Int)),
                (
                    26,
                    (
                        "dnsConfig".into(),
                        FieldType::Message("PodDNSConfig".into()),
                    ),
                ),
                (27, ("shareProcessNamespace".into(), FieldType::Bool)),
                (
                    28,
                    (
                        "readinessGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodReadinessGate".into(),
                        ))),
                    ),
                ),
                (29, ("runtimeClassName".into(), FieldType::String)),
                (30, ("enableServiceLinks".into(), FieldType::Bool)),
                (31, ("preemptionPolicy".into(), FieldType::String)),
                (32, ("overhead".into(), FieldType::QuantityMap)),
                (
                    33,
                    (
                        "topologySpreadConstraints".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "TopologySpreadConstraint".into(),
                        ))),
                    ),
                ),
                (
                    34,
                    (
                        "ephemeralContainers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (35, ("setHostnameAsFQDN".into(), FieldType::Bool)),
                (36, ("os".into(), FieldType::Message("PodOS".into()))),
                (37, ("hostUsers".into(), FieldType::Bool)),
                (
                    38,
                    (
                        "schedulingGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodSchedulingGate".into(),
                        ))),
                    ),
                ),
                (
                    39,
                    (
                        "resourceClaims".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodResourceClaim".into(),
                        ))),
                    ),
                ),
                (
                    40,
                    (
                        "resources".into(),
                        FieldType::Message("ResourceRequirements".into()),
                    ),
                ),
            ]),
        }
    }

    fn container_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("image".into(), FieldType::String)),
                (
                    3,
                    (
                        "command".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (
                    4,
                    (
                        "args".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (5, ("workingDir".into(), FieldType::String)),
                (
                    6,
                    (
                        "ports".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("ContainerPort".into()))),
                    ),
                ),
                (
                    7,
                    (
                        "env".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("EnvVar".into()))),
                    ),
                ),
                (
                    8,
                    (
                        "resources".into(),
                        FieldType::Message("ResourceRequirements".into()),
                    ),
                ),
                (
                    9,
                    (
                        "volumeMounts".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("VolumeMount".into()))),
                    ),
                ),
                (
                    10,
                    ("livenessProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    11,
                    ("readinessProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    12,
                    ("lifecycle".into(), FieldType::Message("Lifecycle".into())),
                ),
                (13, ("terminationMessagePath".into(), FieldType::String)),
                (14, ("imagePullPolicy".into(), FieldType::String)),
                (
                    15,
                    (
                        "securityContext".into(),
                        FieldType::Message("SecurityContext".into()),
                    ),
                ),
                (16, ("stdin".into(), FieldType::Bool)),
                (17, ("stdinOnce".into(), FieldType::Bool)),
                (18, ("tty".into(), FieldType::Bool)),
                (
                    19,
                    (
                        "envFrom".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("EnvFromSource".into()))),
                    ),
                ),
                (20, ("terminationMessagePolicy".into(), FieldType::String)),
                // Field numbers 21-24 per k8s.io/api/core/v1 generated.proto.
                // Previously volumeDevices/resizePolicy/restartPolicy were at
                // 23/24/25 (shifted), so restartPolicy (field 24) was dropped
                // and resizePolicy mis-decoded.
                (
                    21,
                    (
                        "volumeDevices".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("VolumeDevice".into()))),
                    ),
                ),
                (
                    22,
                    ("startupProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    23,
                    (
                        "resizePolicy".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ContainerResizePolicy".into(),
                        ))),
                    ),
                ),
                (24, ("restartPolicy".into(), FieldType::String)),
            ]),
        }
    }

    fn container_port_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("hostPort".into(), FieldType::Int)),
                (3, ("containerPort".into(), FieldType::Int)),
                (4, ("protocol".into(), FieldType::String)),
                (5, ("hostIP".into(), FieldType::String)),
            ]),
        }
    }

    fn security_context_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "capabilities".into(),
                        FieldType::Message("Capabilities".into()),
                    ),
                ),
                (2, ("privileged".into(), FieldType::Bool)),
                (
                    3,
                    (
                        "seLinuxOptions".into(),
                        FieldType::Message("SELinuxOptions".into()),
                    ),
                ),
                (4, ("runAsUser".into(), FieldType::Int)),
                (5, ("runAsNonRoot".into(), FieldType::Bool)),
                (6, ("readOnlyRootFilesystem".into(), FieldType::Bool)),
                (7, ("allowPrivilegeEscalation".into(), FieldType::Bool)),
                (8, ("runAsGroup".into(), FieldType::Int)),
                (9, ("procMount".into(), FieldType::String)),
                (
                    11,
                    (
                        "seccompProfile".into(),
                        FieldType::Message("SeccompProfile".into()),
                    ),
                ),
                (
                    12,
                    (
                        "appArmorProfile".into(),
                        FieldType::Message("AppArmorProfile".into()),
                    ),
                ),
            ]),
        }
    }

    fn resource_requirements_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                // ResourceList values are Quantity submessages, not raw strings.
                (1, ("limits".into(), FieldType::QuantityMap)),
                (2, ("requests".into(), FieldType::QuantityMap)),
                (
                    3,
                    (
                        "claims".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("ResourceClaim".into()))),
                    ),
                ),
            ]),
        }
    }

    fn volume_schema() -> MessageSchema {
        // K8s proto: Volume { name = 1, volumeSource = 2 }
        // The VolumeSource is a NESTED message at field 2 (NOT inlined).
        // Its inner source types (hostPath=1, emptyDir=2, ... configMap=19) are
        // decoded by the VolumeSource schema. Because rusternetes' JSON structs
        // inline the source fields directly into Volume (no `volumeSource` key),
        // we mark field 2 as an inlined VolumeSource so its decoded keys are
        // merged up into the Volume object.
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (
                    2,
                    (
                        "volumeSource".into(),
                        FieldType::Inlined("VolumeSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn volume_source_schema() -> MessageSchema {
        // K8s proto VolumeSource — field numbers per
        // k8s.io/api/core/v1/generated.proto. Only the source types rusternetes
        // supports are listed; unknown ones are skipped harmlessly.
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "hostPath".into(),
                        FieldType::Message("HostPathVolumeSource".into()),
                    ),
                ),
                (
                    2,
                    (
                        "emptyDir".into(),
                        FieldType::Message("EmptyDirVolumeSource".into()),
                    ),
                ),
                (
                    6,
                    (
                        "secret".into(),
                        FieldType::Message("SecretVolumeSource".into()),
                    ),
                ),
                (
                    7,
                    ("nfs".into(), FieldType::Message("NFSVolumeSource".into())),
                ),
                (
                    10,
                    (
                        "persistentVolumeClaim".into(),
                        FieldType::Message("PersistentVolumeClaimVolumeSource".into()),
                    ),
                ),
                (
                    16,
                    (
                        "downwardAPI".into(),
                        FieldType::Message("DownwardAPIVolumeSource".into()),
                    ),
                ),
                (
                    19,
                    (
                        "configMap".into(),
                        FieldType::Message("ConfigMapVolumeSource".into()),
                    ),
                ),
                (
                    26,
                    (
                        "projected".into(),
                        FieldType::Message("ProjectedVolumeSource".into()),
                    ),
                ),
                (
                    28,
                    ("csi".into(), FieldType::Message("CSIVolumeSource".into())),
                ),
                (
                    29,
                    (
                        "ephemeral".into(),
                        FieldType::Message("EphemeralVolumeSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn volume_mount_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("readOnly".into(), FieldType::Bool)),
                (3, ("mountPath".into(), FieldType::String)),
                (4, ("subPath".into(), FieldType::String)),
                (5, ("mountPropagation".into(), FieldType::String)),
                (6, ("subPathExpr".into(), FieldType::String)),
                (7, ("recursiveReadOnly".into(), FieldType::String)),
            ]),
        }
    }

    fn env_var_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("value".into(), FieldType::String)),
                (
                    3,
                    (
                        "valueFrom".into(),
                        FieldType::Message("EnvVarSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn env_var_source_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "fieldRef".into(),
                        FieldType::Message("ObjectFieldSelector".into()),
                    ),
                ),
                (
                    2,
                    (
                        "resourceFieldRef".into(),
                        FieldType::Message("ResourceFieldSelector".into()),
                    ),
                ),
                (
                    3,
                    (
                        "configMapKeyRef".into(),
                        FieldType::Message("ConfigMapKeySelector".into()),
                    ),
                ),
                (
                    4,
                    (
                        "secretKeyRef".into(),
                        FieldType::Message("SecretKeySelector".into()),
                    ),
                ),
            ]),
        }
    }

    fn probe_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("handler".into(), FieldType::Message("ProbeHandler".into())),
                ),
                (2, ("initialDelaySeconds".into(), FieldType::Int)),
                (3, ("timeoutSeconds".into(), FieldType::Int)),
                (4, ("periodSeconds".into(), FieldType::Int)),
                (5, ("successThreshold".into(), FieldType::Int)),
                (6, ("failureThreshold".into(), FieldType::Int)),
                (7, ("terminationGracePeriodSeconds".into(), FieldType::Int)),
            ]),
        }
    }

    fn probe_handler_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("exec".into(), FieldType::Message("ExecAction".into()))),
                (
                    2,
                    ("httpGet".into(), FieldType::Message("HTTPGetAction".into())),
                ),
                (
                    3,
                    (
                        "tcpSocket".into(),
                        FieldType::Message("TCPSocketAction".into()),
                    ),
                ),
                (4, ("grpc".into(), FieldType::Message("GRPCAction".into()))),
            ]),
        }
    }

    fn pod_security_context_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "seLinuxOptions".into(),
                        FieldType::Message("SELinuxOptions".into()),
                    ),
                ),
                (2, ("runAsUser".into(), FieldType::Int)),
                (3, ("runAsNonRoot".into(), FieldType::Bool)),
                (
                    4,
                    (
                        "supplementalGroups".into(),
                        FieldType::Repeated(Box::new(FieldType::Int)),
                    ),
                ),
                (5, ("fsGroup".into(), FieldType::Int)),
                (6, ("runAsGroup".into(), FieldType::Int)),
                (
                    7,
                    (
                        "sysctls".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Sysctl".into()))),
                    ),
                ),
                (9, ("fsGroupChangePolicy".into(), FieldType::String)),
                (
                    10,
                    (
                        "seccompProfile".into(),
                        FieldType::Message("SeccompProfile".into()),
                    ),
                ),
                (
                    12,
                    (
                        "appArmorProfile".into(),
                        FieldType::Message("AppArmorProfile".into()),
                    ),
                ),
                (13, ("supplementalGroupsPolicy".into(), FieldType::String)),
            ]),
        }
    }

    /// Decode a protobuf message to JSON using the schema for the given message type.
    /// Returns None if the message type is not in the registry.
    pub fn decode_message(&self, msg_type: &str, data: &[u8]) -> Option<Value> {
        let schema = self.schemas.get(msg_type)?;
        Some(self.decode_with_schema(schema, data))
    }

    /// Decode protobuf bytes using a specific schema
    fn decode_with_schema(&self, schema: &MessageSchema, data: &[u8]) -> Value {
        let mut obj = Map::new();
        let mut repeated_fields: HashMap<String, Vec<Value>> = HashMap::new();
        let mut pos = 0;

        while pos < data.len() {
            // Read tag as varint
            let (tag, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            match wire_type {
                WIRE_VARINT => {
                    let (value, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;

                    if let Some((name, field_type)) = schema.fields.get(&field_num) {
                        let json_val = match field_type {
                            FieldType::Bool => Value::Bool(value != 0),
                            FieldType::Int => json!(value as i64),
                            _ => json!(value as i64),
                        };
                        match field_type {
                            FieldType::Repeated(_) => {
                                repeated_fields
                                    .entry(name.clone())
                                    .or_default()
                                    .push(json_val);
                            }
                            _ => {
                                obj.insert(name.clone(), json_val);
                            }
                        }
                    }
                }
                WIRE_64BIT => {
                    if pos + 8 > data.len() {
                        break;
                    }
                    let value = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    if let Some((name, _)) = schema.fields.get(&field_num) {
                        obj.insert(name.clone(), json!(value));
                    }
                }
                WIRE_LENGTH_DELIMITED => {
                    let (len, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;
                    let len = len as usize;
                    if pos + len > data.len() {
                        break;
                    }
                    let field_data = &data[pos..pos + len];
                    pos += len;

                    if let Some((name, field_type)) = schema.fields.get(&field_num) {
                        let json_val = self.decode_field_value(field_type, field_data);

                        match field_type {
                            FieldType::Repeated(_) => {
                                repeated_fields
                                    .entry(name.clone())
                                    .or_default()
                                    .push(json_val);
                            }
                            FieldType::StringMap => {
                                // Maps are encoded as repeated MapEntry messages.
                                // Each MapEntry has field 1 (key) and field 2 (value).
                                let (key, val) = decode_map_entry(field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, Value::String(val));
                                }
                            }
                            FieldType::MessageMap(ref msg_type) => {
                                // map<string, Message> — decode MapEntry with message value
                                let (key, val) =
                                    self.decode_message_map_entry(msg_type, field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, val);
                                }
                            }
                            FieldType::QuantityMap => {
                                // map<string, Quantity> (ResourceList) — the value
                                // is a Quantity submessage, so unwrap it rather than
                                // reading its bytes as a raw string.
                                let (key, val) = decode_quantity_map_entry(field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, Value::String(val));
                                }
                            }
                            FieldType::Inlined(ref msg_type) => {
                                // Nested message whose decoded keys are merged
                                // directly into THIS object (e.g. VolumeSource).
                                if let Some(Value::Object(inner)) =
                                    self.decode_message(msg_type, field_data)
                                {
                                    for (k, v) in inner {
                                        obj.insert(k, v);
                                    }
                                }
                            }
                            _ => {
                                // Match Go's `omitempty`: K8s gogo-protobuf marshals
                                // optional scalar strings tagged (gogoproto.nullable)=false
                                // by writing an EMPTY STRING onto the wire, whereas a JSON
                                // client omits the field entirely. Emitting the key here
                                // would yield `Some("")` in Rust structs that treat `None`
                                // as "unset", the root cause of a whole class of decode bugs
                                // (empty subPath/host/scheme/podManagementPolicy/phase/...).
                                // Skipping the insert makes protobuf decode produce exactly
                                // what JSON does. Scoped to scalar String/Quantity only:
                                // map values (StringMap), repeated elements, and IntOrString
                                // go through other branches and are untouched.
                                let skip_empty = matches!(
                                    field_type,
                                    FieldType::String | FieldType::Quantity
                                ) && json_val.as_str() == Some("");
                                if !skip_empty {
                                    obj.insert(name.clone(), json_val);
                                }
                            }
                        }
                    }
                }
                WIRE_32BIT => {
                    if pos + 4 > data.len() {
                        break;
                    }
                    let value = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    if let Some((name, _)) = schema.fields.get(&field_num) {
                        obj.insert(name.clone(), json!(value));
                    }
                }
                _ => break,
            }
        }

        // Insert accumulated repeated fields
        for (name, values) in repeated_fields {
            obj.insert(name, Value::Array(values));
        }

        Value::Object(obj)
    }

    /// Decode a single field value based on its type
    fn decode_field_value(&self, field_type: &FieldType, data: &[u8]) -> Value {
        match field_type {
            FieldType::String => Value::String(String::from_utf8_lossy(data).to_string()),
            FieldType::Quantity => {
                // K8s resource.Quantity is a submessage { string string = 1; }.
                // Unwrap field 1 — reading the raw submessage bytes as a string
                // yields corrupted values (e.g. `0a 01 35` -> "\n\u{1}5" not "5").
                Value::String(decode_quantity_submessage(data))
            }
            FieldType::Bytes => {
                use base64::Engine;
                Value::String(base64::engine::general_purpose::STANDARD.encode(data))
            }
            FieldType::Message(msg_type) => {
                if msg_type == "Time" {
                    // K8s Time is a Timestamp proto — decode to RFC3339 string
                    return decode_timestamp(data, false);
                }
                if msg_type == "MicroTime" {
                    // K8s MicroTime has the same wire shape as Time (seconds #1,
                    // nanos #2) but marshals to JSON with microsecond precision.
                    // Without this, fields like Event.eventTime decode to a map
                    // and fail deserialization ("invalid type: map, expected a
                    // string").
                    return decode_timestamp(data, true);
                }
                match self.decode_message(msg_type, data) {
                    Some(v) => v,
                    None => {
                        // Unknown message type — try to decode generically
                        debug!("Unknown proto message type: {}", msg_type);
                        Value::Object(Map::new())
                    }
                }
            }
            FieldType::Int => {
                // Length-delimited int is unusual — treat as a submessage or packed repeated
                if let Some((val, _)) = read_varint(data, 0) {
                    json!(val as i64)
                } else {
                    Value::Null
                }
            }
            FieldType::Bool => {
                if data.first() == Some(&1) {
                    Value::Bool(true)
                } else {
                    Value::Bool(false)
                }
            }
            FieldType::Repeated(inner) => {
                // Single element of a repeated field (not packed)
                self.decode_field_value(inner, data)
            }
            FieldType::StringMap => {
                // Should be handled at the caller level as MapEntry
                Value::Object(Map::new())
            }
            FieldType::MessageMap(_) => {
                // Should be handled at the caller level as MessageMapEntry
                Value::Object(Map::new())
            }
            FieldType::QuantityMap => {
                // Should be handled at the caller level as a Quantity MapEntry
                Value::Object(Map::new())
            }
            FieldType::IntOrString => {
                // K8s IntOrString: in protobuf, encoded as a message with
                // field 1 (type: int32), field 2 (intVal: int32), field 3 (strVal: string)
                decode_int_or_string(data)
            }
            FieldType::JsonRaw => {
                // K8s JSON type: a message with field 1 = bytes containing raw JSON.
                // Decode the message to extract the raw bytes, then parse as JSON.
                let mut pos = 0;
                while pos < data.len() {
                    let (tag, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;
                    let field_num = (tag >> 3) as u32;
                    let wire_type = (tag & 0x07) as u8;
                    if wire_type == WIRE_LENGTH_DELIMITED && field_num == 1 {
                        // field 1: raw bytes containing JSON
                        let (len, new_pos) = match read_varint(data, pos) {
                            Some(v) => v,
                            None => break,
                        };
                        pos = new_pos;
                        let len = len as usize;
                        if pos + len <= data.len() {
                            let raw = &data[pos..pos + len];
                            if let Ok(v) = serde_json::from_slice(raw) {
                                return v;
                            }
                            // If not valid JSON, return as string
                            return Value::String(String::from_utf8_lossy(raw).to_string());
                        }
                    } else {
                        // Skip unknown fields
                        match wire_type {
                            WIRE_VARINT => {
                                let _ = read_varint(data, pos).map(|(_, p)| pos = p);
                            }
                            WIRE_64BIT => {
                                pos += 8;
                            }
                            WIRE_LENGTH_DELIMITED => {
                                if let Some((len, new_pos)) = read_varint(data, pos) {
                                    pos = new_pos + len as usize;
                                } else {
                                    break;
                                }
                            }
                            WIRE_32BIT => {
                                pos += 4;
                            }
                            _ => break,
                        }
                    }
                }
                Value::Null
            }
            FieldType::Inlined(msg_type) => {
                // Standalone decode of an inlined message returns the object as-is
                // (merging into a parent is handled in decode_with_schema).
                self.decode_message(msg_type, data)
                    .unwrap_or_else(|| Value::Object(Map::new()))
            }
        }
    }

    /// Decode a protobuf map entry where value is a message type
    fn decode_message_map_entry(&self, msg_type: &str, data: &[u8]) -> (String, Value) {
        let mut key = String::new();
        let mut val = Value::Null;
        let mut pos = 0;
        while pos < data.len() {
            let (tag, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;
            if wire_type == WIRE_LENGTH_DELIMITED {
                let (len, new_pos) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = new_pos;
                let len = len as usize;
                if pos + len > data.len() {
                    break;
                }
                match field_num {
                    1 => {
                        key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    }
                    2 => {
                        val = self
                            .decode_message(msg_type, &data[pos..pos + len])
                            .unwrap_or(Value::Null);
                    }
                    _ => {}
                }
                pos += len;
            } else if wire_type == WIRE_VARINT {
                let (_, new_pos) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = new_pos;
            } else {
                break;
            }
        }
        (key, val)
    }

    /// Decode a full K8s protobuf-encoded resource (with k8s\0 prefix) to JSON.
    /// Returns (apiVersion, kind, json_bytes) on success.
    pub fn decode_k8s_resource(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < 5 || &data[0..4] != b"k8s\0" {
            return None;
        }
        let envelope = &data[4..];

        // Parse the Unknown envelope to get TypeMeta and raw bytes
        let mut api_version = String::new();
        let mut kind = String::new();
        let mut raw_bytes: Option<&[u8]> = None;

        let mut pos = 0;
        while pos < envelope.len() {
            let (tag, new_pos) = read_varint(envelope, pos)?;
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            if wire_type == WIRE_LENGTH_DELIMITED {
                let (len, new_pos) = read_varint(envelope, pos)?;
                pos = new_pos;
                let len = len as usize;
                if pos + len > envelope.len() {
                    break;
                }
                let field_data = &envelope[pos..pos + len];
                pos += len;

                match field_num {
                    1 => {
                        // TypeMeta
                        let mut tp = 0;
                        while tp < field_data.len() {
                            let (t, ntp) = read_varint(field_data, tp)?;
                            tp = ntp;
                            let fnum = (t >> 3) as u32;
                            let wt = (t & 0x07) as u8;
                            if wt == WIRE_LENGTH_DELIMITED {
                                let (slen, ntp) = read_varint(field_data, tp)?;
                                tp = ntp;
                                let slen = slen as usize;
                                if tp + slen <= field_data.len() {
                                    if let Ok(s) = std::str::from_utf8(&field_data[tp..tp + slen]) {
                                        match fnum {
                                            1 => api_version = s.to_string(),
                                            2 => kind = s.to_string(),
                                            _ => {}
                                        }
                                    }
                                }
                                tp += slen;
                            } else if wt == WIRE_VARINT {
                                let (_, ntp) = read_varint(field_data, tp)?;
                                tp = ntp;
                            } else {
                                break;
                            }
                        }
                    }
                    2 => {
                        // raw bytes — the serialized resource
                        raw_bytes = Some(field_data);
                    }
                    // field 3 = contentEncoding (string, skip)
                    // field 4 = contentType (string, skip)
                    _ => {}
                }
            } else if wire_type == WIRE_VARINT {
                let (_, new_pos) = read_varint(envelope, pos)?;
                pos = new_pos;
            } else if wire_type == WIRE_64BIT {
                pos += 8;
            } else if wire_type == WIRE_32BIT {
                pos += 4;
            } else {
                break;
            }
        }

        if api_version.is_empty() || kind.is_empty() {
            return None;
        }

        let raw = raw_bytes?;

        // Check if raw is already JSON
        if !raw.is_empty() && (raw[0] == b'{' || raw[0] == b'[') {
            return Some(raw.to_vec());
        }

        // Look up the schema. Most kinds are unique, but "Event" exists in BOTH
        // core/v1 and events.k8s.io/v1 with DIFFERENT wire field numbers, so
        // dispatch by apiVersion — otherwise an events.k8s.io/v1 Event decoded
        // with the core/v1 schema mis-maps every field.
        let schema_key: &str = if kind == "Event" && api_version.starts_with("events.k8s.io/") {
            "EventsV1Event"
        } else {
            kind.as_str()
        };
        if let Some(json_obj) = self.decode_message(schema_key, raw) {
            // Add apiVersion and kind to the JSON
            let result = match json_obj {
                Value::Object(m) => {
                    // Insert apiVersion/kind at the top (they're part of TypeMeta, not the raw message)
                    let mut ordered = Map::new();
                    ordered.insert("apiVersion".into(), Value::String(api_version));
                    ordered.insert("kind".into(), Value::String(kind));
                    // Merge the decoded fields
                    for (k, v) in m {
                        ordered.insert(k, v);
                    }
                    Value::Object(ordered)
                }
                other => other,
            };

            serde_json::to_vec(&result).ok()
        } else {
            warn!(
                "No schema found for kind '{}', cannot decode protobuf",
                kind
            );
            None
        }
    }
}

// ========== Helper functions ==========

/// Read a varint from data starting at pos. Returns (value, new_pos).
fn read_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        if pos >= data.len() {
            return None;
        }
        let b = data[pos] as u64;
        pos += 1;
        value |= (b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((value, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Decode a protobuf map entry (field 1 = key, field 2 = value, both strings)
fn decode_map_entry(data: &[u8]) -> (String, String) {
    let mut key = String::new();
    let mut val = String::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_LENGTH_DELIMITED {
            let (len, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            if let Ok(s) = std::str::from_utf8(&data[pos..pos + len]) {
                match field_num {
                    1 => key = s.to_string(),
                    2 => val = s.to_string(),
                    _ => {}
                }
            }
            pos += len;
        } else if wire_type == WIRE_VARINT {
            let (_, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
        } else {
            break;
        }
    }
    (key, val)
}

/// Decode a K8s `resource.Quantity` submessage to its string value.
///
/// On the wire a Quantity is `message Quantity { optional string string = 1; }`,
/// even though K8s JSON marshals it as a bare string. The bytes `0a 01 35` are
/// `Quantity{ string: "5" }`, which must decode to `"5"` — reading them as a raw
/// UTF-8 string would yield the corrupted `"\n\u{1}5"`.
fn decode_quantity_submessage(data: &[u8]) -> String {
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        match wire_type {
            WIRE_LENGTH_DELIMITED => {
                let (len, new_pos) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = new_pos;
                let len = len as usize;
                if pos + len > data.len() {
                    break;
                }
                if field_num == 1 {
                    return String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                }
                pos += len;
            }
            WIRE_VARINT => {
                if let Some((_, new_pos)) = read_varint(data, pos) {
                    pos = new_pos;
                } else {
                    break;
                }
            }
            WIRE_64BIT => pos += 8,
            WIRE_32BIT => pos += 4,
            _ => break,
        }
    }
    String::new()
}

/// Decode a map<string, Quantity> entry: field 1 = key (string),
/// field 2 = value (Quantity submessage, unwrapped via `decode_quantity_submessage`).
fn decode_quantity_map_entry(data: &[u8]) -> (String, String) {
    let mut key = String::new();
    let mut val = String::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_LENGTH_DELIMITED {
            let (len, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            let bytes = &data[pos..pos + len];
            match field_num {
                1 => key = String::from_utf8_lossy(bytes).to_string(),
                2 => val = decode_quantity_submessage(bytes),
                _ => {}
            }
            pos += len;
        } else if wire_type == WIRE_VARINT {
            if let Some((_, new_pos)) = read_varint(data, pos) {
                pos = new_pos;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    (key, val)
}

/// Decode a K8s Timestamp protobuf to RFC3339 string
fn decode_timestamp(data: &[u8], micro: bool) -> Value {
    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_VARINT {
            let (val, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            match field_num {
                1 => seconds = val as i64,
                2 => nanos = val as i32,
                _ => {}
            }
        } else {
            break;
        }
    }
    if seconds == 0 && nanos == 0 {
        return Value::Null;
    }
    // Convert to RFC3339. K8s metav1.MicroTime marshals with microsecond
    // precision (e.g. 2006-01-02T15:04:05.000000Z), whereas metav1.Time uses
    // second precision (2006-01-02T15:04:05Z).
    let dt = chrono::DateTime::from_timestamp(seconds, nanos as u32);
    match dt {
        Some(dt) if micro => Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()),
        Some(dt) => Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::String(format!("{}s", seconds)),
    }
}

/// Decode K8s IntOrString protobuf message
/// Proto: message IntOrString { int64 type = 1; int32 intVal = 2; string strVal = 3; }
fn decode_int_or_string(data: &[u8]) -> Value {
    let mut kind: i64 = 0; // 0 = int, 1 = string
    let mut int_val: i64 = 0;
    let mut str_val = String::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_VARINT {
            let (val, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            match field_num {
                1 => kind = val as i64,
                2 => int_val = val as i64,
                _ => {}
            }
        } else if wire_type == WIRE_LENGTH_DELIMITED {
            let (len, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            if field_num == 3 {
                str_val = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
            }
            pos += len;
        } else {
            break;
        }
    }
    if kind == 1 {
        Value::String(str_val)
    } else {
        json!(int_val)
    }
}

// Placeholder schemas for types we handle but don't need full detail
// These are empty — the decoder treats unknown fields as ignored
impl ProtoRegistry {
    // Additional placeholder types that we reference but don't need full schemas for
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        assert_eq!(read_varint(&[0x08], 0), Some((8, 1)));
        assert_eq!(read_varint(&[0x96, 0x01], 0), Some((150, 2)));
        assert_eq!(read_varint(&[0xac, 0x02], 0), Some((300, 2)));
    }

    #[test]
    fn test_decode_simple_message() {
        let registry = ProtoRegistry::new();
        // A simple LabelSelector with matchLabels = {"app": "nginx"}
        // Encoded as: field 1 (matchLabels) = MapEntry { key="app", value="nginx" }
        // MapEntry: field 1 (key) = "app", field 2 (value) = "nginx"
        // field 1 tag = 0x0a (field 1, wire type 2)
        let map_entry = {
            let mut buf = Vec::new();
            // key field: tag=0x0a, len=3, "app"
            buf.extend_from_slice(&[0x0a, 0x03]);
            buf.extend_from_slice(b"app");
            // value field: tag=0x12, len=5, "nginx"
            buf.extend_from_slice(&[0x12, 0x05]);
            buf.extend_from_slice(b"nginx");
            buf
        };

        let mut label_selector = Vec::new();
        // matchLabels field: tag=0x0a (field 1, wire 2), length, then map_entry
        label_selector.push(0x0a);
        label_selector.push(map_entry.len() as u8);
        label_selector.extend_from_slice(&map_entry);

        let result = registry.decode_message("LabelSelector", &label_selector);
        assert!(result.is_some());
        let val = result.unwrap();
        assert_eq!(
            val.pointer("/matchLabels/app"),
            Some(&Value::String("nginx".into()))
        );
    }

    #[test]
    fn test_decode_volume_with_nested_configmap_source() {
        // Regression: K8s proto Volume{name=1, volumeSource=2}; VolumeSource is a
        // NESTED message, and configMap is field 19 inside it. A previous bug
        // treated field 2 of Volume as `hostPath` directly, so every volume
        // decoded to `hostPath: {}` and pod-create JSON decode failed with
        // `missing field 'path'`. Verify a configMap volume round-trips.
        let registry = ProtoRegistry::new();

        // ConfigMapVolumeSource: localObjectReference{name=1}="my-configmap"
        //   localObjectReference is a nested message (field 1) with name=1.
        let local_obj_ref = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x0c]); // field 1 (name), len=12
            b.extend_from_slice(b"my-configmap");
            b
        };
        let configmap_source = {
            let mut b = Vec::new();
            // field 1 (localObjectReference), wire 2
            b.push(0x0a);
            b.push(local_obj_ref.len() as u8);
            b.extend_from_slice(&local_obj_ref);
            b
        };
        // VolumeSource: configMap = field 19 (tag = 19<<3 | 2 = 0x9a 0x01)
        let volume_source = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x9a, 0x01]); // field 19, wire 2
            b.push(configmap_source.len() as u8);
            b.extend_from_slice(&configmap_source);
            b
        };
        // Volume: name = field 1, volumeSource = field 2
        let volume = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x0b]); // field 1 (name), len=11
            b.extend_from_slice(b"test-volume");
            b.push(0x12); // field 2 (volumeSource), wire 2
            b.push(volume_source.len() as u8);
            b.extend_from_slice(&volume_source);
            b
        };

        let val = registry
            .decode_message("Volume", &volume)
            .expect("Volume should decode");

        // Must NOT contain a bogus hostPath
        assert!(
            val.pointer("/hostPath").is_none(),
            "configMap volume must not decode as hostPath: {val:?}"
        );
        assert_eq!(
            val.pointer("/name"),
            Some(&Value::String("test-volume".into()))
        );
        // configMap.name flattened from localObjectReference
        assert_eq!(
            val.pointer("/configMap/name"),
            Some(&Value::String("my-configmap".into())),
            "expected configMap.name=my-configmap, got {val:?}"
        );
    }

    #[test]
    fn test_decode_volume_with_hostpath_source() {
        // hostPath is field 1 inside VolumeSource; its `path` must survive.
        let registry = ProtoRegistry::new();
        let hostpath_source = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x04]); // field 1 (path), len=4
            b.extend_from_slice(b"/tmp");
            b
        };
        let volume_source = {
            let mut b = Vec::new();
            b.push(0x0a); // field 1 (hostPath), wire 2
            b.push(hostpath_source.len() as u8);
            b.extend_from_slice(&hostpath_source);
            b
        };
        let volume = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x02]); // field 1 (name), len=2
            b.extend_from_slice(b"hp");
            b.push(0x12); // field 2 (volumeSource), wire 2
            b.push(volume_source.len() as u8);
            b.extend_from_slice(&volume_source);
            b
        };
        let val = registry
            .decode_message("Volume", &volume)
            .expect("Volume should decode");
        assert_eq!(
            val.pointer("/hostPath/path"),
            Some(&Value::String("/tmp".into())),
            "expected hostPath.path=/tmp, got {val:?}"
        );
    }

    #[test]
    fn test_decode_deployment_spec_with_template() {
        let registry = ProtoRegistry::new();

        // Build a minimal DeploymentSpec protobuf:
        // field 1 (replicas): varint 1
        // field 3 (template): PodTemplateSpec with a container
        let mut spec = Vec::new();

        // replicas = 1 (field 1, wire type 0 = varint)
        spec.push(0x08); // field 1, varint
        spec.push(0x01); // value = 1

        // Build a minimal PodTemplateSpec
        let mut template = Vec::new();
        // PodTemplateSpec.spec (field 2) = PodSpec
        let mut pod_spec = Vec::new();
        // PodSpec.containers (field 2) = repeated Container
        let mut container = Vec::new();
        // Container.name (field 1) = "test"
        container.push(0x0a); // field 1, length-delimited
        container.push(0x04); // length = 4
        container.extend_from_slice(b"test");
        // Container.image (field 2) = "nginx"
        container.push(0x12); // field 2, length-delimited
        container.push(0x05); // length = 5
        container.extend_from_slice(b"nginx");

        // PodSpec field 2 (containers)
        pod_spec.push(0x12); // field 2, length-delimited
        pod_spec.push(container.len() as u8);
        pod_spec.extend_from_slice(&container);

        // PodTemplateSpec field 2 (spec)
        template.push(0x12); // field 2, length-delimited
        template.push(pod_spec.len() as u8);
        template.extend_from_slice(&pod_spec);

        // DeploymentSpec field 3 (template)
        spec.push(0x1a); // field 3, length-delimited
        spec.push(template.len() as u8);
        spec.extend_from_slice(&template);

        let result = registry.decode_message("DeploymentSpec", &spec);
        assert!(result.is_some());
        let val = result.unwrap();

        // Verify replicas
        assert_eq!(val.get("replicas"), Some(&json!(1)));

        // Verify template exists and has containers
        let tmpl = val.get("template").expect("template should exist");
        let spec_inner = tmpl.get("spec").expect("template.spec should exist");
        let containers = spec_inner
            .get("containers")
            .expect("containers should exist");
        assert!(containers.is_array());
        let first = &containers.as_array().unwrap()[0];
        assert_eq!(first.get("name"), Some(&Value::String("test".into())));
        assert_eq!(first.get("image"), Some(&Value::String("nginx".into())));
    }

    #[test]
    fn test_decode_e2e_pod_with_empty_service_account() {
        // Regression: e2e "lifecycle of Pods" test creates a Pod via protobuf with
        // serviceAccountName explicitly set to "". The scheduler must still see this
        // pod as schedulable (no nodeName, Pending/None phase).
        let registry = ProtoRegistry::new();

        // Build a protobuf-encoded Pod that matches the e2e "lifecycle of Pods" test:
        // metadata: name="pod-test", namespace="pods-6553", uid="..."
        // spec: containers=[{name:"agnhost", image:"agnhost:2.55"}],
        //       restartPolicy="Always", serviceAccountName=""
        // status: {} (empty PodStatus message)
        let mut pod = Vec::new();

        // Field 1: metadata (ObjectMeta)
        let mut meta = Vec::new();
        // name = "pod-test" (field 1)
        meta.push(0x0A); // field 1, wire 2
        meta.push(8);
        meta.extend_from_slice(b"pod-test");
        // namespace = "pods-6553" (field 3)
        meta.push(0x1A); // field 3, wire 2
        meta.push(9);
        meta.extend_from_slice(b"pods-6553");
        // uid = "test-uid-123" (field 5)
        meta.push(0x2A); // field 5, wire 2
        meta.push(11);
        meta.extend_from_slice(b"test-uid-123");

        pod.push(0x0A); // field 1, wire 2
        pod.push(meta.len() as u8);
        pod.extend_from_slice(&meta);

        // Field 2: spec (PodSpec)
        let mut spec = Vec::new();

        // containers = [{name: "agnhost", image: "agnhost:2.55"}]
        let mut container = Vec::new();
        container.push(0x0A); // field 1 (name), wire 2
        container.push(7);
        container.extend_from_slice(b"agnhost");
        container.push(0x12); // field 2 (image), wire 2
        container.push(12);
        container.extend_from_slice(b"agnhost:2.55");
        // args = ["pause"]
        container.push(0x22); // field 4 (args), wire 2
        container.push(5);
        container.extend_from_slice(b"pause");

        spec.push(0x12); // field 2 (containers), wire 2
        spec.push(container.len() as u8);
        spec.extend_from_slice(&container);

        // restartPolicy = "Always"
        spec.push(0x1A); // field 3, wire 2
        spec.push(6);
        spec.extend_from_slice(b"Always");

        // serviceAccountName = "" — gogo-proto writes this empty scalar onto the
        // wire (nullable=false), but decode must OMIT it to match Go's JSON omitempty.
        spec.push(0x42); // field 8, wire 2
        spec.push(0); // empty string

        // NOTE: field 10 (nodeName) and field 19 (schedulerName) are NOT present
        // because they are proto3 optional and not explicitly set

        pod.push(0x12); // field 2 (spec), wire 2
        pod.push(spec.len() as u8);
        pod.extend_from_slice(&spec);

        // Field 3: status (empty PodStatus)
        pod.push(0x1A); // field 3, wire 2
        pod.push(0); // empty message

        let result = registry.decode_message("Pod", &pod);
        assert!(result.is_some(), "Pod should decode");
        let val = result.unwrap();

        // Verify the decoded JSON has the right shape for the scheduler
        assert_eq!(
            val.pointer("/metadata/name"),
            Some(&Value::String("pod-test".into()))
        );
        assert_eq!(
            val.pointer("/metadata/namespace"),
            Some(&Value::String("pods-6553".into()))
        );
        // An empty serviceAccountName on the wire must decode as ABSENT (not
        // Some("")), so downstream defaulting treats it as unset → "default".
        assert!(
            val.pointer("/spec/serviceAccountName").is_none(),
            "empty serviceAccountName must be omitted, got {val:?}"
        );
        // nodeName should be absent (not set in protobuf)
        assert!(
            val.pointer("/spec/nodeName").is_none(),
            "nodeName should be absent"
        );
        // schedulerName should be absent
        assert!(
            val.pointer("/spec/schedulerName").is_none(),
            "schedulerName should be absent"
        );

        // Now deserialize into Pod struct and check scheduler-facing fields
        let pod_struct: rusternetes_common::resources::Pod =
            serde_json::from_value(val).expect("Pod should deserialize");

        let spec = pod_struct.spec.as_ref().expect("spec should exist");
        assert!(
            spec.service_account_name.is_none(),
            "empty serviceAccountName must decode to None, not Some(\"\")"
        );
        assert!(
            spec.node_name.is_none(),
            "nodeName should be None (not set in protobuf)"
        );
        assert!(
            spec.scheduler_name.is_none(),
            "schedulerName should be None (not set in protobuf)"
        );

        // The scheduler's filter: !has_node && (phase is None or Pending)
        let has_node = spec.node_name.as_deref().is_some_and(|n| !n.is_empty());
        let phase = pod_struct.status.as_ref().and_then(|s| s.phase.as_ref());
        let is_pending = matches!(
            phase,
            None | Some(rusternetes_common::types::Phase::Pending)
        );
        assert!(!has_node, "pod should have no node assigned");
        assert!(is_pending, "pod should be pending");

        // Scheduler name check: unwrap_or("default-scheduler")
        let pod_scheduler = spec
            .scheduler_name
            .as_deref()
            .unwrap_or("default-scheduler");
        assert_eq!(
            pod_scheduler, "default-scheduler",
            "scheduler name should default correctly"
        );
    }

    #[test]
    fn test_decode_cronjob_with_nested_job_template() {
        // Regression: batch/v1 CronJob had NO registered schema, so protobuf
        // creates fell through to the best-effort decoder and lost
        // spec.schedule → "missing field `schedule`" (400 Invalid) on every
        // conformance CronJob test. Also exercises the JobSpec field 14/15 fix
        // (podReplacementPolicy=14, managedBy=15 — previously mis-numbered).
        let registry = ProtoRegistry::new();

        // JobSpec { backoffLimit=7:int, podReplacementPolicy=14:str, managedBy=15:str }
        let job_spec = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x38, 0x04]); // field 7 (backoffLimit) varint = 4
            b.push(0x72); // field 14 (podReplacementPolicy), wire 2
            b.push(6);
            b.extend_from_slice(b"Failed");
            b.push(0x7a); // field 15 (managedBy), wire 2
            b.push(1);
            b.extend_from_slice(b"x");
            b
        };
        // JobTemplateSpec { spec=2:JobSpec }
        let job_template = {
            let mut b = Vec::new();
            b.push(0x12); // field 2 (spec), wire 2
            b.push(job_spec.len() as u8);
            b.extend_from_slice(&job_spec);
            b
        };
        // CronJobSpec { schedule=1:str, concurrencyPolicy=3:str, jobTemplate=5:msg }
        let cronjob_spec = {
            let mut b = Vec::new();
            b.push(0x0a); // field 1 (schedule), wire 2
            b.push(11);
            b.extend_from_slice(b"*/1 * * * *");
            b.push(0x1a); // field 3 (concurrencyPolicy), wire 2
            b.push(6);
            b.extend_from_slice(b"Forbid");
            b.push(0x2a); // field 5 (jobTemplate), wire 2
            b.push(job_template.len() as u8);
            b.extend_from_slice(&job_template);
            b
        };
        // CronJob { spec=2:CronJobSpec }
        let cronjob = {
            let mut b = Vec::new();
            b.push(0x12); // field 2 (spec), wire 2
            b.push(cronjob_spec.len() as u8);
            b.extend_from_slice(&cronjob_spec);
            b
        };

        let val = registry
            .decode_message("CronJob", &cronjob)
            .expect("CronJob should decode");

        assert_eq!(
            val.pointer("/spec/schedule"),
            Some(&Value::String("*/1 * * * *".into())),
            "spec.schedule must decode, got {val:?}"
        );
        assert_eq!(
            val.pointer("/spec/concurrencyPolicy"),
            Some(&Value::String("Forbid".into()))
        );
        assert_eq!(
            val.pointer("/spec/jobTemplate/spec/backoffLimit"),
            Some(&json!(4))
        );
        // JobSpec field-number fix: 14=podReplacementPolicy, 15=managedBy.
        assert_eq!(
            val.pointer("/spec/jobTemplate/spec/podReplacementPolicy"),
            Some(&Value::String("Failed".into())),
            "JobSpec field 14 must be podReplacementPolicy, got {val:?}"
        );
        assert_eq!(
            val.pointer("/spec/jobTemplate/spec/managedBy"),
            Some(&Value::String("x".into())),
            "JobSpec field 15 must be managedBy, got {val:?}"
        );
    }

    #[test]
    fn test_generated_registry_covers_core_kinds() {
        // Guards the build.rs codegen: every kind below must resolve to a
        // schema (decoding empty bytes yields an empty object, not None). If
        // vendoring or codegen regresses and drops a group, this fails instead
        // of silently mis-decoding at runtime.
        let r = ProtoRegistry::new();
        for kind in [
            "Pod",
            "PodSpec",
            "Container",
            "PodTemplate",
            "PodTemplateSpec",
            "Deployment",
            "DeploymentSpec",
            "ReplicaSet",
            "StatefulSet",
            "DaemonSet",
            "Job",
            "JobSpec",
            "CronJob",
            "CronJobSpec",
            "JobTemplateSpec",
            "Service",
            "ServiceSpec",
            "ConfigMap",
            "Secret",
            "ObjectMeta",
            "Namespace",
        ] {
            assert!(
                r.decode_message(kind, &[]).is_some(),
                "generated registry missing kind `{kind}`"
            );
        }
    }

    #[test]
    fn test_decode_probe_inlines_handler() {
        // Regression: proto Probe embeds ProbeHandler as field 1 (`handler`,
        // Go ,inline), so K8s JSON flattens the handler (httpGet/exec/...) onto
        // the Probe. If `handler` is not inlined, the decoded probe has no
        // handler and the kubelet can't run it — a readiness probe silently
        // becomes a no-op, and the [sig-apps] "halt if unhealthy" StatefulSet
        // test hangs forever waiting for Ready=false.
        let registry = ProtoRegistry::new();

        // HTTPGetAction { path=1:"/healthz" }
        let httpget = {
            let mut b = vec![0x0a, 0x08];
            b.extend_from_slice(b"/healthz");
            b
        };
        // ProbeHandler { httpGet=2:HTTPGetAction }
        let handler = {
            let mut b = vec![0x12, httpget.len() as u8];
            b.extend_from_slice(&httpget);
            b
        };
        // Probe { handler=1:ProbeHandler, initialDelaySeconds=2:5 }
        let probe = {
            let mut b = vec![0x0a, handler.len() as u8];
            b.extend_from_slice(&handler);
            b.extend_from_slice(&[0x10, 0x05]);
            b
        };

        let val = registry
            .decode_message("Probe", &probe)
            .expect("Probe should decode");

        assert_eq!(
            val.pointer("/httpGet/path"),
            Some(&Value::String("/healthz".into())),
            "httpGet must be flattened onto the Probe, got {val:?}"
        );
        assert!(
            val.pointer("/handler").is_none(),
            "handler must be inlined, not left nested: {val:?}"
        );
        assert_eq!(val.pointer("/initialDelaySeconds"), Some(&json!(5)));
    }

    #[test]
    fn test_decode_podspec_priority_field_numbers() {
        // Regression: PodSpec skipped field 23 (hostAliases), shifting every
        // field up by one. Upstream `priority` (int32, field 25) is defaulted
        // to 0 by admission; on a pod UPDATE round-trip that varint 0 landed on
        // our String `priorityClassName` → "invalid type: integer 0, expected a
        // string" (the [sig-node] var-expansion update failure). Verify field 24
        // decodes as the priorityClassName string and field 25 as the priority
        // integer.
        let registry = ProtoRegistry::new();

        let pod_spec = {
            let mut b = Vec::new();
            // field 24 (priorityClassName), wire 2: tag = 24<<3|2 = 194 → varint C2 01
            b.extend_from_slice(&[0xc2, 0x01, 0x04]);
            b.extend_from_slice(b"high");
            // field 25 (priority), wire 0 (varint): tag = 25<<3|0 = 200 → varint C8 01, value 0
            b.extend_from_slice(&[0xc8, 0x01, 0x00]);
            b
        };

        let val = registry
            .decode_message("PodSpec", &pod_spec)
            .expect("PodSpec should decode");

        assert_eq!(
            val.pointer("/priorityClassName"),
            Some(&Value::String("high".into())),
            "field 24 must be priorityClassName string, got {val:?}"
        );
        assert_eq!(
            val.pointer("/priority"),
            Some(&json!(0)),
            "field 25 must be priority integer 0 (not on priorityClassName), got {val:?}"
        );
    }

    #[test]
    fn test_decode_scale_subresource() {
        // Regression: the /scale subresource (ReplicationController/ReplicaSet/
        // Deployment/StatefulSet) serves autoscaling.v1.Scale, which had no
        // protobuf schema until the autoscaling/v1 group was vendored. Verify a
        // Scale{metadata, spec.replicas, status.replicas, status.selector}
        // round-trips. Proto: Scale{metadata=1, spec=2, status=3},
        // ScaleSpec{replicas=1}, ScaleStatus{replicas=1, selector=2}.
        let registry = ProtoRegistry::new();

        let metadata = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x03]); // ObjectMeta.name (field 1), len 3
            b.extend_from_slice(b"web");
            b
        };
        let spec = vec![0x08, 0x03]; // ScaleSpec.replicas (field 1, varint) = 3
        let status = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x08, 0x05]); // ScaleStatus.replicas (field 1) = 5
            b.extend_from_slice(&[0x12, 0x07]); // ScaleStatus.selector (field 2), len 7
            b.extend_from_slice(b"app=web");
            b
        };
        let scale = {
            let mut b = Vec::new();
            b.push(0x0a); // field 1 (metadata), wire 2
            b.push(metadata.len() as u8);
            b.extend_from_slice(&metadata);
            b.push(0x12); // field 2 (spec), wire 2
            b.push(spec.len() as u8);
            b.extend_from_slice(&spec);
            b.push(0x1a); // field 3 (status), wire 2
            b.push(status.len() as u8);
            b.extend_from_slice(&status);
            b
        };

        let val = registry
            .decode_message("Scale", &scale)
            .expect("Scale should decode");
        assert_eq!(
            val.pointer("/metadata/name"),
            Some(&Value::String("web".into()))
        );
        assert_eq!(val.pointer("/spec/replicas"), Some(&json!(3)));
        assert_eq!(val.pointer("/status/replicas"), Some(&json!(5)));
        assert_eq!(
            val.pointer("/status/selector"),
            Some(&Value::String("app=web".into())),
            "expected Scale to decode fully, got {val:?}"
        );
    }

    #[test]
    fn test_decode_event_micro_time() {
        // Regression: Event.eventTime is a metav1.MicroTime (field 10). MicroTime
        // has the same wire shape as Time but was not special-cased in
        // decode_field_value, so it decoded to a map and Event deserialization
        // failed with "eventTime: invalid type: map, expected a string"
        // ([sig-instrumentation] Events should delete a collection of events).
        // Verify eventTime decodes to a microsecond-precision RFC3339 string.
        let registry = ProtoRegistry::new();

        // MicroTime{ seconds=1136214245 (2006-01-02T22:04:05Z), nanos=123456000 }
        let micro_time = {
            let mut b = Vec::new();
            b.push(0x08); // field 1 (seconds), varint
                          // 1136214245 as varint
            let mut n = 1136214245u64;
            loop {
                let mut byte = (n & 0x7f) as u8;
                n >>= 7;
                if n != 0 {
                    byte |= 0x80;
                }
                b.push(byte);
                if n == 0 {
                    break;
                }
            }
            b.push(0x10); // field 2 (nanos), varint
            let mut nanos = 123456000u64;
            loop {
                let mut byte = (nanos & 0x7f) as u8;
                nanos >>= 7;
                if nanos != 0 {
                    byte |= 0x80;
                }
                b.push(byte);
                if nanos == 0 {
                    break;
                }
            }
            b
        };
        let event = {
            let mut b = Vec::new();
            // field 10 (eventTime), wire 2: tag = 10<<3|2 = 82 → 0x52
            b.push(0x52);
            b.push(micro_time.len() as u8);
            b.extend_from_slice(&micro_time);
            b
        };

        let val = registry
            .decode_message("Event", &event)
            .expect("Event should decode");
        let et = val
            .pointer("/eventTime")
            .expect("eventTime should be present");
        let s = et.as_str().unwrap_or_else(|| {
            panic!("eventTime must decode to a string, not {et:?}");
        });
        assert!(
            s.ends_with('Z') && s.contains('T'),
            "eventTime must be an RFC3339 string, got {s:?}"
        );
        assert!(
            s.contains(".123456"),
            "MicroTime must serialize with microsecond precision, got {s:?}"
        );
    }

    #[test]
    fn test_decode_lease_acquire_time_micro_time() {
        // Regression: LeaseSpec.acquireTime/renewTime (fields 3/4) are MicroTime.
        // Without the MicroTime special-case they decoded to a map and Lease
        // deserialization failed with "spec.acquireTime: invalid type: map,
        // expected a string" ([sig-node] Lease lease API should be available).
        let registry = ProtoRegistry::new();
        // MicroTime{ seconds=1136214245, nanos=0 }
        let micro_time = {
            let mut b = Vec::new();
            b.push(0x08); // field 1 (seconds), varint
            let mut n = 1136214245u64;
            loop {
                let mut byte = (n & 0x7f) as u8;
                n >>= 7;
                if n != 0 {
                    byte |= 0x80;
                }
                b.push(byte);
                if n == 0 {
                    break;
                }
            }
            b
        };
        let lease_spec = {
            let mut b = Vec::new();
            b.push(0x1a); // field 3 (acquireTime), wire 2
            b.push(micro_time.len() as u8);
            b.extend_from_slice(&micro_time);
            b
        };
        let val = registry
            .decode_message("LeaseSpec", &lease_spec)
            .expect("LeaseSpec should decode");
        assert!(
            val.pointer("/acquireTime")
                .and_then(|v| v.as_str())
                .is_some(),
            "acquireTime must decode to a string, got {val:?}"
        );
    }

    #[test]
    fn test_decode_volume_attributes_class() {
        // Regression: VolumeAttributesClass (storage.k8s.io) had no protobuf
        // schema at all, so protobuf creates/gets of the resource could not be
        // decoded. Proto: VolumeAttributesClass{metadata=1, driverName=2,
        // parameters=3 (map<string,string>)}.
        let registry = ProtoRegistry::new();

        let metadata = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x04]); // ObjectMeta.name (field 1), len 4
            b.extend_from_slice(b"fast");
            b
        };
        let param_entry = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x04]); // MapEntry.key (field 1), len 4
            b.extend_from_slice(b"iops");
            b.extend_from_slice(&[0x12, 0x04]); // MapEntry.value (field 2), len 4
            b.extend_from_slice(b"1000");
            b
        };
        let vac = {
            let mut b = Vec::new();
            b.push(0x0a); // field 1 (metadata), wire 2
            b.push(metadata.len() as u8);
            b.extend_from_slice(&metadata);
            b.extend_from_slice(&[0x12, 0x0f]); // field 2 (driverName), wire 2, len 15
            b.extend_from_slice(b"csi.example.com");
            b.push(0x1a); // field 3 (parameters), wire 2
            b.push(param_entry.len() as u8);
            b.extend_from_slice(&param_entry);
            b
        };

        let val = registry
            .decode_message("VolumeAttributesClass", &vac)
            .expect("VolumeAttributesClass should decode");
        assert_eq!(
            val.pointer("/metadata/name"),
            Some(&Value::String("fast".into()))
        );
        assert_eq!(
            val.pointer("/driverName"),
            Some(&Value::String("csi.example.com".into()))
        );
        assert_eq!(
            val.pointer("/parameters/iops"),
            Some(&Value::String("1000".into())),
            "expected parameters map to decode, got {val:?}"
        );
    }

    #[test]
    fn test_decode_quantity_submessage_unwraps_string() {
        // Regression: a K8s resource.Quantity is `message Quantity { string = 1 }`
        // on the wire, NOT a bare string. Bytes `0a 01 35` = Quantity{string:"5"}
        // must decode to "5"; reading the submessage bytes as a raw UTF-8 string
        // produced the corrupted "\n\u{1}5" that failed the client-side
        // resource.ParseQuantity regex (ResourceQuota / Downward API specs).
        assert_eq!(decode_quantity_submessage(&[0x0a, 0x01, 0x35]), "5");
        // "500m"
        assert_eq!(
            decode_quantity_submessage(&[0x0a, 0x04, b'5', b'0', b'0', b'm']),
            "500m"
        );
    }

    #[test]
    fn test_decode_scalar_quantity_field() {
        // EmptyDirVolumeSource.sizeLimit (field 2) is a scalar Quantity.
        let registry = ProtoRegistry::new();
        // Quantity{ string: "1Gi" }
        let quantity = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x03]); // Quantity.string (field 1), len 3
            b.extend_from_slice(b"1Gi");
            b
        };
        let empty_dir = {
            let mut b = Vec::new();
            b.push(0x12); // field 2 (sizeLimit), wire 2
            b.push(quantity.len() as u8);
            b.extend_from_slice(&quantity);
            b
        };
        let val = registry
            .decode_message("EmptyDirVolumeSource", &empty_dir)
            .expect("EmptyDirVolumeSource should decode");
        assert_eq!(
            val.pointer("/sizeLimit"),
            Some(&Value::String("1Gi".into())),
            "scalar Quantity must unwrap to a plain string, got {val:?}"
        );
    }

    #[test]
    fn test_decode_events_k8s_io_event_by_apiversion() {
        // Regression: kind "Event" exists in BOTH core/v1 and events.k8s.io/v1
        // with different wire field numbers. decode_k8s_resource must pick the
        // events.k8s.io schema by apiVersion; otherwise the eventTime MicroTime
        // (field 2) is decoded as core/v1 involvedObject and its inner varint
        // lands in involvedObject.kind → "invalid type: integer".
        fn ld(field: u32, bytes: &[u8]) -> Vec<u8> {
            let mut v = vec![((field << 3) | 2) as u8, bytes.len() as u8];
            v.extend_from_slice(bytes);
            v
        }
        let registry = ProtoRegistry::new();
        // MicroTime { seconds=1 (varint) } — field 1 varint tag 0x08
        let micro = vec![0x08, 0x80, 0x80, 0x80, 0x80, 0x06]; // seconds ≈ 1.6e9
        // ObjectReference { kind=1 "Pod", name=3 "mypod" }
        let mut regarding = Vec::new();
        regarding.extend(ld(1, b"Pod"));
        regarding.extend(ld(3, b"mypod"));
        // events.k8s.io/v1 Event { metadata=1, eventTime=2, regarding=8, note=10, type=11 }
        let meta = ld(1, b"ev1"); // ObjectMeta { name=1 "ev1" }
        let mut ev = Vec::new();
        ev.extend(ld(1, &meta));
        ev.extend(ld(2, &micro));
        ev.extend(ld(8, &regarding));
        ev.extend(ld(10, b"hello"));
        ev.extend(ld(11, b"Normal"));
        // Unknown envelope: typeMeta{apiVersion=1, kind=2}, raw=2
        let mut tm = Vec::new();
        tm.extend(ld(1, b"events.k8s.io/v1"));
        tm.extend(ld(2, b"Event"));
        let mut unknown = Vec::new();
        unknown.extend(ld(1, &tm));
        unknown.extend(ld(2, &ev));
        let mut body = b"k8s\0".to_vec();
        body.extend_from_slice(&unknown);

        let json_bytes = registry
            .decode_k8s_resource(&body)
            .expect("events.k8s.io Event should decode");
        let val: Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(val.pointer("/note"), Some(&Value::String("hello".into())));
        assert_eq!(val.pointer("/type"), Some(&Value::String("Normal".into())));
        assert_eq!(
            val.pointer("/regarding/kind"),
            Some(&Value::String("Pod".into())),
            "regarding must map correctly, got {val}"
        );
        assert!(
            val.pointer("/eventTime").is_some(),
            "eventTime must be present, got {val}"
        );
        assert!(
            val.pointer("/involvedObject").is_none(),
            "must NOT mis-decode as core/v1 involvedObject, got {val}"
        );
        // And it must deserialize into the Rust Event struct.
        let de: std::result::Result<rusternetes_common::resources::Event, _> =
            serde_json::from_slice(&json_bytes);
        assert!(de.is_ok(), "events.k8s.io Event must deserialize: {de:?}");
    }

    #[test]
    fn test_empty_scalar_string_is_omitted_not_some_empty() {
        // Root-cause regression: K8s gogo-protobuf writes optional scalar strings
        // tagged (gogoproto.nullable)=false onto the wire even when empty. Decoding
        // must OMIT them (like Go's JSON omitempty) rather than emit "", which would
        // become Some("") in Rust and defeat every `Option::is_none()` "unset" check.
        let registry = ProtoRegistry::new();
        // VolumeMount{ name="data", mountPath="/data", subPath="" }
        let vm = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x04]); // field 1 (name), wire 2, len 4
            b.extend_from_slice(b"data");
            b.extend_from_slice(&[0x1a, 0x05]); // field 3 (mountPath), wire 2, len 5
            b.extend_from_slice(b"/data");
            b.extend_from_slice(&[0x22, 0x00]); // field 4 (subPath), wire 2, len 0 (empty)
            b
        };
        let val = registry
            .decode_message("VolumeMount", &vm)
            .expect("VolumeMount should decode");
        assert_eq!(
            val.pointer("/name"),
            Some(&Value::String("data".into())),
            "non-empty scalar must be kept, got {val:?}"
        );
        assert_eq!(
            val.pointer("/mountPath"),
            Some(&Value::String("/data".into())),
            "non-empty scalar must be kept, got {val:?}"
        );
        assert!(
            val.pointer("/subPath").is_none(),
            "empty scalar string must be OMITTED (absent), not Some(\"\"), got {val:?}"
        );
    }

    #[test]
    fn test_decode_resource_quota_spec_hard_quantity_map() {
        // Regression: ResourceQuotaSpec.hard (field 1) is a map<string, Quantity>
        // (ResourceList). Each value is a Quantity submessage — decoding it as a
        // raw string corrupted every quota value ([sig-api-machinery]
        // ResourceQuota status specs). Verify hard["cpu"] decodes to "5".
        let registry = ProtoRegistry::new();
        // MapEntry{ key="cpu", value=Quantity{string:"5"} }
        let entry = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x0a, 0x03]); // key (field 1), len 3
            b.extend_from_slice(b"cpu");
            b.extend_from_slice(&[0x12, 0x03]); // value (field 2, Quantity submessage), len 3
            b.extend_from_slice(&[0x0a, 0x01, 0x35]); // Quantity{ string: "5" }
            b
        };
        let spec = {
            let mut b = Vec::new();
            b.push(0x0a); // field 1 (hard), wire 2
            b.push(entry.len() as u8);
            b.extend_from_slice(&entry);
            b
        };
        let val = registry
            .decode_message("ResourceQuotaSpec", &spec)
            .expect("ResourceQuotaSpec should decode");
        assert_eq!(
            val.pointer("/hard/cpu"),
            Some(&Value::String("5".into())),
            "map<string,Quantity> value must unwrap to \"5\", got {val:?}"
        );
    }
}

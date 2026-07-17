# cri-proto

Kubernetes Container Runtime Interface (CRI) v1 gRPC bindings, pinned to
[cri-api release-1.36](https://github.com/kubernetes/cri-api/tree/release-1.36)
(lockstep with critest v1.36.0).

- `cri_proto::v1` — all `runtime.v1` messages, tonic client stubs
  (`RuntimeServiceClient`, `ImageServiceClient`; feature `client`, default) and
  server stubs (`RuntimeServiceServer`, `ImageServiceServer`; feature `server`,
  default).
- `cri_proto::uds::connect_uds` — open a tonic `Channel` over a unix domain
  socket; accepts `unix:///run/x.sock` URIs and bare paths.

This crate must not depend on any other rusternetes crate (see
`plan/01-cri-crates.md`); it is intended for eventual crates.io publication.

## Build requirements

Code is generated at build time with `tonic-prost-build`, which needs
**`protoc`** (Protocol Buffers compiler) on `PATH`, or pointed to via the
`PROTOC` env var:

- macOS: `brew install protobuf`
- Debian/Ubuntu: `apt-get install protobuf-compiler`

(Decision from plan 01-S1: a documented system dependency instead of the
vendored `protobuf-src` build, which adds minutes of C++ compilation to clean
builds. Revisit if it hurts.)

## Re-vendoring the proto

`proto/release-1.36.proto` is checked in (builds never hit the network) and
keeps the upstream Kubernetes Authors Apache-2.0 header. Refresh it with:

```bash
bash scripts/vendor-cri-proto.sh            # release-1.36
bash scripts/vendor-cri-proto.sh release-X.Y
```

## Smoke test against containerd

`tests/containerd_smoke.rs` is `#[ignore]`d by default; run it on a Linux host
or VM with containerd:

```bash
cargo test -p cri-proto -- --ignored
CONTAINERD_SOCK=/run/containerd/containerd.sock cargo test -p cri-proto -- --ignored
```

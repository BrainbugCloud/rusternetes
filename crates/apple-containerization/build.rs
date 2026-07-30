fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/sandbox_context_v3.proto");

    // The guest agent (vminitd) is the *server*; we only ever need the client
    // stubs. The server stubs are still generated because the test suite stands
    // up an in-process fake guest to assert the call sequence (see tests/).
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/sandbox_context_v3.proto"], &["proto"])
        .map_err(|e| {
            format!(
                "failed to compile SandboxContext protos (is `protoc` installed? \
                 set PROTOC to its path if not on PATH): {e}"
            )
        })?;

    Ok(())
}

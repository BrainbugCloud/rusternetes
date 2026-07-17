fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/release-1.36.proto");

    let client = std::env::var_os("CARGO_FEATURE_CLIENT").is_some();
    let server = std::env::var_os("CARGO_FEATURE_SERVER").is_some();

    tonic_prost_build::configure()
        .build_client(client)
        .build_server(server)
        .compile_protos(&["proto/release-1.36.proto"], &["proto"])
        .map_err(|e| {
            format!(
                "failed to compile CRI protos (is `protoc` installed? \
                 set PROTOC to its path if not on PATH): {e}"
            )
        })?;

    Ok(())
}

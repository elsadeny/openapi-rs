fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // Rust 2024 makes environment mutation explicit.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/openapi.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/openapi.proto");
    Ok(())
}
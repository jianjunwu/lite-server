fn main() {
    // FILE_DESCRIPTOR_SET 供 gRPC reflection（评审低#12, opt-in）注册服务描述。
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR is set by cargo"))
            .join("liteserver_descriptor.bin");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(["."])
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&["src/proto/liteserver.proto"], &["src/proto"])
        .expect("Failed to compile protos");
}

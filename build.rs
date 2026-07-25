fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(["."])
        .compile_protos(&["src/proto/liteserver.proto"], &["src/proto"])
        .expect("Failed to compile protos");
}

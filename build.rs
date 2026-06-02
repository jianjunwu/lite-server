fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .bytes(["."])
        .compile_protos(&["src/proto/liteserver.proto"], &["src/proto"])
        .expect("Failed to compile protos");

    prost_build::compile_protos(
        &["src/proto/lite_server/endpoint/v1/endpoint.proto"],
        &["src/proto/"],
    )
    .expect("Failed to compile endpoint protos");
}

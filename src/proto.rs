pub mod liteserver {
    include!(concat!(env!("OUT_DIR"), "/liteserver.rs"));
}

/// Encoded `FileDescriptorSet` for `liteserver.proto`——gRPC reflection
/// （评审低#12, opt-in）注册服务/消息描述的输入。
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/liteserver_descriptor.bin"));

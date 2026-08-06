// prost 生成的 oneof 枚举按值内联最大的消息变体（StreamOpen ≥360B）；不改动
// wire 形态无法收敛,且 .boxed() 会改变全部匹配点的 Rust API——对生成代码
// 放行该 lint。
#[allow(clippy::large_enum_variant)]
pub mod liteserver {
    include!(concat!(env!("OUT_DIR"), "/liteserver.rs"));
}

/// Encoded `FileDescriptorSet` for `liteserver.proto`——gRPC reflection
/// （评审低#12, opt-in）注册服务/消息描述的输入。
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/liteserver_descriptor.bin"));

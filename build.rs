use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(
        &["src/proto/liteserver.proto"],
        &["src/proto/"],
    )?;
    Ok(())
}

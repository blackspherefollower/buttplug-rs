use std::io::Result;
fn main() -> Result<()> {
  prost_build::compile_protos(
    &["src/server/device/protocol/thehandy_v4/handy_rpc.proto"],
    &["src/server/device/protocol/thehandy_v4/"],
  )?;
  Ok(())
}

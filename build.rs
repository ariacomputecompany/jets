fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/jets.proto");
    prost_build::compile_protos(&["proto/jets.proto"], &["proto"])?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/jets.proto");
    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = std::path::Path::new(&out_dir).join("jets_descriptor.bin");
    let mut cfg = prost_build::Config::new();
    cfg.file_descriptor_set_path(&descriptor_path);
    cfg.compile_protos(&["proto/jets.proto"], &["proto"])?;
    Ok(())
}

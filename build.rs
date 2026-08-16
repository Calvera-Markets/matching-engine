fn main() {
    println!("cargo:rerun-if-changed=schemas");
    #[cfg(feature = "sbe")]
    generate();
}

#[cfg(feature = "sbe")]
fn generate() {
    let out = std::env::var("OUT_DIR").expect("OUT_DIR");
    for (src, dst) in [
        ("schemas/order_entry.xml", "order_entry.rs"),
        ("schemas/market_data.xml", "market_data.rs"),
    ] {
        println!("cargo:rerun-if-changed={src}");
        let code = ironsbe_codegen::generate_from_file(std::path::Path::new(src))
            .unwrap_or_else(|e| panic!("{src}: {e}"));
        std::fs::write(format!("{out}/{dst}"), code).unwrap();
    }
}

fn main() {
    println!("cargo:rerun-if-changed=schemas");
    if std::env::var("CARGO_FEATURE_SBE").is_err() {
        return;
    }
}

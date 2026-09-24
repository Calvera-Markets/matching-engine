# By default just list all available commands
[private]
default:
    @just -l

# Lints the code
lint: clippy fmt-check doc-check

# Formats the code with nightly cargo
fmt:
    cargo +nightly fmt

# Checks that the code is formatted
fmt-check:
    cargo +nightly fmt -- --check

# Checks that docs emit no warnings
doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --document-private-items --no-deps

# Checks clippy lints
clippy:
    cargo clippy --no-deps -- -D warnings

# Checks compilation
check:
    cargo check

alias b := build

# Builds in release mode
build:
    cargo build --release

alias t := test

# Runs the tests (default features: ouch, itch)
test *FLAGS:
    cargo test {{FLAGS}}

# FIX feature tests
test-fix *FLAGS:
    cargo test --features fix {{FLAGS}}

# SBE feature tests
test-sbe *FLAGS:
    cargo test --features sbe {{FLAGS}}

# Reports test coverage and writes badges/coverage.svg. Requires cargo-llvm-cov.
coverage *FLAGS:
    cargo llvm-cov {{FLAGS}}
    cargo llvm-cov report --json --summary-only --output-path target/coverage-summary.json
    python3 scripts/coverage_badge.py target/coverage-summary.json badges/coverage.svg

# OUCH in, ITCH out. Args pass through (--port, --wal, --cpu-*).
run *FLAGS:
    cargo run --release -- {{FLAGS}}

# FIX 4.4 in, ITCH out
fix *FLAGS:
    cargo run --release --example fix --features fix -- {{FLAGS}}

# SBE in, SBE out
sbe *FLAGS:
    cargo run --release --example sbe --features sbe -- {{FLAGS}}

# 10M synthetic tape, WAL on. Pass --no-wal, or --no-wal --codec ouch.
tape *FLAGS:
    cargo run --release --example tape_replay -- --synthetic 10000000 --no-latency {{FLAGS}}

# 10M synthetic tape, WAL off, SBE codec
tape-sbe *FLAGS:
    cargo run --release --example tape_replay --features sbe -- --synthetic 10000000 --no-wal --no-latency --codec sbe {{FLAGS}}

# 10M synthetic tape, WAL off, FIX codec
tape-fix *FLAGS:
    cargo run --release --example tape_replay --features fix -- --synthetic 10000000 --no-wal --no-latency --codec fix {{FLAGS}}

# Hot-path bench
bench *FLAGS:
    cargo bench --bench hot -- {{FLAGS}}

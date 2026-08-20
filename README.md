# Matching Engine

Exchange matching engine with 4 threads, 3 SPSC rings, WAL before apply.

```
TCP (OE)  →  ingress  →  Command SPSC  →  engine (WAL + book)  →  Event SPSC  →  egress (private ack)
                                                                ↳  Event SPSC  →  MD / multicast
```

Order-entry and market-data are chosen at **compile time** (`Ingress<Oe>`, `Egress<Oe>`, `MdPub<Md>`). The default binary is OUCH + ITCH. FIX and SBE are feature-gated examples.

- Private egress: ack to whoever sent the order.
- Public tape: a separate queue, so a slow multicast reader cannot stall.

The book uses handles. The engine maps `(client_fd, user_ref)` and turns fills into trades using the maker’s resting price. There is no fsync on the WAL.

## Features

| feature | role | crate |
|---|---|---|
| `ouch` (default) | order entry | hand-rolled OUCH 5.0 subset |
| `itch` (default) | market data | hand-rolled ITCH / MoldUDP |
| `fix` | order entry | IronFix 4.4 tag-value (Logon / HB / Logout, D / F / G → ER) |
| `sbe` | order entry + market data | IronSBE generated stubs (`schemas/*.xml`) |

SBE identity on the wire is numeric `userRef` (OUCH-class). FIX identity is ClOrdID (string table + mutex on the adapter).

```sh
# default: OUCH in, ITCH out
cargo run -p matching-engine --release -- --port 12345 --wal orderbook.wal

# FIX 4.4 in, ITCH out
cargo run -p matching-engine --release --example fix --features fix

# SBE in, SBE out
cargo run -p matching-engine --release --example sbe --features sbe
```

Cores default to 8 / 10 / 12 / 14 (`--cpu-ingress` and friends). Pinning is Linux-only.

## Tape and benches

Same A/C/M stream as the book, but each op is `MatchingEngine::step`. `--codec` encodes each op, runs the engine, then drains private + public rings through the order-entry and market-data codecs.

```sh
cargo test -p matching-engine
cargo test -p matching-engine --features fix
cargo test -p matching-engine --features sbe
cargo run -p calvera-books --release --example tape_replay -- \
  --synthetic 10000000 --no-latency
cargo run -p matching-engine --release --example tape_replay -- \
  --synthetic 10000000 --no-latency
cargo run -p matching-engine --release --example tape_replay -- \
  --synthetic 10000000 --no-wal --no-latency
cargo run -p matching-engine --release --example tape_replay -- \
  --synthetic 10000000 --no-wal --no-latency --codec ouch
cargo run -p matching-engine --release --example tape_replay --features sbe -- \
  --synthetic 10000000 --no-wal --no-latency --codec sbe
cargo run -p matching-engine --release --example tape_replay --features fix -- \
  --synthetic 10000000 --no-wal --no-latency --codec fix
cargo bench -p matching-engine --bench hot
```

On Apple Silicon, 10M synthetic rest/cancel/modify, `--no-latency`:

| | M ops/s | ns/op |
|---|---:|---:|
| book (solo) | 36.3 | 28 |
| engine, WAL off | 6.10 | 164 |
| engine, WAL on | 4.47 | 224 |
| engine, WAL off, ouch+itch | 5.29 | 189 |
| engine, WAL off, sbe | 5.11 | 196 |
| engine, WAL off, fix+itch | 0.55 | 1809 |

The WAL + map + event publish is most of the extra versus the book. OUCH+ITCH and SBE are a small codec tax on top of that (generated zero-copy stubs, numeric `userRef`, no ClOrdID table). FIX is tag-value parse/encode.

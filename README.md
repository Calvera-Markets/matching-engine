# Matching Engine

Exchange matching engine with 4 threads, 3 SPSC rings, WAL before apply.

```
TCP OUCH  →  ingress  →  Command SPSC  →  engine (WAL + book)  →  Event SPSC  →  egress (OUCH reply)
                                                           ↳  Event SPSC  →  ITCH / MoldUDP
```

- OUCH egress: private ack to whoever sent the order.
- ITCH: the public tape.

They are separate queues so a slow multicast reader cannot stall.

The book uses handles. The wire uses `(client_fd, userRefNum)`. The engine keeps that map, and turns fills into trades using the maker’s resting price. There is no fsync on the WAL.

```sh
cargo run -p matching-engine --release -- --port 12345 --wal orderbook.wal
```

Cores default to 8 / 10 / 12 / 14 (`--cpu-ingress` and friends). Pinning is Linux-only.

## Tape and benches

Same A/C/M stream as the book, but each op is `MatchingEngine::step`.

```sh
cargo test -p matching-engine
cargo run -p matching-engine --release --example tape_replay -- \
  --synthetic 10000000 --no-latency
cargo run -p matching-engine --release --example tape_replay -- \
  --synthetic 10000000 --no-wal --no-latency
cargo bench -p matching-engine --bench hot
```

On Apple Silicon, 10M synthetic rest/cancel/modify:

| | M ops/s | ns/op |
|---|---:|---:|
| book (solo) | 36.3 | 28 |
| engine, WAL off | 6.3 | 160 |
| engine, WAL on | 4.4 | 229 |

The WAL + map + event publish is most of the extra.

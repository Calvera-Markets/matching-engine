# Matching Engine

Exchange matching engine with a compile-time swappable wire protocol (OUCH, ITCH, FIX, and SBE). It runs on 4 threads and 3 SPSC rings, and writes the WAL before it applies the order.

The flow is the following:

```
TCP (OE)  →  ingress  →  Command SPSC  →  engine (WAL + book)  →  Event SPSC  →  egress (private ack)
                                                                ↳  Event SPSC  →  MD / multicast
```

Order-entry and market-data are chosen at compile time (`Ingress<Oe>`, `Egress<Oe>`, `MdPub<Md>`). The default binary is OUCH + ITCH. FIX and SBE are feature-gated.

- Private egress: ack to whoever sent the order.
- Public tape: a separate queue, so a slow multicast reader cannot stall.

The book gives each resting order a handle, and cancels, fills, and acks refer to that order by the handle. The engine maps `(client_fd, user_ref)` and turns fills into trades using the maker’s resting price. The WAL write is a store into the mmap, with no fsync, so the engine thread does not wait on disk. A power loss can drop the tail the kernel has not flushed.

## Features

| feature | role | crate |
|---|---|---|
| `ouch` (default) | order entry | hand-rolled OUCH 5.0 subset |
| `itch` (default) | market data | hand-rolled ITCH / MoldUDP |
| `fix` | order entry | IronFix 4.4 tag-value (Logon / HB / Logout, D / F / G → ER) |
| `sbe` | order entry + market data | IronSBE generated stubs (`schemas/*.xml`) |

SBE identity on the wire is numeric `userRef` (OUCH-class). FIX identity is ClOrdID (string table + mutex on the adapter).

## Running

`just` lists the commands. `just run` is OUCH in and ITCH out, `just fix` is FIX in and ITCH out, and `just sbe` is SBE in and out. `just test`, `just test-fix`, and `just test-sbe` are the three test sets.

Cores default to 8 / 10 / 12 / 14 (`--cpu-ingress` and friends). Pinning is Linux-only.

## Tape and benches

Same A/C/M stream as the book, but each op is `MatchingEngine::step`. `--codec` encodes each op, runs the engine, then drains private + public rings through the order-entry and market-data codecs.

`just tape` is the 10M synthetic run with the WAL on. `just tape --no-wal` turns the WAL off, and `just tape --no-wal --codec ouch` adds the OUCH and ITCH codec. `just tape-sbe` and `just tape-fix` are the WAL-off SBE and FIX runs. `just bench` is the hot-path bench.

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

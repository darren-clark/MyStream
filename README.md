# ToyStream

A toy Kafka-like message broker, written to explore zero-allocation protocol handling in C#.

Work in progress — the protocol layer is the part that exists; the server and client don't do
anything useful yet.

## Architecture

Messages are zero-copy `ref struct` views over the `PipeReader` buffer. A connection is
`Connection<THandler>`, which owns the only `async` method on the per-message path; routing lives in
a struct handler so the JIT specialises the connection per role and the dispatch devirtualizes.
Each role implements the switch for exactly the messages it serves, which makes a missing handler a
compile error and an unexpected message type a protocol violation rather than an unimplemented case.

- `ToyStream.Protocol` — the connection, dispatch and wire primitives
- `ToyStream.Server` / `ToyStream.Client` — the per-role handlers
- `ToyStream.Protocol.Benchmark` — a round-trip regression guard (run with `--job Long`)

## Performance research

That design was chosen after measuring eight alternatives. **The experiments are preserved on the
[`perf-research/dispatch-architecture`](../../tree/perf-research/dispatch-architecture) branch**,
which keeps every candidate implementation, the benchmarks, the JIT inspection harnesses and a
detailed write-up of the results — including the measurement traps and the explanations that turned
out to be wrong.

Headline: 1472 ns / 605 B per round trip at the start, ~418 ns / 0 B now.

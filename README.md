# Connection dispatch architecture — performance research

This branch is a frozen record of the experiments that led to choosing the **struct context**
(`ToyStream.Protocol/Dispatch/Context`) as ToyStream's connection architecture. It keeps every
candidate implementation, the benchmarks, the JIT inspection harnesses and the generator scripts, so
the reasoning can be re-examined or re-run later. Nothing here is intended to ship.

---

## 1. What was being decided

ToyStream needs one connection abstraction that both the client and the server are built on. There
will eventually be dozens of message types, and messages are zero-copy `ref struct` views over the
`PipeReader` buffer, so the design has to work without reflection, without boxing, and without the
message escaping the buffer that backs it.

Priorities, in order:

1. **Memory allocation** — hot paths should be completely allocation free.
2. **Performance** — lowest cost for invoking handlers.
3. **Developer experience** — a sensible way to implement the protocol. Both implementations
   (client and server) are written by the same person; this is a core internal component, not a
   general-purpose extensibility point.

AOT compatibility is required, which rules out reflection-based dispatch.

---

## 2. Candidates

| Variant | Location | Shape |
|---|---|---|
| `VirtualConnection` | `Dispatch/VirtualConnection.cs` | abstract method per message type |
| `MapConnection` | `Dispatch/MapConnection.cs` | `Dictionary<MessageType, Delegate>` |
| `MultipleImplementationConnection` | `Dispatch/MultipleImplementationConnection.cs` | single handler object + `isinst` type test |
| `GenericHandlerConnection` / `StructHandlerConnection` | `Dispatch/` | generic/devirtualized struct handler |
| `HeapMessageConnection` | `Dispatch/HeapMessageConnection.cs` | control: copies payload to the heap |
| `Direct` | `Dispatch/Direct/` | one hand-written read loop per role, non-virtual calls |
| `MyDirect` | `Dispatch/MyDirect/` | one-line arms via cached typed delegates |
| **`Context`** | **`Dispatch/Context/`** | **one shared read loop, routing in a struct context** |

---

## 3. Final result

Clean `--job Long` run (100 iterations x 3 launches x 15 warmup), .NET 10, request/response round trip
over paired in-memory pipes:

| Method | Mean | StdDev | Ratio | Allocated |
|---|---:|---:|---:|---:|
| Direct | 398.3 ns | 2.3% | 1.00 | — |
| **Context** | **417.6 ns** | **5.1%** | **1.05** | **—** |
| MyDirect | 533.3 ns | 7.7% | 1.34 | 400 B |

Overall journey for the round trip: **1472 ns / 605 B → ~400 ns / 0 B.**

**Decision: take `Context`, at ~5% over the hand-written `Direct` loop.**

Rationale:

- Allocation is a tie at zero, so priority #1 does not discriminate.
- 5% is a small price for the ergonomics at ~40 message types: a one-line switch arm plus an
  `IRequestHandler<TRequest, TResponse>` implementation, versus ~8-line hand-written switch bodies
  duplicated across two roles (~640 lines).
- `Context` keeps the compile-time guarantee that motivated `Direct`: the dispatcher is constrained
  `where THandler : struct, IRequestHandler<TRequest, TResponse>` and the context passes `this`, so a
  message in the switch without a handler is a **build error**. Client and server switches stay
  disjoint, so an unexpected message type is a protocol violation rather than a missing handler.
- `TContext : struct, IConnectionContext` makes the JIT specialise the connection per role, which
  devirtualizes dispatch and (verified) inlines the whole parse path into the read loop.
- `Context` propagates the parse shortfall into `ReadAtLeastAsync`, so a fragmented message costs one
  wakeup. `Direct` discards it (`TryDecode` uses `out _`) and re-reads blindly, waking once per
  arriving segment. **The in-memory benchmark cannot show this**, and on a real network it runs in
  `Context`'s favour.

Caveat: the 5% rests on one clean run, and `Context`'s StdDev (5.1%) is still double `Direct`'s.

---

## 4. The findings that actually mattered

### 4.1 Allocation: the wins, in order of size

| Change | Effect |
|---|---|
| `PipeScheduler.Inline` on both pipe schedulers | 1422 → 670 ns, work items 2 → 0, StdDev 9.1% → 1.4% |
| `PoolingAsyncValueTaskMethodBuilder` on per-message async methods | 564 B → 64 B |
| Removing an `async` wrapper around `FlushAsync` | 96 B → 0 B, 1023 → 776 ns |
| `ValueTask<T>` + `ManualResetValueTaskSourceCore` instead of `Task<T>` | 605 B → 564 B |

**Never wrap an already-`ValueTask`-returning API in an `async` method just to change its type.**
This pattern was the entire remaining 96 B per round trip:

```csharp
return Flush();
async ValueTask Flush() => await this.writer.FlushAsync(ct).ConfigureAwait(false);
```

Pooling did **not** save it — in a round trip the outer write is still suspended when the inner one
starts, so the overlapping boxes miss the pool. Return the task, or use an
`IsCompletedSuccessfully` fast path.

### 4.2 The dominant cost is the number of `async` methods entered per message

Each distinct `async` method on the per-message path costs roughly 66–170 ns. Factoring dispatch into
a per-message `async ValueTask DispatchRequest<,>()` cost ~38% regardless of how the handler was
reached.

**Rule: exactly one `async` method per message — the read loop.** Everything the connection exposes
is non-async, returning already-completed or forwarded `ValueTask`s. `Context` encodes this
structurally: `TryParseAndDispatch` is synchronous and returns a `ValueTask` for the loop to await.

### 4.3 Dispatch mechanism is irrelevant

Virtual method vs injected interface vs devirtualized struct handler: all within ~2%, and the ordering
flipped between runs. Measured in isolation (`Lookup/DispatchLookupBenchmarks.cs`, 30 entries):

| Step | Mean | Marginal |
|---|---:|---:|
| direct call (floor) | 0.187 ns | — |
| + delegate invoke | 0.404 ns | +0.22 |
| + cast and array index by type byte | 0.746 ns | +0.34 |
| + FrozenDictionary hash | 1.837 ns | +1.09 |
| + Dictionary hash | 2.571 ns | +1.83 |

Full dictionary dispatch is ~2.4 ns, about 0.4% of a round trip. **Choose on design, not speed.**

### 4.4 Zero-copy `ref struct` messages are worth it

`MessageRepresentationBenchmarks`: identical parse, but one arm copies the payload to a `byte[]`
(the cheapest heap option, no transcoding) before the handler sees it.

| Payload | ref struct | heap | ratio | ref struct alloc | heap alloc |
|---|---:|---:|---:|---:|---:|
| 4 B | 608.9 ns | 644.7 ns | 1.06 | 64 B | 128 B |
| 256 B | 613.5 ns | 668.6 ns | 1.09 | 64 B | 376 B |
| 4096 B | 759.3 ns | 1009.2 ns | 1.33 | 64 B | 4216 B |

Allocation is **flat** for ref structs at any payload size. Gen0 at 4 KB is 84x. This under-measures
the real benefit: single connection, no sustained GC pressure.

### 4.5 Switch width costs ~16%, equally for every shape

`SwitchScaleBenchmarks`, 30 generated message types:

| Shape | arms | types used | Mean | Ratio |
|---|---|---|---:|---:|
| Direct | 1 | 1 | 410.3 ns | 1.00 |
| Direct | 30 | 1 | 429.8 ns | 1.05 |
| Direct | 30 | 30 | 475.8 ns | 1.16 |
| Handler | 1 | 1 | 440.5 ns | 1.08 |
| Handler | 30 | 1 | 477.2 ns | 1.16 |
| Handler | 30 | 30 | 512.2 ns | 1.25 |

Total degradation is **+16.0% vs +16.3%** — identical. A 30-way jump table plus 30 generic
instantiations costs ~16% whatever shape you use, so **arm size is a maintenance question only**, and
the gap between shapes survives the switch growing.

---

## 5. Methodology, and the traps that were actually hit

- **Only trust ratios within a single run.** Absolute means drift ~4%+ between benchmark processes.
- **Never read a result off `--job Short`.** A short run claimed a change took the ratio from 1.14 to
  1.04. It had not — that run's Error bars were 180–249 ns, larger than the effect being claimed.
  Use `--job Long` for anything under ~5%.
- **An arm with anomalous StdDev is a broken measurement, not evidence.** Two runs of identical arms
  disagreed 41% vs 3%; the giveaway was one arm at StdDev 10% while the others sat at ~1%.
- **A bimodal arm has no comparable mean** — see §6, this was the big one.
- **Never build while a benchmark runs.** It contaminates results badly.
- **Do not bolt variants onto a shared base class to A/B them** — every arm then pays for whatever the
  base does. Duplicating the measured path is correct experimental design here, not sloppiness.
- **Equalise the arms.** `Direct`'s `ITopicService.CreateTopic` returned `void` while `Context`'s
  returned `ValueTask`, so one arm did strictly more work per message. Both now return `ValueTask`.

### Verifying inlining — use the JIT, not timings

Under BenchmarkDotNet the read loop runs on its own task, so `--disasm` never reaches it; it only
walks the call tree from the benchmark method. Use the standalone drivers:

```powershell
dotnet build ToyStream.Protocol.Benchmark/ToyStream.Protocol.Benchmark.csproj -c Release

$env:DOTNET_TieredCompilation="0"      # single FullOpts jit, no misleading tier0 body
$env:DOTNET_JitDisasmSummary="1"       # every jitted method + IL/code size
$env:DOTNET_JitStdOutFile="$PWD\jit.txt"
$env:DOTNET_JitDisasm="*MoveNext*"     # full listing for matching methods

dotnet run --project ToyStream.Protocol.Benchmark -c Release --no-build -- jitdump          # Context
dotnet run --project ToyStream.Protocol.Benchmark -c Release --no-build -- jitdump direct   # Direct
```

**Clear these variables before benchmarking** — they propagate into BenchmarkDotNet's child process
and will silently invalidate the run.

A method that is provably called but absent from the JitDisasmSummary was inlined.

What this showed: `ServerContext:TryParseAndDispatch` and `MessageDispatcher:TryParseAndDispatchRequest`
are **fully inlined** into the read loop's `MoveNext`. The codegen of the two designs is very close:

| Loop | IL | native |
|---|---:|---:|
| Direct server `MoveNext` | 821 | 4713 |
| Context server `MoveNext` | 851 | 4889 |
| Direct client `MoveNext` | 627 | 3279 |
| Context client `MoveNext` | 851 | 3517 |

`DirectServerConnection:HandleCreateTopic` is also not inlined (IL 39), so `Direct` carries a
non-inlined hot-path call too.

---

## 6. The long wrong turn

For most of this investigation `Context` measured ~13% slower than `Direct`, and four separate
interventions failed to move it:

| Change | Ratio before → after |
|---|---|
| Flush `IsCompletedSuccessfully` fast path + slimmer `DispatchResult` (~64 → ~24 bytes) | 1.14 → 1.15 |
| `[DoesNotReturn]` throw helper instead of an interpolated throw in the switch | 1.15 → 1.10 |
| `[MethodImpl(AggressiveInlining)]` on the one non-inlined dispatch helper | 1.10 → 1.13 |
| Equalising `ITopicService` to return `ValueTask` on both sides | 1.13 → 1.00, then 1.32 |

Six Long runs produced ratios of 1.00, 1.10, 1.13, 1.14, 1.15 and 1.32 — means from 441 to 521 ns —
with `Context`'s own StdDev at 10–13% while `Direct` sat at 2–4%.

**The cause was a `reader.TryRead(...)` probe in front of `ReadAtLeastAsync`.** It created a second
path through the loop whose selection depended on whether the peer's reply had already landed — two
timing modes. BenchmarkDotNet warned `MultimodalDistribution: bimodal (mValue 3.2)` on the *first*
Context run; the warning was noted and then ignored for five more runs while the unstable mean was
treated as comparable.

Deleting the probe halved the variance **and** produced the best mean yet (417.6 ns). It was never
buying anything: `ReadAtLeastAsync` already completes synchronously when the data is buffered.

### Three explanations for the gap that the evidence killed

1. *"The per-message call boundary costs ~14%."* **False** — the server dispatch path is fully inlined.
2. *"The switch plus interpolated throw blow the IL inline budget."* **False** — same evidence.
3. *"`where TResponse : class` forces `__Canon` sharing, which blocks inlining."* **False** —
   `Direct`'s `CompleteResponse<AcknowledgeMessage, AcknowledgeResponse>` has the identical
   struct+class generic mix and inlines fine. The one method that missed the inliner did so on plain
   callee IL size (76 bytes, decode and completion bundled together); `Direct` keeps them as two small
   methods.

---

## 7. Correctness bugs found along the way

- **Use-after-release (production bug).** `PipeReader.AdvanceTo` was being called *before* dispatching
  to the handler in every read path. `AdvanceTo` returns segments to the pipe, and messages are
  `ref struct` views over exactly those segments. It went unnoticed because no handler had ever read a
  payload. `allows ref struct` stops the message escaping to the heap but does nothing about the
  buffer underneath being recycled.
- **`Utf8String` constructors** copied `this.value` instead of `value`, silently producing empty
  strings.
- **`ConnectionBase.Log`** was called with interpolated string arguments on the hot path with the
  write commented out — the interpolation still allocated. Now `[Conditional("PROTOCOL_TRACE")]`.
- **Instance method group → delegate allocates 64 B per dispatch.** C# 11 caches method group
  conversions only when the delegate can live in a *static* field; an instance method group captures
  `this`. Cache it in a readonly field instead.
- **`PrimitiveReader`'s `bytesToRead` is a shortfall, not a total.** `ReadAtLeastAsync(n)` wants a
  total minimum buffer length, so passing the shortfall straight through makes it return immediately
  with the same insufficient buffer — an infinite spin. Correct value is
  `(int)buffer.Length + shortfall`.
- **`IMessage.TryRead` does not consume the message type byte.** Every read path must `Slice(1)` first;
  getting it wrong compiles fine and silently mis-parses the first field.

---

## 8. Reproducing

```powershell
# Benchmarks (use --job Long for anything under ~5%)
dotnet run --project ToyStream.Protocol.Benchmark -c Release -- --filter *AllocationBenchmarks* --job Long
dotnet run --project ToyStream.Protocol.Benchmark -c Release -- --filter *SwitchScale* --job Long
dotnet run --project ToyStream.Protocol.Benchmark -c Release -- --filter *Lookup* --job Long

# Regenerate the 30-type scale fixtures
./scripts/gen-scale-messages.ps1
./scripts/gen-scale-direct.ps1
./scripts/gen-scale-handler.ps1
```

Known issue: `ToyStream.Protocol.Test` does not compile in Release — `ToyProtocolPipeReader`'s
two-argument constructor is inside `#if DEBUG` but `UnitTest1.cs` uses it unconditionally. Build the
benchmark project directly rather than the whole solution when running Release benchmarks.

---

## 9. Gotchas worth remembering

- Messages are `ref struct`s, so `(IFoo)msg` is illegal everywhere (CS0030). Reach explicitly
  implemented interface members through a constrained generic helper.
- A `ref struct` decoded from a `PrimitiveReader` cannot escape the method that created the reader —
  the reader must be the *caller's* local. This is what forces `TryDecode(ref PrimitiveReader, ...)`
  and keeps decode helpers from being tidier.
- C# cannot infer type arguments from constraints, so `SendMessage<TReq, TRespMsg, TResp>` needs all
  three at the call site. Wrap per message type to hide it.
- `PoolingAsyncValueTaskMethodBuilder` requires the returned `ValueTask` be awaited exactly once.
- Generic methods on *interfaces* are expensive — one cost ~60 ns (9%) per call, and it was mistaken
  for a dispatch-strategy difference twice.
- A non-`readonly` struct held in a `readonly` field causes a defensive copy on every member call.
  Context types are `readonly struct` for this reason.

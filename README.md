# Coroutine Transport Protocol

Experiment with making a durable coroutine as both caller and callee.

## What This Repo Is

This repository was the design and exploration space for **Durable Effection** —
a version of [Effection](https://github.com/thefrontside/effection) that records
and replays effect resolutions using
[Durable Streams](https://github.com/durable-streams/durable-streams). The
actual implementation lives in an
[Effection fork](https://github.com/taras/effection/tree/durable-internals). This
repo contains the original design specification, learnings, and documentation.

## The Original Goal

The question we started with:

> Can a coroutine serve as both caller and callee — a unit of computation that
> can be suspended, serialized, transported, and resumed on the other side?

In traditional programming, a coroutine is tied to its process. It yields values,
receives results, and maintains local state — but all of this lives in memory. If
the process crashes, the coroutine's progress is lost.

We wanted to explore whether a coroutine's execution history could be captured as
a **protocol** — a stream of events that fully describes what the coroutine did
and what it received — so that:

1. A coroutine could be **resumed from any checkpoint** by replaying the stream
2. The stream could be **transported** across process boundaries
3. **Observers** could watch the coroutine's progress in real-time
4. The coroutine itself doesn't need to know whether it's running fresh or
   replaying — it behaves identically in both cases

This is the "coroutine transport protocol" — the execution history IS the
transport mechanism.

## What We Built

### DurableReducer

Effection's architecture is built around a **Reducer** — a priority queue that
drives generators forward by calling `effect.enter()`, waiting for resolution,
and feeding results back via `iterator.next(result)`. Every Effect in Effection
flows through this single point.

We built a **DurableReducer** that replaces the built-in Reducer via
`ReducerContext` injection. It operates in two modes:

**Recording** (live execution):
- When a generator yields an Effect, the DurableReducer writes an
  `effect:yielded` event to the Durable Stream
- When the Effect resolves, it writes an `effect:resolved` event
- Scope lifecycle events (`scope:created`, `scope:destroyed`, `scope:set`,
  `scope:delete`) are captured via `Api.Scope` middleware
- When the workflow returns, a `workflow:return` event is recorded

**Replay** (resuming from a stream):
- The DurableReducer reads stored events from the stream
- When a generator yields an Effect, instead of calling `effect.enter()`, it
  feeds the stored result directly via `iterator.next(storedResult)`
- The transition from replay to live happens automatically when stored events
  are exhausted
- The generator doesn't know whether it's replaying or running live

### Infrastructure vs User-Facing Effects

A critical design decision: not all Effection effects are recorded.

**User-facing effects** (recorded):
- `action()`, `sleep()`, `call()` — the coroutine's observable protocol
- `spawn()` — via the task result
- `resource()` — via the provided value
- `all()`, `race()`, `each()` — via their composed effects

**Infrastructure effects** (execute live, never recorded):
- `useCoroutine()`, `useScope()` — internal plumbing
- `do <set(...)>`, `do <delete(...)>` — context mutations
- `await resource`, `await task` — structural coordination

This distinction is what makes the approach work. Infrastructure effects
reconstruct the machinery (scopes, coroutines, context chains) that the
user-facing effects need to operate on. By letting them execute live during
both recording and replay, the scope hierarchy is always real and correct.

### Scope-Aware Replay

Effection's structured concurrency creates scope hierarchies — parent scopes own
child scopes, children cannot outlive parents, and cleanup flows from leaves to
root. When concurrent operations interleave (e.g., `each()` with spawned
children, `all()` with multiple branches), effects from different scopes appear
interleaved in the stream.

The DurableReducer maintains **per-scope replay cursors** to handle this
correctly. Each scope tracks its own position in the stream, so effects from
`each()`'s spawned child land in the child scope's cursor while `each.next()`
effects land in the caller scope's cursor.

### Divergence Detection

If the workflow code changes between runs, the replayed generator may yield a
different effect than what's stored. The DurableReducer compares the yielded
effect's `description` string against the stored `effect:yielded` event. A
mismatch throws a `DivergenceError`:

```
Divergence at offset 3: expected effect "sleep(100)" but got "sleep(200)"
```

This prevents silently feeding stale results to changed code.

### Serialization Boundaries

Not all effect results are JSON-serializable. Scope references, iterables, and
coroutine objects cannot be written to a stream. The DurableReducer uses a
`LiveOnlySentinel` — a JSON-safe placeholder — for these values. Infrastructure
effects that produce non-serializable results (like `useScope()`) are identified
by their descriptions and always execute live, so the sentinel is never consumed
during replay.

## Key Learnings

### 1. Generator Delegation Runs During Replay

This was the most surprising discovery. `yield* stream` (generator delegation)
is NOT an Effection effect — it's a language-level operation. The DurableReducer
can only suppress `effect.enter()` calls, not generator code between yield
points.

This means that during replay, the generator code DOES execute — helper
functions run, variables are computed, delegation chains unwind — but no effects
actually enter. The durable invariants assert that no `effect.enter()` calls
happen and no new `effect:yielded` events are recorded, NOT that generators
don't run.

This is actually what makes the approach robust: the generator reconstructs its
own local state naturally by running its code, it just doesn't interact with the
outside world.

### 2. Context Events Are Informational

We initially expected that `scope:set`/`scope:delete` events would need to be
rehydrated during replay — reading the stored context values and injecting them
into scopes. In practice, context operations re-execute live as infrastructure
effects (`do <set(...)>`). The events are recorded for **observability** (a
subscriber can watch context mutations) but the replay doesn't need them.

### 3. The Reducer Is the Right Interception Point

We considered several approaches for intercepting effects: middleware on
`Api.Scope`, wrapping individual operations, or patching the Effect protocol.
The Reducer turned out to be the only correct interception point because:

- It's the single place where `effect.enter()` is called
- It manages the instruction queue that enforces structured concurrency ordering
- Scope creation, destruction, and cleanup all flow through it
- Replaying through the instruction queue (not around it) means all bookkeeping
  still runs — cleanup handlers are registered, scope ordering is preserved

### 4. Effection's Determinism Makes This Possible

This approach works because Effection is **deterministic**: given the same
generator code and the same effect resolutions, execution follows exactly the
same path. The Reducer processes instructions synchronously in priority order.
There are no races, no callbacks firing in unpredictable order, no
non-deterministic scheduling.

This determinism means we can record just the effect resolutions and replay
the entire execution perfectly. The stream doesn't need to capture every
internal state transition — just the inputs from the outside world.

### 5. `each()` Scope Distribution

The `each()` primitive required the most nuanced handling. Its internal
structure creates a spawned child scope for the subscription, and effects
distribute across scopes in a specific pattern:

- First `subscription.next()` runs in the spawned child scope
- Subsequent `each.next()` calls run in the caller scope
- The terminal `next()` returning `{ done: true }` does NOT yield an action effect

Per-scope replay cursors were essential to handle this correctly.

### 6. The Stream IS the Checkpoint

Following the Durable Streams protocol, the offset after the last written event
IS the checkpoint. No separate checkpoint mechanism is needed. If a workflow
crashes after writing 5 events, resuming from the stream replays those 5 events
and transitions to live execution at the 6th effect.

## How This Relates to the Original Goal

The original question was whether a coroutine could serve as both caller and
callee — suspended, transported, and resumed. The answer is **yes**, with
Effection's structured concurrency model as the foundation.

**The Durable Stream IS the coroutine transport protocol.**

When a workflow runs, its execution history is written to a stream as a sequence
of typed events. This stream fully describes the coroutine's observable behavior:
what effects it yielded, what results it received, what scopes it created. Any
subscriber can read this stream and reconstruct the workflow's state.

To "transport" a coroutine:
1. Run the workflow, which writes events to a Durable Stream
2. Stop the process at any point
3. On the other side, create a fresh generator from the same code
4. Feed it the stored stream — the DurableReducer replays stored results without
   calling `effect.enter()`, fast-forwarding the generator to the exact point
   where it was interrupted
5. Continue live execution from that point

The coroutine doesn't know it was transported. From its perspective, it yielded
an effect and received a result — whether that result came from a live
`effect.enter()` or from a stored stream entry is invisible. This is the
"caller and callee" duality: the same coroutine code can be the caller
(yielding effects, receiving results) or the callee (being driven by a stream
of pre-recorded results).

**Key insight**: the protocol is not about serializing the coroutine's state. It's
about serializing the coroutine's *interactions* — the effects it yielded and the
results it received. The coroutine's internal state (local variables, loop
counters, accumulated values) is reconstructed naturally by re-running the
generator code with stored results. This is fundamentally simpler and more robust
than trying to snapshot and restore generator state.

## Implementation Status

All 7 implementation phases are complete with **139 test steps across 13 test
suites, 0 failures**.

| Phase | Description | Test Suite |
|-------|-------------|------------|
| 1 | DurableReducer + action/sleep | `test/durable.test.ts` (25 steps) |
| 2 | Api.Scope middleware + scope lifecycle | `test/durable-scope.test.ts` (14 steps) |
| 3 | Durable spawn + workflow:return | `test/durable.test.ts` |
| 4 | Durable resource + ensure | `test/durable-resource.test.ts` (12 steps) |
| 5 | Durable all + race | `test/durable-all-race.test.ts` (15 steps) |
| 6 | Durable each | `test/durable-each.test.ts` (11 steps) |
| 7 | Error handling + suspend + context | `test/durable-error-suspend-context.test.ts` (22 steps) |

Additionally, all 27 steps of Effection's own `test/run.test.ts` pass,
validating that the DurableReducer doesn't break any existing behavior.

## Repository Structure

```
coroutine-transport-protocol/     # This repo — design space & documentation
  README.md                       # This file
  docs/design-spec.md             # Original design specification (17 scenarios)
  deno.json                       # Import map pointing to Effection fork
  AGENTS.md                       # AI agent context for development sessions

effection (fork)                  # github.com/taras/effection branch: durable-internals
  lib/durable/
    durable-reducer.ts            # Core DurableReducer (688 lines)
    types.ts                      # DurableEvent union, DurableStream, DivergenceError
    stream.ts                     # InMemoryDurableStream
    mod.ts                        # Public exports
  lib/run.ts                      # Modified to accept { stream } option
  lib/scope-internal.ts           # Api.Scope middleware for scope lifecycle events
  test/durable*.test.ts           # 6 test suites (100+ steps)
```

## Links

- **Effection Fork**: [taras/effection](https://github.com/taras/effection/tree/durable-internals) (branch: `durable-internals`)
- **Effection**: [thefrontside/effection](https://github.com/thefrontside/effection)
- **Durable Streams**: [durable-streams/durable-streams](https://github.com/durable-streams/durable-streams)
- **Design Specification**: [docs/design-spec.md](docs/design-spec.md)

## Running

```bash
# In the Effection fork (~/Repositories/frontside/effection, branch: durable-internals)
deno test --allow-all
```

## Demo Commands

```bash
# In this repo
deno task demo:server

# Original simple pipeline demo
deno task demo:run

# New nested each() demo (nested loops + retries + timeout race)
deno task demo:nested-each

# Inspect stream events
deno task demo:read
deno task demo:read:nested-each
```

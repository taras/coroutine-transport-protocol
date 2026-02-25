# Protocol Redesign: Coroutine Protocol vs Effection Events

> Response to [Charles's review on PR #1](https://github.com/cowboyd/coroutine-transport-protocol/pull/1#pullrequestreview-2756938449)

## Charles's Feedback (Summary)

The external representation of a durable stream should NOT expose Effection internals (scopes, effects). A coroutine's external interface is minimal:

**Proposed event schema:**
```
yield | close<maybe<result>>
```

- `yield` — coroutine yielded a value (outbound)
- `close<Ok>` — normal completion
- `close<Err>` — error completion
- `close<None>` — cancelled (maybe; vs `Err("halted")`)
- `next(value)` — resumption value posted back as inbound events

Key rationale: this must support multiple language runtimes, not just JavaScript. Not every language has exceptions. The protocol should be the universal coroutine interface.

## Where We Agree

The external representation *should* be the coroutine protocol. The current 8-event schema leaks Effection implementation details:

| Current Event | What It Really Is |
|---|---|
| `effect:yielded` | A `yield` (coroutine yielded a value to the outside) |
| `effect:resolved` | A `next()` (outside world responded to the coroutine) |
| `effect:errored` | A `next()` with error (outside world sent an error) |
| `scope:created` | Internal — scope hierarchy management |
| `scope:destroyed` | Internal — scope cleanup lifecycle |
| `scope:set` | Internal — context mutation (informational) |
| `scope:delete` | Internal — context deletion (informational) |
| `workflow:return` | A `close<Ok>` (coroutine finished with a value) |

The mapping is clear: `effect:yielded` → `yield`, `effect:resolved`/`effect:errored` → `next()`, `workflow:return` → `close<Ok>`, and the scope events are internal metadata that shouldn't be first-class protocol events.

## The Hard Question: Concurrency

For a **single sequential coroutine**, `yield` / `close` / `next` is clearly sufficient — it's literally the generator protocol. The hard case is concurrent operations.

### What happens with spawn/all/race/each

Effection's structured concurrency creates multiple logical coroutines that interleave in one stream. For example, `each()` with spawned children:

```
yield "sleep(100)"        ← from parent coroutine
yield "action(fetch)"     ← from child coroutine (spawned by each)
next  Ok("data")          ← response to child's fetch
yield "each.next()"       ← from parent coroutine
next  Ok(undefined)       ← response to parent's sleep
next  Ok(item)            ← response to parent's each.next
```

In the current implementation, **per-scope replay cursors** solve this. Each scope tracks its own position in the stream, so the DurableReducer knows which `next()` response feeds which `yield`. Without scope IDs, how does replay attribute responses to the correct coroutine?

### Possible approaches

**Option A: Nested streams** — Each spawned child gets its own sub-stream. The parent's stream references child streams. Clean separation, but adds protocol complexity (streams-of-streams).

**Option B: Correlation IDs** — Each `yield` carries an ID, each `next()` references it. Flat stream, pairing is explicit. This is essentially what we have now (`effectId` links `effect:yielded` to `effect:resolved`), just with renamed events.

**Option C: Deterministic ordering** — Rely on the fact that Effection's Reducer processes instructions synchronously in priority order. If the generator code and effect resolutions are identical, execution follows exactly the same path. Position-in-stream is sufficient. Simplest protocol, but fragile if concurrency patterns change between versions.

**Option D: Coroutine-per-stream** — Each logical coroutine (parent, each spawned child) is its own stream with its own `yield`/`close`/`next` sequence. A parent's `yield` for "spawn" produces a reference to the child's stream. This is the most faithful to Charles's model — each stream IS one coroutine — but requires a stream-spawning mechanism.

### Open question

Does each spawned child get its own coroutine stream? Or does one stream carry interleaved events from multiple coroutines? If the latter, the protocol needs *something* to disambiguate — whether that's correlation IDs, deterministic position, or some other mechanism.

## The Two-Layer Insight

Perhaps the right framing is:

- **External protocol** (what observers/subscribers see): `yield` | `close<maybe<result>>`
- **Internal encoding** (what the DurableReducer writes/reads for replay): may carry annotations (correlation IDs, parent references) on yield events

The scope events we currently write (`scope:created`, `scope:destroyed`) might not be first-class protocol events but rather **metadata on yield events** — "this yield came from coroutine X which is a child of coroutine Y."

This preserves the principle (the protocol is about coroutines, not scopes) while giving the replay engine what it needs. An external observer sees `yield`/`close`/`next`; the DurableReducer sees the same events plus optional annotations it uses for replay correctness.

## Cancellation: `close<None>` vs `Err("halted")`

These have different semantics:

| | `close<None>` | `Err("halted")` |
|---|---|---|
| **Meaning** | Intentional — parent decided to stop child | Conflated with error |
| **In Effection** | Halting is first-class structured cleanup, not an error | Would make halt look like a failure |
| **For observers** | Dashboard shows "cancelled" — a valid terminal state | Dashboard shows "failed" — misleading |
| **Cross-language** | Universal — every runtime has "stopped without result" | Requires error semantics, which Charles noted not all languages have |

**Leaning toward `close<None>`** — cancellation is intentional and should be distinguishable from errors. In Effection, a halted workflow has its cleanup run successfully; it didn't fail, it was stopped. Observers care about this distinction.

Counter-argument: `close<None>` introduces a three-way terminal state (Ok/Err/None) which is more complex. `Err("halted")` keeps it binary (Ok/Err) at the cost of semantic precision.

## What `next()` as Inbound Means

Currently, the DurableReducer intercepts `effect.enter()` and wraps `routine.next()` to record resolutions. The "inbound" events (what feeds back into the coroutine) aren't separate stream entries — they're the `effect:resolved` events.

Charles is saying: make the `next()` value an **explicit inbound event**. This makes the stream truly bidirectional and self-describing:

```
offset 0: yield { description: "sleep(100)" }     ← outbound
offset 1: next  { value: undefined }               ← inbound (response to offset 0)
offset 2: yield { description: "call(fetchData)" } ← outbound
offset 3: next  { value: { items: [...] } }        ← inbound (response to offset 2)
offset 4: close { status: "ok", value: "done" }    ← outbound (terminal)
```

Reading this stream, you see the full conversation: what the coroutine asked for and what it received. This is powerful for observability and debugging.

**Open question**: Is the `next()` a separate stream entry (as above), or paired with the `yield` (e.g., a `yield` event that gets updated in-place when the response arrives)? Separate entries preserve append-only semantics. Paired entries reduce entry count but require mutation.

## Current Implementation Reference

The current event types (in `@effectionx/durably/types.ts`):

```typescript
type DurableEvent =
  | EffectYielded    // { type: "effect:yielded", scopeId, effectId, description }
  | EffectResolved   // { type: "effect:resolved", effectId, value: Json }
  | EffectErrored    // { type: "effect:errored", effectId, error: SerializedError }
  | ScopeCreated     // { type: "scope:created", scopeId, parentScopeId? }
  | ScopeDestroyed   // { type: "scope:destroyed", scopeId, result }
  | ScopeSet         // { type: "scope:set", scopeId, contextName, value }
  | ScopeDelete      // { type: "scope:delete", scopeId, contextName }
  | WorkflowReturn;  // { type: "workflow:return", scopeId, value: Json }
```

The DurableReducer (`durable-reducer.ts`, 825 lines) uses:
- `ReplayIndex` — indexes replay events for per-scope access with per-scope cursors
- `installScopeMiddleware()` — hooks into `api.Scope` to intercept create/destroy/set/delete
- `handleEffect()` — the core: checks replay index, does divergence detection, records or replays
- Infrastructure effect detection — `useCoroutine()`, `do <set(...)>`, `useScope()`, etc. always execute live

Key architectural fact: scope events (`scope:created`, `scope:destroyed`) are used during replay for two purposes:
1. **Scope ID assignment** — mapping live scopes to their recorded IDs so per-scope cursors work
2. **Divergence detection** — validating parent-child relationships match during replay

Context events (`scope:set`, `scope:delete`) are purely informational — they're recorded for observability but never consumed during replay.

## Specialist Input

### Effection Perspective
- Stripping scope tags forces the reducer to re-run live logic to rebuild scope mappings during replay — workable but more fragile as concurrency increases
- For `each()`/`all()`, a flat protocol needs either deterministic yield-order semantics or lightweight identifiers in yield payloads to attribute responses to specific iterations
- Cancellation as `close<None>` correctly models Effection's halt semantics (intentional, not an error)

### TypeScript Perspective
- Keep outbound (`yield`/`close`) and inbound (`next`) as separate union types — they're produced/consumed on opposite sides
- For `close`, use a three-way discriminant: `{ status: "ok", value: T } | { status: "err", error: E } | { status: "cancelled" }`
- Request/response pairing via sequence numbers on `yield` events, referenced by `next` events

# Durable Effection Design Specification

> **Status**: Archived. This document was the original design specification for
> Durable Effection, written as pseudo-tests describing desired behavior before
> implementation began. All 17 scenarios have been validated by real test suites
> in the [Effection fork](https://github.com/taras/effection/tree/durable-internals).

## Test Coverage Map

| # | Scenario | Validated By |
|---|----------|-------------|
| 1 | Basic Workflow (sleep + return) | `test/durable.test.ts` |
| 2 | Multi-step Workflow | `test/durable.test.ts` |
| 3 | Resume from Completed Stream | `test/durable.test.ts` |
| 4 | Resume Mid-Workflow (replay then live) | `test/durable.test.ts` |
| 5 | Spawn (parent/child interleaved) | `test/durable-scope.test.ts` |
| 6 | Spawn Resume (child scope reconstructed) | `test/durable.test.ts` |
| 7 | Resource Lifecycle | `test/durable-resource.test.ts` |
| 8 | Resource Resume (cleanup still registered) | `test/durable-resource.test.ts` |
| 9 | Context Set and Inherit | `test/durable-error-suspend-context.test.ts` |
| 10 | Error Handling | `test/durable-error-suspend-context.test.ts` |
| 11 | All (concurrent operations) | `test/durable-all-race.test.ts` |
| 12 | Race (first wins, losers halted) | `test/durable-all-race.test.ts` |
| 13 | Serialization Boundaries | Addressed by `LiveOnlySentinel` in DurableReducer |
| 14 | Divergence Detection | `test/durable.test.ts` |
| 15 | Ensure (cleanup registration) | `test/durable-resource.test.ts` |
| 16 | Each (stream iteration checkpoints) | `test/durable-each.test.ts` |
| 17 | Suspend (indefinite pause) | `test/durable-error-suspend-context.test.ts` |

---

## References

These references informed the design and were consulted throughout implementation.

1. **Effection Core Types** (`lib/types.ts`) — `Effect<T>` is the fundamental unit of work: `{ description, enter(resolve, routine) -> cleanup }`. `Operation<T>` is a generator that yields Effects. Effects are the ONLY things that cross the yield boundary.

2. **Effection Reducer** (`lib/reducer.ts`) — A priority queue that processes instructions synchronously. Drives generators forward: calls `effect.enter()`, waits for `resolve()`, then calls `iterator.next(result)`. Enforces structured concurrency ordering.

3. **Effection Action** (`lib/action.ts`) — Foundation for most effects. `sleep`, `suspend`, `lift`, `until` all reduce to `action`. Wraps callback-style APIs with cleanup.

4. **Effection Spawn** (`lib/spawn.ts`) — Gets current scope via `useScope()`, calls `scope.run(op)`. Creates a child task owned by the current scope. Child cannot outlive parent (structured concurrency).

5. **Effection Resource** (`lib/resource.ts`) — Pattern: setup -> `provide(value)` -> cleanup (in finally). The resource's scope stays alive until the enclosing scope exits.

6. **Effection Each** (`lib/each.ts`) — Iterates a stream: `for (let x of yield* each(stream)) { ... yield* each.next() }`. Internally uses spawn and context.

7. **Effection Inspector Scope Protocol** (`inspector/scope/protocol.ts`) — Defines `ScopeEvent` discriminated union: tree, created, destroying, destroyed, set, delete. Events are streamed over SSE.

8. **Effection Inspector Implementation** (`inspector/scope/implementation.ts`) — Hooks into `api.Scope` via `root.around()`. Emits ScopeEvents. Proves the middleware approach works for observing scope lifecycle.

9. **Inspector NodeMap** (`inspector/lib/update-node-map.ts`) — `reduce()` over ScopeEvent stream produces a materialized NodeMap. The stream is the source of truth; state is derived by reducing.

10. **Effection v4.1 Api System** (`lib/api.ts`, `lib/scope-internal.ts`) — `createApi()` creates interceptable API with middleware. `api.Scope` exposes create, destroy, set, delete. `scope.around(api, middleware)` installs middleware.

11. **Durable Streams Protocol** (`PROTOCOL.md`) — Append-only, offset-based, persistent byte streams. Idempotent producers for exactly-once writes. Stream closure signals EOF. Live modes: catch-up, long-poll, SSE.

12. **Durable Sessions Pattern** ([Electric SQL blog](https://electric-sql.com/blog/2026/01/12/durable-sessions-for-collaborative-ai)) — Multiplexes structured data over a single Durable Stream. Layered protocols: Durable Streams -> Durable State -> Specific protocols.

---

## Event Types

These are the events written to the Durable Stream, defined in `lib/durable/types.ts`:

```typescript
type DurableEvent =
  | { type: "scope:created";   scopeId: string; parentScopeId?: string }
  | { type: "scope:destroyed"; scopeId: string; result: { ok: true } | { ok: false; error: SerializedError } }
  | { type: "scope:set";       scopeId: string; contextName: string; value: Json }
  | { type: "scope:delete";    scopeId: string; contextName: string }
  | { type: "effect:yielded";  scopeId: string; effectId: string; description: string }
  | { type: "effect:resolved"; effectId: string; value: Json }
  | { type: "effect:errored";  effectId: string; error: SerializedError }
  | { type: "workflow:return";  scopeId: string; value: Json }

type SerializedError = { name: string; message: string; stack?: string }
```

---

## Scenarios

### 1. Basic Workflow — Sleep and Return

> The simplest case: a workflow that sleeps and returns a value. Each `yield*`
> produces an Effect. The durable runtime writes an `effect:yielded` event when
> the generator yields, and an `effect:resolved` event when the Effect resolves.

```typescript
function* greet(): Operation<string> {
  yield* sleep(100);
  return "hello";
}
```

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="sleep(100)" |
| 2 | `effect:resolved` effectId="e0" value=undefined |
| 3 | `workflow:return` scopeId="0" value="hello" |
| 4 | `scope:destroyed` scopeId="0" result={ ok: true } |

Stream is closed after `scope:destroyed` (workflow complete).

---

### 2. Multi-step Workflow

> Multiple yields produce multiple effect pairs in the stream. The stream is an
> ordered log of everything the workflow did.

```typescript
function* fetchAndProcess(): Operation<string> {
  yield* sleep(100);
  let data = yield* call(async () => "some data");
  return data.toUpperCase();
}
```

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="sleep(100)" |
| 2 | `effect:resolved` effectId="e0" value=undefined |
| 3 | `effect:yielded` scopeId="0" effectId="e1" description="async call ()" |
| 4 | `effect:resolved` effectId="e1" value="some data" |
| 5 | `workflow:return` scopeId="0" value="SOME DATA" |
| 6 | `scope:destroyed` scopeId="0" result={ ok: true } |

---

### 3. Resume from Completed Stream — Full Replay

> Given a stream from a completed execution, a fresh generator can be created
> and fed the stored results through the Reducer. The Reducer replays effects
> WITHOUT calling `enter()` — it feeds stored results through the instruction
> queue so that scope ordering, cleanup registration, and all bookkeeping still
> runs.

On resume:
1. Create a fresh generator from `greet()`
2. Read stream from offset -1 (beginning)
3. For each `effect:yielded` + `effect:resolved` pair:
   - The generator yields an Effect
   - Instead of calling `effect.enter()`, feed the stored result
   - The Reducer calls `iterator.next(storedResult)`
4. The generator reaches the return statement
5. The scope is a real Scope object with correct state

---

### 4. Resume Mid-Workflow — Replay Then Live

> A workflow was interrupted after the first effect resolved. On resume, the
> first effect is replayed (no `enter()`), and the second effect executes live.

```typescript
function* fetchAndProcess(): Operation<string> {
  yield* sleep(100);
  let data = yield* call(async () => "some data");
  return data.toUpperCase();
}
```

**Stream at time of interruption** (not closed):

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="sleep(100)" |
| 2 | `effect:resolved` effectId="e0" value=undefined |

On resume:
1. **Replay phase**: Generator yields sleep Effect -> feed stored result (undefined) without calling `enter()` -> generator advances past sleep
2. **Replay exhausted** -> transition to live phase
3. **Live phase**: Generator yields call Effect -> `effect.enter()` executes -> result written to stream -> workflow completes

---

### 5. Spawn — Parent and Child Interleaved

> When a workflow spawns a child, events from both parent and child are
> interleaved in the same stream. Each event carries a `scopeId` to distinguish
> which scope it belongs to.

```typescript
function* parent(): Operation<number> {
  let task = yield* spawn(function* child() {
    yield* sleep(500);
    return 42;
  });
  return yield* task;
}
```

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="root" |
| 1 | `effect:yielded` scopeId="root" effectId="e0" description="useScope()" |
| 2 | `effect:resolved` effectId="e0" value=\<scope-ref\> |
| 3 | `scope:created` scopeId="child-1" parentScopeId="root" |
| 4 | `effect:yielded` scopeId="child-1" effectId="e1" description="sleep(500)" |
| 5 | `effect:resolved` effectId="e1" value=undefined |
| 6 | `workflow:return` scopeId="child-1" value=42 |
| 7 | `scope:destroyed` scopeId="child-1" result={ ok: true } |
| 8 | `effect:yielded` scopeId="root" effectId="e2" description="await task" |
| 9 | `effect:resolved` effectId="e2" value=42 |
| 10 | `workflow:return` scopeId="root" value=42 |
| 11 | `scope:destroyed` scopeId="root" result={ ok: true } |

> Note: `scope:created` for the child comes from the `Api.Scope.create`
> middleware, the same mechanism the Inspector uses. The scope reference at
> offset 2 is NOT JSON-serializable — addressed by `LiveOnlySentinel`.

---

### 6. Spawn Resume — Child Scope Reconstructed

> On replay, `spawn`'s `scope:created` event triggers `Api.Scope.create`
> middleware which constructs a real child Scope. The Reducer must process the
> child's events before the parent resumes from the spawn point.

Key guarantee: if the workflow is halted AFTER resume but BEFORE completion, all
cleanup handlers registered during replay still run. The Reducer's destruction
ordering is preserved because we replayed through the instruction queue, not
around it.

---

### 7. Resource Lifecycle

> A resource sets up, provides a value, then cleans up when the enclosing scope
> exits. The stream captures the resource's scope creation and the provided
> value.

```typescript
function* useCounter(): Operation<{ count: number }> {
  return yield* resource(function* (provide) {
    let counter = { count: 0 };
    try {
      yield* provide(counter);
    } finally {
      counter.count = -1; // cleanup
    }
  });
}
```

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="root" |
| 1 | `effect:yielded` scopeId="root" effectId="e0" description="await resource" |
| 2 | `scope:created` scopeId="resource-1" parentScopeId="root" |
| 3 | `effect:resolved` effectId="e0" value={ count: 0 } |
| 4 | `workflow:return` scopeId="root" value=0 |
| 5 | `scope:destroyed` scopeId="resource-1" result={ ok: true } |
| 6 | `scope:destroyed` scopeId="root" result={ ok: true } |

> `resource-1` is destroyed BEFORE root (child before parent), preserving
> structured concurrency.

---

### 8. Resource Resume — Cleanup Still Registered

> After resuming from a stream, the resource's cleanup handler must still be
> registered. During replay, when `scope:created` is processed for the resource
> scope, a real scope is created. The resource's finally block is part of the
> generator — so cleanup is inherently registered as the generator is
> fast-forwarded through replay.

---

### 9. Context Set and Inherit

> Context values set on a scope are captured as `scope:set` events. On replay,
> the scope prototype chain is reconstructed (child scope inherits from parent),
> so context inheritance works exactly as in live execution.

```typescript
function* workflow(UserName): Operation<string> {
  yield* UserName.set("Alice");
  let task = yield* spawn(function* () {
    return yield* UserName.expect(); // inherits "Alice"
  });
  return yield* task;
}
```

> **Implementation insight**: Context events (`scope:set`/`scope:delete`) are
> recorded for observability but NOT rehydrated during replay. Context operations
> re-execute live as infrastructure effects (`do <set(...)>`).

---

### 10. Error Handling

> When a workflow throws, the error is captured via `effect:errored` and
> `scope:destroyed` with `{ ok: false }`. On resume, the error state is
> restored — the generator receives the error at the correct yield point.

**Generator error** (thrown after effect resolves):

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="sleep(100)" |
| 2 | `effect:resolved` effectId="e0" value=undefined |
| 3 | `scope:destroyed` scopeId="0" result={ ok: false, error: { name: "Error", message: "..." } } |

**Effect error** (effect itself rejects):

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="async call ()" |
| 2 | `effect:errored` effectId="e0" error={ name: "Error", message: "fetch failed" } |
| 3 | `scope:destroyed` scopeId="0" result={ ok: false, error: { name: "Error", message: "fetch failed" } } |

---

### 11. All — Concurrent Operations

> `all([opA, opB])` spawns each operation as a child, then awaits all of them.
> The stream shows interleaved events from multiple children, each with their
> own `scopeId`.

```typescript
function* workflow(): Operation<[string, number]> {
  return yield* all([
    function* () { yield* sleep(100); return "hello"; },
    function* () { yield* sleep(200); return 42; },
  ]);
}
```

> On replay, child scopes are created in the same order, and their effects are
> replayed with stored results. The `all()` combinator sees both children
> complete and returns the array.

---

### 12. Race — First Wins, Losers Halted

> `race([slow, fast])` spawns children concurrently. When the fast one resolves,
> the slow one is halted (its scope is destroyed). The stream captures the
> destroy event for the loser.

```typescript
function* workflow(): Operation<string> {
  return yield* race([
    function* slow() { yield* sleep(10000); return "slow"; },
    function* fast() { yield* sleep(100); return "fast"; },
  ]);
}
```

> Note: the slow child's sleep has a `effect:yielded` but no `effect:resolved`
> — it was halted. The `scope:destroyed` for "slow" signals the halt. On replay,
> the Reducer processes the destruction without attempting to resolve the pending
> effect.

---

### 13. Serialization Boundaries

> Not all effect results are JSON-serializable. Scope references, AbortSignals,
> server handles — these cannot be written to a stream.

**Resolution**: The DurableReducer uses a `LiveOnlySentinel` for non-serializable
values. Infrastructure effects (`useScope()`, `useCoroutine()`) are identified by
their descriptions and execute live during both recording and replay. Their
results are never written to the stream.

---

### 14. Divergence Detection

> If the workflow code changes between runs, a replayed generator may yield a
> different effect than what's stored in the stream. The runtime detects this and
> throws `DivergenceError`.

```
Effect divergence at offset 1: expected 'sleep(100)', got 'sleep(200)'
```

> The effect `description` string is the primary divergence signal. This is a
> simple heuristic — some changes might produce the same description.

---

### 15. Ensure — Cleanup Registration

> `ensure()` registers a cleanup function that runs when the current scope exits.
> It's syntactic sugar for a resource that does nothing except cleanup.

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="root" |
| 1 | `effect:yielded` scopeId="root" effectId="e0" description="await resource" |
| 2 | `scope:created` scopeId="ensure-1" parentScopeId="root" |
| 3 | `effect:resolved` effectId="e0" value=undefined |
| 4 | `effect:yielded` scopeId="root" effectId="e1" description="sleep(100)" |
| 5 | `effect:resolved` effectId="e1" value=undefined |
| 6 | `scope:destroyed` scopeId="ensure-1" result={ ok: true } |
| 7 | `scope:destroyed` scopeId="root" result={ ok: true } |

> On replay, `ensure-1`'s scope is recreated. Its destructor is registered in the
> generator's finally block. When root is destroyed, `ensure-1` is destroyed
> first (child before parent), and the cleanup runs.

---

### 16. Each — Stream Iteration with Checkpoints

> When iterating over a stream with `each()`, every iteration produces yield
> points that become checkpoints in the durable stream. If the workflow crashes
> after processing item N, it resumes at item N+1.

```typescript
function* processAll(): Operation<void> {
  let items = createChannel<string, void>();
  for (let item of yield* each(items)) {
    yield* processItem(item);
    yield* each.next();
  }
}
```

> **Implementation insight**: The first `subscription.next()` runs in the spawned
> child scope (scope-2), subsequent `each.next()` calls run in the caller scope
> (scope-1). The terminal `next()` that returns `{ done: true }` does NOT yield
> an action effect. Per-scope replay cursors handle this interleaving correctly.

---

### 17. Suspend — Indefinite Pause

> `suspend()` is `action(() => () => {}, "suspend")` — an action that never
> resolves. The workflow pauses indefinitely until its scope is destroyed.

```typescript
function* workflow(): Operation<void> {
  yield* suspend();
}
```

**Expected stream:**

| offset | event |
|--------|-------|
| 0 | `scope:created` scopeId="0" |
| 1 | `effect:yielded` scopeId="0" effectId="e0" description="suspend" |
| 2 | `scope:destroyed` scopeId="0" result={ ok: true } |

> No `effect:resolved` — suspend never resolves. On resume, the generator is at
> the suspend yield point, waiting. The suspend effect re-enters live.

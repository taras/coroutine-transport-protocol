# Two-Event Durable Stream Protocol

> Design proposal to simplify the durable stream protocol from 4 event types
> (`yield`, `next`, `spawn`, `close`) down to 2 (`yield`, `close<Result>`).

See also: [How the Reducer Drives the Generator](./how-the-reducer-drives-the-generator.md)

---

## 1. Motivation

The current 4-event protocol records more than necessary. Two of the four
event types can be eliminated:

- **`yield` + `next` always appear in pairs.** Every `yield` event is
  eventually followed by a `next` event with the same `effectId`. These
  can be merged into a single atomic `yield` that carries both the
  description and the result.

- **`spawn` records information the runtime can derive.** Coroutine IDs
  and parent-child relationships are a function of the deterministic
  execution order — the reducer can reconstruct them without reading
  them from the stream.

Simplifying to 2 events:

- **Cuts stream size roughly in half** (one event per effect instead of two)
- **Eliminates the `effectId` concept entirely** (no cross-referencing between events)
- **Simplifies the replay index** to two maps instead of five
- **Makes every stream entry self-contained** — no orphaned yields, no partial state

## 2. Proposed Protocol

### Event Types

```typescript
type DurableEvent = Yield | Close;
```

#### `yield` — an effect was executed and resolved

```typescript
interface Yield {
  type: "yield";
  coroutineId: string;
  description: string;
  result:
    | { status: "ok"; value?: Json }
    | { status: "err"; error: SerializedError };
}
```

Written **after** the effect resolves. Contains both what happened (description)
and what the outcome was (result). During replay, the description is used for
divergence detection, and the result is fed directly to the generator.

#### `close<Result>` — a coroutine reached a final state

```typescript
interface Close {
  type: "close";
  coroutineId: string;
  result:
    | { status: "ok"; value?: Json }
    | { status: "err"; error: SerializedError }
    | { status: "cancelled" };
}
```

Written when a coroutine finishes (returns, throws, or is cancelled).
Cannot be derived from `yield` events because:

- **Cancellation** produces no yield — the coroutine is halted externally
- **Return values** aren't captured by any yield event
- **Unhandled errors** that kill a coroutine may not correspond to a specific effect

## 3. Why `spawn` Can Be Removed

### The Determinism Argument

The question is: can coroutine IDs be assigned deterministically without
recording `spawn` events, even with concurrent combinators like `all()` and
`race()`?

**Yes.** Here's why.

### Effection's Reduce Loop is Synchronous

The reducer processes instructions in a synchronous `while` loop:

```
reduce(instruction)
│
├─ queue.enqueue(instruction)
├─ if (this.reducing) return    ← re-entrant guard
│
├─ this.reducing = true
├─ while (item = queue.dequeue())
│    ├─ iterator.next(value)    ← advance generator (synchronous)
│    ├─ effect.enter(next)      ← enter effect (may enqueue more instructions)
│    └─ continue
└─ this.reducing = false
```

When an effect resolves synchronously (as all replayed effects do), its
resolution is enqueued and processed in the same loop iteration. No async
gaps, no scheduling nondeterminism.

### Generators Are Deterministic

A generator function given the same inputs produces the same sequence of
yields. The `for` loop in `all()` always iterates its operations array
in order:

```typescript
// all() spawns children in array order — always
for (let operation of ops) {
  tasks.push(yield* spawn(member));
}
```

Each `spawn` yields a `useScope()` effect → creates a child scope →
fires the scope middleware. This happens within the synchronous reduce
loop, in deterministic order.

### Replay Preserves Ordering

During replay, every user-facing effect resolves synchronously (the
recorded result is fed back via `routine.next(Ok(recordedValue))`). This
means the reduce loop processes them in the exact same order as the
original run:

```
Live execution:                    Replay:
                                   
all([op1, op2, op3])               all([op1, op2, op3])
  │                                  │
  ├─ spawn(op1) → scope-1           ├─ spawn(op1) → scope-1  (same)
  ├─ spawn(op2) → scope-2           ├─ spawn(op2) → scope-2  (same)
  ├─ spawn(op3) → scope-3           ├─ spawn(op3) → scope-3  (same)
  │                                  │
  ├─ op1 yields effect               ├─ op1 yields effect
  │   └─ enters, resolves async      │   └─ replayed instantly
  ├─ op2 yields effect               ├─ op2 yields effect
  │   └─ enters, resolves async      │   └─ replayed instantly
  ...                                ...
```

The spawn order is identical because:

1. The `for` loop iterates in array order
2. Each `spawn` is processed synchronously within the reduce loop
3. The per-parent counter increments in the same sequence

### Per-Parent Counter Scheme

Instead of recording spawn events, the reducer assigns coroutine IDs
using a deterministic counter per parent:

```
root                    → "root"
  first child of root   → "root.0"
  second child of root  → "root.1"
    first child of .1   → "root.1.0"
  third child of root   → "root.2"
```

Since the execution order is deterministic, these IDs are stable across
live and replay runs. No stream events needed.

### Priority Queue Consideration

Effection's instruction queue is a priority queue ordered by scope depth.
This doesn't break determinism because priorities are **structurally
determined** by the code (scope nesting depth), not by timing. Two
instructions at the same priority are processed in FIFO order within
their tier.

## 4. Merging `yield` + `next` Into Atomic `yield`

### Current: Two Events Per Effect

```
offset 0: { type: "yield", effectId: "e1", description: "sleep(1000)" }
           ← effect executes, time passes →
offset 1: { type: "next",  effectId: "e1", status: "ok", value: null }
```

The `yield` is written before execution. The `next` is written after
resolution. They're linked by `effectId`.

### Proposed: One Event Per Effect

```
offset 0: { type: "yield", description: "sleep(1000)", result: { status: "ok", value: null } }
```

Written after resolution. The event is **atomic** — it contains
everything needed for replay.

### Write Timing Shifts

```
Current:                              Proposed:

generator yields effect               generator yields effect
  │                                     │
  ├─ stream.append(yield)              ├─ (nothing written yet)
  ├─ effect.enter()                    ├─ effect.enter()
  │   ... runs ...                     │   ... runs ...
  ├─ effect resolves                   ├─ effect resolves
  ├─ stream.append(next)               ├─ stream.append(yield + result)
  └─ generator receives value          └─ generator receives value
```

### Crash Semantics Are Simpler

With the current protocol, a crash between `yield` and `next` leaves an
orphaned `yield` in the stream. The replay index must handle this case
(treat it as "run live").

With atomic yields, a crash mid-effect means **nothing is written**.
The stream is always consistent. On restart, the effect simply runs
live because there's no recorded entry for it. No special handling needed.

### Divergence Detection Still Works

During replay, the reducer still peeks at the next recorded `yield`
before executing:

```
Current:                              Proposed:

peek replayIndex.peekYield()          peek replayIndex.peekYield()
compare description                   compare description
  match → look up resolution            match → result is inline
           feed to generator                    feed to generator
  mismatch → DivergenceError           mismatch → DivergenceError
```

The check happens before execution in both cases. The only difference
is that the result is already in the yield event rather than in a
separate `next` event.

### `effectId` Is Eliminated

With atomic yields, there's no need to cross-reference `yield` and `next`
events by `effectId`. The replay index is just a per-coroutine array of
`{ description, result }` entries, consumed sequentially by cursor position.

## 5. Simplified Replay Index

### Current (4-event protocol)

```
ReplayIndex:
  coroutineYields:  Map<coroutineId, Array<{ offset, event }>>    5 methods
  resolutions:      Map<effectId, next event>                     1 method
  spawnOrder:       Array<spawn event>                            4 methods
  closeEvents:      Map<coroutineId, close event>                 3 methods
  
  Total: ~13 methods, 5 data structures
```

### Proposed (2-event protocol)

```
ReplayIndex:
  yields:  Map<coroutineId, Array<{ description, result }>>      3 methods
  closes:  Map<coroutineId, close event>                         2 methods
  
  Total: ~5 methods, 2 data structures
```

The index becomes trivial:

```typescript
class ReplayIndex {
  private yields = new Map<string, Array<{ description: string; result: Result }>>();
  private cursors = new Map<string, number>();
  private closes = new Map<string, Close>();

  constructor(entries: StreamEntry[]) {
    for (let { event } of entries) {
      if (event.type === "yield") {
        let list = this.yields.get(event.coroutineId) ?? [];
        list.push({ description: event.description, result: event.result });
        this.yields.set(event.coroutineId, list);
      }
      if (event.type === "close") {
        this.closes.set(event.coroutineId, event);
      }
    }
  }

  peekYield(coroutineId: string) {
    let list = this.yields.get(coroutineId);
    let cursor = this.cursors.get(coroutineId) ?? 0;
    return list?.[cursor];
  }

  consumeYield(coroutineId: string) {
    let cursor = this.cursors.get(coroutineId) ?? 0;
    this.cursors.set(coroutineId, cursor + 1);
  }

  hasClose(coroutineId: string) {
    return this.closes.has(coroutineId);
  }

  getClose(coroutineId: string) {
    return this.closes.get(coroutineId);
  }
}
```

## 6. Stream Comparison

### Example: Queue Demo (3 keypresses, then read)

#### Current 4-event stream (14 events)

```
[0]  spawn  root → coroutine-1
[1]  yield  coroutine-1 e1  "action"                          (keypress)
[2]  next   coroutine-1 e1  ok "a"
[3]  yield  coroutine-1 e2  "action"                          (keypress)
[4]  next   coroutine-1 e2  ok "b"
[5]  yield  coroutine-1 e3  "action"                          (keypress)
[6]  next   coroutine-1 e3  ok "c"
[7]  yield  coroutine-1 e4  "action"                          (queue.next)
[8]  next   coroutine-1 e4  ok { done: false, value: "a" }
[9]  yield  coroutine-1 e5  "action"                          (queue.next)
[10] next   coroutine-1 e5  ok { done: false, value: "b" }
[11] yield  coroutine-1 e6  "action"                          (queue.next)
[12] next   coroutine-1 e6  ok { done: false, value: "c" }
[13] close  coroutine-1     ok
[14] close  root            ok
```

#### Proposed 2-event stream (8 events)

```
[0]  yield  root.0  "action"  result: ok "a"                   (keypress)
[1]  yield  root.0  "action"  result: ok "b"                   (keypress)
[2]  yield  root.0  "action"  result: ok "c"                   (keypress)
[3]  yield  root.0  "action"  result: ok { done: false, value: "a" }  (queue.next)
[4]  yield  root.0  "action"  result: ok { done: false, value: "b" }  (queue.next)
[5]  yield  root.0  "action"  result: ok { done: false, value: "c" }  (queue.next)
[6]  close  root.0  result: ok
[7]  close  root    result: ok
```

**43% fewer events.** Same information. No effect IDs. Self-contained entries.

### Example: Pipeline Demo (sleep + call pattern)

#### Current (8 events for 2 effects + lifecycle)

```
[0]  spawn  root → coroutine-1
[1]  yield  coroutine-1 e1  "sleep(2000)"
[2]  next   coroutine-1 e1  ok null
[3]  yield  coroutine-1 e2  "call(async () => ...)"
[4]  next   coroutine-1 e2  ok "ALPHA"
[5]  close  coroutine-1     ok
[6]  close  root            ok
```

#### Proposed (4 events)

```
[0]  yield  root.0  "sleep(2000)"         result: ok null
[1]  yield  root.0  "call(async () => …)" result: ok "ALPHA"
[2]  close  root.0  result: ok
[3]  close  root    result: ok
```

**50% reduction.**

## 7. Migration Path

The simplification can be done in two independent steps, each
separately testable:

### Step 1: Merge `yield` + `next` → atomic `yield`

**Scope:** Change the reducer to write a single `yield` event after
effect resolution instead of `yield` before and `next` after.

**Changes:**
- Remove `wrappedNext` pattern — instead, wrap the entire effect
  lifecycle and append one event on resolution
- Remove `effectId` from events and from the replay index
- Remove `resolutions` map from replay index
- Update replay path to read `result` inline from the `yield` event
- Simplify `ReplayIndex` constructor to skip infrastructure filtering
  of `next` events (they no longer exist)

**Risk:** Low. This is a mechanical refactor of the recording/replay
logic. No change to how effects are classified or how scopes are tracked.

**Validation:** All existing tests should pass with updated event
expectations (fewer events, same behavior).

### Step 2: Remove `spawn` events

**Scope:** Replace recorded coroutine IDs with deterministic per-parent
counters.

**Changes:**
- Remove `spawn` event type from the schema
- Remove `spawnOrder`/`spawnCursor`/`consumedSpawns` from replay index
- Change `installScopeMiddleware` to assign coroutine IDs via
  deterministic counter instead of reading from spawn events
- Use hierarchical ID scheme (`root`, `root.0`, `root.1`, `root.0.0`, etc.)

**Risk:** Medium. Depends on the deterministic spawn ordering argument
(see Section 3). Needs careful testing with:
- `all()` with varying numbers of branches
- `race()` with winners at different positions
- Nested `all()`/`race()` combinations
- `spawn()` inside resource teardown
- Multiple sequential `spawn()` calls

**Validation:** Concurrency test suite must pass. Additionally, add
tests that verify coroutine IDs are stable across live and replay runs.

## 8. Risks and Analysis

### Spawn During Teardown: The Sharpest Risk

This is the most dangerous edge case for the deterministic ID scheme.
It initially appears to create a circular dependency:

> ID assignment depends on execution order → which depends on scope
> tree shape → which depends on ID assignment

Here's the concrete scenario:

```typescript
yield* all([
  function*() {
    yield* ensure(function*() {
      yield* spawn(function*() {   // ← spawn during teardown
        yield* sleep(100);
      });
    });
    yield* suspend();
  },
  function*() {
    yield* sleep(500);
  },
]);
```

When the `all()` completes, the first branch is torn down, its
`ensure()` block fires, and a new child scope is spawned — all during
destruction. Does the per-parent counter produce the same ID during
replay?

#### How Effection Destruction Works

Scopes maintain a `destructors` Set. When a scope is destroyed:

```typescript
// scope-internal.ts — destroy()
for (let destructor of destructors) {
  destructors.delete(destructor);
  yield* destructor();      // ← runs each destructor in insertion order
}
```

`Set` iterates in **insertion order**. Destructors are registered in
two ways:

1. **Child scope creation:** When `scope.run()` creates a child, the
   child's `destroy()` function is registered as an `ensure()` callback
   on the parent:

   ```typescript
   // scope-internal.ts — buildScopeInternal()
   let destroy = () => api.invoke(scope, "destroy", [scope]);
   let unbind = parent ? parent.ensure(destroy) : () => {};
   ```

2. **Explicit `ensure()` calls:** User code registers teardown callbacks
   directly.

Both register into the same `destructors` Set, and the insertion order
is determined by when the registration happened during **forward
execution** — which is deterministic.

#### Why the Circular Dependency Doesn't Exist

The apparent cycle breaks because **IDs do not influence teardown
order**:

1. **Teardown order** is determined by the `destructors` Set insertion
   order, which is determined by the order that child scopes were
   created and `ensure()` callbacks were registered during forward
   execution.

2. **Forward execution order is deterministic** (as proven in Section 3:
   synchronous reduce loop, generators are deterministic, `all()`/`race()`
   spawn in array order).

3. **Therefore teardown order is deterministic** — it follows forward
   creation order (FIFO, not LIFO).

4. **Spawns during teardown** get counter-assigned IDs based on the
   per-parent counter, which increments in the same sequence because
   the teardown code path is the same.

The key insight: IDs are an **output** of the deterministic execution
order, not an **input** to it. The scope tree shape is determined by
the code, not by the IDs.

#### Trace Through the Scenario

**Live execution:**

```
all() spawns two children:
  root.0  (first branch — has ensure callback)
  root.1  (second branch — sleep(500))

Forward execution:
  root.0 registers ensure() → added to root.0's destructors Set
  root.0 suspends
  root.1 sleeps 500ms → completes → all() settles

Teardown of root.0:
  destructors Set = [ensure_callback, ...]
  ensure_callback fires:
    yield* spawn(fn) → creates child scope
    root.0's per-parent counter: 0 → child gets "root.0.0"
    root.0.0 runs sleep(100)
```

**Replay:**

```
all() spawns two children:
  root.0  (same — deterministic)
  root.1  (same — deterministic)

Forward execution (replayed instantly):
  root.0 registers ensure() → same insertion into destructors Set
  root.0 suspends (replayed)
  root.1 sleeps (replayed) → all() settles

Teardown of root.0:
  destructors Set = [ensure_callback, ...]  ← same insertion order
  ensure_callback fires:
    yield* spawn(fn) → creates child scope
    root.0's per-parent counter: 0 → child gets "root.0.0"  ← same ID
    root.0.0 runs sleep(100) (live or replayed)
```

The counter path is identical because the `destructors` Set was
populated in the same deterministic order during forward execution.

#### Important Note: FIFO Teardown

Effection tears down in **insertion order** (FIFO), not reverse creation
order (LIFO). This is unlike most frameworks but doesn't affect the
determinism argument — insertion order is deterministic regardless of
direction.

#### Required Test Case

The following test should verify counter stability across live and
replay for spawn-during-teardown:

```typescript
// Verify: ensure() block that spawns a child, where the ensure
// block is inside a scope that was spawned by all().
// The spawned child's coroutine ID must be identical across runs.

function* workflow(): Operation<void> {
  yield* all([
    function*() {
      yield* ensure(function*() {
        let task = yield* spawn(function*() {
          yield* sleep(50);
        });
        yield* task;
      });
      yield* suspend();
    },
    function*() {
      yield* sleep(200);
      return;
    },
  ]);
}

// Run 1: record to stream
// Run 2: replay from stream
// Assert: coroutine IDs in stream are identical in both runs
```

### Dynamic Spawn Counts

If replay takes a different branch and spawns a different number of
children, the per-parent counters will diverge. This is expected and
correct — it means the code has changed between runs. The divergence
will be caught at the next `yield` where the description no longer
matches, producing a `DivergenceError`.

This is the same behavior as the current protocol — if the code changes,
replay detects it via description mismatch.

### Loss of "Pending Effect" Visibility

With atomic yields, the stream never contains an in-progress effect.
You can't inspect the stream and see "sleep(1000) is currently running."
The trade-off is simplicity — every entry is complete.

For debugging, the reducer could maintain an in-memory "current effect"
per coroutine that isn't persisted, providing the same observability
without stream complexity.

### `effectId` Removal

Some debugging tools and test helpers reference effect IDs. After the
simplification, effects are identified by `(coroutineId, cursorOffset)`.
This is arguably better for debugging — you can say "the 3rd effect in
coroutine root.0" instead of "effect-47."

### Stream Format Compatibility

The 2-event protocol is not backward-compatible with existing 4-event
streams. Streams recorded with the old protocol cannot be replayed with
the new reducer. This is acceptable for a POC but would need a migration
strategy for production use.

## 9. Lifecycle Diagram (2-Event Protocol)

```
                       ┌─────────────────────────────┐
                       │     durable(fn, { stream })  │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │  Read stream → ReplayIndex   │
                       │  (just yields + closes)      │
                       │  Install scope middleware    │
                       │  Set ReducerContext          │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │   scope.run(operation)       │
                       │   generator starts           │
                       │   coroutine ID: "root"       │
                       └──────────────┬──────────────┘
                                      │
             ┌────────────────────────▼────────────────────────┐
             │               reduce(instruction)               │
             │                                                 │
             │  iterator.next(previousValue)                   │
             │       │                                         │
             │       ▼                                         │
             │  generator yields Effect { description, enter } │
             │       │                                         │
             │       ▼                                         │
             │  handleEffect(effect, routine)                  │
             └───────┬─────────────────────────┬──────────────┘
                     │                         │
          ┌──────────▼──────────┐   ┌──────────▼──────────┐
          │   infrastructure    │   │     user-facing      │
          │   (execute live,    │   │                      │
          │    not recorded)    │   │                      │
          └─────────────────────┘   └──────────┬──────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  replayIndex.peekYield      │
                                │  (coroutineId)              │
                                └──────┬────────────┬─────────┘
                                       │            │
                                  has entry    no entry
                                       │            │
                        ┌──────────────▼──┐  ┌──────▼──────────────┐
                        │   REPLAY PATH   │  │     LIVE PATH       │
                        │                 │  │                     │
                        │ Compare         │  │ effect.enter()      │
                        │ description     │  │                     │
                        │   │             │  │  ... runs ...       │
                        │  match          │  │                     │
                        │   │             │  │ effect resolves     │
                        │ Read result     │  │   │                 │
                        │ from same       │  │   ▼                 │
                        │ entry           │  │ stream.append({     │
                        │   │             │  │   type: "yield",    │
                        │ Feed to         │  │   coroutineId,      │
                        │ generator via   │  │   description,      │
                        │ routine.next()  │  │   result            │
                        │                 │  │ })                  │
                        │ (effect.enter   │  │                     │
                        │  never called)  │  │ routine.next(result)│
                        └────────┬────────┘  └──────────┬─────────┘
                                 │                      │
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 generator receives value
                                 synchronous code runs
                                            │
                                            ▼
                                 next yield* ───┐
                                                │
                                  back to reduce()


Scope creation (no stream event):

  yield* spawn(op)
    │
    ├─ scope.create() fires middleware
    ├─ Assign coroutineId via per-parent counter
    │    parent "root" + counter 0 → "root.0"
    │    parent "root" + counter 1 → "root.1"
    │    parent "root.1" + counter 0 → "root.1.0"
    └─ (no event written to stream)


Scope destruction:

  scope.destroy()
    │
    ├─ Determine final status (ok / err / cancelled)
    ├─ stream.append({
    │    type: "close",
    │    coroutineId,
    │    result: { status, value?, error? }
    │  })
    └─ Unregister coroutine
```

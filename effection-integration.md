# Durable Execution for Effection: Architecture Research

**Status:** Research synthesis for review
**Audience:** Charles, Taras
**Inputs:** Two-event durable execution spec (v2), Effection source (lib/), AGENTS.md, Charles's type-constraint feedback

---

## 1. Executive summary

This document maps the two-event durable execution protocol onto Effection's
runtime architecture and incorporates Charles's insight that **type-level
constraints** should replace runtime effect classification. The key conclusion:

- Effection's reducer does not need to change.
- Durability is implemented entirely within a new `DurableEffect` type whose
  `enter()` method handles replay, divergence detection, and persist-before-resume.
- A `Workflow<T>` type constrains generators at compile time so that only
  durable-safe effects can be yielded. All `Workflow`s are `Operation`s, but
  not all `Operation`s are `Workflow`s.
- Scope management (coroutine IDs, Close events) is layered via Effection's
  existing `Context` and `Api.around()` systems.

---

## 2. The protocol (fixed contract)

The spec defines exactly two event types in an append-only stream:

```typescript
type DurableEvent = Yield | Close;

interface Yield {
  type: "yield";
  coroutineId: CoroutineId;      // e.g. "root.0.1"
  description: EffectDescription; // { type, name }
  result: Result;
}

interface Close {
  type: "close";
  coroutineId: CoroutineId;
  result: Result;                 // ok | err | cancelled
}

interface EffectDescription {
  type: string;   // "call", "sleep", "action", etc.
  name: string;   // "fetchOrder", "sleep", etc.
}

type Result =
  | { status: "ok"; value?: Json }
  | { status: "err"; error: SerializedError }
  | { status: "cancelled" };
```

**Yield** is written after an effect resolves. It records what was requested
(description) and what happened (result). During replay the description is
validated and the result is fed directly to the generator.

**Close** is written when a coroutine terminates (completed, failed, or
cancelled). Close events are load-bearing for partial replay — they tell the
runtime which scopes completed before a crash and which need re-execution.

### 2.1 Core invariants from the spec

| # | Name | Rule |
|---|------|------|
| 1 | Deterministic Identity | Coroutine IDs are stable across runs for same code + same resolutions |
| 2 | Transparency | Generator cannot detect replay vs. live |
| 3 | Fork-Join Across Crash | Join result is independent of which children were replayed |
| 4 | **Persist-Before-Resume** | Durable write MUST complete before `iterator.next()` is called |
| 5 | Divergence Detection | Every replayed effect is validated against journal |
| 6 | Lifetime Containment | child ⊆ parent |
| 7 | Single Parent | Tree, not DAG |
| 8 | Implicit Join | Scope waits for all children |
| 9 | Cancellation Replay Fidelity | Cleanup path matches recorded path |
| 10 | Causal Ordering | Stream order respects causality |
| 11–13 | Stream consistency | Append-only, prefix-closed, monotonic indexing |

---

## 3. How Effection's runtime works (relevant internals)

### 3.1 The reducer loop

`lib/reducer.ts` — the synchronous, re-entrant loop that drives all execution:

```typescript
class Reducer {
  reducing = false;
  readonly queue = new InstructionQueue();  // priority queue, deeper scopes first

  reduce = (instruction: Instruction) => {
    this.queue.enqueue(instruction);
    if (this.reducing) return;              // re-entrancy guard
    try {
      this.reducing = true;
      let item = this.queue.dequeue();
      while (item) {
        let [, routine, result, _, method] = item;
        let iterator = routine.data.iterator;
        // Call iterator.next(value), iterator.throw(error), or iterator.return(value)
        let next = iterator[method](result.value);
        if (!next.done) {
          let action = next.value;          // the yielded Effect<T>
          routine.data.exit = action.enter(routine.next, routine);
        }
        item = this.queue.dequeue();
      }
    } finally {
      this.reducing = false;
    }
  };
}
```

Key properties:
- **Synchronous.** No `await` anywhere. Async effects resolve by calling
  `routine.next(result)` from a callback, which re-enters `reduce()`.
- **Re-entrant safe.** If `reduce()` is already running, the instruction is
  enqueued and the outer loop picks it up.
- **Priority ordered.** Deeper scopes run first (FIFO within a tier). This
  is structural, not timing-dependent — it's deterministic.

### 3.2 The Effect interface

```typescript
interface Effect<T> {
  description: string;
  enter(
    resolve: Resolve<Result<T>>,
    routine: Coroutine,
  ): (resolve: Resolve<Result<void>>) => void;
}
```

Every yielded value from a generator is an `Effect`. The reducer calls
`enter()`, which:
1. Starts the actual work (sets timers, makes requests, etc.)
2. Calls `resolve(result)` when done — this enqueues the next instruction
3. Returns a teardown function called during cancellation/scope exit

### 3.3 How existing effects use enter()

**Synchronous / infrastructure effects** call `resolve()` immediately inside
`enter()`:

```typescript
// useScope() — lib/context.ts
function UseScope<T>(fn: (scope: Scope) => T, description: string): Effect<T> {
  return {
    description,
    enter: (resolve, { scope }) => {
      resolve(Ok(fn(scope)));           // resolve immediately
      return (resolve) => resolve(Ok());
    },
  };
}
```

**Asynchronous / user-facing effects** call `resolve()` later from a callback:

```typescript
// sleep() — lib/sleep.ts, via action()
function sleep(duration: number): Operation<void> {
  return action((resolve) => {
    let timeoutId = setTimeout(resolve, duration);  // resolve later
    return () => clearTimeout(timeoutId);
  });
}
```

### 3.4 The type system today

```typescript
interface Operation<T> {
  [Symbol.iterator](): Iterator<Effect<unknown>, T, unknown>;
}
```

An `Operation` is anything whose iterator yields `Effect` values. Generator
functions (`function*`) that only do `yield*` to other operations satisfy this.
The `yield*` delegation means the inner generator's yielded `Effect` values
pass through to the outer generator — the reducer sees a flat sequence of
effects regardless of call depth.

### 3.5 Scope, Context, and Api systems

- **Scope** (`lib/scope-internal.ts`): tree-structured, owns lifetime and
  context. Created via `createScopeInternal(parent)`. Tracks children via
  `Children` context. Destruction runs `ensure()` callbacks in reverse order.

- **Context** (`lib/context.ts`): scope-local key-value storage. Children
  inherit from parents. `createContext<T>(name, default?)` creates a typed
  context. Accessed via `scope.get()`, `scope.set()`, `scope.expect()`.

- **Api** (`lib/api.ts`): middleware system for scope-bound operations.
  `scope.around(api, middlewares, { at: "min" | "max" })` installs
  middleware at different priority layers. Used for `Scope.create`,
  `Scope.destroy`, and `Main.main`.

### 3.6 Task lifecycle

`createTask()` in `lib/task.ts`:

1. Creates a child scope via `createScopeInternal(owner)`
2. Creates a `Future<T>` for the task's result
3. Creates a `Delimiter` (error/cancellation boundary)
4. Registers an `ensure()` on the scope that:
   - Closes the delimiter
   - Resolves or rejects the future
   - Propagates errors to the parent boundary
5. Creates a coroutine and returns a `start()` function

The ensure callback is the natural hook for emitting Close events.

---

## 4. Charles's type-constraint architecture

### 4.1 The problem with runtime classification

The spec's §12 distinguishes "user-facing" from "infrastructure" effects. My
initial analysis proposed classifying them at runtime — e.g., effects that
call `resolve()` synchronously inside `enter()` are infrastructure.

Charles correctly identified this as fragile. The failure mode is silent: a
misclassified effect gets skipped during replay with no error. Worse, the
classification is implicit — there's no way to verify it statically, and new
effects could be misclassified without anyone noticing.

### 4.2 The solution: constrain the yield type

Instead of classifying effects after they're yielded, constrain what can be
yielded at the type level:

```typescript
interface DurableEffect<T> extends Effect<T> {
  effectDescription: EffectDescription;  // { type, name }
}

type Workflow<T> = Iterable<DurableEffect<unknown>, T, unknown>;
```

Key relationships:
- `DurableEffect extends Effect` → every `DurableEffect` is an `Effect`
- `Workflow` yields `DurableEffect` → every `Workflow` is an `Operation`
- `Operation` yields `Effect` → `Operation` is NOT a `Workflow`
- The reducer processes both identically (it just calls `enter()`)

### 4.3 What this buys us

**Compile-time safety.** If you declare `function*(): Workflow<void>`, the
TypeScript compiler rejects `yield*` to any `Operation` that isn't also a
`Workflow`. Using `useAbortSignal()`, `sleep()`, or `each(stream)` inside a
workflow is a type error.

```typescript
function* badWorkflow(): Workflow<void> {
  yield* useAbortSignal();  // TypeError!
  yield* sleep(1000);       // TypeError!
}
```

**No runtime classification.** The reducer doesn't need to know whether an
effect is durable. The `DurableEffect.enter()` method handles its own replay
and persistence logic. The reducer just calls `enter()` as always.

**No reducer changes.** The existing `Reducer.reduce()` loop is untouched.
It processes `DurableEffect` values the same way it processes any `Effect` —
by calling `enter()`, getting a teardown function, and waiting for `resolve()`.

**Clear boundary.** Workflow authors know exactly what they can and can't use.
If it compiles, it's durable. There's no hidden gotcha where "this operation
looks safe but actually breaks replay."

### 4.4 The freeing quality

Charles's observation: workflow authors don't need to understand whether they're
running durably. The type system enforces it. You write workflows using
workflow-enabled effects, and the compiler guarantees the result is safe for
durable execution. There's no "am I in replay mode?" question — the
`DurableEffect.enter()` handles that transparently.

---

## 5. DurableEffect implementation

### 5.1 The enter() method does everything

The central insight: each `DurableEffect` handles its own replay/live dispatch
inside `enter()`. It reads the durable execution context from the scope,
checks the replay index, and either feeds the stored result or executes live
with persistence.

```typescript
interface DurableContext {
  replayIndex: ReplayIndex;
  stream: DurableStream;
  coroutineId: CoroutineId;
}

const DurableCtx = createContext<DurableContext>("@effection/durable");

function createDurableEffect<T>(
  desc: EffectDescription,
  execute: (
    resolve: (result: Result<T>) => void,
    reject: (error: Error) => void,
  ) => () => void,
): DurableEffect<T> {
  return {
    description: `${desc.type}(${desc.name})`,
    effectDescription: desc,
    enter(resolve, routine) {
      let scope = routine.scope;
      let ctx = scope.expect(DurableCtx);
      let entry = ctx.replayIndex.peekYield(ctx.coroutineId);

      if (entry) {
        // ── REPLAY PATH ──
        // Validate description match (§6)
        if (entry.description.type !== desc.type ||
            entry.description.name !== desc.name) {
          let cursor = ctx.replayIndex.getCursor(ctx.coroutineId);
          resolve(Err(new DivergenceError(
            ctx.coroutineId, cursor, entry.description, desc
          )));
          return (exit) => exit(Ok());
        }

        ctx.replayIndex.consumeYield(ctx.coroutineId);

        // Feed stored result synchronously — no I/O, no side effects
        resolve(entry.result);
        return (exit) => exit(Ok());

      } else {
        // ── LIVE PATH ──
        let teardown = execute(
          (result) => {
            // PERSIST BEFORE RESUME (§5)
            let event: Yield = {
              type: "yield",
              coroutineId: ctx.coroutineId,
              description: desc,
              result,
            };
            ctx.stream.append(event).then(() => {
              resolve(result);  // resume generator only after durable write
            });
          },
          (error) => {
            let result: Result<T> = { status: "err", error: serialize(error) };
            let event: Yield = {
              type: "yield",
              coroutineId: ctx.coroutineId,
              description: desc,
              result,
            };
            ctx.stream.append(event).then(() => {
              resolve(result);
            });
          },
        );

        return (exit) => {
          try { teardown(); exit(Ok()); }
          catch (e) { exit(Err(e as Error)); }
        };
      }
    },
  };
}
```

### 5.2 How this satisfies spec invariants

**Persist-before-resume (§5, hard invariant).** During live execution,
`resolve()` is called inside the `.then()` callback of the stream append.
The generator does not advance until the durable write completes. This is
the spec's "Strategy B: buffered write with deferred resume."

**Transparency (§4.3).** During replay, `resolve()` is called synchronously
with the stored result. The reducer processes it in the same tick. The
generator receives the same value via the same `iterator.next()` call
path. It cannot distinguish replay from live.

**Divergence detection (§6).** The description comparison happens inside
`enter()` before the stored result is fed. A mismatch raises
`DivergenceError` through the normal error propagation path (via
`resolve(Err(...))`).

**No reducer changes.** The reducer calls `enter()`, gets a teardown,
waits for `resolve()`. Whether `resolve()` fires synchronously (replay)
or asynchronously (live with persistence) is invisible to the reducer.

### 5.3 Replay path performance

During replay, `enter()` is fully synchronous:
1. Read replay index — in-memory map lookup
2. Compare descriptions — two string comparisons
3. Call `resolve()` — enqueues the next reducer instruction
4. Return teardown (no-op)

The reducer's re-entrancy guard means the enqueued instruction is processed
on the next iteration of the existing `while` loop. There is zero async
overhead during replay. A fully-replayed workflow completes in a single
synchronous reduce cycle.

---

## 6. Workflow-enabled effects

These are the durable equivalents of Effection's built-in operations. They
return `Workflow<T>` instead of `Operation<T>`.

### 6.1 durableSleep

```typescript
function durableSleep(ms: number): Workflow<void> {
  return {
    *[Symbol.iterator]() {
      return (yield createDurableEffect(
        { type: "sleep", name: "sleep" },
        (resolve) => {
          let id = setTimeout(() => resolve({ status: "ok" }), ms);
          return () => clearTimeout(id);
        },
      )) as void;
    },
  };
}
```

### 6.2 durableCall

```typescript
function durableCall<T>(
  name: string,
  fn: () => Promise<T>,
): Workflow<T> {
  return {
    *[Symbol.iterator]() {
      return (yield createDurableEffect(
        { type: "call", name },
        (resolve, reject) => {
          fn().then(
            (value) => resolve({ status: "ok", value: serialize(value) }),
            (error) => reject(error as Error),
          );
          return () => {};
        },
      )) as T;
    },
  };
}
```

### 6.3 durableAction

```typescript
function durableAction<T>(
  name: string,
  executor: (
    resolve: (value: T) => void,
    reject: (error: Error) => void,
  ) => () => void,
): Workflow<T> {
  return {
    *[Symbol.iterator]() {
      return (yield createDurableEffect(
        { type: "action", name },
        (resolve, reject) => {
          return executor(
            (value) => resolve({ status: "ok", value: serialize(value) }),
            reject,
          );
        },
      )) as T;
    },
  };
}
```

### 6.4 versionGate (§9)

```typescript
function versionCheck(
  name: string,
  opts: { minVersion: number; maxVersion: number },
): Workflow<number> {
  return {
    *[Symbol.iterator]() {
      return (yield createDurableEffect(
        { type: "version_gate", name },
        (resolve) => {
          resolve({ status: "ok", value: opts.maxVersion });
          return () => {};
        },
      )) as number;
    },
  };
}
```

### 6.5 Workflow composition

Workflows compose exactly like operations — via `yield*`:

```typescript
function* orderWorkflow(orderId: string): Workflow<void> {
  let version = yield* versionCheck("add-fraud-check", { minVersion: 0, maxVersion: 1 });

  if (version >= 1) {
    yield* durableCall("fraudCheck", () => fraudCheck(orderId));
  }

  let order = yield* durableCall("fetchOrder", () => fetchOrder(orderId));
  yield* durableCall("chargeCard", () => chargeCard(order.payment));
}
```

---

## 7. Coroutine identity

### 7.1 Per-parent counter scheme (§3)

Coroutine IDs are dot-delimited paths assigned deterministically:

```
root                    → "root"
  first child of root   → "root.0"
  second child of root  → "root.1"
    first child of .1   → "root.1.0"
```

### 7.2 Implementation via DurableContext

The coroutine ID is part of the `DurableContext` stored on each scope. When
a durable scope spawns a child, it increments a per-scope counter and
constructs the child's ID:

```typescript
interface DurableContext {
  replayIndex: ReplayIndex;
  stream: DurableStream;
  coroutineId: CoroutineId;
  childCounter: number;
}
```

When `durableSpawn()` creates a child scope:

```typescript
let parentCtx = scope.expect(DurableCtx);
let childId = `${parentCtx.coroutineId}.${parentCtx.childCounter++}`;
childScope.set(DurableCtx, {
  replayIndex: parentCtx.replayIndex,  // shared
  stream: parentCtx.stream,            // shared
  coroutineId: childId,
  childCounter: 0,
});
```

### 7.3 Why determinism holds

The spec identifies four properties (§3.2):

1. **Synchronous reducer.** ✅ Effection's `Reducer.reduce()` is synchronous
   with a re-entrancy guard. No async gaps.
2. **Deterministic generators.** ✅ Same inputs → same yields. `for...of` in
   `all()` processes operands in array order.
3. **Replay preserves ordering.** ✅ Replayed effects resolve synchronously
   inside `DurableEffect.enter()`, so spawns happen in the same order.
4. **Priority ordering is structural.** ✅ `PriorityQueue` orders by scope
   depth (the `Priority` context), FIFO within a tier.

### 7.4 Teardown spawns (§3.4)

When `ensure()` or `finally` blocks spawn children during scope destruction,
the `childCounter` continues incrementing on the same `DurableContext`. This
is safe because teardown order is deterministic (reverse creation order via
the scope's destructor set in `buildScopeInternal`).

---

## 8. Scope management: spawn, join, Close events

### 8.1 durableSpawn

`durableSpawn` is the workflow-compatible equivalent of `spawn()`. It creates
a child scope with its own `DurableContext`, runs a child workflow, and
returns a handle.

The question is: does `durableSpawn` itself yield a `DurableEffect`?

The spec's examples show no explicit "spawn" event in the journal. Coroutine
IDs appear in `Yield` and `Close` events, but the spawn point is not
journaled. Spawns are reconstructed from the deterministic structure of the
code. This makes sense — since the same code with the same resolution
sequence always spawns the same children in the same order (§3.2), recording
spawn events would be redundant.

This means `durableSpawn` does its scope management internally using regular
Effection operations (scope creation, context setup) but its iterator only
yields `DurableEffect` values to the parent workflow. The child workflow
is itself a `Workflow<T>`, so it only yields `DurableEffect` values too.

```typescript
function durableSpawn<T>(op: () => Workflow<T>): Workflow<Task<T>> {
  return {
    *[Symbol.iterator]() {
      let scope = yield* useScope();  // infrastructure — but this is inside
                                       // the spawn implementation, not in
                                       // user workflow code
      // ... create child scope, assign DurableContext, start child ...
      return task;
    },
  };
}
```

**Open question:** How does `durableSpawn`'s iterator avoid yielding
non-durable effects? If `useScope()` is used internally, its `Effect` passes
through `yield*`. One approach: `durableSpawn` doesn't use `yield*` for
infrastructure — it accesses scope directly from the routine in `enter()`.
Another: it's implemented as a `DurableEffect` whose `enter()` method does
the scope setup. This needs resolution.

### 8.2 Close event emission

Close events must be written when a coroutine terminates. The natural hook
is in the scope's `ensure()` callback, which already runs during task
finalization in `createTask()` (`lib/task.ts`):

```typescript
// Inside the durable task setup:
scope.ensure(function* () {
  let ctx = scope.expect(DurableCtx);
  let { outcome } = delimiter;

  let closeEvent: Close;
  if (outcome.exists) {
    closeEvent = {
      type: "close",
      coroutineId: ctx.coroutineId,
      result: outcome.value.ok
        ? { status: "ok", value: serialize(outcome.value.value) }
        : { status: "err", error: serializeError(outcome.value.error) },
    };
  } else {
    closeEvent = {
      type: "close",
      coroutineId: ctx.coroutineId,
      result: { status: "cancelled" },
    };
  }

  yield* durableWrite(ctx.stream, closeEvent);
});
```

### 8.3 Close events during replay

When the replay index has a `Close` event for a coroutine, there are two
cases:

1. **Fully replayed (all yields consumed + Close exists).** The child's
   result can be read directly from the Close event. The runtime may
   optionally skip re-instantiating the generator entirely if the parent
   only needs the return value.

2. **Partially replayed (some yields consumed, no Close).** The child
   replays its recorded yields, then transitions to live execution for
   remaining effects. This is the per-coroutine replay-to-live transition
   (§4.3).

### 8.4 Cancellation during replay

When the replay index has `Close(cancelled)` for a coroutine, the runtime
must call `iterator.return()` on that coroutine's generator after its last
recorded yield has been replayed. If the generator yields effects during
cleanup (`finally` blocks), those effects are replayed from the journal too.

The trigger for cancellation during replay is the `Close(cancelled)` event
in the journal, not a live cancellation signal. The cancellation point is
determined by the position of the Close event relative to the coroutine's
last Yield event.

---

## 9. Replay index

### 9.1 Structure

Built from the stream on startup. Provides per-coroutine cursored access:

```typescript
class ReplayIndex {
  private yields = new Map<CoroutineId, Array<{
    description: EffectDescription;
    result: Result;
  }>>();
  private cursors = new Map<CoroutineId, number>();
  private closes = new Map<CoroutineId, Close>();

  constructor(events: DurableEvent[]) {
    for (const event of events) {
      if (event.type === "yield") {
        const list = this.yields.get(event.coroutineId) ?? [];
        list.push({ description: event.description, result: event.result });
        this.yields.set(event.coroutineId, list);
      }
      if (event.type === "close") {
        this.closes.set(event.coroutineId, event);
      }
    }
  }

  peekYield(id: CoroutineId) {
    const list = this.yields.get(id);
    const cursor = this.cursors.get(id) ?? 0;
    return list?.[cursor];
  }

  consumeYield(id: CoroutineId) {
    const cursor = this.cursors.get(id) ?? 0;
    this.cursors.set(id, cursor + 1);
  }

  getCursor(id: CoroutineId): number {
    return this.cursors.get(id) ?? 0;
  }

  hasClose(id: CoroutineId): boolean {
    return this.closes.has(id);
  }

  getClose(id: CoroutineId): Close | undefined {
    return this.closes.get(id);
  }

  isFullyReplayed(id: CoroutineId): boolean {
    return this.peekYield(id) === undefined && this.hasClose(id);
  }
}
```

### 9.2 Stored as Effection Context

The replay index is part of `DurableContext`, set on the root scope when
a durable execution begins. All child scopes inherit it (Effection contexts
use prototypal inheritance). Each child scope has its own `coroutineId` and
`childCounter` but shares the same `replayIndex` and `stream`.

---

## 10. Entry point: durableRun

The entry point creates a scope, builds the replay index, sets up the
durable context, and runs the workflow:

```typescript
interface DurableRunOptions {
  stream: DurableStream;
}

async function durableRun<T>(
  workflow: () => Workflow<T>,
  options: DurableRunOptions,
): Promise<T> {
  let events = await options.stream.readAll();
  let replayIndex = new ReplayIndex(events);

  let [scope, destroy] = createScope();

  scope.set(DurableCtx, {
    replayIndex,
    stream: options.stream,
    coroutineId: "root",
    childCounter: 0,
  });

  // Install Close event emission on scope destruction
  // (via Api.around or ensure)

  try {
    let task = scope.run(workflow as () => Operation<T>);
    return await task;
  } finally {
    await destroy();
  }
}
```

Because `Workflow<T>` satisfies `Operation<T>`, it can be passed to
`scope.run()` without casting issues at the runtime level. The cast
`as () => Operation<T>` is needed only because TypeScript's inference
for generator return types needs guidance.

---

## 11. What can and cannot be used in workflows

### 11.1 Allowed (compiles)

| Workflow effect | Equivalent Effection operation |
|----------------|-------------------------------|
| `durableSleep(ms)` | `sleep(ms)` |
| `durableCall(name, fn)` | `call(fn)` |
| `durableAction(name, executor)` | `action(executor)` |
| `versionCheck(name, opts)` | (new, no equivalent) |
| `durableSpawn(workflow)` | `spawn(operation)` |
| `durableAll([...workflows])` | `all([...operations])` |
| `durableRace([...workflows])` | `race([...operations])` |

### 11.2 Rejected (type error)

| Operation | Why it's rejected |
|-----------|-------------------|
| `useAbortSignal()` | Returns a scope-bound resource that doesn't survive replay |
| `useScope()` | Infrastructure effect — not durable |
| `each(stream)` | Streams are stateful subscriptions, not serializable |
| `resource(fn)` | Resources hold live state (connections, handles) |
| `ensure(fn)` | Registers cleanup that may not be serializable |
| `on(target, name)` | EventTarget-based, not serializable |
| `sleep(ms)` | Effection's `sleep` — use `durableSleep` instead |
| `call(fn)` | Effection's `call` — use `durableCall` instead |

### 11.3 The boundary is intentional

This is the "freeing" quality Charles described. You don't have to think about
whether something is safe for durable execution — the compiler tells you. The
set of workflow-enabled effects is small and explicit. Each one has a clear
contract: it carries a structured description, it handles its own replay, and
its result is JSON-serializable.

---

## 12. Open questions

### 12.1 durableSpawn and infrastructure effects

`durableSpawn` needs to create scopes and set contexts, which involves
infrastructure effects like `useScope()`. If these `yield*` through the
parent generator, they'd violate the `Workflow` type constraint.

**Option A:** Implement `durableSpawn` as a single `DurableEffect` whose
`enter()` method does all scope setup internally (accessing the routine
and scope directly, not via yielded effects). The spawn itself appears as
one effect in the workflow's iterator.

**Option B:** Allow `durableSpawn` to have an internal `Operation`
implementation that's wrapped in a `Workflow`-compatible shell. The wrapper
ensures only `DurableEffect` values are yielded to the parent. This might
require a helper type or a `scoped()` variant.

**Option C:** Make scope creation and context access available as direct
method calls on the routine/scope (not as yielded effects) so no effects
need to be yielded at all. `durableSpawn` would call `routine.scope.run()`
and `scope.set()` directly inside `enter()`.

Option A or C seem cleanest. The spawn doesn't need to appear in the journal
(§8.1), so there's no reason for it to be a multi-step sequence of yielded
effects.

### 12.2 Serialization boundary

The spec requires `Result.value` to be `Json`. This means workflow effect
results must be JSON-serializable. What about:

- **Dates, BigInts, typed arrays?** Need a serialization strategy (e.g.,
  tagged encoding).
- **Complex domain objects?** Workflow authors must ensure return values from
  `durableCall` are serializable, or provide custom serializers.
- **Error serialization?** The spec defines `SerializedError { message, name?,
  stack? }`. Custom error properties are lost unless explicitly serialized.

### 12.3 DurableStream interface

The spec is intentionally abstract about the stream. The `DurableStream`
interface needs:

```typescript
interface DurableStream {
  readAll(): Promise<DurableEvent[]>;
  append(event: DurableEvent): Promise<void>;
}
```

Implementations could be: in-memory array (for testing), file-based WAL,
database-backed (PGLite?), remote log service.

### 12.4 Batch persistence (Strategy C)

During `all()` with multiple children, several effects may resolve in the
same reducer tick (especially during replay-to-live transition). The spec's
Strategy C suggests batching writes. This could be implemented by having
`DurableEffect.enter()` enqueue writes to a buffer on the `DurableContext`,
with the buffer flushed at the end of each reduce cycle.

### 12.5 Terminal divergence detection

The spec (§6.3) defines two additional divergence cases:

1. **Generator finishes early** — returns while replay index has unconsumed
   entries. This needs detection at the point where the generator's iterator
   returns `{ done: true }`.

2. **Journal exhausted with Close but generator continues** — the replay
   index has a Close event but the generator hasn't finished. This needs
   detection when `peekYield()` returns undefined and `hasClose()` returns
   true, but the generator yields another effect.

Both could be checked in `DurableEffect.enter()` (for case 2) and in the
scope destruction path (for case 1).

### 12.6 Durable `each()` for long-running consumption

Charles's example showed the powerful implication: in a durable workflow,
each iteration of a loop can run on a different VM. This requires a durable
iteration primitive — something like a durable subscription that checkpoints
its position:

```typescript
function* processQueue(): Workflow<void> {
  for (let message of yield* durableEach(messageQueue)) {
    yield* durableCall("process", () => process(message));
    yield* durableEach.next();
    // crash here → resume picks up at next message
  }
}
```

This is a future concern but worth noting as a design target.

---

## 13. Summary of what doesn't change

| Component | Changes? | Notes |
|-----------|----------|-------|
| `Reducer` | No | Unchanged. Calls `enter()` on effects as always. |
| `Effect<T>` | No | Unchanged. `DurableEffect` extends it. |
| `Operation<T>` | No | Unchanged. `Workflow` is a subtype. |
| `Scope` / `ScopeInternal` | No | Unchanged. Durable context stored via existing Context system. |
| `Context` | No | Unchanged. Used to store `DurableContext`. |
| `Api.around()` | No | Unchanged. May be used to layer Close event emission. |
| `PriorityQueue` | No | Unchanged. Deterministic ordering enables replay. |
| `createTask()` | No | Unchanged. Durable variant wraps it with context setup. |
| `spawn()`, `all()`, `race()` | No | Unchanged. Durable variants wrap them. |

---

## 14. Proposed next steps

1. **Validate type system.** Write the `DurableEffect`, `Workflow<T>` types
   and confirm TypeScript correctly rejects `Operation` usage inside
   `Workflow` generators.

2. **Implement `createDurableEffect`.** The core factory function with
   replay/live dispatch inside `enter()`.

3. **Implement `ReplayIndex`.** Standalone class, spec-compliant.

4. **Implement basic workflow effects.** `durableSleep`, `durableCall`,
   `versionCheck`.

5. **Implement `durableRun`.** Entry point with in-memory stream for testing.

6. **Run spec Tier 1 tests.** Golden run, full replay, crash-at-N, persist-
   before-resume.

7. **Resolve `durableSpawn`.** Settle on Option A/C from §12.1.

8. **Implement `durableSpawn`, `durableAll`, `durableRace`.** Structured
   concurrency combinators.

9. **Run spec Tier 2–4 tests.** Divergence detection, structured concurrency,
   deterministic identity.

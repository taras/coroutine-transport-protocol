# References

These references are cited throughout `pseudo-tests.ts` as `[1]`, `[2]`, etc.

## [1] Effection Core Types

Source: `~/Repositories/frontside/effection/lib/types.ts`

- `Effect<T>` is the fundamental unit of work in Effection:
  `{ description: string, enter(resolve, routine) → cleanup }`
- `Operation<T>` is a generator that yields Effects:
  `{ [Symbol.iterator](): Iterator<Effect<unknown>, T, unknown> }`
- `Coroutine` holds the scope reference and iterator; it has
  `next(result)` and `return(result)` methods that drive the generator.
- Effects are the ONLY things that cross the yield boundary.
  Everything else (sleep, spawn, resource) is built on top.

**Relevance:** Each yield produces an Effect. In the durable version,
each Effect resolution is written to the stream. The stream is a
log of Effect descriptions and their results.

## [2] Effection Reducer

Source: `~/Repositories/frontside/effection/lib/reducer.ts`

- A priority queue that processes instructions synchronously.
- Drives generators forward: calls `effect.enter()`, waits for
  `resolve()`, then calls `iterator.next(result)`.
- The instruction queue enforces structured concurrency ordering —
  scope creation, destruction, and cleanup all flow through it.

**Relevance:** We keep the Reducer. On replay, we add a path that
checks "do I have a stored result for this effect?" — if yes,
feed it through the queue without calling `enter()`. The queue
bookkeeping (scope ordering, cleanup registration) still runs.

## [3] Effection Action

Source: `~/Repositories/frontside/effection/lib/action.ts`

- Foundation for most effects. Wraps callback-style APIs with
  cleanup: `action((resolve, reject) => { ... return cleanup })`
- `sleep`, `suspend`, `lift`, `until` all reduce to action.

**Relevance:** Most durable events will be "action resolved with
value X". The description string (e.g. `"sleep(100)"`) identifies
the effect for debugging and divergence detection.

## [4] Effection Spawn

Source: `~/Repositories/frontside/effection/lib/spawn.ts`

- Gets current scope via `useScope()`, calls `scope.run(op)`.
- Creates a child task owned by the current scope.
- Child cannot outlive parent (structured concurrency).

**Relevance:** Spawn creates scope hierarchy. The durable stream
must capture parent/child scope relationships via `scope:created`
events with `parentScopeId`. On replay, child scopes are
reconstructed before the parent resumes.

## [5] Effection Resource

Source: `~/Repositories/frontside/effection/lib/resource.ts`

- Pattern: setup → `provide(value)` → cleanup (in `finally`).
- The resource's scope stays alive until the enclosing scope exits.
- Cleanup is guaranteed to run on scope destruction.

**Relevance:** On replay, skip the setup, restore the provided value
from the stream, but still register the cleanup handler with the
Reducer so teardown works if halted after resume.

## [6] Effection Each

Source: `~/Repositories/frontside/effection/lib/each.ts`

- Iterates a stream: `for (let x of yield* each(stream)) { ... yield* each.next() }`
- Internally uses spawn and context.
- `each.next()` is required per iteration to advance.

**Relevance:** Each iteration is a yield point, so each iteration
gets checkpointed in the durable stream. On resume, iteration
picks up from the last processed item.

## [7] Effection Inspector — Scope Protocol

Source: `~/Repositories/frontside/inspector/scope/protocol.ts`

- Defines `ScopeEvent` discriminated union:
  `tree`, `created`, `destroying`, `destroyed`, `set`, `delete`
- Each event carries a scope id and relevant data.
- Events are streamed over SSE.

**Relevance:** This IS the serialization format for scope lifecycle.
We extend it with effect events (`yielded`, `resolved`, `errored`)
to capture the full execution history.

## [8] Effection Inspector — Scope Implementation

Source: `~/Repositories/frontside/inspector/scope/implementation.ts`

- Hooks into `api.Scope` via `root.around(api.Scope, { create, destroy, set })`
- Emits `ScopeEvent`s whenever scopes are created/destroyed/modified.
- `readTree(root)` walks the scope tree to produce an initial snapshot.
- Recordings: SSE event arrays saved as JSON and replayed later.

**Relevance:** Proves the middleware approach works for observing
scope lifecycle. Durable Effection does the same thing but writes
to a Durable Stream instead of SSE. Recordings prove the event
stream is sufficient to reconstruct the scope tree.

## [9] Effection Inspector — NodeMap

Source: `~/Repositories/frontside/inspector/lib/update-node-map.ts`

- `reduce()` over `ScopeEvent` stream produces a materialized `NodeMap`.
- `NodeMap = Record<string, { id, parentId, data }>`
- The stream is the source of truth; state is derived by reducing.

**Relevance:** Same approach for Durable Effection. The stream IS
the execution history. Workflow state at any point is derived
by reducing events from the beginning to that offset.

## [10] Effection v4.1 Api System

Source: `~/Repositories/frontside/effection` (branch: `v4-1-alpha`)
Files: `lib/api.ts`, `lib/api-internal.ts`, `lib/scope-internal.ts`

- `createApi(name, core)` creates interceptable API with middleware.
- `api.Scope` exposes `create`, `destroy`, `set`, `delete`.
- `scope.around(api, middleware, { at: "min" | "max" })` installs
  middleware at the scope level.
- Middleware stack with min/max ordering for layering.

**Relevance:** This is the extension point. Inspector uses it for
observation; Durable Effection uses it for persistence. The
min/max layering means durability middleware can coexist with
Inspector middleware without conflict. However, Api middleware
only covers scope lifecycle — not individual effect `enter()`/`resolve()`.
We need an additional interception point for effect resolution.

## [11] Durable Streams Protocol

Source: `~/Repositories/durable-streams/durable-streams/PROTOCOL.md`

- Append-only, offset-based, persistent byte streams.
- Offsets are opaque, lexicographically sortable tokens.
- Idempotent producers (`Producer-Id`, `Producer-Epoch`, `Producer-Seq`)
  for exactly-once writes — prevents duplicate events on crash retry.
- Stream closure (`Stream-Closed: true`) signals EOF.
- Live modes: catch-up (historical), long-poll, SSE (real-time).

**Relevance:** Each `DurableEvent` we write gets an offset. The offset
after the last written event IS our checkpoint — no separate
checkpoint mechanism needed. Idempotent producers handle crash
safety. Stream closure signals workflow completion/halt. Live
modes let observers watch workflow progress in real-time.

## [12] Durable Sessions Pattern

Source: https://electric-sql.com/blog/2026/01/12/durable-sessions-for-collaborative-ai

- Multiplexes structured data over a single Durable Stream.
- Layered protocols: Durable Streams → Durable State → Specific protocols.
- Multiple users/agents subscribe to the same stream.
- Late joiners catch up from offset `-1` (beginning).
- TanStack DB reduces stream into reactive client-side state.

**Relevance:** Our `DurableEvent` types are a "specific protocol"
layered on Durable Streams. A durable workflow is inherently
observable — any subscriber can watch scope creation, effect
resolution, and workflow completion. This enables dashboards,
audit logs, and collaborative interaction with running workflows.

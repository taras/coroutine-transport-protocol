# Decision Log

Every architectural, technical, and implementation decision made during the
build of the durable execution integration is recorded here. Decisions are
append-only — superseded decisions are marked `[SUPERSEDED by DEC-NNN]` but
never deleted.

Updated before completion of every phase and committed at the end of each phase.

---

## DEC-001: Use Deno as project runtime

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** Need a runtime for the project. Effection 4.x uses Deno as its
  primary development tool and publishes to JSR.
- **Options considered:**
  1. Node.js with npm/TypeScript toolchain
  2. Deno with JSR imports
- **Decision:** Deno as the project runtime (deno.json, deno test, JSR imports)
- **Rationale:** User preference. Effection itself uses Deno for development.
  JSR imports are first-class. `deno test` eliminates the need for a separate
  test runner.
- **Consequences:** npm packages (durable-streams) are imported via `npm:`
  specifiers. Native addons (lmdb in @durable-streams/server) may need
  `nodeModulesDir: "auto"` if build scripts are required.

## DEC-002: Target effection 4.1.0-alpha.5 with /experimental endpoint

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** Need the latest Effection with the Api middleware system for
  intercepting scope lifecycle events.
- **Options considered:**
  1. Effection 4.0.2 (stable) — no `/experimental` endpoint
  2. Effection 4.1.0-alpha.5 — has `createApi`, `api.Scope`, `api.Main`
- **Decision:** Use `@effection/effection@4.1.0-alpha.5`
- **Rationale:** The `/experimental` endpoint exposes `api.Scope` with
  `create`, `destroy`, `set`, `delete` operations and `around()` middleware.
  This is the extension point needed for intercepting scope destruction to
  emit Close events (future phases). The alpha is published to JSR and works
  with Deno.
- **Consequences:** API surface may change before 4.1 stable. We depend on
  the experimental endpoint which is explicitly unstable. We should pin the
  exact version and be prepared to adapt.

## DEC-003: Single package structure

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** Could structure as a monorepo with separate packages
  (core protocol, durable-streams backend, test utilities) or a single package.
- **Options considered:**
  1. Monorepo with separate packages from day one
  2. Single package, split later when boundaries stabilize
- **Decision:** Single package
- **Rationale:** The boundaries between core protocol, effects, and backend
  adapter are not yet proven. Premature separation adds overhead without
  benefit. Split when the interfaces are stable and there's a concrete need
  (e.g., supporting a second backend).
- **Consequences:** All code lives under `lib/`. The module entry point is
  `lib/mod.ts`. Re-exports control the public API surface.

## DEC-004: Use @std/assert for test assertions

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** Need an assertion library for `deno test`.
- **Options considered:**
  1. `https://deno.land/std` URL imports (legacy style)
  2. `jsr:@std/assert` (modern JSR-style)
- **Decision:** `jsr:@std/assert@1` via import map
- **Rationale:** JSR is the standard for Deno dependencies. Avoids uncached
  URL resolution issues.
- **Consequences:** Added to deno.json imports.

## DEC-005: Sequential workflows only in initial scope

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** The protocol spec covers sequential execution, fork/join,
  races, cancellation, and version gates. Implementing everything at once
  is risky.
- **Options considered:**
  1. Full set (call, sleep, action, spawn, all, race, versionCheck) from start
  2. Minimal: core + call + sleep, add spawn/all/race in a second phase
- **Decision:** Minimal initial scope: ReplayIndex, DurableEffect,
  durableCall, durableSleep, versionCheck, durableRun. No spawn/all/race.
- **Rationale:** Enough to validate core replay correctness (Tier 1) and
  divergence detection (Tier 2) — the fundamental protocol. Structured
  concurrency (Tier 3-4) adds significant complexity that benefits from
  a solid foundation.
- **Consequences:** Spec tests 15-27 (structured concurrency, deterministic
  identity) are deferred. Close event emission can be simplified to
  try/finally in durableRun rather than scope middleware.

## DEC-006: Tier 1-2 tests first

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** The spec defines 37 tests across 7 tiers. Need to decide
  initial test coverage target.
- **Options considered:**
  1. Tier 1-2 first (tests 1-14)
  2. Tier 1-4 all at once
- **Decision:** Tier 1-2 first
- **Rationale:** Core replay correctness (Tier 1, tests 1-7) and divergence
  detection (Tier 2, tests 8-14) validate the fundamental protocol. These
  are achievable with sequential workflows. Tier 3-4 require spawn/all/race.
- **Consequences:** Tests 15-37 deferred to future phases.

## DEC-007: Protocol types are Effection-independent

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** The protocol types (DurableEvent, Yield, Close, Result, etc.)
  could depend on Effection types or be standalone.
- **Decision:** Protocol types in `lib/types.ts` have zero Effection imports.
  The DurableEffect and Workflow types that bridge to Effection are defined
  separately (also in types.ts for now, but with a placeholder shape).
- **Rationale:** The protocol is designed to be runtime-agnostic (spec §1.2).
  Keeping types independent enables potential reuse with other runtimes and
  makes the types testable without Effection.
- **Consequences:** The DurableEffect interface in types.ts uses a generic
  shape for `enter()` that will be aligned with Effection's exact Effect
  interface in Phase 1.

## DEC-008: Three distinct divergence error types

- **Phase:** 0 (Scaffolding)
- **Date:** 2026-02-28
- **Context:** Spec §6.2-6.3 defines three divergence conditions: description
  mismatch, generator finishes early, generator continues past close.
- **Options considered:**
  1. Single DivergenceError class with a `kind` field
  2. Three separate error classes
- **Decision:** Three classes: `DivergenceError`, `EarlyReturnDivergenceError`,
  `ContinuePastCloseDivergenceError`. All share `name = "DivergenceError"`.
- **Rationale:** Each carries different diagnostic fields (expected/actual
  descriptions vs. consumed/total counts). Separate classes enable precise
  `instanceof` checks in tests while sharing the same error name for catch-all
  handling.
- **Consequences:** Error handling code can match on the common name
  `"DivergenceError"` or use instanceof for specific cases.

## DEC-009: Workflow<T> = Generator<DurableEffect<unknown>, T, unknown>

- **Phase:** 1 (Protocol Types)
- **Date:** 2026-02-28
- **Context:** Need a type that constrains generator yields to durable effects
  only, while remaining assignable to Effection's Operation<T>.
- **Options considered:**
  1. `Iterable<DurableEffect<unknown>, T, unknown>` — TypeScript's Iterable
     only has 1 type parameter in the standard lib, cannot constrain yields.
  2. `Generator<DurableEffect<unknown>, T, unknown>` — has 3 type parameters
     (Yield, Return, Next).
  3. Custom interface extending both Generator and Operation.
- **Decision:** `Generator<DurableEffect<unknown>, T, unknown>`
- **Rationale:** Generator's 3 type parameters give TypeScript enough
  information to enforce the yield constraint. When a user writes
  `function*(): Workflow<T>`, TS checks that every `yield` expression
  produces a value assignable to `DurableEffect<unknown>`. Verified:
  `yield* sleep(1000)` inside a Workflow produces TS2741 error.
- **Consequences:** Workflow generators use `yield` (not `yield*`) for direct
  DurableEffect interaction, and `yield*` for delegating to other Workflows.
  The cast `as T` is needed when `yield`-ing a DurableEffect since TS types
  the yield expression as `unknown`.

## DEC-010: DurableEffect mirrors Effection's Effect interface shape exactly

- **Phase:** 1 (Protocol Types)
- **Date:** 2026-02-28
- **Context:** DurableEffect needs to be structurally compatible with
  Effection's `Effect<T>` interface so the reducer processes it identically.
- **Decision:** DurableEffect<T> has the same `description: string` and
  `enter(resolve, routine)` signature as Effect<T>, plus the additional
  `effectDescription: EffectDescription` field.
- **Rationale:** Effection's Effect<T> uses:
  - `enter(resolve: Resolve<Result<T>>, routine: Coroutine)`
  - returns `(resolve: Resolve<Result<void>>) => void` (teardown)
  - `Result<T> = { ok: true, value: T } | { ok: false, error: Error }`
  DurableEffect replicates this exactly. The extra field doesn't affect
  structural compatibility — the reducer ignores unknown properties.
- **Consequences:** Two different "Result" types exist — Effection's internal
  `{ ok, value/error }` and the protocol's `{ status, value/error }`. We
  define `EffectionResult<T>` in types.ts to bridge them without importing
  from Effection.

## DEC-011: CoroutineView — minimal interface instead of importing Coroutine

- **Phase:** 1 (Protocol Types)
- **Date:** 2026-02-28
- **Context:** The `enter()` callback receives an Effection `Coroutine` object.
  We need `routine.scope` to read DurableContext. Coroutine is marked
  `@ignore` in Effection's types (not part of public API).
- **Options considered:**
  1. Import Coroutine type from Effection internals
  2. Use `unknown` and cast at runtime
  3. Define a minimal CoroutineView interface with only what we need
- **Decision:** Define `CoroutineView` with `scope` property typed to match
  Scope's `get()`, `expect()`, `set()` methods.
- **Rationale:** Avoids depending on Effection's private API surface. The
  minimal interface documents exactly which Coroutine fields we rely on.
  If Effection's internal shape changes, the break is localized to this
  interface.
- **Consequences:** At runtime, `enter()` receives the full Coroutine object.
  TypeScript sees only our CoroutineView. This works because we only access
  `scope.expect(context)` which is a public Scope method.

## DEC-012: Verified — routine.scope is accessible in enter() callback

- **Phase:** 1 (Protocol Types)
- **Date:** 2026-02-28
- **Context:** The key risk identified in the plan was whether `routine.scope`
  is accessible from within `enter()`. Needed to confirm from Effection 4.1
  alpha source.
- **Decision:** Confirmed. Effection's `Coroutine` interface in
  `lib/types.ts` (line ~465) has `scope: Scope`. The reducer passes the
  full Coroutine object to `enter()`. We can access `routine.scope.expect(ctx)`
  to read DurableContext from within a DurableEffect's enter method.
- **Rationale:** Verified by reading Effection 4.1.0-alpha.5 source:
  `interface Coroutine<T> { scope: Scope; data: { ... }; next(...); return(...); }`
- **Consequences:** No workaround needed. The direct approach from the
  integration doc works.

## DEC-013: ReplayIndex follows spec §4.1 exactly with no extensions

- **Phase:** 2 (ReplayIndex)
- **Date:** 2026-02-28
- **Context:** The spec provides a reference implementation of ReplayIndex in
  §4.1. Could add extra features (e.g., event filtering, offset tracking).
- **Decision:** Follow the spec exactly. Only additions are `getCursor()` and
  `yieldCount()` which are trivial derived accessors for diagnostics/testing.
- **Rationale:** The ReplayIndex is a critical correctness component. Staying
  minimal and spec-aligned reduces the risk of subtle bugs. Extra features
  can be added if needed.
- **Consequences:** All replay logic depends on this class. It is thoroughly
  tested (21 tests covering empty index, single/multiple yields, close events,
  interleaved coroutines, race scenarios, and spec examples).

## DEC-014: createDurableEffect handles replay/live dispatch inside enter()

- **Phase:** 3 (Durable Runner)
- **Date:** 2026-02-28
- **Context:** The protocol requires each durable effect to check the replay
  index, validate descriptions, and either feed stored results or execute
  live with persist-before-resume. This logic could live in a central
  runner/reducer or inside each effect.
- **Decision:** Each `DurableEffect.enter()` handles its own replay/live
  dispatch internally, reading `DurableContext` from the scope via
  `routine.scope.expect(DurableCtx)`.
- **Rationale:** Keeps the Effection reducer completely untouched. The reducer
  calls `enter()` on every effect — whether `enter()` resolves synchronously
  (replay) or asynchronously (live + persist) is invisible to it. This is
  the architecture from the integration doc §5.1.
- **Consequences:** No changes to Effection internals. The `createDurableEffect`
  factory encapsulates all replay/persistence logic. Each workflow-enabled
  effect (durableSleep, durableCall, etc.) is a thin wrapper over this factory.

## DEC-015: Workflow<T> is directly assignable to Operation<T> — no casts needed

- **Phase:** 3 (Durable Runner)
- **Date:** 2026-02-28
- **Context:** `durableRun` calls `scope.run(workflow)` where workflow returns
  `Workflow<T>` (which is `Generator<DurableEffect<unknown>, T, unknown>`).
  Need to confirm this is assignable to Effection's `Operation<T>`.
- **Options considered:**
  1. Cast `workflow as () => Operation<T>` or use `as any`
  2. Rely on structural assignability
- **Decision:** No cast needed. `DurableEffect` extends `Effect` structurally,
  and TypeScript's covariant yield type means `Generator<DurableEffect, T, unknown>`
  is assignable to the iterator type that `Operation<T>` expects.
- **Rationale:** Verified empirically — `scope.run(workflow)` compiles without
  any type assertions. This confirms the type system design from DEC-009/010.
- **Consequences:** The type boundary between Workflow and Operation is seamless.

## DEC-016: durableRun short-circuits on existing Close event

- **Phase:** 3 (Durable Runner)
- **Date:** 2026-02-28
- **Context:** When `durableRun` is called with a stream that already contains
  a Close event for the root coroutine, should it re-run the workflow or
  return the stored result directly?
- **Decision:** Short-circuit. If `replayIndex.hasClose(coroutineId)` is true,
  return the stored result from the Close event without creating a scope or
  running the workflow.
- **Rationale:** A Close event means the workflow completed in a previous run.
  Re-running it would be wasteful and could produce unexpected behavior
  (e.g., side effects from live effects). The stored result is the canonical
  outcome.
- **Consequences:** Fully-completed workflows return instantly. The early-return
  check uses `hasClose()` (not `isFullyReplayed()`, which requires cursor
  advancement that hasn't happened yet).

## DEC-017: Persist-before-resume via Strategy B (async append + deferred resolve)

- **Phase:** 3 (Durable Runner)
- **Date:** 2026-02-28
- **Context:** The spec §5 defines the persist-before-resume invariant with
  three strategies. Need to choose one for the Effection integration.
- **Decision:** Strategy B — the effect's `enter()` calls `stream.append(event)`
  and places `resolve()` inside the `.then()` callback. The generator does
  not advance until the durable write completes.
- **Rationale:** This is the natural fit for Effection's async resolve model.
  The reducer waits for `resolve()` to be called, so deferring it until after
  the append guarantees persist-before-resume. Verified by the ordering test
  (execute → persist → resume for each step).
- **Consequences:** Live execution has one async hop per effect (the stream
  append). During replay, `resolve()` is called synchronously — zero async
  overhead.

## DEC-018: durableCall constrains T extends Json for serializability

- **Phase:** 3 (Durable Runner)
- **Date:** 2026-02-28
- **Context:** `durableCall<T>(name, fn)` stores the function's return value
  in the journal. The value must be JSON-serializable per the protocol.
- **Decision:** Constrain `T extends Json` at the type level.
- **Rationale:** Catches non-serializable return values at compile time rather
  than silently producing corrupt journal entries. The `Json` type from
  `types.ts` covers all JSON-serializable values.
- **Consequences:** Users must ensure their async functions return JSON-compatible
  values. Complex objects (Dates, class instances) need explicit serialization.
  The constraint is intentionally strict — relaxing it later is easy, but
  tightening it would be a breaking change.

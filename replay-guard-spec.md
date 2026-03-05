# Replay Guards: Stale Input Detection via Effection Middleware

**Status:** Design complete — ready for implementation
**Audience:** Charles, Taras
**Inputs:** Two-event durable execution spec (v2), effection-integration.md, durable-streams.md, prior art survey across 10+ systems

---

## 1. Problem statement

The durable execution protocol already detects **identity divergence** — when the effect a generator yields during replay doesn't match what's recorded in the journal (wrong type, wrong name, wrong position). This catches code changes that alter the effect sequence.

But there's a second class of divergence the protocol doesn't address: **stale input divergence**. The operation identity matches (same `{ type, name }` at the same cursor position), but the external world has changed since the journal entry was recorded. Examples:

- A file was imported during a previous run. The file's contents have since changed. Should replay feed the old import result, or re-read the file?
- A shell command was executed. The command text is the same, but an environment variable it depends on has a new value.
- An HTTP endpoint was called. The response was cached in the journal, but the upstream data has been updated.

The current protocol treats the journal as the single source of truth and replays stored results unconditionally. This is correct for pure durable execution (the Temporal/Restate model), but insufficient for document-component systems where freshness matters — the user expects that editing a source file and re-running the workflow should pick up their changes, not silently replay stale results.

The question: **how should the system separate fact-gathering (has this input changed?) from policy (what do we do about it?), and where do existing systems draw this line?**

---

## 2. Prior art survey

### 2.1 Durable execution systems: logs are authoritative

**Temporal** enforces strict replay determinism. During replay, Commands emitted by the workflow code are compared against the existing Event History. If a corresponding Event matches that Command, execution progresses. There is no validation of whether the external world has changed — the journal is authoritative by design. Non-determinism is detected structurally: if the code emits a Command that doesn't match the Event at that position in the history, Temporal raises a non-determinism error. Environment-dependent operations are forbidden inside orchestrators; they must be pushed into Activities, which are the boundary where the external world is consulted.

When code changes are necessary, Temporal provides two versioning strategies. The patching approach (`GetVersion`/`Patched`) embeds version gates directly in workflow code so that old and new paths coexist. Worker Versioning ties Workers to specific code revisions, routing each workflow execution to the deployment that created it.

**Restate** follows the same model. Its journal-based replay treats the server's view as ground truth. Journal entries are replayed as-is without input validation. Non-deterministic operations (including LLM responses) are explicitly journaled because their results can't be reproduced. Restate uses immutable deployment URLs for version isolation — each deployment gets a stable endpoint, and in-flight executions continue against their original deployment.

**Azure Durable Functions** uses event sourcing with deterministic orchestrators. The documentation explicitly warns against reading environment variables inside orchestrators — these must be passed as input parameters or fetched through Activity functions. An orchestration versioning feature (in preview) allows version coexistence through side-by-side deployment.

**Pattern: "logs are authoritative."** All three systems make the same architectural choice: the journal/event history is unconditionally trusted during replay. Validation of external inputs is not the orchestrator's job — it's pushed to the boundary (Activities, side effects, deployment routing). This is the simplest model and eliminates an entire class of complexity, but it requires that all non-deterministic or environment-dependent reads happen through journaled effect boundaries.

### 2.2 Build systems: content-addressed validation

**Bazel** uses content-addressed storage with SHA256 hashes as the universal validation mechanism. The Action Cache maps action keys (which include input hashes) to output results. The Content Addressable Storage stores artifacts by hash. Staleness detection is implicit: if any input hash changes, the action key changes, and the cache misses. There is no separate validation step — the hash *is* the validation. Bazel also implements early cutoff: if an action re-runs but produces output identical to the cached output, downstream dependents are not rebuilt.

**Nix** uses fixed-output derivations (FODs) identified by name plus output hash. A derivation only rebuilds if its name or declared output hash changes. Input changes don't trigger rebuilds unless the developer manually updates the hash. If a build produces output that doesn't match the declared hash, Nix raises an error. This is a "trust but verify" model — the developer declares what the output should be, and the system validates post-hoc.

**Shake** implements early cutoff as a first-class optimization. Files that rebuild but produce unchanged output don't invalidate dependents. Staleness is detected through modification times or content hashes. The system has no built-in validation of rule changes; rules are opaque Haskell code. A `shakeVersion` field allows manual cache invalidation when build rules change.

**Pattern: "hash-based implicit validation."** Build systems use content hashes as both identity and validation simultaneously. The key insight is that validation is *embedded in the cache key* — there's no separate validation phase because a changed input produces a different key, which is a cache miss by definition. Early cutoff adds a refinement: even when validation says "input changed," if the *output* is unchanged, propagation stops.

### 2.3 Incremental computation: lazy revalidation with early cutoff

**Salsa** (used in rust-analyzer) implements query-based incremental computation. Inputs are explicitly set; derived values are computed through queries. When an input changes, Salsa doesn't immediately recompute all dependents. Instead, on the next access to a derived value, it walks the dependency graph and checks whether any transitive input has changed. If an intermediate query recomputes but produces the same result, the walk stops — this is early cutoff applied to a dependency graph.

Salsa also introduces **durability partitioning**: inputs are classified by expected change frequency (high, medium, low). Queries that depend only on low-durability inputs (like standard library definitions) skip validation entirely when only high-durability inputs have changed. This is a practical optimization — checking whether the standard library changed on every keystroke is wasteful.

**The "Build Systems à la Carte" framework** (Mokhov et al.) formalizes the design space. It distinguishes verifying traces (check if stored result is still valid by comparing input hashes) from constructive traces (can reconstruct the result from the trace without re-executing). Modern build systems generally implement verifying traces with early cutoff.

**Pattern: "lazy revalidation with early cutoff."** Incremental computation systems defer validation until the result is actually needed, then walk the dependency graph checking hashes. The key refinement over build systems is early cutoff at intermediate nodes — recomputation stops propagating when a node's output is unchanged despite changed inputs.

### 2.4 HTTP caching: explicit validation protocol

HTTP caching provides the most explicit separation of validation from decision in any system surveyed.

**Validation mechanism:** The server attaches metadata to responses — `ETag` (content hash) and `Last-Modified` (timestamp). The client stores these alongside the cached response.

**Validation execution:** When the cached response is stale (past `max-age`), the client sends a conditional request with `If-None-Match: <etag>` or `If-Modified-Since: <date>`. The server validates and returns either `304 Not Modified` (reuse cache) or `200` with new content.

**Decision policy:** HTTP separates freshness (is the cache entry still within its declared lifetime?) from validation (does the origin confirm the entry is still valid?). Two extensions formalize the decision policy for stale content. `stale-while-revalidate` says: serve the stale response immediately, but revalidate in the background — prioritize latency over freshness. `stale-if-error` says: if the origin is unreachable, serve stale rather than error — prioritize availability over freshness.

**Pattern: "metadata + conditional validation + configurable policy."** HTTP makes every part of the pipeline explicit and separable. The metadata (ETag/Last-Modified) is attached at write time. The validation (conditional request) is a separate protocol step. The decision (serve stale? revalidate async? error?) is configurable per-response. This is the most complete realization of the validator/decision split.

---

## 3. Pattern catalog

| Pattern | Validation | Decision | Complexity | Representative systems |
|---------|-----------|----------|------------|----------------------|
| **Logs are authoritative** | None during replay | Always use stored result | Low | Temporal, Restate, Azure DF |
| **Hash-based implicit** | Content hash of inputs, embedded in cache key | Hash match → reuse; mismatch → re-execute | Low | Bazel, Nix, HTTP ETag |
| **Early cutoff** | Re-execute, compare output to cached output | Output unchanged → stop propagation | Medium | Shake, Salsa, Bazel |
| **Durability partitioning** | Classify inputs by change frequency; skip stable inputs | Only validate volatile partition | Medium | Salsa |
| **Stale-while-revalidate** | Async background revalidation | Serve stale immediately; update cache when validation completes | Medium | HTTP caching |
| **Conditional validation** | Server-side check against stored metadata | 304 (reuse) or 200 (replace) | Medium | HTTP ETag/Last-Modified |

### 3.1 What they share

Every system that does validation at all (everything except the pure durable execution systems) follows the same two-phase structure:

1. **Compute a fact about assumptions.** Hash the inputs. Check a timestamp. Send a conditional request. The output is a boolean or a diff: "still valid" or "changed, here's how."

2. **Apply a policy to the fact.** Reuse the cached result. Re-execute. Serve stale while revalidating. Error out. The policy is separate from the fact.

The systems differ in *when* validation runs (eagerly at replay start vs. lazily on access), *what* is validated (inputs, outputs, or both), and *how configurable* the policy is (hardcoded vs. user-specified).

### 3.2 What they don't share

Build systems validate inputs *before* deciding whether to re-execute. Incremental computation systems validate *lazily* — only when the result is needed. HTTP caching validates *on staleness* — when the freshness window expires. Durable execution systems don't validate at all — the journal is truth.

The right choice depends on the cost of validation relative to the cost of re-execution, and the tolerance for stale results.

---

## 4. Design implications for the durable execution protocol

### 4.1 The architectural tension

The protocol's current design is "logs are authoritative" — the Temporal model. This is correct for effects with external side effects (charging a credit card, sending an email) where re-execution would be harmful. But the executable document use case introduces effects where re-execution is *desirable*:

- **File resolution** (`resolve`): reading a file's contents. Re-execution is safe and cheap. Staleness is the primary concern.
- **Component import** (`import`): importing a module. Re-execution is safe. The module may have changed.
- **Shell execution** (`exec`): running a command. Re-execution may be unsafe (side effects) or safe (pure computation), depending on the command.

The system needs to support both models: unconditional replay for side-effectful operations, and conditional replay (with validation) for operations where freshness matters.

### 4.2 Where validation fits in the protocol

Validation is split across two phases of execution, separated by a strict I/O boundary:

**Phase 1: Check (before replay begins).** Runs in generator context inside `durableRun`, after the journal is loaded but before the workflow starts. I/O is allowed — this is where file hashing, network checks, and other observation-gathering happens. Results are cached in the middleware closure.

**Phase 2: Decide (during replay).** Runs synchronously inside `DurableEffect.enter()`, after identity matching succeeds but before the stored result is fed to the generator. Must be pure and side-effect-free. Reads from the cache populated during Phase 1.

This split preserves three invariants:

- The replay loop remains synchronous — no I/O during `enter()`.
- The transparency invariant holds — the generator can't detect whether it received a replayed result or a fresh one.
- Identity checking remains the first guard — stale input detection only runs after `{ type, name }` matching succeeds.

### 4.3 What validation must NOT do

**Validation must not perform I/O during replay.** If the decide function reads files, makes network calls, or executes commands, it introduces non-determinism into the replay path. The replay path must remain synchronous and side-effect-free. All I/O-based observation happens in the check phase, before replay begins.

### 4.4 Risks of the validator/decision split

**Complexity.** Every effect now has three possible outcomes during replay (replay, re-execute, error) instead of two (replay or live).

**Re-execution during replay breaks causal ordering.** If a validator triggers re-execution of effect N, and effect N+1 causally depends on effect N's result, the stored result for N+1 may be invalid. This cascading invalidation is the same problem build systems solve with dependency graphs — but durable execution journals don't have explicit dependency edges.

**Re-execution may produce different results.** If effect N is re-executed and produces a different result, the generator may take a different code path, causing identity divergence at effect N+1. This is not a bug — it's the correct behavior when inputs have changed — but it means the journal after position N is invalidated.

**Mitigation:** The simplest safe policy for v1 is: validation failure → error. Don't attempt re-execution during replay. Surface the staleness to the operator and let them decide (re-run from scratch, or accept stale results). This matches Nix's model (hash mismatch = build error) and is fail-closed.

---

## 5. Design: the ReplayGuard middleware

The prior art survey reveals a universal two-phase pattern: compute a fact about assumptions, then apply a policy. The design implements this as an Effection `Api` with two methods — one for each phase — composed via Effection's native `scope.around()` middleware system.

### 5.1 Rich effect descriptions and results

Effect inputs and outputs are stored in their natural locations — no separate metadata field is needed:

- **Effect inputs** (file path, encoding, URL, etc.) belong in extra fields on `EffectDescription`. The `EffectDescription` interface is open: it has `type` and `name` as checked identity fields, plus an index signature `[key: string]: Json` for arbitrary extra fields that are stored verbatim but never compared during divergence detection.
- **Effect outputs** (content hash, status code, duration, etc.) belong in `result.value`. Effects that need staleness validation return rich result objects that include validation data alongside the actual content.

The `ReplayGuard.check` and `decide` methods read from `event.description.*` for inputs and `event.result.value.*` for outputs. This is the natural separation already established by the protocol: descriptions say what was requested, results say what was produced.

### 5.2 The ReplayGuard Api

```typescript
type ReplayOutcome =
  | { outcome: "replay" }
  | { outcome: "error"; error: Error }
  // Future:
  // | { outcome: "reexecute" }
  // | { outcome: "fork" }

interface ReplayGuardApi {
  /**
   * Phase 1: Check
   *
   * Called once per Yield event, before replay begins.
   * Runs in generator context — I/O is allowed.
   * Use this to gather current state (hash files, check timestamps, etc.)
   * and cache results for the decide phase.
   */
  check(event: Yield): Operation<void>;

  /**
   * Phase 2: Decide
   *
   * Called during replay, after identity matching succeeds.
   * Must be pure and synchronous — no I/O, no side effects.
   * Returns the replay outcome based on cached observations.
   */
  decide(event: Yield): ReplayOutcome;
}

const ReplayGuard = createApi<ReplayGuardApi>("replay-guard", {
  *check(_event) {},
  decide(_event) {
    return { outcome: "replay" };
  },
});
```

The default implementation is a pass-through: `check` does nothing, `decide` returns `{ outcome: "replay" }`. This preserves "logs are authoritative" as the default behavior — the journal is unconditionally trusted unless middleware says otherwise.

### 5.3 Why this is two methods, not three

The earlier design (§2 of this document's history) used a three-phase `gather → compare → decide` model with separate interfaces for each. The `ReplayGuard` collapses compare and decide into a single `decide` method. This is simpler and more natural:

- `check` gathers observations and caches them in the middleware closure.
- `decide` reads from the cache, compares, and returns a policy decision in one step.

There's no value in separating "is it stale?" from "what do we do about staleness?" into distinct function calls when they share the same cached state and run in the same synchronous context. The middleware author writes both as a single coherent unit.

### 5.4 Middleware composition

Multiple replay guards compose via Effection's `scope.around()`:

```typescript
function* installGuardA(): Operation<void> {
  const scope = yield* useScope();
  scope.around(ReplayGuard, {
    *check([event], next) {
      // ... gather for guard A ...
      return yield* next(event);
    },
    decide([event], next) {
      if (isStaleByGuardA(event)) {
        return { outcome: "error", error: new Error("stale by A") };
      }
      return next(event);
    },
  });
}

function* installGuardB(): Operation<void> {
  const scope = yield* useScope();
  scope.around(ReplayGuard, {
    *check([event], next) {
      // ... gather for guard B ...
      return yield* next(event);
    },
    decide([event], next) {
      if (isStaleByGuardB(event)) {
        return { outcome: "error", error: new Error("stale by B") };
      }
      return next(event);
    },
  });
}
```

**Decision precedence.** Middleware that has an opinion returns an outcome directly. Middleware that doesn't calls `next(event)` to delegate. The first middleware in the chain that returns `error` wins — the chain short-circuits. If no middleware has an opinion, the default at the bottom of the chain returns `{ outcome: "replay" }`.

This naturally implements the aggregation rule `error > replay`: if *any* guard errors, replay halts. The middleware chain ordering is determined by `scope.around()` installation order, consistent with how all Effection middleware works.

**Scope inheritance.** A parent workflow installs a replay guard, and all child scopes — including children spawned by `durableAll` and `durableRace` — see it through Effection's context inheritance. No explicit registration on `DurableRunOptions`. No passing guards through constructors. Just `yield*` during setup.

### 5.5 Integration into durableRun

The check phase runs in `durableRun`'s generator context, after the journal is loaded and the ReplayIndex is built, but before the workflow starts:

```typescript
async function durableRun<T extends Json | void>(
  workflow: () => Workflow<T> | Operation<T>,
  options: DurableRunOptions,
): Promise<T> {
  const { stream, coroutineId = "root" } = options;
  const events = await stream.readAll();
  const replayIndex = new ReplayIndex(events);

  // Short-circuit on existing Close (unchanged)...

  const [scope, destroy] = createScope();
  scope.set(DurableCtx, { replayIndex, stream, coroutineId, childCounter: 0 });

  try {
    // ── NEW: Check phase ──
    // Runs in generator context. Middleware can yield* for I/O.
    for (const event of events) {
      if (event.type === "yield") {
        yield* ReplayGuard.check(event);
      }
    }

    // Run workflow (unchanged)
    const task = scope.run(workflow);
    const result = await task;

    // Early return divergence check, Close event, etc. (unchanged)...
  } finally {
    // ...
  }
}
```

The check loop iterates all Yield events in journal order. Each installed middleware sees every event and can cache whatever it needs. Since `check` is an `Operation<void>`, middleware can `yield*` to perform I/O (read files, compute hashes, make network requests).

### 5.6 Integration into DurableEffect.enter()

The decide phase runs synchronously inside `enter()`, after identity matching succeeds:

```typescript
// Inside enter(), replay path:

if (entry) {
  // Identity check (existing code)...
  if (entry.description.type !== desc.type ||
      entry.description.name !== desc.name) {
    // DivergenceError (existing code)...
  }

  // ── NEW: Replay guard ──
  const outcome = ReplayGuard.decide(entry);

  if (outcome.outcome === "error") {
    ctx.replayIndex.consumeYield(ctx.coroutineId);
    resolve({
      ok: false,
      error: outcome.error ?? new StaleInputError(ctx.coroutineId, desc),
    });
    return (exit) => exit(VOID_OK);
  }

  // Normal replay (existing code)...
  ctx.replayIndex.consumeYield(ctx.coroutineId);
  resolve(protocolToEffection<T>(entry.result));
  return (exit) => exit(VOID_OK);
}
```

The `decide` call is synchronous. The middleware reads from whatever cache it populated during the check phase and returns an outcome. No I/O, no async, no scheduler round-trips. Replay stays fast.

---

## 6. Example: file content replay guard

This is the primary use case for the executable document runtime.

### 6.1 Intent

> "If the file backing this effect has changed since the journal entry was recorded, replay is no longer safe."

### 6.2 Rich descriptions and results written at live time

When a file-reading effect resolves during live execution, the file path is stored as an extra field on the effect description, and the content hash is included in the result value alongside the content:

```typescript
function* durableResolve(path: string): Workflow<string> {
  const { content } = yield* durableCall("resolve", async () => {
    const content = await Deno.readTextFile(path);
    return { content, contentHash: sha256(content) };
  });
  return content;
}
```

Journal entry:
```
yield root
  description: { type: "call", name: "resolve", path: "./component.mdx" }
  result: { status: "ok", value: { content: "...", contentHash: "sha256:abc123" } }
```

The file path is an *input* to the effect and belongs in the description. The content hash is an *output* of the effect and belongs in the result value. Divergence detection only compares `type` and `name` — the extra `path` field is stored verbatim and never checked.

### 6.3 Middleware implementation

```typescript
function* useFileContentGuard(): Operation<void> {
  const scope = yield* useScope();

  // Cache: filePath → current SHA, populated during check phase
  const cache = new Map<string, string>();

  scope.around(ReplayGuard, {
    *check([event], next) {
      // check reads input from description
      const filePath = event.description.path as string | undefined;
      if (typeof filePath === "string") {
        if (!cache.has(filePath)) {
          // I/O is safe here — we're in generator context, before replay
          const currentSHA = yield* call(() => computeFileHash(filePath));
          cache.set(filePath, currentSHA);
        }
      }
      // Always call next — other middleware may need to check this event too
      return yield* next(event);
    },

    decide([event], next) {
      // check reads input from description
      const filePath = event.description.path as string | undefined;
      // check reads recorded hash from result
      const resultValue = event.result.status === "ok" ? event.result.value : undefined;
      const recordedHash = (resultValue as any)?.contentHash as string | undefined;

      if (typeof filePath === "string" && typeof recordedHash === "string") {
        // decide compares currentHash (from cache) against recordedHash (from result.value)
        const currentSHA = cache.get(filePath);

        if (currentSHA && currentSHA !== recordedHash) {
          return {
            outcome: "error",
            error: new StaleInputError(
              `File changed: ${filePath} ` +
              `(recorded: ${recordedHash.slice(0, 8)}…, ` +
              `current: ${currentSHA.slice(0, 8)}…)`
            ),
          };
        }
      }
      // No opinion — delegate to next middleware or default
      return next(event);
    },
  });
}
```

### 6.4 Usage

```typescript
await durableRun(function* () {
  // Install the replay guard — children inherit it
  yield* useFileContentGuard();

  // This effect's replay will be checked against current file content
  const content = yield* durableResolve("./component.mdx");
  yield* durableCall("render", () => render(content));
}, { stream });
```

### 6.5 Replay scenarios

**File unchanged:**

1. Check phase: read `event.description.path` → `"./component.mdx"`. Hash file → `sha256:abc123`. Cache: `{ "./component.mdx" → "abc123" }`.
2. Replay: identity check passes. `decide` reads `event.result.value.contentHash` → `"abc123"`, compares to `cache.get(filePath)` → match. Returns `next(event)` (no opinion). Default returns `{ outcome: "replay" }`.
3. Stored result fed to generator. No re-execution.

**File changed:**

1. Check phase: read `event.description.path` → `"./component.mdx"`. Hash file → `sha256:def456`. Cache: `{ "./component.mdx" → "def456" }`.
2. Replay: identity check passes. `decide` reads `event.result.value.contentHash` → `"abc123"`, compares to `cache.get(filePath)` → mismatch. Returns `{ outcome: "error" }`.
3. `StaleInputError` propagates through normal Effection error channels.

**Effect has no file path in description:**

1. Check phase: `event.description.path` is undefined. `check` calls `next(event)` without caching.
2. Replay: `decide` sees no `path` in description. Calls `next(event)` (no opinion). Default returns `{ outcome: "replay" }`.
3. Normal replay. Effects without file path data in their description are always replayed — this preserves "logs are authoritative" for effects that don't opt into validation.

### 6.6 Deduplication

The cache is keyed by `filePath`, so if 20 events reference the same file, the hash is computed once during the check loop and reused for all 20 `decide` calls. This is a natural consequence of the closure-based cache pattern — no runtime-managed deduplication needed.

---

## 7. Interaction with existing protocol features

### 7.1 Divergence detection

Identity matching (`description.type` + `description.name` at cursor position) remains the first check. Extra fields on `EffectDescription` beyond `type` and `name` are never compared during divergence detection. Replay guards only run after identity matching succeeds. If identity matching fails, it's a `DivergenceError` regardless of what extra fields are present. The two systems are layered: identity first, then staleness.

### 7.2 Structured concurrency

Replay guards are per-scope, inherited by children. Each `DurableEffect.enter()` call invokes `ReplayGuard.decide()` in its own scope's middleware chain. For `durableAll` with concurrent children, each child's effects are checked independently. If one child's effect is stale and triggers an error, the error propagates through normal structured concurrency channels (sibling cancellation, parent error handling).

The check phase runs once in the parent's scope before the workflow starts. All child scopes inherit the parent's middleware (and its populated cache) through Effection's context inheritance.

### 7.3 Close events

Close events aren't passed through replay guards. They record terminal states derived from preceding effects. If an effect's stored result is stale, the Close event is also stale — but this is handled by gating the effect, not the Close.

### 7.4 Durable Streams backend

Extra fields on `EffectDescription` are additional JSON stored as part of the description object in the Yield event payload. No changes to the Durable Streams protocol or `HttpDurableStream` adapter. The fields serialize and deserialize as part of the event.

### 7.5 Version gates

Version gates and replay guards are complementary. Version gates handle *code* changes (the effect sequence changed). Replay guards handle *input* changes (the effect sequence is the same but external data changed). Both can apply simultaneously — a version gate might add a new replay-guard-aware effect to an existing workflow.

### 7.6 DurableContext

The `DurableContext` does not change. Replay guards are not stored on `DurableContext` — they're composed through Effection's middleware system on the scope, orthogonal to the durable execution context. This keeps the durable protocol's core types clean.

---

## 8. Version roadmap

### v1: Open EffectDescription + error on stale (~100-150 lines)

- Open `EffectDescription` to carry extra fields beyond `type` and `name`.
- Implement `ReplayGuard` Api with `check` and `decide` methods.
- Implement `StaleInputError` error class.
- Wire check phase into `durableRun` (loop over events before workflow starts).
- Wire decide phase into `DurableEffect.enter()` (after identity check).
- Implement `useFileContentGuard()` as the first concrete guard.
- Default behavior: `{ outcome: "replay" }` — logs are authoritative unless a guard says otherwise.

This is enough for the executable document use case: if a source file has changed, the system detects it and errors rather than silently replaying stale content.

### v2: Re-execute decision (+~50 lines)

- Add `{ outcome: "reexecute" }` to `ReplayOutcome`.
- In `DurableEffect.enter()`, handle re-execute by consuming the replay entry and falling through to the live path.
- Handle journal invalidation: when an effect is re-executed, discard all subsequent entries for this coroutine (truncate-and-continue). The coroutine transitions to live execution from that point.

The journal invalidation is the hard part. Truncate-and-continue is the pragmatic choice — it matches what would happen if the operator manually re-ran from that checkpoint.

### v3: Early cutoff (+~30 lines)

- After re-executing an effect, compare the new result to the stored result.
- If identical: continue replay as normal (downstream effects are still valid).
- If different: invalidate subsequent entries (v2 behavior).

This is the Shake/Salsa/Bazel optimization. Minimizes unnecessary re-execution when an input changed but the output didn't (e.g., a file was reformatted but the parsed content is identical).

### v4: Durability hints (+~50 lines)

- Implement a `stable` guard that always returns `{ outcome: "replay" }` for specified effect types, skipping all other guards.
- Implement a `volatile` guard that always returns `{ outcome: "reexecute" }` for specified effect types.

This maps to Salsa's durability partitioning. Standard library imports are `stable` (skip validation), user source files go through `useFileContentGuard`.

Expressed as middleware, durability hints are just guards with hardcoded decisions installed at higher priority:

```typescript
function* useStableGuard(effectNames: string[]): Operation<void> {
  const scope = yield* useScope();
  scope.around(ReplayGuard, {
    *check([event], next) { return yield* next(event); },
    decide([event], next) {
      if (effectNames.includes(event.description.name)) {
        return { outcome: "replay" };  // Skip all other guards
      }
      return next(event);
    },
  });
}
```

---

## 9. Test plan

### Test 1: No guards installed → normal replay

**Setup:** Stream with recorded Yield events. No replay guard middleware installed.

**Action:** `durableRun` replays the stream.

**Assert:** All events replayed from journal. No live execution. No errors. Default `decide` returns `{ outcome: "replay" }` for every event.

---

### Test 2: Guard installed, event has no applicable fields → replay proceeds

**Setup:** `useFileContentGuard()` installed. Event has no `path` in description (or no `contentHash` in result value).

**Action:** Replay.

**Assert:** `check` runs but caches nothing (no `path` in description). `decide` calls `next(event)` (no opinion). Event replayed normally.

---

### Test 3: File unchanged → replay proceeds

**Setup:** Event description: `{ type: "call", name: "resolve", path: "./a.mdx" }`. Result: `{ status: "ok", value: { content: "...", contentHash: "abc123" } }`. File `./a.mdx` currently hashes to `"abc123"`.

**Action:** Replay.

**Assert:** `check` hashes file, caches `"abc123"`. `decide` reads `description.path` and `result.value.contentHash`, sees match, calls `next(event)`. Event replayed. No live execution.

---

### Test 4: File changed → replay errors

**Setup:** Event description: `{ type: "call", name: "resolve", path: "./a.mdx" }`. Result: `{ status: "ok", value: { content: "...", contentHash: "abc123" } }`. File `./a.mdx` currently hashes to `"def456"`.

**Action:** Replay.

**Assert:** `check` hashes file, caches `"def456"`. `decide` reads `description.path` and `result.value.contentHash`, sees mismatch, returns `{ outcome: "error" }`. `StaleInputError` raised. No live execution of this or subsequent effects.

---

### Test 5: Multiple guards, one errors → replay halts

**Setup:** Two guards installed. Guard A: returns `next(event)` (no opinion on this event). Guard B: returns `{ outcome: "error" }`.

**Action:** Replay.

**Assert:** Replay halts with Guard B's error. Order of guard installation does not affect the outcome — any guard returning error halts replay.

---

### Test 6: Check runs before replay, not during

**Setup:** Track when `check` calls occur relative to workflow execution.

**Action:** `durableRun` with a guard installed and a stream with N events.

**Assert:** All `check` calls complete before the first `DurableEffect.enter()` is invoked. `check` is never called from within the replay loop.

---

### Test 7: Decide is pure — same inputs, same output

**Setup:** Spy on `decide`.

**Action:** Replay the same stream twice with the same file state.

**Assert:** `decide` returns identical results for identical inputs. No observable side effects from `decide` calls.

---

### Test 8: Decide not called if identity check fails

**Setup:** Journal has `{ type: "call", name: "fetchOrder" }` at position 0. Generator yields `{ type: "call", name: "chargeCard" }` at position 0.

**Action:** Replay.

**Assert:** `DivergenceError` raised at identity check. `ReplayGuard.decide` is never called for this event.

---

### Test 9: Check deduplicates file hashes via cache

**Setup:** Stream has 5 events, all with `description.path: "./a.mdx"`.

**Action:** Replay.

**Assert:** File `./a.mdx` is hashed exactly once during the check phase. All 5 `decide` calls read from the same cached value.

---

### Test 10: Guard inherited by child scopes

**Setup:** Parent installs `useFileContentGuard()`. Parent spawns children via `durableAll`. Children have Yield events with file path in description and content hash in result value.

**Action:** Replay.

**Assert:** Children's effects pass through the parent's replay guard. A stale file in a child's event produces a `StaleInputError`.

---

### Test 11: Guard error propagates through structured concurrency

**Setup:** `durableAll([childA, childB])`. `childA` has a stale file event. `childB` is clean.

**Action:** Replay.

**Assert:** `childA` raises `StaleInputError`. `childB` is cancelled (fail-fast). Parent receives the error. Standard Effection structured concurrency error propagation.

---

### Test 12: Default behavior is fail-closed

**Setup:** No custom guard installed. Just the default `ReplayGuard`.

**Action:** Replay a stream with extra description fields on events.

**Assert:** Default `decide` returns `{ outcome: "replay" }` for all events. Extra fields are ignored without guards installed. This confirms "logs are authoritative" as the default.

**Note:** Fail-closed means that when a guard *is* installed and detects staleness, the default decision is `error`, not silent continuation. Test 4 verifies this for the file content guard.

---

## 10. Explicit non-goals (v1)

The following are explicitly out of scope for the initial implementation:

- **Re-execution** (`{ outcome: "reexecute" }`): Requires journal truncation. Deferred to v2.
- **Forking** (`{ outcome: "fork" }`): Requires new execution from a journal midpoint. Future.
- **Early cutoff**: Requires re-execution infrastructure from v2. Deferred to v3.
- **Journal truncation / shadow journals**: Tied to re-execution. Deferred to v2.
- **Async validation during replay**: The decide phase is synchronous. All I/O happens in check.
- **Per-effect opt-out of guards**: All events pass through all installed guards. Durability hints (v4) add selective skipping.

---

## 11. Key takeaways

**The "logs are authoritative" model is the correct default.** Every durable execution system in production uses it. Replay guards are an opt-in extension for specific effect types, not a replacement for the core replay model. The default `ReplayGuard` returns `{ outcome: "replay" }` — unconditional trust in the journal.

**Two methods, not three.** The check/decide split maps to the universal two-phase pattern from the prior art: gather observations (check), then apply policy (decide). Collapsing compare+decide into a single `decide` method eliminates a seam that adds no value.

**Effection middleware gives composition for free.** Replay guards compose via `scope.around()`, inherit through scope trees, and chain with standard middleware semantics. No custom registration system, no runtime-managed maps, no explicit aggregation rules. The middleware chain *is* the aggregation.

**Fail-closed is the only safe default.** When a guard detects staleness, it returns `error`. Re-execution is an optimization that requires careful handling of journal invalidation and is deferred to v2.

**The cache-via-closure pattern bridges the I/O boundary.** `check` populates a `Map` in the middleware closure. `decide` reads from it. The closure is the natural bridge between generator context (I/O allowed) and synchronous callbacks (no I/O). No runtime-managed validation context objects.

**Implementation is ~100-150 lines for v1.** The `ReplayGuard` Api definition, the decide call in `DurableEffect.enter()`, the check loop in `durableRun`, `StaleInputError`, and `useFileContentGuard()`. The core protocol changes are minimal — two insertion points in existing code.

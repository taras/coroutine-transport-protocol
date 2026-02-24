# Big Idea

Create a version of [Effection](https://github.com/thefrontside/effection) that uses [Durable Streams](https://github.com/durable-streams/durable-streams) under the hood. We won't be able to use the operations that come with Effection, but we can implement a version of the same operations using Durable Streams so that we can use the same syntax, with different types, that will be durable by default.

## Current State

The durable runner (`runner.ts`) wraps Effection's `run()` and intercepts every Effect at the iterator level. It records events to a `DurableStream` and on resume, replays stored results without calling `effect.enter()`. The runner handles:

- **Basic workflows** — single and multi-step effect recording/replay
- **Spawn** — parent/child scope interleaving with `useScope()` interception via Proxy
- **Resource** — full lifecycle (useCoroutine → useScope → await resource → trap return)
- **Ensure** — cleanup registration preserved across replay
- **All** — concurrent child operations
- **Race** — first-wins with loser halting
- **Suspend** — indefinite pause (yielded but never resolved)
- **Each** — stream iteration with per-item checkpoints via `each()` + `createSignal`
- **Divergence detection** — mismatched effect descriptions throw `DivergenceError`
- **Error handling** — effect errors and generator throws captured in stream

24 passing tests in `runner_test.ts` covering live execution and replay for all 17 pseudo-test scenarios.

## Key Files

- `types.ts` — DurableEvent union, DurableStream interface, Json/SerializedError types
- `stream.ts` — InMemoryDurableStream (test implementation)
- `runner.ts` — durableRun(), wrapOperation(), wrapScope(), __liveOnly fallthrough, child scope ID replay
- `runner_test.ts` — 24 tests covering pseudo-tests 1-12, 14-17
- `pseudo-tests.ts` — Design doc with 17 scenario descriptions (roadmap)

## Serialization Boundaries (__liveOnly)

Non-JSON-serializable effect results (Iterables, Coroutines, etc.) are stored as `{__liveOnly: true, __type, __toString}` sentinels in the stream. During replay, the runner handles these with a **per-scope** strategy:

- If any effect in a scope's replay queue has a `__liveOnly` result (excluding `useCoroutine()`), the **entire scope skips replay** and all effects re-execute live. This avoids divergence caused by mixing synthetic and live effects within a scope.
- `useCoroutine()` is excluded from this check because its placeholder is tolerated by Effection internals (resource, all, race). Re-executing `useCoroutine()` live causes `resource()` to take different code paths, changing the effect sequence.
- Child scope IDs are replayed from the stream via `childScopeQueues` to stay aligned when scopes re-execute.

This approach enables `each()` replay: the subscription scope (which has `__liveOnly` results) re-executes live, recreating the Iterable, while the subscription's `.next()` results (`{done: false, value: N}`) are JSON-safe and replay from stored values.

## Remaining Work

- **Context set/inherit events** (pseudo-test 9) — scope:set/scope:delete events not yet emitted (context works via Effection's scope prototype chain, but isn't explicitly captured in stream)
- **Real Durable Streams backend** — currently only InMemoryDurableStream
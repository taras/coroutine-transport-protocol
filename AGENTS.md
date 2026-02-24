# Big Idea

Create a version of [Effection](https://github.com/thefrontside/effection) that uses [Durable Streams](https://github.com/durable-streams/durable-streams) under the hood. The implementation lives in a fork of Effection (`~/Repositories/frontside/effection`, branch `durable-internals`) where `run()` always goes through a `DurableReducer` that records/replays effect resolutions to a DurableStream.

## Architecture

- **DurableReducer** replaces Effection's `Reducer` via `ReducerContext` injection
- **Recording**: User-facing effects are recorded to the DurableStream (infrastructure effects like `useCoroutine()`, `useScope()` execute live and are not recorded)
- **Replay**: On resume, stored results are fed back to generators without calling `effect.enter()`
- **Divergence detection**: Mismatched effect descriptions throw `DivergenceError`
- **Scope lifecycle**: `Api.Scope` middleware records `scope:created`, `scope:destroyed`, `scope:set`, `scope:delete` events; each scope gets a deterministic durable ID via `WeakMap<Scope, string>`
- `run()` accepts optional `{ stream }` — defaults to ephemeral `InMemoryDurableStream`

## Repository Layout

- `~/Repositories/frontside/effection` (branch: `durable-internals`) — Fork with DurableReducer implementation + Effection's own test suite as validation
- `~/Repositories/cowboyd/coroutine-transport-protocol` — Consumer/demo project, imports from fork

## Current Status: Phase 2 Complete

- All 23 Effection `run.test.ts` tests pass (previously 4 were skipped — now un-skipped)
- 13 durable-specific tests pass (recording, replay, mid-workflow resume, divergence, halt)
- 12 scope lifecycle tests pass (scope:created, scope:destroyed, parent-child hierarchy, scope IDs in effects, replay with scope events, error recording, halt lifecycle)
- 191 total test steps passing across 25 test suites (0 failures)
- Effection fork pushed to `taras/effection` (branch: `durable-internals`)
- Next: Phase 3 (workflow:return wiring + durable spawn resume + divergence validation)

## Implementation Phases

1. ✅ DurableReducer + action/sleep
2. ✅ Api.Scope middleware + scope lifecycle events
3. ⬜ Durable spawn + generation counter (re-scoped: workflow:return wiring + durable spawn resume + divergence validation)
4. ⬜ Durable resource + ensure
5. ⬜ Durable all + race
6. ⬜ Durable each
7. ⬜ Error handling + suspend + context

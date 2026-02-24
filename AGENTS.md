# Big Idea

Create a version of [Effection](https://github.com/thefrontside/effection) that uses [Durable Streams](https://github.com/durable-streams/durable-streams) under the hood. The implementation lives in a fork of Effection (`~/Repositories/frontside/effection`, branch `durable-internals`) where `run()` always goes through a `DurableReducer` that records/replays effect resolutions to a DurableStream.

## Architecture

- **DurableReducer** replaces Effection's `Reducer` via `ReducerContext` injection
- **Recording**: User-facing effects are recorded to the DurableStream (infrastructure effects like `useCoroutine()`, `useScope()` execute live and are not recorded)
- **Replay**: On resume, stored results are fed back to generators without calling `effect.enter()`
- **Divergence detection**: Mismatched effect descriptions throw `DivergenceError`
- `run()` accepts optional `{ stream }` — defaults to ephemeral `InMemoryDurableStream`

## Repository Layout

- `~/Repositories/frontside/effection` (branch: `durable-internals`) — Fork with DurableReducer implementation + Effection's own test suite as validation
- `~/Repositories/cowboyd/coroutine-transport-protocol` — Consumer/demo project, imports from fork

## Current Status: Phase 1 Complete

- 20 of 27 Effection `run.test.ts` tests pass (7 skipped: need spawn/scope internals)
- 13 durable-specific tests pass (recording, replay, mid-workflow resume, divergence, halt)
- Next: Phase 2 (Api.Scope middleware + scope lifecycle events)

## Implementation Phases

1. ✅ DurableReducer + action/sleep
2. ⬜ Api.Scope middleware + scope lifecycle events
3. ⬜ Durable spawn + generation counter
4. ⬜ Durable resource + ensure
5. ⬜ Durable all + race
6. ⬜ Durable each
7. ⬜ Error handling + suspend + context

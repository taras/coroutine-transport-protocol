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

## Current Status: All Phases Complete

- All 27 Effection `run.test.ts` steps pass
- 139 total test steps passing across 13 test suites (0 failures)
- Effection fork pushed to `taras/effection` (branch: `durable-internals`)
- All 7 implementation phases complete — DurableReducer handles recording, replay, mid-workflow resume, divergence detection, and scope lifecycle for all Effection primitives

### Test Inventory
| Suite | Steps | Coverage |
|-------|-------|----------|
| `run.test.ts` | 27 | Effection core (validation) |
| `durable.test.ts` | 25 | Recording, replay, resume, divergence, halt, workflow:return, spawn resume |
| `durable-scope.test.ts` | 14 | Scope lifecycle, hierarchy, error, halt |
| `durable-all-race.test.ts` | 15 | all(), race(), combined nesting |
| `durable-resource.test.ts` | 12 | Resource, ensure, resource+spawn |
| `durable-each.test.ts` | 11 | each() recording, replay, resume, sync streams |
| `durable-error-suspend-context.test.ts` | 22 | Error replay, suspend replay, context recording/replay |

### Key Design Insights
- **Generator delegation runs during replay**: `yield* stream` (generator delegation) is not an Effection effect — the reducer can only suppress `effect.enter()`, not generator code between yield points. Durable invariants assert no `effect.enter()` calls and no new `effect:yielded` events, not that generators don't run.
- **Context events are informational**: `scope:set`/`scope:delete` are recorded for observability but not rehydrated during replay. Context operations re-execute live as infrastructure effects (`do <set(...)>`).
- **Scope-aware replay**: Per-scope cursors handle concurrent interleaving correctly. Effects from `each()`'s spawned child land in the child scope, while `each.next()` effects land in the caller scope.

## Implementation Phases

1. ✅ DurableReducer + action/sleep
2. ✅ Api.Scope middleware + scope lifecycle events
3. ✅ Durable spawn + workflow:return wiring + divergence validation
4. ✅ Durable resource + ensure
5. ✅ Durable all + race
6. ✅ Durable each
7. ✅ Error handling + suspend + context

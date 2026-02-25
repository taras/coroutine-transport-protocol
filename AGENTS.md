# Coroutine Transport Protocol

Design exploration and demo project for **Durable Effection** — Effection workflows that record and replay effect resolutions using [Durable Streams](https://github.com/durable-streams/durable-streams).

## Architecture

The core implementation has been extracted into [`@effectionx/durably`](https://github.com/thefrontside/effectionx/pull/171). This repo is now a consumer/demo that imports from:

- **`@effectionx/durably`** — `durably()` entry point, `DurableReducer`, `InMemoryDurableStream`, types
- **`@effectionx/durably/http`** — `useDurableStream()` resource for HTTP-backed persistence via `@durable-streams/client`
- **`effection`** — from fork (`~/Repositories/frontside/effection`, branch with PR 1127 experimental exports)
- **`effection/experimental`** — reducer internals needed by durably

## Repository Layout

- `~/Repositories/frontside/effectionx/durably/` — `@effectionx/durably` package (PR #171)
- `~/Repositories/frontside/effection/` — Effection fork (PR #1127 — experimental reducer exports)
- `~/Repositories/cowboyd/coroutine-transport-protocol/` — This repo: demo + documentation

## Current Status

- The `lib/` directory has been removed — `HttpDurableStream` and `useDurableStream` now live in `@effectionx/durably/http`
- `demo/pipeline.ts` imports directly from the package
- `demo/server.ts` runs a Durable Streams test server (unchanged)
- `deno.json` import map points to local checkouts of effection and effectionx

## Key Design Insights

- **DurableReducer** replaces Effection's `Reducer` via `ReducerContext` injection
- **Recording**: User-facing effects are recorded to the DurableStream (infrastructure effects execute live)
- **Replay**: On resume, stored results are fed back to generators without calling `effect.enter()`
- **Divergence detection**: Mismatched effect descriptions throw `DivergenceError`
- **Generator delegation runs during replay**: `yield* stream` is not an Effection effect — generators run their code but don't interact with the outside world
- **The stream IS the checkpoint**: The offset after the last written event is the resume point

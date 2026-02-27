# Coroutine Transport Protocol

Design exploration and demo project for **Durable Effection** — Effection workflows that record and replay effect resolutions using [Durable Streams](https://github.com/durable-streams/durable-streams).

## Architecture

The core implementation has been extracted into [`@effectionx/durably`](https://github.com/thefrontside/effectionx/pull/171). This repo is now a consumer/demo that imports from:

- **`@effectionx/durably`** — `durably()` entry point, `DurableReducer`, `InMemoryDurableStream`, types
- **`@effectionx/durably/http`** — `useDurableStream()` resource for HTTP-backed persistence via `@durable-streams/client`
- **`effection`** — preview from [PR #1127](https://github.com/thefrontside/effection/pull/1127) (experimental reducer exports)

## Setup

This project uses **pnpm** and **Node.js 22+**:

```bash
pnpm install
```

Dependencies use pkg.pr.new previews:
- `effection@https://pkg.pr.new/thefrontside/effection@1127`
- `@effectionx/durably@https://pkg.pr.new/thefrontside/effectionx/@effectionx/durably@171`

## Current Status

- Uses pnpm/Node instead of Deno (no relative paths to local checkouts)
- `demo/pipeline.ts` imports directly from npm packages
- `demo/server.ts` runs a Durable Streams test server using Node HTTP

## Key Design Insights

- **DurableReducer** replaces Effection's `Reducer` via `ReducerContext` injection
- **Recording**: User-facing effects are recorded to the DurableStream (infrastructure effects execute live)
- **Replay**: On resume, stored results are fed back to generators without calling `effect.enter()`
- **Divergence detection**: Mismatched effect descriptions throw `DivergenceError`
- **Generator delegation runs during replay**: `yield* stream` is not an Effection effect — generators run their code but don't interact with the outside world
- **The stream IS the checkpoint**: The offset after the last written event is the resume point

# How the DurableReducer Drives the Generator

> How does the reducer drive the generator? What is the sequence of steps relative to the durable stream?

## The Big Picture

In Effection, every generator-based operation is wrapped in a **Coroutine**. The coroutine has an `iterator` (the generator instance) and a `next` callback. A **Reducer** is the engine that drives generators forward by calling `iterator.next(value)` or `iterator.throw(error)`.

The `DurableReducer` replaces Effection's standard reducer via `ReducerContext` injection. It intercepts the single point where all effects execute, adding recording and replay.

## Step-by-Step Sequence

### 1. Setup — `durable()` is called

```typescript
durable(function*() { ... }, { stream })
```

1. Read all existing entries from the `DurableStream`
2. Build a `ReplayIndex` — indexes events by coroutine ID, maps effect IDs to their resolutions
3. Install scope middleware that intercepts `scope.create()` and `scope.destroy()` to track coroutine lifecycles and emit `spawn`/`close` events
4. Set the `DurableReducer` as the `ReducerContext` for the scope
5. Run the operation — the generator starts

### 2. Generator yields an effect

When your generator does `yield* sleep(1000)`, Effection unpacks it into a raw `yield effect` where `effect` is an object with `{ description, enter }`.

The reducer's `reduce()` method is called with an instruction containing the coroutine and result:

```
reduce([priority, routine, result, validator, "next"])
```

Inside `reduce()`:

1. Call `iterator.next(previousValue)` — this advances the generator to the next `yield`
2. The yielded value is an `Effect` object
3. Call `handleEffect(effect, routine)` — **this is where the magic happens**

### 3. `handleEffect()` — the fork in the road

**Step 3a: Classify the effect**

The reducer tags each effect as either **infrastructure** or **user-facing**:

- **Infrastructure** effects (`useCoroutine()`, `useScope()`, `await resource`, etc.) are Effection's internal plumbing — they always execute live, never recorded
- **User-facing** effects (`sleep(1000)`, `action(...)`, `call(...)`) are your workflow's actual side effects — these are recorded/replayed

**Step 3b: Check the replay index**

For user-facing effects, the reducer peeks at the next recorded `yield` event for this coroutine:

```typescript
replayEvent = replayIndex.peekYield(coroutineId)
```

### 4a. Replay Path (stream has a matching event)

If there's a recorded `yield` event:

1. **Divergence check** — compare `replayEvent.description` with the current `effect.description`. If they don't match → throw `DivergenceError`
2. **Look up the resolution** — find the matching `next` event by `effectId`
3. **Feed the result directly to the generator** — call `routine.next(Ok(recordedValue))` or `routine.next(Err(recordedError))`
4. **Skip `effect.enter()` entirely** — the effect never executes. No timer is set, no fetch is made, no action callback runs

The stream entries consumed look like:

```
offset 0: { type: "yield", coroutineId: "root", effectId: "effect-1", description: "sleep(1000)" }
offset 1: { type: "next",  coroutineId: "root", effectId: "effect-1", status: "ok", value: null }
```

The generator receives `null` (the recorded value) and continues instantly.

### 4b. Live Path (no replay event, or replay exhausted)

If there's no recorded event for this coroutine:

1. **Record `yield`** — append to stream:

   ```json
   { "type": "yield", "coroutineId": "root", "effectId": "effect-1", "description": "sleep(1000)" }
   ```

2. **Wrap `routine.next`** — replace the coroutine's `next` callback with a version that records the resolution before passing it through:

   ```typescript
   wrappedNext = (result) => {
     stream.append({ type: "next", coroutineId, effectId, status, value })
     originalNext(result)
   }
   ```

3. **Execute the effect** — call `effect.enter(routine.next, routine)` which starts the actual timer/fetch/action

4. **When the effect resolves** — the wrapped `next` fires, records the `next` event to the stream, then advances the generator with the real result

### 5. Scope Lifecycle (spawn/close)

The scope middleware intercepts child scope creation and destruction:

- **`scope.create()`** — During replay, reuses the recorded `coroutineId` from `spawn` events. During live execution, assigns a new ID and appends a `spawn` event.
- **`scope.destroy()`** — Emits a `close` event with status (`ok`, `err`, or `cancelled`) and optional value/error.

## The 4-Event Protocol

| Event   | Direction  | Meaning                                          |
|---------|------------|--------------------------------------------------|
| `yield` | Outbound   | Coroutine yielded an effect to the outside world  |
| `next`  | Inbound    | Outside world responded to a yield                |
| `spawn` | Structural | Coroutine spawned a child                         |
| `close` | Terminal   | Coroutine reached a final state (ok/err/cancelled)|

## Concrete Example: Queue Demo

Consider this line from the queue demo:

```typescript
yield* waitForKeypress("Press any key...")
```

`waitForKeypress` uses `once()` from `@effectionx/node`, which internally creates an `action` effect.

### First run (recording)

The user presses "a". The stream records:

```
offset 0: { type: "yield", effectId: "effect-1", description: "action", coroutineId: "root" }
                    // ... waiting for user input ...
offset 1: { type: "next",  effectId: "effect-1", status: "ok", value: "a", coroutineId: "root" }
```

### Second run (replay)

```
Generator yields action effect
  → handleEffect checks replay index
  → Finds yield event at cursor, description matches "action"
  → Finds resolution: { status: "ok", value: "a" }
  → Calls routine.next(Ok("a")) directly
  → Generator instantly receives "a" without waiting for keypress
  → queue.add("a") runs (synchronous, not an effect)
  → Generator advances to next iteration
```

The keypress was never actually waited for. The generator re-ran its code but experienced the same result at the yield point, as if time-traveling through its previous execution.

### What about `queue.add()`?

`queue.add()` is **synchronous** — it's a plain function call, not an Effection effect. The reducer never sees it. This means:

- During recording: `queue.add("a")` executes normally
- During replay: `queue.add("a")` **also executes normally**

The queue is populated in both cases because synchronous code between yield points always runs. Only effects (yield points) are subject to record/replay.

## The Stream IS the Checkpoint

There's no separate checkpoint mechanism. The offset after the last written event is the resume point. When you restart:

1. Read the entire stream → build replay index
2. Start the generator from the beginning
3. Effects that have recordings replay instantly
4. When the replay index is exhausted for a coroutine, that coroutine transitions to live execution
5. New effects are appended to the stream from that point forward

The generator literally re-runs its code but experiences the same results at each yield point. Synchronous code between yield points (like `queue.add()`, `console.log()`, `array.push()`) always executes — only the interaction with the outside world (effects) is controlled by the stream.

## Infrastructure vs User-Facing Effects

The reducer maintains a set of known infrastructure effect descriptions:

```
useCoroutine()
useScope()
trap return
await resource
await winner
await delimiter
await future
await destruction
await callcc
await each done
await each context
```

Plus any description starting with `do <`.

These are Effection's internal coordination mechanisms. They must always execute live because they manage the scope tree, resource lifecycle, and iteration protocol — none of which should be replayed from a previous run.

Everything else is classified as user-facing and goes through the record/replay path.

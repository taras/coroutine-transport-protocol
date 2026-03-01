/**
 * durableEach — durable iteration primitive for Effection workflows.
 *
 * Mirrors Effection's `each()` / `each.next()` pattern but journals
 * every fetch as a DurableEffect, so iteration survives crashes and
 * replays from the journal.
 *
 * Usage:
 *   for (let msg of yield* durableEach("queue", source)) {
 *     yield* durableCall("process", () => process(msg));
 *     yield* durableEach.next();
 *   }
 *
 * See effection-integration.md §12.6 for the full design.
 */

import { createContext, ensure, useScope } from "@effection/effection";
import type { Context, Operation } from "@effection/effection";
import { createDurableEffect } from "./effect.ts";
import { serializeError } from "./serialize.ts";
import type { Json, Workflow } from "./types.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Source of items for durable iteration.
 *
 * Each call to `next()` blocks until the next item is available.
 * Returns `{ value: T }` for an item, `{ done: true }` for exhaustion.
 *
 * The `{ done: true }` wrapper (rather than `T | null`) avoids ambiguity
 * when `null` is a legitimate JSON value from the source.
 */
export interface DurableSource<T extends Json> {
  /** Read the next item, blocking until available. */
  next(): Promise<{ value: T } | { done: true }>;
  /**
   * Teardown — called on cancellation or completion.
   *
   * Must be idempotent: may be called more than once (once from effect
   * teardown during an in-flight fetch, once from scope cleanup via
   * ensure()). Subsequent calls after the first should be no-ops.
   */
  close?(): void;
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/** Sentinel for source exhaustion. Not exported — cannot collide with JSON. */
const DONE: unique symbol = Symbol("durableEach.done");
type ItemOrDone<T> = T | typeof DONE;

/** Type guard for the DONE sentinel. */
function isDone<T>(value: ItemOrDone<T>): value is typeof DONE {
  return value === DONE;
}

/** State stored in Effection context, shared between durableEach and durableEach.next(). */
interface DurableEachState<T extends Json> {
  name: string;
  source: DurableSource<T>;
  current: ItemOrDone<T>;
  advanced: boolean;
}

/**
 * Effection context for sharing state between durableEach() and durableEach.next().
 *
 * Set on the current scope by durableEach(). Read back by durableEach.next().
 * Both run in the same scope, so context visibility is guaranteed.
 */
const DurableEachContext: Context<DurableEachState<Json>> = createContext<
  DurableEachState<Json>
>("durableEach.state");

// ---------------------------------------------------------------------------
// durableEachFetch — shared helper for fetching one item
// ---------------------------------------------------------------------------

/**
 * Fetch a single item from the source (or replay it from the journal).
 *
 * Both the initial fetch (inside durableEach) and subsequent fetches
 * (inside durableEach.next) go through this helper. Same effect
 * description, same journal format, same replay path.
 *
 * Journal shape: Yield event with description { type: "each", name }
 * and result value { value: T } | { done: true }.
 */
function durableEachFetch<T extends Json>(
  name: string,
  source: DurableSource<T>,
): Workflow<ItemOrDone<T>> {
  return (function* () {
    const result = (yield createDurableEffect<{ value: T } | { done: true }>(
      { type: "each", name },
      (resolve) => {
        source.next().then(
          (item) => {
            if ("done" in item) {
              resolve({ status: "ok", value: { done: true } });
            } else {
              resolve({ status: "ok", value: { value: item.value } as Json });
            }
          },
          (error) => {
            resolve({
              status: "err",
              error: serializeError(
                error instanceof Error ? error : new Error(String(error)),
              ),
            });
          },
        );
        // Effect teardown — close the source on cancellation during
        // an in-flight source.next() call.
        return () => source.close?.();
      },
    )) as { value: T } | { done: true };

    if ("done" in result) return DONE;
    return result.value;
  })();
}

// ---------------------------------------------------------------------------
// durableEach — initial fetch + returns synchronous iterable
// ---------------------------------------------------------------------------

/**
 * Durable iteration over a DurableSource.
 *
 * Fetches the first item (or replays it), stores state in an Effection
 * context, and returns a synchronous iterable for use with `for...of`.
 *
 * Each iteration must call `yield* durableEach.next()` at the end of
 * the loop body to checkpoint progress and pre-fetch the next item.
 * Failing to do so triggers a runtime error (advance guard).
 *
 * @param name Stable name for the iteration — used in journal descriptions
 * @param source Async source of items
 */
function* _durableEach<T extends Json>(
  name: string,
  source: DurableSource<T>,
): Operation<Iterable<T>> {
  // Get scope and register cleanup BEFORE the first fetch, so that
  // cancellation during the fetch still triggers source teardown.
  const scope = yield* useScope();

  // Guard against nested durableEach in the same scope — the single
  // context slot would clobber the outer iteration's state.
  if (scope.get(DurableEachContext) !== undefined) {
    throw new Error(
      `durableEach("${name}"): cannot nest durableEach calls in the same ` +
        `scope. Use a child scope (e.g., via spawn) for inner iterations.`,
    );
  }

  // Register source teardown on scope exit — ensures cleanup even if
  // the for...of loop breaks or the scope is cancelled without an
  // active effect. Safe to call alongside effect-level teardown
  // because DurableSource.close() must be idempotent.
  yield* ensure(() => { source.close?.(); });

  // Durable fetch of first item — journaled as a Yield event.
  // ensure() is already registered, so cancellation here is safe.
  const first: ItemOrDone<T> = yield* durableEachFetch(name, source);

  // Store state in Effection context for durableEach.next() to access
  const state: DurableEachState<T> = {
    name,
    source,
    current: first,
    advanced: true, // first item was just fetched
  };
  scope.set(DurableEachContext, state as DurableEachState<Json>);

  // Return a synchronous iterable. The iterator generator checks
  // the shared state on each re-entry.
  return {
    *[Symbol.iterator]() {
      while (!isDone(state.current)) {
        // Advance guard: detect missing yield* durableEach.next()
        if (!state.advanced) {
          throw new Error(
            `durableEach("${name}"): yield* durableEach.next() must be ` +
              `called before the next iteration. Each loop body must end ` +
              `with yield* durableEach.next() to checkpoint progress and ` +
              `fetch the next item.`,
          );
        }
        state.advanced = false;
        yield state.current as T;
      }
    },
  };
}

// ---------------------------------------------------------------------------
// durableEach.next — static method to advance iteration
// ---------------------------------------------------------------------------

/**
 * Advance the current durable iteration.
 *
 * Reads state from the Effection context (set by durableEach),
 * fetches the next item (or replays it), and updates the shared state.
 * Must be called at the end of each loop body iteration.
 */
function* _durableEachNext<T extends Json>(): Operation<void> {
  const scope = yield* useScope();
  const state = scope.expect(DurableEachContext) as DurableEachState<T>;
  // Fetch next item first, then mark advanced. If the fetch throws
  // (source error), advanced stays false and re-entry triggers the
  // advance guard — preventing stale current from being re-yielded.
  state.current = yield* durableEachFetch<T>(state.name, state.source);
  state.advanced = true;
}

// ---------------------------------------------------------------------------
// Public API — durableEach with static .next() method
// ---------------------------------------------------------------------------

/**
 * Durable iteration over a DurableSource.
 *
 * Returns an Operation that fetches the first item and yields a
 * synchronous iterable. Use with `for...of`:
 *
 * ```typescript
 * for (let msg of yield* durableEach("queue", source)) {
 *   yield* durableCall("process", () => process(msg));
 *   yield* durableEach.next();
 * }
 * ```
 *
 * @param name Stable name for the iteration
 * @param source Async source of items
 */
export const durableEach: {
  <T extends Json>(
    name: string,
    source: DurableSource<T>,
  ): Operation<Iterable<T>>;
  next<T extends Json>(): Operation<void>;
} = Object.assign(_durableEach, { next: _durableEachNext });

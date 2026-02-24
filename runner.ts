import { run, Ok, Err, type Effect, type Operation, type Task, type Result, type Scope } from "effection";
import type {
  DurableStream,
  Json,
  SerializedError,
  StreamEntry,
} from "./types.ts";

// ── Helpers ────────────────────────────────────────────────────────

let nextId = 0;

function genScopeId(): string {
  return `s${nextId++}`;
}

function genEffectId(): string {
  return `e${nextId++}`;
}

/** Reset ID counters (for testing). */
export function resetIds(): void {
  nextId = 0;
}

function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return { name: "Error", message: String(error) };
}

function deserializeError(err: SerializedError): Error {
  const error = new Error(err.message);
  error.name = err.name;
  if (err.stack) error.stack = err.stack;
  return error;
}

function isJsonSafe(value: unknown): value is Json {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return true;
  if (Array.isArray(value)) return value.every(isJsonSafe);
  if (typeof value === "object") {
    return Object.values(value as Record<string, unknown>).every(isJsonSafe);
  }
  return false;
}

// ── Scope Ref Sentinel ─────────────────────────────────────────────
//
// When a Scope object is resolved (e.g., from useScope()), we write
// a sentinel { __scopeRef: scopeId } into the stream. On replay,
// the runner detects this sentinel and resolves with the real live
// Scope object instead.

interface ScopeRefSentinel {
  __scopeRef: string;
}

function isScopeRefSentinel(value: unknown): value is ScopeRefSentinel {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "__scopeRef" in (value as Record<string, unknown>) &&
    typeof (value as Record<string, unknown>).__scopeRef === "string"
  );
}

// ── Durable Execution Context ──────────────────────────────────────
//
// Shared mutable state for a single durableRun invocation.
// Tracks the stream, replay state, and scope-to-ID mappings.
// Both the root and child scopes share this context.

interface DurableContext {
  stream: DurableStream;
  streamComplete: boolean;
  /** Map Effection Scope objects → durable scopeIds */
  scopeIds: WeakMap<Scope, string>;
  /** Per-scope replay queues: scopeId → ordered list of effect entries */
  replayQueues: Map<string, ReplayEntry[]>;
  /** Per-scope replay cursors: scopeId → current index */
  replayCursors: Map<string, number>;
  /** Per-parent child scope replay: parentScopeId → ordered child scopeIds */
  childScopeQueues: Map<string, string[]>;
  /** Per-parent child scope cursors: parentScopeId → current index */
  childScopeCursors: Map<string, number>;
}

interface ReplayEntry {
  effectId: string;
  description: string;
  result: Result<Json>;
}

// ── Build replay state from stream ─────────────────────────────────

function buildReplayQueues(entries: StreamEntry[]): Map<string, ReplayEntry[]> {
  const queues = new Map<string, ReplayEntry[]>();

  // Index resolutions by effectId
  const resolutions = new Map<string, Result<Json>>();
  for (const { event } of entries) {
    if (event.type === "effect:resolved") {
      resolutions.set(event.effectId, Ok(event.value));
    } else if (event.type === "effect:errored") {
      resolutions.set(event.effectId, Err(deserializeError(event.error)));
    }
  }

  // Walk effect:yielded events, pair with resolutions, group by scope
  for (const { event } of entries) {
    if (event.type === "effect:yielded") {
      const resolution = resolutions.get(event.effectId);
      if (resolution) {
        if (!queues.has(event.scopeId)) {
          queues.set(event.scopeId, []);
        }
        queues.get(event.scopeId)!.push({
          effectId: event.effectId,
          description: event.description,
          result: resolution,
        });
      }
    }
  }

  return queues;
}

function buildChildScopeQueues(entries: StreamEntry[]): Map<string, string[]> {
  const queues = new Map<string, string[]>();
  for (const { event } of entries) {
    if (event.type === "scope:created" && event.parentScopeId) {
      if (!queues.has(event.parentScopeId)) {
        queues.set(event.parentScopeId, []);
      }
      queues.get(event.parentScopeId)!.push(event.scopeId);
    }
  }
  return queues;
}

function isStreamComplete(entries: StreamEntry[]): boolean {
  if (entries.some(({ event }) => event.type === "workflow:return")) {
    return true;
  }
  let rootScopeId: string | undefined;
  for (const { event } of entries) {
    if (event.type === "scope:created" && !event.parentScopeId) {
      rootScopeId = event.scopeId;
      break;
    }
  }
  if (rootScopeId) {
    return entries.some(({ event }) =>
      event.type === "scope:destroyed" &&
      event.scopeId === rootScopeId &&
      !event.result.ok
    );
  }
  return false;
}

// ── Divergence Error ───────────────────────────────────────────────

export class DivergenceError extends Error {
  constructor(
    public readonly index: number,
    public readonly expected: string,
    public readonly actual: string,
  ) {
    super(
      `Effect divergence at replay index ${index}: expected '${expected}', got '${actual}'`,
    );
    this.name = "DivergenceError";
  }
}

// ── Live-Only Placeholder ──────────────────────────────────────────
//
// When an effect resolves with a non-JSON-serializable value (e.g.,
// an Iterable, Coroutine, or other complex object), we store a
// __liveOnly sentinel in the stream. On replay, the runner detects
// this sentinel and lets the real effect's enter() run instead of
// feeding back a placeholder — the value must be re-materialized live.

interface LiveOnlyPlaceholder {
  __liveOnly: true;
  __type: string;
  __toString: string;
}

function isLiveOnlyPlaceholder(value: unknown): value is LiveOnlyPlaceholder {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "__liveOnly" in (value as Record<string, unknown>) &&
    (value as Record<string, unknown>).__liveOnly === true
  );
}

// ── toJson ─────────────────────────────────────────────────────────

function toJson(value: unknown, ctx: DurableContext): Json {
  if (value === undefined) return null;
  // Check if this is a Scope object we're tracking
  if (value !== null && typeof value === "object") {
    const scopeId = ctx.scopeIds.get(value as Scope);
    if (scopeId !== undefined) {
      return { __scopeRef: scopeId };
    }
  }
  if (isJsonSafe(value)) return value;
  return { __liveOnly: true, __type: typeof value, __toString: String(value) } as Json;
}

// ── Wrap Scope ─────────────────────────────────────────────────────
//
// Create a Proxy around a real Scope that intercepts scope.run()
// to wrap child operations with durable instrumentation.

function wrapScope(
  realScope: Scope,
  parentScopeId: string,
  ctx: DurableContext,
): Scope {
  return new Proxy(realScope, {
    get(target, prop, receiver) {
      if (prop === "run") {
        return <T>(childOp: () => Operation<T>): Task<T> => {
          // During replay, reuse stored child scope IDs to stay
          // aligned with the stream. During live execution, generate
          // new IDs.
          const childQueue = ctx.childScopeQueues.get(parentScopeId) ?? [];
          if (!ctx.childScopeCursors.has(parentScopeId)) {
            ctx.childScopeCursors.set(parentScopeId, 0);
          }
          const childCursor = ctx.childScopeCursors.get(parentScopeId)!;

          let childScopeId: string;
          if (childCursor < childQueue.length) {
            // Replay: reuse stored child scope ID
            childScopeId = childQueue[childCursor];
            ctx.childScopeCursors.set(parentScopeId, childCursor + 1);
          } else {
            // Live: generate new ID
            childScopeId = genScopeId();
          }

          if (!ctx.streamComplete) {
            ctx.stream.append({
              type: "scope:created",
              scopeId: childScopeId,
              parentScopeId,
            });
          }

          const wrappedChildOp = wrapOperation(childOp, childScopeId, ctx);
          const task = target.run(wrappedChildOp);

          return task;
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

// ── Wrap Operation ─────────────────────────────────────────────────
//
// Takes an operation and returns a new operation whose generator
// iterator intercepts every Effect:
//   - Replay phase: feed stored result via synthetic Effect
//   - Live phase: wrap real Effect to record resolution to stream

function wrapOperation<T>(
  operation: () => Operation<T>,
  scopeId: string,
  ctx: DurableContext,
): () => Operation<T> {
  return (): Operation<T> => ({
    [Symbol.iterator](): Iterator<Effect<unknown>, T, unknown> {
      const iter = operation()[Symbol.iterator]();
      const replayQueue = ctx.replayQueues.get(scopeId) ?? [];
      if (!ctx.replayCursors.has(scopeId)) {
        ctx.replayCursors.set(scopeId, 0);
      }

      // If any effect in this scope's replay queue resolved with
      // a non-serializable value, skip replay for the entire scope.
      // Re-executing individual effects while replaying others
      // causes divergence (Effection internals take different code
      // paths depending on whether they receive real objects vs
      // placeholders). The all-or-nothing approach is safer:
      // either replay all effects (when all results are serializable)
      // or re-execute all effects (when any result is non-serializable).
      //
      // Exception: useCoroutine() produces non-serializable Coroutine
      // objects, but Effection's internal machinery (resource, all,
      // race, etc.) tolerates receiving placeholder objects for these.
      // So useCoroutine() __liveOnly results don't trigger a scope
      // replay skip.
      const hasLiveOnlyResults = replayQueue.some(
        (entry) =>
          entry.result.ok &&
          isLiveOnlyPlaceholder(entry.result.value) &&
          entry.description !== "useCoroutine()",
      );
      const effectiveReplayQueue = hasLiveOnlyResults ? [] : replayQueue;

      return {
        next(value?: unknown): IteratorResult<Effect<unknown>, T> {
          let result: IteratorResult<Effect<unknown>, T>;
          try {
            result = iter.next(value);
          } catch (e) {
            if (!ctx.streamComplete) {
              ctx.stream.append({
                type: "scope:destroyed",
                scopeId,
                result: { ok: false, error: serializeError(e) },
              });
              // Only close the stream if this is the root scope failing
              // (Child scope failures don't close the stream)
            }
            throw e;
          }

          if (result.done) {
            if (!ctx.streamComplete) {
              ctx.stream.append({
                type: "workflow:return",
                scopeId,
                value: toJson(result.value, ctx),
              });
              ctx.stream.append({
                type: "scope:destroyed",
                scopeId,
                result: { ok: true },
              });
            }
            return result;
          }

          const effect = result.value;
          const cursor = ctx.replayCursors.get(scopeId)!;

          // ── Replay phase ──
          if (cursor < effectiveReplayQueue.length) {
            const entry = effectiveReplayQueue[cursor];

            if (entry.description !== effect.description) {
              throw new DivergenceError(cursor, entry.description, effect.description);
            }

            ctx.replayCursors.set(scopeId, cursor + 1);

            // If the stored result is a non-serializable placeholder,
            // fall through to live execution: the value must be
            // re-materialized by calling the real effect's enter().
            // We advance the cursor (above) to stay aligned, but
            // pass through the original effect without recording
            // duplicate events (they already exist in the stream).
            //
            // Exception: useCoroutine() returns non-serializable
            // Coroutine objects that feed into Effection's internal
            // resource/trap machinery. Re-executing useCoroutine()
            // live produces a real Coroutine that causes resource()
            // to take a different code path, changing the effect
            // sequence. So useCoroutine() must be replayed with the
            // placeholder — Effection internals tolerate it.
            const syntheticEffect: Effect<unknown> = {
              description: effect.description,
              enter(resolve, routine) {
                // If the stored result is a scope ref sentinel,
                // resolve with a wrapped real Scope from the routine
                if (entry.result.ok) {
                  const val = entry.result.value;
                  if (val !== null && val !== undefined && isScopeRefSentinel(val as Json)) {
                    const wrappedScope = wrapScope(routine.scope, scopeId, ctx);
                    ctx.scopeIds.set(wrappedScope, scopeId);
                    ctx.scopeIds.set(routine.scope, scopeId);
                    resolve(Ok(wrappedScope));
                    return (discarded) => discarded(Ok());
                  }
                }
                resolve(entry.result);
                return (discarded) => discarded(Ok());
              },
            };

            return { done: false, value: syntheticEffect };
          }

          // ── Live phase ──
          const effectId = genEffectId();
          if (!ctx.streamComplete) {
            ctx.stream.append({
              type: "effect:yielded",
              scopeId,
              effectId,
              description: effect.description,
            });
          }

          const wrappedEffect: Effect<unknown> = {
            description: effect.description,
            enter(resolve, routine) {
              const wrappedResolve: typeof resolve = (result) => {
                if (!ctx.streamComplete) {
                  if (result.ok) {
                    // If this is a useScope() resolution, register the scope
                    // and wrap it so child scope.run() calls are intercepted
                    let valueToSerialize = result.value;
                    if (
                      effect.description === "useScope()" &&
                      result.value !== null &&
                      typeof result.value === "object"
                    ) {
                      const realScope = result.value as Scope;
                      ctx.scopeIds.set(realScope, scopeId);
                      const wrappedScope = wrapScope(realScope, scopeId, ctx);
                      ctx.scopeIds.set(wrappedScope, scopeId);
                      // Replace the result so the generator gets our wrapped Scope
                      resolve(Ok(wrappedScope));
                      ctx.stream.append({
                        type: "effect:resolved",
                        effectId,
                        value: toJson(wrappedScope, ctx),
                      });
                      return;
                    }
                    ctx.stream.append({
                      type: "effect:resolved",
                      effectId,
                      value: toJson(valueToSerialize, ctx),
                    });
                  } else {
                    ctx.stream.append({
                      type: "effect:errored",
                      effectId,
                      error: serializeError(result.error),
                    });
                  }
                }
                resolve(result);
              };
              return effect.enter(wrappedResolve, routine);
            },
          };

          return { done: false, value: wrappedEffect };
        },

        return(value?: unknown): IteratorResult<Effect<unknown>, T> {
          if (iter.return) {
            return iter.return(value as T);
          }
          return { done: true, value: value as T };
        },

        throw(error?: unknown): IteratorResult<Effect<unknown>, T> {
          if (iter.throw) {
            try {
              return iter.throw(error);
            } catch (e) {
              if (!ctx.streamComplete) {
                ctx.stream.append({
                  type: "scope:destroyed",
                  scopeId,
                  result: { ok: false, error: serializeError(e) },
                });
              }
              throw e;
            }
          }
          throw error;
        },
      };
    },
  });
}

// ── Durable Runner ─────────────────────────────────────────────────

export function durableRun<T>(
  stream: DurableStream,
  operation: () => Operation<T>,
): Task<T> {
  const existingEntries = stream.read();
  const streamComplete = isStreamComplete(existingEntries);

  const ctx: DurableContext = {
    stream,
    streamComplete,
    scopeIds: new WeakMap(),
    replayQueues: buildReplayQueues(existingEntries),
    replayCursors: new Map(),
    childScopeQueues: buildChildScopeQueues(existingEntries),
    childScopeCursors: new Map(),
  };

  // Determine root scopeId: reuse from stream or generate new
  let rootScopeId: string | undefined;
  for (const { event } of existingEntries) {
    if (event.type === "scope:created" && !event.parentScopeId) {
      rootScopeId = event.scopeId;
      break;
    }
  }
  if (!rootScopeId) {
    rootScopeId = genScopeId();
    stream.append({ type: "scope:created", scopeId: rootScopeId });
  }

  // Wrap the root operation and close the stream when done
  const rootOp = wrapOperation(operation, rootScopeId, ctx);

  // Wrap again to handle stream closing (only for root scope)
  const closingOp = (): Operation<T> => ({
    [Symbol.iterator](): Iterator<Effect<unknown>, T, unknown> {
      const iter = rootOp()[Symbol.iterator]();
      return {
        next(value?: unknown): IteratorResult<Effect<unknown>, T> {
          let result: IteratorResult<Effect<unknown>, T>;
          try {
            result = iter.next(value);
          } catch (e) {
            if (!ctx.streamComplete && !stream.closed) {
              stream.close();
            }
            throw e;
          }
          if (result.done && !ctx.streamComplete) {
            stream.close();
          }
          return result;
        },
        return(value?: unknown): IteratorResult<Effect<unknown>, T> {
          return iter.return ? iter.return(value as T) : { done: true, value: value as T };
        },
        throw(error?: unknown): IteratorResult<Effect<unknown>, T> {
          try {
            return iter.throw ? iter.throw(error) : (() => { throw error; })();
          } catch (e) {
            if (!ctx.streamComplete && !stream.closed) {
              stream.close();
            }
            throw e;
          }
        },
      };
    },
  });

  return run(closingOp);
}

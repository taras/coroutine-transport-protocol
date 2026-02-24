import { run, Ok, Err, type Effect, type Operation, type Task, type Result } from "effection";
import type {
  DurableStream,
  Json,
  SerializedError,
  StreamEntry,
} from "./types.ts";

// ── Helpers ────────────────────────────────────────────────────────

let nextId = 0;

function genScopeId(): string {
  return String(nextId++);
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

function toJson(value: unknown): Json {
  if (value === undefined) return null;
  if (isJsonSafe(value)) return value;
  return { __type: typeof value, __toString: String(value) } as Json;
}

// ── Replay Queue ───────────────────────────────────────────────────

interface ReplayEntry {
  effectId: string;
  description: string;
  result: Result<Json>;
}

function buildReplayQueue(entries: StreamEntry[]): ReplayEntry[] {
  const queue: ReplayEntry[] = [];

  const resolutions = new Map<string, Result<Json>>();
  for (const { event } of entries) {
    if (event.type === "effect:resolved") {
      resolutions.set(event.effectId, Ok(event.value));
    } else if (event.type === "effect:errored") {
      resolutions.set(event.effectId, Err(deserializeError(event.error)));
    }
  }

  for (const { event } of entries) {
    if (event.type === "effect:yielded") {
      const resolution = resolutions.get(event.effectId);
      if (resolution) {
        queue.push({
          effectId: event.effectId,
          description: event.description,
          result: resolution,
        });
      }
    }
  }

  return queue;
}

/** Check if the stream represents a completed execution.
 *  Complete means either:
 *  - workflow:return is present (successful completion), or
 *  - scope:destroyed with ok:false for the root scope (errored completion)
 */
function isStreamComplete(entries: StreamEntry[]): boolean {
  if (entries.some(({ event }) => event.type === "workflow:return")) {
    return true;
  }
  // Find the root scope (first scope:created without parentScopeId)
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

// ── Durable Runner ─────────────────────────────────────────────────
//
// Executes an Effection operation while recording all effect
// resolutions to a DurableStream.
//
// If the stream already contains events (from a previous execution),
// stored effect results are replayed: the generator is fast-forwarded
// through effects whose results are known, without calling enter().
// Once stored events are exhausted, live execution continues and
// new events are appended.
//
// If the stream is already complete (has workflow:return), the
// entire execution is pure replay — no new events are written.

export function durableRun<T>(
  stream: DurableStream,
  operation: () => Operation<T>,
): Task<T> {
  const existingEntries = stream.read();
  const replayQueue = buildReplayQueue(existingEntries);
  const streamComplete = isStreamComplete(existingEntries);
  let replayIndex = 0;

  // Determine scopeId: reuse from stream or generate new
  let scopeId: string | undefined;
  for (const { event } of existingEntries) {
    if (event.type === "scope:created" && !event.parentScopeId) {
      scopeId = event.scopeId;
      break;
    }
  }
  if (!scopeId) {
    scopeId = genScopeId();
    stream.append({ type: "scope:created", scopeId });
  }

  const rootScopeId = scopeId;

  const wrappedOperation = (): Operation<T> => {
    return {
      [Symbol.iterator](): Iterator<Effect<unknown>, T, unknown> {
        const iter = operation()[Symbol.iterator]();

        return {
          next(value?: unknown): IteratorResult<Effect<unknown>, T> {
            let result: IteratorResult<Effect<unknown>, T>;
            try {
              result = iter.next(value);
            } catch (e) {
              // Generator threw synchronously (e.g., throw before first yield)
              if (!streamComplete) {
                stream.append({
                  type: "scope:destroyed",
                  scopeId: rootScopeId,
                  result: { ok: false, error: serializeError(e) },
                });
                stream.close();
              }
              throw e;
            }

            if (result.done) {
              // Generator returned
              if (!streamComplete) {
                stream.append({
                  type: "workflow:return",
                  scopeId: rootScopeId,
                  value: toJson(result.value),
                });
                stream.append({
                  type: "scope:destroyed",
                  scopeId: rootScopeId,
                  result: { ok: true },
                });
                stream.close();
              }
              return result;
            }

            const effect = result.value;

            // ── Replay phase ──
            if (replayIndex < replayQueue.length) {
              const entry = replayQueue[replayIndex];

              if (entry.description !== effect.description) {
                throw new DivergenceError(
                  replayIndex,
                  entry.description,
                  effect.description,
                );
              }

              replayIndex++;

              const syntheticEffect: Effect<unknown> = {
                description: effect.description,
                enter(resolve) {
                  resolve(entry.result);
                  return (discarded) => discarded(Ok());
                },
              };

              return { done: false, value: syntheticEffect };
            }

            // ── Live phase ──
            const effectId = genEffectId();
            stream.append({
              type: "effect:yielded",
              scopeId: rootScopeId,
              effectId,
              description: effect.description,
            });

            const wrappedEffect: Effect<unknown> = {
              description: effect.description,
              enter(resolve, routine) {
                const wrappedResolve: typeof resolve = (result) => {
                  if (result.ok) {
                    stream.append({
                      type: "effect:resolved",
                      effectId,
                      value: toJson(result.value),
                    });
                  } else {
                    stream.append({
                      type: "effect:errored",
                      effectId,
                      error: serializeError(result.error),
                    });
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
                if (!streamComplete) {
                  stream.append({
                    type: "scope:destroyed",
                    scopeId: rootScopeId,
                    result: { ok: false, error: serializeError(e) },
                  });
                  stream.close();
                }
                throw e;
              }
            }
            throw error;
          },
        };
      },
    };
  };

  return run(wrappedOperation);
}

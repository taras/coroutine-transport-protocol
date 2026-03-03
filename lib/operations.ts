/**
 * Workflow-enabled effects — durable equivalents of Effection's built-in
 * operations.
 *
 * Each returns a Workflow<T> (a generator that yields a single DurableEffect).
 * These are the building blocks for durable workflows.
 *
 * See integration doc §6.
 */

import { createDurableEffect, type DurableEffectOptions } from "./effect.ts";
import { serializeError } from "./serialize.ts";
import type { Json, Result, Workflow } from "./types.ts";

/**
 * Options for durableCall.
 */
export interface DurableCallOptions<T> {
  /**
   * Generate validation metadata for replay guards.
   *
   * Called after the async function resolves, before persisting the Yield
   * event. The returned object is stored in the event's `meta` field and
   * passed to replay guards on subsequent runs.
   *
   * Example: For a file read, return `{ filePath, fileSHA: sha256(content) }`
   */
  meta?: (value: T) => Record<string, Json>;
}

/**
 * Durable sleep — pauses the workflow for `ms` milliseconds.
 *
 * During replay, resolves synchronously with the stored result.
 * During live execution, uses setTimeout and persists the Yield event.
 *
 * Description: { type: "sleep", name: "sleep" }
 */
export function* durableSleep(ms: number): Workflow<void> {
  yield createDurableEffect<void>(
    { type: "sleep", name: "sleep" },
    (resolve) => {
      const id = setTimeout(() => resolve({ status: "ok" }), ms);
      return () => clearTimeout(id);
    },
  );
}

/**
 * Durable call — wraps an async function for durable execution.
 *
 * The function is called during live execution; its resolved value is
 * serialized and persisted. During replay, the stored value is returned
 * without calling the function.
 *
 * Description: { type: "call", name }
 *
 * IMPORTANT: The function's return value must be JSON-serializable.
 *
 * @param name Stable identifier for the effect (used for divergence detection)
 * @param fn Async function to execute (only called during live execution)
 * @param options Optional configuration including metadata generation
 */
export function* durableCall<T extends Json>(
  name: string,
  fn: () => Promise<T>,
  options?: DurableCallOptions<T>,
): Workflow<T> {
  // Convert the typed meta function to the untyped version expected by
  // createDurableEffect
  const effectOptions: DurableEffectOptions | undefined = options?.meta
    ? { meta: (value) => options.meta!(value as T) }
    : undefined;

  return (yield createDurableEffect<T>(
    { type: "call", name },
    (resolve) => {
      fn().then(
        (value) => resolve({ status: "ok", value: value as Json }),
        (error) => {
          const result: Result = {
            status: "err",
            error: serializeError(
              error instanceof Error ? error : new Error(String(error)),
            ),
          };
          resolve(result);
        },
      );
      return () => {};
    },
    effectOptions,
  )) as T;
}

/**
 * Durable action — generic effect with a custom executor.
 *
 * Like Effection's action(), but durable. The executor receives resolve/reject
 * callbacks and returns a teardown function.
 *
 * Description: { type: "action", name }
 */
export function* durableAction<T extends Json>(
  name: string,
  executor: (
    resolve: (value: T) => void,
    reject: (error: Error) => void,
  ) => () => void,
): Workflow<T> {
  return (yield createDurableEffect<T>(
    { type: "action", name },
    (protocolResolve, reject) => {
      return executor(
        (value: T) =>
          protocolResolve({ status: "ok", value: value as Json }),
        reject,
      );
    },
  )) as T;
}

/**
 * Version gate — enables safe code evolution for durable workflows.
 *
 * During live execution, resolves with `maxVersion`. During replay, the
 * stored version determines which code path the workflow takes.
 *
 * Description: { type: "version_gate", name }
 *
 * See spec §9.
 */
export function* versionCheck(
  name: string,
  opts: { minVersion: number; maxVersion: number },
): Workflow<number> {
  return (yield createDurableEffect<number>(
    { type: "version_gate", name },
    (resolve) => {
      resolve({ status: "ok", value: opts.maxVersion });
      return () => {};
    },
  )) as number;
}

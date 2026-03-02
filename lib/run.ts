/**
 * durableRun — entry point for durable workflow execution.
 *
 * An Operation<T> that reads the event stream, builds the ReplayIndex,
 * sets DurableContext on the current scope, runs the workflow, and emits
 * a Close event when the workflow terminates.
 *
 * Because durableRun is an Operation, it inherits the caller's Effection
 * scope — including any middleware installed via scope.around(). This is
 * how divergence policy overrides work: the caller installs middleware
 * before yield*-ing into durableRun. See DEC-032.
 *
 * See integration doc §10, protocol spec §4.
 */

import { call, useScope } from "@effection/effection";
import type { Operation } from "@effection/effection";
import { DurableCtx } from "./context.ts";
import { EarlyReturnDivergenceError } from "./errors.ts";
import { ReplayIndex } from "./replay-index.ts";
import { deserializeError, serializeError } from "./serialize.ts";
import type { DurableStream } from "./stream.ts";
import type { Close, Json, Workflow } from "./types.ts";

/**
 * Options for durableRun.
 */
export interface DurableRunOptions {
  /** The durable stream to read from and append to. */
  stream: DurableStream;
  /** Coroutine ID for the root workflow. Defaults to "root". */
  coroutineId?: string;
}

/**
 * Execute a durable workflow.
 *
 * 1. Reads all events from the stream and builds a ReplayIndex.
 * 2. Sets DurableContext on the current scope (inherited from caller).
 * 3. Runs the workflow — replayed effects resolve synchronously from
 *    the index; live effects execute and persist before resuming.
 * 4. On completion, appends a Close event to the stream.
 * 5. On error, appends a Close(err) event.
 *
 * Returns the workflow's result value.
 *
 * Usage:
 *   // From async code (standalone):
 *   await run(() => durableRun(workflow, { stream }));
 *
 *   // From inside an Effection generator (inherits scope):
 *   const result = yield* durableRun(workflow, { stream });
 */
export function* durableRun<T extends Json | void>(
  workflow: () => Workflow<T> | Operation<T>,
  options: DurableRunOptions,
): Operation<T> {
  const { stream, coroutineId = "root" } = options;

  // Read all events and build replay index
  const events = yield* call(() => stream.readAll());
  const replayIndex = new ReplayIndex(events);

  // If the root coroutine already has a Close event in the journal,
  // the workflow completed in a previous run. Return the stored result
  // directly without re-running the workflow.
  if (replayIndex.hasClose(coroutineId)) {
    const closeEvent = replayIndex.getClose(coroutineId)!;
    if (closeEvent.result.status === "ok") {
      return closeEvent.result.value as T;
    } else if (closeEvent.result.status === "err") {
      throw deserializeError(closeEvent.result.error);
    } else {
      throw new Error("Workflow was cancelled");
    }
  }

  // Inherit the caller's scope — middleware (e.g., Divergence) is
  // already installed by the caller before yield*-ing into durableRun.
  const scope = yield* useScope();

  scope.set(DurableCtx, {
    replayIndex,
    stream,
    coroutineId,
    childCounter: 0,
  });

  let closeEvent: Close | undefined;

  try {
    // Workflow<T> is structurally assignable to Operation<T>, so
    // yield* accepts it directly — no cast needed.
    const result: T = yield* workflow();

    // §6.3: Check for early return divergence.
    // If the generator returned but the replay index has unconsumed yields,
    // the workflow has diverged. Skip this check when replay has been
    // disabled (run-live mode) — the workflow intentionally diverged and
    // the Divergence API already approved it.
    if (!replayIndex.isReplayDisabled(coroutineId)) {
      const cursor = replayIndex.getCursor(coroutineId);
      const totalYields = replayIndex.yieldCount(coroutineId);
      if (cursor < totalYields) {
        throw new EarlyReturnDivergenceError(coroutineId, cursor, totalYields);
      }
    }

    // Record Close(ok) — will be appended in finally
    closeEvent = {
      type: "close",
      coroutineId,
      result: { status: "ok", value: result as Json },
    };

    return result;
  } catch (error) {
    // Record Close(err) — will be appended in finally
    closeEvent = {
      type: "close",
      coroutineId,
      result: {
        status: "err",
        error: serializeError(
          error instanceof Error ? error : new Error(String(error)),
        ),
      },
    };

    throw error;
  } finally {
    // Append Close event — best-effort. If the append itself fails
    // (e.g., stream is in a fatal state), we swallow the error so it
    // doesn't mask the original workflow error.
    if (closeEvent) {
      try {
        yield* call(() => stream.append(closeEvent!));
      } catch {
        // Close event append failed — the original error is more important.
      }
    }
  }
}

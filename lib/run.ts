/**
 * durableRun — entry point for durable workflow execution.
 *
 * Creates an Effection scope, reads the event stream, builds the ReplayIndex,
 * sets DurableContext on the scope, runs the workflow, and emits a Close event
 * when the workflow terminates.
 *
 * See integration doc §10, protocol spec §4.
 */

import { createScope } from "@effection/effection";
import type { Operation, Scope } from "@effection/effection";
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
  /**
   * Optional setup callback invoked with the Effection scope before the
   * workflow runs. Use this to install middleware (e.g., divergence
   * policy overrides via `scope.around(Divergence, ...)`).
   *
   * The callback receives the raw Scope, not a generator context, so
   * only synchronous scope methods (set, around) are available.
   */
  setup?: (scope: Scope) => void;
}

/**
 * Execute a durable workflow.
 *
 * 1. Reads all events from the stream and builds a ReplayIndex.
 * 2. Creates an Effection scope with DurableContext.
 * 3. Runs the workflow — replayed effects resolve synchronously from
 *    the index; live effects execute and persist before resuming.
 * 4. On completion, appends a Close event to the stream.
 * 5. On error, appends a Close(err) event.
 *
 * Returns the workflow's result value.
 */
export async function durableRun<T extends Json | void>(
  workflow: () => Workflow<T> | Operation<T>,
  options: DurableRunOptions,
): Promise<T> {
  const { stream, coroutineId = "root", setup } = options;

  // Read all events and build replay index
  const events = await stream.readAll();
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

  // Create an Effection scope and set DurableContext
  const [scope, destroy] = createScope();

  scope.set(DurableCtx, {
    replayIndex,
    stream,
    coroutineId,
    childCounter: 0,
  });

  // Allow callers to install middleware or configure the scope before
  // the workflow executes. This is the extension point for divergence
  // policy overrides (DEC-031).
  if (setup) {
    setup(scope);
  }

  try {
    // Workflow<T> is structurally assignable to Operation<T>, so
    // scope.run() accepts it directly — no cast needed.
    const task = scope.run(workflow);
    const result = await task;

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

    // Append Close(ok) event
    const closeEvent: Close = {
      type: "close",
      coroutineId,
      result: { status: "ok", value: result as Json },
    };
    await stream.append(closeEvent);

    return result;
  } catch (error) {
    // Append Close(err) event — best-effort. If the append itself fails
    // (e.g., stream is in a fatal state), we still throw the original
    // workflow error so it isn't masked by the append failure.
    const closeEvent: Close = {
      type: "close",
      coroutineId,
      result: {
        status: "err",
        error: serializeError(
          error instanceof Error ? error : new Error(String(error)),
        ),
      },
    };
    try {
      await stream.append(closeEvent);
    } catch {
      // Close event append failed — the original error is more important.
    }

    throw error;
  } finally {
    // Swallow destroy errors. If the scope is in an error state (e.g.,
    // a child threw), destroy() may throw "halted". We don't want that
    // to mask the original error from the catch block.
    try {
      await destroy();
    } catch {
      // Scope cleanup errors are expected when the workflow failed.
    }
  }
}

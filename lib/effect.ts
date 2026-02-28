/**
 * createDurableEffect — the core factory for durable effects.
 *
 * Each DurableEffect handles its own replay/live dispatch inside enter().
 * It reads DurableContext from the scope, checks the replay index, and
 * either feeds the stored result (replay) or executes live with
 * persist-before-resume semantics.
 *
 * See integration doc §5.1, protocol spec §4.2, §5, §6.
 */

import { DurableCtx, type DurableContext } from "./context.ts";
import {
  ContinuePastCloseDivergenceError,
  DivergenceError,
} from "./errors.ts";
import { protocolToEffection } from "./serialize.ts";
import type {
  DurableEffect,
  EffectDescription,
  EffectionResult,
  Resolve,
  Result,
} from "./types.ts";

/**
 * Executor function signature for live execution.
 *
 * The executor receives:
 * - resolve: call with a protocol Result when the effect completes
 * - reject: call with an Error for unexpected failures
 *
 * Returns a teardown function called during scope destruction/cancellation.
 */
export type Executor<T> = (
  resolve: (result: Result) => void,
  reject: (error: Error) => void,
) => () => void;

/**
 * Creates a DurableEffect that handles replay/live dispatch internally.
 *
 * @param desc Structured description for the journal and divergence detection
 * @param execute Called only during live execution (skipped during replay)
 */
export function createDurableEffect<T>(
  desc: EffectDescription,
  execute: Executor<T>,
): DurableEffect<T> {
  return {
    description: `${desc.type}(${desc.name})`,
    effectDescription: desc,

    enter(
      resolve: Resolve<EffectionResult<T>>,
      routine,
    ): (resolve: Resolve<EffectionResult<void>>) => void {
      const ctx = routine.scope.expect<DurableContext>(DurableCtx);
      const entry = ctx.replayIndex.peekYield(ctx.coroutineId);

      if (entry) {
        // ── REPLAY PATH ──
        // §6.2: Validate description match
        if (
          entry.description.type !== desc.type ||
          entry.description.name !== desc.name
        ) {
          const cursor = ctx.replayIndex.getCursor(ctx.coroutineId);
          resolve({
            ok: false,
            error: new DivergenceError(
              ctx.coroutineId,
              cursor,
              entry.description,
              desc,
            ),
          });
          return (exit) => exit({ ok: true, value: undefined as void });
        }

        // Consume the entry and advance cursor
        ctx.replayIndex.consumeYield(ctx.coroutineId);

        // Feed stored result synchronously — no I/O, no side effects.
        // Convert from protocol Result to Effection Result.
        resolve(protocolToEffection<T>(entry.result));
        return (exit) => exit({ ok: true, value: undefined as void });
      }

      // No replay entry. Check for continue-past-close divergence (§6.3).
      // If the journal has a Close for this coroutine but no more yields,
      // the generator has diverged by continuing to yield effects.
      if (ctx.replayIndex.hasClose(ctx.coroutineId)) {
        const yieldCount = ctx.replayIndex.yieldCount(ctx.coroutineId);
        resolve({
          ok: false,
          error: new ContinuePastCloseDivergenceError(
            ctx.coroutineId,
            yieldCount,
          ),
        });
        return (exit) => exit({ ok: true, value: undefined as void });
      }

      // ── LIVE PATH ──
      // Call the executor. On resolution, persist-before-resume (§5).
      const teardown = execute(
        (result: Result) => {
          const event = {
            type: "yield" as const,
            coroutineId: ctx.coroutineId,
            description: desc,
            result,
          };
          // Strategy B: buffered write with deferred resume.
          // The generator does not advance until the durable write completes.
          ctx.stream.append(event).then(() => {
            resolve(protocolToEffection<T>(result));
          });
        },
        (error: Error) => {
          const result: Result = {
            status: "err",
            error: { message: error.message, name: error.name, stack: error.stack },
          };
          const event = {
            type: "yield" as const,
            coroutineId: ctx.coroutineId,
            description: desc,
            result,
          };
          ctx.stream.append(event).then(() => {
            resolve(protocolToEffection<T>(result));
          });
        },
      );

      // Return teardown that Effection calls during scope destruction
      return (exit: Resolve<EffectionResult<void>>) => {
        try {
          teardown();
          exit({ ok: true, value: undefined as void });
        } catch (e) {
          exit({ ok: false, error: e as Error });
        }
      };
    },
  };
}

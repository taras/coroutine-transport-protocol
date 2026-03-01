/**
 * createDurableEffect — the core factory for durable effects.
 *
 * Each DurableEffect handles its own replay/live dispatch inside enter().
 * It reads DurableContext from the scope, checks the replay index, and
 * either feeds the stored result (replay) or executes live with
 * persist-before-resume semantics.
 *
 * Divergence policy is delegated to the Divergence API (DEC-031).
 * By default, mismatches are fatal. Users can install middleware via
 * scope.around(Divergence, ...) to override behavior per-scope.
 *
 * See integration doc §5.1, protocol spec §4.2, §5, §6.
 */

import { DurableCtx, type DurableContext } from "./context.ts";
import { Divergence } from "./divergence.ts";
import { protocolToEffection, serializeError } from "./serialize.ts";
import type {
  DurableEffect,
  EffectDescription,
  EffectionResult,
  Resolve,
  Result,
} from "./types.ts";

/** Effection void-ok result, used for no-op teardowns. */
const VOID_OK: EffectionResult<void> = { ok: true, value: undefined as void };

/**
 * Executor function signature for live execution.
 *
 * The executor receives:
 * - resolve: call with a protocol Result when the effect completes
 * - reject: call with an Error for unexpected failures
 *
 * Returns a teardown function called during scope destruction/cancellation.
 */
export type Executor = (
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
  execute: Executor,
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

      // ── REPLAY PATH ──
      // Use a labeled block so that divergence decisions of type "run-live"
      // can break out to fall through to the live execution path.
      replay: {
        if (entry) {
          // §6.2: Validate description match
          if (
            entry.description.type !== desc.type ||
            entry.description.name !== desc.name
          ) {
            // Delegate divergence policy to the Divergence API.
            // Api.invoke() runs the middleware chain synchronously.
            const cursor = ctx.replayIndex.getCursor(ctx.coroutineId);
            const decision = Divergence.invoke(
              routine.scope,
              "decide",
              [{
                kind: "description-mismatch",
                coroutineId: ctx.coroutineId,
                cursor,
                expected: entry.description,
                actual: desc,
              }],
            );

            if (decision.type === "throw") {
              resolve({ ok: false, error: decision.error });
              return (exit) => exit(VOID_OK);
            }

            // decision.type === "run-live"
            // Disable replay for this coroutine and fall through to live path.
            ctx.replayIndex.disableReplay(ctx.coroutineId);
            break replay;
          }

          // Description matches — consume the entry and advance cursor
          ctx.replayIndex.consumeYield(ctx.coroutineId);

          // Feed stored result synchronously — no I/O, no side effects.
          // Convert from protocol Result to Effection Result.
          resolve(protocolToEffection<T>(entry.result));
          return (exit) => exit(VOID_OK);
        }

        // No replay entry. Check for continue-past-close divergence (§6.3).
        // If the journal has a Close for this coroutine but no more yields,
        // the generator has diverged by continuing to yield effects.
        if (ctx.replayIndex.hasClose(ctx.coroutineId)) {
          const yieldCount = ctx.replayIndex.yieldCount(ctx.coroutineId);
          const decision = Divergence.invoke(
            routine.scope,
            "decide",
            [{
              kind: "continue-past-close",
              coroutineId: ctx.coroutineId,
              yieldCount,
            }],
          );

          if (decision.type === "throw") {
            resolve({ ok: false, error: decision.error });
            return (exit) => exit(VOID_OK);
          }

          // decision.type === "run-live"
          ctx.replayIndex.disableReplay(ctx.coroutineId);
          break replay;
        }
      } // end replay block

      // ── LIVE PATH ──
      // Reached either because:
      // 1. No replay entry and no Close (normal live execution)
      // 2. Divergence API returned "run-live" (replay disabled)

      /** Persist a Yield event then resume the generator. */
      function persistAndResolve(result: Result): void {
        const event = {
          type: "yield" as const,
          coroutineId: ctx.coroutineId,
          description: desc,
          result,
        };
        // Strategy B: buffered write with deferred resume.
        // The generator does not advance until the durable write completes.
        // If append rejects, deliver the error through Effection's normal
        // error channel to avoid hanging the generator.
        ctx.stream.append(event).then(
          () => resolve(protocolToEffection<T>(result)),
          (err) =>
            resolve({
              ok: false,
              error: err instanceof Error ? err : new Error(String(err)),
            }),
        );
      }

      // Guard against synchronous throws from the executor. If execute()
      // throws before returning a teardown function, we need to persist the
      // error and resolve through the normal channel.
      let teardown: () => void;
      try {
        teardown = execute(
          (result: Result) => persistAndResolve(result),
          (error: Error) => {
            persistAndResolve({
              status: "err",
              error: serializeError(error),
            });
          },
        );
      } catch (e) {
        const error = e instanceof Error ? e : new Error(String(e));
        persistAndResolve({
          status: "err",
          error: serializeError(error),
        });
        return (exit) => exit(VOID_OK);
      }

      // Return teardown that Effection calls during scope destruction
      return (exit: Resolve<EffectionResult<void>>) => {
        try {
          teardown();
          exit(VOID_OK);
        } catch (e) {
          const error = e instanceof Error ? e : new Error(String(e));
          exit({ ok: false, error });
        }
      };
    },
  };
}

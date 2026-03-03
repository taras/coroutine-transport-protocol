/**
 * Error types for the durable execution protocol.
 */

import type { CoroutineId, EffectDescription } from "./types.ts";

/**
 * Raised when the replay index entry at the current cursor position
 * does not match the effect yielded by the generator. See spec §6.2.
 *
 * A DivergenceError is NOT recoverable. The workflow cannot continue
 * because the generator's execution path has diverged from the recorded
 * history.
 */
export class DivergenceError extends Error {
  override name = "DivergenceError";

  constructor(
    public coroutineId: CoroutineId,
    /** Cursor position within the coroutine where divergence was detected. */
    public position: number,
    /** The description from the journal (what was expected). */
    public expected: EffectDescription,
    /** The description from the generator (what was actually yielded). */
    public actual: EffectDescription,
    message?: string,
  ) {
    super(
      message ??
        `Divergence at ${coroutineId}[${position}]: ` +
        `expected ${expected.type}("${expected.name}"), ` +
        `got ${actual.type}("${actual.name}")`,
    );
  }
}

/**
 * Raised when the generator finishes (returns) while the replay index
 * still has unconsumed entries for this coroutine. See spec §6.3.
 */
export class EarlyReturnDivergenceError extends Error {
  override name = "DivergenceError";

  constructor(
    public coroutineId: CoroutineId,
    public consumedCount: number,
    public totalCount: number,
  ) {
    super(
      `Divergence: generator ${coroutineId} returned after ${consumedCount} yields, ` +
      `but journal has ${totalCount} yield entries`,
    );
  }
}

/**
 * Raised when the journal has a Close event for a coroutine but the
 * generator has not finished after consuming all recorded yields.
 * See spec §6.3.
 */
export class ContinuePastCloseDivergenceError extends Error {
  override name = "DivergenceError";

  constructor(
    public coroutineId: CoroutineId,
    public yieldCount: number,
  ) {
    super(
      `Divergence: journal shows ${coroutineId} closed after ${yieldCount} yields, ` +
      `but generator continues to yield effects`,
    );
  }
}

/**
 * Raised by a replay guard when a journal entry's metadata indicates
 * the recorded result is stale (e.g., the source file has changed since
 * the effect was originally executed).
 *
 * StaleInputError is NOT a divergence — the effect identity matches,
 * but the external world has changed. The correct response depends on
 * application policy: re-run from scratch, accept stale results, or
 * (in future versions) re-execute the effect and continue.
 *
 * See replay-guard-spec.md §4.4.
 */
export class StaleInputError extends Error {
  override name = "StaleInputError";

  constructor(
    /** Human-readable description of what changed. */
    message: string,
    /** The Yield event that was detected as stale. */
    public event?: { coroutineId: string; description: { type: string; name: string } },
  ) {
    super(message);
  }
}

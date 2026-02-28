/**
 * Protocol types for the two-event durable execution protocol.
 *
 * These types are the fixed contract defined by protocol-specification.md.
 * They do not depend on Effection internals.
 */

/** Any JSON-serializable value. */
export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json };

/** Serialized error for durable storage. */
export interface SerializedError {
  message: string;
  name?: string;
  stack?: string;
}

/** Result of an effect or coroutine. */
export type Result =
  | { status: "ok"; value?: Json }
  | { status: "err"; error: SerializedError }
  | { status: "cancelled" };

/** Dot-delimited hierarchical coroutine path. See spec §3. */
export type CoroutineId = string;

/**
 * Structured effect identity for divergence detection.
 * See spec §6 for matching rules.
 */
export interface EffectDescription {
  /** Effect category. E.g., "call", "sleep", "action", "spawn", "resource". */
  type: string;
  /** Stable name within the category. E.g., function name, resource label. */
  name: string;
}

/**
 * A Yield event — an effect was executed and resolved.
 * Written after an effect resolves. Records both what was requested
 * (description) and what the outcome was (result). See spec §2.1.
 */
export interface Yield {
  type: "yield";
  coroutineId: CoroutineId;
  description: EffectDescription;
  result: Result;
}

/**
 * A Close event — a coroutine reached a terminal state.
 * Written when a coroutine terminates (completed, failed, or cancelled).
 * See spec §2.2.
 */
export interface Close {
  type: "close";
  coroutineId: CoroutineId;
  result: Result;
}

/** The two event types that make up the durable stream. */
export type DurableEvent = Yield | Close;

// ---------------------------------------------------------------------------
// Effection integration types
// ---------------------------------------------------------------------------

// NOTE: DurableEffect and Workflow types will be defined in Phase 1
// once we verify the exact Effection 4.1 alpha types.
// Placeholder re-exports for now.

/**
 * A DurableEffect extends Effection's Effect with a structured description
 * for divergence detection and replay.
 *
 * This is a placeholder — the full definition depends on Effection's
 * Effect interface shape in 4.1.0-alpha.5.
 */
export interface DurableEffect<T> {
  description: string;
  effectDescription: EffectDescription;
  enter(
    resolve: (result: { ok: true; value: T } | { ok: false; error: Error }) => void,
    routine: unknown,
  ): (resolve: (result: { ok: true; value: void } | { ok: false; error: Error }) => void) => void;
}

/**
 * A Workflow is a generator that only yields DurableEffect values.
 * Every Workflow is an Operation, but not every Operation is a Workflow.
 *
 * Uses Generator (not Iterable) so TypeScript enforces the yield type
 * at compile time.
 */
export type Workflow<T> = Generator<DurableEffect<unknown>, T, unknown>;

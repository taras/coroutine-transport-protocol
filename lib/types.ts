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

/**
 * Effection's internal Result type (distinct from the protocol's Result).
 *
 * Effection uses { ok: true, value: T } | { ok: false, error: Error }.
 * The protocol uses { status: "ok" | "err" | "cancelled" }.
 * We re-declare Effection's shape here so types.ts has no Effection imports.
 */
// TODO this hsould be replaced with Result from effection
export type EffectionResult<T> =
  | { readonly ok: true; value: T }
  | { readonly ok: false; error: Error };

/**
 * Effection's Resolve callback type.
 */
export type Resolve<T> = (value: T) => void;

/**
 * Minimal view of Effection's Coroutine — only the fields we need.
 * The full Coroutine type is internal to Effection (@ignore), but
 * enter() receives it. We need `scope` to read DurableContext.
 */
export interface CoroutineView {
  scope: {
    get<T>(context: { name: string; defaultValue?: T }): T | undefined;
    expect<T>(context: { name: string }): T;
    set<T>(context: { name: string }, value: T): T;
  };
}

/**
 * A DurableEffect extends Effection's Effect interface with a structured
 * `effectDescription` for divergence detection and replay.
 *
 * The `enter()` signature matches Effection 4.1.0-alpha.5's Effect<T> exactly:
 *   enter(resolve: Resolve<EffectionResult<T>>, routine: Coroutine):
 *     (resolve: Resolve<EffectionResult<void>>) => void
 *
 * DurableEffect<T> is assignable to Effect<T> because it has the same shape
 * plus the extra `effectDescription` field.
 */
export interface DurableEffect<T> {
  /** Human-readable description (for Effection's Effect interface). */
  description: string;
  /** Structured description for divergence detection (spec §6). */
  effectDescription: EffectDescription;
  /** Enter the effect — handles replay/live dispatch internally. */
  enter(
    resolve: Resolve<EffectionResult<T>>,
    routine: CoroutineView,
  ): (resolve: Resolve<EffectionResult<void>>) => void;
}

/**
 * A Workflow is a generator that only yields DurableEffect values.
 *
 * Every Workflow is structurally compatible with Operation<T> because
 * DurableEffect<unknown> extends Effect<unknown> (it has all required fields).
 * TypeScript's covariant yield type means Generator<DurableEffect, T, unknown>
 * is assignable to Iterator<Effect, T, unknown>.
 *
 * Uses Generator (not Iterable) so TypeScript enforces the yield type
 * at compile time — yielding a plain Effect inside a Workflow generator
 * is a type error.
 */
export type Workflow<T> = Generator<DurableEffect<unknown>, T, unknown>;

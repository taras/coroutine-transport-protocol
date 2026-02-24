import type { Effect } from "effection";

// ── Workflow types (original) ──────────────────────────────────────

export interface WorkflowEffect extends Effect<unknown> {
  workflowId: string;
  effectId: string;
}

export interface Workflow<T> {
  [Symbol.iterator](): Iterator<WorkflowEffect, T, unknown>;
}

// ── JSON-safe types ────────────────────────────────────────────────

export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json };

export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
}

// ── Durable Event types ────────────────────────────────────────────
//
// These describe what gets written to the Durable Stream.
// Drawn from pseudo-tests.ts event model.

export type DurableEvent =
  | ScopeCreated
  | ScopeDestroyed
  | ScopeSet
  | ScopeDelete
  | EffectYielded
  | EffectResolved
  | EffectErrored
  | WorkflowReturn;

export interface ScopeCreated {
  type: "scope:created";
  scopeId: string;
  parentScopeId?: string;
}

export interface ScopeDestroyed {
  type: "scope:destroyed";
  scopeId: string;
  result: { ok: true } | { ok: false; error: SerializedError };
}

export interface ScopeSet {
  type: "scope:set";
  scopeId: string;
  contextName: string;
  value: Json;
}

export interface ScopeDelete {
  type: "scope:delete";
  scopeId: string;
  contextName: string;
}

export interface EffectYielded {
  type: "effect:yielded";
  scopeId: string;
  effectId: string;
  description: string;
}

export interface EffectResolved {
  type: "effect:resolved";
  effectId: string;
  value: Json;
}

export interface EffectErrored {
  type: "effect:errored";
  effectId: string;
  error: SerializedError;
}

export interface WorkflowReturn {
  type: "workflow:return";
  scopeId: string;
  value: Json;
}

// ── Durable Stream interface ───────────────────────────────────────
//
// Minimal interface matching the Durable Streams protocol concepts:
// append-only, offset-based, readable.

export interface StreamEntry {
  offset: number;
  event: DurableEvent;
}

export interface DurableStream {
  /** Append an event to the stream. Returns the assigned offset. */
  append(event: DurableEvent): number;

  /** Read all entries from `fromOffset` (inclusive) to current tail. */
  read(fromOffset?: number): StreamEntry[];

  /** Get the current number of entries in the stream. */
  length: number;

  /** Whether the stream has been closed (workflow complete/halted). */
  closed: boolean;

  /** Close the stream, signaling EOF. */
  close(): void;
}

/**
 * @module
 * Durable execution for Effection.
 *
 * Implements the two-event durable execution protocol for generator-based
 * structured concurrency, with Durable Streams as the persistence backend.
 */

// Protocol types
export type {
  Close,
  CoroutineId,
  DurableEffect,
  DurableEvent,
  EffectDescription,
  Json,
  Result,
  SerializedError,
  Workflow,
  Yield,
} from "./types.ts";

// ReplayIndex
export { ReplayIndex } from "./replay-index.ts";

// Stream interface
export type { DurableStream } from "./stream.ts";
export { InMemoryStream } from "./stream.ts";

// Errors
export { DivergenceError } from "./errors.ts";

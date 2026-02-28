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
  CoroutineView,
  DurableEffect,
  DurableEvent,
  EffectDescription,
  EffectionResult,
  Json,
  Resolve,
  Result,
  SerializedError,
  Workflow,
  Yield,
} from "./types.ts";

// ReplayIndex
export { ReplayIndex } from "./replay-index.ts";
export type { YieldEntry } from "./replay-index.ts";

// Stream interface
export type { DurableStream } from "./stream.ts";
export { InMemoryStream } from "./stream.ts";

// Errors
export {
  ContinuePastCloseDivergenceError,
  DivergenceError,
  EarlyReturnDivergenceError,
} from "./errors.ts";

// Context
export { DurableCtx } from "./context.ts";
export type { DurableContext } from "./context.ts";

// Serialization utilities
export {
  deserializeError,
  effectionToProtocol,
  protocolToEffection,
  serializeError,
} from "./serialize.ts";

// Core effect factory
export { createDurableEffect } from "./effect.ts";
export type { Executor } from "./effect.ts";

// Workflow-enabled effects
export {
  durableAction,
  durableCall,
  durableSleep,
  versionCheck,
} from "./operations.ts";

// Entry point
export { durableRun } from "./run.ts";
export type { DurableRunOptions } from "./run.ts";

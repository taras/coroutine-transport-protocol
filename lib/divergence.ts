/**
 * Divergence API — pluggable policy for handling replay mismatches.
 *
 * When a durable effect's description doesn't match the replay index
 * during replay, or when a generator continues to yield effects past
 * a recorded Close event, a divergence is detected.
 *
 * By default, divergence is fatal (throws DivergenceError). Users can
 * override this behavior per-scope via Effection's around() middleware
 * to implement custom policies (e.g., switching to live execution).
 *
 * NOTE: We cannot use `createApi()` from `@effection/effection/experimental`
 * because that entry point triggers a circular initialization error in
 * Effection 4.1.0-alpha.5 (scope-internal.ts → api.ts → scope-internal.ts).
 * Instead, we implement a minimal API-compatible object using `createContext`
 * from the main module, which has no such circular dependency.
 *
 * The core decide() function is synchronous (not a generator) because
 * it is called from inside Effect.enter(), which is a synchronous
 * callback. See DEC-031.
 */

import { createContext } from "@effection/effection";
import type { Api, Around, Middleware, Scope } from "@effection/effection";
import {
  ContinuePastCloseDivergenceError,
  DivergenceError,
} from "./errors.ts";
import type { CoroutineId, EffectDescription } from "./types.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** The two kinds of divergence detected during replay. */
export type DivergenceKind = "description-mismatch" | "continue-past-close";

/**
 * Information about a detected divergence.
 *
 * Discriminated union on `kind` so that TypeScript enforces correct
 * field access per variant.
 */
export type DivergenceInfo =
  | {
      kind: "description-mismatch";
      coroutineId: CoroutineId;
      /** Cursor position (yield index) where divergence was detected. */
      cursor: number;
      /** The description from the journal (what was expected). */
      expected: EffectDescription;
      /** The description from the generator (what was actually yielded). */
      actual: EffectDescription;
    }
  | {
      kind: "continue-past-close";
      coroutineId: CoroutineId;
      /** Number of yield entries recorded for this coroutine. */
      yieldCount: number;
    };

/**
 * The policy decision returned by the Divergence API.
 *
 * - "throw": Fail the workflow with the provided error (default behavior).
 * - "run-live": Disable replay for this coroutine and execute live from
 *   this point forward. Previous replay entries are ignored.
 */
export type DivergenceDecision =
  | { type: "throw"; error: Error }
  | { type: "run-live" };

// ---------------------------------------------------------------------------
// API shape (synchronous — not generator-based)
// ---------------------------------------------------------------------------

/**
 * The core shape of the Divergence API.
 *
 * decide() is synchronous because it is called from Effect.enter(),
 * which cannot yield. Middleware installed via scope.around() also
 * runs synchronously in the chain.
 *
 * Usage from Effect.enter() (synchronous):
 *   Divergence.invoke(scope, "decide", [info])
 *
 * Middleware installation (from outside a generator):
 *   scope.around(Divergence, { decide: ([info], next) => { ... } })
 */
interface DivergenceApi {
  decide(info: DivergenceInfo): DivergenceDecision;
}

// ---------------------------------------------------------------------------
// Minimal API implementation (avoids circular dependency in experimental)
// ---------------------------------------------------------------------------

/**
 * Middleware storage type for the Divergence API context.
 * Each scope can have max (outer) and min (inner) middleware stacks.
 */
interface MiddlewareStore {
  max: Partial<Around<DivergenceApi>>[];
  min: Partial<Around<DivergenceApi>>[];
}

/**
 * ScopeInternal-compatible interface for accessing the reduce() method.
 * This is what Effection's scope exposes internally for middleware collection.
 */
interface ScopeWithReduce extends Scope {
  reduce<T, S>(
    context: { name: string; defaultValue?: T },
    fn: (sum: S, item: T) => S,
    initial: S,
  ): S;
}

/** Context for storing middleware on the scope chain. */
const middlewareContext = createContext<MiddlewareStore>(
  "api::DurableEffection.Divergence",
);

/** The default (strict) decide function. */
function defaultDecide(info: DivergenceInfo): DivergenceDecision {
  if (info.kind === "description-mismatch") {
    return {
      type: "throw",
      error: new DivergenceError(
        info.coroutineId,
        info.cursor,
        info.expected,
        info.actual,
      ),
    };
  } else {
    return {
      type: "throw",
      error: new ContinuePastCloseDivergenceError(
        info.coroutineId,
        info.yieldCount,
      ),
    };
  }
}

/**
 * Combine an array of middleware functions into a single middleware.
 */
function combineMiddleware<TArgs extends unknown[], TReturn>(
  middlewares: Middleware<TArgs, TReturn>[],
): Middleware<TArgs, TReturn> {
  if (middlewares.length === 0) {
    return (args, next) => next(...args);
  }
  return middlewares.reduceRight(
    (sum, middleware) => (args, next) =>
      middleware(args, (...args) => sum(args, next)),
  );
}

/**
 * The Divergence API instance.
 *
 * Structurally compatible with Effection's Api<DivergenceApi> and
 * ApiInternal<DivergenceApi> — scope.around() accesses the `context`
 * field to store middleware in the scope chain.
 *
 * Default behavior is strict: all divergences produce a throw decision
 * with the appropriate error type.
 */
// deno-lint-ignore no-explicit-any
export const Divergence: Api<DivergenceApi> & { context: any } = {
  context: middlewareContext,

  invoke(scope, key, args) {
    if (key !== "decide") {
      throw new Error(`Unknown Divergence API method: ${String(key)}`);
    }

    // Collect middleware from the scope chain using reduce().
    // This mirrors createApiInternal's createHandle() logic.
    const $scope = scope as ScopeWithReduce;
    const { min, max } = $scope.reduce(
      middlewareContext,
      (
        sum: { min: Middleware<[DivergenceInfo], DivergenceDecision>[]; max: Middleware<[DivergenceInfo], DivergenceDecision>[] },
        current: MiddlewareStore,
      ) => {
        const minMiddleware = current.min.flatMap((around) =>
          around.decide ? [around.decide] : []
        );
        const maxMiddleware = current.max.flatMap((around) =>
          around.decide ? [around.decide] : []
        );
        sum.min.push(
          ...(minMiddleware as Middleware<[DivergenceInfo], DivergenceDecision>[]),
        );
        sum.max.unshift(
          ...(maxMiddleware as Middleware<[DivergenceInfo], DivergenceDecision>[]),
        );
        return sum;
      },
      {
        min: [] as Middleware<[DivergenceInfo], DivergenceDecision>[],
        max: [] as Middleware<[DivergenceInfo], DivergenceDecision>[],
      },
    );

    const stack = combineMiddleware(
      max.concat(min),
    );

    // deno-lint-ignore no-explicit-any
    return stack(args as [DivergenceInfo], defaultDecide) as any;
  },

  // operations and around are not used from Effect.enter() (synchronous path),
  // but included for Api<A> structural compatibility.
  // deno-lint-ignore no-explicit-any
  operations: {} as any,

  // deno-lint-ignore no-explicit-any
  around: (() => {}) as any,
};

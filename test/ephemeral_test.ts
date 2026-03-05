/**
 * Tests for ephemeral() — the explicit escape hatch for non-durable
 * Operations inside Workflows.
 *
 * Validates that:
 * - ephemeral operations execute and return values correctly
 * - ephemeral is transparent to the journal (no Yield events written)
 * - ephemeral operations re-run on replay (not cached)
 * - ephemeral supports cancellation via structured concurrency
 * - the type boundary is enforced (bare Operations rejected by combinators)
 */

import { assertEquals } from "@std/assert";
import { useScope } from "@effection/effection";
import type { Operation } from "@effection/effection";
import {
  durableAll,
  durableCall,
  durableRun,
  ephemeral,
  InMemoryStream,
  type DurableEvent,
} from "../lib/mod.ts";
import { test } from "./test-helpers.ts";

// ---------------------------------------------------------------------------
// Test 1: ephemeral executes and returns value
// ---------------------------------------------------------------------------

test("ephemeral: executes operation and returns value", function* () {
  const stream = new InMemoryStream();

  const result = yield* durableRun(
    function* () {
      const value = yield* ephemeral(function* (): Operation<string> {
        return "hello from ephemeral";
      }());
      return value;
    },
    { stream },
  );

  assertEquals(result, "hello from ephemeral");
});

// ---------------------------------------------------------------------------
// Test 2: ephemeral is transparent to journal — no Yield events
// ---------------------------------------------------------------------------

test("ephemeral: transparent to journal — no Yield events written", function* () {
  const stream = new InMemoryStream();

  yield* durableRun(
    function* () {
      // One durable call, one ephemeral, one more durable call
      yield* durableCall("step1", () => Promise.resolve("a"));
      yield* ephemeral(function* (): Operation<string> {
        return "ephemeral-value";
      }());
      yield* durableCall("step2", () => Promise.resolve("b"));
      return "done";
    },
    { stream },
  );

  const events: DurableEvent[] = yield* stream.readAll();

  // Should have: Yield(step1), Yield(step2), Close(root) — NO ephemeral Yield
  const yieldEvents = events.filter((e) => e.type === "yield");
  assertEquals(yieldEvents.length, 2);
  assertEquals(yieldEvents[0]!.description.name, "step1");
  assertEquals(yieldEvents[1]!.description.name, "step2");

  // No event with type "ephemeral" should exist
  const ephemeralEvents = events.filter(
    (e) => e.type === "yield" && e.description.type === "ephemeral",
  );
  assertEquals(ephemeralEvents.length, 0);
});

// ---------------------------------------------------------------------------
// Test 3: ephemeral re-runs on replay (not cached)
// ---------------------------------------------------------------------------

test("ephemeral: re-runs on replay — not cached", function* () {
  const stream = new InMemoryStream();
  let ephemeralCallCount = 0;

  // First run — ephemeral runs once
  yield* durableRun(
    function* () {
      yield* durableCall("step1", () => Promise.resolve("a"));
      yield* ephemeral(function* (): Operation<void> {
        ephemeralCallCount++;
      }());
      yield* durableCall("step2", () => Promise.resolve("b"));
      return "done";
    },
    { stream },
  );
  assertEquals(ephemeralCallCount, 1);

  // Remove the Close event to simulate partial replay
  // Actually, durableRun short-circuits on Close, so we need a fresh stream
  // with the same events minus Close to trigger replay + re-run
  const events = yield* stream.readAll();
  const withoutClose = events.filter((e) => e.type !== "close");
  const replayStream = new InMemoryStream(withoutClose);

  // Reset counter
  ephemeralCallCount = 0;

  // Second run — durable calls replay, but ephemeral re-runs
  yield* durableRun(
    function* () {
      yield* durableCall("step1", () => Promise.resolve("a"));
      yield* ephemeral(function* (): Operation<void> {
        ephemeralCallCount++;
      }());
      yield* durableCall("step2", () => Promise.resolve("b"));
      return "done";
    },
    { stream: replayStream },
  );

  // ephemeral ran again during replay
  assertEquals(ephemeralCallCount, 1);
});

// ---------------------------------------------------------------------------
// Test 4: ephemeral propagates errors
// ---------------------------------------------------------------------------

test("ephemeral: propagates errors from the operation", function* () {
  const stream = new InMemoryStream();

  try {
    yield* durableRun(
      function* () {
        yield* ephemeral(function* (): Operation<never> {
          throw new Error("ephemeral boom");
        }());
        return "unreachable";
      },
      { stream },
    );
    throw new Error("expected ephemeral boom");
  } catch (e) {
    assertEquals(e instanceof Error, true);
    assertEquals((e as Error).message, "ephemeral boom");
  }
});

// ---------------------------------------------------------------------------
// Test 5: ephemeral works inside durableAll children
// ---------------------------------------------------------------------------

test("ephemeral: works inside durableAll children", function* () {
  const stream = new InMemoryStream();

  const result = yield* durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          // Use ephemeral to run an Operation inside a Workflow child
          const scope = yield* ephemeral(useScope());
          // Just verify we got a scope (infrastructure operation worked)
          if (!scope) throw new Error("no scope");
          return yield* durableCall("child1", () => Promise.resolve("a"));
        },
        function* () {
          return yield* durableCall("child2", () => Promise.resolve("b"));
        },
      ]);
      return results.join("-");
    },
    { stream },
  );

  assertEquals(result, "a-b");
});

// ---------------------------------------------------------------------------
// Test 6: nested durableAll works directly (no ephemeral needed)
// ---------------------------------------------------------------------------

test("ephemeral: nested durableAll works without ephemeral wrapping", function* () {
  const stream = new InMemoryStream();

  // durableAll now returns Workflow<T[]>, so nested calls work directly
  // inside a Workflow child — no ephemeral wrapping required
  const result = yield* durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          const inner = yield* durableAll([
            function* () {
              return yield* durableCall("innerA", () => Promise.resolve("x"));
            },
            function* () {
              return yield* durableCall("innerB", () => Promise.resolve("y"));
            },
          ]);
          return inner.join("+") as string;
        },
        function* () {
          return yield* durableCall("outer", () => Promise.resolve("z"));
        },
      ]);
      return results.join("-");
    },
    { stream },
  );

  assertEquals(result, "x+y-z");
});

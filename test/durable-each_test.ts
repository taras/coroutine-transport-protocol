/**
 * Tier 4 tests — durable iteration (durableEach).
 *
 * Validates that durableEach correctly journals each fetch,
 * replays from the journal, detects advance guard violations,
 * handles break/cancellation, and integrates with durableCall.
 */

import { assertEquals, assertRejects } from "@std/assert";
import {
  durableCall,
  durableEach,
  durableRun,
  InMemoryStream,
  type DurableEvent,
  type DurableSource,
  type Json,
} from "../lib/mod.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a DurableSource from an array of items. */
function arraySource<T extends Json>(items: T[]): DurableSource<T> & { closed: boolean } {
  let index = 0;
  const src = {
    closed: false,
    next(): Promise<{ value: T } | { done: true }> {
      if (index < items.length) {
        return Promise.resolve({ value: items[index++]! });
      }
      return Promise.resolve({ done: true as const });
    },
    close() {
      src.closed = true;
    },
  };
  return src;
}

/** Track which functions were actually called during live execution. */
function createCallTracker() {
  const calls: string[] = [];
  return {
    calls,
    fn<T extends Json>(name: string, value: T): () => Promise<T> {
      return () => {
        calls.push(name);
        return Promise.resolve(value);
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Test 1: Golden run — 3 items
// ---------------------------------------------------------------------------

Deno.test("each: golden run — 3 items processed, correct journal", async () => {
  const stream = new InMemoryStream();
  const source = arraySource(["a", "b", "c"]);
  const processed: string[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        processed.push(msg);
        yield* durableEach.next();
      }
      return "done";
    },
    { stream },
  );

  assertEquals(result, "done");
  assertEquals(processed, ["a", "b", "c"]);

  // Verify journal: 4 each events (a, b, c, done) + 1 root Close
  const events = await stream.readAll();
  const yieldEvents = events.filter((e) => e.type === "yield");
  assertEquals(yieldEvents.length, 4); // 3 items + 1 done sentinel

  // Check each event description
  for (const y of yieldEvents) {
    if (y.type === "yield") {
      assertEquals(y.description, { type: "each", name: "queue" });
    }
  }

  // Check result values
  if (yieldEvents[0]!.type === "yield") {
    assertEquals(yieldEvents[0]!.result, { status: "ok", value: { value: "a" } });
  }
  if (yieldEvents[1]!.type === "yield") {
    assertEquals(yieldEvents[1]!.result, { status: "ok", value: { value: "b" } });
  }
  if (yieldEvents[2]!.type === "yield") {
    assertEquals(yieldEvents[2]!.result, { status: "ok", value: { value: "c" } });
  }
  if (yieldEvents[3]!.type === "yield") {
    assertEquals(yieldEvents[3]!.result, { status: "ok", value: { done: true } });
  }

  // Root Close event
  const closeEvents = events.filter((e) => e.type === "close");
  assertEquals(closeEvents.length, 1);
  assertEquals(closeEvents[0]!.coroutineId, "root");
});

// ---------------------------------------------------------------------------
// Test 2: Empty source — loop body never executes
// ---------------------------------------------------------------------------

Deno.test("each: empty source — loop body never executes", async () => {
  const stream = new InMemoryStream();
  const source = arraySource<string>([]);
  const processed: string[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("empty", source)) {
        processed.push(msg);
        yield* durableEach.next();
      }
      return "done";
    },
    { stream },
  );

  assertEquals(result, "done");
  assertEquals(processed, []);

  // Journal: 1 each event (done) + 1 root Close
  const events = await stream.readAll();
  const yieldEvents = events.filter((e) => e.type === "yield");
  assertEquals(yieldEvents.length, 1);
  if (yieldEvents[0]!.type === "yield") {
    assertEquals(yieldEvents[0]!.result, { status: "ok", value: { done: true } });
  }
});

// ---------------------------------------------------------------------------
// Test 3: Full replay — no source calls, items replayed from journal
// ---------------------------------------------------------------------------

Deno.test("each: full replay — items replayed from journal without calling source", async () => {
  // Pre-populate stream with all yield events but NO root Close.
  // durableRun will re-run the generator, but all DurableEffects resolve
  // from the replay index — source.next() is never called.
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { value: "x" } },
    },
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { value: "y" } },
    },
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { done: true } },
    },
  ];
  const stream = new InMemoryStream(events);

  // Source should never be called during replay
  let sourceCalled = false;
  const source: DurableSource<string> = {
    next() {
      sourceCalled = true;
      return Promise.resolve({ done: true as const });
    },
  };
  const processed: string[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        processed.push(msg);
        yield* durableEach.next();
      }
      return "done";
    },
    { stream },
  );

  // Generator ran but all effects were replayed from journal
  assertEquals(result, "done");
  assertEquals(processed, ["x", "y"]);
  assertEquals(sourceCalled, false);
});

// ---------------------------------------------------------------------------
// Test 4: Crash recovery (partial replay)
// ---------------------------------------------------------------------------

Deno.test("each: crash recovery — partial replay then live", async () => {
  // Journal has 2 items replayed, 3rd will be live
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { value: "a" } },
    },
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { value: "b" } },
    },
  ];
  const stream = new InMemoryStream(events);

  // Source should only be called for the 3rd item onward
  const sourceItems = ["a", "b", "c"]; // source has all items but replay covers a, b
  let sourceCallCount = 0;
  let sourceIndex = 2; // start from where replay left off
  const source: DurableSource<string> = {
    next() {
      sourceCallCount++;
      if (sourceIndex < sourceItems.length) {
        return Promise.resolve({ value: sourceItems[sourceIndex++]! });
      }
      return Promise.resolve({ done: true as const });
    },
  };
  const processed: string[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        processed.push(msg);
        yield* durableEach.next();
      }
      return "done";
    },
    { stream },
  );

  assertEquals(result, "done");
  assertEquals(processed, ["a", "b", "c"]);
  // Source called twice: once for "c", once for the done sentinel
  assertEquals(sourceCallCount, 2);
});

// ---------------------------------------------------------------------------
// Test 5: With durableCall in loop body — interleaved events
// ---------------------------------------------------------------------------

Deno.test("each: with durableCall in loop — interleaved journal events", async () => {
  const stream = new InMemoryStream();
  const source = arraySource(["msg1", "msg2"]);
  const tracker = createCallTracker();

  await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        yield* durableCall(`process-${msg}`, tracker.fn(`process-${msg}`, null));
        yield* durableEach.next();
      }
    },
    { stream },
  );

  assertEquals(tracker.calls, ["process-msg1", "process-msg2"]);

  // Verify interleaved journal structure
  const events = await stream.readAll();
  const nonClose = events.filter((e) => e.type === "yield");

  // each(msg1), call(process-msg1), each(msg2), call(process-msg2), each(done)
  assertEquals(nonClose.length, 5);
  if (nonClose[0]!.type === "yield") {
    assertEquals(nonClose[0]!.description, { type: "each", name: "queue" });
  }
  if (nonClose[1]!.type === "yield") {
    assertEquals(nonClose[1]!.description, { type: "call", name: "process-msg1" });
  }
  if (nonClose[2]!.type === "yield") {
    assertEquals(nonClose[2]!.description, { type: "each", name: "queue" });
  }
  if (nonClose[3]!.type === "yield") {
    assertEquals(nonClose[3]!.description, { type: "call", name: "process-msg2" });
  }
  if (nonClose[4]!.type === "yield") {
    assertEquals(nonClose[4]!.description, { type: "each", name: "queue" });
    assertEquals(nonClose[4]!.result, { status: "ok", value: { done: true } });
  }
});

// ---------------------------------------------------------------------------
// Test 6: Divergence detection — source name mismatch
// ---------------------------------------------------------------------------

Deno.test("each: divergence — mismatched source name", async () => {
  // Journal was recorded with name "queue" but workflow uses "other"
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "each", name: "queue" },
      result: { status: "ok", value: { value: "a" } },
    },
  ];
  const stream = new InMemoryStream(events);
  const source = arraySource(["a"]);

  await assertRejects(
    () =>
      durableRun(
        function* () {
          for (const _msg of yield* durableEach("other", source)) {
            yield* durableEach.next();
          }
        },
        { stream },
      ),
    Error,
    "Divergence",
  );
});

// ---------------------------------------------------------------------------
// Test 7: Source error — propagated through Effection
// ---------------------------------------------------------------------------

Deno.test("each: source error — propagated to workflow", async () => {
  const stream = new InMemoryStream();
  const source: DurableSource<string> = {
    next() {
      return Promise.reject(new Error("connection lost"));
    },
  };

  await assertRejects(
    () =>
      durableRun(
        function* () {
          for (const _msg of yield* durableEach("queue", source)) {
            yield* durableEach.next();
          }
        },
        { stream },
      ),
    Error,
    "connection lost",
  );
});

// ---------------------------------------------------------------------------
// Test 8: Advance guard — missing durableEach.next()
// ---------------------------------------------------------------------------

Deno.test("each: advance guard — throws when durableEach.next() is missing", async () => {
  const stream = new InMemoryStream();
  const source = arraySource(["a", "b"]);

  await assertRejects(
    () =>
      durableRun(
        function* () {
          for (const _msg of yield* durableEach("queue", source)) {
            // Missing: yield* durableEach.next();
            // The second iteration should trigger the advance guard
          }
        },
        { stream },
      ),
    Error,
    "yield* durableEach.next() must be called",
  );
});

// ---------------------------------------------------------------------------
// Test 9: Break exits cleanly, source closed
// ---------------------------------------------------------------------------

Deno.test("each: break exits cleanly and closes source", async () => {
  const stream = new InMemoryStream();
  const source = arraySource(["a", "b", "c"]);
  const processed: string[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        processed.push(msg);
        if (msg === "b") break;
        yield* durableEach.next();
      }
      return "stopped";
    },
    { stream },
  );

  assertEquals(result, "stopped");
  assertEquals(processed, ["a", "b"]);
  // Source should be closed via ensure() cleanup
  assertEquals(source.closed, true);
});

// ---------------------------------------------------------------------------
// Test 10: Null values in source — not confused with done signal
// ---------------------------------------------------------------------------

Deno.test("each: null values are valid items, not done signals", async () => {
  const stream = new InMemoryStream();
  const source = arraySource<Json>([null, "after-null", null]);
  const processed: Json[] = [];

  const result = await durableRun(
    function* () {
      for (const msg of yield* durableEach("queue", source)) {
        processed.push(msg);
        yield* durableEach.next();
      }
      return "done";
    },
    { stream },
  );

  assertEquals(result, "done");
  assertEquals(processed, [null, "after-null", null]);

  // Verify journal stores { value: null } not { done: true }
  const events = await stream.readAll();
  const yieldEvents = events.filter((e) => e.type === "yield");
  if (yieldEvents[0]!.type === "yield") {
    assertEquals(yieldEvents[0]!.result, { status: "ok", value: { value: null } });
  }
});

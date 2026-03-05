/**
 * Tier 1 tests — core replay correctness.
 *
 * Tests 1-7 from the protocol specification. These validate that
 * durableRun correctly executes workflows live, replays them from
 * stored events, and handles crash recovery scenarios.
 */

import { assertEquals, assertIsError } from "@std/assert";
import {
  durableCall,
  durableRun,
  InMemoryStream,
  type DurableEvent,
  type Json,
  type Workflow,
} from "../lib/mod.ts";
import { test } from "./test-helpers.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
// Test 1: Golden run — execute workflow end-to-end
// ---------------------------------------------------------------------------

test("golden run: executes all effects live and records events", function* () {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", tracker.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  const result = yield* durableRun(workflow, { stream });

  // Verify result
  assertEquals(result, "alpha-beta");

  // Verify all effects were called
  assertEquals(tracker.calls, ["stepA", "stepB"]);

  // Verify stream has 2 Yield events + 1 Close event
  const events = stream.snapshot();
  assertEquals(events.length, 3);

  assertEquals(events[0]!.type, "yield");
  assertEquals(events[0]!.coroutineId, "root");
  if (events[0]!.type === "yield") {
    assertEquals(events[0]!.description, { type: "call", name: "stepA" });
    assertEquals(events[0]!.result, { status: "ok", value: "alpha" });
  }

  assertEquals(events[1]!.type, "yield");
  if (events[1]!.type === "yield") {
    assertEquals(events[1]!.description, { type: "call", name: "stepB" });
    assertEquals(events[1]!.result, { status: "ok", value: "beta" });
  }

  assertEquals(events[2]!.type, "close");
  assertEquals(events[2]!.coroutineId, "root");
  if (events[2]!.type === "close") {
    assertEquals(events[2]!.result, { status: "ok", value: "alpha-beta" });
  }
});

// ---------------------------------------------------------------------------
// Test 2: Full replay — replay entire stream
// ---------------------------------------------------------------------------

test("full replay: returns stored result without re-executing effects", function* () {
  // Pre-populate stream with a complete run
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepA" },
      result: { status: "ok", value: "alpha" },
    },
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepB" },
      result: { status: "ok", value: "beta" },
    },
    {
      type: "close",
      coroutineId: "root",
      result: { status: "ok", value: "alpha-beta" },
    },
  ];
  const stream = new InMemoryStream(events);
  const tracker = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", tracker.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  const result = yield* durableRun(workflow, { stream });

  // Result comes from the stored Close event
  assertEquals(result, "alpha-beta");

  // No effects were actually called — fully replayed from stored Close
  assertEquals(tracker.calls, []);

  // Stream was not modified (no new events appended)
  assertEquals(stream.appendCount, 0);
});

// ---------------------------------------------------------------------------
// Test 3: Crash before first effect — empty stream
// ---------------------------------------------------------------------------

test("crash before first effect: empty stream, all live", function* () {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "alpha"));
    return a;
  }

  const result = yield* durableRun(workflow, { stream });

  assertEquals(result, "alpha");
  assertEquals(tracker.calls, ["stepA"]);

  const events = stream.snapshot();
  assertEquals(events.length, 2); // 1 Yield + 1 Close
});

// ---------------------------------------------------------------------------
// Test 4: Crash at position N — partial replay
// ---------------------------------------------------------------------------

test("crash at position N: first N replayed, rest live", function* () {
  // Stream has only the first Yield event (simulates crash after stepA)
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepA" },
      result: { status: "ok", value: "alpha" },
    },
  ];
  const stream = new InMemoryStream(events);
  const tracker = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "WRONG"));
    const b = yield* durableCall("stepB", tracker.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  const result = yield* durableRun(workflow, { stream });

  // stepA was replayed (returns stored "alpha", not "WRONG")
  // stepB was executed live
  assertEquals(result, "alpha-beta");

  // Only stepB was actually called
  assertEquals(tracker.calls, ["stepB"]);

  // Stream now has: original Yield(stepA) + new Yield(stepB) + Close
  const finalEvents = stream.snapshot();
  assertEquals(finalEvents.length, 3);
  assertEquals(finalEvents[0]!.type, "yield");
  assertEquals(finalEvents[1]!.type, "yield");
  assertEquals(finalEvents[2]!.type, "close");

  // Only 2 appends: Yield(stepB) + Close
  assertEquals(stream.appendCount, 2);
});

// ---------------------------------------------------------------------------
// Test 5: Crash after last effect — all Yields but no Close
// ---------------------------------------------------------------------------

test("crash after last effect: all Yields replayed, Close appended", function* () {
  // Stream has both Yield events but no Close
  const events: DurableEvent[] = [
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepA" },
      result: { status: "ok", value: "alpha" },
    },
    {
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepB" },
      result: { status: "ok", value: "beta" },
    },
  ];
  const stream = new InMemoryStream(events);
  const tracker = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "WRONG"));
    const b = yield* durableCall("stepB", tracker.fn("stepB", "WRONG"));
    return `${a}-${b}`;
  }

  const result = yield* durableRun(workflow, { stream });

  // Both effects replayed from journal
  assertEquals(result, "alpha-beta");
  assertEquals(tracker.calls, []);

  // Only Close event was appended
  assertEquals(stream.appendCount, 1);
  const finalEvents = stream.snapshot();
  assertEquals(finalEvents.length, 3);
  assertEquals(finalEvents[2]!.type, "close");
});

// ---------------------------------------------------------------------------
// Test 6: Persist-before-resume — write completes before generator advances
// ---------------------------------------------------------------------------

test("persist-before-resume: generator does not advance until write completes", function* () {
  const stream = new InMemoryStream();
  const order: string[] = [];

  // Hook into append to track ordering
  stream.onAppend = (event) => {
    if (event.type === "yield") {
      order.push(`persist:${event.type}`);
    }
  };

  function* workflow(): Workflow<string> {
    yield* durableCall("step1", () => {
      order.push("execute:step1");
      return Promise.resolve("one" as const);
    });
    order.push("resumed:after-step1");

    yield* durableCall("step2", () => {
      order.push("execute:step2");
      return Promise.resolve("two" as const);
    });
    order.push("resumed:after-step2");

    return "done";
  }

  yield* durableRun(workflow, { stream });

  // Verify ordering: execute → persist → resume for each step
  assertEquals(order, [
    "execute:step1",
    "persist:yield",
    "resumed:after-step1",
    "execute:step2",
    "persist:yield",
    "resumed:after-step2",
  ]);
});

// ---------------------------------------------------------------------------
// Test 7: Actor handoff — Process A writes N events, Process B resumes
// ---------------------------------------------------------------------------

test("actor handoff: Process B resumes from Process A's events", function* () {
  // Process A: execute first 2 steps then "crash" (we just take the events)
  const streamA = new InMemoryStream();
  const trackerA = createCallTracker();

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", trackerA.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", trackerA.fn("stepB", "beta"));
    const c = yield* durableCall("stepC", trackerA.fn("stepC", "gamma"));
    return `${a}-${b}-${c}`;
  }

  yield* durableRun(workflow, { stream: streamA });

  // Process A executed all steps
  assertEquals(trackerA.calls, ["stepA", "stepB", "stepC"]);

  // Simulate handoff: take only the first 2 Yield events (no Close, no stepC)
  const allEvents = streamA.snapshot();
  const partialEvents = allEvents.slice(0, 2);

  // Process B: resume with partial events
  const streamB = new InMemoryStream(partialEvents);
  const trackerB = createCallTracker();

  function* workflowB(): Workflow<string> {
    const a = yield* durableCall("stepA", trackerB.fn("stepA", "WRONG"));
    const b = yield* durableCall("stepB", trackerB.fn("stepB", "WRONG"));
    const c = yield* durableCall("stepC", trackerB.fn("stepC", "gamma"));
    return `${a}-${b}-${c}`;
  }

  const result = yield* durableRun(workflowB, { stream: streamB });

  // stepA and stepB replayed, stepC executed live
  assertEquals(result, "alpha-beta-gamma");
  assertEquals(trackerB.calls, ["stepC"]);

  // Stream B: 2 original + 1 new Yield + 1 Close
  const finalEvents = streamB.snapshot();
  assertEquals(finalEvents.length, 4);
});

// ---------------------------------------------------------------------------
// Additional: error propagation
// ---------------------------------------------------------------------------

test("golden run with error: records Close(err) event", function* () {
  const stream = new InMemoryStream();

  function* workflow(): Workflow<string> {
    yield* durableCall("failingStep", () =>
      Promise.reject(new Error("boom")),
    );
    return "unreachable";
  }

  try {
    yield* durableRun(workflow, { stream });
    throw new Error("expected error from durableRun");
  } catch (e) {
    assertIsError(e, Error, "boom");
  }

  // Stream has Yield(err) + Close(err)
  const events = stream.snapshot();
  assertEquals(events.length, 2);

  if (events[0]!.type === "yield") {
    assertEquals(events[0]!.result.status, "err");
  }
  assertEquals(events[1]!.type, "close");
  if (events[1]!.type === "close") {
    assertEquals(events[1]!.result.status, "err");
  }
});

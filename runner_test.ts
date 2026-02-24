import {
  assertEquals,
  assertExists,
  assertRejects,
  assertThrows,
} from "jsr:@std/assert";
import { action, call, type Operation } from "effection";
import { InMemoryDurableStream } from "./stream.ts";
import { durableRun, resetIds, DivergenceError } from "./runner.ts";
import type { DurableEvent } from "./types.ts";

// ── Test Helpers ───────────────────────────────────────────────────

/**
 * Assert that an event exists and has the expected type, then return
 * it narrowed to that type. Fails hard if the event is missing or
 * the type doesn't match — no silent skips.
 */
function expectEvent<T extends DurableEvent["type"]>(
  events: DurableEvent[],
  index: number,
  type: T,
): Extract<DurableEvent, { type: T }> {
  const event = events[index];
  assertExists(event, `expected event at index ${index}, but only ${events.length} events exist`);
  assertEquals(
    event.type,
    type,
    `event[${index}]: expected type '${type}', got '${event.type}'`,
  );
  return event as Extract<DurableEvent, { type: T }>;
}

/**
 * Assert that every effect:resolved / effect:errored references
 * an effectId that was previously yielded. Catches orphaned
 * resolutions and broken ID linkage.
 */
function assertEffectLinkage(events: DurableEvent[]): void {
  const yieldedIds = new Set<string>();
  for (const event of events) {
    if (event.type === "effect:yielded") {
      yieldedIds.add(event.effectId);
    }
    if (event.type === "effect:resolved") {
      assertEquals(
        yieldedIds.has(event.effectId),
        true,
        `effect:resolved references unknown effectId '${event.effectId}'`,
      );
    }
    if (event.type === "effect:errored") {
      assertEquals(
        yieldedIds.has(event.effectId),
        true,
        `effect:errored references unknown effectId '${event.effectId}'`,
      );
    }
  }
}

/**
 * Create a deterministic action that resolves immediately with a value.
 * Use this instead of sleep() in tests to avoid timing dependencies.
 * Returns a flag object so callers can verify whether enter() was called.
 */
function immediateAction<T>(
  value: T,
  description: string,
): { op: Operation<T>; entered: boolean } {
  const flag = { op: null as unknown as Operation<T>, entered: false };
  flag.op = action<T>((resolve) => {
    flag.entered = true;
    resolve(value);
    return () => {};
  }, description);
  return flag;
}

// ── Helper: get events from stream ─────────────────────────────────

function streamEvents(stream: InMemoryDurableStream): DurableEvent[] {
  return stream.read().map((e) => e.event);
}

// ═══════════════════════════════════════════════════════════════════
// Test 1: Basic Workflow — Single Effect and Return
// ═══════════════════════════════════════════════════════════════════

Deno.test("basic workflow writes scope and effect events to stream", async () => {
  resetIds();
  const stream = new InMemoryDurableStream();

  const task = durableRun(stream, function* greet(): Operation<string> {
    yield* action<void>((resolve) => {
      resolve(undefined as never);
      return () => {};
    }, "sleep(10)");
    return "hello";
  });

  const result = await task;
  assertEquals(result, "hello");

  const events = streamEvents(stream);

  // Verify exact event count and types
  assertEquals(events.length, 5);
  assertEquals(
    events.map((e) => e.type),
    ["scope:created", "effect:yielded", "effect:resolved", "workflow:return", "scope:destroyed"],
  );

  // Hard assertions on each event — no silent if-guards
  const created = expectEvent(events, 0, "scope:created");
  const scopeId = created.scopeId; // derive, don't hardcode

  const yielded = expectEvent(events, 1, "effect:yielded");
  assertEquals(yielded.scopeId, scopeId);
  assertEquals(yielded.description, "sleep(10)");

  const resolved = expectEvent(events, 2, "effect:resolved");
  assertEquals(resolved.effectId, yielded.effectId, "resolved must reference yielded effectId");
  assertEquals(resolved.value, null); // undefined → null in JSON

  const ret = expectEvent(events, 3, "workflow:return");
  assertEquals(ret.scopeId, scopeId);
  assertEquals(ret.value, "hello");

  const destroyed = expectEvent(events, 4, "scope:destroyed");
  assertEquals(destroyed.scopeId, scopeId);
  assertEquals(destroyed.result, { ok: true });

  // Structural invariants
  assertEffectLinkage(events);
  assertEquals(stream.closed, true);
});

// ═══════════════════════════════════════════════════════════════════
// Test 2: Multi-step Workflow
// ═══════════════════════════════════════════════════════════════════

Deno.test("multi-step workflow produces sequential effect events", async () => {
  resetIds();
  const stream = new InMemoryDurableStream();

  const task = durableRun(stream, function* fetchAndProcess(): Operation<string> {
    yield* action<void>((resolve) => {
      resolve(undefined as never);
      return () => {};
    }, "sleep(10)");
    const data: string = yield* action<string>((resolve) => {
      resolve("some data");
      return () => {};
    }, "call()");
    return data.toUpperCase();
  });

  const result = await task;
  assertEquals(result, "SOME DATA");

  const events = streamEvents(stream);
  assertEquals(events.length, 7);
  assertEquals(
    events.map((e) => e.type),
    [
      "scope:created",
      "effect:yielded",   // sleep
      "effect:resolved",  // sleep resolved
      "effect:yielded",   // call
      "effect:resolved",  // call resolved
      "workflow:return",
      "scope:destroyed",
    ],
  );

  const created = expectEvent(events, 0, "scope:created");
  const scopeId = created.scopeId;

  // First effect pair
  const y0 = expectEvent(events, 1, "effect:yielded");
  assertEquals(y0.scopeId, scopeId);
  assertEquals(y0.description, "sleep(10)");
  const r0 = expectEvent(events, 2, "effect:resolved");
  assertEquals(r0.effectId, y0.effectId);
  assertEquals(r0.value, null);

  // Second effect pair
  const y1 = expectEvent(events, 3, "effect:yielded");
  assertEquals(y1.scopeId, scopeId);
  assertEquals(y1.description, "call()");
  const r1 = expectEvent(events, 4, "effect:resolved");
  assertEquals(r1.effectId, y1.effectId);
  assertEquals(r1.value, "some data");

  // Effect IDs must be unique
  assertEquals(y0.effectId !== y1.effectId, true, "effect IDs must be unique");

  const ret = expectEvent(events, 5, "workflow:return");
  assertEquals(ret.scopeId, scopeId);
  assertEquals(ret.value, "SOME DATA");

  const destroyed = expectEvent(events, 6, "scope:destroyed");
  assertEquals(destroyed.scopeId, scopeId);
  assertEquals(destroyed.result, { ok: true });

  assertEffectLinkage(events);
  assertEquals(stream.closed, true);
});

// ═══════════════════════════════════════════════════════════════════
// Test: InMemoryDurableStream basics
// ═══════════════════════════════════════════════════════════════════

Deno.test("InMemoryDurableStream append, read, and close", () => {
  const stream = new InMemoryDurableStream();

  assertEquals(stream.length, 0);
  assertEquals(stream.closed, false);

  const offset0 = stream.append({ type: "scope:created", scopeId: "0" });
  assertEquals(offset0, 0);
  assertEquals(stream.length, 1);

  const offset1 = stream.append({
    type: "effect:yielded",
    scopeId: "0",
    effectId: "e0",
    description: "sleep(100)",
  });
  assertEquals(offset1, 1);

  const entries = stream.read();
  assertEquals(entries.length, 2);
  assertEquals(entries[0].offset, 0);
  assertEquals(entries[1].offset, 1);

  // Read from offset
  const fromOne = stream.read(1);
  assertEquals(fromOne.length, 1);
  assertEquals(fromOne[0].event.type, "effect:yielded");

  stream.close();
  assertEquals(stream.closed, true);

  // Append after close must throw
  assertThrows(
    () => stream.append({ type: "scope:created", scopeId: "1" }),
    Error,
    "Cannot append to a closed stream",
  );
});

Deno.test("InMemoryDurableStream.from creates pre-populated stream", () => {
  const events: DurableEvent[] = [
    { type: "scope:created", scopeId: "s0" },
    { type: "effect:yielded", scopeId: "s0", effectId: "e0", description: "test" },
  ];

  const open = InMemoryDurableStream.from(events, false);
  assertEquals(open.length, 2);
  assertEquals(open.closed, false);

  const closed = InMemoryDurableStream.from(events, true);
  assertEquals(closed.length, 2);
  assertEquals(closed.closed, true);
  assertThrows(() => closed.append({ type: "scope:created", scopeId: "s1" }));
});

// ═══════════════════════════════════════════════════════════════════
// Test 3: Resume from Completed Stream — Full Replay
// ═══════════════════════════════════════════════════════════════════

Deno.test("resume from completed stream reconstructs final state", async () => {
  resetIds();

  const stream = InMemoryDurableStream.from([
    { type: "scope:created", scopeId: "0" },
    { type: "effect:yielded", scopeId: "0", effectId: "e0", description: "sleep(100)" },
    { type: "effect:resolved", effectId: "e0", value: null },
    { type: "workflow:return", scopeId: "0", value: "hello" },
    { type: "scope:destroyed", scopeId: "0", result: { ok: true } },
  ], true);

  const lengthBefore = stream.length;
  let enterCalled = false;

  const task = durableRun(stream, function* greet(): Operation<string> {
    yield* action<void>((resolve) => {
      enterCalled = true;
      resolve(undefined as never);
      return () => {};
    }, "sleep(100)");
    return "hello";
  });

  const result = await task;
  assertEquals(result, "hello");

  // KEY: enter() must NOT have been called during replay
  assertEquals(enterCalled, false, "effect.enter() must not be called during replay");

  // KEY: no new events written to a complete stream
  assertEquals(stream.length, lengthBefore, "no new events should be appended to a complete stream");
  assertEquals(stream.closed, true, "stream should remain closed");
});

// ═══════════════════════════════════════════════════════════════════
// Test 4: Resume Mid-Workflow — Replay Then Live
// ═══════════════════════════════════════════════════════════════════

Deno.test("resume mid-workflow replays stored effects then executes live", async () => {
  resetIds();

  const stream = InMemoryDurableStream.from([
    { type: "scope:created", scopeId: "0" },
    { type: "effect:yielded", scopeId: "0", effectId: "e0", description: "sleep(100)" },
    { type: "effect:resolved", effectId: "e0", value: null },
  ], false);

  const lengthBefore = stream.length;
  let sleepEntered = false;
  let callEntered = false;

  const task = durableRun(stream, function* fetchAndProcess(): Operation<string> {
    yield* action<void>((resolve) => {
      sleepEntered = true;
      resolve(undefined as never);
      return () => {};
    }, "sleep(100)");

    const data: string = yield* action<string>((resolve) => {
      callEntered = true;
      resolve("some data");
      return () => {};
    }, "call()");

    return data.toUpperCase();
  });

  const result = await task;
  assertEquals(result, "SOME DATA");

  // KEY: first effect replayed (not entered), second effect live (entered)
  assertEquals(sleepEntered, false, "replayed effect must not call enter()");
  assertEquals(callEntered, true, "live effect must call enter()");

  // Verify new events were appended after the pre-existing ones
  const events = streamEvents(stream);
  assertEquals(events.length > lengthBefore, true, "new events must be appended for live phase");

  assertEquals(
    events.map((e) => e.type),
    [
      "scope:created",    // pre-existing
      "effect:yielded",   // pre-existing (sleep)
      "effect:resolved",  // pre-existing (sleep)
      "effect:yielded",   // live (call)
      "effect:resolved",  // live (call)
      "workflow:return",  // live
      "scope:destroyed",  // live
    ],
  );

  // Verify the live call event references the root scope and has correct data
  const liveYielded = expectEvent(events, 3, "effect:yielded");
  assertEquals(liveYielded.scopeId, "0");
  assertEquals(liveYielded.description, "call()");

  const liveResolved = expectEvent(events, 4, "effect:resolved");
  assertEquals(liveResolved.effectId, liveYielded.effectId, "live resolved must reference live yielded");
  assertEquals(liveResolved.value, "some data");

  const ret = expectEvent(events, 5, "workflow:return");
  assertEquals(ret.value, "SOME DATA");

  const destroyed = expectEvent(events, 6, "scope:destroyed");
  assertEquals(destroyed.result, { ok: true });

  assertEffectLinkage(events);
  assertEquals(stream.closed, true);
});

// ═══════════════════════════════════════════════════════════════════
// Test 10a: Error Handling — Effect Error Captured in Stream
// ═══════════════════════════════════════════════════════════════════

Deno.test("effect error is captured in stream", async () => {
  resetIds();
  const stream = new InMemoryDurableStream();

  // call() wraps an async fn; the error propagates as effect:errored
  await assertRejects(
    async () => {
      await durableRun(stream, function* failingEffect(): Operation<string> {
        yield* call(async () => { throw new Error("fetch failed"); });
        return "unreachable";
      });
    },
    Error,
    "fetch failed",
  );

  const events = streamEvents(stream);

  // Must have at least: scope:created, effect:yielded, effect:errored
  assertEquals(events.length >= 3, true, `expected at least 3 events, got ${events.length}`);

  const created = expectEvent(events, 0, "scope:created");
  const scopeId = created.scopeId;

  const yielded = expectEvent(events, 1, "effect:yielded");
  assertEquals(yielded.scopeId, scopeId);

  const errored = expectEvent(events, 2, "effect:errored");
  assertEquals(errored.effectId, yielded.effectId, "errored must reference yielded effectId");
  assertEquals(errored.error.name, "Error");
  assertEquals(errored.error.message, "fetch failed");

  assertEffectLinkage(events);
});

// ═══════════════════════════════════════════════════════════════════
// Test 10b: Generator Throws Before First Yield
// ═══════════════════════════════════════════════════════════════════

Deno.test("generator throw before first yield writes scope:destroyed and closes stream", async () => {
  resetIds();
  const stream = new InMemoryDurableStream();

  await assertRejects(
    async () => {
      await durableRun(stream, function* (): Operation<string> {
        throw new Error("immediate failure");
      });
    },
    Error,
    "immediate failure",
  );

  const events = streamEvents(stream);

  // Must have scope:created + scope:destroyed with error
  assertEquals(events.length >= 2, true, `expected at least 2 events, got ${events.length}`);

  const created = expectEvent(events, 0, "scope:created");
  const scopeId = created.scopeId;

  // Find scope:destroyed (may not be at index 1 depending on runner internals)
  const destroyedEvent = events.find(
    (e) => e.type === "scope:destroyed" && (e as { scopeId?: string }).scopeId === scopeId,
  );
  assertExists(destroyedEvent, "scope:destroyed event must be written");
  assertEquals(destroyedEvent.type, "scope:destroyed");
  if (destroyedEvent.type === "scope:destroyed") {
    assertEquals(destroyedEvent.result.ok, false, "scope:destroyed must have ok: false");
    assertEquals(destroyedEvent.result.ok === false && destroyedEvent.result.error.message, "immediate failure");
  }

  assertEquals(stream.closed, true, "stream must be closed after error");
});

// ═══════════════════════════════════════════════════════════════════
// Test 14: Divergence Detection
// ═══════════════════════════════════════════════════════════════════

Deno.test("divergence between stored and live effects is detected", async () => {
  resetIds();

  const stream = InMemoryDurableStream.from([
    { type: "scope:created", scopeId: "0" },
    { type: "effect:yielded", scopeId: "0", effectId: "e0", description: "sleep(100)" },
    { type: "effect:resolved", effectId: "e0", value: null },
    { type: "workflow:return", scopeId: "0", value: "v1" },
    { type: "scope:destroyed", scopeId: "0", result: { ok: true } },
  ], true);

  const lengthBefore = stream.length;

  let caught: unknown = undefined;
  try {
    await durableRun(stream, function* workflowV2(): Operation<string> {
      yield* action<void>((resolve) => {
        resolve(undefined as never);
        return () => {};
      }, "sleep(200)"); // Different description!
      return "v2";
    });
    // Must not reach here
    assertEquals(true, false, "should have thrown DivergenceError");
  } catch (e) {
    caught = e;
  }

  // Assert the error type and contents
  assertExists(caught, "DivergenceError must be thrown");
  assertEquals(caught instanceof DivergenceError, true, `expected DivergenceError, got ${(caught as Error)?.constructor?.name}`);
  if (caught instanceof DivergenceError) {
    assertEquals(caught.index, 0);
    assertEquals(caught.expected, "sleep(100)");
    assertEquals(caught.actual, "sleep(200)");
  }

  // No extra events should have been appended
  assertEquals(stream.length, lengthBefore, "no events should be appended after divergence");
  assertEquals(stream.closed, true, "stream should remain closed after divergence");
});

// ═══════════════════════════════════════════════════════════════════
// Test: Replay of Errored Effect
// ═══════════════════════════════════════════════════════════════════

Deno.test("resume replays stored effect error without entering effect", async () => {
  resetIds();

  const stream = InMemoryDurableStream.from([
    { type: "scope:created", scopeId: "0" },
    { type: "effect:yielded", scopeId: "0", effectId: "e0", description: "call()" },
    { type: "effect:errored", effectId: "e0", error: { name: "Error", message: "boom" } },
  ], false);

  let entered = false;

  await assertRejects(
    async () => {
      await durableRun(stream, function* (): Operation<string> {
        yield* action<string>((resolve) => {
          entered = true;
          resolve("should not happen");
          return () => {};
        }, "call()");
        return "unreachable";
      });
    },
    Error,
    "boom",
  );

  // KEY: enter() must NOT have been called
  assertEquals(entered, false, "effect.enter() must not be called when replaying an errored effect");
});

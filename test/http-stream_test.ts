/**
 * Tests for HttpDurableStream — HTTP-backed DurableStream adapter.
 *
 * Uses DurableStreamTestServer from @durable-streams/server for
 * real HTTP round-trips against an in-process server.
 */

import { assertEquals, assertRejects } from "@std/assert";
import { DurableStreamTestServer } from "@durable-streams/server";
import { StaleEpochError } from "@durable-streams/client";
import {
  durableAll,
  durableCall,
  durableRun,
  type Json,
  type Workflow,
} from "../lib/mod.ts";
import { HttpDurableStream } from "../lib/http-stream.ts";

// ---------------------------------------------------------------------------
// Shared test server — started once, reused across tests
// ---------------------------------------------------------------------------

const server = new DurableStreamTestServer();
let baseUrl: string;

// Unique stream counter to avoid collisions between tests
let streamCounter = 0;
function uniqueStreamId(): string {
  return `test-stream-${++streamCounter}-${Date.now()}`;
}

// Start server before all tests, stop after
// Deno runs tests in file order; beforeAll/afterAll is simulated
// by relying on the first test waiting for setup.
const serverReady = (async () => {
  await server.start();
  baseUrl = server.url;
})();

// Register a cleanup handler
addEventListener("unload", () => {
  server.stop();
});

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
// Test 1: Append + readAll round-trip
// ---------------------------------------------------------------------------

Deno.test("http: append + readAll round-trip", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  // Append 3 events
  await stream.append({
    type: "yield",
    coroutineId: "root",
    description: { type: "call", name: "stepA" },
    result: { status: "ok", value: "alpha" },
  });
  await stream.append({
    type: "yield",
    coroutineId: "root",
    description: { type: "call", name: "stepB" },
    result: { status: "ok", value: "beta" },
  });
  await stream.append({
    type: "close",
    coroutineId: "root",
    result: { status: "ok", value: "alpha-beta" },
  });

  // Read all events
  const events = await stream.readAll();
  assertEquals(events.length, 3);
  assertEquals(events[0]!.type, "yield");
  assertEquals(events[0]!.coroutineId, "root");
  assertEquals(events[1]!.type, "yield");
  assertEquals(events[2]!.type, "close");
  if (events[2]!.type === "close") {
    assertEquals(events[2]!.result, { status: "ok", value: "alpha-beta" });
  }
});

// ---------------------------------------------------------------------------
// Test 2: Empty stream readAll
// ---------------------------------------------------------------------------

Deno.test("http: empty stream readAll returns []", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  const events = await stream.readAll();
  assertEquals(events, []);
});

// ---------------------------------------------------------------------------
// Test 3: Idempotent retry — same (id, epoch, seq) → 204
// ---------------------------------------------------------------------------

Deno.test("http: idempotent retry — duplicate append succeeds", async () => {
  await serverReady;
  const streamId = uniqueStreamId();

  // Create two stream instances with same producer identity
  const stream1 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });
  const stream2 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  const event = {
    type: "yield" as const,
    coroutineId: "root" as const,
    description: { type: "call", name: "stepA" },
    result: { status: "ok" as const, value: "alpha" },
  };

  // First append — 200
  await stream1.append(event);

  // Second append with same seq — 204 (idempotent)
  await stream2.append(event);

  // Only one event in the stream
  const events = await stream1.readAll();
  assertEquals(events.length, 1);
});

// ---------------------------------------------------------------------------
// Test 4: Epoch fencing — stale epoch → StaleEpochError + fail-fast
// ---------------------------------------------------------------------------

Deno.test("http: epoch fencing — stale epoch rejects and fail-fast", async () => {
  await serverReady;
  const streamId = uniqueStreamId();

  // First producer with epoch 2
  const stream1 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 2,
  });
  await stream1.append({
    type: "yield",
    coroutineId: "root",
    description: { type: "call", name: "stepA" },
    result: { status: "ok", value: "alpha" },
  });

  // Second producer with stale epoch 1 (same producerId)
  const stream2 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  // Should reject with StaleEpochError
  await assertRejects(
    () =>
      stream2.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepB" },
        result: { status: "ok", value: "beta" },
      }),
    StaleEpochError,
  );

  // Future appends should also fail-fast (without making HTTP call)
  await assertRejects(
    () =>
      stream2.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepC" },
        result: { status: "ok", value: "gamma" },
      }),
    StaleEpochError,
  );
});

// ---------------------------------------------------------------------------
// Test 5: durableRun golden run against HTTP stream
// ---------------------------------------------------------------------------

Deno.test("http: durableRun golden run — full workflow against HTTP", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const tracker = createCallTracker();

  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", tracker.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  const result = await durableRun(workflow, { stream });

  assertEquals(result, "alpha-beta");
  assertEquals(tracker.calls, ["stepA", "stepB"]);

  // Verify events persisted in HTTP stream
  const events = await stream.readAll();
  assertEquals(events.length, 3); // 2 Yield + 1 Close
  assertEquals(events[0]!.type, "yield");
  assertEquals(events[1]!.type, "yield");
  assertEquals(events[2]!.type, "close");
});

// ---------------------------------------------------------------------------
// Test 6: durableRun replay from HTTP stream
// ---------------------------------------------------------------------------

Deno.test("http: durableRun replay — no re-execution from HTTP stream", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const tracker1 = createCallTracker();

  // First run — golden (using epoch 1)
  const stream1 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  function* workflow(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker1.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", tracker1.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  await durableRun(workflow, { stream: stream1 });
  assertEquals(tracker1.calls, ["stepA", "stepB"]);

  // Second run — replay with a fresh stream instance reading the same data
  // Uses epoch 2 so producer doesn't conflict
  const tracker2 = createCallTracker();
  const stream2 = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 2,
  });

  function* workflow2(): Workflow<string> {
    const a = yield* durableCall("stepA", tracker2.fn("stepA", "alpha"));
    const b = yield* durableCall("stepB", tracker2.fn("stepB", "beta"));
    return `${a}-${b}`;
  }

  const result = await durableRun(workflow2, { stream: stream2 });

  // Should return same result without re-executing
  assertEquals(result, "alpha-beta");
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Test 7: Concurrent appends via durableAll
// ---------------------------------------------------------------------------

Deno.test("http: concurrent appends via durableAll — correct ordering", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const tracker = createCallTracker();

  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  const result = await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("fetchA", tracker.fn("fetchA", "alpha"));
        },
        function* () {
          return yield* durableCall("fetchB", tracker.fn("fetchB", "beta"));
        },
        function* () {
          return yield* durableCall("fetchC", tracker.fn("fetchC", "gamma"));
        },
      ]);
      return results.join("-");
    },
    { stream },
  );

  assertEquals(result, "alpha-beta-gamma");

  // Verify all effects were called
  assertEquals(tracker.calls.length, 3);

  // Verify events are in the stream
  const events = await stream.readAll();
  // 3 child Yield + 3 child Close + 1 root Close = 7
  assertEquals(events.length, 7);
});

// ---------------------------------------------------------------------------
// Test 8: Network error propagation
// ---------------------------------------------------------------------------

Deno.test("http: network error propagation — fetch failure rejects append", async () => {
  await serverReady;
  const streamId = uniqueStreamId();

  // Use a custom fetch that fails on POST (append) but succeeds on PUT (create)
  let callCount = 0;
  // deno-lint-ignore no-explicit-any
  const failingFetch = (input: any, init: any) => {
    if (init?.method === "PUT") {
      return globalThis.fetch(input, init);
    }
    callCount++;
    return Promise.reject(new Error("Network failure"));
  };

  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
    fetch: failingFetch,
  });

  await assertRejects(
    () =>
      stream.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepA" },
        result: { status: "ok", value: "alpha" },
      }),
    Error,
    "Network failure",
  );

  assertEquals(callCount, 1);
});

// ---------------------------------------------------------------------------
// Test 9: lastOffset tracked from readAll and append
// ---------------------------------------------------------------------------

Deno.test("http: lastOffset tracked from readAll and append", async () => {
  await serverReady;
  const streamId = uniqueStreamId();
  const stream = await HttpDurableStream.connect({
    baseUrl,
    streamId,
    producerId: "p1",
    epoch: 1,
  });

  // Initially undefined
  assertEquals(stream.lastOffset, undefined);

  // After readAll on empty stream — offset should be set (to start position)
  await stream.readAll();
  // The server returns an offset even for empty streams
  // (it's the "current end of stream" position)
  const offsetAfterEmptyRead = stream.lastOffset;

  // After append — offset should update
  await stream.append({
    type: "yield",
    coroutineId: "root",
    description: { type: "call", name: "stepA" },
    result: { status: "ok", value: "alpha" },
  });
  const offsetAfterAppend = stream.lastOffset;
  assertEquals(typeof offsetAfterAppend, "string");

  // After readAll — offset should be at least as far as append
  await stream.readAll();
  const offsetAfterRead = stream.lastOffset;
  assertEquals(typeof offsetAfterRead, "string");

  // Offsets should be non-empty hex strings
  if (offsetAfterAppend) {
    assertEquals(offsetAfterAppend.includes("_"), true);
  }
  if (offsetAfterRead) {
    assertEquals(offsetAfterRead.includes("_"), true);
  }

  // The read offset should equal or exceed the append offset
  // (they're lexicographically comparable hex strings)
  if (offsetAfterAppend && offsetAfterRead) {
    assertEquals(offsetAfterRead >= offsetAfterAppend, true);
  }

  // Empty read might not have an offset (depends on server implementation)
  // so we just verify that after data exists, offsets are tracked
  if (offsetAfterEmptyRead) {
    assertEquals(typeof offsetAfterEmptyRead, "string");
  }
});

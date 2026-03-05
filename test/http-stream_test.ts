/**
 * Tests for useHttpDurableStream — HTTP-backed DurableStream resource.
 *
 * Uses DurableStreamTestServer from @durable-streams/server for
 * real HTTP round-trips against an in-process server.
 *
 * Each test gets its own server instance (random port) managed as an
 * Effection resource — started before the test body and stopped
 * automatically when the scope is torn down.
 *
 * All stream operations are Operation-native — tests run inside
 * Effection scopes via run().
 */

import { assertEquals, assertInstanceOf } from "@std/assert";
import { call, resource, run, scoped } from "@effection/effection";
import type { Operation } from "@effection/effection";
import { DurableStreamTestServer } from "@durable-streams/server";
import { StaleEpochError } from "@durable-streams/client";
import {
  durableAll,
  durableCall,
  durableRun,
  type Json,
  type Workflow,
} from "../lib/mod.ts";
import { useHttpDurableStream } from "../lib/http-stream.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Unique stream counter to avoid collisions between tests. */
let streamCounter = 0;
function uniqueStreamId(): string {
  return `test-stream-${++streamCounter}-${Date.now()}`;
}

/**
 * Test server resource — starts a DurableStreamTestServer on a random port
 * and stops it when the scope is torn down.
 */
function useTestServer(): Operation<string> {
  return resource(function* (provide) {
    const server = new DurableStreamTestServer({ port: 0 });
    yield* call(() => server.start());
    try {
      yield* provide(server.url);
    } finally {
      server.stop();
    }
  });
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
// Test 1: Append + readAll round-trip
// ---------------------------------------------------------------------------

Deno.test("http: append + readAll round-trip", async () => {
  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
    });

    // Append 3 events
    yield* stream.append({
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepA" },
      result: { status: "ok", value: "alpha" },
    });
    yield* stream.append({
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepB" },
      result: { status: "ok", value: "beta" },
    });
    yield* stream.append({
      type: "close",
      coroutineId: "root",
      result: { status: "ok", value: "alpha-beta" },
    });

    // Read all events
    const events = yield* stream.readAll();
    assertEquals(events.length, 3);
    assertEquals(events[0]!.type, "yield");
    assertEquals(events[0]!.coroutineId, "root");
    assertEquals(events[1]!.type, "yield");
    assertEquals(events[2]!.type, "close");
    if (events[2]!.type === "close") {
      assertEquals(events[2]!.result, { status: "ok", value: "alpha-beta" });
    }
  });
});

// ---------------------------------------------------------------------------
// Test 2: Empty stream readAll
// ---------------------------------------------------------------------------

Deno.test("http: empty stream readAll returns []", async () => {
  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
    });
    const events = yield* stream.readAll();
    assertEquals(events, []);
  });
});

// ---------------------------------------------------------------------------
// Test 3: Idempotent retry — same (id, epoch, seq) → 204
// ---------------------------------------------------------------------------

Deno.test("http: idempotent retry — duplicate append succeeds", async () => {
  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();

    const event = {
      type: "yield" as const,
      coroutineId: "root" as const,
      description: { type: "call", name: "stepA" },
      result: { status: "ok" as const, value: "alpha" },
    };

    // Session 1: first append — 200
    yield* scoped(function* () {
      const stream1 = yield* useHttpDurableStream({
        baseUrl,
        streamId,
        producerId: "p1",
        epoch: 1,
      });
      yield* stream1.append(event);
    });

    // Session 2: same producer identity, same seq — 204 (idempotent)
    yield* scoped(function* () {
      const stream2 = yield* useHttpDurableStream({
        baseUrl,
        streamId,
        producerId: "p1",
        epoch: 1,
      });
      yield* stream2.append(event);

      // Only one event in the stream
      const events = yield* stream2.readAll();
      assertEquals(events.length, 1);
    });
  });
});

// ---------------------------------------------------------------------------
// Test 4: Epoch fencing — stale epoch → StaleEpochError + fail-fast
// ---------------------------------------------------------------------------

Deno.test("http: epoch fencing — stale epoch rejects and fail-fast", async () => {
  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();

    // Session 1: producer with epoch 2
    yield* scoped(function* () {
      const stream1 = yield* useHttpDurableStream({
        baseUrl,
        streamId,
        producerId: "p1",
        epoch: 2,
      });
      yield* stream1.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepA" },
        result: { status: "ok", value: "alpha" },
      });
    });

    // Session 2: stale epoch 1 (same producerId)
    yield* scoped(function* () {
      const stream2 = yield* useHttpDurableStream({
        baseUrl,
        streamId,
        producerId: "p1",
        epoch: 1,
      });

      // Should throw StaleEpochError
      let caughtFirst: Error | undefined;
      try {
        yield* stream2.append({
          type: "yield",
          coroutineId: "root",
          description: { type: "call", name: "stepB" },
          result: { status: "ok", value: "beta" },
        });
      } catch (e) {
        caughtFirst = e as Error;
      }
      assertInstanceOf(caughtFirst, StaleEpochError);

      // Future appends should also fail-fast (without making HTTP call)
      let caughtSecond: Error | undefined;
      try {
        yield* stream2.append({
          type: "yield",
          coroutineId: "root",
          description: { type: "call", name: "stepC" },
          result: { status: "ok", value: "gamma" },
        });
      } catch (e) {
        caughtSecond = e as Error;
      }
      assertInstanceOf(caughtSecond, StaleEpochError);
    });
  });
});

// ---------------------------------------------------------------------------
// Test 5: durableRun golden run against HTTP stream
// ---------------------------------------------------------------------------

Deno.test("http: durableRun golden run — full workflow against HTTP", async () => {
  const tracker = createCallTracker();

  const result = await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
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

    const r = yield* durableRun(workflow, { stream });

    // Verify events persisted in HTTP stream
    const events = yield* stream.readAll();
    assertEquals(events.length, 3); // 2 Yield + 1 Close
    assertEquals(events[0]!.type, "yield");
    assertEquals(events[1]!.type, "yield");
    assertEquals(events[2]!.type, "close");

    return r;
  });

  assertEquals(result, "alpha-beta");
  assertEquals(tracker.calls, ["stepA", "stepB"]);
});

// ---------------------------------------------------------------------------
// Test 6: durableRun replay from HTTP stream
// ---------------------------------------------------------------------------

Deno.test("http: durableRun replay — no re-execution from HTTP stream", async () => {
  const tracker1 = createCallTracker();
  const tracker2 = createCallTracker();

  const result = await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();

    // Session 1 — golden run (epoch 1)
    yield* scoped(function* () {
      const stream1 = yield* useHttpDurableStream({
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

      yield* durableRun(workflow, { stream: stream1 });
    });
    assertEquals(tracker1.calls, ["stepA", "stepB"]);

    // Session 2 — replay with a fresh stream instance reading the same data
    // Uses epoch 2 so producer doesn't conflict
    const r = yield* scoped(function* () {
      const stream2 = yield* useHttpDurableStream({
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

      return yield* durableRun(workflow2, { stream: stream2 });
    });

    return r;
  });

  // Should return same result without re-executing
  assertEquals(result, "alpha-beta");
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Test 7: Concurrent appends via durableAll
// ---------------------------------------------------------------------------

Deno.test("http: concurrent appends via durableAll — correct ordering", async () => {
  const tracker = createCallTracker();

  const result = await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
    });

    const r = yield* durableRun(
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

    // Verify events are in the stream
    const events = yield* stream.readAll();
    // 3 child Yield + 3 child Close + 1 root Close = 7
    assertEquals(events.length, 7);

    return r;
  });

  assertEquals(result, "alpha-beta-gamma");

  // Verify all effects were called
  assertEquals(tracker.calls.length, 3);
});

// ---------------------------------------------------------------------------
// Test 8: Network error propagation
// ---------------------------------------------------------------------------

Deno.test("http: network error propagation — fetch failure rejects append", async () => {
  // Use a custom fetch that fails on POST (append) but succeeds on PUT (create)
  // and GET (readAll)
  let callCount = 0;
  // deno-lint-ignore no-explicit-any
  const failingFetch = (input: any, init: any) => {
    if (init?.method === "PUT") {
      return globalThis.fetch(input, init);
    }
    if (init?.method === "POST") {
      callCount++;
      return Promise.reject(new Error("Network failure"));
    }
    // GET for readAll
    return globalThis.fetch(input, init);
  };

  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
      fetch: failingFetch,
    });

    let caught: Error | undefined;
    try {
      yield* stream.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepA" },
        result: { status: "ok", value: "alpha" },
      });
    } catch (e) {
      caught = e as Error;
    }

    assertEquals(caught?.message, "Network failure");
    assertEquals(callCount, 1);
  });
});

// ---------------------------------------------------------------------------
// Test 9: lastOffset tracked from readAll and append
// ---------------------------------------------------------------------------

Deno.test("http: lastOffset tracked from readAll and append", async () => {
  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
    });

    // Initially undefined
    assertEquals(stream.lastOffset, undefined);

    // After readAll on empty stream — offset should be set (to start position)
    yield* stream.readAll();
    // The server returns an offset even for empty streams
    // (it's the "current end of stream" position)
    const offsetAfterEmptyRead = stream.lastOffset;

    // After append — offset should update
    yield* stream.append({
      type: "yield",
      coroutineId: "root",
      description: { type: "call", name: "stepA" },
      result: { status: "ok", value: "alpha" },
    });
    const offsetAfterAppend = stream.lastOffset;
    assertEquals(typeof offsetAfterAppend, "string");

    // After readAll — offset should be at least as far as append
    yield* stream.readAll();
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
});

// ---------------------------------------------------------------------------
// Test 10: Append failure followed by another append — fail-fast
// ---------------------------------------------------------------------------

Deno.test("http: append failure makes subsequent appends fail-fast", async () => {
  let postCount = 0;
  // deno-lint-ignore no-explicit-any
  const flakyFetch = (input: any, init: any) => {
    if (init?.method === "PUT") {
      return globalThis.fetch(input, init);
    }
    if (init?.method === "POST") {
      postCount++;
      // First POST fails with network error
      return Promise.reject(new Error("Connection reset"));
    }
    // GET for readAll
    return globalThis.fetch(input, init);
  };

  await run(function* () {
    const baseUrl = yield* useTestServer();
    const streamId = uniqueStreamId();
    const stream = yield* useHttpDurableStream({
      baseUrl,
      streamId,
      producerId: "p1",
      epoch: 1,
      fetch: flakyFetch,
    });

    // First append — fails with network error
    let caughtFirst: Error | undefined;
    try {
      yield* stream.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepA" },
        result: { status: "ok", value: "alpha" },
      });
    } catch (e) {
      caughtFirst = e as Error;
    }
    assertEquals(caughtFirst?.message, "Connection reset");
    assertEquals(postCount, 1);

    // Second append — should fail-fast with the same fatal error,
    // without making another HTTP call
    let caughtSecond: Error | undefined;
    try {
      yield* stream.append({
        type: "yield",
        coroutineId: "root",
        description: { type: "call", name: "stepB" },
        result: { status: "ok", value: "beta" },
      });
    } catch (e) {
      caughtSecond = e as Error;
    }
    assertEquals(caughtSecond?.message, "Connection reset");

    // No additional HTTP calls were made
    assertEquals(postCount, 1);
  });
});

// ---------------------------------------------------------------------------
// Test 11: durableRun preserves original error when Close(err) append fails
// ---------------------------------------------------------------------------

Deno.test("http: durableRun preserves original error when close append fails", async () => {
  let postCount = 0;
  // deno-lint-ignore no-explicit-any
  const failAfterTwoFetch = (input: any, init: any) => {
    if (init?.method === "PUT") {
      return globalThis.fetch(input, init);
    }
    if (init?.method === "POST") {
      postCount++;
      // Allow first 2 appends (the Yield events), fail on 3rd (the Close event)
      if (postCount <= 2) {
        return globalThis.fetch(input, init);
      }
      return Promise.reject(new Error("Stream write failed"));
    }
    // GET for readAll
    return globalThis.fetch(input, init);
  };

  let caught: Error | undefined;
  try {
    await run(function* () {
      const baseUrl = yield* useTestServer();
      const streamId = uniqueStreamId();
      const stream = yield* useHttpDurableStream({
        baseUrl,
        streamId,
        producerId: "p1",
        epoch: 1,
        fetch: failAfterTwoFetch,
      });

      yield* durableRun(
        function* (): Workflow<string> {
          yield* durableCall("stepA", () => Promise.resolve("alpha"));
          throw new Error("Workflow kaboom");
        },
        { stream },
      );
    });
  } catch (e) {
    caught = e as Error;
  }

  // The original workflow error is preserved, not replaced by
  // "Stream write failed" from the Close(err) append
  assertEquals(caught?.message, "Workflow kaboom");
});

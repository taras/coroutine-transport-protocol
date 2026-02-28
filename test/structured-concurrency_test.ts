/**
 * Tier 3 tests — structured concurrency.
 *
 * Tests 15-23 from the protocol specification. These validate that
 * durableAll, durableRace, and durableSpawn correctly handle child
 * scope lifecycles, Close events, cancellation, and replay.
 */

import { assertEquals, assertRejects } from "@std/assert";
import {
  durableAll,
  durableCall,
  durableRace,
  durableRun,
  InMemoryStream,
  type Json,
  type Workflow,
} from "../lib/mod.ts";

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
// Test 15: Fork/join — all children complete (golden run)
// ---------------------------------------------------------------------------

Deno.test("all: golden run — all children execute live", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("fetchA", tracker.fn("fetchA", "alpha"));
        },
        function* () {
          return yield* durableCall("fetchB", tracker.fn("fetchB", "beta"));
        },
      ]);
      return `${results[0]}-${results[1]}`;
    },
    { stream },
  );

  assertEquals(result, "alpha-beta");
  assertEquals(tracker.calls.sort(), ["fetchA", "fetchB"]);

  // Verify stream structure: child yields, child Closes, root Close
  const events = await stream.readAll();

  const yieldEvents = events.filter((e) => e.type === "yield");
  const closeEvents = events.filter((e) => e.type === "close");

  assertEquals(yieldEvents.length, 2);
  assertEquals(closeEvents.length, 3); // root.0, root.1, root

  // Child coroutine IDs should be root.0 and root.1
  const childCloses = closeEvents.filter((e) => e.coroutineId !== "root");
  const childIds = childCloses.map((e) => e.coroutineId).sort();
  assertEquals(childIds, ["root.0", "root.1"]);

  // Root Close should be last
  assertEquals(closeEvents[closeEvents.length - 1]!.coroutineId, "root");
});

// ---------------------------------------------------------------------------
// Test 15b: Fork/join — full replay
// ---------------------------------------------------------------------------

Deno.test("all: full replay — returns stored result without re-executing", async () => {
  // First: golden run
  const stream = new InMemoryStream();
  const tracker1 = createCallTracker();

  await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("fetchA", tracker1.fn("fetchA", "alpha"));
        },
        function* () {
          return yield* durableCall("fetchB", tracker1.fn("fetchB", "beta"));
        },
      ]);
      return `${results[0]}-${results[1]}`;
    },
    { stream },
  );

  // Second: replay with the same stream
  const tracker2 = createCallTracker();
  const replayStream = new InMemoryStream(await stream.readAll());

  const result = await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("fetchA", tracker2.fn("fetchA", "WRONG"));
        },
        function* () {
          return yield* durableCall("fetchB", tracker2.fn("fetchB", "WRONG"));
        },
      ]);
      return `${results[0]}-${results[1]}`;
    },
    { stream: replayStream },
  );

  // Result from stored Close event
  assertEquals(result, "alpha-beta");

  // No effects re-executed
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Test 16: Fork/join — partial completion (crash after some children)
// ---------------------------------------------------------------------------

Deno.test("all: partial replay — completed children replayed, incomplete re-execute", async () => {
  // Golden run to capture full stream
  const goldenStream = new InMemoryStream();
  const goldenTracker = createCallTracker();

  await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall(
            "fetchA",
            goldenTracker.fn("fetchA", "alpha"),
          );
        },
        function* () {
          return yield* durableCall(
            "fetchB",
            goldenTracker.fn("fetchB", "beta"),
          );
        },
        function* () {
          return yield* durableCall(
            "fetchC",
            goldenTracker.fn("fetchC", "gamma"),
          );
        },
      ]);
      return results.join("-");
    },
    { stream: goldenStream },
  );

  // Simulate crash: keep events for root.0 and root.1 only
  // (child 2's events and root Close are dropped)
  const allEvents = await goldenStream.readAll();
  const partialEvents = allEvents.filter((e) => {
    if (e.coroutineId === "root") return false;
    if (e.coroutineId === "root.2") return false;
    return true;
  });

  const partialStream = new InMemoryStream(partialEvents);
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("fetchA", tracker.fn("fetchA", "WRONG"));
        },
        function* () {
          return yield* durableCall("fetchB", tracker.fn("fetchB", "WRONG"));
        },
        function* () {
          return yield* durableCall("fetchC", tracker.fn("fetchC", "gamma"));
        },
      ]);
      return results.join("-");
    },
    { stream: partialStream },
  );

  // Children 0,1 replayed from stored Close. Child 2 executed live.
  assertEquals(result, "alpha-beta-gamma");

  // Only fetchC was actually called
  assertEquals(tracker.calls, ["fetchC"]);
});

// ---------------------------------------------------------------------------
// Test 17: Nested scopes — inner all inside outer all
// ---------------------------------------------------------------------------

Deno.test("all: nested — inner all inside outer all", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      const results = yield* durableAll([
        function* () {
          // Child 0: has its own nested all
          const inner = yield* durableAll([
            function* () {
              return yield* durableCall("innerA", tracker.fn("innerA", "a"));
            },
            function* () {
              return yield* durableCall("innerB", tracker.fn("innerB", "b"));
            },
          ]);
          return inner.join("+") as string;
        },
        function* () {
          return yield* durableCall("outerB", tracker.fn("outerB", "B"));
        },
      ]);
      return results.join("-");
    },
    { stream },
  );

  assertEquals(result, "a+b-B");
  assertEquals(tracker.calls.sort(), ["innerA", "innerB", "outerB"]);

  // Verify nested coroutine IDs
  const events = await stream.readAll();
  const coroutineIds = [...new Set(events.map((e) => e.coroutineId))].sort();
  assertEquals(coroutineIds, [
    "root",
    "root.0",
    "root.0.0",
    "root.0.1",
    "root.1",
  ]);
});

// ---------------------------------------------------------------------------
// Test 18: Race — first to complete wins, others cancelled
// ---------------------------------------------------------------------------

Deno.test("race: golden run — first to complete wins", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("fast", tracker.fn("fast", "winner"));
        },
        function* () {
          yield* durableCall(
            "slow-step1",
            tracker.fn("slow-step1", "partial"),
          );
          return yield* durableCall(
            "slow-step2",
            tracker.fn("slow-step2", "would-not-reach"),
          );
        },
      ]);
    },
    { stream },
  );

  assertEquals(result, "winner");

  // Verify Close events for the winner
  const events = await stream.readAll();
  const closeEvents = events.filter((e) => e.type === "close");

  const winnerClose = closeEvents.find((e) => e.coroutineId === "root.0");
  assertEquals(winnerClose !== undefined, true);
  if (winnerClose?.type === "close") {
    assertEquals(winnerClose.result.status, "ok");
  }
});

// ---------------------------------------------------------------------------
// Test 19: Race full replay
// ---------------------------------------------------------------------------

Deno.test("race: full replay — returns stored result without re-executing", async () => {
  const stream = new InMemoryStream();
  const tracker1 = createCallTracker();

  const result1 = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("fast", tracker1.fn("fast", "winner"));
        },
        function* () {
          yield* durableCall(
            "slow-step1",
            tracker1.fn("slow-step1", "partial"),
          );
          return yield* durableCall(
            "slow-step2",
            tracker1.fn("slow-step2", "loser"),
          );
        },
      ]);
    },
    { stream },
  );

  // Replay
  const tracker2 = createCallTracker();
  const replayStream = new InMemoryStream(await stream.readAll());

  const result2 = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("fast", tracker2.fn("fast", "WRONG"));
        },
        function* () {
          yield* durableCall(
            "slow-step1",
            tracker2.fn("slow-step1", "WRONG"),
          );
          return yield* durableCall(
            "slow-step2",
            tracker2.fn("slow-step2", "WRONG"),
          );
        },
      ]);
    },
    { stream: replayStream },
  );

  assertEquals(result2, result1);
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Test 20: Error in child — siblings cancelled, error propagated
// ---------------------------------------------------------------------------

Deno.test("all: child error — siblings cancelled, error propagated", async () => {
  const stream = new InMemoryStream();

  await assertRejects(
    () =>
      durableRun(
        function* () {
          const results = yield* durableAll([
            function* () {
              return yield* durableCall<string>("good", () =>
                Promise.resolve("ok"),
              );
            },
            function* () {
              yield* durableCall<string>("failStep", () =>
                Promise.reject(new Error("child-boom")),
              );
              return "unreachable";
            },
          ]);
          return results.join("-");
        },
        { stream },
      ),
    Error,
    "child-boom",
  );

  // The stream should contain Close(err) for the failing child
  const events = await stream.readAll();
  const errCloses = events.filter(
    (e) => e.type === "close" && e.result.status === "err",
  );
  assertEquals(errCloses.length >= 1, true);
});

// ---------------------------------------------------------------------------
// Test 21: Error boundary — parent catches child error
// ---------------------------------------------------------------------------

Deno.test("all: error boundary — parent catches child error", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      try {
        yield* durableAll([
          function* () {
            return yield* durableCall<string>("good", tracker.fn("good", "ok"));
          },
          function* (): Workflow<string> {
            yield* durableCall<string>("failStep", () =>
              Promise.reject(new Error("child-caught")),
            );
            return "unreachable";
          },
        ]);
      } catch {
        // Error caught — continue
      }
      const recovery = yield* durableCall(
        "recovery",
        tracker.fn("recovery", "recovered"),
      );
      return recovery;
    },
    { stream },
  );

  assertEquals(result, "recovered");
  assertEquals(tracker.calls.includes("recovery"), true);
});

// ---------------------------------------------------------------------------
// Test 22: Race — winner Close(ok), loser gets Close event
// ---------------------------------------------------------------------------

Deno.test("race: winner Close(ok), loser gets Close(cancelled)", async () => {
  const stream = new InMemoryStream();

  const result = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("instant", () =>
            Promise.resolve("won"),
          );
        },
        function* () {
          return yield* durableCall("never", () =>
            new Promise<string>(() => {
              /* never resolves */
            }),
          );
        },
      ]);
    },
    { stream },
  );

  assertEquals(result, "won");

  const events = await stream.readAll();
  const closeEvents = events.filter((e) => e.type === "close");

  // Winner Close(ok)
  const winnerClose = closeEvents.find((e) => e.coroutineId === "root.0");
  assertEquals(winnerClose !== undefined, true);
  if (winnerClose?.type === "close") {
    assertEquals(winnerClose.result.status, "ok");
  }

  // Loser Close(cancelled) — Effection cancels losers, runDurableChild
  // detects this via the undefined closeEvent path in finally.
  const loserClose = closeEvents.find((e) => e.coroutineId === "root.1");
  assertEquals(loserClose !== undefined, true);
  if (loserClose?.type === "close") {
    assertEquals(loserClose.result.status, "cancelled");
  }

  // Causal ordering: child Closes before root Close
  const rootCloseIdx = closeEvents.findIndex((e) => e.coroutineId === "root");
  const childCloseIdxs = closeEvents
    .map((e, i) => (e.coroutineId !== "root" ? i : -1))
    .filter((i) => i >= 0);
  for (const childIdx of childCloseIdxs) {
    assertEquals(childIdx < rootCloseIdx, true,
      `child Close at index ${childIdx} should precede root Close at ${rootCloseIdx}`);
  }
});

// ---------------------------------------------------------------------------
// Test 23: Race replay — full replay (all events including root Close)
// ---------------------------------------------------------------------------

Deno.test("race: full replay — returns stored result without re-executing", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result1 = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          const a = yield* durableCall("winA", tracker.fn("winA", "a"));
          return yield* durableCall("winB", tracker.fn("winB", a + "-b"));
        },
        function* () {
          yield* durableCall("loseA", tracker.fn("loseA", "x"));
          return yield* durableCall("loseB", tracker.fn("loseB", "y"));
        },
      ]);
    },
    { stream },
  );

  // Replay with complete journal (root Close present)
  const replayStream = new InMemoryStream(await stream.readAll());
  const tracker2 = createCallTracker();

  const result2 = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          yield* durableCall("winA", tracker2.fn("winA", "WRONG"));
          return yield* durableCall("winB", tracker2.fn("winB", "WRONG"));
        },
        function* () {
          yield* durableCall("loseA", tracker2.fn("loseA", "WRONG"));
          return yield* durableCall("loseB", tracker2.fn("loseB", "WRONG"));
        },
      ]);
    },
    { stream: replayStream },
  );

  assertEquals(result2, result1);
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Test 23b: Race partial replay — root Close stripped, cancelled losers replayed
// ---------------------------------------------------------------------------

Deno.test("race: partial replay — cancelled loser replays via suspend", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("fast", tracker.fn("fast", "winner"));
        },
        function* () {
          yield* durableCall("slowStep", tracker.fn("slowStep", "partial"));
          return yield* durableCall("slowStep2", tracker.fn("slowStep2", "never"));
        },
      ]);
    },
    { stream },
  );

  // Simulate crash: strip root Close but keep everything else
  // (winner Close(ok), loser Close(cancelled), winner yield, loser yield)
  const allEvents = await stream.readAll();
  const partialEvents = allEvents.filter((e) => e.coroutineId !== "root");

  const partialStream = new InMemoryStream(partialEvents);
  const tracker2 = createCallTracker();

  // Replay: winner replays from Close(ok), loser sees Close(cancelled)
  // and suspends (blocks until parent race cancels it naturally).
  const result = await durableRun(
    function* () {
      return yield* durableRace([
        function* () {
          return yield* durableCall("fast", tracker2.fn("fast", "WRONG"));
        },
        function* () {
          yield* durableCall("slowStep", tracker2.fn("slowStep", "WRONG"));
          return yield* durableCall("slowStep2", tracker2.fn("slowStep2", "WRONG"));
        },
      ]);
    },
    { stream: partialStream },
  );

  // Winner's result replayed from journal
  assertEquals(result, "winner");
  // No effects re-executed — winner replayed from Close, loser suspended
  assertEquals(tracker2.calls, []);
});

// ---------------------------------------------------------------------------
// Mixed: durableCall then durableAll
// ---------------------------------------------------------------------------

Deno.test("mixed: durableCall then durableAll", async () => {
  const stream = new InMemoryStream();
  const tracker = createCallTracker();

  const result = await durableRun(
    function* () {
      const prefix = yield* durableCall("prefix", tracker.fn("prefix", "PRE"));
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("A", tracker.fn("A", "a"));
        },
        function* () {
          return yield* durableCall("B", tracker.fn("B", "b"));
        },
      ]);
      return `${prefix}-${results.join(",")}`;
    },
    { stream },
  );

  assertEquals(result, "PRE-a,b");

  // Replay
  const replayStream = new InMemoryStream(await stream.readAll());
  const tracker2 = createCallTracker();

  const result2 = await durableRun(
    function* () {
      const prefix = yield* durableCall(
        "prefix",
        tracker2.fn("prefix", "WRONG"),
      );
      const results = yield* durableAll([
        function* () {
          return yield* durableCall("A", tracker2.fn("A", "WRONG"));
        },
        function* () {
          return yield* durableCall("B", tracker2.fn("B", "WRONG"));
        },
      ]);
      return `${prefix}-${results.join(",")}`;
    },
    { stream: replayStream },
  );

  assertEquals(result2, "PRE-a,b");
  assertEquals(tracker2.calls, []);
});

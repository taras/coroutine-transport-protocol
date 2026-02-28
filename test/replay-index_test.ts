/**
 * ReplayIndex unit tests.
 *
 * Tests the spec-compliant replay index (§4.1) in isolation.
 * No Effection dependency — pure data structure.
 */

import { assertEquals } from "@std/assert";
import { ReplayIndex } from "../lib/replay-index.ts";
import type { DurableEvent } from "../lib/types.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function yieldEvent(
  coroutineId: string,
  type: string,
  name: string,
  value?: unknown,
): DurableEvent {
  return {
    type: "yield",
    coroutineId,
    description: { type, name },
    result: { status: "ok", value: value as undefined },
  };
}

function closeEvent(
  coroutineId: string,
  status: "ok" | "err" | "cancelled" = "ok",
  value?: unknown,
): DurableEvent {
  if (status === "ok") {
    return { type: "close", coroutineId, result: { status: "ok", value: value as undefined } };
  }
  if (status === "err") {
    return {
      type: "close",
      coroutineId,
      result: { status: "err", error: { message: String(value ?? "error") } },
    };
  }
  return { type: "close", coroutineId, result: { status: "cancelled" } };
}

// ---------------------------------------------------------------------------
// Empty index
// ---------------------------------------------------------------------------

Deno.test("empty index: peekYield returns undefined", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.peekYield("root"), undefined);
  assertEquals(idx.peekYield("root.0"), undefined);
});

Deno.test("empty index: hasClose returns false", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.hasClose("root"), false);
});

Deno.test("empty index: getClose returns undefined", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.getClose("root"), undefined);
});

Deno.test("empty index: isFullyReplayed returns false", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.isFullyReplayed("root"), false);
});

Deno.test("empty index: getCursor returns 0", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.getCursor("root"), 0);
});

Deno.test("empty index: yieldCount returns 0", () => {
  const idx = new ReplayIndex([]);
  assertEquals(idx.yieldCount("root"), 0);
});

// ---------------------------------------------------------------------------
// Single coroutine, single yield
// ---------------------------------------------------------------------------

Deno.test("single yield: peekYield returns the entry", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetchOrder", 42),
  ]);
  const entry = idx.peekYield("root.0");
  assertEquals(entry?.description, { type: "call", name: "fetchOrder" });
  assertEquals(entry?.result, { status: "ok", value: 42 });
});

Deno.test("single yield: consumeYield advances cursor", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetchOrder", 42),
  ]);
  assertEquals(idx.getCursor("root.0"), 0);
  idx.consumeYield("root.0");
  assertEquals(idx.getCursor("root.0"), 1);
  assertEquals(idx.peekYield("root.0"), undefined);
});

Deno.test("single yield: yieldCount is 1", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetchOrder"),
  ]);
  assertEquals(idx.yieldCount("root.0"), 1);
});

// ---------------------------------------------------------------------------
// Multiple yields, single coroutine
// ---------------------------------------------------------------------------

Deno.test("multiple yields: cursor advances through sequence", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "sleep", "sleep"),
    yieldEvent("root.0", "call", "transform", "ALPHA"),
  ]);

  assertEquals(idx.yieldCount("root.0"), 2);

  // First peek
  assertEquals(idx.peekYield("root.0")?.description, { type: "sleep", name: "sleep" });
  idx.consumeYield("root.0");

  // Second peek
  assertEquals(idx.peekYield("root.0")?.description, { type: "call", name: "transform" });
  assertEquals(idx.peekYield("root.0")?.result, { status: "ok", value: "ALPHA" });
  idx.consumeYield("root.0");

  // Exhausted
  assertEquals(idx.peekYield("root.0"), undefined);
  assertEquals(idx.getCursor("root.0"), 2);
});

// ---------------------------------------------------------------------------
// Close events
// ---------------------------------------------------------------------------

Deno.test("close event: hasClose returns true", () => {
  const idx = new ReplayIndex([
    closeEvent("root.0", "ok", "done"),
  ]);
  assertEquals(idx.hasClose("root.0"), true);
});

Deno.test("close event: getClose returns the event", () => {
  const close = closeEvent("root.0", "ok", "done");
  const idx = new ReplayIndex([close]);
  assertEquals(idx.getClose("root.0"), close);
});

Deno.test("close cancelled: getClose returns cancelled result", () => {
  const close = closeEvent("root.0", "cancelled");
  const idx = new ReplayIndex([close]);
  assertEquals(idx.getClose("root.0")?.result, { status: "cancelled" });
});

Deno.test("close error: getClose returns error result", () => {
  const close = closeEvent("root.0", "err", "boom");
  const idx = new ReplayIndex([close]);
  assertEquals(idx.getClose("root.0")?.result, {
    status: "err",
    error: { message: "boom" },
  });
});

// ---------------------------------------------------------------------------
// isFullyReplayed
// ---------------------------------------------------------------------------

Deno.test("isFullyReplayed: true when all yields consumed and close exists", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetch", 1),
    yieldEvent("root.0", "call", "transform", 2),
    closeEvent("root.0", "ok", "done"),
  ]);

  assertEquals(idx.isFullyReplayed("root.0"), false); // yields not consumed
  idx.consumeYield("root.0");
  assertEquals(idx.isFullyReplayed("root.0"), false); // 1 yield remaining
  idx.consumeYield("root.0");
  assertEquals(idx.isFullyReplayed("root.0"), true); // all consumed + close exists
});

Deno.test("isFullyReplayed: false when yields consumed but no close", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetch", 1),
  ]);

  idx.consumeYield("root.0");
  assertEquals(idx.isFullyReplayed("root.0"), false); // no close
});

Deno.test("isFullyReplayed: false when close exists but yields not consumed", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "call", "fetch", 1),
    closeEvent("root.0"),
  ]);

  assertEquals(idx.isFullyReplayed("root.0"), false);
});

// ---------------------------------------------------------------------------
// Multiple coroutines (interleaved events)
// ---------------------------------------------------------------------------

Deno.test("interleaved: per-coroutine cursors are independent", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0.0", "call", "fetchUser", { name: "alice" }),
    yieldEvent("root.0.1", "call", "fetchUser", { name: "bob" }),
    closeEvent("root.0.0", "ok", { name: "alice" }),
    closeEvent("root.0.1", "ok", { name: "bob" }),
    yieldEvent("root.0", "call", "merge", "merged"),
    closeEvent("root.0", "ok", "done"),
  ]);

  // Each coroutine has its own cursor
  assertEquals(idx.peekYield("root.0.0")?.description.name, "fetchUser");
  assertEquals(idx.peekYield("root.0.1")?.description.name, "fetchUser");
  assertEquals(idx.peekYield("root.0")?.description.name, "merge");

  // Consuming one doesn't affect others
  idx.consumeYield("root.0.0");
  assertEquals(idx.peekYield("root.0.0"), undefined);
  assertEquals(idx.peekYield("root.0.1")?.description.name, "fetchUser");
  assertEquals(idx.peekYield("root.0")?.description.name, "merge");

  // Full replay status
  assertEquals(idx.isFullyReplayed("root.0.0"), true); // consumed + close
  assertEquals(idx.isFullyReplayed("root.0.1"), false); // not consumed
  assertEquals(idx.isFullyReplayed("root.0"), false); // not consumed
});

// ---------------------------------------------------------------------------
// Race scenario (from spec §10)
// ---------------------------------------------------------------------------

Deno.test("race scenario: partial execution with cancellation", () => {
  // From spec §10.1: race([op1, op2]) where op1 wins after op2 partially executed
  const idx = new ReplayIndex([
    yieldEvent("root.0.1", "call", "step1", null),    // op2's first effect
    yieldEvent("root.0.0", "call", "fetch", "data"),   // op1 completes
    closeEvent("root.0.0", "ok", "data"),               // op1 done
    closeEvent("root.0.1", "cancelled"),                 // op2 cancelled
    closeEvent("root.0", "ok", "data"),                  // race returns op1's result
  ]);

  // op1 (root.0.0): one yield, then close(ok)
  assertEquals(idx.yieldCount("root.0.0"), 1);
  assertEquals(idx.hasClose("root.0.0"), true);
  assertEquals(idx.getClose("root.0.0")?.result.status, "ok");

  // op2 (root.0.1): one yield, then close(cancelled)
  assertEquals(idx.yieldCount("root.0.1"), 1);
  assertEquals(idx.hasClose("root.0.1"), true);
  assertEquals(idx.getClose("root.0.1")?.result.status, "cancelled");

  // race scope (root.0): no yields, just close
  assertEquals(idx.yieldCount("root.0"), 0);
  assertEquals(idx.hasClose("root.0"), true);

  // After consuming op2's yield, it's fully replayed (close exists)
  idx.consumeYield("root.0.1");
  assertEquals(idx.isFullyReplayed("root.0.1"), true);
});

// ---------------------------------------------------------------------------
// Consuming yields on unknown coroutine
// ---------------------------------------------------------------------------

Deno.test("consuming yield on unknown coroutine: cursor still advances", () => {
  const idx = new ReplayIndex([]);
  idx.consumeYield("nonexistent");
  assertEquals(idx.getCursor("nonexistent"), 1);
  assertEquals(idx.peekYield("nonexistent"), undefined);
});

// ---------------------------------------------------------------------------
// Sequential workflow (from spec §11.4)
// ---------------------------------------------------------------------------

Deno.test("sequential workflow: matches spec §11.4 example", () => {
  const idx = new ReplayIndex([
    yieldEvent("root.0", "sleep", "sleep"),
    yieldEvent("root.0", "call", "transform", "ALPHA"),
    closeEvent("root.0", "ok", "ALPHA"),
    closeEvent("root", "ok", "ALPHA"),
  ]);

  // root.0 has 2 yields
  assertEquals(idx.yieldCount("root.0"), 2);

  // Consume both
  idx.consumeYield("root.0");
  idx.consumeYield("root.0");
  assertEquals(idx.isFullyReplayed("root.0"), true);

  // root has 0 yields but has a close
  assertEquals(idx.yieldCount("root"), 0);
  assertEquals(idx.isFullyReplayed("root"), true);
});

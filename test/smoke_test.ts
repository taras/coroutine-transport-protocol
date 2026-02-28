/**
 * Smoke test to verify project scaffolding works.
 */

import { assertEquals } from "@std/assert";
import { ReplayIndex, InMemoryStream } from "../lib/mod.ts";
import type { DurableEvent } from "../lib/mod.ts";

Deno.test("ReplayIndex can be constructed with empty events", () => {
  const index = new ReplayIndex([]);
  assertEquals(index.peekYield("root"), undefined);
  assertEquals(index.hasClose("root"), false);
  assertEquals(index.isFullyReplayed("root"), false);
});

Deno.test("InMemoryStream starts empty", async () => {
  const stream = new InMemoryStream();
  const events = await stream.readAll();
  assertEquals(events, []);
});

Deno.test("InMemoryStream stores and retrieves events", async () => {
  const stream = new InMemoryStream();
  const event: DurableEvent = {
    type: "yield",
    coroutineId: "root.0",
    description: { type: "call", name: "fetchOrder" },
    result: { status: "ok", value: 42 },
  };
  await stream.append(event);
  const events = await stream.readAll();
  assertEquals(events.length, 1);
  assertEquals(events[0], event);
  assertEquals(stream.appendCount, 1);
});

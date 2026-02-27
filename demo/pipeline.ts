/**
 * Durable Effection Demo — Multi-step Data Pipeline
 *
 * This workflow fetches data, processes each item with a delay,
 * and aggregates the results. It has ~9 yield points (call + sleep effects),
 * giving ample opportunity to interrupt mid-execution.
 *
 * Run 1 (recording):
 *   pnpm demo:run
 *   # Ctrl+C after processing a few items
 *
 * Run 2 (replay + resume):
 *   pnpm demo:run
 *   # Replayed steps complete instantly, then continues live
 *
 * Prerequisites:
 *   pnpm demo:server  (in another terminal)
 *
 * Usage:
 *   pnpm demo:run
 */

import { main, sleep, call, suspend } from "effection";
import type { Operation } from "effection";
import { durably } from "@effectionx/durably";
import { useDurableStream } from "@effectionx/durably/http";

const STREAM_URL = "http://localhost:4437/durable-effection-demo";

function* pipeline(): Operation<void> {
  let start = Date.now();

  console.log("\n--- Durable Pipeline Demo ---\n");

  // Step 1: Fetch data (single call effect)
  console.log("[Step 1] Fetching data...");
  let data = yield* call(async () => {
    return { items: ["alpha", "beta", "gamma", "delta"] };
  });
  console.log(`[Step 1] Got ${data.items.length} items\n`);

  // Step 2: Process each item (sleep + call per item)
  console.log("[Step 2] Processing items...");
  let results: string[] = [];
  for (let item of data.items) {
    yield* sleep(2000); // 2s per item — gives time to Ctrl+C
    let processed = yield* call(async () => item.toUpperCase());
    results.push(processed);
    let elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`  [${elapsed}s] Processed: ${item} -> ${processed}`);
  }

  // Step 3: Aggregate results
  console.log("\n[Step 3] Aggregating results...");
  yield* sleep(1000);
  let final = results.join(", ");

  console.log(`\n=== Pipeline complete: ${final} ===`);
  let totalTime = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`=== Total time: ${totalTime}s ===\n`);
}

await main(function* () {
  let stream = yield* useDurableStream(STREAM_URL);

  yield* durably(() => pipeline(), { stream });
});

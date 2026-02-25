/**
 * Durable Effection Demo — Nested each() + deterministic retries/timeouts
 *
 * This example keeps the original pipeline demo intact and adds a richer workflow
 * with explicit nested `each()` loops.
 *
 * Run 1:
 *   deno task demo:nested-each
 *   # Ctrl+C mid-run (for example during batch-b)
 *
 * Run 2:
 *   deno task demo:nested-each
 *   # replayed prefix is instant, then frontier continues live
 *
 * Prerequisites:
 *   deno task demo:server
 */
// deno-lint-ignore-file

import { action, each, main, sleep, suspend } from "effection";
import type { Operation, Stream } from "effection";
import { durably } from "@effectionx/durably";
import { useDurableStream } from "@effectionx/durably/http";

const STREAM_URL = "http://localhost:4437/durable-effection-nested-each-demo-v4";
const REQUEST_TIMEOUT_MS = 1100;

interface Batch {
  id: string;
  items: string[];
}

interface ItemFailure {
  item: string;
  reason: string;
}

const BATCHES: Batch[] = [
  { id: "batch-a", items: ["alpha", "beta", "gamma"] },
  { id: "batch-b", items: ["delta", "epsilon", "zeta"] },
  { id: "batch-c", items: ["eta", "theta"] },
];

function asyncBatches(input: Batch[]): Stream<Batch, void> {
  return {
    *[Symbol.iterator]() {
      let pending = input.slice();
      return {
        *next(): Operation<IteratorResult<Batch, void>> {
          // A small delay per batch creates stable interruption points.
          yield* sleep(650);
          let next = pending.shift();
          if (next) {
            return { done: false, value: next };
          }
          return { done: true, value: undefined };
        },
      };
    },
  };
}

function syncItems(input: string[]): Stream<string, void> {
  return {
    *[Symbol.iterator]() {
      let pending = input.slice();
      return {
        *next(): Operation<IteratorResult<string, void>> {
          let next = pending.shift();
          if (typeof next !== "undefined") {
            return { done: false, value: next };
          }
          return { done: true, value: undefined };
        },
      };
    },
  };
}

function simulatedDelayMs(item: string, attempt: number): number {
  if (item === "gamma" && attempt === 1) return 1400;
  if (item === "zeta" && attempt <= 2) return 1500;
  return 240 + attempt * 140;
}

function shouldTransientFail(item: string, attempt: number): boolean {
  return (
    (item === "epsilon" && attempt === 1) ||
    (item === "theta" && attempt <= 2)
  );
}

function* remoteTransform(
  batchId: string,
  item: string,
  attempt: number,
): Operation<string> {
  let description = `${batchId}/${item}/attempt-${attempt}`;
  return yield* action<string>((resolve, reject) => {
    if (shouldTransientFail(item, attempt)) {
      reject(new Error(`transient upstream error (${description})`));
    } else {
      resolve(`${batchId}:${item.toUpperCase()}`);
    }
    return () => {};
  }, `remote:${description}`);
}

function* processItem(batchId: string, item: string): Operation<string> {
  let maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      let delay = simulatedDelayMs(item, attempt);
      yield* sleep(delay);

      if (delay > REQUEST_TIMEOUT_MS) {
        throw new Error(`request timed out after ${REQUEST_TIMEOUT_MS}ms`);
      }

      let result = yield* remoteTransform(batchId, item, attempt);
      return result;
    } catch (error) {
      if (attempt === maxAttempts) {
        throw error;
      }
      yield* sleep(220 * attempt);
    }
  }

  throw new Error("unreachable");
}

function* nestedEachPipeline(): Operation<void> {
  let start = Date.now();
  let completed: string[] = [];
  let failed: ItemFailure[] = [];

  console.log("\n--- Durable Pipeline Demo (nested each) ---\n");

  for (let batch of yield* each(asyncBatches(BATCHES))) {
    console.log(`[Batch] ${batch.id} (${batch.items.length} items)`);

    for (let item of yield* each(syncItems(batch.items))) {
      let elapsed = ((Date.now() - start) / 1000).toFixed(1);
      console.log(`  [${elapsed}s] -> ${batch.id}/${item}`);

      try {
        let output = yield* processItem(batch.id, item);
        completed.push(output);
        let t = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`  [${t}s] ok   ${output}`);
      } catch (error) {
        let reason = error instanceof Error ? error.message : String(error);
        failed.push({ item: `${batch.id}/${item}`, reason });
        let t = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`  [${t}s] fail ${batch.id}/${item} -> ${reason}`);
      }

      // Explicitly advance each() to make iteration boundaries visible.
      yield* each.next();
    }

    // Explicit outer-loop boundary between batches.
    yield* each.next();
  }

  console.log("\n[Summary]");
  console.log(`  completed: ${completed.length}`);
  console.log(`  failed:    ${failed.length}`);
  console.log(`  outputs:   ${completed.join(", ")}`);

  if (failed.length > 0) {
    console.log("  failures:");
    for (let entry of failed) {
      console.log(`    - ${entry.item}: ${entry.reason}`);
    }
  }

  let total = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n=== Nested each demo complete in ${total}s ===\n`);
}

await main(function* () {
  let stream = yield* useDurableStream(STREAM_URL);
  yield* durably(() => nestedEachPipeline(), { stream });
  
  yield* suspend();
});

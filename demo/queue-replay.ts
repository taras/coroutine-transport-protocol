/**
 * Queue Replay Demo — Exploring ephemeral state during durable execution
 *
 * This demo explores what happens to in-memory queue state during replay.
 *
 * Key observations:
 * - queue.add() is synchronous — always executes, even during replay
 * - queue.next() is an action effect — recorded/replayed
 * - Keypress waits are action effects — recorded/replayed
 *
 * Run 1:
 *   pnpm demo:queue
 *   # Press keys to add items, Ctrl+C to interrupt
 *
 * Run 2:
 *   pnpm demo:queue
 *   # Watch replay behavior — keypresses replay instantly
 *
 * Prerequisites:
 *   pnpm demo:server
 */

import { main, createQueue, type Operation } from "effection";
import { durably } from "@effectionx/durably";
import { useDurableStream } from "@effectionx/durably/http";
import { once } from "@effectionx/node";

const STREAM_URL = "http://localhost:4437/queue-replay-demo";
const ITERATIONS = 10;

function* waitForKeypress(prompt: string): Operation<string> {
  console.log(prompt);

  const { stdin } = process;
  const wasRaw = stdin.isRaw;

  stdin.setRawMode(true);
  stdin.resume();
  stdin.setEncoding("utf8");

  try {
    const [key] = yield* once<[string]>(stdin, "data");

    // Ctrl+C
    if (key === "\u0003") {
      process.exit();
    }

    return key;
  } finally {
    stdin.setRawMode(wasRaw);
    stdin.pause();
  }
}

function* queueDemo(): Operation<void> {
  // Track items added to observe state during replay
  const itemsAdded: string[] = [];
  const queue = createQueue<string, void>();

  console.log("\n--- Queue Replay Demo ---\n");
  console.log("Queue created (empty)");
  console.log(`Items tracked: [${itemsAdded.map(k => JSON.stringify(k)).join(", ")}]\n`);

  // Producer: add keypresses to queue
  for (let i = 0; i < ITERATIONS; i++) {
    console.log(`[${i + 1}/${ITERATIONS}] Waiting for input...`);

    const key = yield* waitForKeypress("  Press any key to add to queue...");
    
    queue.add(key);
    itemsAdded.push(key);
    console.log(`  Added ${JSON.stringify(key)} to queue`);
    console.log(`  Items tracked: [${itemsAdded.map(k => JSON.stringify(k)).join(", ")}]\n`);
  }

  console.log("--- Producer complete ---\n");
  console.log(`Total items added: ${itemsAdded.length}`);
  console.log(`Items tracked: [${itemsAdded.map(k => JSON.stringify(k)).join(", ")}]\n`);

  // Consumer: read and output all items
  console.log("Reading queue contents:");
  const itemsConsumed: string[] = [];
  for (let i = 0; i < ITERATIONS; i++) {
    const result = yield* queue.next();
    if (!result.done) {
      itemsConsumed.push(result.value);
      console.log(`  Read [${i}] = ${JSON.stringify(result.value)}`);
    }
  }

  console.log(`\nConsumed ${itemsConsumed.length} items: [${itemsConsumed.map(k => JSON.stringify(k)).join(", ")}]`);

  queue.close();
  console.log("\n=== Queue demo complete ===\n");
}

await main(function* () {
  const stream = yield* useDurableStream(STREAM_URL);
  yield* durably(() => queueDemo(), { stream });
});

/**
 * Durable Streams server for the demo.
 *
 * Starts an in-memory HTTP server on port 4437.
 * Keep this running across demo workflow invocations —
 * the stream data lives in memory for the duration of this process.
 *
 * Usage:
 *   deno task demo:server
 */

import { DurableStreamTestServer } from "@durable-streams/server";

const server = new DurableStreamTestServer({
  port: 4437,
  host: "0.0.0.0",
});

const url = await server.start();
console.log(`Durable Streams server running at ${url}`);
console.log(`Press Ctrl+C to stop\n`);

// Keep process alive until interrupted
Deno.addSignalListener("SIGINT", async () => {
  console.log("\nShutting down...");
  await server.stop();
  Deno.exit(0);
});

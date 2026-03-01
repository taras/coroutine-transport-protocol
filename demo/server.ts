/**
 * Pane A: Standalone Durable Streams server.
 *
 * Usage: deno task demo:server
 *
 * Starts a DurableStreamTestServer on port 4437 and prints its URL.
 * Runs until interrupted (Ctrl+C).
 */

import { DurableStreamTestServer } from "@durable-streams/server";

const server = new DurableStreamTestServer();

await server.start();

console.log(`Durable Streams server listening at ${server.url}`);
console.log("Press Ctrl+C to stop.\n");

// Keep the process alive until interrupted
Deno.addSignalListener("SIGINT", () => {
  console.log("\nShutting down...");
  server.stop();
  Deno.exit(0);
});

// Block forever
await new Promise(() => {});

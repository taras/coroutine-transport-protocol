/**
 * Durable Streams demo server.
 *
 * Architecture:
 * - DurableStreamTestServer runs on localhost:4438
 * - Deno.serve listens on localhost:4437 and forwards requests to 4438
 * - The proxy emits requests into an Effection signal stream
 * - The root operation stays alive by consuming that stream (no suspend needed)
 */

import {
  call,
  createSignal,
  each,
  main,
  resource,
  type Operation,
} from "effection";
import { DurableStreamTestServer } from "@durable-streams/server";

type RequestEvent = {
  request: Request;
  resolve: (response: Response) => void;
  reject: (error: unknown) => void;
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  let promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function* durableBackend(): Operation<{ url: string }> {
  return yield* resource(function* (provide) {
    let server = new DurableStreamTestServer({
      port: 4438,
      host: "127.0.0.1",
      onStreamCreated(event) {
        console.log(`[stream:created] ${event.path}`);
      },
      onStreamDeleted(event) {
        console.log(`[stream:deleted] ${event.path}`);
      },
    });

    let url = yield* call(() => server.start());
    console.log(`[durable] backend running at ${url}`);

    try {
      yield* provide({ url });
    } finally {
      console.log("[durable] stopping backend...");
      yield* call(() => server.stop());
      console.log("[durable] backend stopped");
    }
  });
}

function* requestStream(port: number): Operation<ReturnType<typeof createSignal<RequestEvent, void>>> {
  return yield* resource(function* (provide) {
    let requests = createSignal<RequestEvent, void>();
    let abort = new AbortController();
    let shuttingDown = false;

    Deno.serve({
      port,
      hostname: "0.0.0.0",
      signal: abort.signal,
    }, (request) => {
      if (shuttingDown) {
        return new Response("shutting down", { status: 503 });
      }

      let response = deferred<Response>();
      requests.send({
        request,
        resolve: response.resolve,
        reject: response.reject,
      });
      return response.promise;
    });

    console.log(`[proxy] listening on http://localhost:${port}`);

    try {
      yield* provide(requests);
    } finally {
      shuttingDown = true;
      abort.abort();
      requests.close();
      console.log("[proxy] stopped");
    }
  });
}

await main(function* () {
  let backend = yield* durableBackend();
  let incoming = yield* requestStream(4437);

  console.log("Press Ctrl+C to stop\n");

  for (let event of yield* each(incoming)) {
    try {
      let started = performance.now();
      let incomingUrl = new URL(event.request.url);
      let target = new URL(incomingUrl.pathname + incomingUrl.search, backend.url);

      let response = yield* call(() =>
        fetch(target, {
          method: event.request.method,
          headers: event.request.headers,
          body: event.request.body,
        })
      );

      let ms = (performance.now() - started).toFixed(1);
      console.log(
        `[http] ${event.request.method} ${incomingUrl.pathname}${incomingUrl.search} -> ${response.status} ${ms}ms`,
      );

      event.resolve(response);
    } catch (error) {
      event.reject(error);
    } finally {
      yield* each.next();
    }
  }
});

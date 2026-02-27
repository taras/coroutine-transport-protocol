/**
 * Durable Streams demo server.
 *
 * Architecture:
 * - DurableStreamTestServer runs on localhost:4438
 * - Node HTTP server listens on localhost:4437 and forwards requests to 4438
 * - The proxy emits requests into an Effection signal stream
 * - The root operation stays alive by consuming that stream (no suspend needed)
 *
 * Usage:
 *   pnpm demo:server
 */

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import {
  call,
  createSignal,
  each,
  main,
  resource,
  type Operation,
} from "effection";
import { DurableStreamTestServer } from "@durable-streams/server";
import stringify from "json-stringify-pretty-compact";
import { colorize, color } from "json-colorizer";

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

/**
 * Format a JSON body for readable logging with syntax highlighting
 */
function formatBody(body: string): string {
  try {
    const parsed = JSON.parse(body);
    const compact = stringify(parsed, { maxLength: 80, indent: 2 });
    return colorize(compact)
      .split("\n")
      .map(line => `      ${line}`)
      .join("\n");
  } catch {
    // Not JSON, return truncated raw body
    const truncated = body.slice(0, 500);
    return `      ${truncated}${body.length > 500 ? '...' : ''}`;
  }
}

/**
 * Convert Node's IncomingMessage to a Web Request
 */
function toWebRequest(req: IncomingMessage, port: number): Request {
  const url = new URL(req.url ?? "/", `http://localhost:${port}`);
  const headers = new Headers();
  
  for (const [key, value] of Object.entries(req.headers)) {
    if (value) {
      if (Array.isArray(value)) {
        for (const v of value) {
          headers.append(key, v);
        }
      } else {
        headers.set(key, value);
      }
    }
  }

  const method = req.method ?? "GET";
  const hasBody = !["GET", "HEAD", "OPTIONS"].includes(method);
  
  if (hasBody) {
    // Convert Node stream to Web ReadableStream
    const body = new ReadableStream({
      start(controller) {
        req.on("data", (chunk: Buffer) => controller.enqueue(chunk));
        req.on("end", () => controller.close());
        req.on("error", (err) => controller.error(err));
      },
    });
    
    return new Request(url, {
      method,
      headers,
      body,
      duplex: "half",
    } as RequestInit);
  }
  
  return new Request(url, { method, headers });
}

/**
 * Send a Web Response back through Node's ServerResponse
 */
async function sendWebResponse(webRes: Response, nodeRes: ServerResponse): Promise<void> {
  nodeRes.statusCode = webRes.status;
  nodeRes.statusMessage = webRes.statusText;
  
  // Copy headers, but skip Content-Encoding since fetch auto-decompresses
  const skipHeaders = new Set(["content-encoding", "content-length"]);
  for (const [key, value] of webRes.headers) {
    if (!skipHeaders.has(key.toLowerCase())) {
      nodeRes.setHeader(key, value);
    }
  }

  if (webRes.body) {
    const reader = webRes.body.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        nodeRes.write(value);
      }
    } finally {
      reader.releaseLock();
    }
  }
  
  nodeRes.end();
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
    let shuttingDown = false;

    const server = createServer(async (nodeReq, nodeRes) => {
      if (shuttingDown) {
        nodeRes.statusCode = 503;
        nodeRes.end("shutting down");
        return;
      }

      try {
        const webRequest = toWebRequest(nodeReq, port);
        const response = deferred<Response>();
        
        requests.send({
          request: webRequest,
          resolve: response.resolve,
          reject: response.reject,
        });

        const webResponse = await response.promise;
        await sendWebResponse(webResponse, nodeRes);
      } catch (error) {
        console.error("[proxy] error handling request:", error);
        nodeRes.statusCode = 500;
        nodeRes.end("Internal Server Error");
      }
    });

    yield* call(() => new Promise<void>((resolve) => {
      server.listen(port, "0.0.0.0", () => resolve());
    }));

    console.log(`[proxy] listening on http://localhost:${port}`);

    try {
      yield* provide(requests);
    } finally {
      shuttingDown = true;
      yield* call(() => new Promise<void>((resolve, reject) => {
        server.close((err) => err ? reject(err) : resolve());
      }));
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

      const hasBody = !["GET", "HEAD", "OPTIONS"].includes(event.request.method);
      
      // Clone headers and remove Accept-Encoding to avoid compression issues
      const headers = new Headers(event.request.headers);
      headers.delete("accept-encoding");

      // Read and log request body if present
      let requestBody: string | undefined;
      if (hasBody && event.request.body) {
        const clonedRequest = event.request.clone();
        requestBody = yield* call(() => clonedRequest.text());
        if (requestBody) {
          console.log(`${color.blue('[req]')} ${color.yellow(event.request.method)} ${incomingUrl.pathname}`);
          console.log(formatBody(requestBody));
        }
      }

      let response = yield* call(() => {
        return fetch(target, {
          method: event.request.method,
          headers,
          body: hasBody ? requestBody : undefined,
        });
      });

      // Clone response to read body for logging
      const clonedResponse = response.clone();
      const responseBody = yield* call(() => clonedResponse.text());

      let ms = (performance.now() - started).toFixed(1);
      const statusColor = response.status < 300 ? color.green : response.status < 400 ? color.yellow : color.red;
      console.log(
        `${color.magenta('[http]')} ${color.yellow(event.request.method)} ${incomingUrl.pathname}${color.gray(incomingUrl.search)} ${color.gray('->')} ${statusColor(String(response.status))} ${color.gray(ms + 'ms')}`,
      );
      if (responseBody) {
        console.log(`${color.green('[res]')}`);
        console.log(formatBody(responseBody));
      }

      event.resolve(response);
    } catch (error) {
      event.reject(error);
    } finally {
      yield* each.next();
    }
  }
});

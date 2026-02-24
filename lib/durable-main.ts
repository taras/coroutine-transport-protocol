import {
  main,
  run,
  resource,
  spawn,
  each,
  createSignal,
  withResolvers,
} from "effection";
import type { Operation } from "effection";
import {
  DurableStream as RemoteStream,
  FetchError,
} from "@durable-streams/client";
import type { DurableEvent, StreamEntry } from "effection/durable";
import { HttpDurableStream } from "./http-durable-stream.ts";

function useDurableStream(url: string): Operation<HttpDurableStream> {
  return resource(function* (provide) {
    // Connect to existing stream or create a new one
    let remoteConnected = withResolvers<RemoteStream>("connect to durable stream");
    connectOrCreate(url).then(remoteConnected.resolve, remoteConnected.reject);
    let remote = yield* remoteConnected.operation;

    // Pre-fetch existing events for replay
    let eventsFetched = withResolvers<StreamEntry[]>("fetch stream events");
    remote
      .stream<DurableEvent>({ json: true, live: false })
      .then((res: { json: <T>() => Promise<T[]> }) => res.json<DurableEvent>())
      .then((items: DurableEvent[]) =>
        eventsFetched.resolve(
          items.map((event: DurableEvent, i: number) => ({ offset: i, event }))
        )
      )
      .catch(eventsFetched.reject);
    let entries = yield* eventsFetched.operation;

    // Create the adapter
    let stream = new HttpDurableStream(remote, entries);

    // Surface producer errors into structured concurrency —
    // if the producer can't write to the server, the workflow should fail
    let errors = createSignal<Error>();
    stream.errorHandler = errors.send;

    yield* spawn(function* () {
      for (let error of yield* each(errors)) {
        throw error;
        yield* each.next();
      }
    });

    try {
      yield* provide(stream);
    } finally {
      // Explicitly flush pending writes, then detach producer.
      // Does NOT close the remote stream — it stays open for resume.
      let shutdown = withResolvers<void>("flush and detach producer");
      stream.flushAndDetach().then(shutdown.resolve, shutdown.reject);
      yield* shutdown.operation;
    }
  });
}

// TODO: This nests run() inside main()'s own run(), creating two scope trees
// and two reducers. The outer scope (from main) uses an ephemeral
// InMemoryDurableStream that records the resource setup — which we don't
// care about persisting. The real durable workflow runs in the inner run()
// with the HttpDurableStream. This works but smells wrong. The proper fix
// is to let main() accept a { stream } option so there's a single scope
// tree with one reducer wired to the HTTP-backed stream.
export function durableMain(
  url: string,
  body: (args: string[]) => Operation<void>,
): Promise<void> {
  return main(function* (args) {
    let stream = yield* useDurableStream(url);

    yield* run(() => body(args), { stream });
  });
}

async function connectOrCreate(url: string): Promise<RemoteStream> {
  try {
    return await RemoteStream.connect({
      url,
      contentType: "application/json",
    });
  } catch (e: unknown) {
    if (e instanceof FetchError && (e as FetchError).status === 404) {
      return await RemoteStream.create({
        url,
        contentType: "application/json",
      });
    }
    throw e;
  }
}

/**
 * HTTP-backed DurableStream implementation using the Durable Streams protocol.
 *
 * Uses raw fetch() for appends (not IdempotentProducer) because durable
 * execution requires synchronous acknowledgment on every write
 * (persist-before-resume). See DEC-026.
 *
 * Concurrent appends are serialized via a promise chain to guarantee
 * server-side sequence ordering. See DEC-027.
 */

import type { DurableStream } from "./stream.ts";
import type { DurableEvent } from "./types.ts";
import {
  stream,
  StaleEpochError,
  SequenceGapError,
  PRODUCER_ID_HEADER,
  PRODUCER_EPOCH_HEADER,
  PRODUCER_SEQ_HEADER,
  PRODUCER_EXPECTED_SEQ_HEADER,
  PRODUCER_RECEIVED_SEQ_HEADER,
  STREAM_OFFSET_HEADER,
} from "@durable-streams/client";

/**
 * Configuration for HttpDurableStream.
 */
export interface HttpDurableStreamOptions {
  /** Base URL of the Durable Streams server (e.g. "http://localhost:4437"). */
  baseUrl: string;
  /** Stream identifier. Will be used as the URL path segment. */
  streamId: string;
  /** Unique producer identifier for idempotent append tracking. */
  producerId: string;
  /** Producer epoch — monotonically increasing. Stale epochs are fenced. */
  epoch: number;
  /** Optional custom fetch implementation (for testing). */
  fetch?: typeof globalThis.fetch;
}

/**
 * DurableStream implementation backed by HTTP calls to a Durable Streams server.
 *
 * Guarantees:
 * - Append-only, prefix-closed, monotonic indexing (server-enforced)
 * - Durability: append() resolves only after HTTP 200 (persist-before-resume)
 * - Concurrent appends serialized via promise chain (DEC-027)
 * - Fatal errors (stale epoch) cause all future appends to fail-fast
 * - Stream-Next-Offset tracked from every response (DEC-029)
 */
export class HttpDurableStream implements DurableStream {
  private readonly streamUrl: string;
  private readonly producerId: string;
  private readonly epoch: number;
  private readonly _fetch: typeof globalThis.fetch;

  /** Next sequence number to assign. Incremented synchronously on each append(). */
  private nextSeq = 0;

  /** Serialization chain for concurrent appends. See DEC-027. */
  private pending: Promise<void> = Promise.resolve();

  /** Set on fatal errors (e.g. StaleEpochError). Future appends fail-fast. */
  private fatalError: Error | undefined;

  /**
   * Last Stream-Next-Offset received from the server.
   * Tracked from both reads and writes (DEC-029).
   * This is the resumption point for future tail() calls.
   */
  lastOffset: string | undefined;

  private constructor(opts: HttpDurableStreamOptions) {
    this.streamUrl = `${opts.baseUrl}/${opts.streamId}`;
    this.producerId = opts.producerId;
    this.epoch = opts.epoch;
    this._fetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /**
   * Create an HttpDurableStream, ensuring the server-side stream exists.
   *
   * Sends PUT to create the stream. 201 = created, 200 = already exists.
   */
  static async connect(
    opts: HttpDurableStreamOptions,
  ): Promise<HttpDurableStream> {
    const instance = new HttpDurableStream(opts);

    // Create the stream on the server (idempotent — 200 means it exists)
    const fetchFn = opts.fetch ?? globalThis.fetch.bind(globalThis);
    const res = await fetchFn(instance.streamUrl, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
    });
    // Consume the body to free the connection
    await res.text();

    // 201 = created, 200 = already exists (idempotent)
    if (res.status !== 201 && res.status !== 200) {
      throw new Error(
        `Failed to create stream: HTTP ${res.status}`,
      );
    }

    return instance;
  }

  /**
   * Read all events in the stream, in append order.
   *
   * Uses the stream() function from @durable-streams/client with
   * offset="-1" (start of stream) and live=false (no tailing).
   */
  async readAll(): Promise<DurableEvent[]> {
    const res = await stream({
      url: this.streamUrl,
      offset: "-1",
      live: false,
      fetch: this._fetch,
    });
    const events = await res.json() as DurableEvent[];
    // Track offset from read (DEC-029)
    if (res.offset) {
      this.lastOffset = res.offset;
    }
    return events;
  }

  /**
   * Append an event to the stream.
   *
   * Sequence numbers are assigned synchronously to preserve ordering.
   * HTTP calls are serialized behind a promise chain so the server
   * always receives them in sequence order (DEC-027).
   *
   * The returned promise resolves only after the server confirms
   * persistence (persist-before-resume).
   */
  append(event: DurableEvent): Promise<void> {
    // Fail-fast if a fatal error has been set
    if (this.fatalError) {
      return Promise.reject(this.fatalError);
    }

    // Assign seq synchronously — ordering is locked in before any async work
    const seq = this.nextSeq++;

    // Chain behind pending — ensures HTTP calls arrive in seq order
    const p = this.pending.then(() => this.doAppend(event, seq));

    // Update the chain. catch(() => {}) prevents a failed append from
    // blocking subsequent appends, but the error still propagates to
    // the original caller via `p`.
    this.pending = p.catch(() => {});

    return p;
  }

  /**
   * Execute a single HTTP append with the given event and sequence number.
   *
   * Any uncertain write outcome (network error, unexpected HTTP status,
   * sequence gap) is treated as fatal — `fatalError` is set so all future
   * appends fail-fast. This prevents sequence drift where later appends
   * would hit 409 SequenceGapError because an earlier seq was never
   * acknowledged.
   */
  private async doAppend(event: DurableEvent, seq: number): Promise<void> {
    // Double-check fatal error (may have been set by a preceding append in the chain)
    if (this.fatalError) {
      throw this.fatalError;
    }

    let res: Response;
    try {
      res = await this._fetch(this.streamUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          [PRODUCER_ID_HEADER]: this.producerId,
          [PRODUCER_EPOCH_HEADER]: String(this.epoch),
          [PRODUCER_SEQ_HEADER]: String(seq),
        },
        body: JSON.stringify(event),
      });
    } catch (err) {
      // Network failure — fatal, sequence state is now uncertain
      const error = err instanceof Error
        ? err
        : new Error(String(err));
      this.fatalError = error;
      throw error;
    }

    // Always consume the body to free the connection
    await res.text();

    switch (res.status) {
      case 200: {
        // Success — capture offset
        const offset = res.headers.get(STREAM_OFFSET_HEADER);
        if (offset) {
          this.lastOffset = offset;
        }
        return;
      }
      case 204: {
        // Duplicate (idempotent success) — capture offset if present
        const offset = res.headers.get(STREAM_OFFSET_HEADER);
        if (offset) {
          this.lastOffset = offset;
        }
        return;
      }
      case 403: {
        // Stale epoch — fatal error
        const currentEpoch = Number(
          res.headers.get(PRODUCER_EPOCH_HEADER) ?? 0,
        );
        const error = new StaleEpochError(currentEpoch);
        this.fatalError = error;
        throw error;
      }
      case 409: {
        // Sequence gap — fatal (should never happen due to serialization,
        // but if it does, sequence state is irrecoverably desynchronized)
        const expected = Number(
          res.headers.get(PRODUCER_EXPECTED_SEQ_HEADER) ?? 0,
        );
        const received = Number(
          res.headers.get(PRODUCER_RECEIVED_SEQ_HEADER) ?? 0,
        );
        const error = new SequenceGapError(expected, received);
        this.fatalError = error;
        throw error;
      }
      default: {
        // Unexpected status — fatal, write outcome is uncertain
        const error = new Error(
          `Unexpected append response: HTTP ${res.status}`,
        );
        this.fatalError = error;
        throw error;
      }
    }
  }
}

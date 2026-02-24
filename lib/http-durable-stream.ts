import { IdempotentProducer } from "@durable-streams/client";
import type { DurableStream as RemoteStream } from "@durable-streams/client";
import type {
  DurableStream,
  DurableEvent,
  StreamEntry,
} from "effection/durable";

export class HttpDurableStream implements DurableStream {
  private buffer: StreamEntry[];
  private _closed = false;
  readonly producer: IdempotentProducer;

  errorHandler?: (err: Error) => void;

  constructor(remote: RemoteStream, initialEntries: StreamEntry[]) {
    this.buffer = [...initialEntries];
    this.producer = new IdempotentProducer(remote, `durable-effection`, {
      autoClaim: true,
      onError: (err: Error) => this.errorHandler?.(err),
    });
  }

  get length() {
    return this.buffer.length;
  }

  get closed() {
    return this._closed;
  }

  append(event: DurableEvent): number {
    if (this._closed) throw new Error("Cannot append to closed stream");
    let offset = this.buffer.length;
    this.buffer.push({ offset, event });
    this.producer.append(JSON.stringify(event));
    return offset;
  }

  read(fromOffset = 0): StreamEntry[] {
    return this.buffer.slice(fromOffset);
  }

  async flush(): Promise<void> {
    await this.producer.flush();
  }

  async flushAndDetach(): Promise<void> {
    this._closed = true;
    await this.producer.flush();
    await this.producer.detach();
  }

  close(): void {
    this._closed = true;
  }
}

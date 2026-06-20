import { ErrorCode, ErrorInfo } from "../../errors.js";

/**
 * Client-side per-turn stream router.
 */
export interface StreamRouter<TOutput> {
  /** Registers or replaces a stream for a turn invocation. */
  createStream(turnId: string, invocationId: string): ReadableStream<TOutput>;
  /** Rebinds a suspended turn stream to a continuation invocation. */
  rebindStream(turnId: string, invocationId: string): boolean;
  /** Routes one decoded output to an active own-turn stream. */
  route(turnId: string, invocationId: string | undefined, output: TOutput): boolean;
  /** Closes an active stream. */
  closeStream(turnId: string): boolean;
  /** Errors an active stream. */
  errorStream(turnId: string, error: ErrorInfo): boolean;
  /** Returns true when a turn has an active stream. */
  has(turnId: string): boolean;
  /** Returns the currently bound invocation id for a turn. */
  activeInvocation(turnId: string): string | undefined;
  /** Returns the currently bound stream for a turn. */
  getStream(turnId: string): ReadableStream<TOutput> | undefined;
  /** Closes all streams. */
  closeAll(): void;
}

/**
 * Options for {@link createStreamRouter}.
 */
export interface StreamRouterOptions<TOutput> {
  /** Returns whether an output terminates the stream. */
  isTerminal(output: TOutput): boolean;
  /** Maximum queued chunks per stream.
   *
   * @defaultValue `1024`
   */
  maxQueuedChunks?: number;
}

/**
 * Creates a bounded O(1) client-side stream router.
 */
export function createStreamRouter<TOutput>(
  options: StreamRouterOptions<TOutput>,
): StreamRouter<TOutput> {
  return new DefaultStreamRouter(options);
}

interface StreamEntry<TOutput> {
  controller: ReadableStreamDefaultController<TOutput>;
  stream: ReadableStream<TOutput>;
  invocationId: string;
}

class DefaultStreamRouter<TOutput> implements StreamRouter<TOutput> {
  private readonly entries = new Map<string, StreamEntry<TOutput>>();
  private readonly maxQueuedChunks: number;

  public constructor(private readonly options: StreamRouterOptions<TOutput>) {
    this.maxQueuedChunks = options.maxQueuedChunks ?? 1024;
  }

  public createStream(turnId: string, invocationId: string): ReadableStream<TOutput> {
    let controller: ReadableStreamDefaultController<TOutput> | undefined;
    const stream = new ReadableStream<TOutput>(
      {
        start: (created) => {
          controller = created;
        },
      },
      {
        highWaterMark: this.maxQueuedChunks,
        size: () => 1,
      },
    );
    if (!controller) {
      throw new ErrorInfo({
        code: ErrorCode.TransportSubscriptionError,
        message: "unable to create stream; ReadableStream did not provide a controller",
      });
    }
    this.entries.set(turnId, { controller, stream, invocationId });
    return stream;
  }

  public rebindStream(turnId: string, invocationId: string): boolean {
    const entry = this.entries.get(turnId);
    if (!entry) {
      return false;
    }
    entry.invocationId = invocationId;
    return true;
  }

  public route(turnId: string, invocationId: string | undefined, output: TOutput): boolean {
    const entry = this.entries.get(turnId);
    if (!entry) {
      return false;
    }
    if (invocationId !== undefined && invocationId !== entry.invocationId) {
      return false;
    }
    const desiredSize = entry.controller.desiredSize;
    if (desiredSize !== null && desiredSize <= 0) {
      this.errorStream(
        turnId,
        new ErrorInfo({
          code: ErrorCode.StreamError,
          statusCode: 507,
          message: "unable to route stream output; stream queue is full",
        }),
      );
      return false;
    }
    try {
      entry.controller.enqueue(output);
    } catch {
      this.entries.delete(turnId);
      return false;
    }
    if (this.options.isTerminal(output)) {
      this.closeStream(turnId);
    }
    return true;
  }

  public closeStream(turnId: string): boolean {
    const entry = this.entries.get(turnId);
    if (!entry) {
      return false;
    }
    try {
      entry.controller.close();
    } catch {
      // The consumer may have cancelled the stream.
    }
    this.entries.delete(turnId);
    return true;
  }

  public errorStream(turnId: string, error: ErrorInfo): boolean {
    const entry = this.entries.get(turnId);
    if (!entry) {
      return false;
    }
    try {
      entry.controller.error(error);
    } catch {
      // The consumer may have cancelled the stream.
    }
    this.entries.delete(turnId);
    return true;
  }

  public has(turnId: string): boolean {
    return this.entries.has(turnId);
  }

  public activeInvocation(turnId: string): string | undefined {
    return this.entries.get(turnId)?.invocationId;
  }

  public getStream(turnId: string): ReadableStream<TOutput> | undefined {
    return this.entries.get(turnId)?.stream;
  }

  public closeAll(): void {
    for (const turnId of Array.from(this.entries.keys())) {
      this.closeStream(turnId);
    }
  }
}

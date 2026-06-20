import { HEADER_INPUT_CLIENT_ID, HEADER_TURN_CLIENT_ID, HEADER_TURN_ID } from "../../constants.js";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import type { InboundMessage } from "../../realtime/index.js";
import type { HeaderMap } from "../../utils.js";
import type { CancelFilter } from "./client-transport.js";

/** Passed to turn cancel authorization hooks. */
export interface CancelRequest {
  /** Raw cancel message. */
  message: InboundMessage;
  /** Parsed cancel filter. */
  filter: CancelFilter;
  /** Matched turn ids. */
  matchedTurnIds: string[];
  /** Matched turn owners by turn id. */
  turnOwners: Map<string, string>;
}

/** Registered server-side turn state. */
export interface ManagedTurn {
  /** Turn identity. */
  turnId: string;
  /** Owning client id. */
  clientId?: string;
  /** Aborts this turn. */
  abort(reason?: unknown): void;
  /** Optional cancel authorization hook. */
  onCancel?(request: CancelRequest): Promise<boolean> | boolean;
  /** Optional turn-scoped error hook. */
  onError?(error: ErrorInfo): void;
  /** Whether this turn has already observed cancellation. */
  cancelled: boolean;
}

/** Input event buffered by invocation id. */
export interface BufferedInputEvent {
  /** Raw inbound message. */
  message: InboundMessage;
  /** Transport headers. */
  headers: HeaderMap;
}

/** Turn manager options. */
export interface TurnManagerOptions {
  /** Maximum buffered input events.
   *
   * @defaultValue `200`
   */
  inputEventBufferLimit?: number;
}

/**
 * O(1) registry for server turns, early input lookup, and cancel routing.
 */
export class TurnManager {
  private readonly turns = new Map<string, ManagedTurn>();
  private readonly inputBuffer = new Map<string, BufferedInputEvent[]>();
  private readonly inputWaiters = new Map<string, Set<(event: BufferedInputEvent) => void>>();
  private readonly bufferOrder: string[] = [];
  private readonly stragglerSeen = new Set<string>();
  private readonly bufferLimit: number;

  /** Creates a manager. */
  public constructor(options: TurnManagerOptions = {}) {
    this.bufferLimit = options.inputEventBufferLimit ?? 200;
  }

  /** Registers a turn for input lookup and cancellation. */
  public register(turn: ManagedTurn): void {
    this.turns.set(turn.turnId, turn);
  }

  /** Deregisters a turn. */
  public deregister(turnId: string): void {
    this.turns.delete(turnId);
  }

  /** Returns current turn owners. */
  public turnOwners(): Map<string, string> {
    const owners = new Map<string, string>();
    for (const [turnId, turn] of this.turns) {
      if (turn.clientId !== undefined) {
        owners.set(turnId, turn.clientId);
      }
    }
    return owners;
  }

  /** Buffers or delivers an inbound input event. */
  public observeInput(message: InboundMessage): void {
    const headers = message.getTransportHeaders();
    const invocationId = headers["invocation-id"];
    if (!invocationId) {
      return;
    }
    const event: BufferedInputEvent = { message, headers };
    const waiters = this.inputWaiters.get(invocationId);
    if (waiters && waiters.size > 0) {
      for (const waiter of waiters) {
        waiter(event);
      }
      this.inputWaiters.delete(invocationId);
      this.stragglerSeen.add(invocationId);
      return;
    }
    if (this.stragglerSeen.has(invocationId)) {
      return;
    }
    let events = this.inputBuffer.get(invocationId);
    if (!events) {
      events = [];
      this.inputBuffer.set(invocationId, events);
    }
    events.push(event);
    this.bufferOrder.push(invocationId);
    this.evictInputBuffer();
  }

  /** Looks up the first input event for an invocation id. */
  public lookupInput(invocationId: string, timeoutMs: number): Promise<BufferedInputEvent> {
    const buffered = this.inputBuffer.get(invocationId)?.shift();
    if (buffered) {
      this.stragglerSeen.add(invocationId);
      return Promise.resolve(buffered);
    }
    return new Promise((resolve, reject) => {
      const waiter = (event: BufferedInputEvent): void => {
        clearTimeout(timer);
        this.inputWaiters.get(invocationId)?.delete(waiter);
        this.stragglerSeen.add(invocationId);
        resolve(event);
      };
      const timer = setTimeout(() => {
        this.inputWaiters.get(invocationId)?.delete(waiter);
        reject(
          new ErrorInfo({
            code: ErrorCode.InputEventNotFound,
            statusCode: 504,
            message: "unable to start turn; input event was not found",
          }),
        );
      }, timeoutMs);
      let waiters = this.inputWaiters.get(invocationId);
      if (!waiters) {
        waiters = new Set();
        this.inputWaiters.set(invocationId, waiters);
      }
      waiters.add(waiter);
    });
  }

  /** Routes a cancel message to matching turns. */
  public routeCancel(message: InboundMessage, fallbackFilter?: CancelFilter): void {
    const headers = message.getTransportHeaders();
    const filter = fallbackFilter ?? parseCancelFilter(headers, message.data);
    const matched = this.matchTurns(filter, headers);
    const owners = this.turnOwners();
    const request: CancelRequest = {
      message,
      filter,
      matchedTurnIds: [...matched],
      turnOwners: owners,
    };
    for (const turnId of matched) {
      const turn = this.turns.get(turnId);
      if (!turn || turn.cancelled) {
        continue;
      }
      void this.authorizeAndAbort(turn, request);
    }
  }

  /** Aborts and clears every registered turn. */
  public close(): void {
    for (const turn of this.turns.values()) {
      turn.abort();
    }
    this.turns.clear();
    this.inputBuffer.clear();
    this.inputWaiters.clear();
  }

  private async authorizeAndAbort(turn: ManagedTurn, request: CancelRequest): Promise<void> {
    try {
      if (turn.onCancel && !(await turn.onCancel(request))) {
        return;
      }
      turn.cancelled = true;
      turn.abort();
    } catch (error) {
      turn.onError?.(
        new ErrorInfo({
          code: ErrorCode.CancelListenerError,
          message: "unable to cancel turn; cancel listener failed",
          cause: error,
        }),
      );
    }
  }

  private matchTurns(filter: CancelFilter, headers: HeaderMap): Set<string> {
    const matched = new Set<string>();
    if ("all" in filter && filter.all) {
      for (const turnId of this.turns.keys()) {
        matched.add(turnId);
      }
      return matched;
    }
    if ("turnId" in filter && filter.turnId) {
      if (this.turns.has(filter.turnId)) {
        matched.add(filter.turnId);
      }
      return matched;
    }
    const targetClientId =
      "clientId" in filter && filter.clientId
        ? filter.clientId
        : "own" in filter && filter.own
          ? (headers[HEADER_INPUT_CLIENT_ID] ?? headers[HEADER_TURN_CLIENT_ID])
          : undefined;
    if (!targetClientId) {
      return matched;
    }
    for (const [turnId, turn] of this.turns) {
      if (turn.clientId === targetClientId) {
        matched.add(turnId);
      }
    }
    return matched;
  }

  private evictInputBuffer(): void {
    while (this.bufferOrder.length > this.bufferLimit) {
      const invocationId = this.bufferOrder.shift();
      if (!invocationId) {
        return;
      }
      const events = this.inputBuffer.get(invocationId);
      events?.shift();
      if (!events || events.length === 0) {
        this.inputBuffer.delete(invocationId);
      }
    }
  }
}

function parseCancelFilter(headers: HeaderMap, data: unknown): CancelFilter {
  if (headers[HEADER_TURN_ID]) {
    return { turnId: headers[HEADER_TURN_ID] };
  }
  const record =
    data !== null && typeof data === "object" ? (data as Record<string, unknown>) : undefined;
  if (typeof record?.turnId === "string") {
    return { turnId: record.turnId };
  }
  if (typeof record?.clientId === "string") {
    return { clientId: record.clientId };
  }
  if (record?.all === true || headers.filter === "all") {
    return { all: true };
  }
  return { own: true };
}

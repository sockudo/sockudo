import {
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_TURN_REASON,
} from "../../constants.js";
import type { Decoder, DecodedEvent } from "../codec/index.js";
import type {
  InboundMessage,
  PaginatedResult,
  Serial,
} from "../../realtime/types.js";
import type { ConversationTree, TurnEndReason } from "./tree.js";

/**
 * Result of decoding one history page into a conversation tree.
 */
export interface DecodeHistoryResult {
  /** Number of history messages processed. */
  processedMessages: number;
  /** Number of decoded codec events folded. */
  decodedEvents: number;
  /** Number of lifecycle messages applied. */
  lifecycleEvents: number;
}

/**
 * Decodes a page of Sockudo history into the shared conversation tree upsert path.
 */
export function decodeHistoryPage<TInput, TOutput, TProjection>(
  page: PaginatedResult<InboundMessage>,
  decoder: Decoder<TInput, TOutput>,
  tree: ConversationTree<TInput | TOutput, TProjection>,
): DecodeHistoryResult {
  let processedMessages = 0;
  let decodedEvents = 0;
  let lifecycleEvents = 0;
  for (const message of page.items) {
    processedMessages += 1;
    const headers = message.getTransportHeaders();
    if (message.name === EVENT_AI_TURN_START) {
      tree.applyTurnLifecycle({
        type: "turn-start",
        headers,
        serial: message.historySerial,
      });
      lifecycleEvents += 1;
      continue;
    }
    if (message.name === EVENT_AI_TURN_END) {
      tree.applyTurnLifecycle({
        type: "turn-end",
        headers,
        serial: message.historySerial,
        ...optionalReason(headers[HEADER_TURN_REASON]),
      });
      lifecycleEvents += 1;
      continue;
    }
    const decoded = decoder.decode(message);
    const events: DecodedEvent<TInput | TOutput>[] = [
      ...decoded.inputs,
      ...decoded.outputs,
    ];
    decodedEvents += events.length;
    tree.applyMessage(events, headers, message.historySerial);
  }
  return { processedMessages, decodedEvents, lifecycleEvents };
}

/**
 * History source used by {@link loadHistoryIntoTree}.
 */
export interface HistoryReader {
  /** Reads one page of normalized history. */
  history(options?: {
    limit?: number;
    direction?: "newest_first" | "oldest_first" | "backwards" | "reverse";
    untilAttach?: boolean;
    end?: Serial;
  }): Promise<PaginatedResult<InboundMessage>>;
}

/**
 * Options for paginated history decoding.
 */
export interface LoadHistoryOptions {
  /** Target number of newly visible turns. */
  limit?: number;
  /** Whether to request `untilAttach` on the first history page. */
  untilAttach?: boolean;
  /** Wire page size. */
  wireLimit?: number;
}

/**
 * Result of a paginated history load.
 */
export interface LoadHistoryResult extends DecodeHistoryResult {
  /** Last page returned by the history source. */
  page: PaginatedResult<InboundMessage>;
}

/**
 * Loads and decodes one backward history page.
 */
export async function loadHistoryIntoTree<TInput, TOutput, TProjection>(
  source: HistoryReader,
  decoder: Decoder<TInput, TOutput>,
  tree: ConversationTree<TInput | TOutput, TProjection>,
  options: LoadHistoryOptions = {},
): Promise<LoadHistoryResult> {
  const historyOptions: {
    direction: "newest_first";
    limit: number;
    untilAttach?: boolean;
  } = {
    direction: "newest_first",
    limit: options.wireLimit ?? (options.limit ?? 100) * 10,
  };
  if (options.untilAttach !== undefined) {
    historyOptions.untilAttach = options.untilAttach;
  }
  const page = await source.history(historyOptions);
  const result = decodeHistoryPage(page, decoder, tree);
  return { ...result, page };
}

function optionalReason(
  value: string | undefined,
): { reason: TurnEndReason } | Record<string, never> {
  switch (value) {
    case "complete":
    case "cancelled":
    case "error":
    case "suspended":
      return { reason: value };
    default:
      return {};
  }
}

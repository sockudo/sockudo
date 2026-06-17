import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_FORK_OF,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_CONTINUE,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../constants.js";
import { EventEmitter, type EventUnsubscribe } from "../../event-emitter.js";
import type { HeaderMap } from "../../utils.js";
import type { DecodedEvent, Reducer, ReducerMeta } from "../codec/index.js";

/**
 * Sockudo serial values preserve unsafe integers as strings.
 */
export type TreeSerial = number | string;

/**
 * Wire turn-end reasons.
 */
export type TurnEndReason = "complete" | "cancelled" | "error" | "suspended";

/**
 * Turn status tracked by the conversation tree.
 */
export type TurnStatus = "active" | TurnEndReason;

/**
 * Turn-keyed node in the conversation tree.
 */
export interface TurnNode<TProjection> {
  /** Stable turn id. */
  turnId: string;
  /** Parent turn id, resolved from parent or fork metadata. */
  parentTurnId?: string;
  /** Turn id this node forked from. */
  forkOf?: string;
  /** Anchored codec message id this turn regenerates. */
  regeneratesCodecMessageId?: string;
  /** Verified client id. */
  clientId?: string;
  /** Current turn status. */
  status: TurnStatus;
  /** Codec projection. May be mutated in place by the reducer. */
  projection: TProjection;
  /** Latest invocation id for the turn or continuation. */
  invocationId?: string;
  /** First known serial for sorting; serial-less optimistic turns sort last. */
  startSerial?: TreeSerial;
  /** Terminal lifecycle serial. */
  endSerial?: TreeSerial;
}

/**
 * Turn lifecycle event accepted by {@link ConversationTree.applyTurnLifecycle}.
 */
export interface TurnLifecycleEvent {
  /** Lifecycle kind. */
  type: "turn-start" | "turn-end";
  /** Transport headers carried by the lifecycle message. */
  headers: HeaderMap;
  /** Serial assigned by Sockudo delivery or history. */
  serial: TreeSerial;
  /** Optional decoded turn-end reason override. */
  reason?: TurnEndReason;
}

/**
 * Folded message event emitted by the tree.
 */
export interface TreeMessageEvent<TEvent, TProjection> {
  /** Owning turn id. */
  turnId: string;
  /** Owning turn node. */
  node: TurnNode<TProjection>;
  /** Folded codec event. */
  event: TEvent;
  /** Codec message id. */
  messageId?: string;
  /** Fold serial. */
  serial: TreeSerial;
}

/**
 * Conversation tree event map.
 */
export interface ConversationTreeEvents<TEvent, TProjection> {
  /** Structural tree state changed. */
  update: { structuralVersion: number };
  /** Preferred message-fold event. */
  message: TreeMessageEvent<TEvent, TProjection>;
  /** Deprecated Ably-compatible message-fold alias. */
  "ably-message": TreeMessageEvent<TEvent, TProjection>;
  /** Turn metadata or status changed. */
  turn: TurnNode<TProjection>;
  /** Turn projection was folded, possibly mutating in place. */
  "turn-projection-updated": TreeMessageEvent<TEvent, TProjection>;
}

/**
 * Conversation tree construction options.
 */
export interface ConversationTreeOptions<TProjection> {
  /** Optional optimistic projection seed for tests and docs adapters. */
  createProjection?(): TProjection;
}

/**
 * Public conversation tree API.
 */
export interface ConversationTree<TEvent, TProjection> {
  /** Current structural cache version. */
  readonly structuralVersion: number;
  /** Applies decoded message events using the shared live/history upsert path. */
  applyMessage(
    decodedEvents: readonly DecodedEvent<TEvent>[],
    transportHeaders: HeaderMap,
    serial: TreeSerial,
  ): TurnNode<TProjection> | undefined;
  /** Applies turn lifecycle metadata. */
  applyTurnLifecycle(
    event: TurnLifecycleEvent,
  ): TurnNode<TProjection> | undefined;
  /** Deletes a codec message id and removes unreachable turns. */
  delete(codecMessageId: string): void;
  /** Docs-compatible upsert alias for {@link applyMessage}. */
  upsert(
    decodedEvents: readonly DecodedEvent<TEvent>[],
    transportHeaders: HeaderMap,
    serial: TreeSerial,
  ): TurnNode<TProjection> | undefined;
  /** Gets a node by turn id. */
  getTurnNode(turnId: string): TurnNode<TProjection> | undefined;
  /** Gets a node by codec message id. */
  getTurnByCodecMessageId(
    codecMessageId: string,
  ): TurnNode<TProjection> | undefined;
  /** Docs-compatible message-granularity node lookup. */
  getNode(codecMessageId: string): TurnNode<TProjection> | undefined;
  /** Gets edit siblings for a turn id or codec message id. */
  getSiblingTurns(id: string): readonly TurnNode<TProjection>[];
  /** Docs-compatible sibling lookup alias. */
  getSiblings(id: string): readonly TurnNode<TProjection>[];
  /** Returns whether a turn or codec message id has edit siblings. */
  hasSiblingTurns(id: string): boolean;
  /** Docs-compatible sibling predicate alias. */
  hasSiblings(id: string): boolean;
  /** Gets the regenerate group for an anchored codec message id. */
  getRegenerateGroup(codecMessageId: string): readonly TurnNode<TProjection>[];
  /** Gets the latest continuation invocation for a turn. */
  getLatestContinuationInvocation(turnId: string): string | undefined;
  /** Gets active and suspended turns grouped by verified client id. */
  getActiveTurnIds(): Map<string, Set<string>>;
  /** Gets defensive transport headers by codec message id. */
  getHeaders(codecMessageId: string): HeaderMap | undefined;
  /** Gets all turn nodes in start-serial order, with optimistic turns last. */
  getTurnNodes(): readonly TurnNode<TProjection>[];
  /** Subscribes to conversation tree events. */
  on<K extends keyof ConversationTreeEvents<TEvent, TProjection>>(
    event: K,
    handler: (payload: ConversationTreeEvents<TEvent, TProjection>[K]) => void,
  ): EventUnsubscribe;
}

/**
 * Creates a turn-keyed conversation tree.
 *
 * Projection ref-equality is not a change signal: reducers may mutate the
 * projection in place, and streaming folds emit `turn-projection-updated`.
 */
export function createConversationTree<TEvent, TProjection>(
  reducer: Reducer<TEvent, TProjection>,
  options: ConversationTreeOptions<TProjection> = {},
): ConversationTree<TEvent, TProjection> {
  return new ConversationTreeImpl(reducer, options);
}

class ConversationTreeImpl<TEvent, TProjection>
  implements ConversationTree<TEvent, TProjection>
{
  private readonly emitter = new EventEmitter<
    ConversationTreeEvents<TEvent, TProjection>
  >();
  private readonly turnIndex = new Map<string, TurnNode<TProjection>>();
  private readonly codecMessageIdToTurnId = new Map<string, string>();
  private readonly turnCodecMessageIds = new Map<string, Set<string>>();
  private readonly headersByCodecMessageId = new Map<string, HeaderMap>();
  private readonly sortedTurns: string[] = [];
  private readonly parentIndex = new Map<string, Set<string>>();
  private readonly rootTurns = new Set<string>();
  private readonly regenerateByMsgId = new Map<string, Set<string>>();
  private readonly pendingParentRefByTurn = new Map<string, string>();
  private readonly pendingForkRefByTurn = new Map<string, string>();
  private readonly siblingCache = new Map<string, CachedNodes<TProjection>>();
  private readonly regenerateCache = new Map<
    string,
    CachedNodes<TProjection>
  >();
  private readonly latestContinuationInvocation = new Map<string, string>();
  private version = 0;

  public constructor(
    private readonly reducer: Reducer<TEvent, TProjection>,
    private readonly options: ConversationTreeOptions<TProjection>,
  ) {}

  public get structuralVersion(): number {
    return this.version;
  }

  public applyMessage(
    decodedEvents: readonly DecodedEvent<TEvent>[],
    transportHeaders: HeaderMap,
    serial: TreeSerial,
  ): TurnNode<TProjection> | undefined {
    if (decodedEvents.length === 0) {
      return undefined;
    }
    const route = this.resolveMessageRoute(decodedEvents, transportHeaders);
    if (!route.turnId) {
      return undefined;
    }
    const metadata = this.metadataFromHeaders(
      route.turnId,
      transportHeaders,
      transportHeaders[HEADER_TURN_CONTINUE] !== "true",
    );
    const node = this.ensureTurn(route.turnId, metadata, serial);
    if (this.promoteSerial(node, serial)) {
      this.bump();
    }
    for (const decoded of decodedEvents) {
      const messageId =
        decoded.messageId ??
        decoded.meta.messageId ??
        transportHeaders[HEADER_CODEC_MESSAGE_ID];
      if (messageId !== undefined) {
        this.attachCodecMessage(node.turnId, messageId, transportHeaders);
      }
      const meta: ReducerMeta =
        messageId === undefined ? { serial } : { serial, messageId };
      node.projection = this.reducer.fold(node.projection, decoded.event, meta);
      const event: TreeMessageEvent<TEvent, TProjection> = {
        turnId: node.turnId,
        node,
        event: decoded.event,
        serial,
      };
      setOptional(event, "messageId", messageId);
      this.emitter.emit("message", event);
      this.emitter.emit("ably-message", event);
      this.emitter.emit("turn-projection-updated", event);
    }
    return node;
  }

  public applyTurnLifecycle(
    event: TurnLifecycleEvent,
  ): TurnNode<TProjection> | undefined {
    const turnId = event.headers[HEADER_TURN_ID];
    if (!turnId) {
      return undefined;
    }
    if (event.type === "turn-start") {
      const metadata = this.metadataFromHeaders(
        turnId,
        event.headers,
        event.headers[HEADER_TURN_CONTINUE] !== "true",
      );
      const node = this.ensureTurn(turnId, metadata, event.serial);
      const wasTerminal = isTerminalStatus(node.status);
      const isContinuation = event.headers[HEADER_TURN_CONTINUE] === "true";
      let changed = this.promoteSerial(node, event.serial);
      if (
        !wasTerminal &&
        node.status !== "active" &&
        (node.status !== "suspended" || isContinuation)
      ) {
        node.status = "active";
        changed = true;
      }
      const invocationId = event.headers[HEADER_INVOCATION_ID];
      if (invocationId !== undefined) {
        node.invocationId = invocationId;
        if (event.headers[HEADER_TURN_CONTINUE] === "true") {
          this.latestContinuationInvocation.set(turnId, invocationId);
        }
      }
      if (event.headers[HEADER_TURN_CONTINUE] !== "true") {
        changed = this.backfillMetadata(node, metadata) || changed;
      }
      if (changed) {
        this.bump();
      }
      this.emitter.emit("turn", node);
      return node;
    }
    const node = this.ensureTurn(
      turnId,
      this.metadataFromHeaders(turnId, event.headers, true),
      undefined,
    );
    const reason =
      event.reason ?? readTurnReason(event.headers[HEADER_TURN_REASON]);
    let changed = false;
    if (reason !== undefined && node.status !== reason) {
      node.status = reason;
      changed = true;
    }
    if (node.endSerial !== event.serial) {
      node.endSerial = event.serial;
      changed = true;
    }
    if (changed) {
      this.bump();
    }
    this.emitter.emit("turn", node);
    return node;
  }

  public delete(codecMessageId: string): void {
    const turnId = this.codecMessageIdToTurnId.get(codecMessageId);
    if (!turnId) {
      return;
    }
    this.codecMessageIdToTurnId.delete(codecMessageId);
    this.headersByCodecMessageId.delete(codecMessageId);
    const ids = this.turnCodecMessageIds.get(turnId);
    ids?.delete(codecMessageId);
    if (ids && ids.size > 0) {
      this.bump();
      return;
    }
    this.removeTurnAndDescendants(turnId);
    this.bump();
  }

  public upsert(
    decodedEvents: readonly DecodedEvent<TEvent>[],
    transportHeaders: HeaderMap,
    serial: TreeSerial,
  ): TurnNode<TProjection> | undefined {
    return this.applyMessage(decodedEvents, transportHeaders, serial);
  }

  public getTurnNode(turnId: string): TurnNode<TProjection> | undefined {
    return this.turnIndex.get(turnId);
  }

  public getTurnByCodecMessageId(
    codecMessageId: string,
  ): TurnNode<TProjection> | undefined {
    const turnId = this.codecMessageIdToTurnId.get(codecMessageId);
    return turnId === undefined ? undefined : this.turnIndex.get(turnId);
  }

  public getNode(codecMessageId: string): TurnNode<TProjection> | undefined {
    return this.getTurnByCodecMessageId(codecMessageId);
  }

  public getSiblingTurns(id: string): readonly TurnNode<TProjection>[] {
    const node = this.resolveNode(id);
    if (!node) {
      return [];
    }
    const cached = this.siblingCache.get(node.turnId);
    if (cached?.version === this.version) {
      return cached.nodes;
    }
    const nodes = this.computeSiblingTurns(node);
    this.siblingCache.set(node.turnId, {
      version: this.version,
      nodes,
    });
    return nodes;
  }

  public getSiblings(id: string): readonly TurnNode<TProjection>[] {
    return this.getSiblingTurns(id);
  }

  public hasSiblingTurns(id: string): boolean {
    return this.getSiblingTurns(id).length > 1;
  }

  public hasSiblings(id: string): boolean {
    return this.hasSiblingTurns(id);
  }

  public getRegenerateGroup(
    codecMessageId: string,
  ): readonly TurnNode<TProjection>[] {
    const cached = this.regenerateCache.get(codecMessageId);
    if (cached?.version === this.version) {
      return cached.nodes;
    }
    const nodes = this.computeRegenerateGroup(codecMessageId);
    this.regenerateCache.set(codecMessageId, {
      version: this.version,
      nodes,
    });
    return nodes;
  }

  public getLatestContinuationInvocation(turnId: string): string | undefined {
    return (
      this.latestContinuationInvocation.get(turnId) ??
      this.turnIndex.get(turnId)?.invocationId
    );
  }

  public getActiveTurnIds(): Map<string, Set<string>> {
    const active = new Map<string, Set<string>>();
    for (const turnId of this.sortedTurns) {
      const node = this.turnIndex.get(turnId);
      if (!node?.clientId || !isLiveStatus(node.status)) {
        continue;
      }
      let turns = active.get(node.clientId);
      if (!turns) {
        turns = new Set();
        active.set(node.clientId, turns);
      }
      turns.add(turnId);
    }
    return active;
  }

  public getHeaders(codecMessageId: string): HeaderMap | undefined {
    return this.headersByCodecMessageId.get(codecMessageId);
  }

  public getTurnNodes(): readonly TurnNode<TProjection>[] {
    return this.sortedTurns.flatMap((turnId) => {
      const node = this.turnIndex.get(turnId);
      return node === undefined ? [] : [node];
    });
  }

  public on<K extends keyof ConversationTreeEvents<TEvent, TProjection>>(
    event: K,
    handler: (payload: ConversationTreeEvents<TEvent, TProjection>[K]) => void,
  ): EventUnsubscribe {
    return this.emitter.on(event, handler);
  }

  private resolveMessageRoute(
    decodedEvents: readonly DecodedEvent<TEvent>[],
    headers: HeaderMap,
  ): { turnId?: string } {
    const codecMessageId =
      headers[HEADER_CODEC_MESSAGE_ID] ??
      decodedEvents[0]?.messageId ??
      decodedEvents[0]?.meta.messageId;
    if (headers[HEADER_TURN_CONTINUE] === "true" && codecMessageId) {
      const existing = this.codecMessageIdToTurnId.get(codecMessageId);
      if (existing) {
        return { turnId: existing };
      }
    }
    const turnId = headers[HEADER_TURN_ID];
    if (turnId) {
      return { turnId };
    }
    if (codecMessageId) {
      const existing = this.codecMessageIdToTurnId.get(codecMessageId);
      if (existing) {
        return { turnId: existing };
      }
    }
    return {};
  }

  private metadataFromHeaders(
    turnId: string,
    headers: HeaderMap,
    allowGraphMetadata: boolean,
  ): TurnMetadata {
    const metadata: TurnMetadata = {};
    if (allowGraphMetadata) {
      const forkRef = headers[HEADER_FORK_OF];
      const forkedTurnId = this.resolveReference(forkRef);
      const parentRef = headers[HEADER_PARENT];
      const parentFromHeader = this.resolveReference(parentRef);
      const parentTurnId =
        forkedTurnId !== undefined
          ? this.turnIndex.get(forkedTurnId)?.parentTurnId
          : parentFromHeader;
      if (parentTurnId !== undefined && parentTurnId !== turnId) {
        metadata.parentTurnId = parentTurnId;
      } else if (parentRef !== undefined) {
        metadata.parentRef = parentRef;
      }
      if (forkedTurnId !== undefined && forkedTurnId !== turnId) {
        metadata.forkOf = forkedTurnId;
      } else if (forkRef !== undefined) {
        metadata.forkRef = forkRef;
      }
      if (headers[HEADER_MSG_REGENERATE] === "true") {
        const anchor =
          headers[HEADER_FORK_OF] ?? headers[HEADER_CODEC_MESSAGE_ID];
        if (anchor !== undefined) {
          metadata.regeneratesCodecMessageId = anchor;
        }
      }
    }
    const clientId =
      headers[HEADER_TURN_CLIENT_ID] ?? headers[HEADER_INPUT_CLIENT_ID];
    if (clientId !== undefined) {
      metadata.clientId = clientId;
    }
    const invocationId = headers[HEADER_INVOCATION_ID];
    if (invocationId !== undefined) {
      metadata.invocationId = invocationId;
    }
    return metadata;
  }

  private ensureTurn(
    turnId: string,
    metadata: TurnMetadata,
    serial: TreeSerial | undefined,
  ): TurnNode<TProjection> {
    const existing = this.turnIndex.get(turnId);
    if (existing) {
      if (this.backfillMetadata(existing, metadata)) {
        this.bump();
      }
      return existing;
    }
    const node: TurnNode<TProjection> = {
      turnId,
      status: "active",
      projection: this.options.createProjection?.() ?? this.reducer.init(),
    };
    setOptional(node, "startSerial", serial);
    this.turnIndex.set(turnId, node);
    this.sortedTurns.push(turnId);
    this.sortTurns();
    this.backfillMetadata(node, metadata);
    this.resolvePendingReferences(turnId, turnId);
    this.bump();
    this.emitter.emit("turn", node);
    return node;
  }

  private backfillMetadata(
    node: TurnNode<TProjection>,
    metadata: TurnMetadata,
  ): boolean {
    let changed = false;
    if (
      metadata.parentTurnId !== undefined &&
      metadata.parentTurnId !== node.turnId &&
      node.parentTurnId !== metadata.parentTurnId
    ) {
      this.moveParent(node, metadata.parentTurnId);
      changed = true;
    } else if (metadata.parentRef !== undefined) {
      this.pendingParentRefByTurn.set(node.turnId, metadata.parentRef);
    } else if (node.parentTurnId === undefined) {
      this.rootTurns.add(node.turnId);
    }
    if (
      metadata.forkOf !== undefined &&
      metadata.forkOf !== node.turnId &&
      node.forkOf !== metadata.forkOf
    ) {
      node.forkOf = metadata.forkOf;
      changed = true;
    } else if (metadata.forkRef !== undefined) {
      this.pendingForkRefByTurn.set(node.turnId, metadata.forkRef);
    }
    if (
      metadata.regeneratesCodecMessageId !== undefined &&
      node.regeneratesCodecMessageId !== metadata.regeneratesCodecMessageId
    ) {
      this.moveRegenerateAnchor(node, metadata.regeneratesCodecMessageId);
      changed = true;
    }
    if (
      metadata.clientId !== undefined &&
      node.clientId !== metadata.clientId
    ) {
      node.clientId = metadata.clientId;
      changed = true;
    }
    if (
      metadata.invocationId !== undefined &&
      node.invocationId !== metadata.invocationId
    ) {
      node.invocationId = metadata.invocationId;
      changed = true;
    }
    return changed;
  }

  private promoteSerial(
    node: TurnNode<TProjection>,
    serial: TreeSerial,
  ): boolean {
    if (
      node.startSerial !== undefined &&
      compareSerial(serial, node.startSerial) >= 0
    ) {
      return false;
    }
    node.startSerial = serial;
    this.sortTurns();
    return true;
  }

  private attachCodecMessage(
    turnId: string,
    codecMessageId: string,
    headers: HeaderMap,
  ): void {
    const previous = this.codecMessageIdToTurnId.get(codecMessageId);
    if (previous === turnId) {
      this.headersByCodecMessageId.set(codecMessageId, headers);
      return;
    }
    if (previous !== undefined) {
      this.turnCodecMessageIds.get(previous)?.delete(codecMessageId);
    }
    this.codecMessageIdToTurnId.set(codecMessageId, turnId);
    this.headersByCodecMessageId.set(codecMessageId, headers);
    let ids = this.turnCodecMessageIds.get(turnId);
    if (!ids) {
      ids = new Set();
      this.turnCodecMessageIds.set(turnId, ids);
    }
    ids.add(codecMessageId);
    this.resolvePendingReferences(codecMessageId, turnId);
    this.bump();
  }

  private resolvePendingReferences(
    reference: string,
    resolvedTurnId: string,
  ): void {
    for (const [turnId, pendingRef] of this.pendingParentRefByTurn) {
      if (pendingRef !== reference || turnId === resolvedTurnId) {
        continue;
      }
      const node = this.turnIndex.get(turnId);
      if (node) {
        this.moveParent(node, resolvedTurnId);
      }
      this.pendingParentRefByTurn.delete(turnId);
    }
    for (const [turnId, pendingRef] of this.pendingForkRefByTurn) {
      if (pendingRef !== reference || turnId === resolvedTurnId) {
        continue;
      }
      const node = this.turnIndex.get(turnId);
      const forked = this.turnIndex.get(resolvedTurnId);
      if (node && forked) {
        node.forkOf = resolvedTurnId;
        this.moveParent(node, forked.parentTurnId);
      }
      this.pendingForkRefByTurn.delete(turnId);
    }
  }

  private moveParent(
    node: TurnNode<TProjection>,
    parentTurnId: string | undefined,
    propagateForkChildren = true,
  ): void {
    if (node.parentTurnId !== undefined) {
      this.parentIndex.get(node.parentTurnId)?.delete(node.turnId);
    } else {
      this.rootTurns.delete(node.turnId);
    }
    if (parentTurnId === undefined) {
      this.rootTurns.add(node.turnId);
      delete node.parentTurnId;
      if (propagateForkChildren) {
        this.moveForkChildrenToParent(node, new Set());
      }
      return;
    }
    node.parentTurnId = parentTurnId;
    let children = this.parentIndex.get(parentTurnId);
    if (!children) {
      children = new Set();
      this.parentIndex.set(parentTurnId, children);
    }
    children.add(node.turnId);
    if (propagateForkChildren) {
      this.moveForkChildrenToParent(node, new Set());
    }
  }

  private moveRegenerateAnchor(
    node: TurnNode<TProjection>,
    anchor: string,
  ): void {
    if (node.regeneratesCodecMessageId !== undefined) {
      this.regenerateByMsgId
        .get(node.regeneratesCodecMessageId)
        ?.delete(node.turnId);
    }
    node.regeneratesCodecMessageId = anchor;
    let turns = this.regenerateByMsgId.get(anchor);
    if (!turns) {
      turns = new Set();
      this.regenerateByMsgId.set(anchor, turns);
    }
    turns.add(node.turnId);
  }

  private moveForkChildrenToParent(
    node: TurnNode<TProjection>,
    seen: Set<string>,
  ): void {
    if (seen.has(node.turnId)) {
      return;
    }
    seen.add(node.turnId);
    for (const candidate of this.turnIndex.values()) {
      if (
        candidate.turnId !== node.turnId &&
        candidate.forkOf === node.turnId
      ) {
        this.moveParent(candidate, node.parentTurnId, false);
        this.moveForkChildrenToParent(candidate, seen);
      }
    }
  }

  private removeTurnAndDescendants(turnId: string): void {
    const children = this.parentIndex.get(turnId);
    if (children) {
      for (const child of Array.from(children)) {
        this.removeTurnAndDescendants(child);
      }
    }
    const node = this.turnIndex.get(turnId);
    if (!node) {
      return;
    }
    if (node.parentTurnId !== undefined) {
      this.parentIndex.get(node.parentTurnId)?.delete(turnId);
    } else {
      this.rootTurns.delete(turnId);
    }
    if (node.regeneratesCodecMessageId !== undefined) {
      this.regenerateByMsgId
        .get(node.regeneratesCodecMessageId)
        ?.delete(turnId);
    }
    for (const codecMessageId of this.turnCodecMessageIds.get(turnId) ?? []) {
      this.codecMessageIdToTurnId.delete(codecMessageId);
      this.headersByCodecMessageId.delete(codecMessageId);
    }
    this.turnCodecMessageIds.delete(turnId);
    this.turnIndex.delete(turnId);
    this.pendingParentRefByTurn.delete(turnId);
    this.pendingForkRefByTurn.delete(turnId);
    this.parentIndex.delete(turnId);
    const index = this.sortedTurns.indexOf(turnId);
    if (index !== -1) {
      this.sortedTurns.splice(index, 1);
    }
    this.latestContinuationInvocation.delete(turnId);
  }

  private computeSiblingTurns(
    node: TurnNode<TProjection>,
  ): TurnNode<TProjection>[] {
    const candidateIds =
      node.parentTurnId === undefined
        ? this.rootTurns
        : this.parentIndex.get(node.parentTurnId);
    if (!candidateIds) {
      return [node];
    }
    const candidateSet = new Set(candidateIds);
    const group = new Set<string>([node.turnId]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const candidateId of candidateSet) {
        if (
          group.has(candidateId) ||
          this.isDescendant(candidateId, node.turnId)
        ) {
          continue;
        }
        const candidate = this.turnIndex.get(candidateId);
        if (!candidate) {
          continue;
        }
        if (
          (candidate.forkOf !== undefined && group.has(candidate.forkOf)) ||
          (node.forkOf !== undefined && candidateId === node.forkOf) ||
          this.forkChainTouches(candidate, group)
        ) {
          group.add(candidateId);
          changed = true;
        }
      }
    }
    return this.nodesInSortOrder(group);
  }

  private computeRegenerateGroup(
    codecMessageId: string,
  ): TurnNode<TProjection>[] {
    const nodes: TurnNode<TProjection>[] = [];
    const owner = this.getTurnByCodecMessageId(codecMessageId);
    if (owner) {
      nodes.push(owner);
    }
    const regenerates = this.regenerateByMsgId.get(codecMessageId);
    if (regenerates) {
      for (const node of this.nodesInSortOrder(regenerates)) {
        if (node.turnId !== owner?.turnId) {
          nodes.push(node);
        }
      }
    }
    return nodes;
  }

  private forkChainTouches(
    node: TurnNode<TProjection>,
    group: Set<string>,
  ): boolean {
    const seen = new Set<string>();
    let current: string | undefined = node.forkOf;
    while (current !== undefined && !seen.has(current)) {
      if (group.has(current)) {
        return true;
      }
      seen.add(current);
      current = this.turnIndex.get(current)?.forkOf;
    }
    return false;
  }

  private isDescendant(candidateId: string, ancestorId: string): boolean {
    const seen = new Set<string>();
    let current = this.turnIndex.get(candidateId)?.parentTurnId;
    while (current !== undefined && !seen.has(current)) {
      if (current === ancestorId) {
        return true;
      }
      seen.add(current);
      current = this.turnIndex.get(current)?.parentTurnId;
    }
    return false;
  }

  private nodesInSortOrder(ids: Set<string>): TurnNode<TProjection>[] {
    const nodes: TurnNode<TProjection>[] = [];
    for (const turnId of this.sortedTurns) {
      if (ids.has(turnId)) {
        const node = this.turnIndex.get(turnId);
        if (node) {
          nodes.push(node);
        }
      }
    }
    return nodes;
  }

  private resolveNode(id: string): TurnNode<TProjection> | undefined {
    return this.turnIndex.get(id) ?? this.getTurnByCodecMessageId(id);
  }

  private resolveReference(reference: string | undefined): string | undefined {
    if (reference === undefined) {
      return undefined;
    }
    return (
      this.codecMessageIdToTurnId.get(reference) ??
      this.turnIndex.get(reference)?.turnId
    );
  }

  private sortTurns(): void {
    this.sortedTurns.sort((left, right) => {
      const leftNode = this.turnIndex.get(left);
      const rightNode = this.turnIndex.get(right);
      return compareOptionalSerial(
        leftNode?.startSerial,
        rightNode?.startSerial,
      );
    });
  }

  private bump(): void {
    this.version += 1;
    this.siblingCache.clear();
    this.regenerateCache.clear();
    this.emitter.emit("update", { structuralVersion: this.version });
  }
}

interface TurnMetadata {
  parentTurnId?: string;
  parentRef?: string;
  forkOf?: string;
  forkRef?: string;
  regeneratesCodecMessageId?: string;
  clientId?: string;
  invocationId?: string;
}

interface CachedNodes<TProjection> {
  version: number;
  nodes: readonly TurnNode<TProjection>[];
}

function readTurnReason(value: string | undefined): TurnEndReason | undefined {
  switch (value) {
    case "complete":
    case "cancelled":
    case "error":
    case "suspended":
      return value;
    default:
      return undefined;
  }
}

function isTerminalStatus(status: TurnStatus): boolean {
  return status === "complete" || status === "cancelled" || status === "error";
}

function isLiveStatus(status: TurnStatus): boolean {
  return status === "active" || status === "suspended";
}

function compareOptionalSerial(
  left: TreeSerial | undefined,
  right: TreeSerial | undefined,
): -1 | 0 | 1 {
  if (left === undefined && right === undefined) {
    return 0;
  }
  if (left === undefined) {
    return 1;
  }
  if (right === undefined) {
    return -1;
  }
  return compareSerial(left, right);
}

function compareSerial(left: TreeSerial, right: TreeSerial): -1 | 0 | 1 {
  const leftBigInt = serialBigInt(left);
  const rightBigInt = serialBigInt(right);
  if (leftBigInt !== undefined && rightBigInt !== undefined) {
    return leftBigInt < rightBigInt ? -1 : leftBigInt > rightBigInt ? 1 : 0;
  }
  return String(left) < String(right)
    ? -1
    : String(left) > String(right)
      ? 1
      : 0;
}

function serialBigInt(value: TreeSerial): bigint | undefined {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? BigInt(value) : undefined;
  }
  return /^\d+$/.test(value) ? BigInt(value) : undefined;
}

function setOptional<T extends object, K extends keyof T>(
  target: T,
  key: K,
  value: T[K] | undefined,
): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

/**
 * Transport roles used by tree routing.
 */
export const treeRoutingRoles = {
  /** User input role. */
  user: "user",
  /** Assistant output role. */
  assistant: "assistant",
  /** Tool output role. */
  tool: "tool",
} as const satisfies Record<string, string>;

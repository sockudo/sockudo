import Foundation

struct ProcessedMessage: Sendable {
  let event: SockudoEvent
  let recoveryUpdate: (channel: String, position: RecoveryPosition)?
  let resyncChannel: String?
  let updatedDeltaStats: DeltaStats?
}

/// Processes incoming WebSocket messages off MainActor: protocol decoding,
/// deduplication, and delta reconstruction. Delivers fully-processed
/// `ProcessedMessage` values for MainActor routing and callback delivery.
actor MessageProcessor {
  private let pipeline: MessagePipeline
  private let prefix: ProtocolPrefix
  private var deduplicator: MessageDeduplicator?
  private var deltaManager: DeltaCompressionManager?

  init(
    pipeline: MessagePipeline, prefix: ProtocolPrefix,
    deduplicationCapacity: Int?, deltaOptions: DeltaOptions?
  ) {
    self.pipeline = pipeline
    self.prefix = prefix
    self.deduplicator = deduplicationCapacity.map { MessageDeduplicator(capacity: $0) }
    self.deltaManager = deltaOptions.map { DeltaCompressionManager(options: $0, prefix: prefix) }
  }

  func process(_ message: URLSessionWebSocketTask.Message) throws -> ProcessedMessage? {
    let event = try pipeline.decode(message)

    if let id = event.messageId {
      if deduplicator?.isDuplicate(id) == true { return nil }
      deduplicator?.track(id)
    }

    let recoveryUpdate: (channel: String, position: RecoveryPosition)?
    if let ch = event.channel, let serial = event.serial {
      recoveryUpdate = (
        ch,
        RecoveryPosition(
          streamID: event.streamID, serial: serial, lastMessageID: event.messageId)
      )
    } else {
      recoveryUpdate = nil
    }

    if event.event == prefix.event("delta"), let ch = event.channel {
      guard let reconstructed = deltaManager?.handleDeltaMessage(channel: ch, data: event.data)
      else {
        return ProcessedMessage(
          event: event, recoveryUpdate: recoveryUpdate,
          resyncChannel: ch, updatedDeltaStats: deltaManager?.getStats())
      }
      return ProcessedMessage(
        event: reconstructed, recoveryUpdate: recoveryUpdate,
        resyncChannel: nil, updatedDeltaStats: deltaManager?.getStats())
    }

    if event.event == prefix.event("delta_cache_sync"), let ch = event.channel {
      deltaManager?.handleCacheSync(channel: ch, data: event.data)
    }
    if event.event == prefix.event("delta_compression_enabled") {
      deltaManager?.handleEnabled(event.data)
    }

    if let ch = event.channel, let seq = event.sequence,
      !prefix.isPlatformEvent(event.event), !prefix.isInternalEvent(event.event)
    {
      let stripped = pipeline.stripDeltaMetadata(from: message)
      deltaManager?.handleFullMessage(
        channel: ch, rawMessage: stripped,
        sequence: seq, conflationKey: event.conflationKey)
    }

    return ProcessedMessage(
      event: event, recoveryUpdate: recoveryUpdate,
      resyncChannel: nil, updatedDeltaStats: nil)
  }

  func clearDeltaState(_ channel: String?) { deltaManager?.clearChannelState(channel) }
  func getDeltaStats() -> DeltaStats? { deltaManager?.getStats() }
  func resetDeltaStats() { deltaManager?.resetStats() }
  func handleFullMessage(
    channel: String, rawMessage: String, sequence: Int?, conflationKey: String?
  ) {
    deltaManager?.handleFullMessage(
      channel: channel, rawMessage: rawMessage,
      sequence: sequence, conflationKey: conflationKey)
  }
}

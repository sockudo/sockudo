import AblyDeltaCodec
import Foundation

private struct CachedMessage {
  let content: String
  let sequence: Int
}

private final class ChannelState {
  let channelName: String
  var conflationKey: String?
  var maxMessagesPerKey = 30
  var conflationCaches: [String: [CachedMessage]] = [:]
  var baseMessage: String?
  var baseSequence: Int?
  var lastSequence: Int?
  var deltaCount = 0
  var fullMessageCount = 0

  init(channelName: String) {
    self.channelName = channelName
  }

  func initialize(from syncData: [String: Any]) {
    conflationKey = syncData["conflation_key"] as? String
    maxMessagesPerKey = max(Int(syncData["max_messages_per_key"] as? Int ?? 10), 30)
    conflationCaches.removeAll()
    if let states = syncData["states"] as? [String: [[String: Any]]] {
      for (key, messages) in states {
        conflationCaches[key] = messages.compactMap { item in
          guard let content = item["content"] as? String,
            let sequence = item["seq"] as? Int
          else {
            return nil
          }
          return CachedMessage(content: content, sequence: sequence)
        }
      }
    }
  }

  func baseMessage(for conflationValue: String?, baseIndex: Int?) -> String? {
    guard conflationKey != nil else { return baseMessage }
    guard let baseIndex else { return nil }
    let key = conflationValue ?? ""
    return conflationCaches[key]?.first(where: { $0.sequence == baseIndex })?.content
  }

  func update(message: String, sequence: Int, conflationValue: String?) {
    if conflationKey != nil || conflationValue != nil {
      let key = conflationValue ?? ""
      var cache = conflationCaches[key, default: []]
      cache.append(CachedMessage(content: message, sequence: sequence))
      while cache.count > maxMessagesPerKey {
        cache.removeFirst()
      }
      conflationCaches[key] = cache
      if conflationKey == nil {
        conflationKey = "enabled"
      }
    } else {
      baseMessage = message
      baseSequence = sequence
    }
    lastSequence = sequence
  }

  func stats() -> ChannelDeltaStats {
    ChannelDeltaStats(
      channelName: channelName,
      conflationKey: conflationKey,
      conflationGroupCount: conflationCaches.count,
      deltaCount: deltaCount,
      fullMessageCount: fullMessageCount,
      totalMessages: deltaCount + fullMessageCount
    )
  }
}

final class DeltaCompressionManager {
  private let options: DeltaOptions
  private let sendEvent: (String, Any) -> Bool
  private let prefix: ProtocolPrefix
  private var enabled = false
  private var channelStates: [String: ChannelState] = [:]
  private var defaultAlgorithm: DeltaAlgorithm = .fossil
  private var totals = (messages: 0, delta: 0, full: 0, rawBytes: 0, wireBytes: 0, errors: 0)

  init(
    options: DeltaOptions, prefix: ProtocolPrefix = ProtocolPrefix(version: 2),
    sendEvent: @escaping (String, Any) -> Bool
  ) {
    self.options = options
    self.prefix = prefix
    self.sendEvent = sendEvent
  }

  func enable() {
    guard enabled == false else { return }
    let payload: [String: Any] = ["algorithms": options.algorithms.map(\.rawValue)]
    _ = sendEvent(prefix.event("enable_delta_compression"), payload)
  }

  func disable() {
    enabled = false
  }

  func handleEnabled(_ data: Any?) {
    guard let dictionary = data as? [String: Any] else {
      enabled = true
      return
    }
    enabled = dictionary["enabled"] as? Bool ?? true
    if let algorithmName = dictionary["algorithm"] as? String,
      let algorithm = DeltaAlgorithm(rawValue: algorithmName)
    {
      defaultAlgorithm = algorithm
    }
  }

  func handleCacheSync(channel: String, data: Any?) {
    guard let dictionary = data as? [String: Any] else { return }
    let state = channelStates[channel] ?? ChannelState(channelName: channel)
    state.initialize(from: dictionary)
    channelStates[channel] = state
  }

  func handleDeltaMessage(channel: String, data: Any?) -> SockudoEvent? {
    guard
      let payload = data as? [String: Any],
      let event = payload["event"] as? String,
      let delta = payload["delta"] as? String,
      let sequence = payload["seq"] as? Int
    else {
      recordError(SockudoError.deltaFailure("Invalid delta payload"))
      return nil
    }

    let algorithm =
      DeltaAlgorithm(rawValue: payload["algorithm"] as? String ?? "") ?? defaultAlgorithm
    let conflationKey = payload["conflation_key"] as? String
    let baseIndex = payload["base_index"] as? Int

    guard let channelState = channelStates[channel] else {
      return nil
    }
    guard let baseMessage = channelState.baseMessage(for: conflationKey, baseIndex: baseIndex)
    else {
      requestResync(channel: channel)
      return nil
    }
    guard let deltaData = Data(base64Encoded: delta) else {
      recordError(SockudoError.deltaFailure("Invalid base64 delta payload"))
      return nil
    }

    do {
      let reconstructed: String
      switch algorithm {
      case .fossil:
        reconstructed = try String(
          decoding: FossilDelta.apply(base: Data(baseMessage.utf8), delta: deltaData),
          as: UTF8.self)
      case .xdelta3:
        let output = try ARTDeltaCodec.applyDelta(
          deltaData, previous: Data(baseMessage.utf8))
        reconstructed = String(decoding: output, as: UTF8.self)
      }

      channelState.update(
        message: reconstructed, sequence: sequence, conflationValue: conflationKey)
      channelState.deltaCount += 1
      totals.messages += 1
      totals.delta += 1
      totals.rawBytes += reconstructed.utf8.count
      totals.wireBytes += deltaData.count
      emitStats()

      let parsed: Any
      if let reconstructedData = reconstructed.data(using: .utf8),
        let json = try? JSON.decode(reconstructedData)
      {
        parsed = json
      } else {
        parsed = reconstructed
      }

      let eventData: Any?
      if let dictionary = parsed as? [String: Any], dictionary.keys.contains("data") {
        eventData = dictionary["data"]
      } else {
        eventData = parsed
      }

      return SockudoEvent(
        event: event,
        channel: channel,
        data: eventData,
        userID: nil,
        streamID: nil,
        messageId: nil,
        rawMessage: reconstructed,
        sequence: sequence,
        conflationKey: conflationKey,
        serial: nil,
        extras: nil
      )
    } catch {
      recordError(error)
      return nil
    }
  }

  func handleFullMessage(
    channel: String, rawMessage: String, sequence: Int?, conflationKey: String?
  ) {
    guard let sequence else { return }
    let state = channelStates[channel] ?? ChannelState(channelName: channel)
    state.update(message: rawMessage, sequence: sequence, conflationValue: conflationKey)
    state.fullMessageCount += 1
    channelStates[channel] = state
    totals.messages += 1
    totals.full += 1
    totals.rawBytes += rawMessage.utf8.count
    totals.wireBytes += rawMessage.utf8.count
    emitStats()
  }

  func getStats() -> DeltaStats {
    let saved = totals.rawBytes - totals.wireBytes
    let percentage = totals.rawBytes > 0 ? (Double(saved) / Double(totals.rawBytes)) * 100 : 0
    return DeltaStats(
      totalMessages: totals.messages,
      deltaMessages: totals.delta,
      fullMessages: totals.full,
      totalBytesWithoutCompression: totals.rawBytes,
      totalBytesWithCompression: totals.wireBytes,
      bandwidthSaved: saved,
      bandwidthSavedPercent: percentage,
      errors: totals.errors,
      channelCount: channelStates.count,
      channels: channelStates.values.map { $0.stats() }.sorted {
        $0.channelName < $1.channelName
      }
    )
  }

  func resetStats() {
    totals = (0, 0, 0, 0, 0, 0)
    emitStats()
  }

  func clearChannelState(_ channel: String? = nil) {
    if let channel {
      channelStates[channel] = nil
    } else {
      channelStates.removeAll()
    }
  }

  private func requestResync(channel: String) {
    _ = sendEvent(prefix.event("delta_sync_error"), ["channel": channel])
    channelStates[channel] = nil
  }

  private func recordError(_ error: Error) {
    totals.errors += 1
    options.onError?(error)
  }

  private func emitStats() {
    options.onStats?(getStats())
  }
}

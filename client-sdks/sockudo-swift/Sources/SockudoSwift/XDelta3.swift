import Foundation
import CXDelta3

enum XDelta3: Sendable {
  static func decode(delta: Data, base: Data) throws -> Data {
    try delta.withUnsafeBytes { deltaPtr in
      try base.withUnsafeBytes { basePtr in
        let maxOutput = 32 * 1024 * 1024
        var output = Data(count: maxOutput)
        var outputSize = UInt64(maxOutput)

        let result = output.withUnsafeMutableBytes { outPtr in
          xd3_decode_memory(
            deltaPtr.baseAddress?.assumingMemoryBound(to: UInt8.self),
            UInt64(delta.count),
            basePtr.baseAddress?.assumingMemoryBound(to: UInt8.self),
            UInt64(base.count),
            outPtr.baseAddress?.assumingMemoryBound(to: UInt8.self),
            &outputSize,
            UInt64(maxOutput),
            0
          )
        }

        guard result == 0 else {
          throw SockudoError.deltaFailure("xd3_decode_memory failed with code \(result)")
        }

        output.count = Int(outputSize)
        return output
      }
    }
  }
}

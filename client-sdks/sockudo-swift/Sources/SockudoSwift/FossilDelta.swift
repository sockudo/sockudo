import Foundation

enum FossilDelta {
  private static let nHash = 16
  private static let digits = Array(
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~".utf8)
  private static let values: [Int] = {
    var map = Array(repeating: -1, count: 128)
    for (index, char) in digits.enumerated() where Int(char) < map.count {
      map[Int(char)] = index
    }
    return map
  }()

  private struct Reader {
    let data: [UInt8]
    var position = 0

    var hasBytes: Bool { position < data.count }

    mutating func byte() throws -> UInt8 {
      guard position < data.count else { throw SockudoError.deltaFailure("out of bounds") }
      defer { position += 1 }
      return data[position]
    }

    mutating func character() throws -> Character {
      Character(UnicodeScalar(try byte()))
    }

    mutating func integer() throws -> Int {
      var value = 0
      while hasBytes {
        let raw = try byte()
        let mapped = raw < 128 ? Self.values[Int(raw)] : -1
        if mapped < 0 {
          position -= 1
          break
        }
        value = (value << 6) + mapped
      }
      return value
    }

    private static let values = FossilDelta.values
  }

  private final class Writer {
    var bytes: [UInt8] = []
    func append(contentsOf slice: ArraySlice<UInt8>) { bytes.append(contentsOf: slice) }
  }

  static func apply(base: Data, delta: Data) throws -> Data {
    var reader = Reader(data: Array(delta))
    let outputSize = try reader.integer()
    guard try reader.character() == "\n" else {
      throw SockudoError.deltaFailure("size integer not terminated by newline")
    }

    let source = Array(base)
    let deltaBytes = Array(delta)
    let writer = Writer()
    var total = 0

    while reader.hasBytes {
      let count = try reader.integer()
      switch try reader.character() {
      case "@":
        let offset = try reader.integer()
        if reader.hasBytes, try reader.character() != "," {
          throw SockudoError.deltaFailure("copy command not terminated by comma")
        }
        total += count
        guard total <= outputSize else {
          throw SockudoError.deltaFailure("copy exceeds output file size")
        }
        guard offset + count <= source.count else {
          throw SockudoError.deltaFailure("copy extends past end of input")
        }
        writer.append(contentsOf: source[offset..<(offset + count)])
      case ":":
        total += count
        guard total <= outputSize else {
          throw SockudoError.deltaFailure(
            "insert command gives an output larger than predicted")
        }
        guard reader.position + count <= deltaBytes.count else {
          throw SockudoError.deltaFailure("insert count exceeds size of delta")
        }
        writer.append(contentsOf: deltaBytes[reader.position..<(reader.position + count)])
        reader.position += count
      case ";":
        let output = Data(writer.bytes)
        guard count == checksum(writer.bytes) else {
          throw SockudoError.deltaFailure("bad checksum")
        }
        guard total == outputSize else {
          throw SockudoError.deltaFailure("generated size does not match predicted size")
        }
        return output
      default:
        throw SockudoError.deltaFailure("unknown delta operator")
      }
    }

    throw SockudoError.deltaFailure("unterminated delta")
  }

  private static func checksum(_ bytes: [UInt8]) -> Int {
    var sum0 = 0
    var sum1 = 0
    var sum2 = 0
    var sum3 = 0
    var index = 0
    var remaining = bytes.count

    while remaining >= 16 {
      sum0 +=
        Int(bytes[index + 0]) + Int(bytes[index + 4]) + Int(bytes[index + 8])
        + Int(bytes[index + 12])
      sum1 +=
        Int(bytes[index + 1]) + Int(bytes[index + 5]) + Int(bytes[index + 9])
        + Int(bytes[index + 13])
      sum2 +=
        Int(bytes[index + 2]) + Int(bytes[index + 6]) + Int(bytes[index + 10])
        + Int(bytes[index + 14])
      sum3 +=
        Int(bytes[index + 3]) + Int(bytes[index + 7]) + Int(bytes[index + 11])
        + Int(bytes[index + 15])
      index += 16
      remaining -= 16
    }

    while remaining >= 4 {
      sum0 += Int(bytes[index + 0])
      sum1 += Int(bytes[index + 1])
      sum2 += Int(bytes[index + 2])
      sum3 += Int(bytes[index + 3])
      index += 4
      remaining -= 4
    }

    sum3 = (((sum3 + (sum2 << 8)) + (sum1 << 16)) + (sum0 << 24))
    switch remaining {
    case 3:
      sum3 += Int(bytes[index + 2]) << 8
      fallthrough
    case 2:
      sum3 += Int(bytes[index + 1]) << 16
      fallthrough
    case 1:
      sum3 += Int(bytes[index + 0]) << 24
    default:
      break
    }

    return Int(UInt32(bitPattern: Int32(sum3)))
  }
}

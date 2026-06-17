import AnyCodable
import Foundation

struct MockEventData: Codable {
  let name: String
  let age: Int
  let job: String
  let metadata: [String: AnyCodable]
}

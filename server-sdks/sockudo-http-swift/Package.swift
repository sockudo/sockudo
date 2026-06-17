// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "Pusher",
  platforms: [
    .iOS(.v13),
    .macOS(.v10_15),
    .tvOS(.v13),
    .watchOS(.v6),
  ],
  products: [
    .library(
      name: "Pusher",
      targets: ["Pusher"]),
    .library(
      name: "Sockudo",
      targets: ["Sockudo"]),
  ],
  dependencies: [
    // Source code linting
    .package(
      url: "https://github.com/realm/SwiftLint",
      .upToNextMajor(from: "0.43.1")),
    // Simple REST API client implementation with 'Codable' types
    .package(
      url: "https://github.com/danielrbrowne/APIota",
      .upToNextMajor(from: "0.2.0")),
    .package(
      url: "https://github.com/Flight-School/AnyCodable",
      .upToNextMajor(from: "0.4.0")),
    // Open-source implementation of Apple's `CryptoKit`
    // (Allows for crypto on Linux, and calls CryptoKit directly on Apple platforms)
    .package(
      url: "https://github.com/apple/swift-crypto",
      .upToNextMajor(from: "1.1.6")),
    // Swift wrapper for TweetNaCl crypto functionality
    .package(
      name: "TweetNacl",
      url: "https://github.com/bitmark-inc/tweetnacl-swiftwrap",
      .revision("f8fd111642bf2336b11ef9ea828510693106e954")),
  ],
  targets: [
    .target(
      name: "Pusher",
      dependencies: [
        "APIota",
        "AnyCodable",
        .product(name: "Crypto", package: "swift-crypto"),
        "TweetNacl",
      ]),
    .target(
      name: "Sockudo",
      dependencies: [
        "APIota",
        "AnyCodable",
        .product(name: "Crypto", package: "swift-crypto"),
        "TweetNacl",
      ]),
    .testTarget(
      name: "PusherTests",
      dependencies: ["Pusher"]),
    .testTarget(
      name: "SockudoTests",
      dependencies: ["Sockudo"]),
  ]
)

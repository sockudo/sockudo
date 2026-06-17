// swift-tools-version: 6.2

import PackageDescription

let package = Package(
  name: "SockudoSwift",
  platforms: [
    .iOS(.v13),
    .macOS(.v10_15),
    .tvOS(.v13),
    .watchOS(.v6),
    .visionOS(.v1),
  ],
  products: [
    .library(
      name: "SockudoSwift",
      targets: ["SockudoSwift"]
    )
  ],
  dependencies: [
    .package(url: "https://github.com/jedisct1/swift-sodium.git", from: "0.9.1"),
    .package(url: "https://github.com/ably/delta-codec-cocoa.git", from: "1.0.0"),
    .package(url: "https://github.com/swiftlang/swift-testing.git", from: "0.12.0"),
  ],
  targets: [
    .target(
      name: "SockudoSwift",
      dependencies: [
        .product(name: "Sodium", package: "swift-sodium"),
        .product(name: "AblyDeltaCodec", package: "delta-codec-cocoa"),
      ],
      swiftSettings: [
        .unsafeFlags(["-Xfrontend", "-strict-concurrency=minimal"])
      ]
    ),
    .testTarget(
      name: "SockudoSwiftTests",
      dependencies: [
        "SockudoSwift",
        .product(name: "Testing", package: "swift-testing"),
      ],
      swiftSettings: [
        .unsafeFlags([
          "-Xfrontend", "-strict-concurrency=minimal",
          "-suppress-warnings",
        ])
      ]
    ),
  ]
)

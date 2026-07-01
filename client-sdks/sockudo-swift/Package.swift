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
    .package(url: "https://github.com/swiftlang/swift-testing.git", from: "0.12.0"),
  ],
  targets: [
    .target(
      name: "CXDelta3",
      path: "Vendor/xdelta3",
      sources: ["xdelta3.c"],
      publicHeadersPath: "include",
      cSettings: [
        .headerSearchPath("."),
        .define("SIZEOF_SIZE_T", to: "8"),
        .define("SIZEOF_UNSIGNED_INT", to: "4"),
        .define("SIZEOF_UNSIGNED_LONG", to: "8"),
        .define("SIZEOF_UNSIGNED_LONG_LONG", to: "8"),
      ]
    ),
    .target(
      name: "SockudoSwift",
      dependencies: [
        .product(name: "Sodium", package: "swift-sodium"),
        "CXDelta3",
      ]
    ),
    .testTarget(
      name: "SockudoSwiftTests",
      dependencies: [
        "SockudoSwift",
        .product(name: "Testing", package: "swift-testing"),
      ],
      swiftSettings: [
        .unsafeFlags(["-suppress-warnings"])
      ]
    ),
  ]
)

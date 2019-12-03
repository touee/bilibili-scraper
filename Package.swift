// swift-tools-version:5.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "bilibili-scraper",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "BilibiliEntityDB",
            targets: ["BilibiliEntityDB"]),
        .executable(
            name: "bilibili-scraper",
            targets: ["bilibili-scraper"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.0.0"),
        
        .package(url: "https://github.com/touee/SwiftTask.git", from: "0.0.12"),
        
        .package(url: "https://github.com/stephencelis/SQLite.swift.git", from: "0.12.0"),
        .package(url: "https://github.com/IBM-Swift/HeliumLogger.git", .upToNextMinor(from: "1.9.0")),
        .package(url: "https://github.com/SwiftyJSON/SwiftyJSON.git", .upToNextMinor(from: "5.0.0")),
        
        .package(url: "https://github.com/touee/BilibiliAPI.git", .upToNextMinor(from: "0.1.3")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "BilibiliEntityDB",
            dependencies: ["SQLite"]),
        .target(
            name: "bilibili-scraper",
            dependencies: ["BilibiliAPI", "BilibiliEntityDB", "SwiftTask", "AsyncHTTPClient", "SQLite", "SwiftyJSON", "HeliumLogger"]),
    ]
)

import XCTest
@testable import NornicDB

final class ModelDownloadTests: XCTestCase {
    func testEachServiceDownloadsOnlyItsOwnDefault() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        for model in DefaultLocalModel.allCases {
            var calls = 0
            try downloadDefaultLocalModel(model, into: directory) { arguments in
                calls += 1
                XCTAssertEqual(arguments.last, model.downloadURL)
                XCTAssertEqual(arguments.prefix(3), ["--fail", "--location", "--output"])
                try ggufMagicHeader.write(to: URL(fileURLWithPath: arguments[3]))
                return 0
            }
            XCTAssertEqual(calls, 1)
            XCTAssertTrue(isValidGGUFFile(atPath: directory.appendingPathComponent(model.filename).path))
        }
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: directory.path).count, 3)
    }

    func testReadyModelSkipsDownloadEntirely() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        for model in DefaultLocalModel.allCases {
            let destination = directory.appendingPathComponent(model.filename)
            let original = ggufMagicHeader + Data([1, 2, 3])
            try original.write(to: destination)
            try downloadDefaultLocalModel(model, into: directory) { _ in
                XCTFail("An installed valid model must not download again")
                return 1
            }
            XCTAssertEqual(try Data(contentsOf: destination), original)
        }
    }

    func testFailedDownloadPreservesExistingModelAndRemovesPartialFile() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let destination = directory.appendingPathComponent(DefaultLocalModel.embeddings.filename)
        let original = Data("previous invalid file".utf8)
        try original.write(to: destination)
        for exitCode: Int32 in [0, 22] {
            XCTAssertThrowsError(try downloadDefaultLocalModel(.embeddings, into: directory) { arguments in
                try Data("invalid download".utf8).write(to: URL(fileURLWithPath: arguments[3]))
                return exitCode
            })
            XCTAssertEqual(try Data(contentsOf: destination), original)
            XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: directory.path), [destination.lastPathComponent])
        }
    }
}

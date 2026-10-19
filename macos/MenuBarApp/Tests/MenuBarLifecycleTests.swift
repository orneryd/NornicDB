import XCTest
@testable import NornicDB

final class MenuBarLifecycleTests: XCTestCase {
    private func temporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: directory) }
        return directory
    }

    func testOnlyOneOwnerAndOwnershipCanTransferAfterExit() throws {
        let path = try temporaryDirectory().appendingPathComponent("session/menubar.lock")
        var first: MenuBarInstanceLock? = MenuBarInstanceLock()
        let second = MenuBarInstanceLock()
        XCTAssertTrue(try XCTUnwrap(first).acquire(at: path))
        XCTAssertFalse(try second.acquire(at: path))
        first = nil
        XCTAssertTrue(try second.acquire(at: path))
        let third = MenuBarInstanceLock()
        XCTAssertFalse(try third.acquire(at: path))
        second.release()
        XCTAssertTrue(try third.acquire(at: path))
    }

    func testLockRefusesSymlinksWithoutChangingTheirTarget() throws {
        let directory = try temporaryDirectory()
        let target = directory.appendingPathComponent("keep.txt")
        let path = directory.appendingPathComponent("menubar.lock")
        try "preserve".write(to: target, atomically: true, encoding: .utf8)
        try FileManager.default.createSymbolicLink(at: path, withDestinationURL: target)
        XCTAssertThrowsError(try MenuBarInstanceLock().acquire(at: path))
        XCTAssertEqual(try String(contentsOf: target), "preserve")
    }

    func testUpgradeReviewAppearsWithoutResettingExistingConfig() throws {
        let directory = try temporaryDirectory()
        let yaml = directory.appendingPathComponent("config.yaml")
        let contents = "embedding:\n  provider: openai\n  model: text-embedding-3-small\n"
        try contents.write(to: yaml, atomically: true, encoding: .utf8)
        let config = ConfigManager(configPath: yaml.path)
        XCTAssertFalse(config.isFirstRun())
        try Data().write(to: directory.appendingPathComponent(".setup_review"))
        XCTAssertTrue(config.isFirstRun())
        config.completeFirstRun()
        XCTAssertFalse(config.isFirstRun())
        XCTAssertEqual(try String(contentsOf: yaml), contents)
        try Data().write(to: directory.appendingPathComponent(".first_run"))
        XCTAssertTrue(config.isFirstRun())
        config.completeFirstRun()
        XCTAssertFalse(config.isFirstRun())
    }
}

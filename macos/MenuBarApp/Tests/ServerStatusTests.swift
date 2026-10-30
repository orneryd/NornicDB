import XCTest
@testable import NornicDB

final class ServerStatusTests: XCTestCase {
    func testInitializingProcessStaysYellowUntilHealthy() {
        for _ in 0..<10 {
            XCTAssertEqual(observedServerStatus(healthy: false, processRunning: true), .starting)
        }
        XCTAssertEqual(observedServerStatus(healthy: true, processRunning: true), .running)
        XCTAssertEqual(observedServerStatus(healthy: false, processRunning: false), .stopped)
    }

    func testOnlyTopLevelLivePIDCountsAsStarting() {
        XCTAssertTrue(launchAgentHasProcess("job = {\n\tstate = running\n\tpid = 123\n}"))
        XCTAssertFalse(launchAgentHasProcess("job = {\n\tstate = not running\n\tlast exit code = 1\n}"))
        XCTAssertFalse(launchAgentHasProcess("\tpid = 0\n\t\tpid = 123\n"))
    }
}

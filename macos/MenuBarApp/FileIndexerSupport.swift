import SwiftUI
import AppKit

final class FileIndexerLogger {
    static let shared = FileIndexerLogger()

    enum Level: String {
        case info = "INFO"
        case warning = "WARN"
        case error = "ERROR"
    }

    private let queue = DispatchQueue(label: "com.nornicdb.file-indexer.logger")
    private let iso8601: ISO8601DateFormatter
    private let logDirectoryURL: URL
    private let logFileURL: URL

    private init() {
        self.iso8601 = ISO8601DateFormatter()
        let home = FileManager.default.homeDirectoryForCurrentUser
        self.logDirectoryURL = home.appendingPathComponent(".nornicdb/logs", isDirectory: true)
        self.logFileURL = logDirectoryURL.appendingPathComponent("file-indexer.log")
        ensureLogFileExists()
    }

    private func ensureLogFileExists() {
        queue.sync {
            do {
                try FileManager.default.createDirectory(at: logDirectoryURL, withIntermediateDirectories: true)
                if !FileManager.default.fileExists(atPath: logFileURL.path) {
                    FileManager.default.createFile(atPath: logFileURL.path, contents: nil)
                }
            } catch {
                print("❌ Failed to initialize file indexer logs: \(error)")
            }
        }
    }

    func log(_ level: Level, _ message: String, metadata: [String: String] = [:]) {
        queue.async {
            var line = "[\(self.iso8601.string(from: Date()))] [\(level.rawValue)] \(message)"
            if !metadata.isEmpty {
                let details = metadata
                    .sorted(by: { $0.key < $1.key })
                    .map { "\($0.key)=\($0.value)" }
                    .joined(separator: " ")
                line += " | \(details)"
            }
            line += "\n"

            guard let data = line.data(using: .utf8) else { return }
            do {
                let handle = try FileHandle(forWritingTo: self.logFileURL)
                defer { try? handle.close() }
                try handle.seekToEnd()
                try handle.write(contentsOf: data)
            } catch {
                print("❌ Failed to write file indexer log: \(error)")
            }
        }
    }

    func openLogFile() {
        ensureLogFileExists()
        NSWorkspace.shared.open(logFileURL)
    }
}

struct IndexedFolderFile: Identifiable {
    var id: String { path }
    let path: String
    let name: String
    let relativePath: String
    let inheritedTags: [String]
    let fileTags: [String]
    let effectiveTags: [String]
}

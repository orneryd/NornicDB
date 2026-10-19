import XCTest
@testable import NornicDB

final class RuntimeSettingsTests: XCTestCase {
    func testSavedAIFeaturesLoadAndEditsReachServerEnvironment() {
        let config = ConfigManager()
        config.loadRuntimeSettings(["EnvironmentVariables": [
            "NORNICDB_KMEANS_CLUSTERING_ENABLED": "true",
            "NORNICDB_MEMORY_DECAY_ENABLED": "false",
            "NORNICDB_AUTO_TLP_ENABLED": "true"]])
        XCTAssertTrue(config.kmeansEnabled)
        XCTAssertFalse(config.memoryDecayEnabled)
        XCTAssertTrue(config.autoTLPEnabled)
        XCTAssertTrue(config.runtimeEnvironment.isEmpty)
        config.kmeansEnabled = false
        config.memoryDecayEnabled = true
        config.autoTLPEnabled = false
        let environment = launchAgentEnvironmentVariables(config: config, homeDir: "/fixture")
        XCTAssertEqual(environment["NORNICDB_KMEANS_CLUSTERING_ENABLED"], "false")
        XCTAssertEqual(environment["NORNICDB_MEMORY_DECAY_ENABLED"], "true")
        XCTAssertEqual(environment["NORNICDB_AUTO_TLP_ENABLED"], "false")
    }

    func testExistingLaunchSettingsLoadIntoTheirEditableOwners() {
        let config = ConfigManager()
        config.loadProviderSettings(["embedding": ["provider": "local", "model": "bge-m3.gguf"]])
        config.loadRuntimeSettings([
            "ProgramArguments": ["/usr/local/bin/nornicdb", "serve", "--config", "/path with spaces/config.yaml", "$(touch /never-execute)"],
            "RunAtLoad": false,
            "EnvironmentVariables": ["NORNICDB_EMBEDDING_PROVIDER": "openai", "NORNICDB_EMBEDDING_MODEL": "text-embedding-3-small",
                "NORNICDB_EMBEDDING_DIMENSIONS": "1536", "NORNICDB_EMBED_WORKER_NUM_WORKERS": "10",
                "NORNICDB_EMBED_CHUNK_OVERLAP": "40", "NORNICDB_EMBEDDING_API_KEY": "fixture-key", "CUSTOM_SETTING": "value with spaces",
                "NORNICDB_UPGRADE_STORAGE": "true"]])
        XCTAssertEqual(config.embeddingProvider, "openai")
        XCTAssertEqual(config.embeddingModel, "text-embedding-3-small")
        XCTAssertEqual(config.embeddingAPIKey, "fixture-key")
        XCTAssertEqual(config.embeddingDimensions, 1536)
        XCTAssertEqual(config.embeddingWorkers, 10)
        XCTAssertEqual(config.embeddingChunkOverlap, 40)
        XCTAssertEqual(config.runtimeEnvironment, ["CUSTOM_SETTING": "value with spaces"])
        XCTAssertEqual(config.runtimeArguments, ["serve", "--config", "/path with spaces/config.yaml", "$(touch /never-execute)"])
        XCTAssertFalse(config.autoStartEnabled)
        config.embeddingModel = "new-model"
        config.embeddingChunkOverlap = 60
        let plist = launchAgentPropertyList(config: config, homeDir: "/fixture")
        let env = plist["EnvironmentVariables"] as? [String: String]
        XCTAssertEqual(env?["NORNICDB_EMBEDDING_MODEL"], "new-model")
        XCTAssertEqual(env?["NORNICDB_EMBED_CHUNK_OVERLAP"], "60")
        XCTAssertEqual(env?["NORNICDB_EMBED_WORKER_NUM_WORKERS"], "10")
        XCTAssertEqual(env?["CUSTOM_SETTING"], "value with spaces")
        XCTAssertNil(env?["NORNICDB_UPGRADE_STORAGE"])
        XCTAssertEqual(plist["ProgramArguments"] as? [String], ["/usr/local/bin/nornicdb"] + config.runtimeArguments)
        XCTAssertEqual(plist["RunAtLoad"] as? Bool, false)
        XCTAssertEqual(plist["KeepAlive"] as? Bool, false)
    }

    func testExternalServicesAndExplicitAdvancedOverridesReachStartup() {
        let config = ConfigManager()
        config.loadProviderSettings([
            "embedding": ["enabled": true, "provider": "voyage", "model": "voyage-code-3", "dimensions": 1024],
            "search_rerank": ["enabled": true, "provider": "http", "model": "remote-rerank", "api_url": "https://example.test/rerank", "api_key": "rerank-fixture"],
            "heimdall": ["enabled": false, "provider": "litellm", "model": "remote-chat", "api_url": "https://example.test/chat", "api_key": "chat-fixture"]])
        config.runtimeEnvironment = ["NORNICDB_EMBEDDING_MODEL": "explicit-override"]
        var env = launchAgentEnvironmentVariables(config: config, homeDir: "/fixture")
        XCTAssertEqual(env["NORNICDB_EMBEDDING_PROVIDER"], "voyage")
        XCTAssertEqual(env["NORNICDB_EMBEDDING_MODEL"], "explicit-override")
        XCTAssertEqual(env["NORNICDB_SEARCH_RERANK_PROVIDER"], "http")
        XCTAssertEqual(env["NORNICDB_SEARCH_RERANK_API_URL"], "https://example.test/rerank")
        XCTAssertEqual(env["NORNICDB_SEARCH_RERANK_API_KEY"], "rerank-fixture")
        XCTAssertEqual(env["NORNICDB_HEIMDALL_ENABLED"], "false")
        XCTAssertEqual(env["NORNICDB_HEIMDALL_PROVIDER"], "litellm")
        XCTAssertEqual(env["NORNICDB_HEIMDALL_MODEL"], "remote-chat")
        XCTAssertEqual(env["NORNICDB_HEIMDALL_API_KEY"], "chat-fixture")
        XCTAssertNil(env["NORNICDB_SEARCH_MIN_SIMILARITY"])
        config.upgradeStorageOnNextStart = true
        env = launchAgentEnvironmentVariables(config: config, homeDir: "/fixture")
        XCTAssertEqual(env["NORNICDB_UPGRADE_STORAGE"], "true")
    }

    func testMalformedConfigurationCannotBeOverwritten() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("config.yaml")
        for text in ["embedding: [broken", "- not-a-config-mapping"] {
            try text.write(to: path, atomically: true, encoding: .utf8)
            let config = ConfigManager(configPath: path.path)
            XCTAssertFalse(config.saveConfig())
            XCTAssertFalse(config.configurationError.isEmpty)
            XCTAssertEqual(try String(contentsOf: path), text)
        }
    }

    func testInvalidWorkersAndDimensionsAreRejectedBeforeWriting() {
        let config = ConfigManager(configPath: "/does/not/exist/config.yaml")
        config.embeddingWorkers = -1
        XCTAssertFalse(config.saveConfig())
        config.embeddingWorkers = 0
        config.embeddingDimensions = 0
        XCTAssertFalse(config.saveConfig())
    }
    func testReloadReadsNewPlistAndStopsAfterFailures() {
        var calls: [[String]] = []
        XCTAssertTrue(reloadServerLaunchAgent(path: "/fixture.plist") { arguments in
            calls.append(arguments)
            return arguments[0] == "unload" ? 1 : 0
        })
        XCTAssertEqual(calls, [["unload", "/fixture.plist"], ["load", "/fixture.plist"], ["start", "com.nornicdb.server"]])
        calls = []
        XCTAssertFalse(reloadServerLaunchAgent(path: "/fixture.plist") { arguments in
            calls.append(arguments)
            return nil
        })
        XCTAssertEqual(calls, [["unload", "/fixture.plist"], ["load", "/fixture.plist"]])
        XCTAssertFalse(reloadServerLaunchAgent(path: "/fixture.plist") { $0[0] == "start" ? 1 : 0 })
    }

    func testWrittenLaunchAgentPreservesArgumentsAndProtectsCredentials() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("server.plist")
        let config = ConfigManager()
        config.embeddingProvider = "openai"
        config.embeddingAPIKey = "fixture-key"
        config.runtimeEnvironment = ["CUSTOM": "a value with spaces"]
        config.runtimeArguments = ["serve", "--config", "/config path/file.yaml"]
        try writeLaunchAgentPlist(config: config, to: path.path)
        let saved = try XCTUnwrap(PropertyListSerialization.propertyList(from: Data(contentsOf: path), format: nil) as? [String: Any])
        let restored = ConfigManager()
        restored.loadRuntimeSettings(saved)
        XCTAssertEqual(restored.embeddingProvider, "openai")
        XCTAssertEqual(restored.embeddingAPIKey, "fixture-key")
        XCTAssertEqual(restored.runtimeArguments, config.runtimeArguments)
        XCTAssertEqual(restored.runtimeEnvironment["CUSTOM"], "a value with spaces")
        let attributes = try FileManager.default.attributesOfItem(atPath: path.path)
        XCTAssertEqual((attributes[.posixPermissions] as? NSNumber)?.intValue, 0o600)
    }

    func testArgumentsBeforeServeRetainTheirOrderOnRewrite() {
        let arguments = ["/usr/local/bin/nornicdb", "--config", "/configuration path/custom.yaml", "serve", "--no-auth"]
        let config = ConfigManager()
        config.loadRuntimeSettings(["ProgramArguments": arguments])
        XCTAssertEqual(launchAgentPropertyList(config: config, homeDir: "/fixture")["ProgramArguments"] as? [String], arguments)
    }

}

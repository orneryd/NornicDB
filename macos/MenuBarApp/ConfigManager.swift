import Foundation
import Combine
import Yams

// MARK: - Config Manager

class ConfigManager: ObservableObject {
    static let defaultEmbeddingModel = "bge-m3.gguf"
    static let defaultSearchRerankModel = "bge-reranker-v2-m3-Q4_K_M.gguf"
    static let defaultHeimdallModel = defaultQwenHeimdallFileName

    @Published var embeddingProvider: String = "local"
    @Published var embeddingURL: String = ""
    @Published var embeddingAPIKey: String = ""
    @Published var searchRerankProvider: String = "local"
    @Published var searchRerankURL: String = ""
    @Published var searchRerankAPIKey: String = ""
    @Published var heimdallProvider: String = "local"
    @Published var heimdallURL: String = ""
    @Published var heimdallAPIKey: String = ""
    @Published var embeddingWorkers: Int = 1
    @Published var runtimeEnvironment: [String: String] = [:]
    @Published var runtimeArguments: [String] = ["serve"]
    @Published var configurationError: String = ""

    func loadRuntimeSettings(_ plist: [String: Any]) {
        let environment = plist["EnvironmentVariables"] as? [String: String] ?? [:]
        runtimeEnvironment = environment
        runtimeEnvironment.removeValue(forKey: "NORNICDB_UPGRADE_STORAGE")
        // Recognized values have one editable owner in the ordinary settings.
        // Keep all other variables as explicit Advanced overrides.
        if let value = environment["NORNICDB_SERVER_BOLT_PORT"] { boltPortNumber = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SERVER_BOLT_PORT") }
        if let value = environment["NORNICDB_HTTP_PORT"] { httpPortNumber = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_HTTP_PORT") }
        if let value = environment["NORNICDB_SERVER_HOST"] { hostAddress = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SERVER_HOST") }
        if let value = environment["NORNICDB_EMBEDDING_PROVIDER"] { embeddingProvider = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_PROVIDER") }
        if let value = environment["NORNICDB_EMBEDDING_API_URL"] { embeddingURL = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_API_URL") }
        if let value = environment["NORNICDB_EMBEDDING_API_KEY"] { embeddingAPIKey = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_API_KEY") }
        if let value = environment["NORNICDB_EMBEDDING_MODEL"] { embeddingModel = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_MODEL") }
        if let value = environment["NORNICDB_SEARCH_RERANK_PROVIDER"] { searchRerankProvider = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SEARCH_RERANK_PROVIDER") }
        if let value = environment["NORNICDB_SEARCH_RERANK_MODEL"] { searchRerankModel = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SEARCH_RERANK_MODEL") }
        if let value = environment["NORNICDB_SEARCH_RERANK_API_URL"] { searchRerankURL = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SEARCH_RERANK_API_URL") }
        if let value = environment["NORNICDB_SEARCH_RERANK_API_KEY"] { searchRerankAPIKey = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_SEARCH_RERANK_API_KEY") }
        if let value = environment["NORNICDB_HEIMDALL_PROVIDER"] { heimdallProvider = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_HEIMDALL_PROVIDER") }
        if let value = environment["NORNICDB_HEIMDALL_MODEL"] { heimdallModel = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_HEIMDALL_MODEL") }
        if let value = environment["NORNICDB_HEIMDALL_API_URL"] { heimdallURL = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_HEIMDALL_API_URL") }
        if let value = environment["NORNICDB_HEIMDALL_API_KEY"] { heimdallAPIKey = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_HEIMDALL_API_KEY") }
        if let value = environment["NORNICDB_AUTH_JWT_SECRET"] { jwtSecret = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_AUTH_JWT_SECRET") }
        if let value = environment["NORNICDB_ENCRYPTION_PASSWORD"] { encryptionPassword = value; runtimeEnvironment.removeValue(forKey: "NORNICDB_ENCRYPTION_PASSWORD") }
        if let value = environment["NORNICDB_EMBEDDING_ENABLED"], let parsed = yamlBool(value) { embeddingsEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_ENABLED") }
        if let value = environment["NORNICDB_SEARCH_RERANK_ENABLED"], let parsed = yamlBool(value) { searchRerankEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_SEARCH_RERANK_ENABLED") }
        if let value = environment["NORNICDB_HEIMDALL_ENABLED"], let parsed = yamlBool(value) { heimdallEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_HEIMDALL_ENABLED") }
        if let value = environment["NORNICDB_KMEANS_CLUSTERING_ENABLED"], let parsed = yamlBool(value) { kmeansEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_KMEANS_CLUSTERING_ENABLED") }
        if let value = environment["NORNICDB_MEMORY_DECAY_ENABLED"], let parsed = yamlBool(value) { memoryDecayEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_MEMORY_DECAY_ENABLED") }
        if let value = environment["NORNICDB_AUTO_TLP_ENABLED"], let parsed = yamlBool(value) { autoTLPEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_AUTO_TLP_ENABLED") }
        if let value = environment["NORNICDB_ENCRYPTION_ENABLED"], let parsed = yamlBool(value) { encryptionEnabled = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_ENCRYPTION_ENABLED") }
        if let value = environment["NORNICDB_EMBEDDING_DIMENSIONS"], let parsed = Int(value) { embeddingDimensions = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBEDDING_DIMENSIONS") }
        if let value = environment["NORNICDB_EMBED_WORKER_NUM_WORKERS"], let parsed = Int(value) { embeddingWorkers = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBED_WORKER_NUM_WORKERS") }
        if let value = environment["NORNICDB_EMBED_CHUNK_SIZE"], let parsed = Int(value) { embeddingChunkSize = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBED_CHUNK_SIZE") }
        if let value = environment["NORNICDB_EMBED_CHUNK_OVERLAP"], let parsed = Int(value) { embeddingChunkOverlap = parsed; runtimeEnvironment.removeValue(forKey: "NORNICDB_EMBED_CHUNK_OVERLAP") }
        useAppleIntelligence = embeddingProvider == "openai" && embeddingModel == "apple-ml-embeddings" && embeddingURL == "http://localhost:\(ConfigManager.appleEmbeddingPort)"
        autoStartEnabled = plist["RunAtLoad"] as? Bool ?? autoStartEnabled
        let arguments = plist["ProgramArguments"] as? [String] ?? []
        runtimeArguments = arguments.isEmpty ? ["serve"] : Array(arguments.dropFirst())
    }
    @Published var embeddingsEnabled: Bool = false
    @Published var kmeansEnabled: Bool = false
    @Published var searchRerankEnabled: Bool = false
    @Published var memoryDecayEnabled: Bool = true
    @Published var autoTLPEnabled: Bool = false
    @Published var heimdallEnabled: Bool = false
    @Published var autoStartEnabled: Bool = true
    @Published var boltPortNumber: String = "7687"
    @Published var httpPortNumber: String = "7474"
    @Published var hostAddress: String = "localhost"

    // Apple Intelligence embeddings
    @Published var useAppleIntelligence: Bool = false
    static let appleEmbeddingPort: UInt16 = 11435
    static let appleEmbeddingDimensions: Int = 512
    static let appleEmbeddingChunkSize: Int = 512
    static let localEmbeddingChunkSize: Int = 8192
    static let defaultEmbeddingChunkOverlap: Int = 50
    /// Get or generate the Apple Intelligence embedding server API key (stored in Keychain)
    /// This is specific to the local Apple ML embedding server, separate from cloud provider API keys.
    static func getAppleIntelligenceAPIKey() -> String {
        // Try to load from Keychain
        if let existingKey = KeychainHelper.shared.getAppleIntelligenceAPIKey(), !existingKey.isEmpty {
            return existingKey
        }

        // Generate a new random key and save it
        let newKey = UUID().uuidString
        _ = KeychainHelper.shared.saveAppleIntelligenceAPIKey(newKey)
        print("🔐 Generated new Apple Intelligence API key")
        return newKey
    }

    /// Suggest embedding dimensions based on known model names
    static func suggestedDimensions(for modelName: String) -> Int {
        let name = modelName.lowercased()
        // BGE-M3 family: 1024 dimensions
        if name.contains("bge-m3") || name.contains("bge_m3") {
            return 1024
        }
        // BGE-Large: 1024 dimensions
        if name.contains("bge-large") || name.contains("bge_large") {
            return 1024
        }
        // BGE-Base: 768 dimensions
        if name.contains("bge-base") || name.contains("bge_base") {
            return 768
        }
        // BGE-Small: 384 dimensions
        if name.contains("bge-small") || name.contains("bge_small") {
            return 384
        }
        // MXBai embed large: 1024 dimensions
        if name.contains("mxbai") && name.contains("large") {
            return 1024
        }
        // Nomic embed: 768 dimensions
        if name.contains("nomic") {
            return 768
        }
        // E5 models
        if name.contains("e5-large") {
            return 1024
        }
        if name.contains("e5-base") {
            return 768
        }
        if name.contains("e5-small") {
            return 384
        }
        // OpenAI text-embedding-3 models
        if name.contains("text-embedding-3-large") {
            return 3072
        }
        if name.contains("text-embedding-3-small") {
            return 1536
        }
        if name.contains("text-embedding-ada") {
            return 1536
        }
        // Default to 1024 for unknown models (common for modern embedding models)
        return 1024
    }

    // Authentication settings
    @Published var adminUsername: String = "admin"
    @Published var adminPassword: String = "password"
    @Published var jwtSecret: String = ""

    // Encryption settings
    @Published var encryptionEnabled: Bool = false
    @Published var encryptionPassword: String = ""
    @Published var encryptionKeychainAccessDenied: Bool = false  // Track if user denied Keychain access

    @Published var embeddingModel: String = ConfigManager.defaultEmbeddingModel
    @Published var searchRerankModel: String = ConfigManager.defaultSearchRerankModel
    @Published var embeddingDimensions: Int = 1024  // Read from config, default 1024 for bge-m3
    @Published var embeddingChunkSize: Int = ConfigManager.localEmbeddingChunkSize
    @Published var embeddingChunkOverlap: Int = ConfigManager.defaultEmbeddingChunkOverlap
    @Published var heimdallModel: String = ConfigManager.defaultHeimdallModel
    @Published var availableModels: [String] = []

    // Config path matches server's FindConfigFile priority: ~/.nornicdb/config.yaml
    private let configPath: String

    init(configPath: String = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath) {
        self.configPath = configPath
    }

    // MARK: - YAML Parsing Helpers

    private typealias YAMLMap = [String: Any]

    private func loadYAMLRoot() -> YAMLMap {
        guard let content = try? String(contentsOfFile: configPath, encoding: .utf8) else {
            return [:]
        }
        do {
            guard let loaded = try Yams.load(yaml: content) else {
                return [:]
            }
            return loaded as? YAMLMap ?? [:]
        } catch {
            configurationError = "Cannot parse the existing YAML configuration. Fix it before saving."
            return [:]
        }
    }

    private func yamlSection(_ root: YAMLMap, _ key: String) -> YAMLMap {
        return root[key] as? YAMLMap ?? [:]
    }

    private func yamlString(_ value: Any?) -> String? {
        if let s = value as? String {
            let trimmed = s.trimmingCharacters(in: .whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        }
        if let n = value as? NSNumber {
            return n.stringValue
        }
        return nil
    }

    private func yamlBool(_ value: Any?) -> Bool? {
        if let b = value as? Bool {
            return b
        }
        if let n = value as? NSNumber {
            return n.boolValue
        }
        if let s = value as? String {
            let normalized = s.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if normalized == "true" || normalized == "1" || normalized == "yes" {
                return true
            }
            if normalized == "false" || normalized == "0" || normalized == "no" {
                return false
            }
        }
        return nil
    }

    private func yamlInt(_ value: Any?) -> Int? {
        if let i = value as? Int {
            return i
        }
        if let n = value as? NSNumber {
            return n.intValue
        }
        if let s = value as? String {
            return Int(s.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        return nil
    }

    // Normalize app-managed config sections to Go's expected YAML scalar types.
    // This prevents Yams from persisting values in a shape that breaks Go unmarshal
    // (e.g. quoted numeric ports as strings).
    private func normalizeConfigRootForGoSchema(_ root: YAMLMap) -> YAMLMap {
        var normalized = root

        // server
        var server = yamlSection(normalized, "server")
        let boltPort = max(1, yamlInt(server["bolt_port"]) ?? 7687)
        let httpPort = max(1, yamlInt(server["http_port"]) ?? 7474)
        server["bolt_port"] = boltPort
        server["http_port"] = httpPort
        server["port"] = boltPort // keep alias in sync for compatibility
        server["host"] = yamlString(server["host"]) ?? "localhost"
        normalized["server"] = server

        // embedding
        var embedding = yamlSection(normalized, "embedding")
        embedding["enabled"] = yamlBool(embedding["enabled"]) ?? false
        embedding["provider"] = yamlString(embedding["provider"]) ?? "local"
        embedding["model"] = yamlString(embedding["model"]) ?? ConfigManager.defaultEmbeddingModel
        embedding["url"] = yamlString(embedding["url"]) ?? ""
        embedding["dimensions"] = max(1, yamlInt(embedding["dimensions"]) ?? 1024)
        normalized["embedding"] = embedding

        // embedding_worker
        var embeddingWorker = yamlSection(normalized, "embedding_worker")
        embeddingWorker["chunk_size"] = max(1, yamlInt(embeddingWorker["chunk_size"]) ?? ConfigManager.localEmbeddingChunkSize)
        embeddingWorker["chunk_overlap"] = max(0, yamlInt(embeddingWorker["chunk_overlap"]) ?? ConfigManager.defaultEmbeddingChunkOverlap)
        normalized["embedding_worker"] = embeddingWorker

        // auth
        var auth = yamlSection(normalized, "auth")
        auth["username"] = yamlString(auth["username"]) ?? "admin"
        auth["password"] = yamlString(auth["password"]) ?? "password"
        auth["jwt_secret"] = yamlString(auth["jwt_secret"]) ?? "[stored-in-keychain]"
        normalized["auth"] = auth

        // database
        var database = yamlSection(normalized, "database")
        database["encryption_enabled"] = yamlBool(database["encryption_enabled"]) ?? false
        database["encryption_password"] = yamlString(database["encryption_password"]) ?? ""
        normalized["database"] = database

        // simple feature sections
        var kmeans = yamlSection(normalized, "kmeans")
        kmeans["enabled"] = yamlBool(kmeans["enabled"]) ?? false
        normalized["kmeans"] = kmeans

        var searchRerank = yamlSection(normalized, "search_rerank")
        searchRerank["enabled"] = yamlBool(searchRerank["enabled"]) ?? false
        searchRerank["provider"] = yamlString(searchRerank["provider"]) ?? "local"
        searchRerank["model"] = yamlString(searchRerank["model"]) ?? ConfigManager.defaultSearchRerankModel
        normalized["search_rerank"] = searchRerank

        var memory = yamlSection(normalized, "memory")
        memory["decay_enabled"] = yamlBool(memory["decay_enabled"]) ?? true
        normalized["memory"] = memory

        var autoTLP = yamlSection(normalized, "auto_tlp")
        autoTLP["enabled"] = yamlBool(autoTLP["enabled"]) ?? false
        normalized["auto_tlp"] = autoTLP

        var heimdall = yamlSection(normalized, "heimdall")
        heimdall["enabled"] = yamlBool(heimdall["enabled"]) ?? false
        heimdall["model"] = yamlString(heimdall["model"]) ?? ConfigManager.defaultHeimdallModel
        normalized["heimdall"] = heimdall

        // Keep storage.path string if present
        var storage = yamlSection(normalized, "storage")
        if let path = yamlString(storage["path"]) {
            storage["path"] = path
            normalized["storage"] = storage
        }

        return normalized
    }

    private let firstRunPath = NSString(string: "~/.nornicdb/.first_run").expandingTildeInPath
    private let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath

    func effectiveEmbeddingChunkSize() -> Int {
        if useAppleIntelligence {
            return ConfigManager.appleEmbeddingChunkSize
        }
        return max(1, embeddingChunkSize)
    }

    func effectiveEmbeddingChunkOverlap() -> Int {
        let maxAllowedOverlap = max(0, effectiveEmbeddingChunkSize() - 1)
        return min(max(0, embeddingChunkOverlap), maxAllowedOverlap)
    }
    let modelsPath = "/usr/local/var/nornicdb/models"
    let dataPath = "/usr/local/var/nornicdb/data"

    // One-shot flag: when true, the next plist write injects
    // NORNICDB_UPGRADE_STORAGE=true so the server boots with the upgrade
    // arm authorized. Cleared after the upgrade completes so we don't
    // re-authorize on every restart. The server itself decides whether
    // an upgrade is actually needed; if nothing is pending, the flag is
    // a no-op.
    @Published var upgradeStorageOnNextStart: Bool = false

    func isFirstRun() -> Bool {
        return FileManager.default.fileExists(atPath: firstRunPath)
    }

    func completeFirstRun() {
        try? FileManager.default.removeItem(atPath: firstRunPath)
    }

    func loadConfig() {
        defer {
            if let data = FileManager.default.contents(atPath: launchAgentPath),
               let plist = try? PropertyListSerialization.propertyList(from: data, format: nil) as? [String: Any] {
                loadRuntimeSettings(plist)
            }
        }
        // Scan available models first
        scanModels()

        // Reset Keychain access denied flags at startup (to allow retry)
        KeychainHelper.shared.resetAllAccessDenied()

        // FIRST: Load secrets from Keychain (preserved across reinstalls)
        // This ensures existing secrets are never lost
        if let keychainJWT = KeychainHelper.shared.getJWTSecret() {
            jwtSecret = keychainJWT
            print("🔐 Loaded JWT secret from Keychain (preserved across reinstall)")
        } else if KeychainHelper.shared.isJWTAccessDenied {
            print("⚠️ JWT Keychain access denied - will use config file value")
        }

        // Only try to load encryption password if we expect it to exist
        // (we'll do this after loading config to check if encryption was enabled)

        guard FileManager.default.fileExists(atPath: configPath) else {
            print("Could not read config file at: \(configPath)")
            // Even without config file, we may have Keychain secrets from previous install
            return
        }
        print("Loading config from: \(configPath)")
        let root = loadYAMLRoot()

        loadProviderSettings(root)

        // Load embedding worker section
        let embeddingWorkerSection = yamlSection(root, "embedding_worker")
        if let chunkSize = yamlInt(embeddingWorkerSection["chunk_size"]), chunkSize > 0 {
            embeddingChunkSize = chunkSize
            print("✅ Loaded embedding chunk size: \(chunkSize)")
        }
        if let chunkOverlap = yamlInt(embeddingWorkerSection["chunk_overlap"]), chunkOverlap >= 0 {
            embeddingChunkOverlap = chunkOverlap
            print("✅ Loaded embedding chunk overlap: \(chunkOverlap)")
        }
        // Apple ML provider has a fixed supported chunk size.
        if useAppleIntelligence {
            embeddingChunkSize = ConfigManager.appleEmbeddingChunkSize
        } else if embeddingChunkSize <= 0 {
            embeddingChunkSize = ConfigManager.localEmbeddingChunkSize
        }
        let maxAllowedOverlap = max(0, embeddingChunkSize - 1)
        embeddingChunkOverlap = min(max(0, embeddingChunkOverlap), maxAllowedOverlap)

        // Load kmeans section
        let kmeansSection = yamlSection(root, "kmeans")
        if let enabled = yamlBool(kmeansSection["enabled"]) {
            kmeansEnabled = enabled
            print("✅ Loaded kmeans enabled: \(kmeansEnabled)")
        }

        // Load memory section
        let memorySection = yamlSection(root, "memory")
        if let enabled = yamlBool(memorySection["decay_enabled"]) {
            memoryDecayEnabled = enabled
            print("✅ Loaded memory decay enabled: \(memoryDecayEnabled)")
        }

        // Load auto_tlp section
        let autoTLPSection = yamlSection(root, "auto_tlp")
        if let enabled = yamlBool(autoTLPSection["enabled"]) {
            autoTLPEnabled = enabled
            print("✅ Loaded auto_tlp enabled: \(autoTLPEnabled)")
        }

        // Load server section
        let serverSection = yamlSection(root, "server")
        if let port = yamlInt(serverSection["bolt_port"]), port > 0 {
            boltPortNumber = "\(port)"
            print("✅ Loaded bolt_port: \(port)")
        } else if let port = yamlString(serverSection["bolt_port"]) {
            boltPortNumber = port
            print("✅ Loaded bolt_port: \(port)")
        }
        if let port = yamlInt(serverSection["http_port"]), port > 0 {
            httpPortNumber = "\(port)"
            print("✅ Loaded http_port: \(port)")
        } else if let port = yamlString(serverSection["http_port"]) {
            httpPortNumber = port
            print("✅ Loaded http_port: \(port)")
        }
        if let host = yamlString(serverSection["host"]) {
            hostAddress = host
            print("✅ Loaded host: \(host)")
        }

        // Load auth section
        let authSection = yamlSection(root, "auth")
        if let username = yamlString(authSection["username"]) {
            adminUsername = username
            print("✅ Loaded username: \(username)")
        }
        if let password = yamlString(authSection["password"]) {
            adminPassword = password
            print("✅ Loaded password: [hidden]")
        }
        if jwtSecret.isEmpty, let jwt = yamlString(authSection["jwt_secret"]),
           !jwt.hasPrefix("[stored-in-keychain]"), !jwt.isEmpty {
            jwtSecret = jwt
            print("📄 Loaded JWT secret from config (migrating to Keychain)")
            _ = KeychainHelper.shared.saveJWTSecret(jwt)
        }

        // Load database/encryption settings
        let dbSection = yamlSection(root, "database")
        let configSaysEncryptionEnabled = yamlBool(dbSection["encryption_enabled"]) ?? false
        print("✅ Config says encryption enabled: \(configSaysEncryptionEnabled)")

        // NOW try to load encryption password from Keychain (only if encryption was/is enabled)
        if configSaysEncryptionEnabled {
            if let keychainEncryption = KeychainHelper.shared.getEncryptionPassword() {
                encryptionPassword = keychainEncryption
                encryptionEnabled = true
                encryptionKeychainAccessDenied = false
                print("🔐 Loaded encryption password from Keychain")
            } else if KeychainHelper.shared.isEncryptionAccessDenied {
                // User denied Keychain access - disable encryption and warn
                print("🚫 Keychain access denied for encryption - disabling encryption")
                encryptionEnabled = false
                encryptionKeychainAccessDenied = true
                encryptionPassword = ""
            } else {
                // No password in Keychain but encryption was enabled - try to load from config
                encryptionEnabled = configSaysEncryptionEnabled
            }
        }

        // Load encryption password from config ONLY if not already loaded from Keychain
        if encryptionPassword.isEmpty && !encryptionKeychainAccessDenied {
            if let password = yamlString(dbSection["encryption_password"]),
               !password.hasPrefix("[stored-in-keychain]"), !password.isEmpty {
                encryptionPassword = password
                encryptionEnabled = true
                print("📄 Loaded encryption password from config (migrating to Keychain)")
                // Migrate to Keychain
                if KeychainHelper.shared.saveEncryptionPassword(password) {
                    print("✅ Migrated encryption password to Keychain")
                } else if KeychainHelper.shared.isEncryptionAccessDenied {
                    print("🚫 Keychain access denied during migration - disabling encryption")
                    encryptionEnabled = false
                    encryptionKeychainAccessDenied = true
                    encryptionPassword = ""
                }
            }
        }

        // If Keychain access was denied for encryption, make sure it's disabled
        if encryptionKeychainAccessDenied {
            encryptionEnabled = false
            encryptionPassword = ""
        }

    }

    func scanModels() {
        // Scan models directory for .gguf files
        guard let files = try? FileManager.default.contentsOfDirectory(atPath: modelsPath) else {
            availableModels = []
            return
        }

        availableModels = files.filter {
            $0.hasSuffix(".gguf") && isValidGGUFFile(atPath: "\(modelsPath)/\($0)")
        }.sorted()
    }

    func loadProviderSettings(_ root: [String: Any]) {
        let embedding = yamlSection(root, "embedding")
        embeddingsEnabled = yamlBool(embedding["enabled"]) ?? embeddingsEnabled
        embeddingProvider = yamlString(embedding["provider"]) ?? "local"
        embeddingURL = yamlString(embedding["url"]) ?? ""
        embeddingAPIKey = yamlString(embedding["api_key"]) ?? ""
        if let model = yamlString(embedding["model"]) { embeddingModel = model }
        let searchRerank = yamlSection(root, "search_rerank")
        searchRerankEnabled = yamlBool(searchRerank["enabled"]) ?? searchRerankEnabled
        searchRerankProvider = yamlString(searchRerank["provider"]) ?? "local"
        searchRerankURL = yamlString(searchRerank["api_url"]) ?? ""
        searchRerankAPIKey = yamlString(searchRerank["api_key"]) ?? ""
        if let model = yamlString(searchRerank["model"]) { searchRerankModel = model }
        let heimdall = yamlSection(root, "heimdall")
        heimdallEnabled = yamlBool(heimdall["enabled"]) ?? heimdallEnabled
        heimdallProvider = yamlString(heimdall["provider"]) ?? "local"
        heimdallURL = yamlString(heimdall["api_url"]) ?? ""
        heimdallAPIKey = yamlString(heimdall["api_key"]) ?? ""
        if let model = yamlString(heimdall["model"]) { heimdallModel = model }
        useAppleIntelligence = embeddingProvider == "openai" && embeddingModel == "apple-ml-embeddings" && embeddingURL == "http://localhost:\(ConfigManager.appleEmbeddingPort)"
        embeddingDimensions = yamlInt(yamlSection(root, "embedding")["dimensions"]) ?? embeddingDimensions
        embeddingWorkers = yamlInt(yamlSection(root, "embedding_worker")["workers"]) ?? embeddingWorkers
    }

    func writeProviderSettings(_ root: inout [String: Any]) {
        var embedding = yamlSection(root, "embedding")
        embedding["enabled"] = embeddingsEnabled
        embedding["provider"] = embeddingProvider
        embedding["model"] = embeddingModel
        if !embeddingURL.isEmpty || embedding["url"] != nil { embedding["url"] = embeddingURL }
        if !embeddingAPIKey.isEmpty || embedding["api_key"] != nil { embedding["api_key"] = embeddingAPIKey }
        embedding["dimensions"] = embeddingDimensions
        if useAppleIntelligence {
            embedding["provider"] = "openai"
            embedding["model"] = "apple-ml-embeddings"
            embedding["url"] = "http://localhost:\(ConfigManager.appleEmbeddingPort)"
            embedding["dimensions"] = ConfigManager.appleEmbeddingDimensions
        }
        root["embedding"] = embedding
        var searchRerank = yamlSection(root, "search_rerank")
        searchRerank["enabled"] = searchRerankEnabled
        searchRerank["provider"] = searchRerankProvider
        searchRerank["model"] = searchRerankModel
        if !searchRerankURL.isEmpty || searchRerank["api_url"] != nil { searchRerank["api_url"] = searchRerankURL }
        if !searchRerankAPIKey.isEmpty || searchRerank["api_key"] != nil { searchRerank["api_key"] = searchRerankAPIKey }
        root["search_rerank"] = searchRerank
        var heimdall = yamlSection(root, "heimdall")
        heimdall["enabled"] = heimdallEnabled
        heimdall["provider"] = heimdallProvider
        heimdall["model"] = heimdallModel
        if !heimdallURL.isEmpty || heimdall["api_url"] != nil { heimdall["api_url"] = heimdallURL }
        if !heimdallAPIKey.isEmpty || heimdall["api_key"] != nil { heimdall["api_key"] = heimdallAPIKey }
        root["heimdall"] = heimdall
    }

    func saveConfig() -> Bool {
        configurationError = ""
        guard embeddingWorkers >= 0 && embeddingDimensions > 0 else {
            configurationError = "Embedding workers must be zero or greater and dimensions must be positive."
            return false
        }
        if FileManager.default.fileExists(atPath: configPath) {
            do {
                let text = try String(contentsOfFile: configPath, encoding: .utf8)
                guard try Yams.load(yaml: text) is [String: Any] else {
                    configurationError = "Existing configuration must be a YAML mapping. Fix it before saving."
                    return false
                }
            } catch {
                configurationError = "Cannot read the existing configuration. Fix its YAML or permissions before saving."
                return false
            }
        }
        var root = loadYAMLRoot()
        writeProviderSettings(&root)

        var authSection = yamlSection(root, "auth")
        var databaseSection = yamlSection(root, "database")
        var embeddingWorkerSection = yamlSection(root, "embedding_worker")
        var kmeansSection = yamlSection(root, "kmeans")
        var memorySection = yamlSection(root, "memory")
        var autoTLPSection = yamlSection(root, "auto_tlp")
        var serverSection = yamlSection(root, "server")

        kmeansSection["enabled"] = kmeansEnabled
        memorySection["decay_enabled"] = memoryDecayEnabled
        autoTLPSection["enabled"] = autoTLPEnabled


        embeddingWorkerSection["chunk_size"] = effectiveEmbeddingChunkSize()
        embeddingWorkerSection["chunk_overlap"] = effectiveEmbeddingChunkOverlap()

        let normalizedBoltPort = max(1, yamlInt(boltPortNumber) ?? 7687)
        let normalizedHTTPPort = max(1, yamlInt(httpPortNumber) ?? 7474)
        serverSection["bolt_port"] = normalizedBoltPort
        serverSection["http_port"] = normalizedHTTPPort
        serverSection["port"] = normalizedBoltPort
        serverSection["host"] = hostAddress

        authSection["username"] = adminUsername
        authSection["password"] = adminPassword

        // Auto-generate JWT secret only if empty, then save to Keychain
        if jwtSecret.isEmpty {
            jwtSecret = ConfigManager.generateRandomSecret()
            print("🔑 Auto-generated NEW JWT secret (was empty)")
        } else {
            print("✅ Preserving existing JWT secret")
        }
        // Save JWT secret to Keychain (secure storage)
        if KeychainHelper.shared.saveJWTSecret(jwtSecret) {
            print("🔐 JWT secret saved to Keychain")
            // Write placeholder to config file indicating it's in Keychain
            authSection["jwt_secret"] = "[stored-in-keychain]"
        } else {
            // Fallback: save to config file if Keychain fails
            print("⚠️ Keychain save failed, storing JWT in config file")
            authSection["jwt_secret"] = jwtSecret
        }

        // Update encryption settings
        // If Keychain access was denied, force encryption to be disabled
        let effectiveEncryptionEnabled = encryptionEnabled && !encryptionKeychainAccessDenied
        databaseSection["encryption_enabled"] = effectiveEncryptionEnabled
        if effectiveEncryptionEnabled {
            // Auto-generate encryption password if empty
            if encryptionPassword.isEmpty {
                encryptionPassword = ConfigManager.generateRandomSecret()
                print("🔑 Auto-generated encryption password")
            }
            // Save encryption password to Keychain (secure storage)
            if KeychainHelper.shared.saveEncryptionPassword(encryptionPassword) {
                print("🔐 Encryption password saved to Keychain")
                // Write placeholder to config file indicating it's in Keychain
                databaseSection["encryption_password"] = "[stored-in-keychain]"
            } else if KeychainHelper.shared.isEncryptionAccessDenied {
                // User denied Keychain access - disable encryption for security
                print("🚫 Keychain access denied - disabling encryption")
                encryptionEnabled = false
                encryptionKeychainAccessDenied = true
                databaseSection["encryption_enabled"] = false
                databaseSection["encryption_password"] = ""
            } else {
                // Fallback: save to config file if Keychain fails for other reasons
                print("⚠️ Keychain save failed, storing encryption password in config file")
                databaseSection["encryption_password"] = encryptionPassword
            }
        } else {
            databaseSection["encryption_password"] = ""
            // Clear from Keychain if encryption is disabled
            _ = KeychainHelper.shared.deleteEncryptionPassword()
        }

        root["auth"] = authSection
        root["database"] = databaseSection
        root["embedding_worker"] = embeddingWorkerSection
        root["kmeans"] = kmeansSection
        root["memory"] = memorySection
        root["auto_tlp"] = autoTLPSection
        root["server"] = serverSection
        // Write back
        do {
            let normalizedRoot = normalizeConfigRootForGoSchema(root)
            let content = try Yams.dump(object: normalizedRoot)
            try content.write(toFile: configPath, atomically: true, encoding: .utf8)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: configPath)


            return true
        } catch {
            print("Failed to write config: \(error)")
            return false
        }
    }

    static func generateRandomSecret() -> String {
        let characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return String((0..<32).map { _ in characters.randomElement()! })
    }
}

func launchAgentEnvironmentVariables(config: ConfigManager, homeDir: String) -> [String: String] {
    let jwtSecretEnv = config.jwtSecret
    let encryptionPasswordEnv = config.encryptionEnabled ? config.encryptionPassword : ""

    var envVars: [String: String] = [
        "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": homeDir,
        "NORNICDB_SERVER_BOLT_PORT": config.boltPortNumber,
        "NORNICDB_HTTP_PORT": config.httpPortNumber,
        "NORNICDB_SERVER_HOST": config.hostAddress,
        "NORNICDB_EMBEDDING_ENABLED": config.embeddingsEnabled ? "true" : "false",
        "NORNICDB_EMBEDDING_PROVIDER": config.useAppleIntelligence ? "openai" : config.embeddingProvider,
        "NORNICDB_EMBEDDING_API_URL": config.useAppleIntelligence ? "http://localhost:\(ConfigManager.appleEmbeddingPort)" : config.embeddingURL,
        "NORNICDB_EMBEDDING_MODEL": config.useAppleIntelligence ? "apple-ml-embeddings" : config.embeddingModel,
        "NORNICDB_EMBEDDING_DIMENSIONS": config.useAppleIntelligence ? "\(ConfigManager.appleEmbeddingDimensions)" : "\(config.embeddingDimensions)",
        "NORNICDB_EMBEDDING_API_KEY": config.useAppleIntelligence ? ConfigManager.getAppleIntelligenceAPIKey() : config.embeddingAPIKey,
        "NORNICDB_EMBED_CHUNK_SIZE": "\(config.effectiveEmbeddingChunkSize())",
        "NORNICDB_EMBED_CHUNK_OVERLAP": "\(config.effectiveEmbeddingChunkOverlap())",
        "NORNICDB_KMEANS_CLUSTERING_ENABLED": config.kmeansEnabled ? "true" : "false",
        "NORNICDB_SEARCH_RERANK_ENABLED": config.searchRerankEnabled ? "true" : "false",
        "NORNICDB_SEARCH_RERANK_PROVIDER": config.searchRerankProvider,
        "NORNICDB_SEARCH_RERANK_API_URL": config.searchRerankURL,
        "NORNICDB_SEARCH_RERANK_API_KEY": config.searchRerankAPIKey,
        "NORNICDB_HEIMDALL_ENABLED": config.heimdallEnabled ? "true" : "false",
        "NORNICDB_HEIMDALL_PROVIDER": config.heimdallProvider,
        "NORNICDB_HEIMDALL_MODEL": config.heimdallModel,
        "NORNICDB_HEIMDALL_API_URL": config.heimdallURL,
        "NORNICDB_HEIMDALL_API_KEY": config.heimdallAPIKey,
        "NORNICDB_EMBED_WORKER_NUM_WORKERS": String(config.embeddingWorkers),
        "NORNICDB_SEARCH_RERANK_MODEL": config.searchRerankModel,
        "NORNICDB_MEMORY_DECAY_ENABLED": config.memoryDecayEnabled ? "true" : "false",
        "NORNICDB_AUTO_TLP_ENABLED": config.autoTLPEnabled ? "true" : "false",
        "NORNICDB_MODELS_DIR": config.modelsPath,
        "NORNICDB_PLUGINS_DIR": "/usr/local/share/nornicdb/plugins",
        "NORNICDB_HEIMDALL_PLUGINS_DIR": "/usr/local/share/nornicdb/plugins/heimdall",
    ]

    if !jwtSecretEnv.isEmpty {
        envVars["NORNICDB_AUTH_JWT_SECRET"] = jwtSecretEnv
    }

    if config.encryptionEnabled && !encryptionPasswordEnv.isEmpty {
        envVars["NORNICDB_ENCRYPTION_ENABLED"] = "true"
        envVars["NORNICDB_ENCRYPTION_PASSWORD"] = encryptionPasswordEnv
    }

    if config.upgradeStorageOnNextStart {
        envVars["NORNICDB_UPGRADE_STORAGE"] = "true"
    }

    envVars.merge(config.runtimeEnvironment) { _, override in override }
    // Storage upgrade is one-shot, never retained as an advanced override.
    envVars.removeValue(forKey: "NORNICDB_UPGRADE_STORAGE")
    if config.upgradeStorageOnNextStart { envVars["NORNICDB_UPGRADE_STORAGE"] = "true" }
    return envVars
}

func launchAgentPropertyList(config: ConfigManager, homeDir: String) -> [String: Any] {
    return [
        "Label": "com.nornicdb.server",
        "ProgramArguments": ["/usr/local/bin/nornicdb"] + config.runtimeArguments,
        "WorkingDirectory": "/usr/local/var/nornicdb",
        "RunAtLoad": config.autoStartEnabled,
        "KeepAlive": config.autoStartEnabled ? ["SuccessfulExit": false, "Crashed": true] as Any : false,
        "ThrottleInterval": 30,
        "StandardOutPath": "/usr/local/var/log/nornicdb/stdout.log",
        "StandardErrorPath": "/usr/local/var/log/nornicdb/stderr.log",
        "EnvironmentVariables": launchAgentEnvironmentVariables(config: config, homeDir: homeDir),
        "ProcessType": "Interactive",
        "Nice": 0,
    ]
}

func writeLaunchAgentPlist(config: ConfigManager, to launchAgentPath: String) throws {
    let homeDir = NSString(string: "~").expandingTildeInPath
    let plist = launchAgentPropertyList(config: config, homeDir: homeDir)
    let plistData = try PropertyListSerialization.data(fromPropertyList: plist, format: .xml, options: 0)
    try plistData.write(to: URL(fileURLWithPath: launchAgentPath), options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: launchAgentPath)
}


/// Reload so launchd reads changed environment/arguments before starting.
@discardableResult
func reloadServerLaunchAgent(path: String,
    execute: ([String]) -> Int32? = { executeLaunchctl(arguments: $0) }) -> Bool {
    _ = execute(["unload", path]) // An unloaded service is valid on first install.
    guard execute(["load", path]) == 0 else { return false }
    return execute(["start", "com.nornicdb.server"]) == 0
}

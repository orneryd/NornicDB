// FileIndexerWindow.swift
// File Indexing UI with folder selection, real-time stats, and search


import SwiftUI
import AppKit

// MARK: - Indexed Folder Model

struct IndexedFolder: Identifiable, Codable {
    let id: String
    var path: String
    var fileCount: Int
    var chunkCount: Int
    var embeddingCount: Int
    var status: FolderStatus
    var lastSync: Date
    var isIndexing: Bool
    var error: String?
    var tags: [String]  // User-defined tags for this folder
    
    /// Folder name derived from path
    var folderName: String {
        return (path as NSString).lastPathComponent
    }
    
    /// Sanitized folder identifier for use as a tag (alphanumeric + underscore)
    var folderTag: String {
        let name = folderName.replacingOccurrences(of: " ", with: "_")
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "_"))
        return String(name.unicodeScalars.filter { allowed.contains($0) })
    }
    
    enum FolderStatus: String, Codable {
        case active
        case inactive
        case stopped
        case error
        case indexing
        
        var icon: String {
            switch self {
            case .active: return "✅"
            case .inactive: return "⏸️"
            case .stopped: return "🛑"
            case .error: return "❌"
            case .indexing: return "⏳"
            }
        }
        
        var label: String {
            switch self {
            case .active: return "Active"
            case .inactive: return "Inactive"
            case .stopped: return "Stopped"
            case .error: return "Error"
            case .indexing: return "Indexing..."
            }
        }
        
        var color: Color {
            switch self {
            case .active: return .green
            case .inactive: return .gray
            case .stopped: return .red
            case .error: return .red
            case .indexing: return .blue
            }
        }
    }
}

// MARK: - Index Stats Model

struct IndexStats: Codable {
    var totalFolders: Int
    var totalFiles: Int
    var totalChunks: Int
    var totalEmbeddings: Int
    var byExtension: [String: Int]
}

// MARK: - Indexing Progress

struct IndexingProgress: Identifiable {
    let id: String
    var path: String
    var totalFiles: Int
    var indexed: Int
    var skipped: Int
    var errored: Int
    var currentFile: String?
    var status: ProgressStatus
    var startTime: Date?
    var endTime: Date?
    
    enum ProgressStatus: String {
        case queued
        case indexing
        case completed
        case cancelled
        case error
    }
    
    var percentComplete: Double {
        guard totalFiles > 0 else { return 0 }
        return Double(indexed) / Double(totalFiles) * 100
    }
}

// MARK: - Server Connection Status

enum ServerConnectionStatus {
    case unknown
    case connected
    case disconnected
    case authRequired
    case authenticated
}

// MARK: - File Watch Manager

class FileWatchManager: ObservableObject {
    
    @Published var watchedFolders: [IndexedFolder] = []
    @Published var stats: IndexStats = IndexStats(totalFolders: 0, totalFiles: 0, totalChunks: 0, totalEmbeddings: 0, byExtension: [:])
    @Published var progressMap: [String: IndexingProgress] = [:]
    @Published var isLoading: Bool = false
    @Published var error: String?
    @Published var connectionStatus: ServerConnectionStatus = .unknown
    @Published var authRequired: Bool = false
    @Published var isAuthenticated: Bool = false
    @Published var activeDatabaseName: String = "nornic"
    @Published var folderFiles: [String: [IndexedFolderFile]] = [:]
    @Published var loadingFilesForFolders: Set<String> = []
    
    /// Delay between indexing each file (in milliseconds) to reduce system load
    @Published var indexingThrottleMs: Int = 50 {
        didSet {
            UserDefaults.standard.set(indexingThrottleMs, forKey: "indexingThrottleMs")
        }
    }
    
    private var fileSystemWatchers: [String: DispatchSourceFileSystemObject] = [:]
    private let indexer = FileIndexer()
    private let indexerService = FileIndexerService.shared
    private let config: ConfigManager
    private let logger = FileIndexerLogger.shared
    
    // Server configuration (from ConfigManager)
    private var serverHost: String { config.hostAddress }
    private var serverPort: String { config.httpPortNumber }
    private var serverBaseURL: String { "http://\(serverHost):\(serverPort)" }
    private var txCommitEndpoint: String { "/db/\(activeDatabaseName)/tx/commit" }
    
    // Persistence path
    private var configPath: String {
        let home = FileManager.default.homeDirectoryForCurrentUser.path
        return "\(home)/.nornicdb/indexed_folders.json"
    }
    
    init(config: ConfigManager) {
        self.config = config
        
        // Load saved throttle setting
        let savedThrottle = UserDefaults.standard.integer(forKey: "indexingThrottleMs")
        self.indexingThrottleMs = savedThrottle > 0 ? savedThrottle : 50
        
        loadSavedFolders()
        setupFileWatching()
        logger.log(.info, "FileWatchManager initialized", metadata: [
            "serverBaseURL": serverBaseURL
        ])
        
        // Check server connection on init
        Task {
            await checkServerConnection()
        }
    }
    
    // MARK: - Server Connection
    
    /// Check if server is running and if auth is required
    @MainActor
    func checkServerConnection() async {
        connectionStatus = .unknown
        
        // First check if server is reachable
        guard let healthURL = URL(string: "\(serverBaseURL)/health") else {
            connectionStatus = .disconnected
            return
        }
        
        do {
            let (_, response) = try await URLSession.shared.data(from: healthURL)
            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                connectionStatus = .disconnected
                return
            }
            
            connectionStatus = .connected
            await resolveDefaultDatabaseName()
            
            // Now check if auth is required
            await checkAuthStatus()
            
        } catch {
            connectionStatus = .disconnected
            self.error = "Cannot connect to NornicDB server at \(serverBaseURL)"
        }
    }
    
    /// Check if authentication is required and if we have a valid token
    @MainActor
    private func checkAuthStatus() async {
        // Use /auth/config endpoint which returns securityEnabled
        guard let authConfigURL = URL(string: "\(serverBaseURL)/auth/config") else {
            return
        }
        
        do {
            let (data, response) = try await URLSession.shared.data(from: authConfigURL)
            
            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                // If /auth/config doesn't exist, check /auth/me with token
                await checkAuthWithToken()
                return
            }
            
            // Parse auth config response
            if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                let securityEnabled = json["securityEnabled"] as? Bool ?? false
                authRequired = securityEnabled
                
                if !securityEnabled {
                    // Auth disabled - no authentication needed
                    isAuthenticated = true
                    connectionStatus = .connected
                } else {
                    // Auth enabled - check if we have a valid token
                    await checkAuthWithToken()
                }
            }
        } catch {
            // Assume auth required if we can't reach config
            await checkAuthWithToken()
        }
    }
    
    /// Check if current token is valid by calling /auth/me
    @MainActor
    private func checkAuthWithToken() async {
        // Access Keychain off the main actor to avoid blocking UI during authorization dialog
        let token: String? = await Task.detached(priority: .userInitiated) {
            return KeychainHelper.shared.getAPIToken()
        }.value
        restoreFileIndexerWindowFocus()
        
        guard let token = token,
              let authMeURL = URL(string: "\(serverBaseURL)/auth/me") else {
            // No token - auth required
            authRequired = true
            isAuthenticated = false
            connectionStatus = .authRequired
            return
        }
        
        var request = URLRequest(url: authMeURL)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        
        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                isAuthenticated = false
                connectionStatus = .authRequired
                return
            }
            
            if httpResponse.statusCode == 200 {
                isAuthenticated = true
                connectionStatus = .authenticated
            } else {
                // Token invalid or expired
                isAuthenticated = false
                connectionStatus = .authRequired
            }
        } catch {
            isAuthenticated = false
            connectionStatus = .authRequired
        }
    }
    
    /// Resolve the server default database so writes go to the same DB shown in UI.
    @MainActor
    private func resolveDefaultDatabaseName() async {
        guard let discoveryURL = URL(string: "\(serverBaseURL)/") else {
            return
        }
        
        do {
            let (data, response) = try await URLSession.shared.data(from: discoveryURL)
            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                return
            }
            
            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let dbName = json["default_database"] as? String,
                  !dbName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                return
            }
            
            activeDatabaseName = dbName
        } catch {
            // Keep fallback default.
        }
    }
    
    /// Authenticate with the server and store token
    @MainActor
    func authenticate(username: String, password: String) async -> Bool {
        let authURL = "\(serverBaseURL)/auth/token"
        print("🔐 Attempting authentication to: \(authURL)")
        
        guard let tokenURL = URL(string: authURL) else {
            print("❌ Invalid auth URL: \(authURL)")
            self.error = "Invalid authentication URL"
            return false
        }
        
        var request = URLRequest(url: tokenURL)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = ["username": username, "password": password]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)
        
        print("🔐 Sending auth request with username: \(username)")
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                print("❌ Invalid HTTP response")
                self.error = "Invalid server response"
                return false
            }
            
            print("🔐 Auth response status: \(httpResponse.statusCode)")
            
            if httpResponse.statusCode == 200 {
                if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let token = json["access_token"] as? String {
                    print("✅ Received auth token, saving to keychain")
                    // Save token off main thread to avoid blocking UI during Keychain access
                    let saved = await Task.detached(priority: .userInitiated) {
                        // Use updateAPIToken to ensure cache is updated even if token exists
                        return KeychainHelper.shared.updateAPIToken(token)
                    }.value
                    restoreFileIndexerWindowFocus()
                    print("✅ Token saved to keychain: \(saved)")
                    isAuthenticated = true
                    connectionStatus = .authenticated
                    return true
                } else {
                    print("❌ Failed to parse token from response")
                    if let responseString = String(data: data, encoding: .utf8) {
                        print("Response body: \(responseString)")
                    }
                    self.error = "Invalid token response from server"
                }
            } else {
                if let responseString = String(data: data, encoding: .utf8) {
                    print("❌ Auth failed with response: \(responseString)")
                    self.error = "Authentication failed: \(responseString)"
                } else {
                    self.error = "Authentication failed with status \(httpResponse.statusCode)"
                }
            }
        } catch {
            print("❌ Auth network error: \(error.localizedDescription)")
            self.error = "Authentication failed: \(error.localizedDescription)"
        }
        
        return false
    }

    @MainActor
    private func restoreFileIndexerWindowFocus() {
        NSApp.activate(ignoringOtherApps: true)

        // Re-focus the File Indexer window in case macOS leaves it behind after Keychain auth dialogs.
        for window in NSApp.windows where window.title == "NornicDB Code Intelligence" {
            window.makeKeyAndOrderFront(nil)
            window.orderFrontRegardless()
            break
        }
    }
    
    /// Get authorization header for API requests (uses cached token to avoid Keychain prompts)
    private func getAuthHeader() -> String? {
        // This uses the cached token from KeychainHelper to avoid triggering Keychain prompts
        if let token = KeychainHelper.shared.getAPIToken() {
            return "Bearer \(token)"
        }
        return nil
    }
    
    /// Get authorization header asynchronously (safe for initial access)
    private func getAuthHeaderAsync() async -> String? {
        let token: String? = await Task.detached(priority: .userInitiated) {
            return KeychainHelper.shared.getAPIToken()
        }.value
        if let token = token {
            return "Bearer \(token)"
        }
        return nil
    }
    
    /// Make an authenticated API request
    private func makeAuthenticatedRequest(
        to endpoint: String,
        method: String = "GET",
        body: Data? = nil
    ) async throws -> (Data, HTTPURLResponse) {
        guard let url = URL(string: "\(serverBaseURL)\(endpoint)") else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid URL"])
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        if let auth = getAuthHeader() {
            request.setValue(auth, forHTTPHeaderField: "Authorization")
        }
        
        if let body = body {
            request.httpBody = body
        }
        
        let (data, response) = try await URLSession.shared.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response"])
        }
        
        // Handle auth errors
        if httpResponse.statusCode == 401 {
            await MainActor.run {
                self.isAuthenticated = false
                self.connectionStatus = .authRequired
            }
            throw NSError(domain: "FileWatchManager", code: 401, userInfo: [NSLocalizedDescriptionKey: "Authentication required"])
        }
        
        return (data, httpResponse)
    }
    
    @MainActor
    private func canWriteToServer() async -> Bool {
        if connectionStatus == .connected || connectionStatus == .authenticated {
            return true
        }
        
        // Try one explicit refresh before giving up.
        await checkServerConnection()
        return connectionStatus == .connected || connectionStatus == .authenticated
    }
    
    /// Execute a Cypher statement and surface Neo4j-style errors even when HTTP status is 200.
    private func executeCypher(_ statement: String, parameters: [String: Any]) async throws -> [String: Any] {
        let body: [String: Any] = [
            "statements": [
                ["statement": statement, "parameters": parameters]
            ]
        ]
        let bodyData = try JSONSerialization.data(withJSONObject: body)
        let (data, response) = try await makeAuthenticatedRequest(to: txCommitEndpoint, method: "POST", body: bodyData)
        
        guard response.statusCode == 200 || response.statusCode == 202 else {
            logger.log(.error, "Cypher HTTP request failed", metadata: [
                "status": "\(response.statusCode)",
                "database": activeDatabaseName,
                "endpoint": txCommitEndpoint
            ])
            throw NSError(
                domain: "FileWatchManager",
                code: response.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "Cypher request failed with status \(response.statusCode) on database '\(activeDatabaseName)'"]
            )
        }
        
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            logger.log(.error, "Cypher response parsing failed", metadata: [
                "database": activeDatabaseName,
                "endpoint": txCommitEndpoint
            ])
            throw NSError(
                domain: "FileWatchManager",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Invalid Cypher response from server"]
            )
        }
        
        if let errors = json["errors"] as? [[String: Any]], !errors.isEmpty {
            let first = errors[0]
            let code = first["code"] as? String ?? "Unknown"
            let message = first["message"] as? String ?? "Cypher execution failed"
            logger.log(.error, "Cypher returned Neo4j error", metadata: [
                "database": activeDatabaseName,
                "code": code,
                "message": message
            ])
            throw NSError(
                domain: "FileWatchManager",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "\(code): \(message)"]
            )
        }
        
        return json
    }
    
    // MARK: - Persistence
    
    func loadSavedFolders() {
        guard FileManager.default.fileExists(atPath: configPath),
              let data = try? Data(contentsOf: URL(fileURLWithPath: configPath)),
              let folders = try? JSONDecoder().decode([IndexedFolder].self, from: data) else {
            return
        }
        
        DispatchQueue.main.async {
            self.watchedFolders = folders
            self.updateStats()
        }
        
        // Resume watching for active folders
        for folder in folders where folder.status == .active {
            startWatching(folder.path)
        }
    }
    
    func saveFolders() {
        // Ensure directory exists
        let dir = (configPath as NSString).deletingLastPathComponent
        try? FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
        
        if let data = try? JSONEncoder().encode(watchedFolders) {
            try? data.write(to: URL(fileURLWithPath: configPath))
        }
    }
    
    // MARK: - Folder Management
    
    func addFolder(_ path: String) {
        // Check if already added
        guard !watchedFolders.contains(where: { $0.path == path }) else {
            error = "Folder is already being indexed"
            return
        }
        
        let folder = IndexedFolder(
            id: UUID().uuidString,
            path: path,
            fileCount: 0,
            chunkCount: 0,
            embeddingCount: 0,
            status: .indexing,
            lastSync: Date(),
            isIndexing: true,
            error: nil,
            tags: []  // User can add tags later
        )
        
        DispatchQueue.main.async {
            self.watchedFolders.append(folder)
            self.saveFolders()
        }
        
        // Start indexing
        startIndexing(path)
    }
    
    func removeFolder(_ folder: IndexedFolder) {
        Task {
            // Stop watching while deletion is in progress to avoid re-index races.
            stopWatching(folder.path)

            do {
                try await deleteNodesForFolder(folder.path)
            } catch {
                print("⚠️  Failed to delete nodes for folder \(folder.path): \(error.localizedDescription)")
                await MainActor.run {
                    self.error = "Failed to remove folder nodes from database: \(error.localizedDescription)"
                }
                // Restore watcher so the folder remains functional if deletion failed.
                startWatching(folder.path)
                return
            }

            await MainActor.run {
                self.watchedFolders.removeAll { $0.id == folder.id }
                self.progressMap.removeValue(forKey: folder.path)
                self.folderFiles.removeValue(forKey: folder.path)
                self.saveFolders()
                self.updateStats()
            }
        }
    }
    
    func refreshFolder(_ folder: IndexedFolder) {
        // Re-index the folder
        startIndexing(folder.path)
    }
    
    func toggleFolder(_ folder: IndexedFolder) {
        guard let index = watchedFolders.firstIndex(where: { $0.id == folder.id }) else { return }
        
        DispatchQueue.main.async {
            if self.watchedFolders[index].status == .active {
                self.watchedFolders[index].status = .inactive
                self.stopWatching(folder.path)
            } else {
                self.watchedFolders[index].status = .active
                self.startWatching(folder.path)
            }
            self.saveFolders()
        }
    }
    
    // MARK: - File Watching
    
    private func setupFileWatching() {
        // Initial setup is done in loadSavedFolders
    }
    
    func startWatching(_ path: String) {
        // Stop existing watcher if any
        stopWatching(path)
        
        let fileDescriptor = open(path, O_EVTONLY)
        guard fileDescriptor >= 0 else {
            print("❌ Failed to open path for watching: \(path)")
            return
        }
        
        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: fileDescriptor,
            eventMask: [.write, .delete, .rename, .extend],
            queue: .global()
        )
        
        source.setEventHandler { [weak self] in
            self?.handleFileChange(in: path)
        }
        
        source.setCancelHandler {
            close(fileDescriptor)
        }
        
        source.resume()
        fileSystemWatchers[path] = source
        
        print("👁️ Started watching: \(path)")
    }
    
    func stopWatching(_ path: String) {
        if let source = fileSystemWatchers[path] {
            source.cancel()
            fileSystemWatchers.removeValue(forKey: path)
            print("🛑 Stopped watching: \(path)")
        }
    }
    
    private func handleFileChange(in path: String) {
        print("📝 File change detected in: \(path)")
        
        // DISABLED: Auto re-indexing causes infinite loop when embeddings are written
        // User must manually click refresh to re-index
        // TODO: Make this smarter - only re-index if actual source files changed, not DB updates
        
        // Debounce - wait 500ms before re-indexing
        // DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { [weak self] in
        //     self?.startIndexing(path)
        // }
    }
    
    // MARK: - Indexing
    
    func startIndexing(_ path: String) {
        logger.log(.info, "Starting folder indexing", metadata: ["path": path])
        // Set up progress tracking
        let progress = IndexingProgress(
            id: path,
            path: path,
            totalFiles: 0,
            indexed: 0,
            skipped: 0,
            errored: 0,
            currentFile: nil,
            status: .indexing,
            startTime: Date(),
            endTime: nil
        )
        
        DispatchQueue.main.async {
            self.progressMap[path] = progress
            
            // Update folder status
            if let index = self.watchedFolders.firstIndex(where: { $0.path == path }) {
                self.watchedFolders[index].status = .indexing
                self.watchedFolders[index].isIndexing = true
            }
        }
        
        // Run indexing in background
        Task {
            // Resolve current connection/auth state before deciding whether to write.
            var canStoreInServer = await MainActor.run {
                return self.connectionStatus == .connected || self.connectionStatus == .authenticated
            }
            if !canStoreInServer {
                canStoreInServer = await self.canWriteToServer()
            }
            if !canStoreInServer {
                logger.log(.warning, "Indexing running without DB writes (not connected/authenticated)", metadata: [
                    "path": path
                ])
            }
            
            let files = await indexerService.indexFolder(at: path, recursive: true)
            logger.log(.info, "Folder scan completed", metadata: [
                "path": path,
                "candidate_files": "\(files.count)"
            ])
            
            // Update progress with total
            await MainActor.run {
                if var prog = self.progressMap[path] {
                    prog.totalFiles = files.count
                    self.progressMap[path] = prog
                }
            }
            
            // Store each file in NornicDB if connected
            var storedCount = 0
            var errorCount = 0
            
            // Get folder tags from watchedFolders
            let folderTags = await MainActor.run {
                return self.watchedFolders.first(where: { $0.path == path })?.tags ?? []
            }
            
            if canStoreInServer {
                for (index, file) in files.enumerated() {
                    // Update current file in progress
                    await MainActor.run {
                        if var prog = self.progressMap[path] {
                            prog.currentFile = file.relativePath
                            prog.indexed = index + 1
                            self.progressMap[path] = prog
                        }
                    }
                    
                    do {
                        try await storeFileInNornicDB(file, folderPath: path, tags: folderTags)
                        storedCount += 1
                    } catch {
                        logger.log(.error, "Failed to store indexed file", metadata: [
                            "relative_path": file.relativePath,
                            "full_path": file.path,
                            "error": error.localizedDescription
                        ])
                        errorCount += 1
                    }
                    
                    // Throttle indexing to reduce system load
                    if indexingThrottleMs > 0 {
                        try? await Task.sleep(nanoseconds: UInt64(indexingThrottleMs) * 1_000_000)
                    }
                }
            }
            
            // Capture final values for Swift 6 concurrency safety
            let finalStoredCount = storedCount
            let finalErrorCount = errorCount
            let totalFileCount = files.count
            let finalCanStoreInServer = canStoreInServer
            logger.log(finalErrorCount > 0 ? .warning : .info, "Indexing pass completed", metadata: [
                "path": path,
                "total_files": "\(totalFileCount)",
                "stored": "\(finalStoredCount)",
                "errors": "\(finalErrorCount)",
                "db_writes_enabled": finalCanStoreInServer ? "true" : "false"
            ])
            
            // Update progress and folder info
            await MainActor.run {
                if var prog = self.progressMap[path] {
                    prog.indexed = finalStoredCount
                    prog.errored = finalErrorCount
                    prog.status = .completed
                    prog.endTime = Date()
                    prog.currentFile = nil
                    self.progressMap[path] = prog
                }
                
                if let index = self.watchedFolders.firstIndex(where: { $0.path == path }) {
                    self.watchedFolders[index].fileCount = totalFileCount
                    self.watchedFolders[index].embeddingCount = finalStoredCount  // Server will generate embeddings
                    self.watchedFolders[index].status = .active
                    self.watchedFolders[index].isIndexing = false
                    self.watchedFolders[index].lastSync = Date()
                    
                    if !finalCanStoreInServer {
                        self.watchedFolders[index].error = "Database write skipped: not connected/authenticated. Re-run indexing after connection is green."
                        self.error = "No files were inserted into NornicDB. Connect/authenticate first, then click Refresh on the folder."
                    } else if finalErrorCount > 0 {
                        self.watchedFolders[index].error = "\(finalErrorCount) files failed to index"
                        self.error = "Some files failed to insert. Check logs for Cypher/API errors."
                    } else {
                        self.watchedFolders[index].error = nil
                        // Clear stale global error once an indexing pass fully succeeds.
                        if self.error?.contains("insert") == true || self.error?.contains("failed") == true {
                            self.error = nil
                        }
                    }
                }
                
                self.saveFolders()
                self.updateStats()
                
                // Start watching for changes
                self.startWatching(path)
                
                // Fetch updated stats from server
                if finalCanStoreInServer {
                    Task {
                        await self.fetchServerStats()
                    }
                }
            }
        }
    }
    
    // MARK: - Stats
    
    func updateStats() {
        stats = IndexStats(
            totalFolders: watchedFolders.count,
            totalFiles: watchedFolders.reduce(0) { $0 + $1.fileCount },
            totalChunks: watchedFolders.reduce(0) { $0 + $1.chunkCount },
            totalEmbeddings: watchedFolders.reduce(0) { $0 + $1.embeddingCount },
            byExtension: [:]
        )
    }
    
    /// Fetch stats from server
    @MainActor
    func fetchServerStats() async {
        guard connectionStatus == .connected || connectionStatus == .authenticated else {
            return
        }
        
        do {
            let (data, response) = try await makeAuthenticatedRequest(to: "/api/index-stats")
            
            if response.statusCode == 200,
               let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                stats.totalFiles = json["totalFiles"] as? Int ?? stats.totalFiles
                stats.totalChunks = json["totalChunks"] as? Int ?? stats.totalChunks
                stats.totalEmbeddings = json["totalEmbeddings"] as? Int ?? stats.totalEmbeddings
                if let byExt = json["byExtension"] as? [String: Int] {
                    stats.byExtension = byExt
                }
            }
        } catch {
            print("Failed to fetch server stats: \(error)")
        }
    }
    
    // MARK: - NornicDB API Integration
    
    /// Store an indexed file in NornicDB with folder tags
    func storeFileInNornicDB(_ file: FileIndexer.IndexedFile, folderPath: String, tags: [String] = []) async throws {
        // Check if node exists and is already up-to-date
        let checkQuery = """
        MATCH (f:File {path: $path})
        RETURN f.last_modified as last_modified,
               f.indexed_date as indexed_date,
               labels(f) as labels
        """
        
        let checkJSON = try await executeCypher(checkQuery, parameters: ["path": file.path])
        var existingLabels: [String] = []
        if let results = checkJSON["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           let row = data.first?["row"] as? [Any] {
            if row.count > 2 {
                existingLabels = tagsFromAny(row[2])
            }
            if row.count > 1, let indexedDate = row[1] as? String {
                // If indexed after file was last modified, skip
                let fileModifiedDate = ISO8601DateFormatter().string(from: file.lastModified)
                if indexedDate >= fileModifiedDate {
                    print("⏭️  Skipping \(file.path) - already indexed and up-to-date")
                    return
                }
            }
        }

        // Derive folder name for automatic tagging
        let folderName = (folderPath as NSString).lastPathComponent
        let folderManagedTags = inheritedTagsForFolder(folderPath, tags: tags)
        let baseLabels = Set(["File", "Node"])
        let existingFileTags = existingLabels.filter { !folderManagedTags.contains($0) && !baseLabels.contains($0) }
        let allTags = uniqueTags(folderManagedTags + existingFileTags)
        
        let properties: [String: Any] = [
            "path": file.path,
            "name": (file.path as NSString).lastPathComponent,
            "extension": file.fileExtension,
            "language": file.language ?? "unknown",
            "size_bytes": file.size,
            "content": file.content,
            "type": file.contentType == .text ? "text" :
                    file.contentType == .document ? "document" :
                    file.contentType == .imageOCR ? "image_ocr" : "image_description",
            "folder_root": folderPath,
            "folder_name": folderName,
            "last_modified": ISO8601DateFormatter().string(from: file.lastModified)
        ]
        let params: [String: Any] = [
            "path": file.path,
            "node_id": "file-\(UUID().uuidString)",
            "props": properties
        ]

        // Prefer explicit update/create flow instead of MERGE.
        // Some async write paths can surface "failed to create node in MERGE: already exists" under contention.
        let desiredNodeLabels = desiredLabels(for: allTags)
        let labelSetClause = setLabelClauses(variable: "f", labels: desiredNodeLabels)
        let labelRemoveClause = removeLabelClauses(
            variable: "f",
            labels: existingLabels.filter { !desiredNodeLabels.contains($0) && $0 != "File" && $0 != "Node" }
        )

        let updateQuery = """
        MATCH (f:File {path: $path})
        SET f:Node
        SET f += $props
        SET f.indexed_date = datetime()
        \(labelSetClause)
        \(labelRemoveClause)
        RETURN f.id as id
        """

        let createQuery = """
        CREATE (f:File:Node {
            path: $path,
            id: $node_id
        })
        SET f += $props
        SET f.indexed_date = datetime()
        \(labelSetClause)
        RETURN f.id as id
        """

        // If we already saw the node in the check query, update directly.
        if let results = checkJSON["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           !data.isEmpty {
            _ = try await executeCypher(updateQuery, parameters: params)
            return
        }

        // Not found in pre-check: try create. If a concurrent write created it first,
        // retry as update.
        do {
            _ = try await executeCypher(createQuery, parameters: params)
        } catch {
            let message = error.localizedDescription.lowercased()
            if message.contains("already exists") || message.contains("constraint") {
                _ = try await executeCypher(updateQuery, parameters: params)
            } else {
                throw error
            }
        }
    }
    
    /// Sanitize a string to be a valid tag (alphanumeric + underscore)
    private func sanitizeTag(_ tag: String) -> String {
        let trimmed = tag.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedSeparators = trimmed
            .replacingOccurrences(of: " ", with: "_")
            .replacingOccurrences(of: "-", with: "_")
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "_"))
        let filtered = String(normalizedSeparators.unicodeScalars.filter { allowed.contains($0) })

        // Collapse duplicate underscores and trim leading/trailing underscores.
        let collapsed = filtered.replacingOccurrences(of: "_+", with: "_", options: .regularExpression)
        let stripped = collapsed.trimmingCharacters(in: CharacterSet(charactersIn: "_"))

        guard !stripped.isEmpty else { return "" }

        // Keep tags label-safe: first character must be a letter or underscore.
        if let first = stripped.unicodeScalars.first,
           CharacterSet.decimalDigits.contains(first) {
            return "_" + stripped
        }
        return stripped
    }

    private func desiredLabels(for effectiveTags: [String]) -> [String] {
        uniqueTags(["File", "Node"] + effectiveTags.map { sanitizeTag($0) }.filter { !$0.isEmpty })
    }

    private func setLabelClauses(variable: String, labels: [String]) -> String {
        labels
            .map { sanitizeTag($0) }
            .filter { !$0.isEmpty }
            .map { "SET \(variable):\($0)" }
            .joined(separator: "\n")
    }

    private func removeLabelClauses(variable: String, labels: [String]) -> String {
        labels
            .map { sanitizeTag($0) }
            .filter { !$0.isEmpty }
            .map { "REMOVE \(variable):\($0)" }
            .joined(separator: "\n")
    }

    private func uniqueTags(_ tags: [String]) -> [String] {
        var seen = Set<String>()
        var result: [String] = []
        for tag in tags {
            if !tag.isEmpty && !seen.contains(tag) {
                seen.insert(tag)
                result.append(tag)
            }
        }
        return result
    }

    func inheritedTagsForFolder(_ folderPath: String, tags: [String]) -> [String] {
        let folderName = (folderPath as NSString).lastPathComponent
        let folderTag = sanitizeTag(folderName)
        let sanitizedTags = tags.map { sanitizeTag($0) }.filter { !$0.isEmpty }
        return uniqueTags(["File", folderTag] + sanitizedTags)
    }

    private func tagsFromAny(_ value: Any?) -> [String] {
        if let raw = value as? String {
            // Defensive: older/broken writes may have stored expression text instead of arrays.
            // Ignore those placeholders so subsequent writes can heal data back to arrays.
            if raw.lowercased().contains("reduce(") {
                return []
            }
            let sanitized = sanitizeTag(raw)
            return sanitized.isEmpty ? [] : [sanitized]
        }
        guard let values = value as? [Any] else { return [] }
        return values.compactMap { $0 as? String }.map { sanitizeTag($0) }.filter { !$0.isEmpty }
    }
    
    /// Update folder tags for all files in a folder (labels-only).
    func updateFolderTags(_ folderPath: String, tags: [String], previousTags: [String]? = nil) async throws {
        let newFolderManagedTags = inheritedTagsForFolder(folderPath, tags: tags)
        let oldFolderManagedTags = inheritedTagsForFolder(folderPath, tags: previousTags ?? tags)
        let baseLabels = Set(["File", "Node"])

        let fetchQuery = """
        MATCH (f:File {folder_root: $folder_root})
        RETURN f.path as path, labels(f) as labels
        """
        let fetched = try await executeCypher(fetchQuery, parameters: ["folder_root": folderPath])

        var updatedCount = 0
        if let results = fetched["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]] {
            for entry in data {
                guard let row = entry["row"] as? [Any], row.count >= 2 else { continue }
                guard let path = row[0] as? String else { continue }
                let existingLabels = uniqueTags(tagsFromAny(row[1]))
                let fileSpecificLabels = existingLabels.filter { !oldFolderManagedTags.contains($0) && !baseLabels.contains($0) }
                let effectiveTags = uniqueTags(newFolderManagedTags + fileSpecificLabels)
                let desired = desiredLabels(for: effectiveTags)
                let labelsToRemove = oldFolderManagedTags.filter { !newFolderManagedTags.contains($0) }
                let addClauses = setLabelClauses(variable: "f", labels: desired)
                let removeClauses = removeLabelClauses(variable: "f", labels: labelsToRemove)

                let updateQuery = """
                MATCH (f:File {path: $path, folder_root: $folder_root})
                \(addClauses)
                \(removeClauses)
                RETURN count(f) as updated
                """
                let result = try await executeCypher(updateQuery, parameters: [
                    "path": path,
                    "folder_root": folderPath,
                ])

                if let r = result["results"] as? [[String: Any]],
                   let d = r.first?["data"] as? [[String: Any]],
                   let out = d.first?["row"] as? [Any],
                   let count = out.first as? NSNumber {
                    updatedCount += count.intValue
                }
            }
        }
        print("✅ Updated tags for \(updatedCount) files in \(folderPath)")
        await loadIndexedFiles(for: folderPath)
    }
    
    /// Add a tag to a folder (updates all files)
    func addTagToFolder(_ folderPath: String, tag: String) async throws {
        guard let index = watchedFolders.firstIndex(where: { $0.path == folderPath }) else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Folder not found"])
        }
        
        let sanitized = sanitizeTag(tag)
        guard !sanitized.isEmpty else { return }
        
        let previousTags = watchedFolders[index].tags
        // Add to local folder tags
        if !watchedFolders[index].tags.contains(sanitized) {
            await MainActor.run {
                self.watchedFolders[index].tags.append(sanitized)
                self.saveFolders()
            }
        }
        
        // Update all files in database (labels-only)
        try await updateFolderTags(folderPath, tags: watchedFolders[index].tags, previousTags: previousTags)
    }
    
    /// Remove a tag from a folder (updates all files)
    func removeTagFromFolder(_ folderPath: String, tag: String) async throws {
        guard let index = watchedFolders.firstIndex(where: { $0.path == folderPath }) else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Folder not found"])
        }
        
        let sanitized = sanitizeTag(tag)
        
        let previousTags = watchedFolders[index].tags
        // Remove from local folder tags
        await MainActor.run {
            self.watchedFolders[index].tags.removeAll { $0 == sanitized }
            self.saveFolders()
        }
        
        // Update all files in database (labels-only)
        try await updateFolderTags(folderPath, tags: watchedFolders[index].tags, previousTags: previousTags)
    }

    /// Load indexed files for a folder so users can manage file-level tags.
    @MainActor
    func loadIndexedFiles(for folderPath: String) async {
        loadingFilesForFolders.insert(folderPath)
        defer { loadingFilesForFolders.remove(folderPath) }

        guard let folder = watchedFolders.first(where: { $0.path == folderPath }) else {
            folderFiles[folderPath] = []
            return
        }
        let defaultInherited = inheritedTagsForFolder(folderPath, tags: folder.tags)

        let query = """
        MATCH (f:File {folder_root: $folder_root})
        RETURN f.path as path,
               f.name as name,
               f.folder_root as folder_root,
               labels(f) as labels
        ORDER BY f.path
        """

        do {
            let result = try await executeCypher(query, parameters: [
                "folder_root": folderPath
            ])

            let parsed: [IndexedFolderFile]
            if let results = result["results"] as? [[String: Any]],
               let data = results.first?["data"] as? [[String: Any]] {
                parsed = data.compactMap { rowWrapper in
                    guard let row = rowWrapper["row"] as? [Any], row.count >= 4 else { return nil }
                    guard let path = row[0] as? String else { return nil }
                    let name = (row[1] as? String) ?? (path as NSString).lastPathComponent
                    let inherited = defaultInherited
                    let labels = uniqueTags(tagsFromAny(row[3]))
                    let fileTags = labels.filter { !inherited.contains($0) && $0 != "File" && $0 != "Node" }
                    let effective = uniqueTags(inherited + fileTags)
                    let rel = path.hasPrefix(folderPath + "/") ? String(path.dropFirst(folderPath.count + 1)) : (path as NSString).lastPathComponent
                    return IndexedFolderFile(
                        path: path,
                        name: name,
                        relativePath: rel,
                        inheritedTags: inherited,
                        fileTags: fileTags,
                        effectiveTags: effective
                    )
                }
            } else {
                parsed = []
            }
            folderFiles[folderPath] = parsed
        } catch {
            self.error = "Failed to load indexed files: \(error.localizedDescription)"
            folderFiles[folderPath] = []
        }
    }

    /// Add a tag to a specific file. Inherited folder tags are not duplicated in file tags.
    func addTagToFile(folderPath: String, filePath: String, tag: String) async throws {
        guard let folder = watchedFolders.first(where: { $0.path == folderPath }) else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Folder not found"])
        }
        let inherited = inheritedTagsForFolder(folderPath, tags: folder.tags)
        let sanitized = sanitizeTag(tag)
        guard !sanitized.isEmpty else { return }
        guard !inherited.contains(sanitized) else {
            await loadIndexedFiles(for: folderPath)
            return
        }

        let currentFileTags = folderFiles[folderPath]?.first(where: { $0.path == filePath })?.fileTags ?? []
        let newFileTags = uniqueTags(currentFileTags + [sanitized])
        let effectiveTags = uniqueTags(inherited + newFileTags)
        let labelSetClause = setLabelClauses(variable: "f", labels: desiredLabels(for: effectiveTags))

        let query = """
        MATCH (f:File {path: $path, folder_root: $folder_root})
        \(labelSetClause)
        RETURN f.path as path
        """

        _ = try await executeCypher(query, parameters: [
            "path": filePath,
            "folder_root": folderPath,
        ])
        await loadIndexedFiles(for: folderPath)
    }

    /// Remove a file-level tag from a specific file. Inherited tags cannot be removed here.
    func removeTagFromFile(folderPath: String, filePath: String, tag: String) async throws {
        guard let folder = watchedFolders.first(where: { $0.path == folderPath }) else {
            throw NSError(domain: "FileWatchManager", code: -1, userInfo: [NSLocalizedDescriptionKey: "Folder not found"])
        }
        let inherited = inheritedTagsForFolder(folderPath, tags: folder.tags)
        let sanitized = sanitizeTag(tag)
        guard !sanitized.isEmpty else { return }
        guard !inherited.contains(sanitized) else {
            await loadIndexedFiles(for: folderPath)
            return
        }

        let currentFileTags = folderFiles[folderPath]?.first(where: { $0.path == filePath })?.fileTags ?? []
        let newFileTags = currentFileTags.filter { $0 != sanitized }
        let effectiveTags = uniqueTags(inherited + newFileTags)
        let labelSetClause = setLabelClauses(variable: "f", labels: desiredLabels(for: effectiveTags))
        let removeClause = removeLabelClauses(variable: "f", labels: [sanitized])

        let query = """
        MATCH (f:File {path: $path, folder_root: $folder_root})
        \(labelSetClause)
        \(removeClause)
        RETURN f.path as path
        """

        _ = try await executeCypher(query, parameters: [
            "path": filePath,
            "folder_root": folderPath,
        ])
        await loadIndexedFiles(for: folderPath)
    }
    
    /// Delete all file nodes and their chunks for a folder
    func deleteNodesForFolder(_ folderPath: String) async throws {
        // First, get count of nodes to be deleted for logging
        let countQuery = """
        MATCH (f:File {folder_root: $folder_root})
        OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c)
        RETURN count(DISTINCT f) as fileCount, count(DISTINCT c) as chunkCount
        """
        
        let countJSON = try await executeCypher(countQuery, parameters: ["folder_root": folderPath])
        
        var fileCount = 0
        var chunkCount = 0
        if let results = countJSON["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           let row = data.first?["row"] as? [Any] {
            if let v = row.first as? Int {
                fileCount = v
            } else if let v = row.first as? NSNumber {
                fileCount = v.intValue
            }
            if row.count > 1 {
                if let v = row[1] as? Int {
                    chunkCount = v
                } else if let v = row[1] as? NSNumber {
                    chunkCount = v.intValue
                }
            }
        }
        
        print("🗑️  Deleting \(fileCount) File nodes and \(chunkCount) FileChunk nodes for folder: \(folderPath)")
        
        // Delete all File nodes for this folder root.
        // This is the primary contract for "trash folder" behavior.
        let deleteQuery = """
        MATCH (f:File {folder_root: $folder_root})
        DETACH DELETE f
        RETURN count(f) as deleted
        """
        
        _ = try await executeCypher(deleteQuery, parameters: ["folder_root": folderPath])

        // Defensive verification pass (best effort).
        let verifyQuery = """
        MATCH (f:File {folder_root: $folder_root})
        RETURN count(f) as remaining
        """
        let verifyJSON = try await executeCypher(verifyQuery, parameters: ["folder_root": folderPath])
        var remaining = 0
        if let results = verifyJSON["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           let row = data.first?["row"] as? [Any],
           let value = row.first as? NSNumber {
            remaining = value.intValue
        }
        if remaining > 0 {
            throw NSError(
                domain: "FileWatchManager",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Folder delete incomplete: \(remaining) nodes still remain"]
            )
        }
        print("✅ Successfully deleted all File nodes for folder: \(folderPath)")
    }
    
    /// Perform vector search
    func vectorSearch(query: String, limit: Int = 20, minSimilarity: Double = 0.0) async throws -> [VectorSearchResult] {
        // Use the correct NornicDB search endpoint (POST /nornicdb/search)
        let requestBody: [String: Any] = [
            "query": query,
            "labels": ["File"],
            "limit": limit
        ]
        
        let bodyData = try JSONSerialization.data(withJSONObject: requestBody)
        let (data, response) = try await makeAuthenticatedRequest(to: "/nornicdb/search", method: "POST", body: bodyData)
        
        guard response.statusCode == 200 else {
            let errorMsg = String(data: data, encoding: .utf8) ?? "Unknown error"
            throw NSError(domain: "FileWatchManager", code: response.statusCode,
                         userInfo: [NSLocalizedDescriptionKey: "Search failed: \(errorMsg)"])
        }
        
        // Parse search results - the API returns an array of SearchResult objects
        // Each result has: node (with properties), score, rrf_score, bm25_rank
        // RRF scores are typically in 0-0.1 range, not 0-1 like cosine similarity
        guard let results = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            return []
        }
        
        return results.compactMap { result in
            // Get RRF score (already ranked by relevance)
            let score = result["rrf_score"] as? Double ?? result["score"] as? Double ?? 0.0
            
            // Filter by min similarity (if set above 0)
            guard score >= minSimilarity else {
                return nil
            }
            
            // Get node properties
            guard let node = result["node"] as? [String: Any],
                  let properties = node["properties"] as? [String: Any],
                  let id = node["id"] as? String else {
                return nil
            }
            
            let path = properties["path"] as? String ?? ""
            let name = properties["name"] as? String ?? (path as NSString).lastPathComponent
            
            return VectorSearchResult(
                id: id,
                title: name,
                path: path,
                similarity: score,
                language: properties["language"] as? String
            )
        }
    }
}

// MARK: - Vector Search Result

struct VectorSearchResult: Identifiable {
    let id: String
    let title: String
    let path: String
    let similarity: Double
    let language: String?
}


// MARK: - File Indexer View

struct FileIndexerView: View {
    @ObservedObject var config: ConfigManager
    @StateObject var watchManager: FileWatchManager
    @State private var searchQuery: String = ""
    @State private var searchResults: [VectorSearchResult] = []
    @State private var isSearching: Bool = false
    @State private var showSearchSettings: Bool = false
    @State private var minSimilarity: Double = 0.0  // RRF scores are typically 0-0.1, not 0-1
    @State private var searchLimit: Int = 20
    @State private var authUsername: String = ""
    @State private var authPassword: String = ""
    @State private var isAuthenticating: Bool = false
    
    // Tag editing state
    @State private var editingTagsForFolder: String? = nil  // Folder path being edited
    @State private var newTagText: String = ""
    @State var isUpdatingTags: Bool = false
    @State var expandedFolders: Set<String> = []
    @State var newFileTagTextByPath: [String: String] = [:]
    
    init(config: ConfigManager) {
        self.config = config
        self._watchManager = StateObject(wrappedValue: FileWatchManager(config: config))
    }
    
    var body: some View {
        VStack(spacing: 0) {
            // Header
            headerView
            
            // Connection status bar
            connectionStatusBar
            
            Divider()
            
            // Error banner
            if let error = watchManager.error {
                errorBanner(error)
            }
            
            ScrollView {
                VStack(spacing: 20) {
                    // Auth required warning
                    if watchManager.connectionStatus == .authRequired {
                        authRequiredBanner
                    }
                    
                    // Search (only if connected/authenticated)
                    if watchManager.connectionStatus == .connected || watchManager.connectionStatus == .authenticated {
                        searchSection
                    }
                    
                    // Stats
                    statsSection
                    
                    // Folders
                    foldersSection
                }
                .padding()
            }
        }
        .frame(minWidth: 700, minHeight: 600)
        .onAppear {
            Task {
                // Small delay to let UI fully initialize before potentially triggering Keychain dialog
                try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
                await watchManager.checkServerConnection()
            }
        }
    }
    
    // MARK: - Connection Status Bar
    
    var connectionStatusBar: some View {
        HStack {
            Circle()
                .fill(connectionColor)
                .frame(width: 8, height: 8)
            
            Text(connectionText)
                .font(.caption)
                .foregroundColor(.secondary)
            
            Spacer()
            
            if watchManager.connectionStatus == .disconnected {
                Button("Retry") {
                    Task {
                        await watchManager.checkServerConnection()
                    }
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            }
        }
        .padding(.horizontal)
        .padding(.vertical, 6)
        .background(connectionColor.opacity(0.1))
    }
    
    var connectionColor: Color {
        switch watchManager.connectionStatus {
        case .unknown: return .gray
        case .connected: return .green
        case .disconnected: return .red
        case .authRequired: return .orange
        case .authenticated: return .green
        }
    }
    
    var connectionText: String {
        switch watchManager.connectionStatus {
        case .unknown: return "Checking server..."
        case .connected: return "Connected (no auth required)"
        case .disconnected: return "Server not available"
        case .authRequired: return "Authentication required"
        case .authenticated: return "Connected and authenticated"
        }
    }
    
    // MARK: - Auth Required Banner
    
    var authRequiredBanner: some View {
        VStack(spacing: 16) {
            HStack(spacing: 8) {
                Image(systemName: "lock.fill")
                    .font(.title2)
                    .foregroundColor(.orange)
                Text("Sign In to NornicDB")
                    .font(.title3)
                    .fontWeight(.semibold)
            }
            
            Text("Enter credentials to enable semantic search and file indexing")
                .font(.callout)
                .foregroundColor(.secondary)
            
            VStack(alignment: .leading, spacing: 12) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Username")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextField("admin", text: $authUsername)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 300)
                }
                
                VStack(alignment: .leading, spacing: 4) {
                    Text("Password")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    SecureField("password", text: $authPassword)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 300)
                }
            }
            
            Button {
                NSLog("🔐 Direct Sign In button CLICKED! username='\(authUsername)'")
                performAuth()
            } label: {
                if isAuthenticating {
                    HStack {
                        ProgressView()
                            .scaleEffect(0.8)
                        Text("Signing In...")
                    }
                } else {
                    Text("Sign In")
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(authUsername.isEmpty || authPassword.isEmpty || isAuthenticating)
        }
        .frame(maxWidth: .infinity)
        .padding()
        .background(Color.orange.opacity(0.1))
        .onAppear {
            // Pre-fill from config
            authUsername = config.adminUsername
            authPassword = config.adminPassword
            NSLog("🔐 Auth banner appeared, pre-filled username='\(authUsername)'")
        }
        .cornerRadius(10)
    }
    
    // MARK: - Authentication
    
    func performAuth() {
        NSLog("🔐 [FileIndexer] Starting authentication for user: \(authUsername)")
        isAuthenticating = true
        Task {
            NSLog("🔐 [FileIndexer] Calling authenticate method...")
            let success = await watchManager.authenticate(username: authUsername, password: authPassword)
            NSLog("🔐 [FileIndexer] Authentication result: \(success)")
            await MainActor.run {
                isAuthenticating = false
                if success {
                    NSLog("✅ [FileIndexer] Authentication successful!")
                    // Fetch stats after auth
                    Task {
                        await watchManager.fetchServerStats()
                    }
                } else {
                    NSLog("❌ [FileIndexer] Authentication failed")
                    watchManager.error = "Authentication failed. Please check your credentials."
                }
            }
        }
    }
    
    // MARK: - Header
    
    var headerView: some View {
        HStack {
            HStack(spacing: 8) {
                Image(systemName: "brain.head.profile")
                    .font(.title2)
                    .foregroundColor(.blue)
                Text("NornicDB Code Intelligence")
                    .font(.title2)
                    .fontWeight(.semibold)
            }
            
            Spacer()
            
            Text("File indexing, semantic search, and embeddings")
                .font(.caption)
                .foregroundColor(.secondary)
        }
        .padding()
        .background(Color(NSColor.controlBackgroundColor))
    }
    
    // MARK: - Error Banner
    
    func errorBanner(_ error: String) -> some View {
        HStack {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundColor(.orange)
            Text(error)
                .font(.callout)
            Spacer()
            Button(action: openIndexerLogs) {
                Label("Open Logs", systemImage: "doc.text.magnifyingglass")
                    .font(.caption)
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            Button(action: { watchManager.error = nil }) {
                Image(systemName: "xmark.circle.fill")
                    .foregroundColor(.secondary)
            }
            .buttonStyle(.plain)
        }
        .padding()
        .background(Color.orange.opacity(0.1))
    }
    
    // MARK: - Search Section
    
    var searchSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Image(systemName: "magnifyingglass")
                    .foregroundColor(.secondary)
                
                TextField("Search indexed files by content...", text: $searchQuery)
                    .textFieldStyle(.roundedBorder)
                    .onSubmit { performSearch() }
                
                if !searchQuery.isEmpty {
                    Button(action: { searchQuery = ""; searchResults = [] }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(.secondary)
                    }
                    .buttonStyle(.plain)
                }
                
                Button(action: performSearch) {
                    if isSearching {
                        ProgressView()
                            .scaleEffect(0.7)
                    } else {
                        Image(systemName: "magnifyingglass")
                    }
                }
                .buttonStyle(.bordered)
                .disabled(isSearching || searchQuery.isEmpty)
                
                Button(action: { showSearchSettings.toggle() }) {
                    Image(systemName: "gear")
                }
                .buttonStyle(.plain)
            }
            
            // Search settings
            if showSearchSettings {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(spacing: 20) {
                        HStack {
                            Text("Min Similarity:")
                                .font(.caption)
                            Slider(value: $minSimilarity, in: 0.5...1.0, step: 0.05)
                                .frame(width: 100)
                            Text("\(minSimilarity, specifier: "%.2f")")
                                .font(.caption)
                                .monospacedDigit()
                        }
                        
                        HStack {
                            Text("Max Results:")
                                .font(.caption)
                            TextField("", value: $searchLimit, format: .number)
                                .textFieldStyle(.roundedBorder)
                                .frame(width: 60)
                        }
                    }
                    
                    HStack {
                        Text("Indexing Throttle:")
                            .font(.caption)
                        Slider(value: Binding(
                            get: { Double(watchManager.indexingThrottleMs) },
                            set: { watchManager.indexingThrottleMs = Int($0) }
                        ), in: 0...500, step: 10)
                            .frame(width: 150)
                        Text("\(watchManager.indexingThrottleMs)ms")
                            .font(.caption)
                            .monospacedDigit()
                            .frame(width: 50, alignment: .trailing)
                        
                        Text(watchManager.indexingThrottleMs == 0 ? "(fastest)" : watchManager.indexingThrottleMs >= 200 ? "(gentle)" : "")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                }
                .padding(.horizontal)
            }
            
            // Search results
            if !searchResults.isEmpty {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Found \(searchResults.count) results")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    ForEach(searchResults) { result in
                        searchResultRow(result)
                    }
                }
            }
        }
        .padding()
        .background(Color.secondary.opacity(0.05))
        .cornerRadius(10)
    }
    
    func searchResultRow(_ result: VectorSearchResult) -> some View {
        HStack {
            Image(systemName: "doc.text")
                .foregroundColor(.blue)
            
            VStack(alignment: .leading, spacing: 2) {
                Text(result.title)
                    .font(.callout)
                    .fontWeight(.medium)
                Text(result.path)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            if let lang = result.language {
                Text(lang)
                    .font(.caption2)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.blue.opacity(0.1))
                    .cornerRadius(4)
            }
            
            Text("\(result.similarity * 100, specifier: "%.1f")%")
                .font(.caption)
                .foregroundColor(.green)
                .monospacedDigit()
        }
        .padding(8)
        .background(Color.secondary.opacity(0.05))
        .cornerRadius(6)
        .onTapGesture {
            // Open file in default app
            NSWorkspace.shared.open(URL(fileURLWithPath: result.path))
        }
    }
    
    func performSearch() {
        guard !searchQuery.isEmpty else { return }
        guard watchManager.connectionStatus == .connected || watchManager.connectionStatus == .authenticated else {
            watchManager.error = "Cannot search: not connected to NornicDB server"
            return
        }
        
        isSearching = true
        searchResults = []
        
        Task {
            do {
                let results = try await watchManager.vectorSearch(
                    query: searchQuery,
                    limit: searchLimit,
                    minSimilarity: minSimilarity
                )
                
                await MainActor.run {
                    searchResults = results
                    isSearching = false
                }
            } catch {
                await MainActor.run {
                    watchManager.error = "Search failed: \(error.localizedDescription)"
                    isSearching = false
                }
            }
        }
    }
    
    // MARK: - Stats Section
    
    var statsSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("📊 Index Statistics")
                .font(.headline)
            
            LazyVGrid(columns: [
                GridItem(.flexible()),
                GridItem(.flexible()),
                GridItem(.flexible()),
                GridItem(.flexible())
            ], spacing: 15) {
                StatCard(icon: "folder.fill", value: watchManager.stats.totalFolders, label: "Folders Watched", color: .blue)
                StatCard(icon: "doc.fill", value: watchManager.stats.totalFiles, label: "Files Indexed", color: .green)
                StatCard(icon: "puzzlepiece.fill", value: watchManager.stats.totalChunks, label: "Chunks Created", color: .orange)
                StatCard(icon: "target", value: watchManager.stats.totalEmbeddings, label: "Embeddings", color: .purple)
            }
        }
        .padding()
        .background(Color.secondary.opacity(0.05))
        .cornerRadius(10)
    }
    
    // MARK: - Folders Section
    
    var foldersSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("📂 Indexed Folders")
                    .font(.headline)
                
                Spacer()
                
                Button(action: { watchManager.updateStats() }) {
                    HStack(spacing: 4) {
                        Image(systemName: "arrow.clockwise")
                        Text("Refresh")
                    }
                }
                .buttonStyle(.bordered)

                Button(action: openIndexerLogs) {
                    HStack(spacing: 4) {
                        Image(systemName: "doc.text.magnifyingglass")
                        Text("Open Logs")
                    }
                }
                .buttonStyle(.bordered)
                
                Button(action: selectFolder) {
                    HStack(spacing: 4) {
                        Image(systemName: "plus")
                        Text("Add Folder")
                    }
                }
                .buttonStyle(.borderedProminent)
            }
            
            if watchManager.watchedFolders.isEmpty {
                emptyState
            } else {
                VStack(spacing: 10) {
                    ForEach(watchManager.watchedFolders) { folder in
                        folderRow(folder)
                    }
                }
            }
        }
        .padding()
        .background(Color.secondary.opacity(0.05))
        .cornerRadius(10)
    }
    
    var emptyState: some View {
        VStack(spacing: 12) {
            Image(systemName: "folder.badge.plus")
                .font(.largeTitle)
                .foregroundColor(.secondary)
            Text("No folders are being indexed")
                .font(.callout)
                .foregroundColor(.secondary)
            Text("Click \"Add Folder\" to start indexing a workspace")
                .font(.caption)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(40)
    }
    
    func folderRow(_ folder: IndexedFolder) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 12) {
                // Status icon
                Text(folder.status.icon)
                    .font(.title2)
                
                // Folder info
                VStack(alignment: .leading, spacing: 4) {
                    Text((folder.path as NSString).lastPathComponent)
                        .font(.callout)
                        .fontWeight(.medium)
                    
                    Text(folder.path)
                        .font(.caption)
                        .foregroundColor(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                    
                    HStack(spacing: 12) {
                        Label("\(folder.fileCount) files", systemImage: "doc")
                        Label("Last: \(formatDate(folder.lastSync))", systemImage: "clock")
                    }
                    .font(.caption2)
                    .foregroundColor(.secondary)
                }
                
                Spacer()
                
                // Progress indicator if indexing
                if let progress = watchManager.progressMap[folder.path], progress.status == .indexing {
                    VStack(alignment: .trailing, spacing: 4) {
                        ProgressView(value: progress.percentComplete, total: 100)
                            .frame(width: 100)
                        Text("\(progress.indexed)/\(progress.totalFiles)")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                }
                
                // Actions
                HStack(spacing: 8) {
                    Button(action: { 
                        withAnimation {
                            editingTagsForFolder = editingTagsForFolder == folder.path ? nil : folder.path
                        }
                    }) {
                        Image(systemName: "tag")
                            .foregroundColor(folder.isIndexing ? .secondary : (editingTagsForFolder == folder.path ? .accentColor : .primary))
                    }
                    .buttonStyle(.plain)
                    .disabled(folder.isIndexing)
                    .help(folder.isIndexing ? "Cannot edit tags while indexing" : "Manage tags")

                    Button(action: {
                        toggleFolderFiles(folder)
                    }) {
                        Image(systemName: expandedFolders.contains(folder.path) ? "chevron.down.circle" : "chevron.right.circle")
                    }
                    .buttonStyle(.plain)
                    .help("Browse indexed files")
                    
                    Button(action: { watchManager.refreshFolder(folder) }) {
                        Image(systemName: "arrow.clockwise")
                    }
                    .buttonStyle(.plain)
                    .help("Re-index folder")
                    
                    Button(action: { watchManager.toggleFolder(folder) }) {
                        Image(systemName: folder.status == .active ? "pause.fill" : "play.fill")
                    }
                    .buttonStyle(.plain)
                    .help(folder.status == .active ? "Pause watching" : "Resume watching")
                    
                    Button(action: { watchManager.removeFolder(folder) }) {
                        Image(systemName: "trash")
                            .foregroundColor(.red)
                    }
                    .buttonStyle(.plain)
                    .help("Remove from index")
                }
            }
            
            // Tags section
            VStack(alignment: .leading, spacing: 6) {
                // Always show tags if any exist
                if !folder.tags.isEmpty {
                    tagDisplayRow(folder)
                }
                
                // Show tag editor when expanded (but not while indexing)
                if editingTagsForFolder == folder.path && !folder.isIndexing {
                    tagEditorRow(folder)
                }
                
                // Show warning if trying to edit while indexing
                if editingTagsForFolder == folder.path && folder.isIndexing {
                    HStack(spacing: 4) {
                        Image(systemName: "info.circle")
                            .font(.caption2)
                            .foregroundColor(.orange)
                        Text("Tag editing disabled while indexing is in progress")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                    .padding(.top, 4)
                }

                if expandedFolders.contains(folder.path) {
                    folderFilesSection(folder)
                }
            }
        }
        .padding()
        .background(folder.status.color.opacity(0.1))
        .cornerRadius(8)
    }
    
    // Display current tags for a folder
    func tagDisplayRow(_ folder: IndexedFolder) -> some View {
        HStack(spacing: 4) {
            Image(systemName: "tag.fill")
                .font(.caption2)
                .foregroundColor(.secondary)
            
            // Auto-tags (folder name)
            Text(folder.folderTag)
                .font(.caption2)
                .padding(.horizontal, 6)
                .padding(.vertical, 2)
                .background(Color.blue.opacity(0.2))
                .cornerRadius(4)
            
            // User tags
            ForEach(folder.tags, id: \.self) { tag in
                HStack(spacing: 2) {
                    Text(tag)
                        .font(.caption2)
                    
                    // Only show remove button when editing and not indexing
                    if editingTagsForFolder == folder.path && !folder.isIndexing {
                        Button(action: {
                            removeTag(tag, from: folder)
                        }) {
                            Image(systemName: "xmark.circle.fill")
                                .font(.caption2)
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.horizontal, 6)
                .padding(.vertical, 2)
                .background(Color.green.opacity(0.2))
                .cornerRadius(4)
            }
        }
    }
    
    // Tag editor row for adding new tags
    func tagEditorRow(_ folder: IndexedFolder) -> some View {
        HStack(spacing: 8) {
            TextField("Add tag...", text: $newTagText)
                .textFieldStyle(.roundedBorder)
                .frame(width: 150)
                .onSubmit {
                    addTag(to: folder)
                }
            
            Button(action: { addTag(to: folder) }) {
                if isUpdatingTags {
                    ProgressView()
                        .scaleEffect(0.7)
                } else {
                    Image(systemName: "plus.circle.fill")
                }
            }
            .buttonStyle(.plain)
            .disabled(newTagText.trimmingCharacters(in: .whitespaces).isEmpty || isUpdatingTags)
            
            Spacer()
            
            Text("Tags help organize and filter files in search")
                .font(.caption2)
                .foregroundColor(.secondary)
        }
        .padding(.top, 4)
    }
    
    // Add a tag to a folder
    func addTag(to folder: IndexedFolder) {
        let tag = newTagText.trimmingCharacters(in: .whitespaces)
        guard !tag.isEmpty else { return }
        
        isUpdatingTags = true
        Task {
            do {
                try await watchManager.addTagToFolder(folder.path, tag: tag)
                await MainActor.run {
                    newTagText = ""
                    isUpdatingTags = false
                }
            } catch {
                await MainActor.run {
                    watchManager.error = "Failed to add tag: \(error.localizedDescription)"
                    isUpdatingTags = false
                }
            }
        }
    }
    
    // Remove a tag from a folder
    func removeTag(_ tag: String, from folder: IndexedFolder) {
        isUpdatingTags = true
        Task {
            do {
                try await watchManager.removeTagFromFolder(folder.path, tag: tag)
                await MainActor.run {
                    isUpdatingTags = false
                }
            } catch {
                await MainActor.run {
                    watchManager.error = "Failed to remove tag: \(error.localizedDescription)"
                    isUpdatingTags = false
                }
            }
        }
    }
    
    func selectFolder() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = false
        panel.message = "Select a folder to index"
        panel.prompt = "Index Folder"
        
        if panel.runModal() == .OK, let url = panel.url {
            watchManager.addFolder(url.path)
        }
    }

    func openIndexerLogs() {
        FileIndexerLogger.shared.openLogFile()
    }
    
    func formatDate(_ date: Date) -> String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .abbreviated
        return formatter.localizedString(for: date, relativeTo: Date())
    }
}

// MARK: - Stat Card

struct StatCard: View {
    let icon: String
    let value: Int
    let label: String
    let color: Color
    
    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: icon)
                .font(.title2)
                .foregroundColor(color)
            
            Text("\(value)")
                .font(.title)
                .fontWeight(.bold)
                .monospacedDigit()
            
            Text(label)
                .font(.caption)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .padding()
        .background(color.opacity(0.1))
        .cornerRadius(10)
    }
}

// MARK: - Window Controller

class FileIndexerWindowController {
    static let shared = FileIndexerWindowController()
    
    private var window: NSWindow?
    private var windowDelegate: WindowDelegate?  // Strong reference to prevent deallocation
    
    func showWindow(config: ConfigManager) {
        if let existingWindow = window {
            existingWindow.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        
        let contentView = FileIndexerView(config: config)
        
        let hostingController = NSHostingController(rootView: contentView)
        
        let newWindow = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 800, height: 700),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        
        newWindow.title = "NornicDB Code Intelligence"
        newWindow.contentViewController = hostingController
        newWindow.center()
        newWindow.setFrameAutosaveName("FileIndexerWindow")
        
        // Clean up reference when closed
        windowDelegate = WindowDelegate { [weak self] in
            self?.window = nil
            self?.windowDelegate = nil  // Release delegate when window closes
        }
        newWindow.delegate = windowDelegate
        
        window = newWindow
        newWindow.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
    
    private class WindowDelegate: NSObject, NSWindowDelegate {
        let onClose: () -> Void
        
        init(onClose: @escaping () -> Void) {
            self.onClose = onClose
        }
        
        func windowWillClose(_ notification: Notification) {
            onClose()
        }
    }
}

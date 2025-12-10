// FileIndexerWindow.swift
// File Indexing UI with folder selection, real-time stats, and search
// Similar to Mimir VS Code Intelligence panel

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
    
    // Server configuration (from ConfigManager)
    private var serverHost: String { config.hostAddress }
    private var serverPort: String { config.httpPortNumber }
    private var serverBaseURL: String { "http://\(serverHost):\(serverPort)" }
    
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
        guard let token = KeychainHelper.shared.getAPIToken(),
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
                    let saved = KeychainHelper.shared.saveAPIToken(token)
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
    
    /// Get authorization header for API requests
    private func getAuthHeader() -> String? {
        if let token = KeychainHelper.shared.getAPIToken() {
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
            error: nil
        )
        
        DispatchQueue.main.async {
            self.watchedFolders.append(folder)
            self.saveFolders()
        }
        
        // Start indexing
        startIndexing(path)
    }
    
    func removeFolder(_ folder: IndexedFolder) {
        // Stop watching
        stopWatching(folder.path)
        
        // Delete all nodes from NornicDB in background
        Task {
            do {
                try await deleteNodesForFolder(folder.path)
            } catch {
                print("⚠️  Failed to delete nodes for folder \(folder.path): \(error.localizedDescription)")
                // Continue with removal even if DB cleanup fails
            }
        }
        
        // Remove from list
        DispatchQueue.main.async {
            self.watchedFolders.removeAll { $0.id == folder.id }
            self.progressMap.removeValue(forKey: folder.path)
            self.saveFolders()
            self.updateStats()
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
            // Check if we can connect to server
            let canStoreInServer = connectionStatus == .connected || connectionStatus == .authenticated
            
            let files = await indexerService.indexFolder(at: path, recursive: true)
            
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
                        try await storeFileInNornicDB(file, folderPath: path)
                        storedCount += 1
                    } catch {
                        print("Failed to store file \(file.relativePath): \(error)")
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
                    
                    if finalErrorCount > 0 && !canStoreInServer {
                        self.watchedFolders[index].error = "Server not connected - files indexed locally only"
                    } else if finalErrorCount > 0 {
                        self.watchedFolders[index].error = "\(finalErrorCount) files failed to index"
                    } else {
                        self.watchedFolders[index].error = nil
                    }
                }
                
                self.saveFolders()
                self.updateStats()
                
                // Start watching for changes
                self.startWatching(path)
                
                // Fetch updated stats from server
                if canStoreInServer {
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
    
    /// Store an indexed file in NornicDB
    func storeFileInNornicDB(_ file: FileIndexer.IndexedFile, folderPath: String) async throws {
        // Check if node exists and is already up-to-date
        let checkQuery = """
        MATCH (f:File {path: $path})
        RETURN f.last_modified as last_modified, f.indexed_date as indexed_date
        """
        
        let checkParams: [String: Any] = ["path": file.path]
        let checkBody: [String: Any] = [
            "statements": [["statement": checkQuery, "parameters": checkParams]]
        ]
        
        let checkData = try JSONSerialization.data(withJSONObject: checkBody)
        let (checkResponseData, _) = try await makeAuthenticatedRequest(to: "/db/neo4j/tx/commit", method: "POST", body: checkData)
        
        if let json = try? JSONSerialization.jsonObject(with: checkResponseData) as? [String: Any],
           let results = json["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           let row = data.first?["row"] as? [String],
           let _ = row.first,
           let indexedDate = row.last {
            // If indexed after file was last modified, skip
            let fileModifiedDate = ISO8601DateFormatter().string(from: file.lastModified)
            if indexedDate >= fileModifiedDate {
                print("⏭️  Skipping \(file.path) - already indexed and up-to-date")
                return
            }
        }
        
        // Create or update the File node via Cypher
        let query = """
        MERGE (f:File:Node {path: $path})
        ON CREATE SET f.id = 'file-' + toString(timestamp()) + '-' + substring(randomUUID(), 0, 8)
        SET 
            f.name = $name,
            f.extension = $extension,
            f.language = $language,
            f.size_bytes = $size_bytes,
            f.content = $content,
            f.type = $content_type,
            f.indexed_date = datetime(),
            f.folder_root = $folder_root,
            f.last_modified = $last_modified
        RETURN f.id as id
        """
        
        let params: [String: Any] = [
            "path": file.path,
            "name": (file.path as NSString).lastPathComponent,
            "extension": file.fileExtension,
            "language": file.language ?? "unknown",
            "size_bytes": file.size,
            "content": file.content,
            "content_type": file.contentType == .text ? "text" : 
                           file.contentType == .document ? "document" :
                           file.contentType == .imageOCR ? "image_ocr" : "image_description",
            "folder_root": folderPath,
            "last_modified": ISO8601DateFormatter().string(from: file.lastModified)
        ]
        
        let body: [String: Any] = [
            "statements": [
                ["statement": query, "parameters": params]
            ]
        ]
        
        let bodyData = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await makeAuthenticatedRequest(to: "/db/neo4j/tx/commit", method: "POST", body: bodyData)
        
        if response.statusCode != 200 {
            throw NSError(domain: "FileWatchManager", code: response.statusCode, 
                         userInfo: [NSLocalizedDescriptionKey: "Failed to store file in NornicDB"])
        }
    }
    
    /// Delete all file nodes and their chunks for a folder
    func deleteNodesForFolder(_ folderPath: String) async throws {
        // First, get count of nodes to be deleted for logging
        let countQuery = """
        MATCH (f:File {folder_root: $folder_root})
        OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c)
        RETURN count(DISTINCT f) as fileCount, count(DISTINCT c) as chunkCount
        """
        
        let countBody: [String: Any] = [
            "statements": [
                ["statement": countQuery, "parameters": ["folder_root": folderPath]]
            ]
        ]
        
        let countData = try JSONSerialization.data(withJSONObject: countBody)
        let (countResponseData, _) = try await makeAuthenticatedRequest(to: "/db/neo4j/tx/commit", method: "POST", body: countData)
        
        var fileCount = 0
        var chunkCount = 0
        if let json = try? JSONSerialization.jsonObject(with: countResponseData) as? [String: Any],
           let results = json["results"] as? [[String: Any]],
           let data = results.first?["data"] as? [[String: Any]],
           let row = data.first?["row"] as? [Int] {
            fileCount = row[0]
            chunkCount = row[1]
        }
        
        print("🗑️  Deleting \(fileCount) File nodes and \(chunkCount) FileChunk nodes for folder: \(folderPath)")
        
        // Delete all File nodes and their associated FileChunk nodes
        // DETACH DELETE on File removes HAS_CHUNK relationships
        // Then explicitly delete orphaned FileChunk nodes
        let deleteQuery = """
        MATCH (f:File {folder_root: $folder_root})
        OPTIONAL MATCH (f)-[:HAS_CHUNK]->(c)
        WITH f, collect(c) as chunks
        DETACH DELETE f
        FOREACH (chunk IN chunks | DETACH DELETE chunk)
        """
        
        let deleteBody: [String: Any] = [
            "statements": [
                ["statement": deleteQuery, "parameters": ["folder_root": folderPath]]
            ]
        ]
        
        let deleteData = try JSONSerialization.data(withJSONObject: deleteBody)
        let (_, response) = try await makeAuthenticatedRequest(to: "/db/neo4j/tx/commit", method: "POST", body: deleteData)
        
        if response.statusCode == 200 {
            print("✅ Successfully deleted all nodes for folder: \(folderPath)")
        } else {
            throw NSError(domain: "FileWatchManager", code: response.statusCode,
                         userInfo: [NSLocalizedDescriptionKey: "Failed to delete nodes from NornicDB"])
        }
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
    @StateObject private var watchManager: FileWatchManager
    @State private var searchQuery: String = ""
    @State private var searchResults: [VectorSearchResult] = []
    @State private var isSearching: Bool = false
    @State private var showSearchSettings: Bool = false
    @State private var minSimilarity: Double = 0.0  // RRF scores are typically 0-0.1, not 0-1
    @State private var searchLimit: Int = 20
    @State private var authUsername: String = ""
    @State private var authPassword: String = ""
    @State private var isAuthenticating: Bool = false
    
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
        .padding()
        .background(folder.status.color.opacity(0.1))
        .cornerRadius(8)
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

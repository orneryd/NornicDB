import Foundation
import Network

/// EmbeddingServer provides an OpenAI-compatible HTTP API for text embeddings
/// using Apple's on-device ML models.
///
/// API Endpoint: POST /v1/embeddings
/// Compatible with OpenAI embeddings API format
///
/// Security: Requires Bearer token authentication matching the configured API key.
/// The API key should match NORNICDB_EMBEDDING_API_KEY environment variable.
///
/// Example usage:
/// ```bash
/// curl http://localhost:11435/v1/embeddings \
///   -H "Content-Type: application/json" \
///   -H "Authorization: Bearer your-api-key" \
///   -d '{"input": "Hello, world!", "model": "apple-ml-embeddings"}'
/// ```
class EmbeddingServer: ObservableObject {
    
    // MARK: - Published Properties
    
    @Published var isRunning = false
    @Published var port: UInt16 = 11435
    @Published var requestCount: Int = 0
    @Published var totalLatency: TimeInterval = 0.0
    @Published var lastError: String?
    @Published var requiresAuth: Bool = true  // Enable authentication by default
    
    // MARK: - Private Properties
    
    private var listener: NWListener?
    private var connections: [NWConnection] = []
    let embedder: AppleMLEmbedder
    private let queue = DispatchQueue(label: "com.nornicdb.embedding-server", qos: .userInitiated)
    
    /// API key for authentication. If set, requests must include Authorization: Bearer <key>
    private var apiKey: String?
    
    private func updatePublished(_ updates: @escaping () -> Void) {
        if Thread.isMainThread {
            updates()
        } else {
            DispatchQueue.main.async(execute: updates)
        }
    }
    
    // MARK: - Computed Properties
    
    var averageLatency: TimeInterval {
        guard requestCount > 0 else { return 0.0 }
        return totalLatency / Double(requestCount)
    }
    
    // MARK: - Initialization
    
    init() {
        self.embedder = AppleMLEmbedder()
        // API key must be set via setAPIKey() before starting the server
        // The menu bar app calls setAPIKey() with the key from Keychain
    }
    
    // MARK: - Configuration
    
    /// Set the API key for authentication.
    /// Requests must include "Authorization: Bearer <key>" header.
    /// MUST be called before start() for security.
    func setAPIKey(_ key: String?) {
        self.apiKey = key
        if let k = key, !k.isEmpty {
			print("🔐 Embedding server API key configured")
        } else {
            print("⚠️  Embedding server API key NOT configured - server will reject all requests!")
        }
    }
    
    /// Check if authentication is properly configured
    var isAuthConfigured: Bool {
        return apiKey != nil && !apiKey!.isEmpty
    }
    
    /// Validate request authentication.
    /// Returns nil if valid, or an error message if invalid.
    private func validateAuth(headers: [String: String]) -> String? {
        // Auth is disabled - allow all (for development/testing only)
        if !requiresAuth {
            return nil
        }
        
        // Auth is required but no key configured - DENY ALL (secure default)
        guard let expectedKey = apiKey, !expectedKey.isEmpty else {
            print("🔒 Auth DENIED: No API key configured on server")
            return "Server authentication not configured"
        }
        
        // Check for Authorization header
        guard let authHeader = headers["Authorization"] ?? headers["authorization"] else {
            return "Missing Authorization header"
        }
        
        // Expect "Bearer <key>" format
        let parts = authHeader.split(separator: " ", maxSplits: 1)
        guard parts.count == 2, parts[0].lowercased() == "bearer" else {
            return "Invalid Authorization header format (expected: Bearer <key>)"
        }
        
        let providedKey = String(parts[1])
        guard providedKey == expectedKey else {
            return "Invalid API key"
        }
        
        return nil // Valid
    }
    
    // MARK: - Server Control
    
    /// Start the embedding server
    func start() throws {
        guard !isRunning else {
            print("⚠️  Embedding server already running on port \(port)")
            return
        }
        
        // Check if embeddings are available
        guard AppleMLEmbedder.isAvailable() else {
            let error = "Apple ML embeddings not available on this system"
            updatePublished {
                self.lastError = error
            }
            throw EmbeddingServerError.embeddingsNotAvailable
        }
        
        // Create listener bound to localhost only (127.0.0.1)
        // This ensures the embedding server is only accessible locally for security
        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true
        
        // Bind to localhost only - not all interfaces
        let localhost = NWEndpoint.Host("127.0.0.1")
        parameters.requiredLocalEndpoint = NWEndpoint.hostPort(host: localhost, port: NWEndpoint.Port(integerLiteral: port))
        
        guard let listener = try? NWListener(using: parameters) else {
            let error = "Failed to create listener on 127.0.0.1:\(port)"
            updatePublished {
                self.lastError = error
            }
            throw EmbeddingServerError.failedToStart(error)
        }
        
        self.listener = listener
        
        // Set up listener handlers
        listener.stateUpdateHandler = { [weak self] state in
            self?.handleListenerState(state)
        }
        
        listener.newConnectionHandler = { [weak self] connection in
            self?.handleNewConnection(connection)
        }
        
        // Start listening
        listener.start(queue: queue)
        
        print("🚀 Embedding server starting on 127.0.0.1:\(port)...")
    }
    
    /// Stop the embedding server
    func stop() {
        guard isRunning else { return }
        
        // Close all connections
        for connection in connections {
            connection.cancel()
        }
        connections.removeAll()
        
        // Stop listener
        listener?.cancel()
        listener = nil
        
        updatePublished {
            self.isRunning = false
        }
        print("🛑 Embedding server stopped")
    }
    
    // MARK: - Connection Handling
    
    private func handleListenerState(_ state: NWListener.State) {
        switch state {
        case .ready:
            updatePublished {
                self.isRunning = true
                self.lastError = nil
            }
            print("✅ Embedding server ready on port \(port)")
            
        case .failed(let error):
            updatePublished {
                self.isRunning = false
                self.lastError = error.localizedDescription
            }
            print("❌ Embedding server failed: \(error)")
            
        case .cancelled:
            updatePublished {
                self.isRunning = false
            }
            print("🛑 Embedding server cancelled")
            
        default:
            break
        }
    }
    
    private func handleNewConnection(_ connection: NWConnection) {
        connections.append(connection)
        
        connection.stateUpdateHandler = { [weak self] state in
            if case .failed(_) = state {
                self?.removeConnection(connection)
            } else if case .cancelled = state {
                self?.removeConnection(connection)
            }
        }
        
        connection.start(queue: queue)
        receiveRequest(on: connection)
    }
    
    private func removeConnection(_ connection: NWConnection) {
        connections.removeAll { $0 === connection }
    }
    
    // MARK: - Request/Response Handling
    
    private var connectionBuffers: [ObjectIdentifier: Data] = [:]
    
    private func receiveRequest(on connection: NWConnection) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, error in
            guard let self = self else { return }
            
            let connectionId = ObjectIdentifier(connection)
            
            if let data = data, !data.isEmpty {
                // Append to buffer
                if self.connectionBuffers[connectionId] == nil {
                    self.connectionBuffers[connectionId] = Data()
                }
                self.connectionBuffers[connectionId]?.append(data)
                
                // Check if we have a complete HTTP request (look for \r\n\r\n in body)
                if let bufferedData = self.connectionBuffers[connectionId],
                   let str = String(data: bufferedData, encoding: .utf8),
                   str.contains("\r\n\r\n") {
                    // Check if body is complete by looking for Content-Length
                    if self.isRequestComplete(str) {
                        self.connectionBuffers.removeValue(forKey: connectionId)
                        self.handleRequest(data: bufferedData, connection: connection)
                        return
                    }
                }
            }
            
            if isComplete {
                // Connection closed, process whatever we have
                if let bufferedData = self.connectionBuffers[connectionId], !bufferedData.isEmpty {
                    self.connectionBuffers.removeValue(forKey: connectionId)
                    self.handleRequest(data: bufferedData, connection: connection)
                } else {
                    connection.cancel()
                }
            } else if error == nil {
                self.receiveRequest(on: connection)
            }
        }
    }
    
    private func isRequestComplete(_ request: String) -> Bool {
        // Find header/body separator
        guard let separatorRange = request.range(of: "\r\n\r\n") else {
            return false
        }
        
        let headerPart = String(request[..<separatorRange.lowerBound])
        let bodyPart = String(request[separatorRange.upperBound...])
        
        // Parse Content-Length
        let lines = headerPart.components(separatedBy: "\r\n")
        for line in lines {
            let lower = line.lowercased()
            if lower.starts(with: "content-length:") {
                if let lengthStr = line.split(separator: ":").last?.trimmingCharacters(in: .whitespaces),
                   let expectedLength = Int(lengthStr) {
                    // Check if we have enough body data
                    return bodyPart.utf8.count >= expectedLength
                }
            }
        }
        
        // No Content-Length header, assume complete after headers
        return true
    }
    
    private func handleRequest(data: Data, connection: NWConnection) {
        let startTime = Date()
        
        // Parse HTTP request
        guard let request = parseHTTPRequest(data) else {
            sendErrorResponse(connection: connection, statusCode: 400, message: "Invalid HTTP request")
            return
        }
        
        // Debug logging for route matching
        print("📥 Request: \(request.method) '\(request.path)'")
        
        // Route request - normalize path by stripping query string
        let normalizedPath = request.path.components(separatedBy: "?").first ?? request.path
        
        // Health check is always allowed without auth (for monitoring)
        if request.method == "GET" && request.path == "/health" {
            handleHealthCheck(connection: connection)
            return
        }
        
        // Validate authentication for all other endpoints
        if let authError = validateAuth(headers: request.headers) {
            print("🔒 Auth failed: \(authError)")
            sendErrorResponse(connection: connection, statusCode: 401, message: authError)
            return
        }
        
        if request.method == "POST" && (normalizedPath == "/v1/embeddings" || normalizedPath == "/embeddings") {
            handleEmbeddingsRequest(request: request, connection: connection, startTime: startTime)
        } else if request.method == "GET" && request.path == "/" {
            handleRootRequest(connection: connection)
        } else {
            sendErrorResponse(connection: connection, statusCode: 404, message: "Not found")
        }
    }
    
    private func handleEmbeddingsRequest(request: HTTPRequest, connection: NWConnection, startTime: Date) {
        // Parse request body
        guard let body = request.body else {
            sendErrorResponse(connection: connection, statusCode: 400, message: "Missing request body")
            return
        }
        
        guard let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else {
            sendErrorResponse(connection: connection, statusCode: 400, message: "Invalid JSON body")
            return
        }
        
        // Extract input
        let input: [String]
        if let singleInput = json["input"] as? String {
            input = [singleInput]
        } else if let multiInput = json["input"] as? [String] {
            input = multiInput
        } else {
            sendErrorResponse(connection: connection, statusCode: 400, message: "Missing or invalid 'input' field")
            return
        }
        
        // Generate embeddings
        do {
            let embeddings = try embedder.embedBatch(texts: input)
            
            // Build OpenAI-compatible response
            let data = embeddings.enumerated().map { index, embedding in
                [
                    "object": "embedding",
                    "embedding": embedding,
                    "index": index
                ] as [String: Any]
            }
            
            let totalTokens = input.reduce(0) { $0 + embedder.estimateTokenCount(text: $1) }
            
            let response: [String: Any] = [
                "object": "list",
                "data": data,
                "model": embedder.model,
                "usage": [
                    "prompt_tokens": totalTokens,
                    "total_tokens": totalTokens
                ]
            ]
            
            sendJSONResponse(connection: connection, statusCode: 200, json: response)
            
            // Update stats
            let latency = Date().timeIntervalSince(startTime)
            updatePublished {
                self.requestCount += 1
                self.totalLatency += latency
            }
            
        } catch {
            sendErrorResponse(connection: connection, statusCode: 500, message: "Embedding generation failed: \(error.localizedDescription)")
        }
    }
    
    private func handleHealthCheck(connection: NWConnection) {
        let response: [String: Any] = [
            "status": "ok",
            "model": embedder.model,
            "dimensions": embedder.dimensions,
            "requests_served": requestCount,
            "average_latency_ms": averageLatency * 1000
        ]
        sendJSONResponse(connection: connection, statusCode: 200, json: response)
    }
    
    private func handleRootRequest(connection: NWConnection) {
        let response: [String: Any] = [
            "name": "NornicDB Embedding Server",
            "version": "1.0.0",
            "description": "OpenAI-compatible embeddings API using Apple ML",
            "model": embedder.model,
            "dimensions": embedder.dimensions,
            "endpoints": [
                "POST /v1/embeddings": "Generate text embeddings",
                "GET /health": "Health check",
                "GET /": "API information"
            ]
        ]
        sendJSONResponse(connection: connection, statusCode: 200, json: response)
    }
    
    // MARK: - HTTP Utilities
    
    private func parseHTTPRequest(_ data: Data) -> HTTPRequest? {
        guard let requestString = String(data: data, encoding: .utf8) else {
            return nil
        }
        
        // Find the header/body separator (\r\n\r\n)
        // This is more reliable than splitting by lines
        let separator = "\r\n\r\n"
        
        guard let separatorRange = requestString.range(of: separator) else {
            // No body separator found - try with just \n\n as fallback
            if let fallbackRange = requestString.range(of: "\n\n") {
                return parseWithSeparator(requestString, separatorRange: fallbackRange)
            }
            // Headers only, no body
            return parseHeadersOnly(requestString)
        }
        
        return parseWithSeparator(requestString, separatorRange: separatorRange)
    }
    
    private func parseHeadersOnly(_ requestString: String) -> HTTPRequest? {
        let lines = requestString.components(separatedBy: "\r\n")
        guard let requestLine = lines.first else { return nil }
        
        let parts = requestLine.components(separatedBy: " ")
        guard parts.count >= 2 else { return nil }
        
        // Parse headers from remaining lines
        var headers: [String: String] = [:]
        for i in 1..<lines.count {
            let line = lines[i]
            if line.isEmpty { break }
            if let colonIndex = line.firstIndex(of: ":") {
                let key = String(line[..<colonIndex]).trimmingCharacters(in: .whitespaces)
                let value = String(line[line.index(after: colonIndex)...]).trimmingCharacters(in: .whitespaces)
                headers[key] = value
            }
        }
        
        return HTTPRequest(method: parts[0], path: parts[1], headers: headers, body: nil)
    }
    
    private func parseWithSeparator(_ requestString: String, separatorRange: Range<String.Index>) -> HTTPRequest? {
        let headerPart = String(requestString[..<separatorRange.lowerBound])
        let bodyPart = String(requestString[separatorRange.upperBound...])
        
        // Parse request line and headers
        let lines = headerPart.components(separatedBy: "\r\n")
        guard let requestLine = lines.first else { return nil }
        
        let parts = requestLine.components(separatedBy: " ")
        guard parts.count >= 2 else { return nil }
        
        let method = parts[0]
        let path = parts[1]
        
        // Parse headers from remaining lines
        var headers: [String: String] = [:]
        for i in 1..<lines.count {
            let line = lines[i]
            if line.isEmpty { break }
            if let colonIndex = line.firstIndex(of: ":") {
                let key = String(line[..<colonIndex]).trimmingCharacters(in: .whitespaces)
                let value = String(line[line.index(after: colonIndex)...]).trimmingCharacters(in: .whitespaces)
                headers[key] = value
            }
        }
        
        // Get body if present
        let body: Data?
        if !bodyPart.isEmpty {
            body = bodyPart.data(using: .utf8)
        } else {
            body = nil
        }
        
        return HTTPRequest(method: method, path: path, headers: headers, body: body)
    }
    
    private func sendJSONResponse(connection: NWConnection, statusCode: Int, json: [String: Any]) {
        guard let jsonData = try? JSONSerialization.data(withJSONObject: json, options: .prettyPrinted) else {
            sendErrorResponse(connection: connection, statusCode: 500, message: "Failed to serialize JSON")
            return
        }
        
        let statusText = HTTPStatusText(statusCode: statusCode)
        let response = """
        HTTP/1.1 \(statusCode) \(statusText)\r
        Content-Type: application/json\r
        Content-Length: \(jsonData.count)\r
        Access-Control-Allow-Origin: *\r
        Connection: close\r
        \r
        
        """
        
        var responseData = response.data(using: .utf8)!
        responseData.append(jsonData)
        
        connection.send(content: responseData, completion: .contentProcessed { error in
            if let error = error {
                print("❌ Send error: \(error)")
            }
            connection.cancel()
        })
    }
    
    private func sendErrorResponse(connection: NWConnection, statusCode: Int, message: String) {
        let json: [String: Any] = [
            "error": [
                "message": message,
                "type": "invalid_request_error",
                "code": statusCode
            ]
        ]
        sendJSONResponse(connection: connection, statusCode: statusCode, json: json)
    }
    
    private func HTTPStatusText(statusCode: Int) -> String {
        switch statusCode {
        case 200: return "OK"
        case 400: return "Bad Request"
        case 404: return "Not Found"
        case 500: return "Internal Server Error"
        default: return "Unknown"
        }
    }
}

// MARK: - Supporting Types

struct HTTPRequest {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data?
}

enum EmbeddingServerError: Error, LocalizedError {
    case embeddingsNotAvailable
    case failedToStart(String)
    
    var errorDescription: String? {
        switch self {
        case .embeddingsNotAvailable:
            return "Apple ML embeddings are not available on this system"
        case .failedToStart(let reason):
            return "Failed to start server: \(reason)"
        }
    }
}

// MARK: - Server Configuration

extension EmbeddingServer {
    /// Load configuration from UserDefaults
    func loadConfiguration() {
        if let savedPort = UserDefaults.standard.object(forKey: "embedding_server_port") as? Int {
            self.port = UInt16(savedPort)
        }
    }
    
    /// Save configuration to UserDefaults
    func saveConfiguration() {
        UserDefaults.standard.set(Int(port), forKey: "embedding_server_port")
    }
    
    /// Reset statistics
    func resetStatistics() {
        updatePublished {
            self.requestCount = 0
            self.totalLatency = 0.0
        }
    }
}

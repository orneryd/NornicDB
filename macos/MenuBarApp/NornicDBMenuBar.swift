import SwiftUI
import AppKit
import Foundation
import Security
import NaturalLanguage

// MARK: - Apple NLEmbedding HTTP Service

/// Provides a local HTTP server that exposes Apple's NLEmbedding to the NornicDB server.
/// Listens on port 7475 and provides /embed, /embed/batch, and /health endpoints.
class AppleEmbeddingService {
    static let shared = AppleEmbeddingService()
    
    private var server: HTTPServer?
    private let port: UInt16 = 7475
    private let embedding: NLEmbedding?
    
    private init() {
        // Load English sentence embedding model
        embedding = NLEmbedding.sentenceEmbedding(for: .english)
        if embedding == nil {
            print("⚠️ Apple NLEmbedding not available on this system")
        }
    }
    
    /// Start the embedding service
    func start() {
        guard embedding != nil else {
            print("❌ Cannot start embedding service: NLEmbedding not available")
            return
        }
        
        server = HTTPServer(port: port)
        server?.addHandler(path: "/health") { _ in
            return HTTPResponse(
                statusCode: 200,
                headers: ["Content-Type": "application/json"],
                body: "{\"status\":\"ok\",\"provider\":\"apple\",\"model\":\"NLEmbedding\"}"
            )
        }
        
        server?.addHandler(path: "/embed") { [weak self] request in
            return self?.handleEmbed(request) ?? HTTPResponse(statusCode: 500, body: "Service unavailable")
        }
        
        server?.addHandler(path: "/embed/batch") { [weak self] request in
            return self?.handleEmbedBatch(request) ?? HTTPResponse(statusCode: 500, body: "Service unavailable")
        }
        
        server?.start()
        print("✅ Apple Embedding Service started on port \(port)")
    }
    
    /// Stop the embedding service
    func stop() {
        server?.stop()
        server = nil
        print("⏹️ Apple Embedding Service stopped")
    }
    
    /// Handle single embedding request
    private func handleEmbed(_ request: HTTPRequest) -> HTTPResponse {
        guard let embedding = self.embedding else {
            return HTTPResponse(statusCode: 503, body: "{\"error\":\"NLEmbedding not available\"}")
        }
        
        guard let body = request.body,
              let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let text = json["text"] as? String else {
            return HTTPResponse(statusCode: 400, body: "{\"error\":\"Missing 'text' field\"}")
        }
        
        guard let vector = embedding.vector(for: text) else {
            return HTTPResponse(statusCode: 500, body: "{\"error\":\"Failed to generate embedding\"}")
        }
        
        let response: [String: Any] = ["embedding": vector]
        guard let responseData = try? JSONSerialization.data(withJSONObject: response) else {
            return HTTPResponse(statusCode: 500, body: "{\"error\":\"Failed to serialize response\"}")
        }
        
        return HTTPResponse(
            statusCode: 200,
            headers: ["Content-Type": "application/json"],
            body: String(data: responseData, encoding: .utf8) ?? "{}"
        )
    }
    
    /// Handle batch embedding request
    private func handleEmbedBatch(_ request: HTTPRequest) -> HTTPResponse {
        guard let embedding = self.embedding else {
            return HTTPResponse(statusCode: 503, body: "{\"error\":\"NLEmbedding not available\"}")
        }
        
        guard let body = request.body,
              let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let texts = json["texts"] as? [String] else {
            return HTTPResponse(statusCode: 400, body: "{\"error\":\"Missing 'texts' field\"}")
        }
        
        var embeddings: [[Double]] = []
        for text in texts {
            if let vector = embedding.vector(for: text) {
                embeddings.append(vector)
            } else {
                embeddings.append([]) // Empty array for failed embeddings
            }
        }
        
        let response: [String: Any] = ["embeddings": embeddings]
        guard let responseData = try? JSONSerialization.data(withJSONObject: response) else {
            return HTTPResponse(statusCode: 500, body: "{\"error\":\"Failed to serialize response\"}")
        }
        
        return HTTPResponse(
            statusCode: 200,
            headers: ["Content-Type": "application/json"],
            body: String(data: responseData, encoding: .utf8) ?? "{}"
        )
    }
}

// MARK: - Simple HTTP Server (no external dependencies)

/// Minimal HTTP server for the embedding service
class HTTPServer {
    private var socket: Int32 = -1
    private let port: UInt16
    private var isRunning = false
    private var handlers: [String: (HTTPRequest) -> HTTPResponse] = [:]
    private let queue = DispatchQueue(label: "com.nornicdb.embedding-server", qos: .userInitiated)
    
    init(port: UInt16) {
        self.port = port
    }
    
    func addHandler(path: String, handler: @escaping (HTTPRequest) -> HTTPResponse) {
        handlers[path] = handler
    }
    
    func start() {
        queue.async { [weak self] in
            self?.runServer()
        }
    }
    
    func stop() {
        isRunning = false
        if socket >= 0 {
            close(socket)
            socket = -1
        }
    }
    
    private func runServer() {
        socket = Darwin.socket(AF_INET, SOCK_STREAM, 0)
        guard socket >= 0 else {
            print("❌ Failed to create socket")
            return
        }
        
        var opt: Int32 = 1
        setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &opt, socklen_t(MemoryLayout<Int32>.size))
        
        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        addr.sin_addr.s_addr = INADDR_ANY
        
        let bindResult = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                bind(socket, sockaddrPtr, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        
        guard bindResult >= 0 else {
            print("❌ Failed to bind to port \(port): \(String(cString: strerror(errno)))")
            close(socket)
            socket = -1
            return
        }
        
        guard listen(socket, 10) >= 0 else {
            print("❌ Failed to listen on socket")
            close(socket)
            socket = -1
            return
        }
        
        isRunning = true
        
        while isRunning {
            var clientAddr = sockaddr_in()
            var addrLen = socklen_t(MemoryLayout<sockaddr_in>.size)
            
            let clientSocket = withUnsafeMutablePointer(to: &clientAddr) { ptr in
                ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                    accept(socket, sockaddrPtr, &addrLen)
                }
            }
            
            guard clientSocket >= 0 else {
                continue
            }
            
            // Handle request in background
            DispatchQueue.global().async { [weak self] in
                self?.handleClient(clientSocket)
            }
        }
    }
    
    private func handleClient(_ clientSocket: Int32) {
        defer { close(clientSocket) }
        
        // Read request
        var buffer = [UInt8](repeating: 0, count: 65536)
        let bytesRead = recv(clientSocket, &buffer, buffer.count, 0)
        
        guard bytesRead > 0 else { return }
        
        let requestData = Data(buffer[0..<bytesRead])
        guard let requestString = String(data: requestData, encoding: .utf8) else { return }
        
        // Parse request
        let request = parseHTTPRequest(requestString, data: requestData)
        
        // Find handler
        let response: HTTPResponse
        if let handler = handlers[request.path] {
            response = handler(request)
        } else {
            response = HTTPResponse(statusCode: 404, body: "Not Found")
        }
        
        // Send response
        let responseString = "HTTP/1.1 \(response.statusCode) \(statusText(response.statusCode))\r\n" +
            response.headers.map { "\($0.key): \($0.value)" }.joined(separator: "\r\n") +
            "\r\nContent-Length: \(response.body.count)\r\n\r\n" +
            response.body
        
        _ = responseString.withCString { ptr in
            send(clientSocket, ptr, strlen(ptr), 0)
        }
    }
    
    private func parseHTTPRequest(_ string: String, data: Data) -> HTTPRequest {
        let lines = string.components(separatedBy: "\r\n")
        guard let firstLine = lines.first else {
            return HTTPRequest(method: "GET", path: "/", headers: [:], body: nil)
        }
        
        let parts = firstLine.components(separatedBy: " ")
        let method = parts.count > 0 ? parts[0] : "GET"
        let path = parts.count > 1 ? parts[1].components(separatedBy: "?")[0] : "/"
        
        var headers: [String: String] = [:]
        var bodyStartIndex: Int?
        
        for (index, line) in lines.enumerated() {
            if line.isEmpty {
                bodyStartIndex = index + 1
                break
            }
            if let colonIndex = line.firstIndex(of: ":") {
                let key = String(line[..<colonIndex]).trimmingCharacters(in: .whitespaces)
                let value = String(line[line.index(after: colonIndex)...]).trimmingCharacters(in: .whitespaces)
                headers[key] = value
            }
        }
        
        var body: Data?
        if let startIndex = bodyStartIndex, startIndex < lines.count {
            let bodyString = lines[startIndex...].joined(separator: "\r\n")
            body = bodyString.data(using: .utf8)
        }
        
        return HTTPRequest(method: method, path: path, headers: headers, body: body)
    }
    
    private func statusText(_ code: Int) -> String {
        switch code {
        case 200: return "OK"
        case 400: return "Bad Request"
        case 404: return "Not Found"
        case 500: return "Internal Server Error"
        case 503: return "Service Unavailable"
        default: return "Unknown"
        }
    }
}

struct HTTPRequest {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data?
}

struct HTTPResponse {
    let statusCode: Int
    var headers: [String: String] = ["Content-Type": "text/plain"]
    var body: String = ""
    
    init(statusCode: Int, headers: [String: String] = ["Content-Type": "application/json"], body: String = "") {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

// MARK: - Keychain Helper for Secure Token Storage

class KeychainHelper {
    static let shared = KeychainHelper()
    
    private let service = "com.nornicdb.menubar"
    private let apiTokenAccount = "api_token"
    
    private init() {}
    
    /// Save API token to Keychain
    func saveAPIToken(_ token: String) -> Bool {
        // Delete existing token first
        deleteAPIToken()
        
        guard let tokenData = token.data(using: .utf8) else { return false }
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: apiTokenAccount,
            kSecValueData as String: tokenData,
            kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlockedThisDeviceOnly
        ]
        
        let status = SecItemAdd(query as CFDictionary, nil)
        
        if status == errSecSuccess {
            print("✅ API token saved to Keychain")
            return true
        } else {
            print("❌ Failed to save API token to Keychain: \(status)")
            return false
        }
    }
    
    /// Retrieve API token from Keychain
    func getAPIToken() -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: apiTokenAccount,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        
        if status == errSecSuccess,
           let tokenData = result as? Data,
           let token = String(data: tokenData, encoding: .utf8) {
            return token
        }
        
        return nil
    }
    
    /// Delete API token from Keychain
    @discardableResult
    func deleteAPIToken() -> Bool {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: apiTokenAccount
        ]
        
        let status = SecItemDelete(query as CFDictionary)
        return status == errSecSuccess || status == errSecItemNotFound
    }
    
    /// Check if API token exists in Keychain
    func hasAPIToken() -> Bool {
        return getAPIToken() != nil
    }
}

@main
struct NornicDBMenuBarApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate
    
    var body: some Scene {
        // Use Settings scene instead of WindowGroup to avoid blank window
        // The menu bar app manages its own windows via AppDelegate
        Settings {
            EmptyView()
        }
    }
}

class AppDelegate: NSObject, NSApplicationDelegate, ObservableObject {
    private var statusItem: NSStatusItem!
    private var healthCheckTimer: Timer?
    @Published var serverStatus: ServerStatus = .unknown
    var configManager: ConfigManager = ConfigManager()
    private var settingsWindowController: NSWindowController?
    private var firstRunWindowController: NSWindowController?
    
    func applicationDidFinishLaunching(_ notification: Notification) {
        // Hide dock icon - we only want menu bar presence
        NSApp.setActivationPolicy(.accessory)
        
        // Load configuration
        configManager.loadConfig()
        
        // Start Apple Embedding Service (provides NLEmbedding via HTTP for the Go server)
        // This allows "apple" embedding provider to work without downloading models
        AppleEmbeddingService.shared.start()
        
        // Create menu bar item
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        
        if let button = statusItem.button {
            updateStatusIcon(for: .unknown)
            button.action = #selector(showMenu)
            button.sendAction(on: [.leftMouseUp, .rightMouseUp])
        }
        
        // Start health monitoring
        startHealthCheck()
        
        // Show first-run wizard if needed
        if configManager.isFirstRun() {
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                self.showFirstRunWizard()
            }
        }
    }
    
    func applicationWillTerminate(_ notification: Notification) {
        healthCheckTimer?.invalidate()
    }
    
    @objc func showMenu() {
        let menu = NSMenu()
        
        // Status header
        let statusText: String
        switch serverStatus {
        case .running:
            statusText = "🟢 Running"
        case .stopped:
            statusText = "🔴 Stopped"
        case .starting:
            statusText = "🟡 Starting..."
        case .unknown:
            statusText = "⚪️ Unknown"
        }
        menu.addItem(NSMenuItem(title: "NornicDB - \(statusText)", action: nil, keyEquivalent: ""))
        menu.addItem(NSMenuItem.separator())
        
        // Actions
        if serverStatus == .running {
            menu.addItem(NSMenuItem(title: "Open Web UI", action: #selector(openWebUI), keyEquivalent: "o"))
            menu.addItem(NSMenuItem(title: "Stop Server", action: #selector(stopServer), keyEquivalent: "s"))
        } else {
            menu.addItem(NSMenuItem(title: "Start Server", action: #selector(startServer), keyEquivalent: "s"))
        }
        
        menu.addItem(NSMenuItem(title: "Restart Server", action: #selector(restartServer), keyEquivalent: "r"))
        menu.addItem(NSMenuItem.separator())
        
        // Configuration
        menu.addItem(NSMenuItem(title: "Settings...", action: #selector(openSettings), keyEquivalent: ","))
        menu.addItem(NSMenuItem(title: "Open Config File", action: #selector(openConfig), keyEquivalent: ""))
        menu.addItem(NSMenuItem(title: "Show Logs", action: #selector(showLogs), keyEquivalent: "l"))
        menu.addItem(NSMenuItem.separator())
        
        // Code Intelligence
        menu.addItem(NSMenuItem(title: "Code Intelligence...", action: #selector(openCodeIntelligence), keyEquivalent: "i"))
        menu.addItem(NSMenuItem.separator())
        
        // Models
        menu.addItem(NSMenuItem(title: "Download Models", action: #selector(downloadModels), keyEquivalent: ""))
        menu.addItem(NSMenuItem(title: "Open Models Folder", action: #selector(openModelsFolder), keyEquivalent: ""))
        menu.addItem(NSMenuItem.separator())
        
        // Info
        menu.addItem(NSMenuItem(title: "About NornicDB", action: #selector(showAbout), keyEquivalent: ""))
        menu.addItem(NSMenuItem(title: "Check for Updates", action: #selector(checkUpdates), keyEquivalent: ""))
        menu.addItem(NSMenuItem.separator())
        
        // Quit
        menu.addItem(NSMenuItem(title: "Quit", action: #selector(quit), keyEquivalent: "q"))
        
        statusItem.menu = menu
        statusItem.button?.performClick(nil) // Show menu
        
        // Clear menu after it's dismissed
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
            self.statusItem.menu = nil
        }
    }
    
    private func startHealthCheck() {
        // Check immediately
        checkHealth()
        
        // Then check every 10 seconds
        healthCheckTimer = Timer.scheduledTimer(withTimeInterval: 10.0, repeats: true) { [weak self] _ in
            self?.checkHealth()
        }
    }
    
    private func checkHealth() {
        let url = URL(string: "http://localhost:7474/health")!
        
        URLSession.shared.dataTask(with: url) { [weak self] data, response, error in
            DispatchQueue.main.async {
                if let httpResponse = response as? HTTPURLResponse,
                   httpResponse.statusCode == 200 {
                    self?.updateStatus(.running)
                } else {
                    self?.updateStatus(.stopped)
                }
            }
        }.resume()
    }
    
    private func updateStatus(_ status: ServerStatus) {
        guard serverStatus != status else { return }
        serverStatus = status
        updateStatusIcon(for: status)
    }
    
    private func updateStatusIcon(for status: ServerStatus) {
        guard let button = statusItem.button else { return }
        
        // Create NornicDB logo-based icon with status color
        // Based on the 3-point nexus from the official logo
        let size = NSSize(width: 18, height: 18)
        let image = NSImage(size: size, flipped: false) { rect in
            let color: NSColor
            switch status {
            case .running:
                color = NSColor.systemGreen
            case .stopped:
                color = NSColor.systemRed
            case .starting:
                color = NSColor.systemYellow
            case .unknown:
                color = NSColor.systemGray
            }
            
            // Central nexus (inspired by the logo's golden core)
            let centerX: CGFloat = 9
            let centerY: CGFloat = 9
            
            // Outer ring
            color.setFill()
            let outerRing = NSBezierPath(ovalIn: NSRect(x: centerX - 4, y: centerY - 4, width: 8, height: 8))
            outerRing.fill()
            
            // Middle ring (darker)
            color.withAlphaComponent(0.6).setFill()
            let middleRing = NSBezierPath(ovalIn: NSRect(x: centerX - 2.5, y: centerY - 2.5, width: 5, height: 5))
            middleRing.fill()
            
            // Inner core (bright)
            color.setFill()
            let innerCore = NSBezierPath(ovalIn: NSRect(x: centerX - 1.5, y: centerY - 1.5, width: 3, height: 3))
            innerCore.fill()
            
            // Three destiny nodes (the 3 points of Norns)
            color.withAlphaComponent(0.8).setFill()
            
            // Top node (Urðr - Past)
            let topNode = NSBezierPath(ovalIn: NSRect(x: centerX - 1.5, y: 2, width: 3, height: 3))
            topNode.fill()
            
            // Bottom-left node (Verðandi - Present)
            let leftNode = NSBezierPath(ovalIn: NSRect(x: 3, y: 14, width: 3, height: 3))
            leftNode.fill()
            
            // Bottom-right node (Skuld - Future)
            let rightNode = NSBezierPath(ovalIn: NSRect(x: 12, y: 14, width: 3, height: 3))
            rightNode.fill()
            
            // Connecting threads (thin lines connecting nodes to center)
            color.withAlphaComponent(0.5).setStroke()
            
            let thread1 = NSBezierPath()
            thread1.lineWidth = 1.0
            thread1.move(to: NSPoint(x: centerX, y: 5))
            thread1.line(to: NSPoint(x: centerX, y: centerY - 4))
            thread1.stroke()
            
            let thread2 = NSBezierPath()
            thread2.lineWidth = 1.0
            thread2.move(to: NSPoint(x: 4.5, y: 15.5))
            thread2.line(to: NSPoint(x: centerX - 2.5, y: centerY + 2))
            thread2.stroke()
            
            let thread3 = NSBezierPath()
            thread3.lineWidth = 1.0
            thread3.move(to: NSPoint(x: 13.5, y: 15.5))
            thread3.line(to: NSPoint(x: centerX + 2.5, y: centerY + 2))
            thread3.stroke()
            
            return true
        }
        
        button.image = image
        button.title = ""
    }
    
    // MARK: - Actions
    
    @objc func openWebUI() {
        NSWorkspace.shared.open(URL(string: "http://localhost:7474")!)
    }
    
    @objc func startServer() {
        updateStatus(.starting)
        executeCommand("launchctl", args: ["start", "com.nornicdb.server"])
        DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
            self.checkHealth()
        }
    }
    
    @objc func stopServer() {
        executeCommand("launchctl", args: ["stop", "com.nornicdb.server"])
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
            self.updateStatus(.stopped)
        }
    }
    
    @objc func restartServer() {
        stopServer()
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
            self.startServer()
        }
    }
    
    @objc func openSettings() {
        if settingsWindowController == nil {
            let settingsView = SettingsView(config: configManager)
            let hostingController = NSHostingController(rootView: settingsView)
            
            let window = NSWindow(contentViewController: hostingController)
            window.title = "NornicDB Settings"
            window.setContentSize(NSSize(width: 550, height: 550))
            window.styleMask = [.titled, .closable]
            window.center()
            
            settingsWindowController = NSWindowController(window: window)
        }
        
        settingsWindowController?.showWindow(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
    
    func showFirstRunWizard() {
        let wizardView = FirstRunWizard(config: configManager) {
            self.firstRunWindowController?.window?.close()
            self.firstRunWindowController = nil
        }
        let hostingController = NSHostingController(rootView: wizardView)
        
        let window = NSWindow(contentViewController: hostingController)
        window.title = "Welcome to NornicDB"
        window.setContentSize(NSSize(width: 600, height: 500))
        window.styleMask = [.titled, .closable]
        window.center()
        
        firstRunWindowController = NSWindowController(window: window)
        firstRunWindowController?.showWindow(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
    
    @objc func openConfig() {
        let configPath = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath
        NSWorkspace.shared.open(URL(fileURLWithPath: configPath))
    }
    
    @objc func showLogs() {
        // Open both log files in Console.app for live viewing
        let stderrLog = "/usr/local/var/log/nornicdb/stderr.log"
        let stdoutLog = "/usr/local/var/log/nornicdb/stdout.log"
        
        // Try to open in Console.app first (native macOS log viewer)
        if let consoleApp = NSWorkspace.shared.urlForApplication(withBundleIdentifier: "com.apple.Console") {
            NSWorkspace.shared.open([URL(fileURLWithPath: stderrLog), URL(fileURLWithPath: stdoutLog)],
                                   withApplicationAt: consoleApp,
                                   configuration: NSWorkspace.OpenConfiguration())
        } else {
            // Fallback: open in default text editor
            NSWorkspace.shared.open(URL(fileURLWithPath: stderrLog))
            NSWorkspace.shared.open(URL(fileURLWithPath: stdoutLog))
        }
    }
    
    @objc func downloadModels() {
        let alert = NSAlert()
        alert.messageText = "Download Default Models"
        alert.informativeText = "This will download:\n• BGE-M3 embedding model (~400MB)\n• Qwen2.5-0.5B-Instruct model (~350MB)\n\nTotal: ~750MB\n\nDownloading from HuggingFace..."
        alert.alertStyle = .informational
        alert.addButton(withTitle: "Download")
        alert.addButton(withTitle: "Cancel")
        
        if alert.runModal() == .alertFirstButtonReturn {
            // Show progress indicator
            let progress = NSAlert()
            progress.messageText = "Downloading Models..."
            progress.informativeText = "This may take several minutes depending on your connection.\n\nCheck the console for progress."
            progress.alertStyle = .informational
            progress.addButton(withTitle: "OK")
            
            // Execute download script
            DispatchQueue.global(qos: .userInitiated).async {
                let task = Process()
                task.launchPath = "/bin/bash"
                task.arguments = ["-c", "cd /usr/local/var/nornicdb && curl -L -o models/bge-m3.gguf https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf && curl -L -o models/qwen2.5-0.5b-instruct.gguf https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf"]
                
                // Create models directory first
                try? FileManager.default.createDirectory(atPath: "/usr/local/var/nornicdb/models", withIntermediateDirectories: true, attributes: nil)
                
                task.launch()
                task.waitUntilExit()
                
                DispatchQueue.main.async {
                    let result = NSAlert()
                    if task.terminationStatus == 0 {
                        result.messageText = "Download Complete"
                        result.informativeText = "Models downloaded successfully!\n\nYou can now select them in Settings → Models tab."
                        result.alertStyle = .informational
                    } else {
                        result.messageText = "Download Failed"
                        result.informativeText = "Failed to download models. Please check your internet connection and try again."
                        result.alertStyle = .warning
                    }
                    result.addButton(withTitle: "OK")
                    result.runModal()
                    
                    // Refresh models list in config manager
                    self.configManager.scanModels()
                }
            }
            
            progress.runModal()
        }
    }
    
    @objc func openModelsFolder() {
        let modelsPath = "/usr/local/var/nornicdb/models"
        
        // Create directory if it doesn't exist
        try? FileManager.default.createDirectory(atPath: modelsPath, withIntermediateDirectories: true, attributes: nil)
        
        NSWorkspace.shared.open(URL(fileURLWithPath: modelsPath))
    }
    
    @objc func openCodeIntelligence() {
        FileIndexerWindowController.shared.showWindow()
    }
    
    @objc func showAbout() {
        let alert = NSAlert()
        alert.messageText = "NornicDB"
        alert.informativeText = "High-Performance Graph Database\n\nVersion: 1.0.0\nBuild: arm64"
        alert.alertStyle = .informational
        alert.addButton(withTitle: "OK")
        alert.addButton(withTitle: "Visit Website")
        
        let response = alert.runModal()
        if response == .alertSecondButtonReturn {
            NSWorkspace.shared.open(URL(string: "https://github.com/orneryd/nornicdb")!)
        }
    }
    
    @objc func checkUpdates() {
        NSWorkspace.shared.open(URL(string: "https://github.com/orneryd/nornicdb/releases")!)
    }
    
    @objc func quit() {
        NSApplication.shared.terminate(nil)
    }
    
    private func executeCommand(_ command: String, args: [String]) {
        let task = Process()
        task.launchPath = "/usr/bin/env"
        task.arguments = [command] + args
        task.launch()
    }
}

enum ServerStatus {
    case running
    case stopped
    case starting
    case unknown
}

// MARK: - Config Manager

class ConfigManager: ObservableObject {
    static let shared = ConfigManager()
    
    @Published var embeddingsEnabled: Bool = false
    @Published var kmeansEnabled: Bool = false
    @Published var autoTLPEnabled: Bool = false
    @Published var heimdallEnabled: Bool = false
    @Published var autoStartEnabled: Bool = true
    @Published var portNumber: String = "7687"
    @Published var hostAddress: String = "localhost"
    
    // Authentication settings
    @Published var adminUsername: String = "admin"
    @Published var adminPassword: String = "password"
    @Published var jwtSecret: String = ""
    
    // Encryption settings
    @Published var encryptionEnabled: Bool = false
    @Published var encryptionPassword: String = ""
    
    // API token is stored securely in Keychain via KeychainHelper
    // Use getAPIToken() and setAPIToken() methods instead of direct access
    
    // Embedding provider: "local" (GGUF model), "apple" (NLEmbedding), "ollama"
    @Published var embeddingProvider: String = "local"
    @Published var embeddingModel: String = "bge-m3.gguf"
    @Published var heimdallModel: String = "qwen2.5-0.5b-instruct.gguf"
    @Published var availableModels: [String] = []
    
    // Available embedding providers
    static let embeddingProviders = [
        ("apple", "Apple Built-in (No download required)"),
        ("local", "Local GGUF Model (High quality)"),
        ("openai", "OpenAI Server (External)")
    ]
    
    // Config path matches server's FindConfigFile priority: ~/.nornicdb/config.yaml
    private let configPath = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath
    private let firstRunPath = NSString(string: "~/.nornicdb/.first_run").expandingTildeInPath
    private let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
    private let modelsPath = "/usr/local/var/nornicdb/models"
    
    func isFirstRun() -> Bool {
        return FileManager.default.fileExists(atPath: firstRunPath)
    }
    
    func completeFirstRun() {
        try? FileManager.default.removeItem(atPath: firstRunPath)
    }
    
    func loadConfig() {
        // Scan available models first
        scanModels()
        
        guard let content = try? String(contentsOfFile: configPath, encoding: .utf8) else {
            print("Could not read config file at: \(configPath)")
            return
        }
        
        print("Loading config from: \(configPath)")
        
        // Parse YAML (simple string matching since we don't have a YAML parser)
        let lines = content.components(separatedBy: .newlines)
        let context = lines.joined(separator: "\n")
        
        // Load feature enabled flags
        if let embeddingSection = context.range(of: "embedding:.*?enabled:", options: .regularExpression) {
            let start = embeddingSection.upperBound
            if let value = extractBoolValue(from: context, after: start) {
                embeddingsEnabled = value
                print("Loaded embeddings enabled: \(value)")
            }
        }
        
        if let kmeansSection = context.range(of: "kmeans:.*?enabled:", options: .regularExpression) {
            let start = kmeansSection.upperBound
            if let value = extractBoolValue(from: context, after: start) {
                kmeansEnabled = value
                print("Loaded kmeans enabled: \(value)")
            }
        }
        
        if let tlpSection = context.range(of: "auto_tlp:.*?enabled:", options: .regularExpression) {
            let start = tlpSection.upperBound
            if let value = extractBoolValue(from: context, after: start) {
                autoTLPEnabled = value
                print("Loaded auto_tlp enabled: \(value)")
            }
        }
        
        if let heimdallSection = context.range(of: "heimdall:.*?enabled:", options: .regularExpression) {
            let start = heimdallSection.upperBound
            if let value = extractBoolValue(from: context, after: start) {
                heimdallEnabled = value
                print("Loaded heimdall enabled: \(value)")
            }
        }
        
        // Load embedding provider
        if let providerSection = context.range(of: "embedding:.*?provider:", options: .regularExpression) {
            let start = providerSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                embeddingProvider = value
                print("Loaded embedding provider: \(value)")
            }
        }
        
        // Load model selections
        if let embeddingModelSection = context.range(of: "embedding:.*?model:", options: .regularExpression) {
            let start = embeddingModelSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                embeddingModel = value
                print("Loaded embedding model: \(value)")
            }
        }
        
        if let heimdallModelSection = context.range(of: "heimdall:.*?model:", options: .regularExpression) {
            let start = heimdallModelSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                heimdallModel = value
                print("Loaded heimdall model: \(value)")
            }
        }
        
        // Load server settings (try both port and bolt_port)
        if let serverPortSection = context.range(of: "server:.*?bolt_port:", options: .regularExpression) {
            let start = serverPortSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                portNumber = value
                print("Loaded bolt_port: \(value)")
            }
        } else if let serverPortSection = context.range(of: "server:.*?port:", options: .regularExpression) {
            let start = serverPortSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                portNumber = value
                print("Loaded port: \(value)")
            }
        }
        
        if let serverHostSection = context.range(of: "server:.*?host:", options: .regularExpression) {
            let start = serverHostSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                hostAddress = value
                print("Loaded host: \(value)")
            }
        }
        
        // Load auth settings
        if let authSection = context.range(of: "auth:.*?username:", options: .regularExpression) {
            let start = authSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                adminUsername = value
                print("Loaded username: \(value)")
            }
        }
        
        if let authPasswordSection = context.range(of: "auth:.*?password:", options: .regularExpression) {
            let start = authPasswordSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                adminPassword = value
                print("Loaded password: [hidden]")
            }
        }
        
        if let jwtSection = context.range(of: "auth:.*?jwt_secret:", options: .regularExpression) {
            let start = jwtSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                jwtSecret = value
                print("✅ Loaded JWT secret: \(value.prefix(8))... (length: \(value.count))")
            } else {
                print("⚠️ JWT secret field found but value is empty")
            }
        }
        
        // Load encryption settings
        if let enabledMatch = context.range(of: "database:.*?encryption_enabled:\\s*(true|false)", options: .regularExpression) {
            let enabledValue = context[enabledMatch].contains("true")
            encryptionEnabled = enabledValue
            print("Loaded encryption enabled: \(encryptionEnabled)")
        }
        if let passwordSection = context.range(of: "database:.*?encryption_password:", options: .regularExpression) {
            let start = passwordSection.upperBound
            if let value = extractStringValue(from: context, after: start) {
                encryptionPassword = value
                print("✅ Loaded encryption password: \(value.prefix(8))... (length: \(value.count))")
            } else {
                print("⚠️ Encryption password field found but value is empty")
            }
        }
    }
    
    private func extractStringValue(from text: String, after index: String.Index) -> String? {
        let substring = text[index...]
        if let lineEnd = substring.firstIndex(of: "\n") {
            let value = substring[..<lineEnd]
                .trimmingCharacters(in: .whitespaces)
                .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            return value.isEmpty ? nil : value
        }
        return nil
    }
    
    func scanModels() {
        // Scan models directory for .gguf files
        guard let files = try? FileManager.default.contentsOfDirectory(atPath: modelsPath) else {
            availableModels = []
            return
        }
        
        availableModels = files.filter { $0.hasSuffix(".gguf") }.sorted()
    }
    
    private func extractBoolValue(from text: String, after index: String.Index) -> Bool? {
        let substring = text[index...]
        if let lineEnd = substring.firstIndex(of: "\n") {
            let value = substring[..<lineEnd].trimmingCharacters(in: .whitespaces)
            return value.lowercased() == "true"
        }
        return nil
    }
    
    func saveConfig() -> Bool {
        guard var content = try? String(contentsOfFile: configPath, encoding: .utf8) else {
            return false
        }
        
        // Ensure required sections exist
        content = ensureSectionExists(in: content, section: "auth", defaultContent: """
        
        auth:
          username: "admin"
          password: "password"
          jwt_secret: ""
        """)
        
        content = ensureSectionExists(in: content, section: "database", defaultContent: """
        
        database:
          encryption_enabled: false
          encryption_password: ""
        """)
        
        // Update each feature setting
        content = updateYAMLValue(in: content, section: "embedding", key: "enabled", value: embeddingsEnabled)
        content = updateYAMLValue(in: content, section: "kmeans", key: "enabled", value: kmeansEnabled)
        content = updateYAMLValue(in: content, section: "auto_tlp", key: "enabled", value: autoTLPEnabled)
        content = updateYAMLValue(in: content, section: "heimdall", key: "enabled", value: heimdallEnabled)
        
        // Update embedding provider
        content = updateYAMLStringValue(in: content, section: "embedding", key: "provider", value: embeddingProvider)
        
        // Update model selections
        content = updateYAMLStringValue(in: content, section: "embedding", key: "model", value: embeddingModel)
        content = updateYAMLStringValue(in: content, section: "heimdall", key: "model", value: heimdallModel)
        
        // Update server settings (update both port and bolt_port for compatibility)
        content = updateYAMLStringValue(in: content, section: "server", key: "port", value: portNumber)
        content = updateYAMLStringValue(in: content, section: "server", key: "bolt_port", value: portNumber)
        content = updateYAMLStringValue(in: content, section: "server", key: "host", value: hostAddress)
        
        // Update auth settings
        content = updateYAMLStringValue(in: content, section: "auth", key: "username", value: adminUsername)
        content = updateYAMLStringValue(in: content, section: "auth", key: "password", value: adminPassword)
        
        // Auto-generate JWT secret only if empty
        print("💾 Saving JWT secret - current value length: \(jwtSecret.count)")
        if jwtSecret.isEmpty {
            jwtSecret = generateRandomSecret()
            print("🔑 Auto-generated NEW JWT secret (was empty)")
        } else {
            print("✅ Preserving existing JWT secret: \(jwtSecret.prefix(8))...")
        }
        // Only save JWT secret if we have one
        if !jwtSecret.isEmpty {
            content = updateYAMLStringValue(in: content, section: "auth", key: "jwt_secret", value: jwtSecret)
        }
        
        // Update encryption settings
        content = updateYAMLValue(in: content, section: "database", key: "encryption_enabled", value: encryptionEnabled)
        // Always preserve encryption password - don't clear it when disabled
        print("💾 Saving encryption password - enabled: \(encryptionEnabled), current value length: \(encryptionPassword.count)")
        if encryptionEnabled && encryptionPassword.isEmpty {
            // Auto-generate encryption password only if enabling and empty
            encryptionPassword = generateRandomSecret()
            print("🔑 Auto-generated NEW encryption password (was empty, encryption enabled)")
        } else if !encryptionPassword.isEmpty {
            print("✅ Preserving existing encryption password: \(encryptionPassword.prefix(8))...")
        }
        // Save password regardless of enabled state (preserves existing passwords)
        if !encryptionPassword.isEmpty {
            content = updateYAMLStringValue(in: content, section: "database", key: "encryption_password", value: encryptionPassword)
        }
        
        // Write back
        do {
            try content.write(toFile: configPath, atomically: true, encoding: .utf8)
            
            // Update auto-start if needed
            updateAutoStart()
            
            return true
        } catch {
            print("Failed to write config: \(error)")
            return false
        }
    }
    
    private func updateAutoStart() {
        let launchAgentPath = self.launchAgentPath
        let isLoaded = FileManager.default.fileExists(atPath: launchAgentPath)
        
        if autoStartEnabled && !isLoaded {
            // Load launch agent
            let task = Process()
            task.launchPath = "/usr/bin/env"
            task.arguments = ["launchctl", "load", launchAgentPath]
            task.launch()
        } else if !autoStartEnabled && isLoaded {
            // Unload launch agent
            let task = Process()
            task.launchPath = "/usr/bin/env"
            task.arguments = ["launchctl", "unload", launchAgentPath]
            task.launch()
        }
    }
    
    private func ensureSectionExists(in content: String, section: String, defaultContent: String) -> String {
        // Check if section exists
        let pattern = "^\(section):"
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.anchorsMatchLines]) {
            let range = NSRange(content.startIndex..., in: content)
            if regex.firstMatch(in: content, options: [], range: range) != nil {
                return content // Section exists
            }
        }
        // Section doesn't exist, append it
        return content + defaultContent
    }
    
    private func updateYAMLStringValue(in content: String, section: String, key: String, value: String) -> String {
        var result = content
        let pattern = "(\(section):(?:[^\n]*\n)*?\\s+\(key):\\s*)(?:[^\n]*)"
        
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators]) {
            let range = NSRange(content.startIndex..., in: content)
            let replacement = "$1\(value)"
            result = regex.stringByReplacingMatches(in: content, options: [], range: range, withTemplate: replacement)
        }
        
        return result
    }
    
    private func updateYAMLValue(in content: String, section: String, key: String, value: Bool) -> String {
        var result = content
        let pattern = "(\(section):(?:[^\n]*\n)*?\\s+\(key):\\s*)(?:true|false)"
        
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators]) {
            let range = NSRange(content.startIndex..., in: content)
            let replacement = "$1\(value)"
            result = regex.stringByReplacingMatches(in: content, options: [], range: range, withTemplate: replacement)
        }
        
        return result
    }
    
    func generateRandomSecret() -> String {
        let characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return String((0..<32).map { _ in characters.randomElement()! })
    }
    
    // MARK: - Server API Communication
    
    /// Get the base URL for API calls
    var serverBaseURL: String {
        return "http://\(hostAddress):7474"
    }
    
    /// Get API token from Keychain
    private func getAPIToken() -> String? {
        return KeychainHelper.shared.getAPIToken()
    }
    
    /// Save API token to Keychain
    private func setAPIToken(_ token: String) {
        _ = KeychainHelper.shared.saveAPIToken(token)
    }
    
    /// Clear API token from Keychain
    private func clearAPIToken() {
        KeychainHelper.shared.deleteAPIToken()
    }
    
    /// Obtain an API token from the server using admin credentials
    func obtainAPIToken() async -> Bool {
        guard let url = URL(string: "\(serverBaseURL)/auth/token") else {
            print("Invalid server URL")
            return false
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = [
            "username": adminUsername,
            "password": adminPassword
        ]
        
        do {
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                print("Failed to obtain token: bad response")
                return false
            }
            
            if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
               let token = json["token"] as? String {
                setAPIToken(token)
                print("Successfully obtained API token and saved to Keychain")
                return true
            }
        } catch {
            print("Failed to obtain token: \(error)")
        }
        return false
    }
    
    /// Make an authenticated API request
    private func makeAuthenticatedRequest(to endpoint: String, method: String = "POST", retryOnAuthFailure: Bool = true) async -> (success: Bool, message: String, data: [String: Any]?) {
        // Ensure we have a token
        var token = getAPIToken()
        if token == nil {
            let gotToken = await obtainAPIToken()
            if !gotToken {
                return (false, "Failed to authenticate with server", nil)
            }
            token = getAPIToken()
        }
        
        guard let token = token,
              let url = URL(string: "\(serverBaseURL)\(endpoint)") else {
            return (false, "Invalid server URL or missing token", nil)
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            
            guard let httpResponse = response as? HTTPURLResponse else {
                return (false, "Invalid response", nil)
            }
            
            if httpResponse.statusCode == 200 {
                let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                let message = json?["message"] as? String ?? "Success"
                return (true, message, json)
            } else if (httpResponse.statusCode == 401 || httpResponse.statusCode == 403) && retryOnAuthFailure {
                // Token expired, clear it and retry
                clearAPIToken()
                let gotToken = await obtainAPIToken()
                if gotToken {
                    return await makeAuthenticatedRequest(to: endpoint, method: method, retryOnAuthFailure: false)
                }
                return (false, "Authentication failed", nil)
            } else {
                return (false, "Server returned status \(httpResponse.statusCode)", nil)
            }
        } catch {
            return (false, "Request failed: \(error.localizedDescription)", nil)
        }
    }
    
    /// Clear all embeddings (requires server restart or embed trigger after)
    func clearEmbeddings() async -> (success: Bool, message: String) {
        let result = await makeAuthenticatedRequest(to: "/nornicdb/embed/clear")
        return (result.success, result.message)
    }
    
    /// Trigger embedding regeneration for all nodes
    func triggerEmbeddings() async -> (success: Bool, message: String) {
        let result = await makeAuthenticatedRequest(to: "/nornicdb/embed/trigger")
        return (result.success, result.message)
    }
    
    /// Rebuild search indexes
    func rebuildSearchIndex() async -> (success: Bool, message: String) {
        let result = await makeAuthenticatedRequest(to: "/nornicdb/search/rebuild")
        return (result.success, result.message)
    }
    
    /// Get embedding stats
    func getEmbeddingStats() async -> (success: Bool, message: String, stats: [String: Any]?) {
        let result = await makeAuthenticatedRequest(to: "/nornicdb/embed/stats", method: "GET")
        return (result.success, result.message, result.data)
    }
    
    /// Full reset: clear embeddings, then trigger regeneration
    func resetEmbeddings() async -> (success: Bool, message: String) {
        // Step 1: Clear existing embeddings
        let clearResult = await clearEmbeddings()
        if !clearResult.success {
            return clearResult
        }
        
        // Step 2: Trigger regeneration
        let triggerResult = await triggerEmbeddings()
        if !triggerResult.success {
            return (false, "Cleared embeddings but failed to trigger regeneration: \(triggerResult.message)")
        }
        
        return (true, "Embeddings reset and regeneration started. This may take a while depending on data size.")
    }
    
    /// Check if we have a valid API token stored
    var hasStoredAPIToken: Bool {
        return KeychainHelper.shared.hasAPIToken()
    }
}

// MARK: - Settings View

struct SettingsView: View {
    @ObservedObject var config: ConfigManager
    @State private var showingSaveAlert = false
    @State private var saveSuccess = false
    @State private var selectedTab = 0
    
    // Track original values to detect changes
    @State private var originalEmbeddingsEnabled: Bool = false
    @State private var originalKmeansEnabled: Bool = false
    @State private var originalAutoTLPEnabled: Bool = false
    @State private var originalHeimdallEnabled: Bool = false
    @State private var originalAutoStartEnabled: Bool = true
    @State private var originalPortNumber: String = "7687"
    @State private var originalHostAddress: String = "localhost"
    @State private var originalEmbeddingProvider: String = "local"
    @State private var originalEmbeddingModel: String = "bge-m3.gguf"
    @State private var originalHeimdallModel: String = "qwen2.5-0.5b-instruct.gguf"
    @State private var originalAdminUsername: String = "admin"
    @State private var originalAdminPassword: String = "password"
    @State private var originalJWTSecret: String = ""
    @State private var originalEncryptionEnabled: Bool = false
    @State private var originalEncryptionPassword: String = ""
    
    // Progress tracking
    @State private var isSaving: Bool = false
    @State private var saveProgress: String = ""
    
    // Show/hide sensitive fields
    @State private var showEncryptionKey: Bool = false
    
    // Check if there are unsaved changes
    var hasChanges: Bool {
        return config.embeddingsEnabled != originalEmbeddingsEnabled ||
               config.kmeansEnabled != originalKmeansEnabled ||
               config.autoTLPEnabled != originalAutoTLPEnabled ||
               config.heimdallEnabled != originalHeimdallEnabled ||
               config.autoStartEnabled != originalAutoStartEnabled ||
               config.portNumber != originalPortNumber ||
               config.hostAddress != originalHostAddress ||
               config.embeddingProvider != originalEmbeddingProvider ||
               config.embeddingModel != originalEmbeddingModel ||
               config.heimdallModel != originalHeimdallModel ||
               config.adminUsername != originalAdminUsername ||
               config.adminPassword != originalAdminPassword ||
               config.jwtSecret != originalJWTSecret ||
               config.encryptionEnabled != originalEncryptionEnabled ||
               config.encryptionPassword != originalEncryptionPassword
    }
    
    var body: some View {
        VStack(spacing: 0) {
            // Tab selector
            Picker("", selection: $selectedTab) {
                Text("Features").tag(0)
                Text("Server").tag(1)
                Text("Models").tag(2)
                Text("Security").tag(3)
                Text("Startup").tag(4)
            }
            .pickerStyle(.segmented)
            .padding()
            
            Divider()
            
            // Tab content
            TabView(selection: $selectedTab) {
                featuresTab.tag(0)
                serverTab.tag(1)
                modelsTab.tag(2)
                securityTab.tag(3)
                startupTab.tag(4)
            }
            .tabViewStyle(.automatic)
            
            Divider()
            
            // Progress Indicator
            if isSaving {
                HStack {
                    ProgressView()
                        .scaleEffect(0.8)
                        .padding(.leading)
                    Text(saveProgress)
                        .font(.caption)
                        .foregroundColor(.secondary)
                    Spacer()
                }
                .padding(.horizontal)
                .padding(.vertical, 8)
                .background(Color.secondary.opacity(0.1))
                
                Divider()
            }
            
            // Action Buttons
            HStack {
                Button("Cancel") {
                    config.loadConfig()
                    NSApp.keyWindow?.close()
                }
                .keyboardShortcut(.cancelAction)
                .disabled(isSaving)
                
                Spacer()
                
                if hasChanges {
                    Text("Unsaved changes")
                        .font(.caption)
                        .foregroundColor(.orange)
                        .padding(.trailing, 8)
                }
                
                Button("Save & Restart") {
                    saveAndRestart()
                }
                .keyboardShortcut(.defaultAction)
                .buttonStyle(.borderedProminent)
                .disabled(!hasChanges || isSaving)
            }
            .padding()
        }
        .frame(width: 550, height: isSaving ? 580 : 550)
        .onAppear {
            captureOriginalValues()
        }
    }
    
    private func captureOriginalValues() {
        // Reload config from file to ensure we have the latest values
        config.loadConfig()
        
        // Capture current values as originals
        originalEmbeddingsEnabled = config.embeddingsEnabled
        originalKmeansEnabled = config.kmeansEnabled
        originalAutoTLPEnabled = config.autoTLPEnabled
        originalHeimdallEnabled = config.heimdallEnabled
        originalAutoStartEnabled = config.autoStartEnabled
        originalPortNumber = config.portNumber
        originalHostAddress = config.hostAddress
        originalEmbeddingProvider = config.embeddingProvider
        originalEmbeddingModel = config.embeddingModel
        originalHeimdallModel = config.heimdallModel
        originalAdminUsername = config.adminUsername
        originalAdminPassword = config.adminPassword
        originalJWTSecret = config.jwtSecret
        originalEncryptionEnabled = config.encryptionEnabled
        originalEncryptionPassword = config.encryptionPassword
    }
    
    private func saveAndRestart() {
        isSaving = true
        saveProgress = "Saving configuration..."
        
        DispatchQueue.global(qos: .userInitiated).async {
            let success = config.saveConfig()
            
            DispatchQueue.main.async {
                if success {
                    saveProgress = "Configuration saved. Restarting server..."
                    
                    // Restart the server
                    let task = Process()
                    task.launchPath = "/usr/bin/env"
                    task.arguments = ["launchctl", "kickstart", "-k", "gui/\(getuid())/com.nornicdb.server"]
                    task.launch()
                    
                    // Wait for restart
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        saveProgress = "Server restarted successfully!"
                        
                        // Close window after short delay
                        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                            isSaving = false
                            captureOriginalValues() // Update original values
                            NSApp.keyWindow?.close()
                        }
                    }
                } else {
                    saveProgress = "Failed to save configuration"
                    
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        isSaving = false
                        saveProgress = ""
                    }
                }
            }
        }
    }
    
    var featuresTab: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 15) {
                Text("AI & Analytics Features")
                    .font(.headline)
                    .padding(.bottom, 5)
                
                Text("Toggle advanced features. Changes require a server restart.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.bottom, 10)
                
                FeatureToggle(
                    title: "Embeddings",
                    description: "Vector embeddings for semantic search",
                    isEnabled: $config.embeddingsEnabled,
                    icon: "brain.head.profile"
                )
                
                FeatureToggle(
                    title: "K-Means Clustering",
                    description: "Automatic node clustering and organization",
                    isEnabled: $config.kmeansEnabled,
                    icon: "circle.hexagongrid.fill"
                )
                
                FeatureToggle(
                    title: "Auto-TLP",
                    description: "Automatic temporal link prediction",
                    isEnabled: $config.autoTLPEnabled,
                    icon: "clock.arrow.circlepath"
                )
                
                FeatureToggle(
                    title: "Heimdall",
                    description: "AI guardian with cognitive monitoring",
                    isEnabled: $config.heimdallEnabled,
                    icon: "eye.fill"
                )
            }
            .padding()
        }
    }
    
    var serverTab: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("Server Configuration")
                    .font(.headline)
                    .padding(.bottom, 5)
                
                Text("Configure server network settings.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.bottom, 10)
                
                // Port setting
                VStack(alignment: .leading, spacing: 8) {
                    Text("Port Number")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    Text("The port NornicDB listens on (default: 7687)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextField("7687", text: $config.portNumber)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 150)
                }
                .padding()
                .background(RoundedRectangle(cornerRadius: 8).fill(Color.gray.opacity(0.1)))
                
                // Host setting
                VStack(alignment: .leading, spacing: 8) {
                    Text("Host Address")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    Text("Interface to listen on (localhost = local only, 0.0.0.0 = all interfaces)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextField("localhost", text: $config.hostAddress)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 200)
                }
                .padding()
                .background(RoundedRectangle(cornerRadius: 8).fill(Color.gray.opacity(0.1)))
                
                Spacer()
            }
            .padding()
        }
    }
    
    var modelsTab: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("AI Models & Providers")
                    .font(.headline)
                    .padding(.bottom, 5)
                
                Text("Select embedding provider and models for AI features.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.bottom, 10)
                
                // Embedding Provider Selection
                VStack(alignment: .leading, spacing: 8) {
                    Text("Embedding Provider")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    
                    Picker("Provider", selection: $config.embeddingProvider) {
                        ForEach(ConfigManager.embeddingProviders, id: \.0) { provider in
                            Text(provider.1).tag(provider.0)
                        }
                    }
                    .pickerStyle(.menu)
                    .frame(maxWidth: 400)
                    
                    // Provider-specific info
                    Group {
                        if config.embeddingProvider == "apple" {
                            HStack(spacing: 8) {
                                Image(systemName: "apple.logo")
                                    .foregroundColor(.blue)
                                Text("Uses macOS built-in NLEmbedding. No model download required!")
                                    .font(.caption)
                                    .foregroundColor(.green)
                            }
                            .padding(.vertical, 4)
                        } else if config.embeddingProvider == "local" {
                            HStack(spacing: 8) {
                                Image(systemName: "cpu")
                                    .foregroundColor(.orange)
                                Text("Uses local GGUF model. Higher quality, requires model download.")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            .padding(.vertical, 4)
                        } else if config.embeddingProvider == "ollama" {
                            HStack(spacing: 8) {
                                Image(systemName: "server.rack")
                                    .foregroundColor(.purple)
                                Text("Requires Ollama server running locally.")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            .padding(.vertical, 4)
                        }
                    }
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                Divider()
                
                // Only show model selection if provider is "local"
                if config.embeddingProvider == "local" {
                    if config.availableModels.isEmpty {
                        VStack(spacing: 15) {
                            Text("⚠️ No models found")
                                .font(.title3)
                                .foregroundColor(.orange)
                            
                            Text("Download models from the menu:\nNornicDB → Download Models")
                                .font(.body)
                                .multilineTextAlignment(.center)
                                .foregroundColor(.secondary)
                            
                            Button("Refresh Models List") {
                                config.scanModels()
                            }
                            .padding(.top, 10)
                        }
                        .frame(maxWidth: .infinity)
                        .padding()
                    } else {
                        VStack(alignment: .leading, spacing: 20) {
                            // Embedding Model Selection
                            VStack(alignment: .leading, spacing: 8) {
                                Text("Embedding Model")
                                    .font(.subheadline)
                                    .fontWeight(.medium)
                                
                                Text("Used for semantic search and vector embeddings")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                                
                                Picker("Embedding Model", selection: $config.embeddingModel) {
                                    ForEach(config.availableModels, id: \.self) { model in
                                        Text(model).tag(model)
                                    }
                                }
                                .pickerStyle(.menu)
                                .frame(maxWidth: 400)
                            }
                        }
                    }
                }
                
                // Heimdall section (always show if models available)
                if !config.availableModels.isEmpty {
                    VStack(alignment: .leading, spacing: 20) {
                        
                        Divider()
                        
                        // Heimdall Model Selection
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Heimdall LLM Model")
                                .font(.subheadline)
                                .fontWeight(.medium)
                            
                            Text("Used for AI-powered monitoring and insights")
                                .font(.caption)
                                .foregroundColor(.secondary)
                            
                            Picker("Heimdall Model", selection: $config.heimdallModel) {
                                ForEach(config.availableModels, id: \.self) { model in
                                    Text(model).tag(model)
                                }
                            }
                            .pickerStyle(.menu)
                            .frame(maxWidth: 400)
                        }
                        
                        Divider()
                        
                        // Model Info
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Available Models (\(config.availableModels.count))")
                                .font(.subheadline)
                                .fontWeight(.medium)
                            
                            ForEach(config.availableModels, id: \.self) { model in
                                HStack {
                                    Image(systemName: "doc.fill")
                                        .foregroundColor(.blue)
                                    Text(model)
                                        .font(.caption)
                                    Spacer()
                                }
                                .padding(.leading, 10)
                            }
                            
                            Button("Refresh List") {
                                config.scanModels()
                            }
                            .padding(.top, 8)
                        }
                    }
                    .padding()
                }
                
                Divider()
                
                // Embedding Management Section
                VStack(alignment: .leading, spacing: 12) {
                    Text("Embedding Management")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    
                    Text("If you change embedding provider or model, existing embeddings may become incompatible. Use these tools to regenerate them.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    HStack(spacing: 12) {
                        Button(action: {
                            isResettingEmbeddings = true
                            resetEmbeddingsMessage = ""
                            Task {
                                let result = await config.resetEmbeddings()
                                await MainActor.run {
                                    isResettingEmbeddings = false
                                    resetEmbeddingsMessage = result.message
                                    resetEmbeddingsSuccess = result.success
                                }
                            }
                        }) {
                            HStack {
                                if isResettingEmbeddings {
                                    ProgressView()
                                        .scaleEffect(0.7)
                                } else {
                                    Image(systemName: "arrow.triangle.2.circlepath")
                                }
                                Text("Reset & Regenerate Embeddings")
                            }
                        }
                        .buttonStyle(.bordered)
                        .disabled(isResettingEmbeddings)
                        
                        Button(action: {
                            Task {
                                let result = await config.getEmbeddingStats()
                                await MainActor.run {
                                    if result.success, let stats = result.stats {
                                        let total = stats["total_nodes"] ?? 0
                                        let withEmbed = stats["nodes_with_embeddings"] ?? 0
                                        resetEmbeddingsMessage = "Stats: \(withEmbed)/\(total) nodes have embeddings"
                                        resetEmbeddingsSuccess = true
                                    } else {
                                        resetEmbeddingsMessage = result.message
                                        resetEmbeddingsSuccess = false
                                    }
                                }
                            }
                        }) {
                            HStack {
                                Image(systemName: "chart.bar")
                                Text("Get Stats")
                            }
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    if !resetEmbeddingsMessage.isEmpty {
                        HStack(spacing: 6) {
                            Image(systemName: resetEmbeddingsSuccess ? "checkmark.circle.fill" : "exclamationmark.circle.fill")
                                .foregroundColor(resetEmbeddingsSuccess ? .green : .orange)
                            Text(resetEmbeddingsMessage)
                                .font(.caption)
                                .foregroundColor(resetEmbeddingsSuccess ? .green : .orange)
                        }
                        .padding(.top, 4)
                    }
                    
                    if config.hasStoredAPIToken {
                        HStack(spacing: 6) {
                            Image(systemName: "key.fill")
                                .foregroundColor(.green)
                            Text("API token stored in Keychain")
                                .font(.caption2)
                                .foregroundColor(.secondary)
                        }
                        .padding(.top, 2)
                    }
                }
                .padding()
                .background(Color.orange.opacity(0.1))
                .cornerRadius(8)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding()
        }
    }
    
    // State for embedding reset
    @State private var isResettingEmbeddings = false
    @State private var resetEmbeddingsMessage = ""
    @State private var resetEmbeddingsSuccess = false
    
    var securityTab: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("Security Settings")
                    .font(.title2)
                    .bold()
                
                Text("Configure authentication and security for NornicDB")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                Divider()
                
                // Admin Credentials
                VStack(alignment: .leading, spacing: 15) {
                    Text("Admin Credentials")
                        .font(.headline)
                    
                    HStack {
                        Text("Username:")
                            .frame(width: 120, alignment: .trailing)
                        TextField("admin", text: $config.adminUsername)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    HStack {
                        Text("Password:")
                            .frame(width: 120, alignment: .trailing)
                        SecureField("Enter password", text: $config.adminPassword)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    if config.adminPassword.count < 8 && !config.adminPassword.isEmpty {
                        HStack {
                            Spacer().frame(width: 120)
                            Text("⚠️ Password must be at least 8 characters")
                                .font(.caption)
                                .foregroundColor(.orange)
                        }
                    }
                    
                    Text("💡 These credentials are used to access the NornicDB web UI and API")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 120)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                // JWT Secret
                VStack(alignment: .leading, spacing: 15) {
                    Text("JWT Secret")
                        .font(.headline)
                    
                    HStack {
                        Text("Secret:")
                            .frame(width: 120, alignment: .trailing)
                        SecureField("Auto-generated if empty", text: $config.jwtSecret)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    HStack {
                        Spacer().frame(width: 120)
                        Button("Generate Random Secret") {
                            config.jwtSecret = config.generateRandomSecret()
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    Text("💡 The JWT secret is used to sign authentication tokens. Leave empty for auto-generation, or set a consistent value for tokens to persist across restarts.")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 120)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                // Encryption
                VStack(alignment: .leading, spacing: 15) {
                    Text("Database Encryption")
                        .font(.headline)
                    
                    Toggle("Enable Encryption at Rest", isOn: $config.encryptionEnabled)
                    
                    if config.encryptionEnabled {
                        HStack {
                            Text("Encryption Key:")
                                .frame(width: 120, alignment: .trailing)
                            
                            if showEncryptionKey {
                                TextField("Enter encryption password", text: $config.encryptionPassword)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                    .frame(maxWidth: 200)
                                    .font(.system(.body, design: .monospaced))
                            } else {
                                SecureField("Enter encryption password", text: $config.encryptionPassword)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                    .frame(maxWidth: 200)
                            }
                            
                            Button(action: { showEncryptionKey.toggle() }) {
                                Image(systemName: showEncryptionKey ? "eye.slash" : "eye")
                            }
                            .buttonStyle(.borderless)
                            .help(showEncryptionKey ? "Hide key" : "Show key")
                            
                            Button(action: {
                                NSPasteboard.general.clearContents()
                                NSPasteboard.general.setString(config.encryptionPassword, forType: .string)
                            }) {
                                Image(systemName: "doc.on.doc")
                            }
                            .buttonStyle(.borderless)
                            .help("Copy to clipboard")
                            .disabled(config.encryptionPassword.isEmpty)
                        }
                        
                        HStack {
                            Spacer().frame(width: 120)
                            Button("Generate Strong Key") {
                                config.encryptionPassword = config.generateRandomSecret()
                                showEncryptionKey = true  // Show the newly generated key
                            }
                            .buttonStyle(.bordered)
                        }
                        
                        if config.encryptionPassword.count < 16 && !config.encryptionPassword.isEmpty {
                            HStack {
                                Spacer().frame(width: 120)
                                Text("⚠️ Encryption key should be at least 16 characters")
                                    .font(.caption)
                                    .foregroundColor(.orange)
                            }
                        }
                    }
                    
                    Text("⚠️ Enabling encryption will protect your data at rest. Keep your encryption password safe — data cannot be recovered without it!")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 0)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                Spacer()
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding()
        }
    }
    
    var startupTab: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("Startup Behavior")
                    .font(.headline)
                    .padding(.bottom, 5)
                
                Text("Configure how NornicDB starts on your Mac.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.bottom, 10)
                
                VStack(alignment: .leading, spacing: 12) {
                    Toggle(isOn: $config.autoStartEnabled) {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Start at Login")
                                .font(.headline)
                            Text("Automatically start NornicDB when you log in to your Mac")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }
                    .toggleStyle(.switch)
                }
                .padding()
                .background(RoundedRectangle(cornerRadius: 8).fill(Color.gray.opacity(0.1)))
                
                VStack(alignment: .leading, spacing: 12) {
                    Text("💡 Tips")
                        .font(.headline)
                    
                    VStack(alignment: .leading, spacing: 8) {
                        HStack(alignment: .top, spacing: 8) {
                            Text("•")
                            Text("Menu bar app will launch automatically with the server")
                                .font(.caption)
                        }
                        HStack(alignment: .top, spacing: 8) {
                            Text("•")
                            Text("Server restarts automatically if it crashes")
                                .font(.caption)
                        }
                        HStack(alignment: .top, spacing: 8) {
                            Text("•")
                            Text("Disable auto-start if you only need NornicDB occasionally")
                                .font(.caption)
                        }
                    }
                    .foregroundColor(.secondary)
                }
                .padding()
                .background(RoundedRectangle(cornerRadius: 8).fill(Color.blue.opacity(0.1)))
                
                Spacer()
            }
            .padding()
        }
    }
}

struct FeatureToggle: View {
    let title: String
    let description: String
    @Binding var isEnabled: Bool
    let icon: String
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: icon)
                .font(.system(size: 24))
                .foregroundColor(isEnabled ? .blue : .gray)
                .frame(width: 30)
            
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.headline)
                Text(description)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            Toggle("", isOn: $isEnabled)
                .labelsHidden()
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color.gray.opacity(0.1))
        )
    }
}

// MARK: - First Run Wizard

struct FirstRunWizard: View {
    @ObservedObject var config: ConfigManager
    @State private var currentStep = 0
    @State private var selectedPreset: ConfigPreset = .standard  // Default to recommended
    let onComplete: () -> Void
    
    @State private var isDownloadingModels: Bool = false
    @State private var downloadProgress: String = ""
    @State private var bgeModelExists: Bool = false
    @State private var qwenModelExists: Bool = false
    @State private var serverIsRunning: Bool = false
    @State private var isSaving: Bool = false
    @State private var saveProgress: String = ""
    @State private var showEncryptionKey: Bool = false
    
    var body: some View {
        VStack(spacing: 0) {
            // Header
            VStack(spacing: 12) {
                Image(systemName: "database.fill")
                    .font(.system(size: 48))
                    .foregroundColor(.blue)
                
                Text("Welcome to NornicDB!")
                    .font(.title)
                    .fontWeight(.bold)
                
                Text("Let's set up your graph database")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }
            .padding(.top, 30)
            .padding(.bottom, 20)
            
            Divider()
            
            // Step Indicators
            HStack(spacing: 12) {
                ForEach(0..<4) { step in
                    HStack(spacing: 8) {
                        ZStack {
                            Circle()
                                .fill(currentStep >= step ? Color.blue : Color.gray.opacity(0.3))
                                .frame(width: 32, height: 32)
                            
                            if currentStep > step {
                                Image(systemName: "checkmark")
                                    .foregroundColor(.white)
                                    .font(.system(size: 14, weight: .bold))
                            } else {
                                Text("\(step + 1)")
                                    .foregroundColor(currentStep >= step ? .white : .gray)
                                    .font(.system(size: 14, weight: .semibold))
                            }
                        }
                        
                        Text(stepLabel(for: step))
                            .font(.subheadline)
                            .fontWeight(currentStep == step ? .semibold : .regular)
                            .foregroundColor(currentStep >= step ? .primary : .secondary)
                            .fixedSize(horizontal: true, vertical: false)  // Prevent text wrapping
                    }
                    .fixedSize(horizontal: true, vertical: false)  // Keep HStack inline
                    
                    if step < 3 {
                        Rectangle()
                            .fill(currentStep > step ? Color.blue : Color.gray.opacity(0.3))
                            .frame(height: 2)
                            .frame(maxWidth: .infinity)
                    }
                }
            }
            .padding(.horizontal, 40)
            .padding(.vertical, 20)
            
            Divider()
            
            // Step content
            TabView(selection: $currentStep) {
                welcomeStep.tag(0)
                presetStep.tag(1)
                securityStep.tag(2)
                confirmStep.tag(3)
            }
            .tabViewStyle(.automatic)
            .onChange(of: currentStep) { newStep in
                // Refresh model status when navigating to review step
                if newStep == 3 {
                    checkModelFiles()
                }
            }
            
            Divider()
            
            // Navigation
            HStack {
                if currentStep > 0 {
                    Button("Back") {
                        withAnimation {
                            currentStep -= 1
                        }
                    }
                } else {
                    Spacer()
                }
                
                Spacer()
                
                if currentStep < 3 {
                    Button("Next") {
                        withAnimation {
                            currentStep += 1
                        }
                    }
                    .buttonStyle(.borderedProminent)
                } else {
                    Button(serverIsRunning ? "Save & Restart Server" : "Save & Start Server") {
                        saveAndStartServer()
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(isDownloadingModels)
                }
            }
            .padding()
        }
        .frame(width: 750, height: 688)  // 25% larger (600*1.25=750, 550*1.25=688)
        .onAppear {
            // Load existing config values first (preserves user's settings)
            print("🎨 Wizard onAppear - loading config...")
            print("  Before load - JWT: \(config.jwtSecret.count) chars, Encryption: \(config.encryptionPassword.count) chars")
            config.loadConfig()
            print("  After load - JWT: \(config.jwtSecret.count) chars, Encryption: \(config.encryptionPassword.count) chars")
            checkServerStatus()
            checkModelFiles()
        }
    }
    
    private func stepLabel(for step: Int) -> String {
        switch step {
        case 0: return "Welcome"
        case 1: return "Features"
        case 2: return "Security"
        case 3: return "Review"
        default: return ""
        }
    }
    
    private func checkServerStatus() {
        // Check if server is already running
        let url = URL(string: "http://localhost:7474/health")!
        
        URLSession.shared.dataTask(with: url) { data, response, error in
            DispatchQueue.main.async {
                if let httpResponse = response as? HTTPURLResponse,
                   httpResponse.statusCode == 200 {
                    serverIsRunning = true
                } else {
                    serverIsRunning = false
                }
            }
        }.resume()
    }
    
    private func saveAndStartServer() {
        isSaving = true
        saveProgress = "Applying settings..."
        
        // Apply the selected preset
        applyPreset()
        
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
            saveProgress = "Saving configuration..."
            
            // Save configuration
            if config.saveConfig() {
                // Mark first run as complete
                config.completeFirstRun()
                
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                    saveProgress = serverIsRunning ? "Restarting server..." : "Starting server..."
                    
                    // Start or restart server
                    if serverIsRunning {
                        // Restart the server
                        let task = Process()
                        task.launchPath = "/usr/bin/env"
                        task.arguments = ["launchctl", "kickstart", "-k", "gui/\(getuid())/com.nornicdb.server"]
                        task.launch()
                        
                        saveProgress = "Waiting for server to restart..."
                    } else {
                        // First time: CREATE the LaunchAgent plist, then load and start
                        saveProgress = "Creating service configuration..."
                        
                        let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
                        let homeDir = NSString(string: "~").expandingTildeInPath
                        
                        // Create the plist content
                        let plistContent = """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
                        <plist version="1.0">
                        <dict>
                            <key>Label</key>
                            <string>com.nornicdb.server</string>
                            <key>ProgramArguments</key>
                            <array>
                                <string>/usr/local/bin/nornicdb</string>
                                <string>serve</string>
                                <string>--data-dir</string>
                                <string>/usr/local/var/nornicdb/data</string>
                                <string>--bolt-port</string>
                                <string>7687</string>
                                <string>--http-port</string>
                                <string>7474</string>
                            </array>
                            <key>WorkingDirectory</key>
                            <string>/usr/local/var/nornicdb</string>
                            <key>RunAtLoad</key>
                            <true/>
                            <key>KeepAlive</key>
                            <dict>
                                <key>SuccessfulExit</key>
                                <false/>
                                <key>Crashed</key>
                                <true/>
                            </dict>
                            <key>ThrottleInterval</key>
                            <integer>30</integer>
                            <key>StandardOutPath</key>
                            <string>/usr/local/var/log/nornicdb/stdout.log</string>
                            <key>StandardErrorPath</key>
                            <string>/usr/local/var/log/nornicdb/stderr.log</string>
                            <key>EnvironmentVariables</key>
                            <dict>
                                <key>PATH</key>
                                <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
                                <key>HOME</key>
                                <string>\(homeDir)</string>
                            </dict>
                            <key>ProcessType</key>
                            <string>Interactive</string>
                            <key>Nice</key>
                            <integer>0</integer>
                        </dict>
                        </plist>
                        """
                        
                        // Write the plist file
                        do {
                            try plistContent.write(toFile: launchAgentPath, atomically: true, encoding: .utf8)
                            print("Created server plist at: \(launchAgentPath)")
                        } catch {
                            print("Failed to create server plist: \(error)")
                        }
                        
                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                            saveProgress = "Loading service..."
                            
                            // Load the LaunchAgent
                            let loadTask = Process()
                            loadTask.launchPath = "/usr/bin/env"
                            loadTask.arguments = ["launchctl", "load", launchAgentPath]
                            loadTask.launch()
                            loadTask.waitUntilExit()
                            
                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                                saveProgress = "Starting server..."
                                
                                // Then start the server
                                let startTask = Process()
                                startTask.launchPath = "/usr/bin/env"
                                startTask.arguments = ["launchctl", "start", "com.nornicdb.server"]
                                startTask.launch()
                            }
                        }
                    }
                    
                    // Wait and verify server is running
                    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                        saveProgress = "Waiting for server to be ready..."
                        
                        // Poll health endpoint
                        waitForServerHealth(attempts: 10) { success in
                            if success {
                                saveProgress = "✅ Server is running!"
                                
                                DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                                    isSaving = false
                                    onComplete()
                                }
                            } else {
                                saveProgress = "⚠️ Server may still be starting. Check menu bar status."
                                
                                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                                    isSaving = false
                                    onComplete()
                                }
                            }
                        }
                    }
                }
            } else {
                saveProgress = "Failed to save configuration"
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    isSaving = false
                }
            }
        }
    }
    
    private func waitForServerHealth(attempts: Int, completion: @escaping (Bool) -> Void) {
        guard attempts > 0 else {
            completion(false)
            return
        }
        
        // Health endpoint is always on HTTP port 7474
        let url = URL(string: "http://localhost:7474/health")!
        var request = URLRequest(url: url)
        request.timeoutInterval = 2.0
        
        URLSession.shared.dataTask(with: request) { _, response, error in
            DispatchQueue.main.async {
                if let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 {
                    completion(true)
                } else {
                    // Retry after 1 second
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                        waitForServerHealth(attempts: attempts - 1, completion: completion)
                    }
                }
            }
        }.resume()
    }
    
    var welcomeStep: some View {
        VStack(spacing: 20) {
            Text("Step 1: Welcome")
                .font(.headline)
            
            VStack(alignment: .leading, spacing: 15) {
                InfoRow(icon: "bolt.fill", title: "Neo4j Compatible", description: "Drop-in replacement for Neo4j with 3-52x better performance")
                InfoRow(icon: "cpu.fill", title: "Native Performance", description: "Optimized for Apple Silicon with Metal acceleration")
                InfoRow(icon: "brain.head.profile", title: "AI-Powered", description: "Built-in embeddings, clustering, and predictions")
                InfoRow(icon: "shield.fill", title: "Privacy First", description: "Runs entirely on your Mac - your data never leaves")
            }
            .padding()
            
            Spacer()
        }
        .padding()
    }
    
    var presetStep: some View {
        ScrollView {
            VStack(spacing: 20) {
                Text("Step 2: Choose Your Setup")
                    .font(.headline)
                
                Text("Select a configuration preset based on your needs")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                VStack(spacing: 15) {
                    PresetOption(
                        preset: .basic,
                        selected: $selectedPreset,
                        title: "Basic",
                        subtitle: "Essential features only",
                        features: ["Neo4j compatibility", "Fast queries", "Low resource usage"]
                    )
                    
                    PresetOption(
                        preset: .standard,
                        selected: $selectedPreset,
                        title: "Standard (Recommended)",
                        subtitle: "Great for most users",
                        features: ["All basic features", "Vector embeddings", "K-Means clustering"]
                    )
                    
                    PresetOption(
                        preset: .advanced,
                        selected: $selectedPreset,
                        title: "Advanced",
                        subtitle: "Full AI capabilities",
                        features: ["All features enabled", "Heimdall AI guardian", "Auto-predictions", "Maximum performance"]
                    )
                }
                .padding()
            }
            .padding()
        }
    }
    
    var securityStep: some View {
        ScrollView {
            VStack(spacing: 20) {
                Text("Step 3: Security")
                    .font(.headline)
                
                Text("Configure authentication and encryption")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                // Admin Credentials
                VStack(alignment: .leading, spacing: 15) {
                    Text("Admin Credentials")
                        .font(.headline)
                    
                    Text("Set your admin credentials for accessing NornicDB")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    HStack {
                        Text("Username:")
                            .frame(width: 120, alignment: .trailing)
                        TextField("admin", text: $config.adminUsername)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    HStack {
                        Text("Password:")
                            .frame(width: 120, alignment: .trailing)
                        SecureField("Enter password", text: $config.adminPassword)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    if config.adminPassword.count < 8 && !config.adminPassword.isEmpty {
                        HStack {
                            Spacer().frame(width: 120)
                            Text("⚠️ Password must be at least 8 characters")
                                .font(.caption)
                                .foregroundColor(.orange)
                        }
                    }
                    
                    Text("💡 These credentials are used to access the NornicDB web UI and API")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 120)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                // JWT Secret
                VStack(alignment: .leading, spacing: 15) {
                    Text("JWT Secret")
                        .font(.headline)
                    
                    HStack {
                        Text("Secret:")
                            .frame(width: 120, alignment: .trailing)
                        SecureField("Auto-generated if empty", text: $config.jwtSecret)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .frame(maxWidth: 250)
                    }
                    
                    HStack {
                        Spacer().frame(width: 120)
                        Button("Generate Random Secret") {
                            config.jwtSecret = config.generateRandomSecret()
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    Text("💡 The JWT secret is used to sign authentication tokens. Leave empty for auto-generation, or set a consistent value for tokens to persist across restarts.")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 120)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                // Encryption
                VStack(alignment: .leading, spacing: 15) {
                    Text("Database Encryption (Optional)")
                        .font(.headline)
                    
                    Toggle("Enable Encryption at Rest", isOn: $config.encryptionEnabled)
                    
                    if config.encryptionEnabled {
                        HStack {
                            Text("Encryption Key:")
                                .frame(width: 120, alignment: .trailing)
                            
                            if showEncryptionKey {
                                TextField("Enter encryption password", text: $config.encryptionPassword)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                    .frame(maxWidth: 200)
                                    .font(.system(.body, design: .monospaced))
                            } else {
                                SecureField("Enter encryption password", text: $config.encryptionPassword)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                    .frame(maxWidth: 200)
                            }
                            
                            Button(action: { showEncryptionKey.toggle() }) {
                                Image(systemName: showEncryptionKey ? "eye.slash" : "eye")
                            }
                            .buttonStyle(.borderless)
                            .help(showEncryptionKey ? "Hide key" : "Show key")
                            
                            Button(action: {
                                NSPasteboard.general.clearContents()
                                NSPasteboard.general.setString(config.encryptionPassword, forType: .string)
                            }) {
                                Image(systemName: "doc.on.doc")
                            }
                            .buttonStyle(.borderless)
                            .help("Copy to clipboard")
                            .disabled(config.encryptionPassword.isEmpty)
                        }
                        
                        HStack {
                            Spacer().frame(width: 120)
                            Button("Generate Strong Key") {
                                config.encryptionPassword = config.generateRandomSecret()
                                showEncryptionKey = true  // Show the newly generated key
                            }
                            .buttonStyle(.bordered)
                        }
                        
                        if config.encryptionPassword.count < 16 && !config.encryptionPassword.isEmpty {
                            HStack {
                                Spacer().frame(width: 120)
                                Text("⚠️ Encryption key should be at least 16 characters")
                                    .font(.caption)
                                    .foregroundColor(.orange)
                            }
                        }
                    }
                    
                    Text("⚠️ Enabling encryption will protect your data at rest. Keep your encryption password safe — data cannot be recovered without it!")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.leading, 0)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                
                Spacer()
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding()
        }
    }
    
    var confirmStep: some View {
        ScrollView {
            VStack(spacing: 20) {
                Text("Step 4: Review & Start")
                    .font(.headline)
                
                Text("Here's what will be enabled:")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                VStack(alignment: .leading, spacing: 15) {
                    FeatureSummary(enabled: getPresetFeatures().embeddings, title: "Embeddings", icon: "brain.head.profile")
                    FeatureSummary(enabled: getPresetFeatures().kmeans, title: "K-Means Clustering", icon: "circle.hexagongrid.fill")
                    FeatureSummary(enabled: getPresetFeatures().autoTLP, title: "Auto-TLP", icon: "clock.arrow.circlepath")
                    FeatureSummary(enabled: getPresetFeatures().heimdall, title: "Heimdall", icon: "eye.fill")
                }
                .padding()
                
                // Authentication Summary
                Divider()
                
                VStack(alignment: .leading, spacing: 12) {
                    Text("🔐 Authentication")
                        .font(.headline)
                    
                    HStack {
                        Text("Username:")
                            .foregroundColor(.secondary)
                            .frame(width: 100, alignment: .trailing)
                        Text(config.adminUsername)
                            .fontWeight(.medium)
                        Spacer()
                    }
                    
                    HStack {
                        Text("Password:")
                            .foregroundColor(.secondary)
                            .frame(width: 100, alignment: .trailing)
                        Text(String(repeating: "•", count: config.adminPassword.count))
                            .fontWeight(.medium)
                        Spacer()
                    }
                    
                    if !config.jwtSecret.isEmpty {
                        HStack {
                            Text("JWT Secret:")
                                .foregroundColor(.secondary)
                                .frame(width: 100, alignment: .trailing)
                            Text("Custom (set)")
                                .fontWeight(.medium)
                                .foregroundColor(.green)
                            Spacer()
                        }
                    }
                    
                    if config.encryptionEnabled {
                        HStack {
                            Text("Encryption:")
                                .foregroundColor(.secondary)
                                .frame(width: 100, alignment: .trailing)
                            Text("Enabled ✓")
                                .fontWeight(.medium)
                                .foregroundColor(.green)
                            Spacer()
                        }
                    }
                    
                    Text("💡 Go back to Step 2 (Setup) to change these settings")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.top, 4)
                }
                .padding()
                .background(Color.secondary.opacity(0.1))
                .cornerRadius(8)
                .padding(.horizontal)
                
                // Model Requirements Section
                if needsModels() {
                    Divider()
                    
                    VStack(spacing: 15) {
                        HStack {
                            Text("AI Models Required")
                                .font(.headline)
                            
                            Spacer()
                            
                            Button(action: {
                                let modelsPath = "/usr/local/var/nornicdb/models"
                                try? FileManager.default.createDirectory(atPath: modelsPath, withIntermediateDirectories: true, attributes: nil)
                                NSWorkspace.shared.open(URL(fileURLWithPath: modelsPath))
                            }) {
                                HStack(spacing: 4) {
                                    Image(systemName: "folder.fill")
                                    Text("Open Models Folder")
                                }
                                .font(.caption)
                            }
                            .buttonStyle(.plain)
                            .foregroundColor(.blue)
                        }
                        
                        if isDownloadingModels {
                            VStack(spacing: 10) {
                                ProgressView()
                                Text(downloadProgress)
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            .padding()
                        } else {
                            // Embedding Model (Standard & Advanced)
                            if selectedPreset == .standard || selectedPreset == .advanced {
                                ModelDownloadRow(
                                    modelName: "BGE-M3 Embedding Model",
                                    fileName: "bge-m3.gguf",
                                    size: "~400MB",
                                    exists: bgeModelExists,
                                    onDownload: { downloadBGEModel() }
                                )
                            }
                            
                            // Heimdall Model (Advanced only)
                            if selectedPreset == .advanced {
                                ModelDownloadRow(
                                    modelName: "Qwen2.5-0.5B-Instruct (Heimdall)",
                                    fileName: "qwen2.5-0.5b-instruct.gguf",
                                    size: "~350MB",
                                    exists: qwenModelExists,
                                    onDownload: { downloadQwenModel() }
                                )
                            }
                            
                            if !allRequiredModelsExist() {
                                HStack(spacing: 8) {
                                    Image(systemName: "exclamationmark.triangle.fill")
                                        .foregroundColor(.orange)
                                    Text("Without these models, you'll need to manually configure AI features or add your own .gguf models to the folder")
                                        .font(.caption)
                                        .foregroundColor(.orange)
                                }
                                .padding(.horizontal)
                                .padding(.top, 8)
                            }
                        }
                    }
                    .padding()
                }
                
                Divider()
                
                VStack(spacing: 8) {
                    HStack(spacing: 6) {
                        Image(systemName: "checkmark.circle.fill")
                            .foregroundColor(.green)
                        Text("Auto-start at login")
                            .font(.caption)
                    }
                    
                    HStack(spacing: 6) {
                        Image(systemName: "checkmark.circle.fill")
                            .foregroundColor(.green)
                        Text("Menu bar app for easy management")
                            .font(.caption)
                    }
                    
                    HStack(spacing: 6) {
                        Image(systemName: "checkmark.circle.fill")
                            .foregroundColor(.green)
                        Text("Access at http://localhost:7687")
                            .font(.caption)
                    }
                }
                .padding()
                
                Text("You can change these settings anytime from the menu bar app")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .padding()
            }
            .padding()
        }
        .onAppear {
            checkModelFiles()
        }
    }
    
    private func needsModels() -> Bool {
        return selectedPreset == .standard || selectedPreset == .advanced
    }
    
    private func allRequiredModelsExist() -> Bool {
        if selectedPreset == .standard {
            return bgeModelExists
        } else if selectedPreset == .advanced {
            return bgeModelExists && qwenModelExists
        }
        return true
    }
    
    private func checkModelFiles() {
        let modelsPath = "/usr/local/var/nornicdb/models"
        let fileManager = FileManager.default
        
        let bgePath = "\(modelsPath)/bge-m3.gguf"
        let qwenPath = "\(modelsPath)/qwen2.5-0.5b-instruct.gguf"
        
        bgeModelExists = fileManager.fileExists(atPath: bgePath)
        qwenModelExists = fileManager.fileExists(atPath: qwenPath)
        
        print("Checking models:")
        print("  BGE: \(bgePath) - exists: \(bgeModelExists)")
        print("  Qwen: \(qwenPath) - exists: \(qwenModelExists)")
    }
    
    private func downloadBGEModel() {
        isDownloadingModels = true
        downloadProgress = "Downloading BGE-M3 model (~400MB)..."
        
        DispatchQueue.global(qos: .userInitiated).async {
            let task = Process()
            task.launchPath = "/bin/bash"
            task.arguments = ["-c", "mkdir -p /usr/local/var/nornicdb/models && curl -L -o /usr/local/var/nornicdb/models/bge-m3.gguf https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf"]
            
            task.launch()
            task.waitUntilExit()
            
            DispatchQueue.main.async {
                if task.terminationStatus == 0 {
                    downloadProgress = "BGE-M3 downloaded successfully!"
                    bgeModelExists = true
                } else {
                    downloadProgress = "Download failed. You can download manually later."
                }
                
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    isDownloadingModels = false
                    downloadProgress = ""
                }
            }
        }
    }
    
    private func downloadQwenModel() {
        isDownloadingModels = true
        downloadProgress = "Downloading Qwen2.5-0.5B model (~350MB)..."
        
        DispatchQueue.global(qos: .userInitiated).async {
            let task = Process()
            task.launchPath = "/bin/bash"
            task.arguments = ["-c", "mkdir -p /usr/local/var/nornicdb/models && curl -L -o /usr/local/var/nornicdb/models/qwen2.5-0.5b-instruct.gguf https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf"]
            
            task.launch()
            task.waitUntilExit()
            
            DispatchQueue.main.async {
                if task.terminationStatus == 0 {
                    downloadProgress = "Qwen2.5 downloaded successfully!"
                    qwenModelExists = true
                } else {
                    downloadProgress = "Download failed. You can download manually later."
                }
                
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    isDownloadingModels = false
                    downloadProgress = ""
                }
            }
        }
    }

    func applyPreset() {
        let features = getPresetFeatures()
        config.embeddingsEnabled = features.embeddings
        config.kmeansEnabled = features.kmeans
        config.autoTLPEnabled = features.autoTLP
        config.heimdallEnabled = features.heimdall
        config.autoStartEnabled = true
    }
    
    func getPresetFeatures() -> (embeddings: Bool, kmeans: Bool, autoTLP: Bool, heimdall: Bool) {
        switch selectedPreset {
        case .basic:
            return (false, false, false, false)
        case .standard:
            return (true, true, false, false)
        case .advanced:
            return (true, true, true, true)
        }
    }
}

enum ConfigPreset {
    case basic
    case standard
    case advanced
}

struct InfoRow: View {
    let icon: String
    let title: String
    let description: String
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: icon)
                .font(.system(size: 20))
                .foregroundColor(.blue)
                .frame(width: 24)
            
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.subheadline)
                    .fontWeight(.medium)
                Text(description)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
    }
}

struct PresetOption: View {
    let preset: ConfigPreset
    @Binding var selected: ConfigPreset
    let title: String
    let subtitle: String
    let features: [String]
    
    var isSelected: Bool {
        selected == preset
    }
    
    var body: some View {
        Button(action: { selected = preset }) {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .font(.system(size: 24))
                    .foregroundColor(isSelected ? .blue : .gray)
                
                VStack(alignment: .leading, spacing: 8) {
                    Text(title)
                        .font(.headline)
                        .foregroundColor(.primary)
                    Text(subtitle)
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(features, id: \.self) { feature in
                            HStack(spacing: 6) {
                                Text("•")
                                    .foregroundColor(.blue)
                                Text(feature)
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                    .padding(.top, 4)
                }
                
                Spacer()
            }
            .padding()
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .stroke(isSelected ? Color.blue : Color.gray.opacity(0.3), lineWidth: 2)
                    .background(RoundedRectangle(cornerRadius: 12).fill(isSelected ? Color.blue.opacity(0.1) : Color.clear))
            )
        }
        .buttonStyle(.plain)
    }
}

struct FeatureSummary: View {
    let enabled: Bool
    let title: String
    let icon: String
    
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: icon)
                .foregroundColor(enabled ? .blue : .gray)
            Text(title)
                .foregroundColor(enabled ? .primary : .secondary)
            Spacer()
            Image(systemName: enabled ? "checkmark.circle.fill" : "xmark.circle")
                .foregroundColor(enabled ? .green : .gray)
        }
    }
}

struct ModelDownloadRow: View {
    let modelName: String
    let fileName: String
    let size: String
    let exists: Bool
    let onDownload: () -> Void
    
    var body: some View {
        HStack(alignment: .center, spacing: 15) {
            // Status Icon
            ZStack {
                Circle()
                    .fill(exists ? Color.green.opacity(0.2) : Color.orange.opacity(0.2))
                    .frame(width: 40, height: 40)
                
                Image(systemName: exists ? "checkmark.circle.fill" : "exclamationmark.circle.fill")
                    .foregroundColor(exists ? .green : .orange)
                    .font(.system(size: 22))
            }
            
            // Model Info
            VStack(alignment: .leading, spacing: 4) {
                Text(modelName)
                    .font(.subheadline)
                    .fontWeight(.medium)
                Text(fileName)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            // Status or Action
            if exists {
                VStack(alignment: .trailing, spacing: 2) {
                    Text("✓ Installed")
                        .font(.caption)
                        .fontWeight(.semibold)
                        .foregroundColor(.green)
                    Text("Ready to use")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            } else {
                Button(action: onDownload) {
                    HStack(spacing: 6) {
                        Image(systemName: "arrow.down.circle.fill")
                        VStack(alignment: .leading, spacing: 2) {
                            Text("Download")
                                .font(.caption)
                                .fontWeight(.semibold)
                            Text(size)
                                .font(.caption)
                        }
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
                    .background(Color.blue)
                    .foregroundColor(.white)
                    .cornerRadius(6)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 10)
                .fill(exists ? Color.green.opacity(0.05) : Color.orange.opacity(0.05))
                .overlay(
                    RoundedRectangle(cornerRadius: 10)
                        .stroke(exists ? Color.green.opacity(0.3) : Color.orange.opacity(0.3), lineWidth: 1)
                )
        )
    }
}

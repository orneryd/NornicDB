import SwiftUI
import AppKit
import Foundation
import Security

// MARK: - Keychain Helper for Secure Secret Storage

/// Keychain access result - distinguishes between "no value" and "access denied"
enum KeychainResult {
    case success(String)
    case notFound
    case accessDenied
    case otherError(OSStatus)
}

/// Securely stores sensitive secrets (JWT, encryption password, API tokens) in macOS Keychain
/// instead of plain text in config files.
class KeychainHelper {
    static let shared = KeychainHelper()
    
    private let service = "com.nornicdb.menubar"
    
    // Account names for different secrets (keychain identifiers, not actual credentials)
    // nosec: These are keychain account identifiers, not hardcoded secrets
    private let jwtSecretAccount = "jwt_secret"
    private let encryptionKeyAccount = "encryption_key"  // Keychain identifier for encryption credential
    private let apiTokenAccount = "api_token"
    private let appleIntelligenceAPIKeyAccount = "apple_intelligence_api_key"  // Local embedding server auth
    // Future: openai_api_key, anthropic_api_key, etc.
    
    // Track if user has denied access to specific secrets
    private var accessDeniedForJWT = false
    private var accessDeniedForEncryption = false
    private var accessDeniedForAPIToken = false
    
    // Cache secrets after first successful load to avoid multiple prompts
    private var cachedJWT: String?
    private var cachedEncryption: String?
    private var cachedAppleIntelligenceAPIKey: String?
    private var cachedAPIToken: String?
    private var hasAttemptedJWTLoad = false
    private var hasAttemptedEncryptionLoad = false
    private var hasAttemptedAPITokenLoad = false
    
    private init() {}
    
    // MARK: - Access Status
    
    /// Check if Keychain access was denied for JWT secret
    var isJWTAccessDenied: Bool { accessDeniedForJWT }
    
    /// Check if Keychain access was denied for encryption password
    var isEncryptionAccessDenied: Bool { accessDeniedForEncryption }
    
    /// Check if Keychain access was denied for API token
    var isAPITokenAccessDenied: Bool { accessDeniedForAPIToken }
    
    /// Reset access denied flag for JWT (to retry on next startup)
    func resetJWTAccessDenied() { accessDeniedForJWT = false }
    
    /// Reset access denied flag for encryption (to retry on next startup)
    func resetEncryptionAccessDenied() { accessDeniedForEncryption = false }
    
    /// Reset all access denied flags (for retry on startup)
    func resetAllAccessDenied() {
        accessDeniedForJWT = false
        accessDeniedForEncryption = false
        accessDeniedForAPIToken = false
        hasAttemptedJWTLoad = false
        hasAttemptedEncryptionLoad = false
        hasAttemptedAPITokenLoad = false
    }
    
    // MARK: - Generic Keychain Operations
    
    /// Save a secret to Keychain - ONLY if it doesn't already exist
    /// This preserves secrets across reinstalls
    private func saveSecret(_ secret: String, account: String, overwrite: Bool = false) -> Bool {
        // Check if secret already exists in Keychain
        let existingResult = getSecretWithStatus(account: account)
        switch existingResult {
        case .success(_):
            if !overwrite {
                print("🔐 Secret already exists in Keychain, preserving existing value")
                return true // Return true since we have a valid secret
            }
        case .accessDenied:
            print("🚫 Keychain access denied - cannot save")
            return false
        case .notFound, .otherError(_):
            break // Continue to save
        }
        
        // Delete existing secret first (only if we're overwriting or it doesn't exist)
        deleteSecret(account: account)
        
        guard !secret.isEmpty, let secretData = secret.data(using: .utf8) else { return false }
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecValueData as String: secretData,
            kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlockedThisDeviceOnly
        ]
        
        let status = SecItemAdd(query as CFDictionary, nil)
        
        if status == errSecSuccess {
            print("✅ Secret saved to Keychain")
            return true
        } else if status == errSecAuthFailed || status == errSecUserCanceled || status == errSecInteractionNotAllowed {
            print("🚫 Keychain access denied when saving")
            return false
        } else {
            print("❌ Failed to save to Keychain")
            return false
        }
    }
    
    /// Retrieve a secret from Keychain with detailed status
    private func getSecretWithStatus(account: String) -> KeychainResult {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        
        switch status {
        case errSecSuccess:
            if let secretData = result as? Data,
               let secret = String(data: secretData, encoding: .utf8) {
                return .success(secret)
            }
            return .otherError(status)
        case errSecItemNotFound:
            return .notFound
        case errSecAuthFailed, errSecUserCanceled, errSecInteractionNotAllowed:
            return .accessDenied
        default:
            return .otherError(status)
        }
    }
    
    /// Retrieve a secret from Keychain (simple interface)
    private func getSecret(account: String) -> String? {
        switch getSecretWithStatus(account: account) {
        case .success(let secret):
            return secret
        default:
            return nil
        }
    }
    
    /// Delete a secret from Keychain
    @discardableResult
    private func deleteSecret(account: String) -> Bool {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account
        ]
        
        let status = SecItemDelete(query as CFDictionary)
        return status == errSecSuccess || status == errSecItemNotFound
    }
    
    // MARK: - JWT Secret
    
    /// Save JWT secret - won't overwrite if one already exists
    func saveJWTSecret(_ secret: String) -> Bool {
        let result = saveSecret(secret, account: jwtSecretAccount, overwrite: false)
        if result {
            cachedJWT = secret  // Update cache on successful save
        } else {
            // Check if it was an access denial or if secret already exists
            let checkResult = getSecretWithStatus(account: jwtSecretAccount)
            if case .accessDenied = checkResult {
                accessDeniedForJWT = true
            } else if case .success(let existing) = checkResult {
                cachedJWT = existing  // Cache existing value
            }
        }
        return result
    }
    
    /// Save JWT secret - forces overwrite even if one exists (use when user explicitly changes it)
    func updateJWTSecret(_ secret: String) -> Bool {
        let result = saveSecret(secret, account: jwtSecretAccount, overwrite: true)
        if result {
            cachedJWT = secret  // Update cache
        }
        return result
    }
    
    /// Get JWT secret with access tracking (cached after first access)
    func getJWTSecret() -> String? {
        // Return cached value if we already loaded it successfully
        if let cached = cachedJWT {
            return cached
        }
        
        // Only attempt to load once per session to avoid multiple prompts
        if hasAttemptedJWTLoad && accessDeniedForJWT {
            return nil
        }
        
        hasAttemptedJWTLoad = true
        let result = getSecretWithStatus(account: jwtSecretAccount)
        switch result {
        case .success(let secret):
            accessDeniedForJWT = false
            cachedJWT = secret
            return secret
        case .accessDenied:
            accessDeniedForJWT = true
            print("🚫 Keychain access denied for JWT secret")
            return nil
        default:
            return nil
        }
    }
    
    func deleteJWTSecret() -> Bool {
        return deleteSecret(account: jwtSecretAccount)
    }
    
    func hasJWTSecret() -> Bool {
        return getJWTSecret() != nil
    }
    
    // MARK: - Encryption Password
    
    /// Save encryption password - won't overwrite if one already exists
    func saveEncryptionPassword(_ password: String) -> Bool {
        let result = saveSecret(password, account: encryptionKeyAccount, overwrite: false)
        if result {
            cachedEncryption = password  // Update cache on successful save
        } else {
            let checkResult = getSecretWithStatus(account: encryptionKeyAccount)
            if case .accessDenied = checkResult {
                accessDeniedForEncryption = true
            } else if case .success(let existing) = checkResult {
                cachedEncryption = existing  // Cache existing value
            }
        }
        return result
    }
    
    /// Save encryption password - forces overwrite even if one exists (use when user explicitly changes it)
    func updateEncryptionPassword(_ password: String) -> Bool {
        let result = saveSecret(password, account: encryptionKeyAccount, overwrite: true)
        if result {
            cachedEncryption = password  // Update cache
        }
        return result
    }
    
    /// Get encryption password with access tracking (cached after first access)
    func getEncryptionPassword() -> String? {
        // Return cached value if we already loaded it successfully
        if let cached = cachedEncryption {
            return cached
        }
        
        // Only attempt to load once per session to avoid multiple prompts
        if hasAttemptedEncryptionLoad && accessDeniedForEncryption {
            return nil
        }
        
        hasAttemptedEncryptionLoad = true
        let result = getSecretWithStatus(account: encryptionKeyAccount)
        switch result {
        case .success(let secret):
            accessDeniedForEncryption = false
            cachedEncryption = secret
            return secret
        case .accessDenied:
            accessDeniedForEncryption = true
            print("🚫 Keychain access denied for encryption credential")
            return nil
        default:
            return nil
        }
    }
    
    func deleteEncryptionPassword() -> Bool {
        return deleteSecret(account: encryptionKeyAccount)
    }
    
    func hasEncryptionPassword() -> Bool {
        return getEncryptionPassword() != nil
    }
    
    // MARK: - API Token
    
    /// Save API token - won't overwrite if one already exists
    func saveAPIToken(_ token: String) -> Bool {
        return saveSecret(token, account: apiTokenAccount, overwrite: false)
    }
    
    /// Save API token - forces overwrite even if one exists
    func updateAPIToken(_ token: String) -> Bool {
        let result = saveSecret(token, account: apiTokenAccount, overwrite: true)
        if result {
            cachedAPIToken = token  // Update cache
        }
        return result
    }
    
    /// Get API token with caching (cached after first access)
    func getAPIToken() -> String? {
        // Return cached value if we already loaded it successfully
        if let cached = cachedAPIToken {
            return cached
        }
        
        // Only attempt to load once per session to avoid multiple prompts
        if hasAttemptedAPITokenLoad && accessDeniedForAPIToken {
            return nil
        }
        
        hasAttemptedAPITokenLoad = true
        let result = getSecretWithStatus(account: apiTokenAccount)
        switch result {
        case .success(let secret):
            accessDeniedForAPIToken = false
            cachedAPIToken = secret
            return secret
        case .accessDenied:
            accessDeniedForAPIToken = true
            print("🚫 Keychain access denied for API token")
            return nil
        default:
            return nil
        }
    }
    
    // MARK: - Apple Intelligence API Key (Local Embedding Server)
    
    /// Save Apple Intelligence API key - won't overwrite if one already exists
    func saveAppleIntelligenceAPIKey(_ key: String) -> Bool {
        let result = saveSecret(key, account: appleIntelligenceAPIKeyAccount, overwrite: false)
        if result {
            cachedAppleIntelligenceAPIKey = key
        }
        return result
    }
    
    /// Save Apple Intelligence API key - forces overwrite even if one exists
    func updateAppleIntelligenceAPIKey(_ key: String) -> Bool {
        let result = saveSecret(key, account: appleIntelligenceAPIKeyAccount, overwrite: true)
        if result {
            cachedAppleIntelligenceAPIKey = key
        }
        return result
    }
    
    /// Get Apple Intelligence API key with caching
    func getAppleIntelligenceAPIKey() -> String? {
        // Return cached value if we already loaded it successfully
        if let cached = cachedAppleIntelligenceAPIKey {
            return cached
        }
        
        let result = getSecretWithStatus(account: appleIntelligenceAPIKeyAccount)
        switch result {
        case .success(let secret):
            cachedAppleIntelligenceAPIKey = secret
            return secret
        default:
            return nil
        }
    }
    
    func deleteAPIToken() -> Bool {
        return deleteSecret(account: apiTokenAccount)
    }
    
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
    private var fileIndexerWindowController: NSWindowController?
    
    // Apple Intelligence Embedding Server
    @MainActor
    lazy var embeddingServer: EmbeddingServer = {
        let server = EmbeddingServer()
        server.port = ConfigManager.appleEmbeddingPort
        server.loadConfiguration()
        // Set API key for secure communication (only NornicDB can call it)
        server.setAPIKey(ConfigManager.getAppleIntelligenceAPIKey())
        return server
    }()
    
    func applicationDidFinishLaunching(_ notification: Notification) {
        // Hide dock icon - we only want menu bar presence
        NSApp.setActivationPolicy(.accessory)
        
        // Load configuration
        configManager.loadConfig()
        
        // Start Apple Intelligence embedding server if enabled
        if configManager.useAppleIntelligence && configManager.embeddingsEnabled && AppleMLEmbedder.isAvailable() {
            Task { @MainActor in
                do {
                    try self.embeddingServer.start()
                    print("✅ Apple Intelligence embedding server auto-started (enabled in config)")
                } catch {
                    print("❌ Failed to auto-start embedding server: \(error)")
                }
            }
        }
        
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
        // Stop Apple Intelligence embedding server if running
        Task { @MainActor in
            if self.embeddingServer.isRunning {
                self.embeddingServer.stop()
                print("🛑 Apple Intelligence embedding server stopped (app quit)")
            }
        }
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
        menu.addItem(NSMenuItem(title: "File Indexer...", action: #selector(openFileIndexer), keyEquivalent: "i"))
        menu.addItem(NSMenuItem(title: "Open Config File", action: #selector(openConfig), keyEquivalent: ""))
        menu.addItem(NSMenuItem(title: "Show Logs", action: #selector(showLogs), keyEquivalent: "l"))
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
            let settingsView = SettingsView(config: configManager, appDelegate: self)
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
    
    @objc func openFileIndexer() {
        if fileIndexerWindowController == nil {
            let fileIndexerView = FileIndexerView(config: configManager)
            let hostingController = NSHostingController(rootView: fileIndexerView)
            
            let window = NSWindow(contentViewController: hostingController)
            window.title = "NornicDB File Indexer"
            window.setContentSize(NSSize(width: 900, height: 700))
            window.styleMask = [.titled, .closable, .resizable, .miniaturizable]
            window.center()
            
            fileIndexerWindowController = NSWindowController(window: window)
        }
        
        fileIndexerWindowController?.showWindow(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
    
    func showFirstRunWizard() {
        let wizardView = FirstRunWizard(config: configManager, appDelegate: self) {
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
    @Published var embeddingsEnabled: Bool = false
    @Published var kmeansEnabled: Bool = false
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
    
    // Authentication settings
    @Published var adminUsername: String = "admin"
    @Published var adminPassword: String = "password"
    @Published var jwtSecret: String = ""
    
    // Encryption settings
    @Published var encryptionEnabled: Bool = false
    @Published var encryptionPassword: String = ""
    @Published var encryptionKeychainAccessDenied: Bool = false  // Track if user denied Keychain access
    
    @Published var embeddingModel: String = "bge-m3.gguf"
    @Published var embeddingDimensions: Int = 1024  // Read from config, default 1024 for bge-m3
    @Published var heimdallModel: String = "qwen2.5-0.5b-instruct.gguf"
    @Published var availableModels: [String] = []
    
    // Config path matches server's FindConfigFile priority: ~/.nornicdb/config.yaml
    private let configPath = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath
    
    // MARK: - YAML Parsing Helpers
    
    /// Extract a YAML section's content (everything until the next top-level key)
    /// This properly handles indented content within a section
    private func extractYAMLSection(named sectionName: String, from content: String) -> String? {
        let lines = content.components(separatedBy: .newlines)
        var inSection = false
        var sectionLines: [String] = []
        
        for line in lines {
            // Check if this is the start of our target section (no leading whitespace)
            if line.hasPrefix("\(sectionName):") && !line.hasPrefix(" ") && !line.hasPrefix("\t") {
                inSection = true
                continue
            }
            
            // If we're in the section, check if we've hit another top-level key
            if inSection {
                // A line that starts with a non-whitespace character and contains ":" is a new section
                let trimmed = line.trimmingCharacters(in: .whitespaces)
                if !line.isEmpty && !line.hasPrefix(" ") && !line.hasPrefix("\t") && !line.hasPrefix("#") && trimmed.contains(":") {
                    // We've hit a new top-level section, stop
                    break
                }
                sectionLines.append(line)
            }
        }
        
        return sectionLines.isEmpty ? nil : sectionLines.joined(separator: "\n")
    }
    
    /// Get a boolean value from a YAML section
    private func getYAMLBool(key: String, from section: String, default defaultValue: Bool = false) -> Bool {
        // Look for "key: true" or "key: false" within section lines
        for line in section.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("\(key):") {
                if trimmed.contains("true") { return true }
                if trimmed.contains("false") { return false }
            }
        }
        return defaultValue
    }
    
    /// Get a string value from a YAML section
    private func getYAMLString(key: String, from section: String) -> String? {
        for line in section.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("\(key):") {
                let value = trimmed.dropFirst("\(key):".count).trimmingCharacters(in: .whitespaces)
                // Remove quotes if present
                if value.hasPrefix("\"") && value.hasSuffix("\"") {
                    return String(value.dropFirst().dropLast())
                }
                if value.hasPrefix("'") && value.hasSuffix("'") {
                    return String(value.dropFirst().dropLast())
                }
                return value.isEmpty ? nil : value
            }
        }
        return nil
    }
    
    /// Get an integer value from a YAML section
    private func getYAMLInt(key: String, from section: String) -> Int? {
        if let stringValue = getYAMLString(key: key, from: section) {
            return Int(stringValue)
        }
        return nil
    }
    
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
        
        guard let content = try? String(contentsOfFile: configPath, encoding: .utf8) else {
            print("Could not read config file at: \(configPath)")
            // Even without config file, we may have Keychain secrets from previous install
            return
        }
        
        print("Loading config from: \(configPath)")
        
        // Parse YAML sections properly (each section ends when a new top-level key starts)
        
        // Load embedding section
        if let embeddingSection = extractYAMLSection(named: "embedding", from: content) {
            embeddingsEnabled = getYAMLBool(key: "enabled", from: embeddingSection)
            print("✅ Loaded embeddings enabled: \(embeddingsEnabled)")
            
            if let model = getYAMLString(key: "model", from: embeddingSection) {
                embeddingModel = model
                print("✅ Loaded embedding model: \(model)")
            }
            
            // Check if using Apple Intelligence (provider is "openai" with localhost:11435 URL)
            if let provider = getYAMLString(key: "provider", from: embeddingSection),
               let url = getYAMLString(key: "url", from: embeddingSection) {
                useAppleIntelligence = provider == "openai" && url.contains("localhost:\(ConfigManager.appleEmbeddingPort)")
                print("✅ Loaded use Apple Intelligence: \(useAppleIntelligence)")
            }
            
            // Load embedding dimensions from config
            if let dims = getYAMLInt(key: "dimensions", from: embeddingSection), dims > 0 {
                embeddingDimensions = dims
                print("✅ Loaded embedding dimensions: \(dims)")
            }
        }
        
        // Load kmeans section
        if let kmeansSection = extractYAMLSection(named: "kmeans", from: content) {
            kmeansEnabled = getYAMLBool(key: "enabled", from: kmeansSection)
            print("✅ Loaded kmeans enabled: \(kmeansEnabled)")
        }
        
        // Load auto_tlp section
        if let autoTLPSection = extractYAMLSection(named: "auto_tlp", from: content) {
            autoTLPEnabled = getYAMLBool(key: "enabled", from: autoTLPSection)
            print("✅ Loaded auto_tlp enabled: \(autoTLPEnabled)")
        }
        
        // Load heimdall section
        if let heimdallSection = extractYAMLSection(named: "heimdall", from: content) {
            heimdallEnabled = getYAMLBool(key: "enabled", from: heimdallSection)
            print("✅ Loaded heimdall enabled: \(heimdallEnabled)")
            
            if let model = getYAMLString(key: "model", from: heimdallSection) {
                heimdallModel = model
                print("✅ Loaded heimdall model: \(model)")
            }
        }
        
        // Load server section
        if let serverSection = extractYAMLSection(named: "server", from: content) {
            if let port = getYAMLString(key: "bolt_port", from: serverSection) {
                boltPortNumber = port
                print("✅ Loaded bolt_port: \(port)")
            }
            if let port = getYAMLString(key: "http_port", from: serverSection) {
                httpPortNumber = port
                print("✅ Loaded http_port: \(port)")
            }
            if let host = getYAMLString(key: "host", from: serverSection) {
                hostAddress = host
                print("✅ Loaded host: \(host)")
            }
        }
        
        // Load auth section
        if let authSection = extractYAMLSection(named: "auth", from: content) {
            if let username = getYAMLString(key: "username", from: authSection) {
                adminUsername = username
                print("✅ Loaded username: \(username)")
            }
            if let password = getYAMLString(key: "password", from: authSection) {
                adminPassword = password
                print("✅ Loaded password: [hidden]")
            }
            
            // Load JWT secret from config file ONLY if not already loaded from Keychain
            if jwtSecret.isEmpty {
                if let jwt = getYAMLString(key: "jwt_secret", from: authSection),
                   !jwt.hasPrefix("[stored-in-keychain]"),
                   !jwt.isEmpty {
                    jwtSecret = jwt
                    print("📄 Loaded JWT secret from config (migrating to Keychain)")
                    _ = KeychainHelper.shared.saveJWTSecret(jwt)
                }
            }
        }
        
        // Load database/encryption settings
        var configSaysEncryptionEnabled = false
        var databaseSectionContent: String? = nil
        if let dbSection = extractYAMLSection(named: "database", from: content) {
            databaseSectionContent = dbSection
            configSaysEncryptionEnabled = getYAMLBool(key: "encryption_enabled", from: dbSection)
            print("✅ Config says encryption enabled: \(configSaysEncryptionEnabled)")
        }
        
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
        if encryptionPassword.isEmpty && !encryptionKeychainAccessDenied, let dbSection = databaseSectionContent {
            if let password = getYAMLString(key: "encryption_password", from: dbSection),
               !password.hasPrefix("[stored-in-keychain]"),
               !password.isEmpty {
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
        
        content = ensureSectionExists(in: content, section: "embedding", defaultContent: """
        
        embedding:
          enabled: false
          provider: local
          model: bge-m3.gguf
          url: ""
          dimensions: 1024
        """)
        
        content = ensureSectionExists(in: content, section: "kmeans", defaultContent: """
        
        kmeans:
          enabled: false
        """)
        
        content = ensureSectionExists(in: content, section: "auto_tlp", defaultContent: """
        
        auto_tlp:
          enabled: false
        """)
        
        content = ensureSectionExists(in: content, section: "heimdall", defaultContent: """
        
        heimdall:
          enabled: false
          model: qwen2.5-0.5b-instruct.gguf
        """)
        
        content = ensureSectionExists(in: content, section: "server", defaultContent: """
        
        server:
          bolt_port: 7687
          http_port: 7474
          host: localhost
        """)
        
        // Update each feature setting
        content = updateYAMLValue(in: content, section: "embedding", key: "enabled", value: embeddingsEnabled)
        content = updateYAMLValue(in: content, section: "kmeans", key: "enabled", value: kmeansEnabled)
        content = updateYAMLValue(in: content, section: "auto_tlp", key: "enabled", value: autoTLPEnabled)
        content = updateYAMLValue(in: content, section: "heimdall", key: "enabled", value: heimdallEnabled)
        
        // Update model selections and Apple Intelligence settings
        if useAppleIntelligence {
            // Configure NornicDB to use Apple Intelligence via local embedding server
            // Note: Only set base URL - NornicDB adds /v1/embeddings automatically for openai provider
            content = updateYAMLStringValue(in: content, section: "embedding", key: "provider", value: "openai")
            content = updateYAMLStringValue(in: content, section: "embedding", key: "url", value: "http://localhost:\(ConfigManager.appleEmbeddingPort)")
            content = updateYAMLStringValue(in: content, section: "embedding", key: "model", value: "apple-ml-embeddings")
            content = updateYAMLIntValue(in: content, section: "embedding", key: "dimensions", value: ConfigManager.appleEmbeddingDimensions)
        } else {
            // Use the selected local model
            content = updateYAMLStringValue(in: content, section: "embedding", key: "provider", value: "local")
            content = updateYAMLStringValue(in: content, section: "embedding", key: "url", value: "")
            content = updateYAMLStringValue(in: content, section: "embedding", key: "model", value: embeddingModel)
            // Reset dimensions to default (will be auto-detected from model)
            content = updateYAMLIntValue(in: content, section: "embedding", key: "dimensions", value: 1024)
        }
        content = updateYAMLStringValue(in: content, section: "heimdall", key: "model", value: heimdallModel)
        
        // Update server settings
        content = updateYAMLStringValue(in: content, section: "server", key: "bolt_port", value: boltPortNumber)
        content = updateYAMLStringValue(in: content, section: "server", key: "http_port", value: httpPortNumber)
        content = updateYAMLStringValue(in: content, section: "server", key: "host", value: hostAddress)
        
        // Update auth settings
        content = updateYAMLStringValue(in: content, section: "auth", key: "username", value: adminUsername)
        content = updateYAMLStringValue(in: content, section: "auth", key: "password", value: adminPassword)
        
        // Auto-generate JWT secret only if empty, then save to Keychain
        print("💾 Saving JWT secret - current value length: \(jwtSecret.count)")
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
            content = updateYAMLStringValue(in: content, section: "auth", key: "jwt_secret", value: "\"[stored-in-keychain]\"")
        } else {
            // Fallback: save to config file if Keychain fails
            print("⚠️ Keychain save failed, storing JWT in config file")
            content = updateYAMLStringValue(in: content, section: "auth", key: "jwt_secret", value: jwtSecret)
        }
        
        // Update encryption settings
        // If Keychain access was denied, force encryption to be disabled
        let effectiveEncryptionEnabled = encryptionEnabled && !encryptionKeychainAccessDenied
        content = updateYAMLValue(in: content, section: "database", key: "encryption_enabled", value: effectiveEncryptionEnabled)
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
                content = updateYAMLStringValue(in: content, section: "database", key: "encryption_password", value: "\"[stored-in-keychain]\"")
            } else if KeychainHelper.shared.isEncryptionAccessDenied {
                // User denied Keychain access - disable encryption for security
                print("🚫 Keychain access denied - disabling encryption")
                encryptionEnabled = false
                encryptionKeychainAccessDenied = true
                content = updateYAMLValue(in: content, section: "database", key: "encryption_enabled", value: false)
                content = updateYAMLStringValue(in: content, section: "database", key: "encryption_password", value: "")
            } else {
                // Fallback: save to config file if Keychain fails for other reasons
                print("⚠️ Keychain save failed, storing encryption password in config file")
                content = updateYAMLStringValue(in: content, section: "database", key: "encryption_password", value: encryptionPassword)
            }
        } else {
            content = updateYAMLStringValue(in: content, section: "database", key: "encryption_password", value: "")
            // Clear from Keychain if encryption is disabled
            _ = KeychainHelper.shared.deleteEncryptionPassword()
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
        // Pattern explanation:
        // - Match section header (e.g., "embedding:")
        // - Then any lines (non-greedy) until we find the key
        // - Match the key with optional whitespace and colon
        // - Capture everything up to and including "key: " as group 1
        // - Match the rest of the line (the value to replace) - but NOT greedy across newlines
        let pattern = "(\(section):(?:[^\n]*\n)*?\\s+\(key):[ \\t]*)([^\n]*)"
        
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators]) {
            let range = NSRange(content.startIndex..., in: content)
            if regex.firstMatch(in: content, options: [], range: range) != nil {
                // Escape any special regex characters in the replacement value
                let escapedValue = NSRegularExpression.escapedTemplate(for: value)
                let replacement = "$1\(escapedValue)"
                result = regex.stringByReplacingMatches(in: content, options: [], range: range, withTemplate: replacement)
            } else {
                // Key doesn't exist, add it to the section
                result = addKeyToSection(in: content, section: section, key: key, value: value)
            }
        }
        
        return result
    }
    
    private func updateYAMLIntValue(in content: String, section: String, key: String, value: Int) -> String {
        var result = content
        let pattern = "(\(section):(?:[^\n]*\n)*?\\s+\(key):\\s*)(?:\\d+)"
        
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators]) {
            let range = NSRange(content.startIndex..., in: content)
            if regex.firstMatch(in: content, options: [], range: range) != nil {
                let replacement = "$1\(value)"
                result = regex.stringByReplacingMatches(in: content, options: [], range: range, withTemplate: replacement)
            } else {
                // Key doesn't exist, add it to the section
                result = addKeyToSection(in: content, section: section, key: key, value: "\(value)")
            }
        }
        
        return result
    }
    
    private func updateYAMLValue(in content: String, section: String, key: String, value: Bool) -> String {
        var result = content
        let pattern = "(\(section):(?:[^\n]*\n)*?\\s+\(key):\\s*)(?:true|false)"
        
        if let regex = try? NSRegularExpression(pattern: pattern, options: [.dotMatchesLineSeparators]) {
            let range = NSRange(content.startIndex..., in: content)
            if regex.firstMatch(in: content, options: [], range: range) != nil {
                let replacement = "$1\(value)"
                result = regex.stringByReplacingMatches(in: content, options: [], range: range, withTemplate: replacement)
            } else {
                // Key doesn't exist, add it to the section
                result = addKeyToSection(in: content, section: section, key: key, value: "\(value)")
            }
        }
        
        return result
    }
    
    /// Add a key-value pair to an existing YAML section
    private func addKeyToSection(in content: String, section: String, key: String, value: String) -> String {
        let lines = content.components(separatedBy: "\n")
        var result: [String] = []
        var foundSectionHeader = false
        var sectionHeaderIndex = -1
        var lastKeyInSectionIndex = -1
        var inSection = false
        
        // First pass: find the section header and the last key in that section
        for (index, line) in lines.enumerated() {
            // Look for the exact section header (e.g., "embedding:" at start of line)
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed == "\(section):" || line.hasPrefix("\(section):") && !line.hasPrefix(" ") && !line.hasPrefix("\t") {
                foundSectionHeader = true
                inSection = true
                sectionHeaderIndex = index
                lastKeyInSectionIndex = index // Default to header if no keys
                continue
            }
            
            if inSection {
                // If line is indented and has a colon, it's a key in this section
                let isIndented = line.hasPrefix("  ") || line.hasPrefix("\t")
                let hasColon = line.contains(":")
                let isEmpty = line.trimmingCharacters(in: .whitespaces).isEmpty
                
                if isIndented && hasColon {
                    lastKeyInSectionIndex = index
                }
                // If we hit a non-indented, non-empty line, we've left the section
                else if !isEmpty && !isIndented {
                    inSection = false
                }
            }
        }
        
        // If section doesn't exist, don't add orphaned keys
        if !foundSectionHeader {
            print("⚠️ Section '\(section)' not found, cannot add key '\(key)'")
            return content
        }
        
        // Second pass: build result with new key inserted after last key in section
        for (index, line) in lines.enumerated() {
            result.append(line)
            
            // Insert new key after the last key in the section
            if index == lastKeyInSectionIndex {
                result.append("  \(key): \(value)")
            }
        }
        
        return result.joined(separator: "\n")
    }
    
    static func generateRandomSecret() -> String {
        let characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return String((0..<32).map { _ in characters.randomElement()! })
    }
}

// MARK: - Settings View

struct SettingsView: View {
    @ObservedObject var config: ConfigManager
    let appDelegate: AppDelegate
    @State private var showingSaveAlert = false
    @State private var saveSuccess = false
    @State private var selectedTab = 0
    
    // Track original values to detect changes
    @State private var originalEmbeddingsEnabled: Bool = false
    @State private var originalUseAppleIntelligence: Bool = false
    @State private var originalKmeansEnabled: Bool = false
    @State private var originalAutoTLPEnabled: Bool = false
    @State private var originalHeimdallEnabled: Bool = false
    @State private var originalAutoStartEnabled: Bool = true
    @State private var originalBoltPortNumber: String = "7687"
    @State private var originalHttpPortNumber: String = "7474"
    @State private var originalHostAddress: String = "localhost"
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
               config.useAppleIntelligence != originalUseAppleIntelligence ||
               config.kmeansEnabled != originalKmeansEnabled ||
               config.autoTLPEnabled != originalAutoTLPEnabled ||
               config.heimdallEnabled != originalHeimdallEnabled ||
               config.autoStartEnabled != originalAutoStartEnabled ||
               config.boltPortNumber != originalBoltPortNumber ||
               config.httpPortNumber != originalHttpPortNumber ||
               config.hostAddress != originalHostAddress ||
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
        originalUseAppleIntelligence = config.useAppleIntelligence
        originalKmeansEnabled = config.kmeansEnabled
        originalAutoTLPEnabled = config.autoTLPEnabled
        originalHeimdallEnabled = config.heimdallEnabled
        originalAutoStartEnabled = config.autoStartEnabled
        originalBoltPortNumber = config.boltPortNumber
        originalHttpPortNumber = config.httpPortNumber
        originalHostAddress = config.hostAddress
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
        
        // Pre-generate embedding API key if Apple Intelligence is being enabled
        // This ensures the key is in Keychain before the server reads it
        if config.useAppleIntelligence && config.embeddingsEnabled {
            let apiKey = ConfigManager.getAppleIntelligenceAPIKey()
            print("🔐 Embedding API key ready: \(apiKey.prefix(8))...")
        }
        
        DispatchQueue.global(qos: .userInitiated).async {
            let success = config.saveConfig()
            
            DispatchQueue.main.async {
                if success {
                    saveProgress = "Updating service configuration..."
                    
                    // Manage Apple Intelligence Embedding Server
                    // IMPORTANT: Must start and be ready BEFORE NornicDB restarts
                    if config.useAppleIntelligence && config.embeddingsEnabled {
                        if !appDelegate.embeddingServer.isRunning {
                            saveProgress = "Starting Apple Intelligence..."
                            do {
                                try appDelegate.embeddingServer.start()
                                print("✅ Apple Intelligence embedding server started")
                                // Give server time to be fully ready
                                Thread.sleep(forTimeInterval: 1.0)
                            } catch {
                                print("❌ Failed to start embedding server: \(error)")
                            }
                        }
                    } else {
                        // Stop embedding server if running
                        if appDelegate.embeddingServer.isRunning {
                            saveProgress = "Stopping Apple Intelligence..."
                            appDelegate.embeddingServer.stop()
                            print("🛑 Apple Intelligence embedding server stopped")
                        }
                    }
                    
                    // Update the LaunchAgent plist with current secrets from Keychain
                    self.updateServerPlist()
                    
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                        self.saveProgress = "Restarting server..."
                        
                        // Unload and reload to pick up new plist
                        let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
                        
                        let unloadTask = Process()
                        unloadTask.launchPath = "/usr/bin/env"
                        unloadTask.arguments = ["launchctl", "unload", launchAgentPath]
                        unloadTask.launch()
                        unloadTask.waitUntilExit()
                        
                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                            let loadTask = Process()
                            loadTask.launchPath = "/usr/bin/env"
                            loadTask.arguments = ["launchctl", "load", launchAgentPath]
                            loadTask.launch()
                            loadTask.waitUntilExit()
                            
                            // Wait for restart
                            DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                                self.saveProgress = "Server restarted successfully!"
                                
                                // Close window after short delay
                                DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                                    self.isSaving = false
                                    self.captureOriginalValues() // Update original values
                                    NSApp.keyWindow?.close()
                                }
                            }
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
    
    /// Updates the LaunchAgent plist with current configuration including secrets from Keychain
    private func updateServerPlist() {
        let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
        let homeDir = NSString(string: "~").expandingTildeInPath
        
        // Get secrets from Keychain for environment variables
        let jwtSecretEnv = KeychainHelper.shared.getJWTSecret() ?? config.jwtSecret
        let encryptionPasswordEnv = config.encryptionEnabled ? (KeychainHelper.shared.getEncryptionPassword() ?? config.encryptionPassword) : ""
        
        // Build environment variables section with all settings
        // Using env vars instead of CLI args allows config file to be the source of truth
        var envVars = """
                <key>PATH</key>
                <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
                <key>HOME</key>
                <string>\(homeDir)</string>
                <key>NORNICDB_SERVER_BOLT_PORT</key>
                <string>\(config.boltPortNumber)</string>
                <key>NORNICDB_HTTP_PORT</key>
                <string>\(config.httpPortNumber)</string>
                <key>NORNICDB_SERVER_HOST</key>
                <string>\(config.hostAddress)</string>
                <key>NORNICDB_EMBEDDING_ENABLED</key>
                <string>\(config.embeddingsEnabled ? "true" : "false")</string>
                <key>NORNICDB_EMBEDDING_PROVIDER</key>
                <string>\(config.useAppleIntelligence ? "openai" : "local")</string>
                <key>NORNICDB_EMBEDDING_API_URL</key>
                <string>\(config.useAppleIntelligence ? "http://localhost:\(ConfigManager.appleEmbeddingPort)" : "")</string>
                <key>NORNICDB_EMBEDDING_MODEL</key>
                <string>\(config.useAppleIntelligence ? "apple-ml-embeddings" : config.embeddingModel)</string>
                <key>NORNICDB_EMBEDDING_DIMENSIONS</key>
                <string>\(config.useAppleIntelligence ? "\(ConfigManager.appleEmbeddingDimensions)" : "\(config.embeddingDimensions)")</string>
                <key>NORNICDB_EMBEDDING_API_KEY</key>
                <string>\(config.useAppleIntelligence ? ConfigManager.getAppleIntelligenceAPIKey() : "")</string>
                <key>NORNICDB_SEARCH_MIN_SIMILARITY</key>
                <string>\(config.useAppleIntelligence ? "0" : "0.5")</string>
                <key>NORNICDB_KMEANS_CLUSTERING_ENABLED</key>
                <string>\(config.kmeansEnabled ? "true" : "false")</string>
                <key>NORNICDB_AUTO_TLP_ENABLED</key>
                <string>\(config.autoTLPEnabled ? "true" : "false")</string>
                <key>NORNICDB_HEIMDALL_ENABLED</key>
                <string>\(config.heimdallEnabled ? "true" : "false")</string>
                <key>NORNICDB_HEIMDALL_MODEL</key>
                <string>\(config.heimdallModel)</string>
                <key>NORNICDB_MODELS_DIR</key>
                <string>/usr/local/var/nornicdb/models</string>
                <key>NORNICDB_PLUGINS_DIR</key>
                <string>/usr/local/share/nornicdb/plugins</string>
                <key>NORNICDB_HEIMDALL_PLUGINS_DIR</key>
                <string>/usr/local/share/nornicdb/plugins/heimdall</string>
        """
        
        // Add JWT secret if available (from Keychain)
        if !jwtSecretEnv.isEmpty {
            envVars += """
            
                    <key>NORNICDB_AUTH_JWT_SECRET</key>
                    <string>\(jwtSecretEnv)</string>
            """
        }
        
        // Add encryption settings if encryption is enabled
        if config.encryptionEnabled && !encryptionPasswordEnv.isEmpty {
            envVars += """
            
                    <key>NORNICDB_ENCRYPTION_ENABLED</key>
                    <string>true</string>
                    <key>NORNICDB_ENCRYPTION_PASSWORD</key>
                    <string>\(encryptionPasswordEnv)</string>
            """
        }
        
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
        \(envVars)
            </dict>
            <key>ProcessType</key>
            <string>Interactive</string>
            <key>Nice</key>
            <integer>0</integer>
        </dict>
        </plist>
        """
        
        do {
            try plistContent.write(toFile: launchAgentPath, atomically: true, encoding: .utf8)
            print("✅ Updated server plist with secrets from Keychain")
        } catch {
            print("❌ Failed to update server plist: \(error)")
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
                
                // Apple Intelligence toggle - only show if embeddings are enabled and available
                if config.embeddingsEnabled && AppleMLEmbedder.isAvailable() {
                    VStack(alignment: .leading, spacing: 8) {
                        HStack {
                            Image(systemName: "apple.logo")
                                .font(.title2)
                                .foregroundColor(.accentColor)
                            
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Use Apple Intelligence")
                                    .font(.subheadline)
                                    .fontWeight(.medium)
                                Text("On-device embeddings via Apple ML (512 dims)")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            
                            Spacer()
                            
                            Toggle("", isOn: $config.useAppleIntelligence)
                                .toggleStyle(.switch)
                        }
                        
                        if config.useAppleIntelligence {
                            HStack(spacing: 8) {
                                Image(systemName: "checkmark.circle.fill")
                                    .foregroundColor(.green)
                                    .font(.caption)
                                Text("NornicDB will use local Apple ML for embeddings")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            .padding(.top, 4)
                        }
                    }
                    .padding()
                    .background(RoundedRectangle(cornerRadius: 8).fill(Color.blue.opacity(0.1)))
                }
                
                FeatureToggle(
                    title: "K-Means Clustering",
                    description: "Automatic node clustering and organization",
                    isEnabled: $config.kmeansEnabled,
                    icon: "circle.hexagongrid.fill"
                )
                
                FeatureToggle(
                    title: "Auto-TLP",
                    description: "Automatic Topological Link prediction",
                    isEnabled: $config.autoTLPEnabled,
                    icon: "point.3.connected.trianglepath.dotted"
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
                    Text("Bolt Port")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    Text("The Bolt protocol port (default: 7687)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextField("7687", text: $config.boltPortNumber)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 150)
                }
                .padding()
                .background(RoundedRectangle(cornerRadius: 8).fill(Color.gray.opacity(0.1)))
                
                // HTTP Port setting
                VStack(alignment: .leading, spacing: 8) {
                    Text("HTTP Port")
                        .font(.subheadline)
                        .fontWeight(.medium)
                    Text("The HTTP API port (default: 7474)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextField("7474", text: $config.httpPortNumber)
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
                Text("AI Models")
                    .font(.headline)
                    .padding(.bottom, 5)
                
                Text("Select which models to use for embeddings and AI features. Download models first if none are available.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .padding(.bottom, 10)
                
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
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
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
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding()
        }
    }
    
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
                            config.jwtSecret = ConfigManager.generateRandomSecret()
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
                    
                    // Show warning if Keychain access was denied
                    if config.encryptionKeychainAccessDenied {
                        HStack {
                            Image(systemName: "exclamationmark.triangle.fill")
                                .foregroundColor(.orange)
                            Text("Keychain access was denied. Encryption is disabled for security.")
                                .font(.caption)
                                .foregroundColor(.orange)
                        }
                        .padding(8)
                        .background(Color.orange.opacity(0.1))
                        .cornerRadius(6)
                        
                        Button("Retry Keychain Access") {
                            // Reset the access denied flag and try again
                            KeychainHelper.shared.resetEncryptionAccessDenied()
                            config.encryptionKeychainAccessDenied = false
                            // User can now try enabling encryption again
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    Toggle("Enable Encryption at Rest", isOn: Binding(
                        get: { config.encryptionEnabled },
                        set: { newValue in
                            if newValue && !config.encryptionEnabled {
                                // User is enabling encryption - generate password and try Keychain
                                if config.encryptionPassword.isEmpty {
                                    config.encryptionPassword = ConfigManager.generateRandomSecret()
                                    showEncryptionKey = true  // Show the generated key
                                }
                                // Try to save to Keychain - this will trigger the permission prompt
                                if KeychainHelper.shared.saveEncryptionPassword(config.encryptionPassword) {
                                    config.encryptionEnabled = true
                                    config.encryptionKeychainAccessDenied = false
                                    print("✅ Encryption password saved to Keychain")
                                } else if KeychainHelper.shared.isEncryptionAccessDenied {
                                    // User denied Keychain access
                                    config.encryptionEnabled = false
                                    config.encryptionKeychainAccessDenied = true
                                    config.encryptionPassword = ""
                                    print("🚫 User denied Keychain access - encryption disabled")
                                } else {
                                    // Some other error, but allow encryption anyway
                                    config.encryptionEnabled = true
                                    print("⚠️ Keychain save failed but allowing encryption")
                                }
                            } else {
                                config.encryptionEnabled = newValue
                            }
                        }
                    ))
                    .disabled(config.encryptionKeychainAccessDenied)
                    
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
                                config.encryptionPassword = ConfigManager.generateRandomSecret()
                                showEncryptionKey = true  // Show the newly generated key
                                // Update Keychain with new password
                                _ = KeychainHelper.shared.updateEncryptionPassword(config.encryptionPassword)
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
    let appDelegate: AppDelegate
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
            config.loadConfig()
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
        
        // Pre-generate embedding API key if Apple Intelligence is enabled
        // This ensures the key is in Keychain before the server reads it
        if config.useAppleIntelligence && config.embeddingsEnabled {
            let apiKey = ConfigManager.getAppleIntelligenceAPIKey()
            print("🔐 Embedding API key ready: \(apiKey.prefix(8))...")
        }
        
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
            saveProgress = "Saving configuration..."
            
            // Save configuration
            if config.saveConfig() {
                // Mark first run as complete
                config.completeFirstRun()
                
                // Start Apple Intelligence embedding server if enabled
                // IMPORTANT: Must start and be ready BEFORE NornicDB starts
                if config.useAppleIntelligence && config.embeddingsEnabled && AppleMLEmbedder.isAvailable() {
                    if !appDelegate.embeddingServer.isRunning {
                        saveProgress = "Starting Apple Intelligence..."
                        do {
                            try appDelegate.embeddingServer.start()
                            print("✅ Apple Intelligence embedding server started from wizard")
                            // Give server time to be fully ready
                            Thread.sleep(forTimeInterval: 1.0)
                        } catch {
                            print("❌ Failed to start embedding server from wizard: \(error)")
                        }
                    }
                }
                
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
                        
                        // Get secrets from Keychain for environment variables
                        let jwtSecretEnv = KeychainHelper.shared.getJWTSecret() ?? config.jwtSecret
                        let encryptionPasswordEnv = config.encryptionEnabled ? (KeychainHelper.shared.getEncryptionPassword() ?? config.encryptionPassword) : ""
                        
                        // Build environment variables section with all settings
                        // Using env vars instead of CLI args allows config file to be the source of truth
                        var envVars = """
                                <key>PATH</key>
                                <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
                                <key>HOME</key>
                                <string>\(homeDir)</string>
                                <key>NORNICDB_SERVER_BOLT_PORT</key>
                                <string>\(config.boltPortNumber)</string>
                                <key>NORNICDB_HTTP_PORT</key>
                                <string>\(config.httpPortNumber)</string>
                                <key>NORNICDB_SERVER_HOST</key>
                                <string>\(config.hostAddress)</string>
                                <key>NORNICDB_EMBEDDING_ENABLED</key>
                                <string>\(config.embeddingsEnabled ? "true" : "false")</string>
                                <key>NORNICDB_EMBEDDING_PROVIDER</key>
                                <string>\(config.useAppleIntelligence ? "openai" : "local")</string>
                                <key>NORNICDB_EMBEDDING_API_URL</key>
                                <string>\(config.useAppleIntelligence ? "http://localhost:\(ConfigManager.appleEmbeddingPort)" : "")</string>
                                <key>NORNICDB_EMBEDDING_MODEL</key>
                                <string>\(config.useAppleIntelligence ? "apple-ml-embeddings" : config.embeddingModel)</string>
                                <key>NORNICDB_EMBEDDING_DIMENSIONS</key>
                                <string>\(config.useAppleIntelligence ? "\(ConfigManager.appleEmbeddingDimensions)" : "\(config.embeddingDimensions)")</string>
                                <key>NORNICDB_EMBEDDING_API_KEY</key>
                                <string>\(config.useAppleIntelligence ? ConfigManager.getAppleIntelligenceAPIKey() : "")</string>
                                <key>NORNICDB_SEARCH_MIN_SIMILARITY</key>
                                <string>\(config.useAppleIntelligence ? "0" : "0.5")</string>
                                <key>NORNICDB_KMEANS_CLUSTERING_ENABLED</key>
                                <string>\(config.kmeansEnabled ? "true" : "false")</string>
                                <key>NORNICDB_AUTO_TLP_ENABLED</key>
                                <string>\(config.autoTLPEnabled ? "true" : "false")</string>
                                <key>NORNICDB_HEIMDALL_ENABLED</key>
                                <string>\(config.heimdallEnabled ? "true" : "false")</string>
                                <key>NORNICDB_HEIMDALL_MODEL</key>
                                <string>\(config.heimdallModel)</string>
                                <key>NORNICDB_MODELS_DIR</key>
                                <string>/usr/local/var/nornicdb/models</string>
                                <key>NORNICDB_PLUGINS_DIR</key>
                                <string>/usr/local/share/nornicdb/plugins</string>
                                <key>NORNICDB_HEIMDALL_PLUGINS_DIR</key>
                                <string>/usr/local/share/nornicdb/plugins/heimdall</string>
                        """
                        
                        // Add JWT secret if available (from Keychain)
                        if !jwtSecretEnv.isEmpty {
                            envVars += """
                            
                                    <key>NORNICDB_AUTH_JWT_SECRET</key>
                                    <string>\(jwtSecretEnv)</string>
                            """
                        }
                        
                        // Add encryption settings if encryption is enabled
                        if config.encryptionEnabled && !encryptionPasswordEnv.isEmpty {
                            envVars += """
                            
                                    <key>NORNICDB_ENCRYPTION_ENABLED</key>
                                    <string>true</string>
                                    <key>NORNICDB_ENCRYPTION_PASSWORD</key>
                                    <string>\(encryptionPasswordEnv)</string>
                            """
                        }
                        
                        // Create the plist content with secrets as environment variables
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
                        \(envVars)
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
                            config.jwtSecret = ConfigManager.generateRandomSecret()
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
                    
                    // Show warning if Keychain access was denied
                    if config.encryptionKeychainAccessDenied {
                        HStack {
                            Image(systemName: "exclamationmark.triangle.fill")
                                .foregroundColor(.orange)
                            Text("Keychain access was denied. Encryption is disabled for security.")
                                .font(.caption)
                                .foregroundColor(.orange)
                        }
                        .padding(8)
                        .background(Color.orange.opacity(0.1))
                        .cornerRadius(6)
                        
                        Button("Retry Keychain Access") {
                            // Reset the access denied flag and try again
                            KeychainHelper.shared.resetEncryptionAccessDenied()
                            config.encryptionKeychainAccessDenied = false
                            // User can now try enabling encryption again
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    Toggle("Enable Encryption at Rest", isOn: Binding(
                        get: { config.encryptionEnabled },
                        set: { newValue in
                            if newValue && !config.encryptionEnabled {
                                // User is enabling encryption - generate password and try Keychain
                                if config.encryptionPassword.isEmpty {
                                    config.encryptionPassword = ConfigManager.generateRandomSecret()
                                    showEncryptionKey = true  // Show the generated key
                                }
                                // Try to save to Keychain - this will trigger the permission prompt
                                if KeychainHelper.shared.saveEncryptionPassword(config.encryptionPassword) {
                                    config.encryptionEnabled = true
                                    config.encryptionKeychainAccessDenied = false
                                    print("✅ Encryption password saved to Keychain")
                                } else if KeychainHelper.shared.isEncryptionAccessDenied {
                                    // User denied Keychain access
                                    config.encryptionEnabled = false
                                    config.encryptionKeychainAccessDenied = true
                                    config.encryptionPassword = ""
                                    print("🚫 User denied Keychain access - encryption disabled")
                                } else {
                                    // Some other error, but allow encryption anyway
                                    config.encryptionEnabled = true
                                    print("⚠️ Keychain save failed but allowing encryption")
                                }
                            } else {
                                config.encryptionEnabled = newValue
                            }
                        }
                    ))
                    .disabled(config.encryptionKeychainAccessDenied)
                    
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
                                config.encryptionPassword = ConfigManager.generateRandomSecret()
                                showEncryptionKey = true  // Show the newly generated key
                                // Update Keychain with new password
                                _ = KeychainHelper.shared.updateEncryptionPassword(config.encryptionPassword)
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
                
                // Apple Intelligence Option
                if (selectedPreset == .standard || selectedPreset == .advanced) && AppleMLEmbedder.isAvailable() {
                    Divider()
                    
                    VStack(alignment: .leading, spacing: 15) {
                        Text("Embedding Provider")
                            .font(.headline)
                        
                        VStack(alignment: .leading, spacing: 12) {
                            Button(action: {
                                config.useAppleIntelligence = true
                            }) {
                                HStack(spacing: 12) {
                                    Image(systemName: "apple.logo")
                                        .font(.title2)
                                        .foregroundColor(.accentColor)
                                        .frame(width: 30)
                                    
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text("Use Apple Intelligence")
                                            .font(.subheadline)
                                            .fontWeight(.medium)
                                            .foregroundColor(.primary)
                                        Text("On-device, privacy-first embeddings (512 dims)")
                                            .font(.caption)
                                            .foregroundColor(.secondary)
                                        HStack(spacing: 4) {
                                            Image(systemName: "checkmark.circle.fill")
                                                .foregroundColor(.green)
                                                .font(.caption2)
                                            Text("No download required • Zero cost • Private")
                                                .font(.caption2)
                                                .foregroundColor(.green)
                                        }
                                    }
                                    
                                    Spacer()
                                    
                                    if config.useAppleIntelligence {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundColor(.green)
                                            .font(.title3)
                                    }
                                }
                                .padding()
                                .background(
                                    RoundedRectangle(cornerRadius: 8)
                                        .fill(config.useAppleIntelligence ? Color.blue.opacity(0.1) : Color.gray.opacity(0.05))
                                )
                                .overlay(
                                    RoundedRectangle(cornerRadius: 8)
                                        .stroke(config.useAppleIntelligence ? Color.blue : Color.clear, lineWidth: 2)
                                )
                            }
                            .buttonStyle(.plain)
                            
                            Button(action: {
                                config.useAppleIntelligence = false
                            }) {
                                HStack(spacing: 12) {
                                    Image(systemName: "externaldrive.fill")
                                        .font(.title2)
                                        .foregroundColor(.purple)
                                        .frame(width: 30)
                                    
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text("Use Local GGUF Models")
                                            .font(.subheadline)
                                            .fontWeight(.medium)
                                            .foregroundColor(.primary)
                                        Text("Download BGE-M3 model (1024 dims)")
                                            .font(.caption)
                                            .foregroundColor(.secondary)
                                        HStack(spacing: 4) {
                                            Image(systemName: "arrow.down.circle.fill")
                                                .foregroundColor(.orange)
                                                .font(.caption2)
                                            Text("~400MB download • Higher dimensions")
                                                .font(.caption2)
                                                .foregroundColor(.orange)
                                        }
                                    }
                                    
                                    Spacer()
                                    
                                    if !config.useAppleIntelligence {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundColor(.green)
                                            .font(.title3)
                                    }
                                }
                                .padding()
                                .background(
                                    RoundedRectangle(cornerRadius: 8)
                                        .fill(!config.useAppleIntelligence ? Color.purple.opacity(0.1) : Color.gray.opacity(0.05))
                                )
                                .overlay(
                                    RoundedRectangle(cornerRadius: 8)
                                        .stroke(!config.useAppleIntelligence ? Color.purple : Color.clear, lineWidth: 2)
                                )
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding()
                }
                
                // Model Requirements Section (only show if NOT using Apple Intelligence)
                if needsModels() && !config.useAppleIntelligence {
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
            
            // Default to Apple Intelligence if available for Standard/Advanced presets
            if AppleMLEmbedder.isAvailable() && (selectedPreset == .standard || selectedPreset == .advanced) {
                config.useAppleIntelligence = true
            }
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


import SwiftUI
import AppKit
import Foundation
import Security
import Yams

private let defaultQwenHeimdallFileName = "qwen3-0.6b-instruct.gguf"
private let defaultQwenHeimdallDownloadURL = "https://huggingface.co/unsloth/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q4_K_M.gguf"
private let ggufMagicHeader = Data([0x47, 0x47, 0x55, 0x46])

private func shellQuoted(_ value: String) -> String {
    return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
}

private func isValidGGUFFile(atPath path: String) -> Bool {
    guard FileManager.default.fileExists(atPath: path) else {
        return false
    }

    let url = URL(fileURLWithPath: path)
    guard let handle = try? FileHandle(forReadingFrom: url) else {
        return false
    }
    defer {
        try? handle.close()
    }

    guard let header = try? handle.read(upToCount: ggufMagicHeader.count),
          header == ggufMagicHeader else {
        return false
    }

    return true
}

@discardableResult
private func removeInvalidGGUFFile(atPath path: String) -> Bool {
    guard FileManager.default.fileExists(atPath: path), !isValidGGUFFile(atPath: path) else {
        return false
    }

    try? FileManager.default.removeItem(atPath: path)
    return true
}

private func modelDownloadFailureMessage(modelsPath: String) -> String {
    return "Model download failed. Please download manually and place in \(modelsPath)"
}

private func appDisplayVersion() -> String {
    if let short = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String,
       !short.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        return short
    }
    if let build = Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String,
       !build.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        return build
    }
    return "unknown"
}

private func appBuildArchitecture() -> String {
#if arch(arm64)
    return "arm64"
#elseif arch(x86_64)
    return "x86_64"
#else
    return "unknown"
#endif
}

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
        let result = saveSecret(token, account: apiTokenAccount, overwrite: false)
        if result {
            cachedAPIToken = token  // Update cache to avoid re-prompting Keychain
        }
        return result
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
        ensureServerLaunchAgentExists()

        if !isLaunchAgentLoaded(label: "com.nornicdb.server") {
            _ = executeLaunchctl(arguments: ["load", serverLaunchAgentPath])
        }

        _ = executeLaunchctl(arguments: ["start", "com.nornicdb.server"], wait: false)
        DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
            self.checkHealth()
        }
    }
    
    @objc func stopServer() {
        if isLaunchAgentLoaded(label: "com.nornicdb.server") {
            _ = executeLaunchctl(arguments: ["unload", serverLaunchAgentPath])
        }
        self.updateStatus(.stopped)
    }
    
    @objc func restartServer() {
        updateStatus(.starting)
        ensureServerLaunchAgentExists()

        if isLaunchAgentLoaded(label: "com.nornicdb.server") {
            _ = executeLaunchctl(arguments: ["kickstart", "-k", launchdServiceTarget(for: "com.nornicdb.server")], wait: false)
        } else {
            _ = executeLaunchctl(arguments: ["load", serverLaunchAgentPath])
            _ = executeLaunchctl(arguments: ["start", "com.nornicdb.server"], wait: false)
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
            self.checkHealth()
        }
    }

    private var serverLaunchAgentPath: String {
        return NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
    }

    private func ensureServerLaunchAgentExists() {
        guard !FileManager.default.fileExists(atPath: serverLaunchAgentPath) else {
            return
        }

        do {
            try writeLaunchAgentPlist(config: configManager, to: serverLaunchAgentPath)
        } catch {
            print("Failed to create server LaunchAgent plist: \(error)")
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
        alert.informativeText = "This will download:\n• BGE-M3 embedding model (~400MB)\n• BGE reranker model (~440MB)\n• qwen3-0.6b-Instruct model (~350MB)\n\nTotal: ~890MB\n\nDownloading from HuggingFace..."
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
                let modelsPath = self.configManager.modelsPath
                let bgePath = "\(modelsPath)/bge-m3.gguf"
                let bgeRerankerPath = "\(modelsPath)/bge-reranker-v2-m3-Q4_K_M.gguf"
                let qwenPath = "\(modelsPath)/\(defaultQwenHeimdallFileName)"
                let task = Process()
                task.launchPath = "/bin/bash"
                task.arguments = ["-c", "mkdir -p \(shellQuoted(modelsPath)) && curl -fL -o \(shellQuoted(bgePath)) https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf && curl -fL -o \(shellQuoted(bgeRerankerPath)) https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF/resolve/main/bge-reranker-v2-m3-Q4_K_M.gguf && curl -fL -o \(shellQuoted(qwenPath)) \(shellQuoted(defaultQwenHeimdallDownloadURL))"]
                
                // Create models directory first
                try? FileManager.default.createDirectory(atPath: modelsPath, withIntermediateDirectories: true, attributes: nil)
                
                task.launch()
                task.waitUntilExit()
                
                DispatchQueue.main.async {
                    let result = NSAlert()
                    let downloadsValid = isValidGGUFFile(atPath: bgePath)
                        && isValidGGUFFile(atPath: bgeRerankerPath)
                        && isValidGGUFFile(atPath: qwenPath)
                    if task.terminationStatus == 0 && downloadsValid {
                        result.messageText = "Download Complete"
                        result.informativeText = "Models downloaded successfully!\n\nYou can now select them in Settings → Models tab."
                        result.alertStyle = .informational
                    } else {
                        _ = removeInvalidGGUFFile(atPath: bgePath)
                        _ = removeInvalidGGUFFile(atPath: bgeRerankerPath)
                        _ = removeInvalidGGUFFile(atPath: qwenPath)
                        result.messageText = "Download Failed"
                        result.informativeText = modelDownloadFailureMessage(modelsPath: modelsPath)
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
        let modelsPath = configManager.modelsPath
        
        // Create directory if it doesn't exist
        try? FileManager.default.createDirectory(atPath: modelsPath, withIntermediateDirectories: true, attributes: nil)
        
        NSWorkspace.shared.open(URL(fileURLWithPath: modelsPath))
    }
    
    @objc func showAbout() {
        let alert = NSAlert()
        alert.messageText = "NornicDB"
        alert.informativeText = "High-Performance Graph Database\n\nVersion: \(appDisplayVersion())\nBuild: \(appBuildArchitecture())"
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

private func launchdServiceTarget(for label: String) -> String {
    return "gui/\(getuid())/\(label)"
}

@discardableResult
private func executeLaunchctl(arguments: [String], wait: Bool = true) -> Int32? {
    let task = Process()
    task.launchPath = "/usr/bin/env"
    task.arguments = ["launchctl"] + arguments

    do {
        try task.run()
        if wait {
            task.waitUntilExit()
            return task.terminationStatus
        }
        return nil
    } catch {
        print("launchctl command failed: \(arguments.joined(separator: " ")): \(error)")
        return nil
    }
}

private func isLaunchAgentLoaded(label: String) -> Bool {
    return executeLaunchctl(arguments: ["print", launchdServiceTarget(for: label)]) == 0
}

// MARK: - Config Manager

class ConfigManager: ObservableObject {
    static let defaultEmbeddingModel = "bge-m3.gguf"
    static let defaultSearchRerankModel = "bge-reranker-v2-m3-Q4_K_M.gguf"
    static let defaultHeimdallModel = defaultQwenHeimdallFileName

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
    private let configPath = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath
    
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
            print("⚠️ Failed to parse YAML with Yams: \(error)")
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

        // Load embedding section
        let embeddingSection = yamlSection(root, "embedding")
        if let enabled = yamlBool(embeddingSection["enabled"]) {
            embeddingsEnabled = enabled
            print("✅ Loaded embeddings enabled: \(embeddingsEnabled)")
        }
        if let model = yamlString(embeddingSection["model"]) {
            embeddingModel = normalizeModelName(model, fallback: ConfigManager.defaultEmbeddingModel)
            print("✅ Loaded embedding model: \(embeddingModel)")
        }
        if let provider = yamlString(embeddingSection["provider"]) {
            let url = yamlString(embeddingSection["url"]) ?? ""
            useAppleIntelligence = provider == "openai" && url.contains("localhost:\(ConfigManager.appleEmbeddingPort)")
            print("✅ Loaded use Apple Intelligence: \(useAppleIntelligence)")
        }
        if let dims = yamlInt(embeddingSection["dimensions"]), dims > 0 {
            embeddingDimensions = dims
            print("✅ Loaded embedding dimensions: \(dims)")
        }

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

        // Load search_rerank section
        let searchRerankSection = yamlSection(root, "search_rerank")
        if let enabled = yamlBool(searchRerankSection["enabled"]) {
            searchRerankEnabled = enabled
            print("✅ Loaded search_rerank enabled: \(searchRerankEnabled)")
        }
        if let model = yamlString(searchRerankSection["model"]) {
            searchRerankModel = normalizeModelName(model, fallback: ConfigManager.defaultSearchRerankModel)
            print("✅ Loaded search_rerank model: \(searchRerankModel)")
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

        // Load heimdall section
        let heimdallSection = yamlSection(root, "heimdall")
        if let enabled = yamlBool(heimdallSection["enabled"]) {
            heimdallEnabled = enabled
            print("✅ Loaded heimdall enabled: \(heimdallEnabled)")
        }
        if let model = yamlString(heimdallSection["model"]) {
            heimdallModel = normalizeModelName(model, fallback: ConfigManager.defaultHeimdallModel)
            print("✅ Loaded heimdall model: \(heimdallModel)")
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
        
        // Ensure we always have valid model names in memory for settings/save.
        embeddingModel = normalizeModelName(embeddingModel, fallback: ConfigManager.defaultEmbeddingModel)
        searchRerankModel = normalizeModelName(searchRerankModel, fallback: ConfigManager.defaultSearchRerankModel)
        heimdallModel = normalizeModelName(heimdallModel, fallback: ConfigManager.defaultHeimdallModel)
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

    private func normalizeModelName(_ raw: String, fallback: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return fallback
        }
        if trimmed == "apple-ml-embeddings" {
            return trimmed
        }
        if trimmed.hasSuffix(".gguf") {
            return trimmed
        }
        return "\(trimmed).gguf"
    }
    
    func saveConfig() -> Bool {
        var root = loadYAMLRoot()

        var authSection = yamlSection(root, "auth")
        var databaseSection = yamlSection(root, "database")
        var embeddingSection = yamlSection(root, "embedding")
        var embeddingWorkerSection = yamlSection(root, "embedding_worker")
        var kmeansSection = yamlSection(root, "kmeans")
        var searchRerankSection = yamlSection(root, "search_rerank")
        var memorySection = yamlSection(root, "memory")
        var autoTLPSection = yamlSection(root, "auto_tlp")
        var heimdallSection = yamlSection(root, "heimdall")
        var serverSection = yamlSection(root, "server")

        embeddingSection["enabled"] = embeddingsEnabled
        kmeansSection["enabled"] = kmeansEnabled
        searchRerankSection["enabled"] = searchRerankEnabled
        searchRerankSection["provider"] = "local"
        searchRerankSection["model"] = normalizeModelName(searchRerankModel, fallback: ConfigManager.defaultSearchRerankModel)
        memorySection["decay_enabled"] = memoryDecayEnabled
        autoTLPSection["enabled"] = autoTLPEnabled
        heimdallSection["enabled"] = heimdallEnabled

        if useAppleIntelligence {
            embeddingChunkSize = ConfigManager.appleEmbeddingChunkSize
            embeddingSection["provider"] = "openai"
            embeddingSection["url"] = "http://localhost:\(ConfigManager.appleEmbeddingPort)"
            embeddingSection["model"] = "apple-ml-embeddings"
            embeddingSection["dimensions"] = ConfigManager.appleEmbeddingDimensions
        } else {
            embeddingSection["provider"] = "local"
            embeddingSection["url"] = ""
            embeddingSection["model"] = normalizeModelName(embeddingModel, fallback: ConfigManager.defaultEmbeddingModel)
            embeddingSection["dimensions"] = embeddingDimensions
        }
        heimdallSection["model"] = normalizeModelName(heimdallModel, fallback: ConfigManager.defaultHeimdallModel)

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
        root["embedding"] = embeddingSection
        root["embedding_worker"] = embeddingWorkerSection
        root["kmeans"] = kmeansSection
        root["search_rerank"] = searchRerankSection
        root["memory"] = memorySection
        root["auto_tlp"] = autoTLPSection
        root["heimdall"] = heimdallSection
        root["server"] = serverSection
        // Write back
        do {
            let normalizedRoot = normalizeConfigRootForGoSchema(root)
            let content = try Yams.dump(object: normalizedRoot)
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
        let isLoaded = isLaunchAgentLoaded(label: "com.nornicdb.server")
        
        if autoStartEnabled && !isLoaded {
            // Load launch agent
            _ = executeLaunchctl(arguments: ["load", launchAgentPath], wait: false)
        } else if !autoStartEnabled && isLoaded {
            // Unload launch agent
            _ = executeLaunchctl(arguments: ["unload", launchAgentPath], wait: false)
        }
    }
    
    static func generateRandomSecret() -> String {
        let characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        return String((0..<32).map { _ in characters.randomElement()! })
    }
}

private func launchAgentEnvironmentVariables(config: ConfigManager, homeDir: String) -> [String: String] {
    let jwtSecretEnv = KeychainHelper.shared.getJWTSecret() ?? config.jwtSecret
    let encryptionPasswordEnv = config.encryptionEnabled ? (KeychainHelper.shared.getEncryptionPassword() ?? config.encryptionPassword) : ""

    var envVars: [String: String] = [
        "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": homeDir,
        "NORNICDB_SERVER_BOLT_PORT": config.boltPortNumber,
        "NORNICDB_HTTP_PORT": config.httpPortNumber,
        "NORNICDB_SERVER_HOST": config.hostAddress,
        "NORNICDB_EMBEDDING_ENABLED": config.embeddingsEnabled ? "true" : "false",
        "NORNICDB_EMBEDDING_PROVIDER": config.useAppleIntelligence ? "openai" : "local",
        "NORNICDB_EMBEDDING_API_URL": config.useAppleIntelligence ? "http://localhost:\(ConfigManager.appleEmbeddingPort)" : "",
        "NORNICDB_EMBEDDING_MODEL": config.useAppleIntelligence ? "apple-ml-embeddings" : config.embeddingModel,
        "NORNICDB_EMBEDDING_DIMENSIONS": config.useAppleIntelligence ? "\(ConfigManager.appleEmbeddingDimensions)" : "\(config.embeddingDimensions)",
        "NORNICDB_EMBEDDING_API_KEY": config.useAppleIntelligence ? ConfigManager.getAppleIntelligenceAPIKey() : "",
        "NORNICDB_EMBED_CHUNK_SIZE": "\(config.effectiveEmbeddingChunkSize())",
        "NORNICDB_SEARCH_MIN_SIMILARITY": config.useAppleIntelligence ? "0" : "0.5",
        "NORNICDB_KMEANS_CLUSTERING_ENABLED": config.kmeansEnabled ? "true" : "false",
        "NORNICDB_SEARCH_RERANK_ENABLED": config.searchRerankEnabled ? "true" : "false",
        "NORNICDB_SEARCH_RERANK_PROVIDER": "local",
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

    return envVars
}

private func launchAgentPropertyList(config: ConfigManager, homeDir: String) -> [String: Any] {
    return [
        "Label": "com.nornicdb.server",
        "ProgramArguments": ["/usr/local/bin/nornicdb", "serve"],
        "WorkingDirectory": "/usr/local/var/nornicdb",
        "RunAtLoad": true,
        "KeepAlive": [
            "SuccessfulExit": false,
            "Crashed": true,
        ],
        "ThrottleInterval": 30,
        "StandardOutPath": "/usr/local/var/log/nornicdb/stdout.log",
        "StandardErrorPath": "/usr/local/var/log/nornicdb/stderr.log",
        "EnvironmentVariables": launchAgentEnvironmentVariables(config: config, homeDir: homeDir),
        "ProcessType": "Interactive",
        "Nice": 0,
    ]
}

private func writeLaunchAgentPlist(config: ConfigManager, to launchAgentPath: String) throws {
    let homeDir = NSString(string: "~").expandingTildeInPath
    let plist = launchAgentPropertyList(config: config, homeDir: homeDir)
    let plistData = try PropertyListSerialization.data(fromPropertyList: plist, format: .xml, options: 0)
    try plistData.write(to: URL(fileURLWithPath: launchAgentPath), options: .atomic)
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
    @State private var originalSearchRerankEnabled: Bool = false
    @State private var originalMemoryDecayEnabled: Bool = true
    @State private var originalAutoTLPEnabled: Bool = false
    @State private var originalHeimdallEnabled: Bool = false
    @State private var originalAutoStartEnabled: Bool = true
    @State private var originalBoltPortNumber: String = "7687"
    @State private var originalHttpPortNumber: String = "7474"
    @State private var originalHostAddress: String = "localhost"
    @State private var originalEmbeddingModel: String = ConfigManager.defaultEmbeddingModel
    @State private var originalEmbeddingDimensions: Int = 1024
    @State private var originalEmbeddingChunkSize: Int = ConfigManager.localEmbeddingChunkSize
    @State private var originalEmbeddingChunkOverlap: Int = ConfigManager.defaultEmbeddingChunkOverlap
    @State private var originalSearchRerankModel: String = ConfigManager.defaultSearchRerankModel
    @State private var originalHeimdallModel: String = ConfigManager.defaultHeimdallModel
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

    // Storage-upgrade restart flow
    @State private var showStorageUpgradeConfirm: Bool = false
    @State private var isRunningStorageUpgrade: Bool = false
    
    // Check if there are unsaved changes
    var hasChanges: Bool {
        return config.embeddingsEnabled != originalEmbeddingsEnabled ||
               config.useAppleIntelligence != originalUseAppleIntelligence ||
               config.kmeansEnabled != originalKmeansEnabled ||
               config.searchRerankEnabled != originalSearchRerankEnabled ||
             config.memoryDecayEnabled != originalMemoryDecayEnabled ||
               config.autoTLPEnabled != originalAutoTLPEnabled ||
               config.heimdallEnabled != originalHeimdallEnabled ||
               config.autoStartEnabled != originalAutoStartEnabled ||
               config.boltPortNumber != originalBoltPortNumber ||
               config.httpPortNumber != originalHttpPortNumber ||
               config.hostAddress != originalHostAddress ||
               config.embeddingModel != originalEmbeddingModel ||
               config.embeddingDimensions != originalEmbeddingDimensions ||
               config.embeddingChunkSize != originalEmbeddingChunkSize ||
               config.embeddingChunkOverlap != originalEmbeddingChunkOverlap ||
               config.searchRerankModel != originalSearchRerankModel ||
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
        .alert("Restart with storage upgrade?", isPresented: $showStorageUpgradeConfirm) {
            Button("Cancel", role: .cancel) {}
            Button("Stop & Restart with Upgrade", role: .destructive) {
                runStorageUpgradeRestart()
            }
        } message: {
            Text("This will stop the running server and restart it with the --upgrade-storage flag. If the on-disk format is older than this build, it will be migrated; otherwise the flag is a no-op. Storage upgrades are one-way — back up \(config.dataPath) before continuing.")
        }
    }

    private func captureOriginalValues() {
        // Reload config from file to ensure we have the latest values
        config.loadConfig()
        
        // Capture current values as originals
        originalEmbeddingsEnabled = config.embeddingsEnabled
        originalUseAppleIntelligence = config.useAppleIntelligence
        originalKmeansEnabled = config.kmeansEnabled
        originalSearchRerankEnabled = config.searchRerankEnabled
        originalMemoryDecayEnabled = config.memoryDecayEnabled
        originalAutoTLPEnabled = config.autoTLPEnabled
        originalHeimdallEnabled = config.heimdallEnabled
        originalAutoStartEnabled = config.autoStartEnabled
        originalBoltPortNumber = config.boltPortNumber
        originalHttpPortNumber = config.httpPortNumber
        originalHostAddress = config.hostAddress
        originalEmbeddingModel = config.embeddingModel
        originalEmbeddingDimensions = config.embeddingDimensions
        originalEmbeddingChunkSize = config.embeddingChunkSize
        originalEmbeddingChunkOverlap = config.embeddingChunkOverlap
        originalSearchRerankModel = config.searchRerankModel
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
        do {
            try writeLaunchAgentPlist(config: config, to: launchAgentPath)
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
                            
                            // Warning about Apple Intelligence limitations
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: "info.circle")
                                    .foregroundColor(.orange)
                                    .font(.caption)
                                Text("Best for simple text search. For code, technical docs, or complex content, use a dedicated model like BGE-M3 for better results.")
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }
                            .padding(.top, 2)
                        }
                    }
                    .padding()
                    .background(RoundedRectangle(cornerRadius: 8).fill(Color.blue.opacity(0.1)))
                }

                if config.embeddingsEnabled {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack {
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Embedding Chunk Size")
                                    .font(.subheadline)
                                    .fontWeight(.medium)
                                Text(config.useAppleIntelligence
                                     ? "Fixed to Apple ML supported maximum"
                                     : "Controls max tokens per embedding chunk")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            Spacer()
                            Stepper(value: $config.embeddingChunkSize, in: 128...ConfigManager.localEmbeddingChunkSize, step: 64) {
                                Text("\(config.embeddingChunkSize)")
                                    .frame(minWidth: 70, alignment: .trailing)
                            }
                            .disabled(config.useAppleIntelligence)
                        }

                        HStack {
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Embedding Chunk Overlap")
                                    .font(.subheadline)
                                    .fontWeight(.medium)
                                Text(config.useAppleIntelligence
                                     ? "Overlap between chunks for local indexing"
                                     : "Controls how much context is preserved between chunks")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            Spacer()
                            Stepper(value: $config.embeddingChunkOverlap, in: 0...max(0, config.embeddingChunkSize - 1), step: 16) {
                                Text("\(config.embeddingChunkOverlap)")
                                    .frame(minWidth: 70, alignment: .trailing)
                            }
                        }
                    }
                    .padding()
                    .background(RoundedRectangle(cornerRadius: 8).fill(Color.secondary.opacity(0.08)))
                    .onChange(of: config.useAppleIntelligence) { useApple in
                        if useApple {
                            config.embeddingChunkSize = ConfigManager.appleEmbeddingChunkSize
                        } else if config.embeddingChunkSize <= 0 {
                            config.embeddingChunkSize = ConfigManager.localEmbeddingChunkSize
                        }
                        config.embeddingChunkOverlap = min(max(0, config.embeddingChunkOverlap), max(0, config.embeddingChunkSize - 1))
                    }
                    .onChange(of: config.embeddingChunkSize) { newChunkSize in
                        config.embeddingChunkOverlap = min(max(0, config.embeddingChunkOverlap), max(0, newChunkSize - 1))
                    }
                }
                
                FeatureToggle(
                    title: "K-Means Clustering",
                    description: "Automatic node clustering and organization",
                    isEnabled: $config.kmeansEnabled,
                    icon: "circle.hexagongrid.fill"
                )

                FeatureToggle(
                    title: "Search Reranking",
                    description: "Stage-2 reranking for improved result relevance",
                    isEnabled: $config.searchRerankEnabled,
                    icon: "line.3.horizontal.decrease.circle.fill"
                )

                FeatureToggle(
                    title: "Memory Decay",
                    description: "Natural episodic, semantic, and procedural memory decay",
                    isEnabled: $config.memoryDecayEnabled,
                    icon: "brain.filled.head.profile"
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
                storageUpgradeMaintenance

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
                            .onChange(of: config.embeddingModel) { newModel in
                                // Auto-set dimensions based on known model defaults
                                config.embeddingDimensions = ConfigManager.suggestedDimensions(for: newModel)
                            }
                            
                            // Embedding Dimensions selector
                            HStack(spacing: 12) {
                                Text("Dimensions")
                                    .font(.subheadline)
                                    .fontWeight(.semibold)
                                    .foregroundColor(.primary)
                                
                                Picker("", selection: $config.embeddingDimensions) {
                                    Text("384").tag(384)
                                    Text("512").tag(512)
                                    Text("768").tag(768)
                                    Text("1024").tag(1024)
                                    Text("1536").tag(1536)
                                    Text("3072").tag(3072)
                                }
                                .pickerStyle(.menu)
                                .labelsHidden()
                                .frame(width: 90)
                                
                                Text("\(config.embeddingDimensions)")
                                    .font(.caption)
                                    .monospacedDigit()
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 4)
                                    .background(Color.secondary.opacity(0.18))
                                    .cornerRadius(6)
                                
                                Text("must match model output")
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                            }
                            .padding(.top, 4)
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

                        // Search Reranker Model Selection
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Search Reranker Model")
                                .font(.subheadline)
                                .fontWeight(.medium)

                            Text("Used for Stage-2 reranking in hybrid/vector search")
                                .font(.caption)
                                .foregroundColor(.secondary)

                            Picker("Search Reranker Model", selection: $config.searchRerankModel) {
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
    
    // Always-visible maintenance section on the Server tab. The
    // installer can't detect whether an upgrade is actually pending
    // (the server has to start to find out, and it refuses to start
    // when one is required), so this is a manual operator escape
    // hatch: stop the LaunchAgent, rewrite the plist with
    // NORNICDB_UPGRADE_STORAGE=true, reload. If no upgrade is
    // pending the env var is a no-op and the server starts normally.
    var storageUpgradeMaintenance: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 10) {
                Image(systemName: "arrow.up.circle.fill")
                    .font(.title2)
                    .foregroundColor(.orange)
                Text("Storage upgrade")
                    .font(.headline)
                Spacer()
            }

            Text("If the server fails to start with a storage-upgrade error, use this to restart it with --upgrade-storage. Storage upgrades are one-way — back up \(config.dataPath) before continuing. If no upgrade is needed, this is a no-op.")
                .font(.caption)
                .foregroundColor(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            Button(action: {
                showStorageUpgradeConfirm = true
            }) {
                HStack(spacing: 6) {
                    if isRunningStorageUpgrade {
                        ProgressView().scaleEffect(0.6)
                    } else {
                        Image(systemName: "arrow.up.circle.fill")
                    }
                    Text(isRunningStorageUpgrade ? "Restarting with upgrade…" : "Stop server and restart with storage upgrade")
                        .fontWeight(.medium)
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(.orange)
            .disabled(isRunningStorageUpgrade)
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 10)
                .fill(Color.orange.opacity(0.12))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 10)
                .stroke(Color.orange, lineWidth: 2)
        )
    }

    private func runStorageUpgradeRestart() {
        isRunningStorageUpgrade = true

        // Set the one-shot flag and rewrite the plist so launchd picks
        // up the new env var on the next start. After the upgrade
        // completes we'll clear the flag and rewrite again so subsequent
        // restarts don't re-authorize the upgrade arm.
        config.upgradeStorageOnNextStart = true
        let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
        do {
            try writeLaunchAgentPlist(config: config, to: launchAgentPath)
        } catch {
            print("Failed to write LaunchAgent plist: \(error)")
            isRunningStorageUpgrade = false
            return
        }

        // Unload + reload so launchd reads the updated plist.
        DispatchQueue.global(qos: .userInitiated).async {
            let unload = Process()
            unload.launchPath = "/usr/bin/env"
            unload.arguments = ["launchctl", "unload", launchAgentPath]
            unload.launch()
            unload.waitUntilExit()

            Thread.sleep(forTimeInterval: 0.5)

            let load = Process()
            load.launchPath = "/usr/bin/env"
            load.arguments = ["launchctl", "load", launchAgentPath]
            load.launch()
            load.waitUntilExit()

            // Wait for the server to come back up, then clear the
            // one-shot flag and rewrite the plist without the env var.
            DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
                waitForServerHealthAfterUpgrade(attempts: 20) { success in
                    if success {
                        config.upgradeStorageOnNextStart = false
                        try? writeLaunchAgentPlist(config: config, to: launchAgentPath)
                    }
                    isRunningStorageUpgrade = false
                }
            }
        }
    }

    private func waitForServerHealthAfterUpgrade(attempts: Int, completion: @escaping (Bool) -> Void) {
        guard attempts > 0 else {
            completion(false)
            return
        }
        let url = URL(string: "http://localhost:7474/health")!
        var request = URLRequest(url: url)
        request.timeoutInterval = 2.0
        URLSession.shared.dataTask(with: request) { _, response, _ in
            DispatchQueue.main.async {
                if let http = response as? HTTPURLResponse, http.statusCode == 200 {
                    completion(true)
                } else {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                        waitForServerHealthAfterUpgrade(attempts: attempts - 1, completion: completion)
                    }
                }
            }
        }.resume()
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
    @State private var hasExplicitEmbeddingProviderSelection = false
    let onComplete: () -> Void
    
    @State private var isDownloadingModels: Bool = false
    @State private var downloadProgress: String = ""
    @State private var bgeModelExists: Bool = false
    @State private var bgeRerankerModelExists: Bool = false
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

                Text("Version \(appDisplayVersion())")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
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
                        // Write the plist file
                        do {
                            try writeLaunchAgentPlist(config: config, to: launchAgentPath)
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

                                // Clear the one-shot upgrade flag so future
                                // restarts don't re-authorize the upgrade arm,
                                // and rewrite the plist to drop the env var.
                                if config.upgradeStorageOnNextStart {
                                    config.upgradeStorageOnNextStart = false
                                    let launchAgentPath = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
                                    try? writeLaunchAgentPlist(config: config, to: launchAgentPath)
                                }

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
        ScrollView {
            VStack(spacing: 20) {
                storageUpgradePrompt

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
                        features: ["All basic features", "Vector embeddings", "Balanced local AI setup"]
                    )
                    
                    PresetOption(
                        preset: .advanced,
                        selected: $selectedPreset,
                        title: "Advanced",
                        subtitle: "Full AI capabilities",
                        features: ["All standard features", "Search reranking", "Heimdall AI guardian", "Auto-predictions"]
                    )
                }
                .padding()
            }
            .padding()
        }
        .overlay(alignment: .bottom) {
            wizardScrollHint
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
        .overlay(alignment: .bottom) {
            wizardScrollHint
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
                    FeatureSummary(enabled: getPresetFeatures().searchRerank, title: "Search Reranking", icon: "line.3.horizontal.decrease.circle.fill")
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
                                hasExplicitEmbeddingProviderSelection = true
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
                                hasExplicitEmbeddingProviderSelection = true
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
                
                // Model requirements vary by preset. Standard needs embeddings only;
                // Advanced adds the reranker and Heimdall models.
                if needsModels() {
                    Divider()
                    
                    VStack(spacing: 15) {
                        HStack {
                            Text("AI Models Required")
                                .font(.headline)
                            
                            Spacer()
                            
                            Button(action: {
                                let modelsPath = config.modelsPath
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
                            // Embedding model is only required when using local GGUF embeddings.
                            if !config.useAppleIntelligence && (selectedPreset == .standard || selectedPreset == .advanced) {
                                ModelDownloadRow(
                                    modelName: "BGE-M3 Embedding Model",
                                    fileName: "bge-m3.gguf",
                                    size: "~400MB",
                                    exists: bgeModelExists,
                                    onDownload: { downloadBGEModel() }
                                )
                            }

                            if selectedPreset == .advanced {
                                ModelDownloadRow(
                                    modelName: "BGE Reranker Model",
                                    fileName: "bge-reranker-v2-m3-Q4_K_M.gguf",
                                    size: "~440MB",
                                    exists: bgeRerankerModelExists,
                                    onDownload: { downloadBGERerankerModel() }
                                )
                            }
                            
                            // Heimdall Model (Advanced only)
                            if selectedPreset == .advanced {
                                ModelDownloadRow(
                                    modelName: "qwen3-0.6b-Instruct (Heimdall)",
                                    fileName: defaultQwenHeimdallFileName,
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
                        Text("Access at http://localhost:7474")
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

            // Default installation behavior: use local GGUF (bge-m3) unless the user
            // explicitly selects Apple Intelligence in the wizard.
            if !hasExplicitEmbeddingProviderSelection {
                config.useAppleIntelligence = false
            }
        }
        .overlay(alignment: .bottom) {
            wizardScrollHint
        }
    }

    // Always-visible review-step banner. The installer can't tell
    // whether the existing data directory needs an upgrade — the
    // server has to start to find out, and it refuses to start if
    // one is required. So we offer the option unconditionally;
    // toggling it sets ConfigManager.upgradeStorageOnNextStart, which
    // the next plist write turns into NORNICDB_UPGRADE_STORAGE=true.
    // Harmless when no upgrade is pending.
    private var storageUpgradePrompt: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 10) {
                Image(systemName: "arrow.up.circle.fill")
                    .font(.title2)
                    .foregroundColor(.orange)
                Text("Existing data directory?")
                    .font(.headline)
                    .foregroundColor(.primary)
                Spacer()
            }

            Toggle(isOn: $config.upgradeStorageOnNextStart) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Authorize storage upgrade on first start")
                        .font(.subheadline)
                        .fontWeight(.semibold)
                    Text("Enable this if you're upgrading from an older NornicDB version with existing data in \(config.dataPath). The server will refuse to start with out-of-date storage unless this is set. Storage upgrades are one-way — ⚠️ BACK UP YOUR DATA FIRST! ⚠️ The flag is a no-op if no upgrade is needed.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
            .toggleStyle(.checkbox)
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 10)
                .fill(Color.orange.opacity(0.12))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 10)
                .stroke(Color.orange, lineWidth: 2)
        )
        .padding(.horizontal)
    }

    private var wizardScrollHint: some View {
        VStack(spacing: 0) {
            LinearGradient(
                colors: [
                    Color.clear,
                    Color(NSColor.windowBackgroundColor).opacity(0.94),
                ],
                startPoint: .top,
                endPoint: .bottom
            )
            .frame(height: 22)

            HStack(spacing: 6) {
                Image(systemName: "arrow.down")
                    .font(.caption2)
                Text("Scroll for more")
                    .font(.caption2)
            }
            .foregroundColor(.secondary)
            .padding(.bottom, 6)
            .frame(maxWidth: .infinity)
            .background(Color(NSColor.windowBackgroundColor).opacity(0.94))
        }
        .allowsHitTesting(false)
    }
    
    private func needsModels() -> Bool {
        return selectedPreset == .standard || selectedPreset == .advanced
    }
    
    private func allRequiredModelsExist() -> Bool {
        if selectedPreset == .standard {
            if config.useAppleIntelligence {
                return true
            }
            return bgeModelExists
        } else if selectedPreset == .advanced {
            if config.useAppleIntelligence {
                return bgeRerankerModelExists && qwenModelExists
            }
            return bgeModelExists && bgeRerankerModelExists && qwenModelExists
        }
        return true
    }
    
    private func checkModelFiles() {
        let modelsPath = config.modelsPath
        let fileManager = FileManager.default
        
        let bgePath = "\(modelsPath)/bge-m3.gguf"
        let bgeRerankerPath = "\(modelsPath)/bge-reranker-v2-m3-Q4_K_M.gguf"
        let qwenPath = "\(modelsPath)/\(defaultQwenHeimdallFileName)"
        
        bgeModelExists = fileManager.fileExists(atPath: bgePath) && isValidGGUFFile(atPath: bgePath)
        bgeRerankerModelExists = fileManager.fileExists(atPath: bgeRerankerPath) && isValidGGUFFile(atPath: bgeRerankerPath)
        qwenModelExists = fileManager.fileExists(atPath: qwenPath) && isValidGGUFFile(atPath: qwenPath)
        
        print("Checking models:")
        print("  BGE: \(bgePath) - exists: \(bgeModelExists)")
        print("  BGE Reranker: \(bgeRerankerPath) - exists: \(bgeRerankerModelExists)")
        print("  Qwen: \(qwenPath) - exists: \(qwenModelExists)")
    }
    
    private func downloadBGEModel() {
        isDownloadingModels = true
        downloadProgress = "Downloading BGE-M3 model (~400MB)..."
        
        DispatchQueue.global(qos: .userInitiated).async {
            let modelsPath = config.modelsPath
            let modelPath = "\(modelsPath)/bge-m3.gguf"
            let task = Process()
            task.launchPath = "/bin/bash"
            task.arguments = ["-c", "mkdir -p \(shellQuoted(modelsPath)) && curl -fL -o \(shellQuoted(modelPath)) https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf"]
            
            task.launch()
            task.waitUntilExit()
            
            DispatchQueue.main.async {
                if task.terminationStatus == 0 && isValidGGUFFile(atPath: modelPath) {
                    downloadProgress = "BGE-M3 downloaded successfully!"
                    bgeModelExists = true
                } else {
                    _ = removeInvalidGGUFFile(atPath: modelPath)
                    downloadProgress = modelDownloadFailureMessage(modelsPath: modelsPath)
                    bgeModelExists = false
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
        downloadProgress = "Downloading qwen3-0.6b model (~350MB)..."
        
        DispatchQueue.global(qos: .userInitiated).async {
            let modelsPath = config.modelsPath
            let modelPath = "\(modelsPath)/\(defaultQwenHeimdallFileName)"
            let task = Process()
            task.launchPath = "/bin/bash"
            task.arguments = ["-c", "mkdir -p \(shellQuoted(modelsPath)) && curl -fL -o \(shellQuoted(modelPath)) \(shellQuoted(defaultQwenHeimdallDownloadURL))"]
            
            task.launch()
            task.waitUntilExit()
            
            DispatchQueue.main.async {
                if task.terminationStatus == 0 && isValidGGUFFile(atPath: modelPath) {
                    downloadProgress = "qwen3-0.6b downloaded successfully!"
                    qwenModelExists = true
                } else {
                    _ = removeInvalidGGUFFile(atPath: modelPath)
                    downloadProgress = modelDownloadFailureMessage(modelsPath: modelsPath)
                    qwenModelExists = false
                }
                
                DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
                    isDownloadingModels = false
                    downloadProgress = ""
                }
            }
        }
    }

    private func downloadBGERerankerModel() {
        isDownloadingModels = true
        downloadProgress = "Downloading BGE reranker model (~440MB)..."

        DispatchQueue.global(qos: .userInitiated).async {
            let modelsPath = config.modelsPath
            let modelPath = "\(modelsPath)/bge-reranker-v2-m3-Q4_K_M.gguf"
            let task = Process()
            task.launchPath = "/bin/bash"
            task.arguments = ["-c", "mkdir -p \(shellQuoted(modelsPath)) && curl -fL -o \(shellQuoted(modelPath)) https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF/resolve/main/bge-reranker-v2-m3-Q4_K_M.gguf"]

            task.launch()
            task.waitUntilExit()

            DispatchQueue.main.async {
                if task.terminationStatus == 0 && isValidGGUFFile(atPath: modelPath) {
                    downloadProgress = "BGE reranker downloaded successfully!"
                    bgeRerankerModelExists = true
                } else {
                    _ = removeInvalidGGUFFile(atPath: modelPath)
                    downloadProgress = modelDownloadFailureMessage(modelsPath: modelsPath)
                    bgeRerankerModelExists = false
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
        config.kmeansEnabled = false
        config.searchRerankEnabled = features.searchRerank
        if config.searchRerankModel.isEmpty {
            config.searchRerankModel = "bge-reranker-v2-m3-Q4_K_M.gguf"
        }
        config.autoTLPEnabled = features.autoTLP
        config.heimdallEnabled = features.heimdall
        config.autoStartEnabled = true
    }
    
    func getPresetFeatures() -> (embeddings: Bool, searchRerank: Bool, autoTLP: Bool, heimdall: Bool) {
        switch selectedPreset {
        case .basic:
            return (false, false, false, false)
        case .standard:
            return (true, false, false, false)
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


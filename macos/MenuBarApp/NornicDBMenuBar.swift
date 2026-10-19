import SwiftUI
import AppKit
import Foundation
import Security
import Yams

let defaultQwenHeimdallFileName = "qwen3-0.6b-instruct.gguf"
let defaultQwenHeimdallDownloadURL = "https://huggingface.co/unsloth/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q4_K_M.gguf"
let ggufMagicHeader = Data([0x47, 0x47, 0x55, 0x46])

func shellQuoted(_ value: String) -> String {
    return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
}

func isValidGGUFFile(atPath path: String) -> Bool {
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
func removeInvalidGGUFFile(atPath path: String) -> Bool {
    guard FileManager.default.fileExists(atPath: path), !isValidGGUFFile(atPath: path) else {
        return false
    }

    try? FileManager.default.removeItem(atPath: path)
    return true
}

func modelDownloadFailureMessage(modelsPath: String) -> String {
    return "Model download failed. Please download manually and place in \(modelsPath)"
}

func appDisplayVersion() -> String {
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

func appBuildArchitecture() -> String {
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
    private let instanceLock = MenuBarInstanceLock()
    private var ownsAppSession = false
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
        NSApp.setActivationPolicy(.accessory)
        do {
            let lockURL = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".nornicdb/menubar.lock")
            guard try instanceLock.acquire(at: lockURL) else {
                NSRunningApplication.runningApplications(withBundleIdentifier: "com.nornicdb.menubar")
                    .first(where: { $0.processIdentifier != getpid() })?.activate(options: [.activateIgnoringOtherApps])
                NSApp.terminate(nil)
                return
            }
        } catch {
            let alert = NSAlert()
            alert.messageText = "Cannot start NornicDB menu bar app"
            alert.informativeText = "Cannot lock the app session. Check permissions for ~/.nornicdb/menubar.lock before retrying."
            alert.runModal()
            NSApp.terminate(nil)
            return
        }
        ownsAppSession = true
        retireLegacyMenuBarInstances()
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
        if configManager.isFirstRun() { showFirstRunWizard() }
    }
    
    func applicationDidBecomeActive(_ notification: Notification) {
        if ownsAppSession && statusItem != nil && configManager.isFirstRun() {
            showFirstRunWizard()
        }
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows: Bool) -> Bool {
        guard ownsAppSession && statusItem != nil else { return false }
        if configManager.isFirstRun() { showFirstRunWizard() }
        else { openSettings() }
        return false
    }

    func applicationWillTerminate(_ notification: Notification) {
        guard ownsAppSession else { return }
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
    
    // Older binaries predate the lock. Only the installed lock owner retires
    // those copies; the existing health timer also catches later legacy launches.
    private func retireLegacyMenuBarInstances() {
        guard Bundle.main.bundleURL.standardizedFileURL.path == "/Applications/NornicDB.app" else { return }
        for app in NSRunningApplication.runningApplications(withBundleIdentifier: "com.nornicdb.menubar")
            where app.processIdentifier != getpid() {
            app.forceTerminate()
        }
    }

    private var statusGeneration = 0

    /// Launch submission is complete; readiness is observed independently of windows.
    func serverLaunchSubmitted() {
        updateStatus(.starting)
        checkHealth()
    }

    private func checkHealth() {
        retireLegacyMenuBarInstances()
        let generation = statusGeneration
        guard let url = URL(string: "http://localhost:\(configManager.httpPortNumber)/health") else { return }
        var request = URLRequest(url: url)
        request.timeoutInterval = 2
        URLSession.shared.dataTask(with: request) { [weak self] _, response, _ in
            let healthy = (response as? HTTPURLResponse)?.statusCode == 200
            let processRunning = serverLaunchAgentHasProcess()
            DispatchQueue.main.async {
                guard let self = self, self.statusGeneration == generation else { return }
                self.updateStatus(observedServerStatus(healthy: healthy, processRunning: processRunning))
                if healthy && self.configManager.upgradeStorageOnNextStart {
                    let path = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
                    self.configManager.upgradeStorageOnNextStart = false
                    do { try writeLaunchAgentPlist(config: self.configManager, to: path) }
                    catch { self.configManager.upgradeStorageOnNextStart = true }
                }
            }
        }.resume()
    }

    private func updateStatus(_ status: ServerStatus) {
        statusGeneration += 1
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
        let confirmation = NSAlert()
        confirmation.messageText = "Stop NornicDB server?"
        confirmation.informativeText = "Active connections will be interrupted."
        confirmation.addButton(withTitle: "Cancel")
        confirmation.addButton(withTitle: "Stop Server")
        guard confirmation.runModal() == .alertSecondButtonReturn else { return }
        if isLaunchAgentLoaded(label: "com.nornicdb.server") {
            _ = executeLaunchctl(arguments: ["unload", serverLaunchAgentPath])
        }
        self.updateStatus(.stopped)
    }
    
    @objc func restartServer() {
        let confirmation = NSAlert()
        confirmation.messageText = "Restart NornicDB server?"
        confirmation.informativeText = "Active connections will be interrupted."
        confirmation.addButton(withTitle: "Cancel")
        confirmation.addButton(withTitle: "Restart Server")
        guard confirmation.runModal() == .alertSecondButtonReturn else { return }
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
            window.setContentSize(NSSize(width: 820, height: 650))
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
        if let existing = firstRunWindowController {
            existing.showWindow(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        let wizardView = FirstRunWizard(config: configManager, appDelegate: self) {
            self.firstRunWindowController?.window?.close()
            self.firstRunWindowController = nil
        }
        let hostingController = NSHostingController(rootView: wizardView)
        
        let window = NSWindow(contentViewController: hostingController)
        window.title = "Welcome to NornicDB"
        window.setContentSize(NSSize(width: 760, height: 680))
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
        alert.informativeText = "This will download:\n• BGE-M3 embedding model (~400MB)\n• BGE reranker model (~440MB)\n• qwen3-0.6b-Instruct model (~350MB)\n\nTotal: ~1.2GB\n\nDownloading from HuggingFace..."
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
                        result.informativeText = "Models downloaded successfully!\n\nYou can now select them in Settings → AI services."
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

func launchdServiceTarget(for label: String) -> String {
    return "gui/\(getuid())/\(label)"
}

@discardableResult
func executeLaunchctl(arguments: [String], wait: Bool = true) -> Int32? {
    let task = Process()
    task.launchPath = "/usr/bin/env"
    task.arguments = ["launchctl"] + arguments
    task.standardOutput = FileHandle.nullDevice
    task.standardError = FileHandle.nullDevice

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

func isLaunchAgentLoaded(label: String) -> Bool {
    return executeLaunchctl(arguments: ["print", launchdServiceTarget(for: label)]) == 0
}

// MARK: - Settings View

struct SettingsView: View {
    @ObservedObject var config: ConfigManager
    let appDelegate: AppDelegate
    @State private var showingSaveAlert = false
    @State private var saveSuccess = false
    @State private var selectedTab = 0
    
    // Track original values to detect changes
    @State private var originalServiceSettings: [String] = []
    @State private var originalRuntimeEnvironment: [String: String] = [:]
    @State private var originalRuntimeArguments: [String] = []
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
        return config.runtimeEnvironment != originalRuntimeEnvironment ||
               config.runtimeArguments != originalRuntimeArguments ||
               serviceSettings != originalServiceSettings || config.embeddingsEnabled != originalEmbeddingsEnabled ||
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
                Text("AI features").tag(0)
                Text("Server").tag(1)
                Text("AI services").tag(2)
                Text("Security").tag(3)
                Text("Startup").tag(4)
                Text("Advanced").tag(5)
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
                AdvancedRuntimeSettingsView(config: config).tag(5)
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

    private var serviceSettings: [String] {
        [config.embeddingProvider, config.embeddingURL, config.embeddingAPIKey,
         config.searchRerankProvider, config.searchRerankURL, config.searchRerankAPIKey,
         config.heimdallProvider, config.heimdallURL, config.heimdallAPIKey, String(config.embeddingWorkers)]
    }

    private func captureOriginalValues() {
        // Reload config from file to ensure we have the latest values
        config.loadConfig()
        
        originalServiceSettings = serviceSettings
        originalRuntimeEnvironment = config.runtimeEnvironment
        originalRuntimeArguments = config.runtimeArguments
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
        guard config.saveConfig() else {
            saveProgress = config.configurationError.isEmpty ? "Cannot save configuration." : config.configurationError
            isSaving = false
            return
        }
        if config.useAppleIntelligence && config.embeddingsEnabled {
            if !appDelegate.embeddingServer.isRunning {
                do { try appDelegate.embeddingServer.start() }
                catch { saveProgress = "Cannot start the Apple embedding adapter."; isSaving = false; return }
            }
        } else if appDelegate.embeddingServer.isRunning { appDelegate.embeddingServer.stop() }
        let path = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
        do { try writeLaunchAgentPlist(config: config, to: path) }
        catch { saveProgress = "Cannot save startup settings. Check LaunchAgent permissions."; isSaving = false; return }
        guard reloadServerLaunchAgent(path: path) else {
            saveProgress = "Cannot restart the service. Check its logs and startup settings."
            isSaving = false
            return
        }
        saveProgress = "Waiting for server readiness..."
        waitForServerHealthAfterUpgrade(attempts: 20) { success in
            isSaving = false
            if success {
                captureOriginalValues()
                NSApp.keyWindow?.close()
            } else { saveProgress = "Server is not ready. Check its logs and startup settings." }
        }
    }

    var featuresTab: some View {
        ScrollView {
            AIFeatureSettingsView(config: config).padding()
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
    
    var modelsTab: some View { AIServiceSettingsView(config: config, downloadModels: { appDelegate.downloadModels() }) }

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


/// A live launchd process can initialize for longer than a health polling interval.
func observedServerStatus(healthy: Bool, processRunning: Bool) -> ServerStatus {
    healthy ? .running : (processRunning ? .starting : .stopped)
}

func launchAgentHasProcess(_ output: String) -> Bool {
    // Only inspect the job's top-level PID, never nested environment values.
    output.split(separator: "\n").contains { line in
        let prefix = "\tpid = "
        guard line.hasPrefix(prefix), let pid = Int32(line.dropFirst(prefix.count)), pid > 0 else { return false }
        return true
    }
}

private func serverLaunchAgentHasProcess() -> Bool {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/bin/launchctl")
    process.arguments = ["print", launchdServiceTarget(for: "com.nornicdb.server")]
    let output = Pipe()
    process.standardOutput = output
    process.standardError = FileHandle.nullDevice
    do {
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return false }
        return launchAgentHasProcess(String(decoding: data, as: UTF8.self))
    } catch { return false }
}

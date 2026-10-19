import SwiftUI
import AppKit

/// Setup edits loaded configuration directly; navigating never applies presets.
struct FirstRunWizard: View {
    @ObservedObject var config: ConfigManager
    let appDelegate: AppDelegate
    let onComplete: () -> Void
    @State private var currentStep = 0
    @State private var showAdvanced = false
    @State private var isSaving = false
    @State private var saveProgress = ""

    var body: some View {
        VStack(spacing: 12) {
            Text("Set up NornicDB").font(.title2).padding(.top)
            Text("Existing settings are loaded. Enable only the services you need.")
                .font(.caption).foregroundColor(.secondary)
            HStack {
                ForEach(0..<3) { step in
                    Text(["1. Server / Security", "2. Optional AI", "3. Review"][step])
                        .fontWeight(currentStep == step ? .bold : .regular)
                }
                Toggle("Advanced", isOn: $showAdvanced).toggleStyle(.checkbox)
            }.padding(.horizontal)
            Divider()
            if showAdvanced {
                AdvancedRuntimeSettingsView(config: config)
            } else if currentStep == 0 {
                ScrollView {
                    VStack {
                        SettingsView(config: config, appDelegate: appDelegate).serverTab
                        SettingsView(config: config, appDelegate: appDelegate).securityTab
                        Toggle("Start automatically at login", isOn: $config.autoStartEnabled).padding()
                        Toggle("Authorize one storage upgrade on next start", isOn: $config.upgradeStorageOnNextStart).padding(.horizontal)
                        Text("For existing older storage only. Back up your database first; storage upgrades are one-way.")
                            .font(.caption).foregroundColor(.secondary).padding()
                    }
                }
            } else if currentStep == 1 {
                AIServiceSettingsView(config: config)
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 14) {
                        Text("Review your configuration").font(.headline)
                        Text("Server: \(config.hostAddress), HTTP \(config.httpPortNumber), Bolt \(config.boltPortNumber)")
                        Text("Authentication: \(config.adminUsername). Encryption: \(config.encryptionEnabled ? "enabled" : "disabled")")
                        summary("Embeddings", enabled: config.embeddingsEnabled, provider: config.embeddingProvider, model: config.embeddingModel)
                        summary("Reranking", enabled: config.searchRerankEnabled, provider: config.searchRerankProvider, model: config.searchRerankModel)
                        summary("Heimdall", enabled: config.heimdallEnabled, provider: config.heimdallProvider, model: config.heimdallModel)
                        Text("Advanced: \(config.runtimeEnvironment.count) environment overrides, \(config.runtimeArguments.count) runtime arguments")
                        Text("Advanced overrides take precedence. Review that tab if an override conflicts with your settings.").font(.caption)
                    }.padding()
                }
            }
            Divider()
            if !saveProgress.isEmpty { Text(saveProgress).font(.caption) }
            HStack {
                if currentStep > 0 { Button("Back") { showAdvanced = false; currentStep -= 1 } }
                Spacer()
                if currentStep < 2 {
                    Button("Continue") { showAdvanced = false; currentStep += 1 }
                } else {
                    Button("Save and Start Server") { saveAndStartServer() }
                }
            }.padding().disabled(isSaving)
        }
        .frame(minWidth: 720, minHeight: 600)
    }

    private func summary(_ name: String, enabled: Bool, provider: String, model: String) -> some View {
        Text("\(name): \(enabled ? "enabled" : "disabled") · \(provider) · \(model)")
    }

    private func saveAndStartServer() {
        isSaving = true
        saveProgress = "Saving configuration..."
        guard config.saveConfig() else {
            saveProgress = config.configurationError.isEmpty ? "Cannot save configuration." : config.configurationError
            isSaving = false
            return
        }
        if config.useAppleIntelligence && config.embeddingsEnabled && !appDelegate.embeddingServer.isRunning {
            do { try appDelegate.embeddingServer.start() }
            catch { saveProgress = "Cannot start the Apple embedding adapter."; isSaving = false; return }
        }
        let path = NSString(string: "~/Library/LaunchAgents/com.nornicdb.server.plist").expandingTildeInPath
        do { try writeLaunchAgentPlist(config: config, to: path) }
        catch { saveProgress = "Cannot save startup settings. Check LaunchAgent permissions."; isSaving = false; return }
        guard reloadServerLaunchAgent(path: path) else {
            saveProgress = "Cannot start the service. Inspect the server logs and startup settings."
            isSaving = false
            return
        }
        saveProgress = "Waiting for server readiness..."
        waitForServerHealth(attempts: 20) { success in
            isSaving = false
            guard success else {
                saveProgress = "Server is not ready. Check its logs and settings before retrying."
                return
            }
            config.completeFirstRun()
            if config.upgradeStorageOnNextStart {
                config.upgradeStorageOnNextStart = false
                try? writeLaunchAgentPlist(config: config, to: path)
            }
            onComplete()
        }
    }

    private func waitForServerHealth(attempts: Int, completion: @escaping (Bool) -> Void) {
        guard attempts > 0 else {
            completion(false)
            return
        }

        // Health endpoint is always on HTTP port 7474
        let url = URL(string: "http://localhost:\(config.httpPortNumber)/health")!
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

}

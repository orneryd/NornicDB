import SwiftUI

/// Shared service controls used by setup and Settings. Remote model IDs are never
/// rewritten as filenames, and disabling a service leaves its configuration intact.
struct AIServiceSettingsView: View {
    @ObservedObject var config: ConfigManager
    let downloadModel: (DefaultLocalModel) -> Void
    let openModelsFolder: () -> Void
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                AIFeatureSettingsView(config: config)
                Divider()
                Text("Optional AI services").font(.headline)
                Text("Configure each service independently. Remote providers do not require local model downloads.").foregroundColor(.secondary)
                ServiceProviderEditor(title: "Embeddings", enabled: $config.embeddingsEnabled,
                    provider: $config.embeddingProvider, model: $config.embeddingModel,
                    endpoint: $config.embeddingURL, apiKey: $config.embeddingAPIKey,
                    providers: ["local", "ollama", "openai", "orca", "voyage"],
                    defaultModel: .embeddings, downloadModel: downloadModel, modelsPath: config.modelsPath, availableModels: config.availableModels, openModelsFolder: openModelsFolder)
                if config.embeddingsEnabled {
                    if AppleMLEmbedder.isAvailable() {
                        Toggle("Use local Apple Intelligence adapter (512 dimensions)", isOn: $config.useAppleIntelligence)
                    }
                    DisclosureGroup("Chunking") {
                        HStack {
                            Text("Tokens per chunk")
                            TextField("Chunk size", value: $config.embeddingChunkSize, formatter: NumberFormatter())
                            Text("Overlap")
                            TextField("Overlap", value: $config.embeddingChunkOverlap, formatter: NumberFormatter())
                        }
                    }
                    HStack {
                        Text("Dimensions")
                        TextField("Dimensions", value: $config.embeddingDimensions, formatter: NumberFormatter())
                        Text("Workers")
                        TextField("Workers", value: $config.embeddingWorkers, formatter: NumberFormatter())
                    }
                }
                ServiceProviderEditor(title: "Reranking", enabled: $config.searchRerankEnabled,
                    provider: $config.searchRerankProvider, model: $config.searchRerankModel,
                    endpoint: $config.searchRerankURL, apiKey: $config.searchRerankAPIKey,
                    providers: ["local", "ollama", "openai", "http"],
                    defaultModel: .reranking, downloadModel: downloadModel, modelsPath: config.modelsPath, availableModels: config.availableModels, openModelsFolder: openModelsFolder)
                ServiceProviderEditor(title: "Heimdall assistant", enabled: $config.heimdallEnabled,
                    provider: $config.heimdallProvider, model: $config.heimdallModel,
                    endpoint: $config.heimdallURL, apiKey: $config.heimdallAPIKey,
                    providers: ["local", "ollama", "openai", "orca", "vllm", "litellm"],
                    defaultModel: .inference, downloadModel: downloadModel, modelsPath: config.modelsPath, availableModels: config.availableModels, openModelsFolder: openModelsFolder)
                Text("Local models: \(config.modelsPath)")
                    .font(.caption).foregroundColor(.secondary)
            }.padding()
        }.onAppear { config.scanModels() }
    }
}

struct ServiceProviderEditor: View {
    let title: String
    @Binding var enabled: Bool
    @Binding var provider: String
    @Binding var model: String
    @Binding var endpoint: String
    @Binding var apiKey: String
    let providers: [String]
    let defaultModel: DefaultLocalModel
    let downloadModel: (DefaultLocalModel) -> Void
    let modelsPath: String
    let availableModels: [String]
    let openModelsFolder: () -> Void
    var body: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 10) {
                Toggle(title, isOn: $enabled)
                if !enabled {
                    Text("Saved: \(provider) · \(model)").font(.caption).foregroundColor(.secondary)
                }
                if enabled {
                    Picker("Provider", selection: $provider) {
                        ForEach(Array(Set(providers + [provider])).sorted(), id: \.self) { Text($0).tag($0) }
                    }
                    TextField(provider == "local" ? "GGUF filename" : "Model ID", text: $model)
                    if provider == "local" {
                        HStack {
                            if isValidGGUFFile(atPath: model.hasPrefix("/") ? model : "\(modelsPath)/\(model)") {
                                Label("Ready · \(model)", systemImage: "checkmark.circle.fill").foregroundColor(.green)
                            } else if isValidGGUFFile(atPath: "\(modelsPath)/\(defaultModel.filename)") {
                                Label("Default model ready · \(defaultModel.filename)", systemImage: "checkmark.circle.fill").foregroundColor(.green)
                                Button("Use installed default model") { model = defaultModel.filename }
                            } else {
                                Button("Download default model (\(defaultModel.filename))…") { downloadModel(defaultModel) }
                            }
                            Button("Open Models Folder", action: openModelsFolder)
                        }
                    }
                    if provider != "local" {
                        TextField("API endpoint (provider default when empty)", text: $endpoint)
                        SecureField("API key (or configure its environment variable in Advanced)", text: $apiKey)
                    }
                }
            }.padding(6)
        }
    }
}

struct AdvancedRuntimeSettingsView: View {
    @ObservedObject var config: ConfigManager
    @State private var newName = ""
    @State private var revealValues = false
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 12) {
                Text("Advanced startup settings").font(.headline)
                Text("Optional overrides apply on every server launch. Arguments override environment variables, which override normal settings. Remove an override to use the normal setting. Values are stored in your private LaunchAgent file.")
                    .font(.caption).foregroundColor(.secondary)
                Text("Environment variables").font(.subheadline)
                Toggle("Show values (may include credentials)", isOn: $revealValues)
                ForEach(config.runtimeEnvironment.keys.sorted(), id: \.self) { key in
                    HStack {
                        Text(key).font(.system(.caption, design: .monospaced))
                        if revealValues {
                            TextField("Value", text: Binding(get: { config.runtimeEnvironment[key] ?? "" }, set: { config.runtimeEnvironment[key] = $0 }))
                        } else {
                            SecureField("Value", text: Binding(get: { config.runtimeEnvironment[key] ?? "" }, set: { config.runtimeEnvironment[key] = $0 }))
                        }
                        Button("Remove") { config.runtimeEnvironment.removeValue(forKey: key) }
                    }
                }
                HStack {
                    TextField("VARIABLE_NAME", text: $newName)
                    Button("Add variable") { config.runtimeEnvironment[newName] = ""; newName = "" }
                        .disabled(newName.range(of: "^[A-Za-z_][A-Za-z0-9_]*$", options: .regularExpression) == nil || config.runtimeEnvironment[newName] != nil || newName == "NORNICDB_UPGRADE_STORAGE")
                }
                Divider()
                Text("Runtime arguments").font(.subheadline)
                Text("One argument per row, after the binary. Keep ‘serve’ to run the database. Put flags and separate values in separate rows. No shell evaluation.").font(.caption)
                ForEach(config.runtimeArguments.indices, id: \.self) { index in
                    HStack {
                        TextField("Argument", text: Binding(
                            get: { config.runtimeArguments.indices.contains(index) ? config.runtimeArguments[index] : "" },
                            set: { if config.runtimeArguments.indices.contains(index) { config.runtimeArguments[index] = $0 } }))
                        Button("Remove") { config.runtimeArguments.remove(at: index) }
                    }
                }
                Button("Add argument") { config.runtimeArguments.append("") }
            }.padding()
        }
    }
}

/// Compact first step; detailed key-management actions remain in Settings.
struct ServerSecuritySetupView: View {
    @ObservedObject var config: ConfigManager
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                StorageUpgradeSetupControl(config: config)
                GroupBox("Server") {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack { Text("Listen address"); TextField("localhost", text: $config.hostAddress) }
                        HStack {
                            Text("HTTP port")
                            TextField("7474", text: $config.httpPortNumber)
                            Text("Bolt port")
                            TextField("7687", text: $config.boltPortNumber)
                        }
                        Toggle("Start automatically at login", isOn: $config.autoStartEnabled)
                    }.padding(8)
                }
                GroupBox("Security") {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack { Text("Admin username"); TextField("Username", text: $config.adminUsername) }
                        HStack { Text("Admin password"); SecureField("Password", text: $config.adminPassword) }
                        Toggle("Encrypt database storage", isOn: $config.encryptionEnabled)
                        if config.encryptionEnabled {
                            SecureField("Encryption password (saved securely)", text: $config.encryptionPassword)
                            Text("Keep your existing encryption password when upgrading an encrypted database.")
                                .font(.caption).foregroundColor(.secondary)
                        }
                        Text("Detailed key management is available in Settings → Security.")
                            .font(.caption).foregroundColor(.secondary)
                    }.padding(8)
                }
            }.padding()
        }
    }
}

/// One authorization shared by the first setup page and its review.
struct StorageUpgradeSetupControl: View {
    @ObservedObject var config: ConfigManager
    var body: some View {
        GroupBox("Existing database / storage upgrade") {
            VStack(alignment: .leading, spacing: 8) {
                Toggle("Authorize storage upgrade on the next start", isOn: $config.upgradeStorageOnNextStart)
                Text("Enable for older existing storage. Back up your database first: upgrades are one-way. This authorization is cleared after a healthy start.")
                    .font(.caption).foregroundColor(.secondary)
            }.padding(8)
        }
    }
}

/// The same feature switches are available during setup and in Settings.
struct AIFeatureSettingsView: View {
    @ObservedObject var config: ConfigManager
    var body: some View {
        GroupBox("AI & analytics features") {
            VStack(alignment: .leading, spacing: 15) {
                Text("Optional features. Changes apply when the server starts or restarts.")
                    .font(.caption).foregroundColor(.secondary)
                FeatureToggle(title: "K-Means Clustering",
                    description: "Automatic node clustering and organization",
                    isEnabled: $config.kmeansEnabled, icon: "circle.hexagongrid.fill")
                FeatureToggle(title: "Memory Decay",
                    description: "Natural episodic, semantic, and procedural memory decay",
                    isEnabled: $config.memoryDecayEnabled, icon: "brain.filled.head.profile")
                VStack(alignment: .leading, spacing: 6) {
                    Text("Search cursor continuation").font(.headline)
                    HStack {
                        Text("Maximum active cursors")
                        TextField("0", value: $config.searchCursorMax, formatter: NumberFormatter())
                            .frame(width: 100)
                    }
                    Text("0 disables continuation. A positive number enables it and limits retained cursors across the server. Changes require a restart.")
                        .font(.caption).foregroundColor(.secondary)
                }
                FeatureToggle(title: "Auto-TLP",
                    description: "Automatic Topological Link prediction",
                    isEnabled: $config.autoTLPEnabled, icon: "point.3.connected.trianglepath.dotted")
            }.padding(8)
        }
    }
}

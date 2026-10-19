import SwiftUI

/// Shared service controls used by setup and Settings. Remote model IDs are never
/// rewritten as filenames, and disabling a service leaves its configuration intact.
struct AIServiceSettingsView: View {
    @ObservedObject var config: ConfigManager
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                Text("Optional AI services").font(.headline)
                Text("Configure each service independently. Remote providers do not require local model downloads.").foregroundColor(.secondary)
                ServiceProviderEditor(title: "Embeddings", enabled: $config.embeddingsEnabled,
                    provider: $config.embeddingProvider, model: $config.embeddingModel,
                    endpoint: $config.embeddingURL, apiKey: $config.embeddingAPIKey,
                    providers: ["local", "ollama", "openai", "orca", "voyage"])
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
                    providers: ["local", "ollama", "openai", "http"])
                ServiceProviderEditor(title: "Heimdall assistant", enabled: $config.heimdallEnabled,
                    provider: $config.heimdallProvider, model: $config.heimdallModel,
                    endpoint: $config.heimdallURL, apiKey: $config.heimdallAPIKey,
                    providers: ["local", "ollama", "openai", "orca", "vllm", "litellm"])
                Text("Local models: \(config.modelsPath). Use Download Models from the menu to install GGUF files.")
                    .font(.caption).foregroundColor(.secondary)
            }.padding()
        }
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
    var body: some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 10) {
                Toggle(title, isOn: $enabled)
                if enabled {
                    Picker("Provider", selection: $provider) {
                        ForEach(Array(Set(providers + [provider])).sorted(), id: \.self) { Text($0).tag($0) }
                    }
                    TextField(provider == "local" ? "GGUF filename" : "Model ID", text: $model)
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
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 12) {
                Text("Advanced startup settings").font(.headline)
                Text("Optional overrides apply on every server launch. Arguments override environment variables, which override normal settings. Remove an override to use the normal setting. Values are stored in your private LaunchAgent file.")
                    .font(.caption).foregroundColor(.secondary)
                Text("Environment variables").font(.subheadline)
                ForEach(config.runtimeEnvironment.keys.sorted(), id: \.self) { key in
                    HStack {
                        Text(key).font(.system(.caption, design: .monospaced))
                        SecureField("Value", text: Binding(get: { config.runtimeEnvironment[key] ?? "" }, set: { config.runtimeEnvironment[key] = $0 }))
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
                Text("One argument per row, after ‘nornicdb serve’. Put a flag and its separate value in separate rows. No shell expansion or evaluation.").font(.caption)
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

import SwiftUI

/// Settings view for the Embedding Server
struct EmbeddingServerSettingsView: View {
    @ObservedObject var embeddingServer: EmbeddingServer
    @State private var portString: String = ""
    @State private var autoStart: Bool = false
    @State private var showError: Bool = false
    @State private var errorMessage: String = ""
    
    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            // Header
            HStack {
                Image(systemName: "brain.head.profile")
                    .font(.system(size: 32))
                    .foregroundColor(.blue)
                
                VStack(alignment: .leading, spacing: 4) {
                    Text("Apple ML Embedding Server")
                        .font(.title2)
                        .fontWeight(.bold)
                    Text("OpenAI-compatible embeddings API using on-device Apple Intelligence")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
            .padding(.bottom, 10)
            
            Divider()
            
            // Server Status
            GroupBox(label: Label("Server Status", systemImage: "server.rack")) {
                VStack(alignment: .leading, spacing: 12) {
                    HStack {
                        Circle()
                            .fill(embeddingServer.isRunning ? Color.green : Color.red)
                            .frame(width: 12, height: 12)
                        Text(embeddingServer.isRunning ? "Running" : "Stopped")
                            .font(.headline)
                        Spacer()
                        
                        if embeddingServer.isRunning {
                            Button(action: stopServer) {
                                Label("Stop", systemImage: "stop.circle.fill")
                            }
                            .buttonStyle(.borderedProminent)
                            .tint(.red)
                        } else {
                            Button(action: startServer) {
                                Label("Start", systemImage: "play.circle.fill")
                            }
                            .buttonStyle(.borderedProminent)
                            .tint(.green)
                        }
                    }
                    
                    if embeddingServer.isRunning {
                        VStack(alignment: .leading, spacing: 8) {
                            InfoRow(
                                label: "Endpoint",
                                value: "http://localhost:\(embeddingServer.port)/v1/embeddings",
                                icon: "link"
                            )
                            InfoRow(
                                label: "Model",
                                value: embeddingServer.embedder?.model ?? "N/A",
                                icon: "cpu"
                            )
                            InfoRow(
                                label: "Dimensions",
                                value: "\(embeddingServer.embedder?.dimensions ?? 0)",
                                icon: "chart.bar"
                            )
                        }
                        .padding(.top, 8)
                    }
                    
                    if let error = embeddingServer.lastError {
                        HStack(alignment: .top, spacing: 8) {
                            Image(systemName: "exclamationmark.triangle.fill")
                                .foregroundColor(.orange)
                            Text(error)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                        .padding(8)
                        .background(Color.orange.opacity(0.1))
                        .cornerRadius(6)
                    }
                }
                .padding(12)
            }
            
            // Configuration
            GroupBox(label: Label("Configuration", systemImage: "gearshape")) {
                VStack(alignment: .leading, spacing: 16) {
                    // Port Configuration
                    HStack {
                        Text("Port:")
                            .frame(width: 100, alignment: .leading)
                        TextField("11435", text: $portString)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 100)
                            .disabled(embeddingServer.isRunning)
                            .onChange(of: portString) { newValue in
                                validateAndUpdatePort(newValue)
                            }
                        
                        if embeddingServer.isRunning {
                            Text("(restart required)")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }
                    
                    // Auto-start Toggle
                    Toggle(isOn: $autoStart) {
                        VStack(alignment: .leading, spacing: 2) {
                            Text("Start automatically on login")
                            Text("Server will start when the menu bar app launches")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }
                    .onChange(of: autoStart) { newValue in
                        UserDefaults.standard.set(newValue, forKey: "embedding_server_auto_start")
                    }
                }
                .padding(12)
            }
            
            // Statistics
            if embeddingServer.requestCount > 0 {
                GroupBox(label: Label("Statistics", systemImage: "chart.line.uptrend.xyaxis")) {
                    VStack(alignment: .leading, spacing: 12) {
                        StatRow(
                            label: "Requests Served",
                            value: "\(embeddingServer.requestCount)",
                            icon: "arrow.up.arrow.down"
                        )
                        StatRow(
                            label: "Average Latency",
                            value: String(format: "%.1f ms", embeddingServer.averageLatency * 1000),
                            icon: "clock"
                        )
                        
                        Button("Reset Statistics") {
                            embeddingServer.resetStatistics()
                        }
                        .buttonStyle(.bordered)
                    }
                    .padding(12)
                }
            }
            
            // Usage Example
            GroupBox(label: Label("Usage Example", systemImage: "terminal")) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("cURL Example:")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    ScrollView(.horizontal, showsIndicators: false) {
                        Text("""
                        curl http://localhost:\(embeddingServer.port)/v1/embeddings \\
                          -H "Content-Type: application/json" \\
                          -d '{"input": "Hello, world!", "model": "apple-ml-embeddings"}'
                        """)
                        .font(.system(.caption, design: .monospaced))
                        .padding(8)
                        .background(Color.gray.opacity(0.1))
                        .cornerRadius(6)
                    }
                    
                    Button(action: copyExample) {
                        Label("Copy to Clipboard", systemImage: "doc.on.doc")
                    }
                    .buttonStyle(.bordered)
                }
                .padding(12)
            }
            
            // Info
            VStack(alignment: .leading, spacing: 8) {
                HStack(spacing: 8) {
                    Image(systemName: "info.circle")
                        .foregroundColor(.blue)
                    Text("About Apple ML Embeddings")
                        .font(.headline)
                }
                
                Text("This server uses Apple's on-device Natural Language framework to generate text embeddings. All processing happens locally on your Mac - no data is sent to external servers. The API is compatible with OpenAI's embeddings endpoint format.")
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                
                HStack(spacing: 16) {
                    Label("Privacy-first", systemImage: "lock.shield")
                    Label("Zero cost", systemImage: "dollarsign.circle")
                    Label("Fast", systemImage: "bolt")
                }
                .font(.caption)
                .foregroundColor(.blue)
            }
            .padding()
            .background(Color.blue.opacity(0.05))
            .cornerRadius(8)
            
            Spacer()
        }
        .padding(20)
        .frame(minWidth: 600, minHeight: 700)
        .onAppear {
            loadSettings()
        }
        .alert("Error", isPresented: $showError) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(errorMessage)
        }
    }
    
    // MARK: - Actions
    
    private func startServer() {
        do {
            try embeddingServer.start()
        } catch {
            errorMessage = error.localizedDescription
            showError = true
        }
    }
    
    private func stopServer() {
        embeddingServer.stop()
    }
    
    private func validateAndUpdatePort(_ value: String) {
        guard let port = UInt16(value), port >= 1024, port <= 65535 else {
            return
        }
        embeddingServer.port = port
        embeddingServer.saveConfiguration()
    }
    
    private func loadSettings() {
        portString = String(embeddingServer.port)
        autoStart = UserDefaults.standard.bool(forKey: "embedding_server_auto_start")
        embeddingServer.loadConfiguration()
    }
    
    private func copyExample() {
        let example = """
        curl http://localhost:\(embeddingServer.port)/v1/embeddings \\
          -H "Content-Type: application/json" \\
          -d '{"input": "Hello, world!", "model": "apple-ml-embeddings"}'
        """
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(example, forType: .string)
    }
}

// MARK: - Supporting Views

struct InfoRow: View {
    let label: String
    let value: String
    let icon: String
    
    var body: some View {
        HStack {
            Image(systemName: icon)
                .foregroundColor(.blue)
                .frame(width: 20)
            Text(label + ":")
                .foregroundColor(.secondary)
            Text(value)
                .fontWeight(.medium)
                .textSelection(.enabled)
            Spacer()
        }
        .font(.caption)
    }
}

struct StatRow: View {
    let label: String
    let value: String
    let icon: String
    
    var body: some View {
        HStack {
            Image(systemName: icon)
                .foregroundColor(.green)
                .frame(width: 20)
            Text(label + ":")
                .foregroundColor(.secondary)
            Spacer()
            Text(value)
                .fontWeight(.semibold)
        }
        .font(.subheadline)
    }
}

// MARK: - Preview

struct EmbeddingServerSettingsView_Previews: PreviewProvider {
    static var previews: some View {
        EmbeddingServerSettingsView(embeddingServer: EmbeddingServer())
    }
}

# macOS Apple Intelligence Integration

Enhance NornicDB on macOS with Siri, Spotlight, Shortcuts, and system-wide intelligence features.

## 🎯 Overview

macOS offers even more integration points than iOS due to its desktop nature and accessibility to system services.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    macOS Apple Intelligence Stack                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                           SIRI & VOICE                                 │  │
│  │  "Hey Siri, what do I know about the Kubernetes project?"             │  │
│  │  "Summarize my notes from last week's meeting"                        │  │
│  └────────────────────────────────┬──────────────────────────────────────┘  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    APP INTENTS (macOS 13+)                             │  │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐     │  │
│  │  │   Query     │ │   Store     │ │  Summarize  │ │   Relate    │     │  │
│  │  │   Memory    │ │   Memory    │ │   Topic     │ │   Concepts  │     │  │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘     │  │
│  └────────────────────────────────┬──────────────────────────────────────┘  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      SPOTLIGHT (Core Spotlight)                        │  │
│  │  • System-wide search: "memory: kubernetes"                           │  │
│  │  • Quick Look preview for graph nodes                                 │  │
│  │  • File content indexing from documents                               │  │
│  └────────────────────────────────┬──────────────────────────────────────┘  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      SHORTCUTS & AUTOMATION                            │  │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐     │  │
│  │  │Daily Review │ │Quick Capture│ │Export Graph │ │ Automation  │     │  │
│  │  │  Morning    │ │  Hotkey     │ │   to JSON   │ │   Scripts   │     │  │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘     │  │
│  └────────────────────────────────┬──────────────────────────────────────┘  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                     SERVICES & CONTEXT MENU                            │  │
│  │  • Right-click any text → "Add to NornicDB"                           │  │
│  │  • Services menu → "Search in NornicDB"                               │  │
│  │  • Quick Actions in Finder                                            │  │
│  └────────────────────────────────┬──────────────────────────────────────┘  │
│                                   │                                          │
│                                   ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       NOTIFICATION CENTER                              │  │
│  │  • Memory reminders based on decay                                    │  │
│  │  • Related content suggestions                                        │  │
│  │  • Daily digest notifications                                         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🗣️ Siri Integration (App Intents)

### App Intents for macOS

macOS 13+ supports the same App Intents framework as iOS:

```swift
import AppIntents

// MARK: - Query Memory Intent

@available(macOS 13.0, *)
struct QueryMemoryIntent: AppIntent {
    static var title: LocalizedStringResource = "Search Memories"
    static var description = IntentDescription(
        "Search your personal knowledge graph using natural language",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = false
    
    @Parameter(title: "Query", description: "What to search for")
    var query: String
    
    @Parameter(title: "Limit", default: 10)
    var limit: Int
    
    func perform() async throws -> some IntentResult & ReturnsValue<[MemoryEntity]> & ProvidesDialog {
        // Connect to NornicDB server
        let client = NornicDBClient.shared
        
        // Perform vector search via Bolt protocol
        let results = try await client.vectorSearch(query: query, limit: limit)
        
        // Convert to entities
        let entities = results.map { MemoryEntity(from: $0) }
        
        // Generate spoken response
        let dialog = generateDialog(for: entities, query: query)
        
        return .result(value: entities, dialog: dialog)
    }
    
    private func generateDialog(for entities: [MemoryEntity], query: String) -> IntentDialog {
        if entities.isEmpty {
            return IntentDialog("I couldn't find any memories about \(query).")
        }
        
        let count = entities.count
        let preview = String(entities.first!.content.prefix(100))
        
        return IntentDialog("""
            Found \(count) memories about \(query). 
            Most relevant: \(preview)...
            """)
    }
}

// MARK: - Store Memory Intent

@available(macOS 13.0, *)
struct StoreMemoryIntent: AppIntent {
    static var title: LocalizedStringResource = "Store Memory"
    static var description = IntentDescription(
        "Save a new memory to your knowledge graph",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = false
    
    @Parameter(title: "Content", description: "What to remember")
    var content: String
    
    @Parameter(title: "Tags", description: "Optional tags (comma-separated)")
    var tags: String?
    
    @Parameter(title: "Type", default: "Note")
    var memoryType: String
    
    func perform() async throws -> some IntentResult & ProvidesDialog {
        let client = NornicDBClient.shared
        
        // Parse tags
        let tagList = tags?.split(separator: ",").map { String($0.trimmingCharacters(in: .whitespaces)) } ?? []
        
        // Create memory via Cypher
        let cypher = """
            CREATE (m:Memory {
                content: $content,
                type: $type,
                tags: $tags,
                timestamp: datetime(),
                source: 'siri'
            })
            RETURN m
        """
        
        let result = try await client.execute(cypher, params: [
            "content": content,
            "type": memoryType,
            "tags": tagList
        ])
        
        // Auto-link to related memories
        let linkCount = try await client.autoLink(nodeId: result.nodeId)
        
        return .result(dialog: IntentDialog(
            "Saved! Found \(linkCount) related memories and connected them."
        ))
    }
}

// MARK: - Summarize Topic Intent

@available(macOS 13.0, *)
struct SummarizeTopicIntent: AppIntent {
    static var title: LocalizedStringResource = "Summarize Topic"
    static var description = IntentDescription(
        "Get an AI summary of everything you know about a topic",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = false
    
    @Parameter(title: "Topic")
    var topic: String
    
    @Parameter(title: "Include Related", default: true)
    var includeRelated: Bool
    
    func perform() async throws -> some IntentResult & ProvidesDialog {
        let client = NornicDBClient.shared
        
        // Vector search for relevant memories
        let memories = try await client.vectorSearch(query: topic, limit: 20)
        
        // Expand graph context if requested
        var context = memories
        if includeRelated {
            context = try await client.expandContext(memories, depth: 2)
        }
        
        // Use Heimdall for summarization if available
        let summary: String
        if client.isHeimdallEnabled {
            summary = try await client.heimdallSummarize(topic: topic, context: context)
        } else {
            summary = generateTemplateSummary(topic: topic, context: context)
        }
        
        return .result(dialog: IntentDialog(summary))
    }
    
    private func generateTemplateSummary(topic: String, context: [Memory]) -> String {
        let count = context.count
        let topics = Set(context.flatMap { $0.tags }).prefix(5)
        let connections = context.map { $0.connections }.reduce(0, +)
        let newest = context.max(by: { $0.timestamp < $1.timestamp })
        
        return """
        Based on \(count) memories about "\(topic)":
        
        Related topics: \(topics.joined(separator: ", "))
        Total connections: \(connections)
        Most recent: \(newest?.timestamp.formatted() ?? "Unknown")
        
        Key insight: \(context.first?.content.prefix(200) ?? "No content available")...
        """
    }
}

// MARK: - Show Connections Intent

@available(macOS 13.0, *)
struct ShowConnectionsIntent: AppIntent {
    static var title: LocalizedStringResource = "Show Connections"
    static var description = IntentDescription(
        "Visualize how a topic connects to other knowledge",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = true  // Opens app for visualization
    
    @Parameter(title: "Topic")
    var topic: String
    
    @Parameter(title: "Depth", default: 2)
    var depth: Int
    
    func perform() async throws -> some IntentResult {
        // Open NornicDB web UI with graph visualization
        let escapedTopic = topic.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? topic
        let url = URL(string: "http://localhost:7474/graph?query=\(escapedTopic)&depth=\(depth)")!
        
        NSWorkspace.shared.open(url)
        
        return .result()
    }
}
```

### Siri Phrases (App Shortcuts Provider)

```swift
@available(macOS 13.0, *)
struct NornicDBShortcuts: AppShortcutsProvider {
    
    @AppShortcutsBuilder
    static var appShortcuts: [AppShortcut] {
        
        // Search memories
        AppShortcut(
            intent: QueryMemoryIntent(),
            phrases: [
                "Search \(.applicationName) for \(\.$query)",
                "What do I know about \(\.$query)",
                "Find memories about \(\.$query)",
                "Search my knowledge for \(\.$query)",
                "What did I learn about \(\.$query)",
                "Show me notes on \(\.$query)"
            ],
            shortTitle: "Search Memories",
            systemImageName: "brain.head.profile"
        )
        
        // Store memory
        AppShortcut(
            intent: StoreMemoryIntent(),
            phrases: [
                "Remember \(\.$content) in \(.applicationName)",
                "Save to \(.applicationName): \(\.$content)",
                "Store in my knowledge graph: \(\.$content)",
                "Add to brain: \(\.$content)",
                "Note to self: \(\.$content)"
            ],
            shortTitle: "Store Memory",
            systemImageName: "plus.circle"
        )
        
        // Summarize topic
        AppShortcut(
            intent: SummarizeTopicIntent(),
            phrases: [
                "Summarize \(\.$topic) in \(.applicationName)",
                "What's the summary of \(\.$topic)",
                "Give me an overview of \(\.$topic)",
                "Tell me about \(\.$topic) from my notes"
            ],
            shortTitle: "Summarize",
            systemImageName: "doc.text"
        )
        
        // Show connections
        AppShortcut(
            intent: ShowConnectionsIntent(),
            phrases: [
                "Show connections for \(\.$topic)",
                "How does \(\.$topic) relate to other things",
                "Visualize \(\.$topic) connections",
                "Graph of \(\.$topic)"
            ],
            shortTitle: "Show Connections",
            systemImageName: "point.3.connected.trianglepath.dotted"
        )
        
        // Daily review
        AppShortcut(
            intent: DailyReviewIntent(),
            phrases: [
                "Daily memory review",
                "What did I capture today",
                "Show today's memories",
                "Morning knowledge review"
            ],
            shortTitle: "Daily Review",
            systemImageName: "calendar"
        )
    }
}
```

## 🔍 Spotlight Integration

### Core Spotlight Indexing

Index all memories for system-wide search:

```swift
import CoreSpotlight
import UniformTypeIdentifiers

class SpotlightIndexer {
    static let shared = SpotlightIndexer()
    
    private let index = CSSearchableIndex.default()
    private let domainIdentifier = "com.nornicdb.memories"
    
    // MARK: - Index Single Memory
    
    func indexMemory(_ memory: Memory) {
        let attributeSet = CSSearchableItemAttributeSet(contentType: .text)
        
        // Basic metadata
        attributeSet.title = String(memory.content.prefix(60))
        attributeSet.contentDescription = memory.content
        attributeSet.keywords = memory.tags + ["memory", "nornicdb", "knowledge"]
        
        // Dates
        attributeSet.contentCreationDate = memory.timestamp
        attributeSet.contentModificationDate = memory.lastModified
        
        // Custom attributes
        attributeSet.identifier = memory.id
        attributeSet.relatedUniqueIdentifier = memory.id
        attributeSet.domainIdentifier = domainIdentifier
        
        // Graph-specific metadata (searchable as "memory:type:note")
        attributeSet.setValue(memory.type, forCustomKey: CSCustomAttributeKey(keyName: "memoryType")!)
        attributeSet.setValue(memory.connections, forCustomKey: CSCustomAttributeKey(keyName: "connections")!)
        attributeSet.setValue(memory.decayScore, forCustomKey: CSCustomAttributeKey(keyName: "importance")!)
        
        // Create searchable item
        let item = CSSearchableItem(
            uniqueIdentifier: memory.id,
            domainIdentifier: domainIdentifier,
            attributeSet: attributeSet
        )
        
        // Set expiration based on decay (low decay = expires sooner from Spotlight)
        let daysUntilExpiry = Int(memory.decayScore * 365 * 2)  // Up to 2 years
        item.expirationDate = Calendar.current.date(
            byAdding: .day,
            value: max(30, daysUntilExpiry),  // Minimum 30 days
            to: Date()
        )
        
        index.indexSearchableItems([item]) { error in
            if let error = error {
                print("❌ Spotlight indexing error: \(error)")
            }
        }
    }
    
    // MARK: - Batch Indexing
    
    func reindexAll() async {
        print("🔍 Reindexing all memories for Spotlight...")
        
        // Delete existing index
        try? await index.deleteAllSearchableItems()
        
        // Get all memories from NornicDB
        let client = NornicDBClient.shared
        guard let memories = try? await client.getAllMemories() else {
            print("❌ Failed to fetch memories for indexing")
            return
        }
        
        // Batch index
        let batchSize = 100
        for batch in memories.chunked(into: batchSize) {
            let items = batch.map { memory -> CSSearchableItem in
                let attributeSet = CSSearchableItemAttributeSet(contentType: .text)
                attributeSet.title = String(memory.content.prefix(60))
                attributeSet.contentDescription = memory.content
                attributeSet.keywords = memory.tags
                attributeSet.contentCreationDate = memory.timestamp
                
                return CSSearchableItem(
                    uniqueIdentifier: memory.id,
                    domainIdentifier: domainIdentifier,
                    attributeSet: attributeSet
                )
            }
            
            try? await index.indexSearchableItems(items)
        }
        
        print("✅ Indexed \(memories.count) memories for Spotlight")
    }
    
    // MARK: - Handle Spotlight Selection
    
    func handleSpotlightContinuation(_ userActivity: NSUserActivity) -> Bool {
        guard userActivity.activityType == CSSearchableItemActionType,
              let identifier = userActivity.userInfo?[CSSearchableItemActivityIdentifier] as? String else {
            return false
        }
        
        // Open memory in NornicDB UI
        let url = URL(string: "http://localhost:7474/memory/\(identifier)")!
        NSWorkspace.shared.open(url)
        
        return true
    }
}
```

### Quick Look Preview (qlgenerator)

Create a Quick Look generator for `.nornic` graph export files:

```swift
// NornicDBQuickLook/PreviewProvider.swift

import Cocoa
import QuickLookUI

class PreviewProvider: QLPreviewProvider {
    
    func providePreview(for request: QLFilePreviewRequest) async -> QLPreviewReply {
        // Load graph data
        let data = try! Data(contentsOf: request.fileURL)
        let graph = try! JSONDecoder().decode(GraphExport.self, from: data)
        
        // Generate HTML preview with graph visualization
        let html = generateGraphPreview(graph)
        
        return QLPreviewReply(dataOfContentType: .html, contentSize: CGSize(width: 800, height: 600)) { replyToUpdate in
            return html.data(using: .utf8)!
        }
    }
    
    private func generateGraphPreview(_ graph: GraphExport) -> String {
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { font-family: -apple-system; padding: 20px; background: #1a1a2e; color: #eee; }
                .stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }
                .stat { background: #16213e; padding: 20px; border-radius: 10px; text-align: center; }
                .stat-value { font-size: 32px; font-weight: bold; color: #e94560; }
                .stat-label { color: #888; margin-top: 5px; }
                .preview { margin-top: 20px; }
                .memory { background: #16213e; padding: 15px; border-radius: 8px; margin: 10px 0; }
                .memory-content { color: #fff; }
                .memory-tags { color: #e94560; font-size: 12px; margin-top: 5px; }
            </style>
        </head>
        <body>
            <h1>📊 NornicDB Graph Export</h1>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-value">\(graph.nodes.count)</div>
                    <div class="stat-label">Memories</div>
                </div>
                <div class="stat">
                    <div class="stat-value">\(graph.edges.count)</div>
                    <div class="stat-label">Connections</div>
                </div>
                <div class="stat">
                    <div class="stat-value">\(graph.tags.count)</div>
                    <div class="stat-label">Tags</div>
                </div>
            </div>
            
            <div class="preview">
                <h2>Recent Memories</h2>
                \(graph.nodes.prefix(5).map { node in
                    """
                    <div class="memory">
                        <div class="memory-content">\(node.content.prefix(200))...</div>
                        <div class="memory-tags">\(node.tags.joined(separator: ", "))</div>
                    </div>
                    """
                }.joined())
            </div>
        </body>
        </html>
        """
    }
}
```

## ⚡ Shortcuts Actions

### Custom Shortcut Actions

Define reusable actions for Shortcuts.app:

```swift
import AppIntents

// MARK: - Export Graph Action

@available(macOS 13.0, *)
struct ExportGraphAction: AppIntent {
    static var title: LocalizedStringResource = "Export Knowledge Graph"
    static var description = IntentDescription("Export your knowledge graph to a JSON file")
    
    @Parameter(title: "Output Folder")
    var outputFolder: IntentFile?
    
    @Parameter(title: "Include Embeddings", default: false)
    var includeEmbeddings: Bool
    
    func perform() async throws -> some IntentResult & ReturnsValue<IntentFile> {
        let client = NornicDBClient.shared
        
        // Export graph
        let export = try await client.exportGraph(includeEmbeddings: includeEmbeddings)
        let jsonData = try JSONEncoder().encode(export)
        
        // Write to file
        let filename = "nornicdb-export-\(Date().formatted(.iso8601)).json"
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(filename)
        try jsonData.write(to: tempURL)
        
        return .result(value: IntentFile(fileURL: tempURL))
    }
}

// MARK: - Import Memories Action

@available(macOS 13.0, *)
struct ImportMemoriesAction: AppIntent {
    static var title: LocalizedStringResource = "Import Memories"
    static var description = IntentDescription("Import memories from a text file or JSON")
    
    @Parameter(title: "File")
    var file: IntentFile
    
    @Parameter(title: "Auto-tag", default: true)
    var autoTag: Bool
    
    func perform() async throws -> some IntentResult & ProvidesDialog {
        let client = NornicDBClient.shared
        
        let data = try Data(contentsOf: file.fileURL!)
        
        // Detect format and import
        var importedCount = 0
        
        if let jsonExport = try? JSONDecoder().decode(GraphExport.self, from: data) {
            // JSON export format
            importedCount = try await client.importGraph(jsonExport)
        } else if let text = String(data: data, encoding: .utf8) {
            // Plain text - split by newlines
            let lines = text.split(separator: "\n").map { String($0) }
            for line in lines where !line.isEmpty {
                try await client.createMemory(content: line, autoTag: autoTag)
                importedCount += 1
            }
        }
        
        return .result(dialog: IntentDialog("Imported \(importedCount) memories successfully!"))
    }
}

// MARK: - Run Cypher Query Action

@available(macOS 13.0, *)
struct RunCypherAction: AppIntent {
    static var title: LocalizedStringResource = "Run Cypher Query"
    static var description = IntentDescription("Execute a Cypher query against NornicDB")
    
    @Parameter(title: "Query")
    var query: String
    
    func perform() async throws -> some IntentResult & ReturnsValue<String> {
        let client = NornicDBClient.shared
        
        let result = try await client.execute(query, params: [:])
        let jsonResult = try JSONEncoder().encode(result)
        
        return .result(value: String(data: jsonResult, encoding: .utf8)!)
    }
}

// MARK: - Get Memory Stats Action

@available(macOS 13.0, *)
struct MemoryStatsAction: AppIntent {
    static var title: LocalizedStringResource = "Memory Statistics"
    static var description = IntentDescription("Get statistics about your knowledge graph")
    
    func perform() async throws -> some IntentResult & ReturnsValue<String> & ProvidesDialog {
        let client = NornicDBClient.shared
        
        let stats = try await client.getStats()
        
        let summary = """
        📊 NornicDB Statistics
        
        Total Memories: \(stats.nodeCount)
        Total Connections: \(stats.edgeCount)
        Unique Tags: \(stats.tagCount)
        Database Size: \(stats.sizeFormatted)
        
        Most Used Tags: \(stats.topTags.prefix(5).joined(separator: ", "))
        Average Connections: \(String(format: "%.1f", stats.avgConnections))
        """
        
        return .result(value: summary, dialog: IntentDialog(summary))
    }
}
```

## 🖱️ Services Menu Integration

Add NornicDB to the right-click context menu:

### Services Definition (Info.plist)

```xml
<key>NSServices</key>
<array>
    <dict>
        <key>NSMenuItem</key>
        <dict>
            <key>default</key>
            <string>Add to NornicDB</string>
        </dict>
        <key>NSMessage</key>
        <string>addToNornicDB</string>
        <key>NSPortName</key>
        <string>NornicDB</string>
        <key>NSSendTypes</key>
        <array>
            <string>NSStringPboardType</string>
            <string>public.utf8-plain-text</string>
        </array>
    </dict>
    <dict>
        <key>NSMenuItem</key>
        <dict>
            <key>default</key>
            <string>Search in NornicDB</string>
        </dict>
        <key>NSMessage</key>
        <string>searchInNornicDB</string>
        <key>NSPortName</key>
        <string>NornicDB</string>
        <key>NSSendTypes</key>
        <array>
            <string>NSStringPboardType</string>
            <string>public.utf8-plain-text</string>
        </array>
    </dict>
</array>
```

### Service Handlers

```swift
extension AppDelegate {
    
    // Service: Add selected text to NornicDB
    @objc func addToNornicDB(_ pboard: NSPasteboard, userData: String, error: AutoreleasingUnsafeMutablePointer<NSString>) {
        guard let text = pboard.string(forType: .string), !text.isEmpty else {
            error.pointee = "No text selected" as NSString
            return
        }
        
        Task {
            let client = NornicDBClient.shared
            
            do {
                // Create memory from selected text
                let memory = try await client.createMemory(
                    content: text,
                    type: "Note",
                    source: "services"
                )
                
                // Show notification
                await MainActor.run {
                    showNotification(
                        title: "Memory Saved",
                        body: "Added to NornicDB with \(memory.connections) connections"
                    )
                }
            } catch {
                error.pointee = "Failed to save: \(error.localizedDescription)" as NSString
            }
        }
    }
    
    // Service: Search NornicDB for selected text
    @objc func searchInNornicDB(_ pboard: NSPasteboard, userData: String, error: AutoreleasingUnsafeMutablePointer<NSString>) {
        guard let text = pboard.string(forType: .string), !text.isEmpty else {
            error.pointee = "No text selected" as NSString
            return
        }
        
        // Open search in web UI
        let escaped = text.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? text
        let url = URL(string: "http://localhost:7474/search?q=\(escaped)")!
        NSWorkspace.shared.open(url)
    }
}
```

## 🔔 Notification Center

### Smart Memory Reminders

```swift
import UserNotifications

class MemoryNotificationManager {
    static let shared = MemoryNotificationManager()
    
    private let center = UNUserNotificationCenter.current()
    
    func requestPermission() async -> Bool {
        do {
            return try await center.requestAuthorization(options: [.alert, .sound, .badge])
        } catch {
            return false
        }
    }
    
    // MARK: - Daily Digest
    
    func scheduleDailyDigest(at hour: Int = 9) {
        let content = UNMutableNotificationContent()
        content.title = "📚 Daily Knowledge Review"
        content.subtitle = "Your morning memory summary"
        content.categoryIdentifier = "DAILY_DIGEST"
        
        var dateComponents = DateComponents()
        dateComponents.hour = hour
        dateComponents.minute = 0
        
        let trigger = UNCalendarNotificationTrigger(dateMatching: dateComponents, repeats: true)
        let request = UNNotificationRequest(identifier: "daily-digest", content: content, trigger: trigger)
        
        center.add(request)
    }
    
    // MARK: - Memory Resurrection (based on decay)
    
    func scheduleMemoryResurrection(_ memory: Memory) {
        // Resurface important memories that are decaying
        guard memory.decayScore < 0.5 && memory.decayScore > 0.2 else { return }
        
        let content = UNMutableNotificationContent()
        content.title = "💭 Remember this?"
        content.body = String(memory.content.prefix(100)) + "..."
        content.categoryIdentifier = "MEMORY_RESURRECTION"
        content.userInfo = ["memoryId": memory.id]
        
        // Random time in the next 24-72 hours
        let delay = TimeInterval.random(in: 86400...259200)
        let trigger = UNTimeIntervalNotificationTrigger(timeInterval: delay, repeats: false)
        
        let request = UNNotificationRequest(
            identifier: "resurrect-\(memory.id)",
            content: content,
            trigger: trigger
        )
        
        center.add(request)
    }
    
    // MARK: - Related Content Suggestions
    
    func notifyRelatedContent(for currentWork: String) async {
        let client = NornicDBClient.shared
        
        guard let related = try? await client.vectorSearch(query: currentWork, limit: 3) else {
            return
        }
        
        guard !related.isEmpty else { return }
        
        let content = UNMutableNotificationContent()
        content.title = "💡 Related Knowledge"
        content.body = "Found \(related.count) memories related to what you're working on"
        content.categoryIdentifier = "RELATED_CONTENT"
        content.userInfo = ["memoryIds": related.map { $0.id }]
        
        let request = UNNotificationRequest(
            identifier: "related-\(UUID().uuidString)",
            content: content,
            trigger: nil  // Deliver immediately
        )
        
        try? await center.add(request)
    }
}

// MARK: - Notification Actions

extension MemoryNotificationManager: UNUserNotificationCenterDelegate {
    
    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse
    ) async {
        switch response.actionIdentifier {
        case "VIEW_MEMORY":
            if let memoryId = response.notification.request.content.userInfo["memoryId"] as? String {
                let url = URL(string: "http://localhost:7474/memory/\(memoryId)")!
                NSWorkspace.shared.open(url)
            }
            
        case "BOOST_MEMORY":
            if let memoryId = response.notification.request.content.userInfo["memoryId"] as? String {
                let client = NornicDBClient.shared
                try? await client.boostDecay(memoryId: memoryId)
            }
            
        case "OPEN_DAILY_DIGEST":
            let url = URL(string: "http://localhost:7474/daily-review")!
            NSWorkspace.shared.open(url)
            
        default:
            break
        }
    }
}
```

## ⌨️ Global Hotkeys

### Quick Capture Hotkey

```swift
import Carbon

class GlobalHotkeyManager {
    static let shared = GlobalHotkeyManager()
    
    private var quickCaptureHotKey: EventHotKeyRef?
    private var searchHotKey: EventHotKeyRef?
    
    func registerHotkeys() {
        // Cmd+Shift+N for quick capture
        registerHotkey(
            keyCode: UInt32(kVK_ANSI_N),
            modifiers: UInt32(cmdKey | shiftKey),
            id: 1
        ) { [weak self] in
            self?.showQuickCapture()
        }
        
        // Cmd+Shift+F for quick search
        registerHotkey(
            keyCode: UInt32(kVK_ANSI_F),
            modifiers: UInt32(cmdKey | shiftKey),
            id: 2
        ) { [weak self] in
            self?.showQuickSearch()
        }
    }
    
    private func showQuickCapture() {
        // Show floating capture window
        let captureWindow = QuickCaptureWindow()
        captureWindow.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
    
    private func showQuickSearch() {
        // Show Spotlight-like search popup
        let searchWindow = QuickSearchWindow()
        searchWindow.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
}

// MARK: - Quick Capture Window

class QuickCaptureWindow: NSWindow {
    init() {
        super.init(
            contentRect: NSRect(x: 0, y: 0, width: 600, height: 100),
            styleMask: [.borderless, .resizable],
            backing: .buffered,
            defer: false
        )
        
        self.level = .floating
        self.isOpaque = false
        self.backgroundColor = .clear
        self.center()
        
        let hostingView = NSHostingView(rootView: QuickCaptureView { [weak self] in
            self?.close()
        })
        self.contentView = hostingView
    }
}

struct QuickCaptureView: View {
    @State private var content = ""
    @State private var tags = ""
    let onDismiss: () -> Void
    
    var body: some View {
        VStack(spacing: 12) {
            TextField("What do you want to remember?", text: $content)
                .textFieldStyle(.plain)
                .font(.title2)
                .onSubmit {
                    saveMemory()
                }
            
            HStack {
                TextField("Tags (comma-separated)", text: $tags)
                    .textFieldStyle(.plain)
                    .font(.caption)
                
                Button("Save") {
                    saveMemory()
                }
                .keyboardShortcut(.return)
            }
        }
        .padding(20)
        .background(
            RoundedRectangle(cornerRadius: 16)
                .fill(.ultraThinMaterial)
                .shadow(radius: 20)
        )
        .padding()
    }
    
    private func saveMemory() {
        guard !content.isEmpty else { return }
        
        Task {
            let client = NornicDBClient.shared
            let tagList = tags.split(separator: ",").map { String($0.trimmingCharacters(in: .whitespaces)) }
            
            try? await client.createMemory(content: content, tags: tagList)
            
            await MainActor.run {
                onDismiss()
            }
        }
    }
}
```

## 🔄 Focus Mode Integration

Adapt NornicDB behavior based on active Focus mode:

```swift
import Intents

class FocusModeObserver {
    static let shared = FocusModeObserver()
    
    @Published var currentFocus: INFocus?
    
    func startObserving() {
        // Observe Focus mode changes
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(focusDidChange),
            name: NSNotification.Name("INFocusStatusDidChange"),
            object: nil
        )
        
        // Get initial state
        updateFocusState()
    }
    
    @objc private func focusDidChange() {
        updateFocusState()
    }
    
    private func updateFocusState() {
        INFocusStatusCenter.default.requestAuthorization { status in
            if status == .authorized {
                let focusStatus = INFocusStatusCenter.default.focusStatus
                self.currentFocus = focusStatus.isFocused ? focusStatus.focus : nil
                self.applyFocusContext()
            }
        }
    }
    
    private func applyFocusContext() {
        let context: String?
        
        switch currentFocus?.identifier {
        case "com.apple.focus.work":
            context = "work"
        case "com.apple.focus.personal":
            context = "personal"
        case "com.apple.focus.sleep":
            context = nil  // Disable notifications during sleep
        default:
            context = currentFocus?.identifier
        }
        
        NornicDBClient.shared.setQueryContext(context)
    }
}
```

## 📋 Implementation Checklist

### Phase 1: Core Integration (Week 1-2)
- [ ] App Intents framework setup
- [ ] QueryMemoryIntent implementation
- [ ] StoreMemoryIntent implementation
- [ ] Basic Siri phrases

### Phase 2: Spotlight & Services (Week 3)
- [ ] Core Spotlight indexing
- [ ] Services menu integration
- [ ] Quick Look preview (optional)

### Phase 3: Shortcuts & Automation (Week 4)
- [ ] Export/Import actions
- [ ] Run Cypher action
- [ ] Stats action

### Phase 4: Polish (Week 5)
- [ ] Global hotkeys
- [ ] Notifications
- [ ] Focus mode integration

## 🎯 Example Siri Conversations

### Search
```
User: "Hey Siri, what do I know about React performance?"

Siri: "Found 8 memories about React performance. Most relevant:
       'React.memo() prevents unnecessary re-renders by memoizing 
       components. Use useMemo for expensive calculations and 
       useCallback for stable function references...'"
```

### Store
```
User: "Hey Siri, remember that Docker containers share the host 
       kernel unlike VMs"

Siri: "Saved! I found 5 related memories about Docker and 
       virtualization and connected them."
```

### Summarize
```
User: "Hey Siri, summarize what I've learned about Kubernetes"

Siri: "Based on 23 memories about Kubernetes:

       You've covered deployment strategies, focusing on rolling 
       updates and blue-green deployments. Key concepts include 
       pods, services, and ingress controllers. Most recent notes 
       were about Helm charts and GitOps workflows.
       
       Related topics: Docker, DevOps, AWS EKS"
```

## 📄 Files to Create

```
macos/
├── MenuBarApp/
│   ├── Intents/
│   │   ├── QueryMemoryIntent.swift
│   │   ├── StoreMemoryIntent.swift
│   │   ├── SummarizeIntent.swift
│   │   └── ShortcutsProvider.swift
│   ├── Services/
│   │   ├── SpotlightIndexer.swift
│   │   └── ServiceHandlers.swift
│   ├── Notifications/
│   │   └── NotificationManager.swift
│   └── Hotkeys/
│       ├── GlobalHotkeyManager.swift
│       └── QuickCaptureWindow.swift
└── docs/
    └── APPLE_INTELLIGENCE.md (this file)
```

## Next Steps

1. **Update NornicDBMenuBar.swift** to integrate App Intents
2. **Create NornicDBClient.swift** for Bolt protocol communication from Swift
3. **Add Spotlight indexing** on memory creation/update
4. **Register Services** in Info.plist
5. **Implement global hotkeys** for quick capture

This brings feature parity with iOS while leveraging macOS-specific capabilities like Services menu and global hotkeys! 🚀

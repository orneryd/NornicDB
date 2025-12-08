# Apple Intelligence Integration

NornicDB for iOS deeply integrates with Apple's intelligence features to provide a seamless, voice-first experience for managing your personal knowledge graph.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Apple Intelligence Stack                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         SIRI                                     │   │
│  │  "Hey Siri, what did I learn about Swift last week?"            │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      APP INTENTS                                 │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │   │
│  │  │   Query     │ │   Store     │ │  Summarize  │               │   │
│  │  │   Memory    │ │   Memory    │ │   Topic     │               │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      SHORTCUTS                                   │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │   │
│  │  │Quick Capture│ │Daily Review │ │Export Graph │               │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      SPOTLIGHT                                   │   │
│  │  System-wide search across all memories                         │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      WIDGETS                                     │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │   │
│  │  │Memory of Day│ │Quick Stats  │ │Recent Graph │               │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## App Intents (iOS 16+, Enhanced in iOS 17+)

### 1. Query Memory Intent

The primary Siri interface for searching your knowledge graph.

```swift
import AppIntents

@available(iOS 17.0, *)
struct QueryMemoryIntent: AppIntent {
    static var title: LocalizedStringResource = "Search Memories"
    static var description = IntentDescription(
        "Search your personal knowledge graph using natural language",
        categoryName: "Knowledge"
    )
    
    // Don't open app for simple queries
    static var openAppWhenRun: Bool = false
    
    // Parameters
    @Parameter(title: "Query", description: "What to search for")
    var query: String
    
    @Parameter(title: "Limit", default: 5)
    var limit: Int
    
    @Parameter(title: "Include Related", default: true)
    var includeRelated: Bool
    
    // Main execution
    func perform() async throws -> some IntentResult & ReturnsValue<[MemoryEntity]> & ProvidesDialog {
        let db = NornicDBManager.shared
        
        // Perform vector search
        let results = try await db.vectorSearch(query: query, limit: limit)
        
        // Optionally expand to related memories
        var memories = results
        if includeRelated {
            memories = try await db.expandContext(results, depth: 1)
        }
        
        // Convert to entities
        let entities = memories.map { MemoryEntity(from: $0) }
        
        // Generate spoken response
        let dialog = generateDialog(for: entities, query: query)
        
        return .result(value: entities, dialog: dialog)
    }
    
    private func generateDialog(for entities: [MemoryEntity], query: String) -> IntentDialog {
        if entities.isEmpty {
            return IntentDialog("I couldn't find any memories about \(query).")
        }
        
        let count = entities.count
        let preview = entities.first!.content.prefix(100)
        
        return IntentDialog("""
            Found \(count) memories about \(query). 
            Most relevant: \(preview)...
            """)
    }
}
```

### 2. Store Memory Intent

Quick capture of new memories via voice.

```swift
@available(iOS 17.0, *)
struct StoreMemoryIntent: AppIntent {
    static var title: LocalizedStringResource = "Store Memory"
    static var description = IntentDescription(
        "Save a new memory to your knowledge graph",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = false
    
    @Parameter(title: "Content", description: "What to remember")
    var content: String
    
    @Parameter(title: "Tags", description: "Optional tags")
    var tags: [String]?
    
    @Parameter(title: "Type", default: .note)
    var memoryType: MemoryType
    
    func perform() async throws -> some IntentResult & ProvidesDialog {
        let db = NornicDBManager.shared
        
        // Create the memory
        let memory = try await db.createMemory(
            content: content,
            type: memoryType.rawValue,
            tags: tags ?? []
        )
        
        // Auto-link to related memories
        try await db.autoLink(memory)
        
        return .result(dialog: IntentDialog(
            "Saved! I found \(memory.connections) related memories."
        ))
    }
}

enum MemoryType: String, AppEnum {
    case note = "Note"
    case idea = "Idea"
    case task = "Task"
    case quote = "Quote"
    case learning = "Learning"
    
    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: "Memory Type")
    }
    
    static var caseDisplayRepresentations: [MemoryType: DisplayRepresentation] {
        [
            .note: "Note",
            .idea: "Idea",
            .task: "Task",
            .quote: "Quote",
            .learning: "Learning"
        ]
    }
}
```

### 3. Summarize Topic Intent

Graph-RAG powered summarization.

```swift
@available(iOS 17.0, *)
struct SummarizeTopicIntent: AppIntent {
    static var title: LocalizedStringResource = "Summarize Topic"
    static var description = IntentDescription(
        "Get an AI summary of everything you know about a topic",
        categoryName: "Knowledge"
    )
    
    static var openAppWhenRun: Bool = false
    
    @Parameter(title: "Topic")
    var topic: String
    
    @Parameter(title: "Time Range", default: .allTime)
    var timeRange: TimeRange
    
    func perform() async throws -> some IntentResult & ProvidesDialog {
        let db = NornicDBManager.shared
        let rag = GraphRAG.shared
        
        // Find all memories about topic
        let memories = try await db.vectorSearch(
            query: topic,
            limit: 20,
            timeRange: timeRange.dateRange
        )
        
        // Expand graph context
        let context = try await db.expandContext(memories, depth: 2)
        
        // Generate summary
        let summary = try await rag.summarize(
            topic: topic,
            context: context
        )
        
        return .result(dialog: IntentDialog(summary))
    }
}

enum TimeRange: String, AppEnum {
    case today = "Today"
    case thisWeek = "This Week"
    case thisMonth = "This Month"
    case thisYear = "This Year"
    case allTime = "All Time"
    
    var dateRange: ClosedRange<Date>? {
        let now = Date()
        switch self {
        case .today:
            return Calendar.current.startOfDay(for: now)...now
        case .thisWeek:
            let weekAgo = Calendar.current.date(byAdding: .day, value: -7, to: now)!
            return weekAgo...now
        case .thisMonth:
            let monthAgo = Calendar.current.date(byAdding: .month, value: -1, to: now)!
            return monthAgo...now
        case .thisYear:
            let yearAgo = Calendar.current.date(byAdding: .year, value: -1, to: now)!
            return yearAgo...now
        case .allTime:
            return nil
        }
    }
    
    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(name: "Time Range")
    }
    
    static var caseDisplayRepresentations: [TimeRange: DisplayRepresentation] {
        [
            .today: "Today",
            .thisWeek: "This Week",
            .thisMonth: "This Month",
            .thisYear: "This Year",
            .allTime: "All Time"
        ]
    }
}
```

### 4. Show Connections Intent

Visualize relationships for a topic.

```swift
@available(iOS 17.0, *)
struct ShowConnectionsIntent: AppIntent {
    static var title: LocalizedStringResource = "Show Connections"
    static var description = IntentDescription(
        "See how a memory connects to others",
        categoryName: "Knowledge"
    )
    
    // Opens app for visualization
    static var openAppWhenRun: Bool = true
    
    @Parameter(title: "Memory")
    var memory: MemoryEntity
    
    @Parameter(title: "Depth", default: 2)
    var depth: Int
    
    func perform() async throws -> some IntentResult {
        // App will handle visualization
        NavigationState.shared.showConnections(
            memoryId: memory.id,
            depth: depth
        )
        
        return .result()
    }
}
```

## Siri Phrases (App Shortcuts Provider)

Define natural phrases users can say to trigger intents.

```swift
@available(iOS 17.0, *)
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
                "Remember \(\.$query) in \(.applicationName)",
                "Search my memories for \(\.$query)",
                "What did I learn about \(\.$query)"
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
                "Store memory: \(\.$content)",
                "Add to my brain: \(\.$content)",
                "Note to self in \(.applicationName): \(\.$content)"
            ],
            shortTitle: "Store Memory",
            systemImageName: "plus.circle"
        )
        
        // Summarize topic
        AppShortcut(
            intent: SummarizeTopicIntent(),
            phrases: [
                "Summarize \(\.$topic) in \(.applicationName)",
                "What do I know about \(\.$topic)",
                "Give me a summary of \(\.$topic)",
                "Tell me about \(\.$topic) from my memories"
            ],
            shortTitle: "Summarize",
            systemImageName: "doc.text"
        )
        
        // Daily review
        AppShortcut(
            intent: DailyReviewIntent(),
            phrases: [
                "Daily review in \(.applicationName)",
                "What did I capture today",
                "Show today's memories",
                "Review my day in \(.applicationName)"
            ],
            shortTitle: "Daily Review",
            systemImageName: "calendar"
        )
        
        // Quick stats
        AppShortcut(
            intent: MemoryStatsIntent(),
            phrases: [
                "Memory stats in \(.applicationName)",
                "How many memories do I have",
                "Show my \(.applicationName) statistics"
            ],
            shortTitle: "Stats",
            systemImageName: "chart.bar"
        )
    }
}
```

## Entity Definitions

Define data types that Siri can understand and display.

```swift
@available(iOS 17.0, *)
struct MemoryEntity: AppEntity, Identifiable {
    
    static var typeDisplayRepresentation: TypeDisplayRepresentation {
        TypeDisplayRepresentation(
            name: LocalizedStringResource("Memory"),
            numericFormat: "\(placeholder: .int) memories"
        )
    }
    
    static var defaultQuery = MemoryEntityQuery()
    
    var id: String
    var content: String
    var timestamp: Date
    var tags: [String]
    var connections: Int
    var decayScore: Double
    var type: String
    
    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(
            title: "\(content.prefix(50))...",
            subtitle: "\(tags.joined(separator: ", "))",
            image: .init(systemName: iconForType(type))
        )
    }
    
    init(from memory: MemoryNode) {
        self.id = memory.id
        self.content = memory.content
        self.timestamp = memory.timestamp
        self.tags = memory.tags
        self.connections = memory.connectionCount
        self.decayScore = memory.decayScore
        self.type = memory.type
    }
    
    private func iconForType(_ type: String) -> String {
        switch type {
        case "Note": return "note.text"
        case "Idea": return "lightbulb"
        case "Task": return "checkmark.circle"
        case "Quote": return "quote.bubble"
        case "Learning": return "book"
        default: return "brain"
        }
    }
}

struct MemoryEntityQuery: EntityQuery {
    
    func entities(for identifiers: [String]) async throws -> [MemoryEntity] {
        let db = NornicDBManager.shared
        return try await db.getMemories(ids: identifiers).map { MemoryEntity(from: $0) }
    }
    
    func suggestedEntities() async throws -> [MemoryEntity] {
        let db = NornicDBManager.shared
        // Return recent/important memories
        return try await db.getRecentMemories(limit: 10).map { MemoryEntity(from: $0) }
    }
}
```

## Spotlight Integration

Index memories for system-wide search.

```swift
import CoreSpotlight
import MobileCoreServices

class SpotlightIndexer {
    static let shared = SpotlightIndexer()
    
    private let index = CSSearchableIndex.default()
    
    func indexMemory(_ memory: MemoryNode) {
        let attributeSet = CSSearchableItemAttributeSet(contentType: .text)
        
        attributeSet.title = memory.content.prefix(50).description
        attributeSet.contentDescription = memory.content
        attributeSet.keywords = memory.tags
        attributeSet.contentCreationDate = memory.timestamp
        attributeSet.relatedUniqueIdentifier = memory.id
        
        // Custom attributes
        attributeSet.setValue(memory.type, forCustomKey: "memoryType" as CSCustomAttributeKey)
        attributeSet.setValue(memory.decayScore, forCustomKey: "decayScore" as CSCustomAttributeKey)
        
        let item = CSSearchableItem(
            uniqueIdentifier: memory.id,
            domainIdentifier: "com.nornicdb.memories",
            attributeSet: attributeSet
        )
        
        // Set expiration based on decay
        let daysUntilExpiry = Int(memory.decayScore * 365)
        item.expirationDate = Calendar.current.date(
            byAdding: .day,
            value: daysUntilExpiry,
            to: Date()
        )
        
        index.indexSearchableItems([item])
    }
    
    func removeMemory(id: String) {
        index.deleteSearchableItems(withIdentifiers: [id])
    }
    
    func reindexAll() async {
        // Delete existing index
        try? await index.deleteAllSearchableItems()
        
        // Reindex all memories
        let db = NornicDBManager.shared
        let memories = try? await db.getAllMemories()
        
        for memory in memories ?? [] {
            indexMemory(memory)
        }
    }
}

// Handle Spotlight taps
extension AppDelegate {
    func application(
        _ application: UIApplication,
        continue userActivity: NSUserActivity,
        restorationHandler: @escaping ([UIUserActivityRestoring]?) -> Void
    ) -> Bool {
        if userActivity.activityType == CSSearchableItemActionType,
           let identifier = userActivity.userInfo?[CSSearchableItemActivityIdentifier] as? String {
            // Navigate to memory
            NavigationState.shared.showMemory(id: identifier)
            return true
        }
        return false
    }
}
```

## WidgetKit Integration

### Memory of the Day Widget

```swift
import WidgetKit
import SwiftUI

struct MemoryOfDayEntry: TimelineEntry {
    let date: Date
    let memory: MemoryNode?
}

struct MemoryOfDayProvider: TimelineProvider {
    
    func placeholder(in context: Context) -> MemoryOfDayEntry {
        MemoryOfDayEntry(date: Date(), memory: nil)
    }
    
    func getSnapshot(in context: Context, completion: @escaping (MemoryOfDayEntry) -> Void) {
        Task {
            let memory = try? await NornicDBManager.shared.getRandomMemory()
            completion(MemoryOfDayEntry(date: Date(), memory: memory))
        }
    }
    
    func getTimeline(in context: Context, completion: @escaping (Timeline<MemoryOfDayEntry>) -> Void) {
        Task {
            let memory = try? await NornicDBManager.shared.getRandomImportantMemory()
            let entry = MemoryOfDayEntry(date: Date(), memory: memory)
            
            // Refresh daily
            let nextUpdate = Calendar.current.date(
                byAdding: .hour,
                value: 24,
                to: Date()
            )!
            
            let timeline = Timeline(entries: [entry], policy: .after(nextUpdate))
            completion(timeline)
        }
    }
}

struct MemoryOfDayWidget: Widget {
    let kind: String = "MemoryOfDay"
    
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: kind, provider: MemoryOfDayProvider()) { entry in
            MemoryOfDayWidgetView(entry: entry)
        }
        .configurationDisplayName("Memory of the Day")
        .description("Resurface a meaningful memory")
        .supportedFamilies([.systemSmall, .systemMedium, .systemLarge])
    }
}

struct MemoryOfDayWidgetView: View {
    let entry: MemoryOfDayEntry
    
    var body: some View {
        if let memory = entry.memory {
            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Image(systemName: "brain.head.profile")
                        .foregroundColor(.purple)
                    Text("Memory")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Text(memory.content)
                    .font(.body)
                    .lineLimit(4)
                
                Spacer()
                
                HStack {
                    Text(memory.timestamp, style: .relative)
                        .font(.caption2)
                        .foregroundColor(.secondary)
                    
                    Spacer()
                    
                    Text("\(memory.connectionCount) connections")
                        .font(.caption2)
                        .foregroundColor(.purple)
                }
            }
            .padding()
        } else {
            VStack {
                Image(systemName: "brain")
                    .font(.largeTitle)
                    .foregroundColor(.gray)
                Text("No memories yet")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
    }
}
```

### Quick Stats Widget

```swift
struct QuickStatsEntry: TimelineEntry {
    let date: Date
    let totalMemories: Int
    let todayCount: Int
    let topTag: String?
    let connectionCount: Int
}

struct QuickStatsWidget: Widget {
    let kind: String = "QuickStats"
    
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: kind, provider: QuickStatsProvider()) { entry in
            QuickStatsWidgetView(entry: entry)
        }
        .configurationDisplayName("Memory Stats")
        .description("Quick overview of your knowledge graph")
        .supportedFamilies([.systemSmall])
    }
}

struct QuickStatsWidgetView: View {
    let entry: QuickStatsEntry
    
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Image(systemName: "chart.bar.fill")
                    .foregroundColor(.purple)
                Text("NornicDB")
                    .font(.caption.bold())
            }
            
            HStack {
                StatItem(value: entry.totalMemories, label: "Total")
                Spacer()
                StatItem(value: entry.todayCount, label: "Today")
            }
            
            Divider()
            
            HStack {
                Image(systemName: "link")
                    .font(.caption)
                Text("\(entry.connectionCount) connections")
                    .font(.caption)
            }
            .foregroundColor(.secondary)
            
            if let tag = entry.topTag {
                HStack {
                    Image(systemName: "tag")
                        .font(.caption)
                    Text(tag)
                        .font(.caption)
                }
                .foregroundColor(.purple)
            }
        }
        .padding()
    }
}

struct StatItem: View {
    let value: Int
    let label: String
    
    var body: some View {
        VStack {
            Text("\(value)")
                .font(.title2.bold())
            Text(label)
                .font(.caption2)
                .foregroundColor(.secondary)
        }
    }
}
```

## Focus Mode Integration

Different memory contexts per Focus mode.

```swift
import Intents

class FocusModeHandler {
    static let shared = FocusModeHandler()
    
    func handleFocusChange(to focus: INFocus?) {
        guard let focus = focus else {
            // No focus active
            NornicDBManager.shared.setContext(nil)
            return
        }
        
        // Map focus to memory context
        let context: String?
        switch focus.identifier {
        case "com.apple.focus.work":
            context = "work"
        case "com.apple.focus.personal":
            context = "personal"
        case "com.apple.focus.sleep":
            context = nil // Don't show memories during sleep
        default:
            context = focus.identifier
        }
        
        NornicDBManager.shared.setContext(context)
    }
}

// Update memory queries based on focus
extension NornicDBManager {
    private var currentContext: String?
    
    func setContext(_ context: String?) {
        self.currentContext = context
    }
    
    func vectorSearch(query: String, limit: Int) async throws -> [MemoryNode] {
        var cypher = """
            CALL db.index.vector.queryNodes('memory_embeddings', \(limit * 2), $embedding)
            YIELD node, score
        """
        
        // Filter by context if active
        if let context = currentContext {
            cypher += """
                WHERE node.context = '\(context)' OR node.context IS NULL
            """
        }
        
        cypher += """
            RETURN node, score
            ORDER BY score DESC
            LIMIT \(limit)
        """
        
        return try await query(cypher)
    }
}
```

## Handoff Support

Start on iPhone, continue on Mac.

```swift
import UIKit

class HandoffManager {
    static let shared = HandoffManager()
    
    func createUserActivity(for memory: MemoryNode) -> NSUserActivity {
        let activity = NSUserActivity(activityType: "com.nornicdb.viewMemory")
        activity.title = "View Memory"
        activity.userInfo = ["memoryId": memory.id]
        activity.isEligibleForHandoff = true
        activity.isEligibleForSearch = true
        
        // For macOS continuation
        activity.requiredUserInfoKeys = ["memoryId"]
        
        return activity
    }
    
    func handleIncomingActivity(_ activity: NSUserActivity) -> Bool {
        guard activity.activityType == "com.nornicdb.viewMemory",
              let memoryId = activity.userInfo?["memoryId"] as? String else {
            return false
        }
        
        NavigationState.shared.showMemory(id: memoryId)
        return true
    }
}

// Usage in SwiftUI
struct MemoryDetailView: View {
    let memory: MemoryNode
    
    var body: some View {
        ScrollView {
            // ... memory content
        }
        .userActivity("com.nornicdb.viewMemory") { activity in
            activity.title = "View Memory"
            activity.userInfo = ["memoryId": memory.id]
            activity.isEligibleForHandoff = true
        }
    }
}
```

## Live Activities (iOS 16.1+)

Real-time knowledge graph updates.

```swift
import ActivityKit

struct MemoryCaptureAttributes: ActivityAttributes {
    public struct ContentState: Codable, Hashable {
        var capturedCount: Int
        var lastMemory: String
        var status: CaptureStatus
    }
    
    var sessionName: String
    var startTime: Date
}

enum CaptureStatus: String, Codable {
    case capturing
    case processing
    case complete
}

class LiveActivityManager {
    static let shared = LiveActivityManager()
    
    private var currentActivity: Activity<MemoryCaptureAttributes>?
    
    func startCaptureSession(name: String) async {
        let attributes = MemoryCaptureAttributes(
            sessionName: name,
            startTime: Date()
        )
        
        let initialState = MemoryCaptureAttributes.ContentState(
            capturedCount: 0,
            lastMemory: "",
            status: .capturing
        )
        
        do {
            currentActivity = try Activity.request(
                attributes: attributes,
                contentState: initialState,
                pushType: nil
            )
        } catch {
            print("Failed to start live activity: \(error)")
        }
    }
    
    func updateCapture(count: Int, lastMemory: String) async {
        guard let activity = currentActivity else { return }
        
        let state = MemoryCaptureAttributes.ContentState(
            capturedCount: count,
            lastMemory: String(lastMemory.prefix(30)),
            status: .capturing
        )
        
        await activity.update(using: state)
    }
    
    func endCaptureSession() async {
        guard let activity = currentActivity else { return }
        
        let finalState = MemoryCaptureAttributes.ContentState(
            capturedCount: 0,
            lastMemory: "",
            status: .complete
        )
        
        await activity.end(using: finalState, dismissalPolicy: .after(.now + 60))
        currentActivity = nil
    }
}
```

## Example Siri Conversations

### Memory Search
```
User: "Hey Siri, what do I know about machine learning?"

Siri: "Found 12 memories about machine learning. Most relevant:
       'Neural networks use backpropagation for training...'
       Would you like me to summarize or show all?"

User: "Summarize"

Siri: "Based on your 12 memories about machine learning:
       - You've studied neural networks, focusing on backpropagation
       - Related topics include Python, TensorFlow, and your AI project
       - Most recent note was 3 days ago about attention mechanisms
       - Connected to 5 people including your mentor Alex"
```

### Quick Capture
```
User: "Hey Siri, remember that the design review is moved to Thursday"

Siri: "Saved to NornicDB. I found 3 related memories about design reviews
       and connected them automatically."
```

### Context-Aware
```
User: [Work Focus active]
      "Hey Siri, show my recent notes"

Siri: "Here are your recent work-related notes:
       - Project timeline update (2 hours ago)
       - Meeting with stakeholders (yesterday)
       - API design decisions (3 days ago)"
```

## Next Steps

1. [Metal ML Implementation →](METAL_ML.md)
2. [Background Processing →](BACKGROUND_PROCESSING.md)
3. [Model Requirements →](MODELS.md)

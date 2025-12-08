# Background Processing

NornicDB for iOS uses Apple's BGTaskScheduler to perform heavy operations while minimizing battery impact.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Background Task Architecture                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    FOREGROUND TASKS                              │   │
│  │  • Vector search (<100ms)                                        │   │
│  │  • Single embedding (<20ms)                                      │   │
│  │  • Graph queries (<50ms)                                         │   │
│  │  • Store memory (<10ms)                                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │               BGAppRefreshTask (30 seconds max)                  │   │
│  │  • Memory decay calculations                                     │   │
│  │  • Spotlight index updates                                       │   │
│  │  • Statistics computation                                        │   │
│  │  • Quick relationship updates                                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              BGProcessingTask (minutes, charging)                │   │
│  │  🔌 Requires External Power                                      │   │
│  │  • Full embedding regeneration                                   │   │
│  │  • K-means reclustering                                          │   │
│  │  • Database compaction                                           │   │
│  │  • Large batch imports                                           │   │
│  │  • Vector index rebuilding                                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Task Definitions

### Info.plist Configuration

```xml
<key>BGTaskSchedulerPermittedIdentifiers</key>
<array>
    <string>com.nornicdb.embedding-refresh</string>
    <string>com.nornicdb.decay-update</string>
    <string>com.nornicdb.db-compaction</string>
    <string>com.nornicdb.kmeans-clustering</string>
    <string>com.nornicdb.spotlight-sync</string>
</array>
<key>UIBackgroundModes</key>
<array>
    <string>fetch</string>
    <string>processing</string>
</array>
```

### Task Registration

```swift
import BackgroundTasks

class BackgroundTaskManager {
    static let shared = BackgroundTaskManager()
    
    // Task identifiers
    private enum TaskID {
        static let embeddingRefresh = "com.nornicdb.embedding-refresh"
        static let decayUpdate = "com.nornicdb.decay-update"
        static let dbCompaction = "com.nornicdb.db-compaction"
        static let kmeansClustering = "com.nornicdb.kmeans-clustering"
        static let spotlightSync = "com.nornicdb.spotlight-sync"
    }
    
    func registerAllTasks() {
        // Processing tasks (require charging)
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: TaskID.embeddingRefresh,
            using: nil
        ) { task in
            self.handleEmbeddingRefresh(task as! BGProcessingTask)
        }
        
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: TaskID.kmeansClustering,
            using: nil
        ) { task in
            self.handleKMeansClustering(task as! BGProcessingTask)
        }
        
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: TaskID.dbCompaction,
            using: nil
        ) { task in
            self.handleDBCompaction(task as! BGProcessingTask)
        }
        
        // Refresh tasks (quick, no charging required)
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: TaskID.decayUpdate,
            using: nil
        ) { task in
            self.handleDecayUpdate(task as! BGAppRefreshTask)
        }
        
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: TaskID.spotlightSync,
            using: nil
        ) { task in
            self.handleSpotlightSync(task as! BGAppRefreshTask)
        }
    }
}
```

## Processing Tasks (Charging Required)

### Embedding Refresh

Re-generates embeddings for memories that were stored without them (offline captures).

```swift
extension BackgroundTaskManager {
    
    func scheduleEmbeddingRefresh() {
        let request = BGProcessingTaskRequest(identifier: TaskID.embeddingRefresh)
        request.requiresExternalPower = true  // Only when charging
        request.requiresNetworkConnectivity = false  // Works offline
        request.earliestBeginDate = Date(timeIntervalSinceNow: 3600)  // 1 hour
        
        do {
            try BGTaskScheduler.shared.submit(request)
            print("📅 Scheduled embedding refresh")
        } catch {
            print("❌ Failed to schedule embedding refresh: \(error)")
        }
    }
    
    private func handleEmbeddingRefresh(_ task: BGProcessingTask) {
        print("🔄 Starting embedding refresh...")
        
        // Create operation queue
        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1
        
        let operation = EmbeddingRefreshOperation()
        
        // Handle expiration
        task.expirationHandler = {
            operation.cancel()
            print("⏰ Embedding refresh expired")
        }
        
        // Completion
        operation.completionBlock = {
            let success = !operation.isCancelled
            task.setTaskCompleted(success: success)
            
            if success {
                print("✅ Embedding refresh completed")
                // Schedule next refresh
                self.scheduleEmbeddingRefresh()
            } else {
                print("❌ Embedding refresh cancelled")
            }
        }
        
        queue.addOperation(operation)
    }
}

class EmbeddingRefreshOperation: Operation {
    private let db = NornicDBManager.shared
    private let embedder = ModelManager.shared
    
    override func main() {
        guard !isCancelled else { return }
        
        // Find memories without embeddings
        let memoriesNeedingEmbeddings = db.getMemoriesWithoutEmbeddings()
        
        print("📝 Found \(memoriesNeedingEmbeddings.count) memories needing embeddings")
        
        // Process in batches
        let batchSize = ThermalManager.shared.maxBatchSize
        
        for batch in memoriesNeedingEmbeddings.chunked(into: batchSize) {
            guard !isCancelled else { break }
            
            autoreleasepool {
                // Generate embeddings
                let texts = batch.map { $0.content }
                if let embeddings = try? embedder.embedBatch(texts) {
                    // Store embeddings
                    for (memory, embedding) in zip(batch, embeddings) {
                        db.updateEmbedding(memoryId: memory.id, embedding: embedding)
                    }
                }
            }
            
            // Check thermal state
            if ThermalManager.shared.shouldThrottle {
                Thread.sleep(forTimeInterval: 1.0)
            }
        }
    }
}
```

### K-Means Clustering

Rebuilds vector clusters for efficient search.

```swift
extension BackgroundTaskManager {
    
    func scheduleKMeansClustering() {
        let request = BGProcessingTaskRequest(identifier: TaskID.kmeansClustering)
        request.requiresExternalPower = true
        request.requiresNetworkConnectivity = false
        // Schedule for tomorrow night
        let tomorrow = Calendar.current.date(byAdding: .day, value: 1, to: Date())!
        let nightTime = Calendar.current.date(bySettingHour: 2, minute: 0, second: 0, of: tomorrow)!
        request.earliestBeginDate = nightTime
        
        try? BGTaskScheduler.shared.submit(request)
    }
    
    private func handleKMeansClustering(_ task: BGProcessingTask) {
        print("🎯 Starting k-means clustering...")
        
        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1
        
        let operation = KMeansClusteringOperation()
        
        task.expirationHandler = {
            operation.cancel()
        }
        
        operation.completionBlock = {
            task.setTaskCompleted(success: !operation.isCancelled)
            if !operation.isCancelled {
                self.scheduleKMeansClustering()
            }
        }
        
        queue.addOperation(operation)
    }
}

class KMeansClusteringOperation: Operation {
    private let db = NornicDBManager.shared
    private let metal = MetalAccelerator.shared
    
    override func main() {
        guard !isCancelled else { return }
        
        // Get all embeddings
        let memories = db.getAllMemoriesWithEmbeddings()
        guard memories.count >= 100 else {
            print("📊 Not enough memories for clustering (\(memories.count))")
            return
        }
        
        let vectors = memories.map { $0.embedding! }
        
        // Determine optimal k (sqrt of n / 2)
        let k = max(10, Int(sqrt(Double(vectors.count)) / 2))
        
        print("🎯 Clustering \(vectors.count) vectors into \(k) clusters")
        
        // Run k-means on GPU
        guard let result = try? metal?.kmeansCluster(vectors: vectors, k: k) else {
            print("❌ K-means failed")
            return
        }
        
        // Update cluster assignments
        for (memory, cluster) in zip(memories, result.assignments) {
            guard !isCancelled else { break }
            db.updateCluster(memoryId: memory.id, cluster: cluster)
        }
        
        // Store centroids
        db.storeCentroids(result.centroids)
        
        print("✅ Clustering complete: \(k) clusters created")
    }
}
```

### Database Compaction

Reclaims space and optimizes storage.

```swift
extension BackgroundTaskManager {
    
    func scheduleDBCompaction() {
        let request = BGProcessingTaskRequest(identifier: TaskID.dbCompaction)
        request.requiresExternalPower = true
        request.requiresNetworkConnectivity = false
        // Weekly on Sunday night
        request.earliestBeginDate = nextSunday()
        
        try? BGTaskScheduler.shared.submit(request)
    }
    
    private func handleDBCompaction(_ task: BGProcessingTask) {
        print("🗜️ Starting database compaction...")
        
        let queue = OperationQueue()
        let operation = DBCompactionOperation()
        
        task.expirationHandler = { operation.cancel() }
        
        operation.completionBlock = {
            task.setTaskCompleted(success: !operation.isCancelled)
            self.scheduleDBCompaction()
        }
        
        queue.addOperation(operation)
    }
}

class DBCompactionOperation: Operation {
    override func main() {
        guard !isCancelled else { return }
        
        let db = NornicDBManager.shared
        
        // Get database stats before
        let sizeBefore = db.getDatabaseSize()
        print("📊 Database size before: \(formatBytes(sizeBefore))")
        
        // Run compaction
        db.compact()
        
        // Get stats after
        let sizeAfter = db.getDatabaseSize()
        let saved = sizeBefore - sizeAfter
        print("✅ Compaction complete. Saved \(formatBytes(saved))")
    }
    
    private func formatBytes(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }
}
```

## Refresh Tasks (Quick, No Charging)

### Memory Decay Updates

Updates decay scores for all memories.

```swift
extension BackgroundTaskManager {
    
    func scheduleDecayUpdate() {
        let request = BGAppRefreshTaskRequest(identifier: TaskID.decayUpdate)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 3600)  // 1 hour
        
        try? BGTaskScheduler.shared.submit(request)
    }
    
    private func handleDecayUpdate(_ task: BGAppRefreshTask) {
        print("📉 Updating memory decay scores...")
        
        // Quick operation - must complete in 30 seconds
        let startTime = Date()
        let timeout: TimeInterval = 25  // Leave 5 seconds buffer
        
        task.expirationHandler = {
            print("⏰ Decay update expired")
        }
        
        Task {
            let db = NornicDBManager.shared
            
            // Get memories that need decay update
            let memories = await db.getMemoriesNeedingDecayUpdate(limit: 1000)
            
            for memory in memories {
                guard Date().timeIntervalSince(startTime) < timeout else { break }
                
                // Calculate new decay score
                let newDecay = calculateDecay(
                    createdAt: memory.timestamp,
                    lastAccessed: memory.lastAccessed,
                    accessCount: memory.accessCount,
                    connections: memory.connections
                )
                
                await db.updateDecay(memoryId: memory.id, decay: newDecay)
            }
            
            task.setTaskCompleted(success: true)
            self.scheduleDecayUpdate()
        }
    }
    
    private func calculateDecay(
        createdAt: Date,
        lastAccessed: Date?,
        accessCount: Int,
        connections: Int
    ) -> Double {
        let now = Date()
        
        // Base decay from age
        let ageInDays = now.timeIntervalSince(createdAt) / 86400
        let ageFactor = exp(-ageInDays / 30)  // Half-life of 30 days
        
        // Boost from recent access
        var accessFactor = 1.0
        if let lastAccessed = lastAccessed {
            let daysSinceAccess = now.timeIntervalSince(lastAccessed) / 86400
            accessFactor = 1.0 + exp(-daysSinceAccess / 7)  // Recent access boosts
        }
        
        // Boost from connections
        let connectionFactor = 1.0 + Double(connections) * 0.1
        
        // Boost from access count
        let usageFactor = 1.0 + log1p(Double(accessCount)) * 0.2
        
        return min(1.0, ageFactor * accessFactor * connectionFactor * usageFactor)
    }
}
```

### Spotlight Sync

Keeps Spotlight index up to date.

```swift
extension BackgroundTaskManager {
    
    func scheduleSpotlightSync() {
        let request = BGAppRefreshTaskRequest(identifier: TaskID.spotlightSync)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 7200)  // 2 hours
        
        try? BGTaskScheduler.shared.submit(request)
    }
    
    private func handleSpotlightSync(_ task: BGAppRefreshTask) {
        print("🔍 Syncing Spotlight index...")
        
        task.expirationHandler = {
            print("⏰ Spotlight sync expired")
        }
        
        Task {
            let db = NornicDBManager.shared
            let indexer = SpotlightIndexer.shared
            
            // Get recently updated memories
            let recentlyUpdated = await db.getMemoriesUpdatedSince(
                indexer.lastSyncDate
            )
            
            for memory in recentlyUpdated.prefix(500) {
                indexer.indexMemory(memory)
            }
            
            indexer.lastSyncDate = Date()
            
            task.setTaskCompleted(success: true)
            self.scheduleSpotlightSync()
        }
    }
}
```

## Deferred Work Queue

For non-urgent operations that should run when conditions are favorable.

```swift
class DeferredWorkQueue {
    static let shared = DeferredWorkQueue()
    
    private var pendingWork: [DeferredWork] = []
    private let queue = DispatchQueue(label: "com.nornicdb.deferred")
    
    struct DeferredWork {
        let id: UUID
        let priority: Priority
        let requiresCharging: Bool
        let work: () async throws -> Void
        let createdAt: Date
        
        enum Priority: Int, Comparable {
            case low = 0
            case medium = 1
            case high = 2
            
            static func < (lhs: Priority, rhs: Priority) -> Bool {
                lhs.rawValue < rhs.rawValue
            }
        }
    }
    
    func enqueue(
        priority: DeferredWork.Priority = .medium,
        requiresCharging: Bool = false,
        work: @escaping () async throws -> Void
    ) {
        let item = DeferredWork(
            id: UUID(),
            priority: priority,
            requiresCharging: requiresCharging,
            work: work,
            createdAt: Date()
        )
        
        queue.async {
            self.pendingWork.append(item)
            self.pendingWork.sort { $0.priority > $1.priority }
        }
        
        // Try to process if conditions are favorable
        checkAndProcess()
    }
    
    func checkAndProcess() {
        let isCharging = ProcessInfo.processInfo.isLowPowerModeEnabled == false
        let thermalOK = !ThermalManager.shared.shouldThrottle
        
        guard thermalOK else { return }
        
        Task {
            // Process items that can run now
            for item in pendingWork {
                if item.requiresCharging && !isCharging {
                    continue
                }
                
                do {
                    try await item.work()
                    queue.async {
                        self.pendingWork.removeAll { $0.id == item.id }
                    }
                } catch {
                    print("❌ Deferred work failed: \(error)")
                }
            }
        }
    }
}
```

## Usage Examples

### Scheduling on App Events

```swift
class AppDelegate: UIResponder, UIApplicationDelegate {
    
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
    ) -> Bool {
        // Register background tasks
        BackgroundTaskManager.shared.registerAllTasks()
        
        // Schedule initial tasks
        BackgroundTaskManager.shared.scheduleDecayUpdate()
        BackgroundTaskManager.shared.scheduleSpotlightSync()
        
        return true
    }
    
    func applicationDidEnterBackground(_ application: UIApplication) {
        // Schedule processing tasks when going to background
        BackgroundTaskManager.shared.scheduleEmbeddingRefresh()
        BackgroundTaskManager.shared.scheduleKMeansClustering()
    }
}
```

### Testing Background Tasks

```swift
// In debugger, trigger background task manually:
// e -l objc -- (void)[[BGTaskScheduler sharedScheduler] _simulateLaunchForTaskWithIdentifier:@"com.nornicdb.embedding-refresh"]

// Or use Xcode's Debug menu:
// Debug > Simulate Background Fetch
```

## Battery Impact Analysis

| Task | Frequency | Duration | Power | Battery/Day |
|------|-----------|----------|-------|-------------|
| Decay Update | Hourly | 5-15s | Low | ~0.5% |
| Spotlight Sync | 2 hours | 10-20s | Low | ~0.3% |
| Embedding Refresh | Daily | 5-30min | High | ~2% (charging) |
| K-Means | Weekly | 10-60min | High | ~3% (charging) |
| Compaction | Weekly | 5-15min | Medium | ~1% (charging) |

**Total daily battery impact: <1%** (heavy tasks only run while charging)

## Next Steps

1. [Model Requirements →](MODELS.md)
2. [API Reference →](API.md)
3. [Roadmap →](ROADMAP.md)

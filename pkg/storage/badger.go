// Package storage provides storage engine implementations for NornicDB.
//
// BadgerEngine provides persistent disk-based storage using BadgerDB.
// It implements the Engine interface with full ACID transaction support.
package storage

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/observability"
)

const maxMVCCCommitSequence = ^uint64(0)

// Key prefixes for BadgerDB storage organization
// Using single-byte prefixes for efficiency
const (
	prefixNode             = byte(0x01) // nodes:nodeID -> Node
	prefixEdge             = byte(0x02) // edges:edgeID -> Edge
	prefixLabelIndex       = byte(0x03) // label:labelName:nodeID -> []byte{}
	prefixOutgoingIndex    = byte(0x04) // outgoing:nodeID:edgeID -> []byte{}
	prefixIncomingIndex    = byte(0x05) // incoming:nodeID:edgeID -> []byte{}
	prefixEdgeTypeIndex    = byte(0x06) // edgetype:type:edgeID -> []byte{} (for fast type lookups)
	prefixPendingEmbed     = byte(0x07) // pending_embed:nodeID -> []byte{} (nodes needing embedding)
	prefixEmbedding        = byte(0x08) // embedding:nodeID:chunkIndex -> []float32 (separate storage for large embeddings)
	prefixSchema           = byte(0x09) // schema:global -> JSON(SchemaDefinition)
	prefixTemporalIndex    = byte(0x0A) // temporal:namespace:label:keyprops:keyhash:valid_from:nodeID -> []byte{}
	prefixTemporalHead     = byte(0x0B) // temporal_current:namespace:label:keyprops:keyhash -> nodeID
	prefixMVCCNode         = byte(0x0C) // mvcc_node:nodeID:version -> Node version payload
	prefixMVCCEdge         = byte(0x0D) // mvcc_edge:edgeID:version -> Edge version payload
	prefixMVCCNodeHead     = byte(0x0E) // mvcc_node_head:nodeID -> MVCCHead
	prefixMVCCEdgeHead     = byte(0x0F) // mvcc_edge_head:edgeID -> MVCCHead
	prefixMVCCMeta         = byte(0x10) // mvcc_meta:* -> MVCC metadata (sequence, rebuild markers)
	prefixEdgeBetweenIndex = byte(0x11) // edgebetween_set:start:end:type:edgeID -> []byte{} (all exact relationship lookups)
	prefixEdgeBetweenHead  = byte(0x12) // edgebetween_head:start:end:type -> edgeID (fast single relationship lookup)
	prefixMVCCOutgoingAdj  = byte(0x23) // mvcc_outgoing:nodeNum:edgeNum:version -> adjacency membership tombstone/live
	prefixMVCCIncomingAdj  = byte(0x24) // mvcc_incoming:nodeNum:edgeNum:version -> adjacency membership tombstone/live
	prefixMVCCPruneFloor   = byte(0x25) // pruned historical floor:kind:numID -> MVCCVersion
	prefixBufferedFlush    = byte(0x13) // buffered_flush:txID -> []byte{} (write-behind commit reached Badger)
)

// prefixMVCCMeta subkeys reserved by storage metadata records.
const (
	prefixMVCCMetaSchemaVersion         = byte(0x02)
	prefixMVCCMetaEdgeBetweenIndexReady = byte(0x03)
	// prefixMVCCMetaNamespaceSeq stores the last committed MVCC sequence
	// for one namespace. Encoded as
	//   [prefixMVCCMeta, prefixMVCCMetaNamespaceSeq, namespace bytes…]
	// (the namespace is the trailing variable-length field; its bytes are
	// already a valid Badger key segment because namespaces are validated
	// to contain neither ':' nor binary control bytes).
	//
	// On engine open, the legacy global key [prefixMVCCMeta, 0x01] (no
	// namespace component) is read once as a backward-compatibility seed:
	// per-namespace counters bootstrap to max(persisted-per-ns, legacy)
	// before the first allocation, so versions in any namespace remain
	// strictly greater than anything previously committed under the
	// global counter. The legacy key is left in place for older binaries
	// that may still read it; new commits no longer write it.
	prefixMVCCMetaNamespaceSeq = byte(0x04)

	// prefixMVCCMetaLabelIndexReady marks completion of the one-time
	// label-index startup backfill (see badger_label_index_backfill.go).
	// Older stores carried over from binaries that wrote node bodies
	// without the corresponding label-index keys — or from mid-write
	// file-level copies — return zero rows from MATCH (n:Label) until
	// this marker is present and the rebuild has run. Same shape as
	// prefixMVCCMetaEdgeBetweenIndexReady.
	prefixMVCCMetaLabelIndexReady = byte(0x05)
	// prefixMVCCMetaLabelCountReady marks completion of the label-count
	// verification/rebuild pass at engine open.
	prefixMVCCMetaLabelCountReady = byte(0x06)
	// prefixMVCCMetaLabelCount stores one namespace-scoped label count as:
	//   [prefixMVCCMeta, prefixMVCCMetaLabelCount, namespace bytes..., 0x00, label] -> uint64 count
	prefixMVCCMetaLabelCount = byte(0x07)
	// prefixMVCCMetaCleanShutdown is present only after the owning DB has
	// stopped writers, flushed async state, and completed a graceful shutdown.
	// Startup consumes it before accepting work, so a crash always leaves the
	// next process on the conservative derived-index rebuild path.
	prefixMVCCMetaCleanShutdown = byte(0x08)
	// prefixMVCCMetaEdgeTypeCountReady marks completion of the edge-type-count
	// verification/rebuild pass at engine open. Same shape as the label-count
	// ready marker.
	prefixMVCCMetaEdgeTypeCountReady = byte(0x09)
	// prefixMVCCMetaEdgeTypeCount stores one namespace-scoped relationship-type
	// count as:
	//   [prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount, namespace bytes..., 0x00, type] -> uint64 count
	prefixMVCCMetaEdgeTypeCount = byte(0x0A)
	// prefixMVCCMetaLabelCountDelta stores committed per-label count deltas as:
	//   [prefixMVCCMeta, prefixMVCCMetaLabelCountDelta, namespace..., 0x00, label, 0x00, unique8] -> int64
	// These are the append-only change log of prefixMVCCMetaLabelCount (see
	// badger_count_deltas.go), not positional pair counts: sub-IDs 0x0F/0x10
	// are the first free MVCC-meta sub-keys.
	prefixMVCCMetaLabelCountDelta = byte(0x0F)
	// prefixMVCCMetaEdgeTypeCountDelta stores committed per-type count deltas:
	//   [prefixMVCCMeta, prefixMVCCMetaEdgeTypeCountDelta, namespace..., 0x00, type, 0x00, unique8] -> int64
	prefixMVCCMetaEdgeTypeCountDelta = byte(0x10)
	// prefixMVCCMetaEdgeTypeStartLabelCount / ...EndLabelCount store the
	// positional namespace-scoped (label, relationship-type) counts Neo4j
	// answers one-labeled-endpoint shapes from its counts store, e.g.
	// (s:Label)-[:T]->() and ()-[:T]->(e:Label):
	//   [prefixMVCCMeta, sub, namespace bytes..., 0x00, label, 0x00, type] -> uint64 count
	// The start tier counts edges whose physical START endpoint carries the
	// label; the end tier counts edges whose physical END endpoint does.
	prefixMVCCMetaEdgeTypeStartLabelCount = byte(0x0B)
	prefixMVCCMetaEdgeTypeEndLabelCount   = byte(0x0C)
	// prefixMVCCMetaLargeCommitIntent marks a commit larger than one Badger
	// batch while its batches are written (see largeCommitIntentKey).
	prefixMVCCMetaLargeCommitIntent = byte(0x0D)
	// prefixMVCCMetaDeleteCleanupMarker is a throw-away key the deleted-entry
	// clean-up writes and then drops, to have Badger compact level 0 (see
	// deleteCleanupMarkerKey). It never outlives one clean-up.
	prefixMVCCMetaDeleteCleanupMarker = byte(0x0E)
	// prefixMVCCMetaTemporalIndexFormat records the temporal history index's
	// key format; see temporalIndexFormatKey.
	prefixMVCCMetaTemporalIndexFormat = byte(0x12)
)

// maxNodeSize is the maximum size for a node to be stored inline (50KB to leave room for BadgerDB overhead)
// Nodes exceeding this will have embeddings stored separately
const maxNodeSize = 50 * 1024 // 50KB

const (
	defaultBadgerNodeCacheMaxEntries   = 10000
	defaultBadgerNodeBodyCacheMaxBytes = int64(256 << 20)
	defaultBadgerEdgeTypeCacheMaxTypes = 50
	defaultBadgerLabelFirstCacheMax    = 1000
)

// BadgerEngine provides persistent storage using BadgerDB.
//
// Features:
//   - ACID transactions for all operations
//   - Persistent storage to disk
//   - Secondary indexes for efficient queries
//   - Thread-safe concurrent access
//   - Automatic crash recovery
//
// Key Structure:
//   - Nodes: 0x01 + nodeID -> JSON(Node)
//   - Edges: 0x02 + edgeID -> JSON(Edge)
//   - Label Index: 0x03 + label + 0x00 + nodeID -> empty
//   - Outgoing Index: 0x04 + nodeID + 0x00 + edgeID -> empty
//   - Incoming Index: 0x05 + nodeID + 0x00 + edgeID -> empty
//   - Edge-Between Set: 0x18 + startID + 0x00 + endID + 0x00 + type + 0x00 + edgeID -> empty
//   - Edge-Between Head: 0x19 + startID + 0x00 + endID + 0x00 + type -> edgeID
//
// Example:
//
//	engine, err := storage.NewBadgerEngine("/path/to/data")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer engine.Close()
//
//	node := &storage.Node{
//		ID:     "user-123",
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	engine.CreateNode(node)
type BadgerEngine struct {
	// dropPrefixTailHook is nil in production and held under the lifecycle barrier.
	dropPrefixTailHook    func(string) error
	storedEntityObservers atomic.Pointer[storedEntityObserverRegistry]

	db      *managedBadgerDB
	mu      sync.RWMutex // Protects lifecycle state (e.g., Close) and any coarse-grained engine invariants
	closing bool         // write admission is closed before the final buffered drain
	// prefixDrops records prefix deletes for the transactions open across
	// them (prefixDroppedSince).
	prefixDrops prefixDrops
	// pendingCandidates accelerates the durable pending-embedding index. Adds
	// are published by the common commit writer only after a successful commit.
	pendingCandidates           pendingEmbeddingCandidates
	pendingEmbeddingAdds        sync.Map // *badger.Txn -> map[NodeID]struct{}
	pendingCandidatesGeneration atomic.Uint64
	// changes is the change data capture state (change_capture.go, #937).
	changes                            changeCapture
	notificationMu                     sync.Mutex
	notificationHead, notificationTail *commitNotification
	notificationDraining               bool
	closeMu                            sync.Mutex
	// work is the engine's one admission, completion and outcome owner.
	// Helper transactions and reserved commit notifications are its accepted
	// work; Close seals it after the last durable write published, and its
	// Shutdown releases engine state only after every ticket returned.
	work lifecycle.WorkLifetime
	// writeBarrier orders Close against durable writes. Every public
	// non-transactional mutator holds it for read for its whole duration,
	// and an explicit transaction holds it for read from just before its
	// Badger commit through the post-commit tail (derived counts, MVCC
	// sequence, ID counters, caches). Listeners hold their notification's work ticket. Close takes it for write
	// before releasing engine state, so a write whose Badger commit has
	// already returned always finishes publishing and reports success, and a
	// write that arrives after Close fails cleanly with ErrStorageClosed.
	// Lock order: writeBarrier before mu; Close stops its background
	// backfills before taking the barrier so they cannot wait on it.
	writeBarrier sync.RWMutex
	// deleteCleanup counts deletes and runs the deleted-entry clean-up; nil
	// for in-memory engines (see badger_delete_cleanup.go).
	deleteCleanup    *deleteCleanup
	inMemory         bool   // True if running in memory-only mode (testing)
	dataDir          string // Captured from BadgerOptions.DataDir; used by migration logging.
	catalogUpgradeV4 *CatalogUpgradeV4

	// writeBehind buffers implicit autocommit commits for background replay
	// (see badger_write_behind.go). Nil when disabled.
	writeBehind *WriteBehindBuffer
	// memoryPolicy is the database-owned soft admission budget for retained
	// write-behind generations. Nil preserves the legacy unbounded path.
	memoryPolicy *memory.Policy

	// bufferedCommitWAL, when set, receives the full payload of every
	// acknowledged write-behind commit before the ACK (see
	// commitBufferedLocked). The WALEngine installs it at construction.
	// Writes are guarded by the WAL's own mutex; the pointer is set once
	// before the engine serves traffic.
	bufferedCommitWAL *WAL

	// migrationDidRun is set by RunOnStartMigrations when at least one
	// migration arm rewrote bodies. The engine open path uses it to
	// decide whether to pay the close+reopen+Flatten cost: most opens
	// don't migrate, and only an actual migration leaves enough stale
	// keys behind to make synchronous compaction worthwhile.
	migrationDidRun bool

	// Per-namespace schema (Neo4j-compatible: each database has its own schema).
	schemasMu sync.RWMutex
	schemas   map[string]*SchemaManager // namespace -> schema

	// temporalIndexReady is set once RebuildTemporalIndexes has rebuilt the
	// temporal history index from every stored node (and cleared while a
	// rebuild runs): every temporal constraint known by then is indexed
	// completely, and every write since maintains it. See
	// temporal_index_completeness.go.
	temporalIndexReady atomic.Bool

	// Hot node cache for frequently accessed nodes
	// Dramatically speeds up repeated MATCH lookups
	nodeCache   map[NodeID]*Node
	nodeCacheMu sync.RWMutex
	// queryNodeCache holds nodes as GetQueryNode reads them (no stored
	// vectors) that nodeCache doesn't hold in full. It is guarded by
	// nodeCacheMu and invalidated wherever nodeCache is.
	queryNodeCache map[NodeID]*Node
	// The generations order each read cache's fills against its writes
	// (see cacheGeneration).
	nodeCacheGen       cacheGeneration
	edgeCacheGen       cacheGeneration
	edgeTypeCacheGen   cacheGeneration
	adjCacheGen        cacheGeneration
	labelFirstCacheGen cacheGeneration
	cacheHits          int64
	cacheMisses        int64

	// Current primary-key body cache for MVCC GetNodeVisibleAt reads. Entries
	// are tagged with Badger's item version so a snapshot read can only reuse a
	// decoded body when it has first observed the exact primary-key version.
	nodeBodyCache         map[NodeID]*nodeBodyCacheEntry
	nodeBodyCacheMu       sync.RWMutex
	nodeBodyCacheLRU      list.List
	nodeBodyCacheBytes    int64
	nodeBodyCacheMaxBytes int64

	// Edge type cache for mutual relationship queries
	// Caches edges by type for O(1) lookup
	relationshipUniqueIndexes relationshipUniqueIndexSet
	edgeTypeCache             map[string][]*Edge // edgeType -> edges of that type
	edgeTypeCacheMu           sync.RWMutex

	// Hot edge-body cache. BFS-style traversals (shortestPath, variable-
	// length MATCH) re-read the same edges repeatedly across requests; the
	// CPU profile showed badger.Txn.Get + decodeEdgeBodyByID was the
	// dominant per-request cost on warm runs. Populated lazily inside
	// collectEdgesByIndexPrefix and invalidated on every edge mutation.
	edgeCache         map[EdgeID]*Edge
	edgeCacheVersions map[EdgeID]uint64 // physical record versions, guarded by edgeCacheMu
	edgeCacheMu       sync.RWMutex
	edgeCacheMaxItems int

	// Per-node adjacency-ID caches. Stores the result of an
	// outgoing/incoming index iteration so subsequent BFS frontier
	// expansions skip the Badger iterator entirely. Profiling showed
	// Iterator.Rewind+Next+parseItem was ~55% of warm shortestPath cost
	// after the edge-body cache landed. Invalidated when any edge that
	// touches one of the cached nodes is created/updated/deleted.
	outgoingAdjCache map[NodeID][]EdgeID
	incomingAdjCache map[NodeID][]EdgeID
	adjCacheMu       sync.RWMutex
	adjCacheMaxNodes int

	// Fast label lookup cache: label -> first node ID
	// Used by ID-only label scans to avoid repeated index iteration.
	labelFirstNodeCache   map[string]NodeID
	labelFirstNodeCacheMu sync.RWMutex

	// Cache sizing (tunable via config/options).
	// Used by the cache invalidation helpers to preserve invariants.
	nodeCacheMaxEntries   int
	edgeTypeCacheMaxTypes int
	labelFirstCacheMax    int

	// Cached counts for O(1) stats lookups (updated on create/delete)
	// Eliminates expensive full table scans for node/edge counts
	// totalCountWriteMu publishes cached totals and retires their buffered delta
	// together. It is released before any storage callbacks.
	totalCountWriteMu sync.RWMutex
	nodeCount         atomic.Int64
	edgeCount         atomic.Int64

	// mvccByNamespace holds the per-database MVCC sequence counter and
	// high-water clamp. Each transaction is pinned to one namespace at
	// BeginTransaction (see BadgerTransaction.namespace), so the engine
	// keeps an independent monotonic sequence per database — a
	// high-throughput tenant cannot accelerate another tenant's version
	// numbers, and DROP DATABASE drops its counter alongside its keys.
	//
	// Each namespaceMVCCState.seq is a strictly-increasing uint64
	// incremented on every commit in that namespace. .highWaterNanos
	// caches the most recent commit-timestamp stamped onto any version
	// in the namespace; currentMVCCReadVersion clamps a transaction's
	// readTS to max(now, highWater) so a non-monotonic wall clock
	// (containerized CI runners under NTP correction, Linux
	// clock_gettime drift across goroutine schedulings) cannot let a
	// freshly-committed head's timestamp exceed a subsequent
	// BeginTransaction's sample — which previously surfaced as spurious
	// "node X changed after transaction start" conflicts on
	// single-goroutine code paths.
	//
	// On engine open, the legacy engine-global mvccSequenceKey is read
	// once as a backward-compatibility seed; new namespaces bootstrap
	// to max(persisted-per-ns, legacy-global) so no namespace can
	// regress past versions previously emitted under the global
	// counter. See loadPersistedMVCCSequence and namespaceMVCC().
	mvccByNamespaceMu    sync.RWMutex
	mvccByNamespace      map[string]*namespaceMVCCState
	mvccLegacyGlobalSeed uint64

	retentionPolicy           RetentionPolicy
	activeMVCCSnapshotReaders atomic.Int64
	mvccPruneMu               sync.Mutex
	mvccFloorKeysMu           sync.RWMutex
	mvccFloorKeys             map[[9]byte]struct{}
	lifecycleController       MVCCLifecycleController

	// Cached per-namespace counts for O(1) multi-database stats.
	// Keys are namespace prefixes like "nornic:".
	namespaceCountsMu   sync.RWMutex
	namespaceNodeCounts map[string]int64
	namespaceEdgeCounts map[string]int64

	edgeBetweenIndexBackfillMu     sync.Mutex
	edgeBetweenIndexBackfillCancel context.CancelFunc
	edgeBetweenIndexBackfillDone   chan struct{}

	// labelIndexBackfill* mirrors the edge-between backfill state for
	// the (label, nodeNumID) index rebuild. See
	// badger_label_index_backfill.go for the rebuild contract.
	labelIndexBackfillMu     sync.Mutex
	labelIndexBackfillCancel context.CancelFunc
	labelIndexBackfillDone   chan struct{}

	// idDict assigns compact 8-byte numeric IDs to string node/edge IDs
	// so secondary indexes (edge-between, outgoing/incoming, edge-type,
	// label, MVCC heads) can encode references with 8 bytes instead of
	// 40–50-byte UUID strings. See id_dictionary.go for the full layout.
	idDict *idDictionary

	// propKeyDict tokenizes property-key NAMES (per-namespace) to varint
	// IDs so node and edge bodies can store keys as 1-2 byte integers
	// instead of repeating the string in every record. See
	// property_key_dictionary.go for the full layout.
	propKeyDict *propertyKeyDictionary

	walPropertySource walPropertySource

	// storageVersion is the on-disk schema version after migrations have
	// run during engine open. Written once during NewBadgerEngineWithOptions
	// and read-only thereafter. The encode path picks codecs deterministically
	// from this value; the decode path dispatches on per-record format bytes
	// because old bodies coexist with new ones until rewritten.
	storageVersion int

	// Event callbacks for external coordination (search indexes, caches, etc.)
	// These are fired AFTER storage operations succeed, every registered one
	// in registration order
	onNodeCreated         []NodeEventCallback
	onNodeUpdated         []NodeEventCallback
	onNodeDeleted         []NodeDeleteCallback
	onEdgeCreated         []EdgeEventCallback
	onEdgeUpdated         []EdgeEventCallback
	onEdgeDeleted         []EdgeDeleteCallback
	onNamespaceReset      []NamespaceResetCallback
	callbackMu            sync.RWMutex
	graphMutationVersions graphMutationVersions

	// Knowledge-layer decay scoring (Phase 4).
	decayEnabled  bool
	accumulator   accessIncrementor
	revealAll     atomic.Bool
	revealQueryMu sync.RWMutex

	// The entities suppressed on demand (badger_manual_suppression.go), as
	// kind+ID, and their count, which reads check before taking the lock.
	manualSuppressionMu    sync.RWMutex
	manualSuppressed       map[string]struct{}
	manualSuppressionCount atomic.Int64

	// suppressionEnds are the stored suppressions of deleted entities still
	// to delete (endSuppressions, suppressionEnding while it runs), under
	// manualSuppressionMu. suppressionWriteMu orders every write of a stored
	// suppression.
	suppressionEnds    map[string]struct{}
	suppressionEnding  bool
	suppressionWriteMu sync.Mutex

	// labelCountWriteMu orders folds/rebuilds of the derived label counts
	// against each other and keeps backup's count snapshot exclusive. Commit
	// paths do not take it: every commit appends a unique delta key instead
	// of a read-modify-write on the shared base key (PR #1043 item #2).
	labelCountWriteMu sync.RWMutex

	// edgeTypeCountWriteMu is the edge-type analogue of labelCountWriteMu:
	// it orders folds/rebuilds of the derived per-type counts (issue #638).
	// Commit paths append unique delta keys instead of taking it.
	edgeTypeCountWriteMu sync.RWMutex

	// countDeltaSeq numbers derived-count delta keys uniquely within this
	// engine lifetime. Folding at open removes every persisted delta before
	// new writes, so suffix reuse across restarts cannot collide.
	countDeltaSeq atomic.Uint64

	// countDeltaPending counts appended delta keys since the last background
	// fold trigger; countFold folds them off the read path (PR #1043 issue B).
	countDeltaPending atomic.Int64
	countFold         *countDeltaFoldWorker

	// embeddingsEnabled gates the pending-embed index write on node creates.
	// When false, new nodes skip the pendingEmbed marker since no embed worker
	// will consume it — saves one Badger Set per user node on hot-path writes.
	embeddingsEnabled      atomic.Bool
	embeddingLabelPolicies atomic.Pointer[embeddingLabelPolicies]
	// embeddingTextPolicy says which properties and labels feed managed
	// embedding text (SetEmbeddingTextPolicy, #963).
	embeddingTextPolicy atomic.Pointer[EmbeddingTextPolicy]

	// log is the structured *slog.Logger for storage subsystem emissions.
	// Tagged at construction with component=storage, engine=badger.
	// D-01 logger DI; D-07 storage internals use e.log directly. Discard
	// fallback installed in NewBadgerEngineWithOptions when opts.Logger == nil.
	log *slog.Logger

	// Plan 04-04-06: pre-bound storage observers cached at AttachMetrics
	// time (MET-25). Hot-path observation funnels through these — zero
	// WithLabelValues lookup per Get/Put/Delete/Scan call. Nil-safe: when
	// opObserve is the zero value (no metrics attached) the deferred
	// observation in observeStorageOp is a no-op.
	storageMetrics  *observability.StorageMetrics
	mvccMetrics     *observability.MVCCMetrics
	opDurGet        observability.BoundLatencyObserver
	opDurPut        observability.BoundLatencyObserver
	opDurDelete     observability.BoundLatencyObserver
	opDurScan       observability.BoundLatencyObserver
	metricsAttached atomic.Bool
}

// IsInMemory returns true if the engine is running in memory-only mode.
// In-memory mode is used for testing - there's no disk to fsync to.
func (b *BadgerEngine) IsInMemory() bool {
	return b.inMemory
}

// DB returns the underlying *badger.DB handle. Used by Plan 04-04-04
// bytes_metrics_sweeper to call EstimateSize(prefix). Read-only handle —
// callers must not invoke lifecycle methods (Close/DropAll) on the
// returned pointer; the engine owns lifecycle. Badger runs in managed mode
// (see managedBadgerDB): transactions and writes must go through the
// engine, never through this handle.
func (b *BadgerEngine) DB() *badger.DB {
	if b.db == nil {
		return nil
	}
	return b.db.DB
}

// OnNodeCreated adds a callback to be invoked when nodes are created.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeCreated(callback NodeEventCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeCreated = append(b.onNodeCreated, callback)
}

// OnNodeUpdated adds a callback to be invoked when nodes are updated.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeUpdated(callback NodeEventCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeUpdated = append(b.onNodeUpdated, callback)
}

// OnNodeDeleted adds a callback to be invoked when nodes are deleted.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeDeleted = append(b.onNodeDeleted, callback)
}

// OnEdgeCreated adds a callback to be invoked when edges are created.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeCreated(callback EdgeEventCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeCreated = append(b.onEdgeCreated, callback)
}

// OnEdgeUpdated adds a callback to be invoked when edges are updated.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeUpdated = append(b.onEdgeUpdated, callback)
}

// OnEdgeDeleted adds a callback to be invoked when edges are deleted.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeDeleted = append(b.onEdgeDeleted, callback)
}

// NamespaceResetCallback reports that every node and relationship whose ID
// starts with prefix was removed at once (DeleteByPrefix), without a per-entity
// event.
type NamespaceResetCallback func(prefix string)

// NamespaceResetNotifier is implemented by engines that report DeleteByPrefix.
// Consumers that derive state from the stored entities (search indexes) drop
// that state for the namespace.
type NamespaceResetNotifier interface {
	OnNamespaceReset(callback NamespaceResetCallback)
}

// OnNamespaceReset registers a callback for DeleteByPrefix. It is called in
// commit order with the other storage event callbacks.
func (b *BadgerEngine) OnNamespaceReset(callback NamespaceResetCallback) {
	if callback == nil {
		return
	}
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNamespaceReset = append(b.onNamespaceReset, callback)
}

func (b *BadgerEngine) dispatchNamespaceReset(prefix string) {
	b.callbackMu.RLock()
	callbacks := b.onNamespaceReset
	b.callbackMu.RUnlock()
	for _, callback := range callbacks {
		callback(prefix)
	}
}

// dispatchNodeCreated publishes callbacks for an already visible graph mutation.
func (b *BadgerEngine) dispatchNodeCreated(node *Node) {
	b.callbackMu.RLock()
	callbacks := b.onNodeCreated
	b.callbackMu.RUnlock()

	for _, callback := range callbacks {
		callback(node)
	}
}

// dispatchNodeUpdated publishes callbacks for an already visible graph mutation.
func (b *BadgerEngine) dispatchNodeUpdated(node *Node) {
	b.dispatchNodeUpdatedOutcome(node, false)
}

func (b *BadgerEngine) dispatchNodeUpdatedOutcome(node *Node, outcome bool) {
	b.publishNodeOutcome(node, outcome, func() {
		b.callbackMu.RLock()
		callbacks := b.onNodeUpdated
		b.callbackMu.RUnlock()
		for _, callback := range callbacks {
			callback(node)
		}
	})
}

func (b *BadgerEngine) dispatchNodeDeleted(nodeID NodeID) {
	b.callbackMu.RLock()
	callbacks := b.onNodeDeleted
	b.callbackMu.RUnlock()

	for _, callback := range callbacks {
		callback(nodeID)
	}
}

// dispatchEdgeCreated publishes callbacks for an already visible graph mutation.
func (b *BadgerEngine) dispatchEdgeCreated(edge *Edge) {
	b.callbackMu.RLock()
	callbacks := b.onEdgeCreated
	b.callbackMu.RUnlock()

	for _, callback := range callbacks {
		callback(edge)
	}
}

// dispatchEdgeUpdated publishes callbacks for an already visible graph mutation.
func (b *BadgerEngine) dispatchEdgeUpdated(edge *Edge) {
	b.callbackMu.RLock()
	callbacks := b.onEdgeUpdated
	b.callbackMu.RUnlock()

	for _, callback := range callbacks {
		callback(edge)
	}
}

// dispatchEdgeDeleted publishes callbacks for an already visible graph mutation.
func (b *BadgerEngine) dispatchEdgeDeleted(edgeID EdgeID) {
	b.callbackMu.RLock()
	callbacks := b.onEdgeDeleted
	b.callbackMu.RUnlock()

	for _, callback := range callbacks {
		callback(edgeID)
	}
}

// BadgerOptions configures the BadgerDB engine.
type BadgerOptions struct {
	// CatalogUpgradeV4 is the optional exact catalog migration plan. It is
	// captured at open; startup integration decides when V4 is authorized.
	CatalogUpgradeV4 *CatalogUpgradeV4

	// DataDir is the directory for storing data files.
	// Required.
	DataDir string

	// InMemory runs BadgerDB in memory-only mode.
	// Useful for testing. Data is not persisted. Unless HighPerformance or
	// LowMemory is set, in-memory engines use a smaller test-oriented profile
	// so large test suites do not retain one default-sized Badger arena per
	// fixture while still accepting normal-sized schema and node values.
	InMemory bool

	// SyncWrites forces fsync after each write.
	// Slower but more durable.
	SyncWrites bool

	// WriteBehind buffers implicit autocommit transaction commits in the
	// engine's rotating commit buffer and replays them into Badger in
	// background batches. Explicit transactions and snapshot opens drain the
	// buffer first. Off by default.
	WriteBehind bool

	// WriteBehindInterval rotates the active buffer generation this often.
	// Defaults to 50ms when WriteBehind is set and this is <= 0.
	WriteBehindInterval time.Duration

	// WriteBehindMaxOps rotates the active generation once it holds this
	// many buffered operations. 0 selects auto sizing against the engine's
	// default cap (defaultWriteBehindMaxOps); a positive value is a manual
	// cap for operators with unusual hardware.
	WriteBehindMaxOps int
	// MemoryPolicy is the optional shared application budget. Buffered commit
	// payloads reserve before their replayable WAL record is written.
	MemoryPolicy *memory.Policy

	// BadgerInternalLogger is the logger handed to BadgerDB itself for its
	// own internal logging (compaction, value-log GC, etc.). If nil, BadgerDB's
	// quiet/default logger is used. This field replaces the previous
	// BadgerOptions.Logger which collided with the new structured *slog.Logger
	// field below; rename is internal-only (no external setter ever existed
	// outside doc comments — verified 2026-05-01).
	BadgerInternalLogger badger.Logger

	// Logger is the structured *slog.Logger threaded into the storage engine.
	// D-01 logger DI: optional; nil falls back to a discard handler at ctor
	// entry per D-01a so existing callers (and tests) compile unchanged.
	// Once set, BadgerEngine emits all operational diagnostics through this
	// logger tagged with component=storage, engine=badger.
	Logger *slog.Logger

	// LowMemory enables memory-constrained settings.
	// Reduces MemTableSize and other buffers to use less RAM.
	LowMemory bool

	// HighPerformance enables aggressive caching and larger buffers.
	// Uses more RAM but significantly faster writes/reads.
	HighPerformance bool

	// EncryptionKey is the 16, 24, or 32 byte key for AES encryption.
	// If provided, all data will be encrypted at rest using AES-CTR.
	// WARNING: If you lose this key, your data is irrecoverable!
	// Leave empty to disable encryption.
	EncryptionKey []byte

	// AllowStorageUpgrade authorizes the engine to advance the on-disk
	// storage version through whichever migration arms this binary
	// understands. Without it, a binary that opens a data directory
	// older than its own version refuses to start. The upgrade is
	// one-way; operators should back up before passing this flag.
	AllowStorageUpgrade bool

	// NodeCacheMaxEntries is the maximum number of nodes held in the in-process
	// hot node cache (used by GetNode). When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	NodeCacheMaxEntries int

	// NodeBodyCacheMaxBytes bounds decoded MVCC node bodies by retained bytes.
	// Entries larger than the budget are not cached. Set to 0 for the default.
	NodeBodyCacheMaxBytes int64

	// EdgeTypeCacheMaxTypes is the maximum number of distinct edge types cached
	// for GetEdgesByType. When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	EdgeTypeCacheMaxTypes int

	// LabelFirstNodeCacheMaxEntries is the maximum number of labels cached for
	// fast label-first lookups. When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	LabelFirstNodeCacheMaxEntries int

	// EngineOptions holds engine-wide MVCC retention defaults and similar policies.
	EngineOptions EngineOptions
}

// NewBadgerEngine creates a new persistent storage engine with default settings.
//
// This is the simplest way to create a storage engine. The engine uses BadgerDB
// for persistent disk storage with ACID transaction guarantees. All data is
// stored in the specified directory and persists across restarts.
//
// Parameters:
//   - dataDir: Directory path for storing data files. Created if it doesn't exist.
//
// Returns:
//   - *BadgerEngine on success
//   - error if database cannot be opened (e.g., permissions, disk space)
//
// Example 1 - Basic Usage:
//
//	engine, err := storage.NewBadgerEngine("./data/nornicdb")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer engine.Close()
//
//	// Engine is ready - create nodes
//	node := &storage.Node{
//		ID:     "user-1",
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	engine.CreateNode(node)
//
// Example 2 - Production Application:
//
//	// Use absolute path for production
//	dataDir := filepath.Join(os.Getenv("APP_HOME"), "data", "nornicdb")
//	engine, err := storage.NewBadgerEngine(dataDir)
//	if err != nil {
//		return fmt.Errorf("failed to open database: %w", err)
//	}
//	defer engine.Close()
//
// Example 3 - Multiple Databases:
//
//	// Main application database
//	mainDB, _ := storage.NewBadgerEngine("./data/main")
//	defer mainDB.Close()
//
//	// Test database
//	testDB, _ := storage.NewBadgerEngine("./data/test")
//	defer testDB.Close()
//
//	// Cache database
//	cacheDB, _ := storage.NewBadgerEngine("./data/cache")
//	defer cacheDB.Close()
//
// ELI12:
//
// Think of NewBadgerEngine like setting up a filing cabinet in your room.
// You tell it "put the cabinet here" (the dataDir), and it creates folders
// and organizes everything. Even if you turn off your computer, the cabinet
// stays there with all your files inside. Next time you start up, all your
// data is still there!
//
// Disk Usage:
//   - Approximately 2-3x the size of your actual data
//   - Includes write-ahead log and compaction overhead
//
// Thread Safety:
//
//	Safe for concurrent use from multiple goroutines.
func NewBadgerEngine(dataDir string) (*BadgerEngine, error) {
	return NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dataDir,
	})
}

// NewBadgerEngineWithOptions creates a BadgerEngine with custom configuration.
//
// Use this function when you need fine-grained control over the storage engine
// behavior, such as enabling in-memory mode for testing, forcing synchronous
// writes for maximum durability, or reducing memory usage.
//
// Parameters:
//   - opts: BadgerOptions struct with configuration settings
//
// Returns:
//   - *BadgerEngine on success
//   - error if database cannot be opened
//
// Example 1 - In-Memory Database for Testing:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:  "./test", // Still needs a path but won't be used
//		InMemory: true,     // All data in RAM, lost on shutdown
//	})
//	defer engine.Close()
//
//	// Perfect for unit tests - fast and clean
//	testCreateNodes(engine)
//
// Example 2 - Maximum Durability for Financial Data:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:    "./data/transactions",
//		SyncWrites: true, // Force fsync after each write (slower but safer)
//	})
//	// Guaranteed data persistence even if power fails
//
// Example 3 - Low Memory Mode for Embedded Devices:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:   "./data/nornicdb",
//		LowMemory: true, // Reduces RAM usage by 50-70%
//	})
//	// Uses ~50MB instead of ~150MB for typical workloads
//
// Example 4 - Custom Logger Integration:
//
//	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir: "./data/nornicdb",
//		Logger:  &BadgerLogger{zlog: logger}, // Custom logging
//	})
//
// ELI12:
//
// NewBadgerEngine is like getting a basic backpack for school.
// NewBadgerEngineWithOptions is like customizing your backpack - you can:
//   - Make it waterproof (SyncWrites = true)
//   - Make it lighter but less storage (LowMemory = true)
//   - Use it as a temporary bag (InMemory = true)
//   - Add custom labels (Logger)
//
// Configuration Trade-offs:
//   - SyncWrites=true: Slower writes (2-5x) but maximum safety
//   - LowMemory=true: Less RAM but slightly slower
//   - InMemory=true: Fastest but data lost on shutdown
//
// Thread Safety:
//
//	Safe for concurrent use from multiple goroutines.
func NewBadgerEngineWithOptions(opts BadgerOptions) (*BadgerEngine, error) {
	inMemoryDefaultProfile := opts.InMemory && !opts.HighPerformance && !opts.LowMemory
	retentionPolicy := normalizeRetentionPolicy(opts.EngineOptions.RetentionPolicy)
	// Precommit validation cannot see a peer that publishes afterward. Native
	// conflict tracking closes that window for the writer's enrolled keys in
	// every storage mode, including the default high-performance server mode.
	badgerOpts := badger.DefaultOptions(opts.DataDir).WithDetectConflicts(true)

	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	if opts.SyncWrites {
		badgerOpts = badgerOpts.WithSyncWrites(true)
	}

	if opts.BadgerInternalLogger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.BadgerInternalLogger)
	} else {
		// Use a quiet logger by default
		badgerOpts = badgerOpts.WithLogger(nil)
	}

	// D-01a discard fallback: structured *slog.Logger is optional. When the
	// caller does not supply one (e.g., test paths, scripts/perf_direct), a
	// discard logger keeps the LOG-01 invariant that no storage code path
	// reaches stdlib log printers even when no observability stack exists.
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	storageLog := opts.Logger.With("component", "storage", "engine", "badger")

	// Enable encryption at rest if key is provided
	if len(opts.EncryptionKey) > 0 {
		// Validate key length (must be 16, 24, or 32 bytes for AES-128/192/256)
		keyLen := len(opts.EncryptionKey)
		if keyLen != 16 && keyLen != 24 && keyLen != 32 {
			return nil, fmt.Errorf("encryption key must be 16, 24, or 32 bytes (got %d bytes)", keyLen)
		}
		badgerOpts = badgerOpts.WithEncryptionKey(opts.EncryptionKey)
	}

	if opts.HighPerformance {
		// HIGH PERFORMANCE MODE: Maximize speed, use more RAM
		// Target: Get close to in-memory performance for small-medium datasets
		badgerOpts = badgerOpts.
			WithMemTableSize(128 << 20).     // 128MB memtable (8x default) - fewer flushes
			WithValueLogFileSize(256 << 20). // 256MB value log files
			WithNumMemtables(5).             // 5 memtables for write buffering
			WithNumLevelZeroTables(10).      // More L0 tables before compaction
			WithNumLevelZeroTablesStall(20). // Higher stall threshold
			WithValueThreshold(1 << 20).     // 1MB - keep most values in LSM tree
			WithBlockCacheSize(256 << 20).   // 256MB block cache
			WithIndexCacheSize(128 << 20).   // 128MB index cache
			WithNumCompactors(4).            // More parallel compaction
			WithCompactL0OnClose(false)      // Don't compact on close (faster shutdown)
	} else if opts.LowMemory {
		// LOW MEMORY MODE: Minimize RAM usage
		badgerOpts = badgerOpts.
			WithMemTableSize(8 << 20).      // 8MB memtable
			WithValueLogFileSize(32 << 20). // 32MB value log
			WithNumMemtables(1).            // Single memtable
			WithNumLevelZeroTables(1).      // Aggressive compaction
			WithNumLevelZeroTablesStall(2).
			WithValueThreshold(512).     // Small values in LSM
			WithBlockCacheSize(8 << 20). // 8MB block cache
			WithIndexCacheSize(4 << 20)  // 4MB index cache
	} else if inMemoryDefaultProfile {
		// In-memory Badger is primarily used by tests. The ordinary default
		// profile allocates a 64MB memtable arena for every fixture, which makes
		// large package suites sensitive to cgroup memory and swap settings. Keep
		// the default 64KB value threshold so schema definitions and moderately
		// sized properties remain valid in InMemory mode, but shrink the arenas
		// and caches that dominate per-fixture retained memory.
		badgerOpts = badgerOpts.
			WithMemTableSize(32 << 20).
			WithValueLogFileSize(128 << 20).
			WithNumMemtables(2).
			WithNumLevelZeroTables(5).
			WithNumLevelZeroTablesStall(10).
			WithValueThreshold(64 << 10).
			WithBlockCacheSize(8 << 20).
			WithIndexCacheSize(4 << 20)
	} else {
		// DEFAULT: Balanced settings
		badgerOpts = badgerOpts.
			WithMemTableSize(64 << 20).      // 64MB memtable (default)
			WithValueLogFileSize(128 << 20). // 128MB value log
			WithNumMemtables(3).             // 3 memtables
			WithNumLevelZeroTables(5).       // Default L0 tables
			WithNumLevelZeroTablesStall(10).
			WithValueThreshold(64 << 10). // 64KB threshold - allows larger property values
			WithBlockCacheSize(64 << 20). // 64MB block cache
			WithIndexCacheSize(32 << 20)  // 32MB index cache
	}

	db, err := openManagedBadger(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// Warn (but don't fail) if the data directory still holds legacy gob
	// bodies. The decoder handles them, but operators should run
	// MigrateBadgerToMsgpack to upgrade in place.
	if detected, hasData, derr := detectStoredSerializer(db); derr != nil {
		db.Close()
		return nil, fmt.Errorf("detecting storage serializer: %w", derr)
	} else if hasData && detected == detectedGob {
		storageLog.Warn("legacy gob-encoded bodies detected; run MigrateBadgerToMsgpack to upgrade in place",
			"data_dir", opts.DataDir,
		)
	}

	engine := &BadgerEngine{
		db:              db,
		inMemory:        opts.InMemory,
		dataDir:         opts.DataDir,
		memoryPolicy:    opts.MemoryPolicy,
		schemas:         make(map[string]*SchemaManager),
		retentionPolicy: retentionPolicy,

		nodeCacheMaxEntries:   opts.NodeCacheMaxEntries,
		nodeBodyCacheMaxBytes: opts.NodeBodyCacheMaxBytes,
		edgeTypeCacheMaxTypes: opts.EdgeTypeCacheMaxTypes,
		labelFirstCacheMax:    opts.LabelFirstNodeCacheMaxEntries,

		log: storageLog,
	}

	if opts.CatalogUpgradeV4 != nil {
		plan := *opts.CatalogUpgradeV4
		engine.catalogUpgradeV4 = &plan
	}

	if engine.nodeCacheMaxEntries <= 0 {
		engine.nodeCacheMaxEntries = defaultBadgerNodeCacheMaxEntries
	}
	if engine.nodeBodyCacheMaxBytes <= 0 {
		engine.nodeBodyCacheMaxBytes = defaultBadgerNodeBodyCacheMaxBytes
	}
	if engine.edgeTypeCacheMaxTypes <= 0 {
		engine.edgeTypeCacheMaxTypes = defaultBadgerEdgeTypeCacheMaxTypes
	}
	if engine.labelFirstCacheMax <= 0 {
		engine.labelFirstCacheMax = defaultBadgerLabelFirstCacheMax
	}

	engine.nodeCache = make(map[NodeID]*Node, engine.nodeCacheMaxEntries)
	engine.nodeBodyCache = make(map[NodeID]*nodeBodyCacheEntry, engine.nodeCacheMaxEntries)
	engine.edgeTypeCache = make(map[string][]*Edge, engine.edgeTypeCacheMaxTypes)
	engine.labelFirstNodeCache = make(map[string]NodeID, engine.labelFirstCacheMax)
	// Mirror the node cache sizing knob unless we add a dedicated tunable.
	engine.edgeCacheMaxItems = engine.nodeCacheMaxEntries
	engine.edgeCache = make(map[EdgeID]*Edge, engine.edgeCacheMaxItems)
	engine.adjCacheMaxNodes = engine.nodeCacheMaxEntries
	engine.outgoingAdjCache = make(map[NodeID][]EdgeID, engine.adjCacheMaxNodes)
	engine.incomingAdjCache = make(map[NodeID][]EdgeID, engine.adjCacheMaxNodes)
	engine.namespaceNodeCounts = make(map[string]int64)
	engine.namespaceEdgeCounts = make(map[string]int64)

	engine.idDict = newIDDictionary()
	engine.idDict.setFreelistTTL(opts.EngineOptions.IDFreelistTTL)
	if err := engine.idDict.loadFromBadger(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load id dictionary: %w", err)
	}

	engine.propKeyDict = newPropertyKeyDictionary()
	if err := engine.propKeyDict.loadFromBadger(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load property key dictionary: %w", err)
	}
	if err := engine.initializeWALPropertyIdentity(engine.db, false, false); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize property dictionary identity: %w", err)
	}
	if err := engine.loadManualSuppressions(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load manual suppressions: %w", err)
	}

	if err := engine.initializeMVCCSequence(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize mvcc sequence: %w", err)
	}

	if err := engine.recoverNamespaceDrops(nil); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover namespace DROP: %w", err)
	}
	// Initialize cached counts by scanning existing data (one-time cost)
	// This enables O(1) stats lookups instead of O(N) scans on every request
	if err := engine.initializeCounts(); err != nil {
		db.Close() // Clean up on error
		return nil, fmt.Errorf("failed to initialize counts: %w", err)
	}

	// Run migrations BEFORE loadPersistedSchemas. The schema loader's
	// rebuildUniqueConstraintValues phase decodes every node body to
	// populate per-namespace unique-value caches; it can only succeed
	// once bodies are in the current (v2) format. Pre-migration this
	// would fail immediately on the first v0/v1 body with "unexpected
	// format byte 0xff; expected V2 (0x10)" and the engine would
	// refuse to open even with --upgrade-storage.
	if err := engine.runOnStartMigrations(opts.AllowStorageUpgrade, migrationStartupOpening); err != nil {
		db.Close()
		return nil, err
	}

	// If a migration arm rewrote bodies, the v2 versions of every
	// node and edge are still in memtables. Closing the DB drains
	// memtables to SSTs; reopening gives Flatten a tree where the v1
	// originals and v2 rewrites are both on disk so it can collapse
	// them. Without this, Flatten only sees the untouched v1 SSTs and
	// reclaims a few percent of disk instead of unwinding the upgrade
	// bloat. reopenForPostMigrationCompaction also re-runs the dict
	// loaders against the fresh DB handle.
	if engine.migrationDidRun && !opts.InMemory {
		newDB, err := engine.reopenForPostMigrationCompaction(badgerOpts)
		if err != nil {
			return nil, fmt.Errorf("post-migration reopen: %w", err)
		}
		engine.db = newDB
		db = newDB
		engine.compactAfterMigration()
	}

	if err := engine.loadMVCCFloorKeys(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load mvcc prune floors: %w", err)
	}

	// Now bodies are guaranteed v2. Schema load + unique-value rebuild
	// can decode them.
	if err := engine.loadPersistedSchemas(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load persisted schema: %w", err)
	}

	if err := engine.ensureLabelCounts(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize label counts: %w", err)
	}

	// Per-type edge counts (issue #638): marker-gated verify/rebuild pass
	// over the edge-type index, same shape as the label counts above.
	if err := engine.ensureEdgeTypeCounts(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize edge-type counts: %w", err)
	}

	// Edge-between index backfill runs only after migrations have
	// settled the body format. Failure here is non-fatal at open
	// time — the index self-heals from the outgoing index on read.
	if err := engine.startIndexBackfill(startupEdgeBetweenIndex, initializeIndexBackfill); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize edge-between index: %w", err)
	}

	// Label-index backfill: rebuilds (label, nodeNumID) entries from
	// node bodies for stores carried over from binaries that didn't
	// maintain the index, or restored via file-level copy. Marker-gated
	// so the walk runs at most once per store. Same non-fatal stance as
	// the edge-between backfill — MATCH (n:Label) on an unrebuilt store
	// reads the partial on-disk index until the goroutine finishes.
	if err := engine.startIndexBackfill(startupLabelIndex, initializeIndexBackfill); err != nil {
		engine.stopIndexBackfill(startupEdgeBetweenIndex)
		engine.stopIndexBackfill(startupLabelIndex)
		db.Close()
		return nil, fmt.Errorf("failed to initialize label index: %w", err)
	}

	if !opts.InMemory {
		engine.startDeleteCleanup()
		engine.startCountDeltaFoldWorker()
	}

	if opts.WriteBehind {
		interval := opts.WriteBehindInterval
		if interval <= 0 {
			interval = 50 * time.Millisecond
		}
		engine.writeBehind = NewWriteBehindBuffer(interval, opts.WriteBehindMaxOps, engine.replayWriteBehind)
		engine.writeBehind.setDispatch(engine.drainCommitNotifications)
		engine.writeBehind.SetWarn(func(msg string) {
			engine.log.Warn(msg, "subsystem", "storage")
		})
	}
	return engine, nil
}

// reopenForPostMigrationCompaction closes the current Badger handle
// (forcing memtable flush to SSTs) and opens a fresh one with the same
// options, then re-loads the in-memory state that depends on the live
// DB pointer. Returns the new *badger.DB so the caller can swap it on
// the engine.
//
// The state we restore is everything that was loaded BEFORE migrations
// ran in NewBadgerEngineWithOptions: id dictionary, property-key
// dictionary, MVCC sequence, cached counts. Schema load is intentionally
// deferred to the caller — it decodes node bodies, so it has to run
// after migrations + reopen, not from inside this helper.
func (b *BadgerEngine) reopenForPostMigrationCompaction(badgerOpts badger.Options) (*managedBadgerDB, error) {
	if b.log != nil {
		b.log.Info("post-migration DB reopen starting")
	}
	if err := b.db.Close(); err != nil {
		return nil, fmt.Errorf("close pre-compaction DB: %w", err)
	}
	newDB, err := openManagedBadger(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("reopen DB: %w", err)
	}
	if err := b.idDict.loadFromBadger(newDB); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reload id dictionary: %w", err)
	}
	if err := b.propKeyDict.loadFromBadger(newDB); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reload property-key dictionary: %w", err)
	}
	// initializeMVCCSequence + initializeCounts read off b.db, so swap
	// it before calling them.
	b.db = newDB
	if err := b.initializeWALPropertyIdentity(newDB, false, false); err != nil {
		newDB.Close()
		return nil, err
	}
	if err := b.initializeMVCCSequence(); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reinitialize mvcc sequence: %w", err)
	}
	if err := b.initializeCounts(); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reinitialize counts: %w", err)
	}
	if b.log != nil {
		b.log.Info("post-migration DB reopen complete")
	}
	return newDB, nil
}

// NewBadgerEngineInMemory creates an in-memory BadgerDB for testing.
//
// Data is not persisted and is lost when the engine is closed.
// Useful for unit tests that need persistent storage semantics
// without actual disk I/O.
//
// Example:
//
//	engine, err := storage.NewBadgerEngineInMemory()
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer engine.Close()
//
//	// Use engine for testing...
func NewBadgerEngineInMemory() (*BadgerEngine, error) {
	return NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
	})
}

// initializeMVCCSequence reads the legacy engine-global MVCC sequence
// counter (if any) into b.mvccLegacyGlobalSeed. Per-namespace counters
// hydrate lazily via namespaceMVCC() and bootstrap to
// max(persisted-per-ns, legacyGlobalSeed) so versions in any namespace
// remain strictly greater than anything emitted under the global
// counter by older binaries.
func (b *BadgerEngine) initializeMVCCSequence() error {
	seed, err := b.loadPersistedMVCCSequence()
	if err != nil {
		return err
	}
	b.mvccLegacyGlobalSeed = seed
	return nil
}

func (b *BadgerEngine) loadPersistedMVCCSequence() (uint64, error) {
	var seq uint64
	err := b.db.viewHeld(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccSequenceKey())
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid mvcc sequence length: %d", len(val))
			}
			seq = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// allocateMVCCVersion mints the next (CommitTimestamp, CommitSequence)
// pair for namespace. namespace must be non-empty; the transaction layer
// enforces this via pinNamespaceFromIDLocked.
//
// CRITICAL: the per-namespace sequence is NOT written to the user's
// badger.Txn. Doing so would make every commit in the same namespace
// touch the same hot key, causing Badger to surface a write-write
// conflict ("Transaction Conflict") on the second concurrent commit
// before our constraint-validation lock can catch the actual user-data
// conflict. The contract pinned by
// docs/plans/consumer-pinned-error-contract-plan.md §2.1 requires
// commit-time UNIQUE violations to surface as
// "constraint violation: ... already exists", not Badger conflicts.
//
// Durability: the counter is in-memory authoritative for the lifetime of
// the engine; on restart, namespaceMVCC seeds the counter from
// (a) the persisted per-namespace key, last written by persistMVCCSequence,
// and (b) the legacy global counter as a backward-compat seed. A crash
// between commit and persistence loses at most the unflushed window of
// allocated-but-unpersisted sequences, which is reconciled by the
// startup max-of-persisted-and-legacy floor.
func (b *BadgerEngine) allocateMVCCVersion(txn *badger.Txn, namespace string, commitTime time.Time) (MVCCVersion, error) {
	_ = txn // intentionally unused — see doc above
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return MVCCVersion{}, err
	}
	seq, saturated := state.nextSequence()
	commitStamp, err := state.reserveCommitTimestamp(commitTime.UTC(), saturated)
	if err != nil {
		return MVCCVersion{}, err
	}
	return MVCCVersion{
		CommitTimestamp: commitStamp,
		CommitSequence:  seq,
	}, nil
}

// persistMVCCSequence writes the namespace's current sequence to disk in
// a separate Badger transaction so the persistence write does not
// participate in the user transaction's conflict-detection set. Best
// effort: a failure is logged at debug level rather than failing the
// commit, since the in-memory counter is authoritative and any lost
// persistence window is reconciled at next engine open.
func (b *BadgerEngine) persistMVCCSequence(namespace string) {
	if namespace == "" {
		return
	}
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return
	}
	seq := state.seq.Load()
	if seq == 0 {
		return
	}
	encodedSeq := make([]byte, 8)
	binary.BigEndian.PutUint64(encodedSeq, seq)
	persistKey := state.persistKey
	_ = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(persistKey, encodedSeq)
	})
}

func (b *BadgerEngine) readSchemaVersion() (int, error) {
	var version int
	err := b.db.View(func(txn *badger.Txn) error {
		var err error
		version, err = accessSchemaVersionInTxn(txn, loadSchemaVersion, 0)
		return err
	})
	return version, err
}

type schemaVersionAccess uint8

const (
	loadSchemaVersion schemaVersionAccess = iota
	storeSchemaVersion
	schemaVersionRecordBytes = 8
)

// accessSchemaVersionInTxn owns the existing schema-version record. The caller
// retains transaction ownership. Loads accept the historical one-byte ready
// marker as version zero; stores always emit the same eight-byte big-endian
// value at the unchanged key. Store's first return value is unused.
func accessSchemaVersionInTxn(txn *badger.Txn, operation schemaVersionAccess, value int) (int, error) {
	switch operation {
	case storeSchemaVersion:
		buf := make([]byte, schemaVersionRecordBytes)
		binary.BigEndian.PutUint64(buf, uint64(value))
		return 0, txn.Set(mvccSchemaVersionKey(), buf)
	case loadSchemaVersion:
	default:
		panic("invalid schema-version access")
	}
	item, err := txn.Get(mvccSchemaVersionKey())
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var version int
	err = item.Value(func(val []byte) error {
		// The historical edge-between ready marker predates this record.
		if len(val) == 1 && val[0] == 1 {
			return nil
		}
		if len(val) != schemaVersionRecordBytes {
			return fmt.Errorf("invalid schema version length: %d", len(val))
		}
		version = int(binary.BigEndian.Uint64(val))
		return nil
	})
	return version, err
}

func (b *BadgerEngine) writeSchemaVersion(version int) error {
	return b.db.Update(func(txn *badger.Txn) error {
		_, err := accessSchemaVersionInTxn(txn, storeSchemaVersion, version)
		return err
	})
}

// ============================================================================

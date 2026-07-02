// Package storage provides the storage engine interface and implementations for NornicDB.
//
// The storage layer is designed for Neo4j compatibility while adding NornicDB-specific
// extensions for memory decay, vector embeddings, and automatic relationship inference.
//
// Design Principles:
//   - Neo4j JSON export/import compatibility
//   - Testability through dependency injection
//   - Thread-safe implementations
//   - Property graph model (labeled property graph)
//
// Example Usage:
//
//	// Create storage engine
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create nodes
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User", "Person"},
//		Properties: map[string]any{
//			"name":  "Alice",
//			"email": "alice@example.com",
//		},
//		CreatedAt: time.Now(),
//	}
//	engine.CreateNode(node)
//
//	// Create relationships
//	edge := &storage.Edge{
//		ID:        storage.EdgeID("follows-1"),
//		StartNode: storage.NodeID("user-123"),
//		EndNode:   storage.NodeID("user-456"),
//		Type:      "FOLLOWS",
//		CreatedAt: time.Now(),
//	}
//	engine.CreateEdge(edge)
//
//	// Export to Neo4j format
//	nodes, _ := engine.AllNodes()
//	edges, _ := engine.AllEdges()
//	export := storage.ToNeo4jExport(nodes, edges)
//
//	// Save as JSON
//	data, _ := json.MarshalIndent(export, "", "  ")
//	os.WriteFile("graph-export.json", data, 0644)
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Common errors
var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrConflict         = errors.New("conflict")
	ErrExhausted        = errors.New("exhausted")
	ErrInvalidID        = errors.New("invalid id")
	ErrInvalidData      = errors.New("invalid data")
	ErrNotImplemented   = errors.New("not implemented")
	ErrInvalidEdge      = errors.New("invalid edge: start or end node not found")
	ErrStorageClosed    = errors.New("storage closed")
	ErrIterationStopped = errors.New("iteration stopped") // Sentinel to stop streaming early
	// ErrCrossNamespaceTransaction is returned when a single transaction
	// attempts to mix writes from multiple namespaces. The transaction layer
	// pins each transaction to one namespace at the first prefixed write, and
	// every subsequent write must share that prefix. Per-database MVCC
	// counters depend on this invariant.
	ErrCrossNamespaceTransaction = errors.New("transaction spans multiple namespaces")
	// ErrNotVisibleAtSnapshot signals that the entity exists (a head
	// record is present) but is not visible to the snapshot version
	// the caller passed. Distinct from ErrNotFound, which means the
	// entity has no head at all. Callers that maintain transaction
	// snapshot isolation MUST treat this as a hard miss rather than
	// falling back to a fresh-view read of the primary key, which
	// would expose peer commits that landed between the reader's
	// begin and the read.
	ErrNotVisibleAtSnapshot = errors.New("entity not visible at snapshot")
)

// NodeID is a strongly-typed unique identifier for graph nodes.
//
// Using a custom type provides:
//   - Type safety (can't accidentally use EdgeID where NodeID is expected)
//   - Clear API semantics
//   - Future extensibility (could add methods)
//
// Example:
//
//	id := storage.NodeID("user-123")
//	node, err := engine.GetNode(id)
type NodeID string

// EdgeID is a strongly-typed unique identifier for graph edges (relationships).
//
// Similar to NodeID, provides type safety and API clarity.
//
// Example:
//
//	id := storage.EdgeID("follows-456")
//	edge, err := engine.GetEdge(id)
type EdgeID string

// Node represents a graph node (vertex) in the labeled property graph.
//
// Nodes follow the Neo4j data model with NornicDB-specific extensions for
// memory decay, semantic search, and access tracking. Nodes are the fundamental
// entities in the graph and can represent people, documents, concepts, or any
// other entity in your domain.
//
// Core Neo4j Fields:
//   - ID: Unique identifier (must be unique across all nodes)
//   - Labels: Type tags like ["Person", "User"] (Neo4j :Person:User)
//   - Properties: Key-value data (any JSON-serializable types)
//     See docs/user-guides/property-data-types.md for complete type reference
//
// NornicDB Extensions (not exported to Neo4j):
//   - CreatedAt: When the node was first created
//   - UpdatedAt: Last modification timestamp
//   - NamedEmbeddings: Named vector embeddings (e.g., "title", "content", "default")
//   - ChunkEmbeddings: Chunked embeddings for long documents (legacy, migration support)
//
// Decay scoring is handled by the knowledge-layer scoring system (pkg/knowledgepolicy).
// Scores are computed at query time from AccessMeta and retention bindings, not stored on the node.
//
// Example 1 - Basic User Node:
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-alice"),
//		Labels: []string{"Person", "User"},
//		Properties: map[string]any{
//			"name":     "Alice Johnson",
//			"age":      30,
//			"email":    "alice@example.com",
//			"verified": true,
//		},
//		CreatedAt: time.Now(),
//	}
//	engine.CreateNode(node)
//
// Example 2 - Document Node with Metadata:
//
//	doc := &storage.Node{
//		ID:     storage.NodeID("doc-readme"),
//		Labels: []string{"Document", "Markdown"},
//		Properties: map[string]any{
//			"title":    "README.md",
//			"content":  "# Welcome to...",
//			"path":     "./README.md",
//			"size":     4096,
//			"language": "markdown",
//		},
//		CreatedAt: time.Now(),
//		Embedding: generateEmbedding("# Welcome to..."), // For semantic search
//	}
//
// Example 3 - Concept Node for Knowledge Graph:
//
//	concept := &storage.Node{
//		ID:     storage.NodeID("concept-database"),
//		Labels: []string{"Concept", "Technology"},
//		Properties: map[string]any{
//			"name":        "Database Systems",
//			"definition":  "Systems for storing and retrieving data",
//			"category":    "Software",
//			"importance":  "high",
//		},
//		CreatedAt: time.Now(),
//	}
//
// ELI12:
//
// Think of a Node like a character card in a trading card game:
//   - ID: The card's unique number (no two cards have the same)
//   - Labels: The types on the card (["Hero", "Warrior"])
//   - Properties: Stats on the card (name: "Alice", strength: 10, health: 100)
//
// NornicDB adds extra info:
//   - Embedding: A secret code that helps find similar cards
//   - Decay scoring: How "fresh" the card is — computed automatically by knowledge-layer policies
//
// Just like trading cards can be rare or common, frequently used or forgotten,
// Nodes track their usage and importance in the graph!
//
// Neo4j Compatibility:
//   - Labels map to Neo4j labels (e.g., :Person:User)
//   - Properties map to Neo4j properties
//   - ID must be unique across all nodes
//   - Extensions stored with "_" prefix in Neo4j exports
//
// Thread Safety:
//
//	Node structs are NOT thread-safe. The storage engine handles concurrency.
type Node struct {
	ID         NodeID         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`

	// NornicDB extensions
	CreatedAt       time.Time            `json:"-"`
	UpdatedAt       time.Time            `json:"-"`
	NamedEmbeddings map[string][]float32 `json:"-"` // Named vector embeddings (e.g., "title", "content", "default")
	ChunkEmbeddings [][]float32          `json:"-"` // Chunked embeddings for long documents (legacy, migration support)

	// Embedding metadata (separate from user Properties to avoid namespace pollution)
	// Keys: embedding_model, embedding_dimensions, has_embedding, embedded_at, has_chunks, chunk_count
	EmbedMeta map[string]any `json:"-"`

	// Internal storage flags (not exposed to users, used during encode/decode)
	EmbeddingsStoredSeparately bool `json:"-"` // True when embeddings are stored in separate keys (large node optimization)
	VisibilitySuppressed       bool `json:"-"` // Set by deindex cleanup when decay score falls below visibility threshold
}

// Edge represents a directed graph relationship (arc) between two nodes.
//
// Edges are directed connections that link nodes together, representing relationships
// like "Alice KNOWS Bob" or "Document CITES Paper". They follow the Neo4j relationship
// model with NornicDB extensions for automatic relationship inference and confidence scoring.
//
// Core Neo4j Fields:
//   - ID: Unique identifier for the relationship
//   - StartNode: Source node ID (where the arrow starts)
//   - EndNode: Target node ID (where the arrow points)
//   - Type: Relationship type (e.g., "KNOWS", "FOLLOWS", "CONTAINS")
//   - Properties: Key-value data about the relationship
//
// NornicDB Extensions:
//   - CreatedAt: When the relationship was created
//   - Confidence: How certain we are this relationship exists (0.0-1.0)
//   - AutoGenerated: True if detected by ML/inference, false if manually created
//
// Example 1 - Social Network Relationship:
//
//	edge := &storage.Edge{
//		ID:         storage.EdgeID("friendship-123"),
//		StartNode:  storage.NodeID("alice"),
//		EndNode:    storage.NodeID("bob"),
//		Type:       "KNOWS",
//		Properties: map[string]any{
//			"since":    "2020-01-15",
//			"strength": "close_friend",
//			"mutuality": true,
//		},
//		CreatedAt:     time.Now(),
//		Confidence:    1.0,  // Manually created = 100% certain
//		AutoGenerated: false,
//	}
//	engine.CreateEdge(edge)
//
// Example 2 - Document Citation:
//
//	citation := &storage.Edge{
//		ID:        storage.EdgeID("cite-paper-5"),
//		StartNode: storage.NodeID("paper-123"),
//		EndNode:   storage.NodeID("paper-456"),
//		Type:      "CITES",
//		Properties: map[string]any{
//			"context":    "Methods section",
//			"page":       12,
//			"importance": "high",
//		},
//		CreatedAt: time.Now(),
//		Confidence: 1.0,
//	}
//
// Example 3 - Auto-Detected Semantic Relationship:
//
//	// NornicDB inference engine detected similarity
//	autoEdge := &storage.Edge{
//		ID:            storage.EdgeID("similar-42"),
//		StartNode:     storage.NodeID("note-1"),
//		EndNode:       storage.NodeID("note-2"),
//		Type:          "SIMILAR_TO",
//		Confidence:    0.87,  // 87% confidence from embedding similarity
//		AutoGenerated: true,  // Created automatically
//		Properties: map[string]any{
//			"similarity":  0.87,
//			"method":      "cosine_similarity",
//			"detected_at": time.Now(),
//			"reason":      "High semantic similarity in embeddings",
//		},
//	}
//
// ELI12:
//
// Think of an Edge like a string connecting two beads (nodes):
//   - StartNode: The first bead (where you start)
//   - EndNode: The second bead (where the string goes to)
//   - Type: What kind of string? ("FRIENDS_WITH", "PARENT_OF", "LIKES")
//   - Properties: Info about the connection ("since when?", "how strong?")
//
// The arrow matters! "Alice KNOWS Bob" is different from "Bob KNOWS Alice"
// (they could both be true, but they're separate relationships).
//
// NornicDB's cool additions:
//   - Confidence: "I'm 85% sure these two things are related"
//   - AutoGenerated: "I found this connection myself!" vs "A human told me"
//
// Imagine your brain automatically connecting ideas: "Oh, these two notes
// seem related!" That's what AutoGenerated edges do - the system notices
// patterns and creates connections for you!
//
// Neo4j Compatibility:
//   - Type maps to Neo4j relationship type (e.g., -[:KNOWS]->)
//   - StartNode/EndNode map to Neo4j node IDs
//   - Properties map to Neo4j relationship properties
//   - Direction is always preserved (Neo4j requirement)
//
// Thread Safety:
//
//	Edge structs are NOT thread-safe. The storage engine handles concurrency.
type Edge struct {
	ID         EdgeID         `json:"id"`
	StartNode  NodeID         `json:"startNode"`
	EndNode    NodeID         `json:"endNode"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`

	// NornicDB extensions
	CreatedAt            time.Time `json:"-"`
	UpdatedAt            time.Time `json:"-"`
	Confidence           float64   `json:"-"`
	AutoGenerated        bool      `json:"-"`
	VisibilitySuppressed bool      `json:"-"`
}

// Engine defines the storage engine interface for graph database operations.
//
// All Engine implementations MUST be:
//   - Thread-safe: Safe for concurrent access from multiple goroutines
//   - ACID-like: Operations are atomic within their scope
//   - Idempotent where appropriate: CreateNode fails if ID exists
//
// The interface provides standard graph database operations:
//   - CRUD for nodes and edges
//   - Label-based queries
//   - Graph traversal (outgoing/incoming edges)
//   - Bulk operations for import/export
//   - Statistics
//
// Implementations:
//   - MemoryEngine: In-memory storage for testing and small datasets
//   - BadgerEngine: Persistent disk storage (planned)
//
// Example Usage:
//
//	var engine storage.Engine
//	engine = storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create data
//	node := &storage.Node{
//		ID:     "n1",
//		Labels: []string{"Person"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	if _, err := engine.CreateNode(node); err != nil {
//		log.Fatal(err)
//	}
//
//	// Query
//	people, _ := engine.GetNodesByLabel("Person")
//	// emit "Found N people" via the configured logger
//
//	// Traversal
//	outgoing, _ := engine.GetOutgoingEdges("n1")
//	for _, edge := range outgoing {
//		// emit "<start> -> <end> [<type>]" via the configured logger
//		_ = edge
//	}
type Engine interface {
	// Node operations
	CreateNode(node *Node) (NodeID, error) // Returns the actual stored ID (may be prefixed for namespaced engines)
	GetNode(id NodeID) (*Node, error)
	UpdateNode(node *Node) error
	DeleteNode(id NodeID) error

	// Edge operations
	CreateEdge(edge *Edge) error
	GetEdge(id EdgeID) (*Edge, error)
	UpdateEdge(edge *Edge) error
	DeleteEdge(id EdgeID) error

	// Query operations
	GetNodesByLabel(label string) ([]*Node, error)
	GetFirstNodeByLabel(label string) (*Node, error) // Optimized for LIMIT 1
	GetOutgoingEdges(nodeID NodeID) ([]*Edge, error)
	GetIncomingEdges(nodeID NodeID) ([]*Edge, error)
	GetEdgesBetween(startID, endID NodeID) ([]*Edge, error)
	GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge
	GetEdgesByType(edgeType string) ([]*Edge, error) // Fast lookup by edge type
	AllNodes() ([]*Node, error)
	AllEdges() ([]*Edge, error)
	GetAllNodes() []*Node

	// Degree operations (for graph algorithms)
	GetInDegree(nodeID NodeID) int
	GetOutDegree(nodeID NodeID) int

	// Schema operations
	GetSchema() *SchemaManager

	// Bulk operations (for import)
	BulkCreateNodes(nodes []*Node) error
	BulkCreateEdges(edges []*Edge) error

	// Bulk delete operations (for async flush performance)
	BulkDeleteNodes(ids []NodeID) error
	BulkDeleteEdges(ids []EdgeID) error

	// Batch read operations (for traversal performance)
	// BatchGetNodes fetches multiple nodes in a single operation
	// Returns a map for O(1) lookup by ID
	BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error)

	// Lifecycle
	Close() error

	// Stats
	NodeCount() (int64, error)
	EdgeCount() (int64, error)

	// DeleteByPrefix deletes all nodes and edges with IDs starting with the given prefix.
	// Used for DROP DATABASE operations to delete all data in a namespace.
	// Returns the number of nodes and edges deleted.
	//
	// This is an optional interface - engines that don't support it will return an error.
	// For multi-database support, this must be implemented.
	DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error)
}

// PrefixStatsEngine is an optional extension interface that provides fast per-prefix
// statistics without scanning and decoding all records.
//
// The prefix refers to the *ID prefix* (e.g., a database namespace prefix like "nornic:")
// and is applied to stored NodeID/EdgeID values (not the internal key prefix bytes).
//
// This is primarily used by NamespacedEngine so that NodeCount/EdgeCount can remain fast
// in multi-database deployments while still returning namespace-scoped results.
type PrefixStatsEngine interface {
	NodeCountByPrefix(prefix string) (int64, error)
	EdgeCountByPrefix(prefix string) (int64, error)
}

// LabelStatsEngine is an optional extension interface for fast label-cardinality
// lookups without materializing rows.
type LabelStatsEngine interface {
	NodeCountByLabel(label string) (int64, error)
}

// NamespaceLabelStatsProvider is an optional extension interface for fast
// namespace-scoped label-cardinality lookups.
type NamespaceLabelStatsProvider interface {
	NodeCountByLabelInNamespace(namespace, label string) (int64, error)
}

// AdjacentEdgesEngine is an optional extension interface for fetching both
// directions of edges incident to a node in a single underlying transaction.
//
// BFS-style traversals (shortestPath, variable-length MATCH) historically
// called GetOutgoingEdges + GetIncomingEdges per frontier node, opening two
// fresh Badger view transactions for each. With ~500-1000 frontier nodes per
// shortestPath request, that adds 1000-2000 transaction opens — the dominant
// per-request fixed cost the profile flagged. Engines that can fold both
// directions into a single view should implement this; callers fall back to
// the pair of single-direction calls when an engine doesn't.
type AdjacentEdgesEngine interface {
	GetAdjacentEdges(nodeID NodeID) (outgoing, incoming []*Edge, err error)
}

// ProjectedNodeReader is an optional extension interface for reading a node
// while decoding only a caller-specified subset of user properties.
//
// Implementations still return node metadata such as ID, labels, and timestamps,
// but Properties contains only the requested keys. A nil properties slice means
// "full node"; an empty non-nil slice means "no user properties".
type ProjectedNodeReader interface {
	GetNodeProjected(id NodeID, properties []string) (*Node, error)
}

// NamespaceLister is an optional extension interface that reports the known
// database namespaces stored in an engine.
//
// Returned values are unqualified namespace names (e.g., "nornic", "db2"),
// not ID prefixes (e.g., "nornic:").
type NamespaceLister interface {
	ListNamespaces() []string
}

// NamespaceSchemaProvider is an optional extension interface that provides per-namespace schema.
//
// This enables multi-database deployments to maintain isolated constraints/indexes per database,
// matching Neo4j’s per-database schema model.
type NamespaceSchemaProvider interface {
	GetSchemaForNamespace(namespace string) *SchemaManager
}

// TemporalLookupEngine is an optional extension interface for efficient temporal lookups
// on a namespaced engine view.
type TemporalLookupEngine interface {
	GetTemporalNodeAsOf(label, keyProp string, keyValue interface{}, validFromProp, validToProp string, asOf time.Time) (*Node, error)
}

// NamespaceTemporalLookupProvider is an optional extension interface that provides
// efficient temporal lookups scoped to a storage namespace.
type NamespaceTemporalLookupProvider interface {
	GetTemporalNodeAsOfInNamespace(namespace, label, keyProp string, keyValue interface{}, validFromProp, validToProp string, asOf time.Time) (*Node, error)
}

// TemporalCurrentNodeEngine is an optional extension interface for deciding whether
// a temporal node represents the current/live version for indexing and query routing.
type TemporalCurrentNodeEngine interface {
	IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error)
}

// NamespaceTemporalCurrentNodeProvider is an optional extension interface that
// evaluates current/live temporal versions within a namespace.
type NamespaceTemporalCurrentNodeProvider interface {
	IsCurrentTemporalNodeInNamespace(namespace string, node *Node, asOf time.Time) (bool, error)
}

// TemporalPruneOptions controls pruning of older temporal versions.
type TemporalPruneOptions struct {
	// MaxVersionsPerKey keeps at most this many closed historical versions per temporal key.
	// Zero means unlimited.
	MaxVersionsPerKey int

	// MinRetentionAge protects versions newer than now-MinRetentionAge from pruning.
	// Zero means no age-based protection.
	MinRetentionAge time.Duration
}

// TemporalMaintenanceEngine is an optional extension interface for rebuilding and
// pruning temporal index state after upgrades, restores, or operator maintenance.
type TemporalMaintenanceEngine interface {
	RebuildTemporalIndexes(ctx context.Context) error
	PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error)
}

// MVCCVersion identifies one committed storage version.
//
// Versions are ordered first by committed timestamp, then by a monotonic
// sequence allocated at commit time to break same-timestamp ties.
type MVCCVersion struct {
	CommitTimestamp time.Time
	CommitSequence  uint64
}

// IsZero reports whether the version is uninitialized.
func (v MVCCVersion) IsZero() bool {
	return v.CommitTimestamp.IsZero() && v.CommitSequence == 0
}

// Compare returns -1, 0, or 1 using MVCC ordering semantics.
func (v MVCCVersion) Compare(other MVCCVersion) int {
	vTime := v.CommitTimestamp.UTC().UnixNano()
	oTime := other.CommitTimestamp.UTC().UnixNano()
	switch {
	case vTime < oTime:
		return -1
	case vTime > oTime:
		return 1
	case v.CommitSequence < other.CommitSequence:
		return -1
	case v.CommitSequence > other.CommitSequence:
		return 1
	default:
		return 0
	}
}

// String renders a stable debug form for logs and diagnostics.
func (v MVCCVersion) String() string {
	if v.IsZero() {
		return "mvcc<zero>"
	}
	return fmt.Sprintf("mvcc<ts=%s seq=%d>", v.CommitTimestamp.UTC().Format(time.RFC3339Nano), v.CommitSequence)
}

// MVCCReadMode selects latest-visible versus snapshot-visible reads.
type MVCCReadMode string

const (
	MVCCReadLatest   MVCCReadMode = "latest"
	MVCCReadSnapshot MVCCReadMode = "snapshot"
)

// MVCCReadSelector describes which committed version a read should resolve.
type MVCCReadSelector struct {
	Mode    MVCCReadMode
	Version MVCCVersion
}

// MVCCHead stores the current persisted head for a logical record.
type MVCCHead struct {
	Version      MVCCVersion
	Tombstoned   bool
	FloorVersion MVCCVersion
}

// DefaultRetentionPolicyMaxVersionsPerKey — closed historical versions
// preserved per key by default. Zero means "no history, head-only" which
// collapses every write to a single primary-key write (no MVCC version
// archival). Operators who need audit/rollback raise
// NORNICDB_MVCC_RETENTION_MAX_VERSIONS via config.
const DefaultRetentionPolicyMaxVersionsPerKey = 0

// RetentionPolicy controls default MVCC historical retention for a storage engine.
// MaxVersionsPerKey applies to closed historical versions; the current head is always preserved.
// When MaxVersionsPerKey <= 0 the engine skips archival entirely: updates
// overwrite in place and deletes remove the primary key without creating a
// version record.
// TTL optionally protects versions newer than now-TTL from pruning.
type RetentionPolicy struct {
	MaxVersionsPerKey int
	TTL               time.Duration
}

// RetainsHistory reports whether this policy keeps any closed historical
// versions. Callers on the write hot path use this to short-circuit archival
// work when the head-only configuration is active.
func (p RetentionPolicy) RetainsHistory() bool {
	return p.MaxVersionsPerKey > 0
}

// EngineOptions contains storage-engine-wide options shared across engine implementations.
type EngineOptions struct {
	RetentionPolicy RetentionPolicy
	// IDFreelistTTL debounces numID recycling: after a node/edge is
	// pruned, its numID stays parked for this long before allocations
	// can reclaim it. Zero = engine default (30 seconds).
	IDFreelistTTL time.Duration
}

func normalizeRetentionPolicy(policy RetentionPolicy) RetentionPolicy {
	// MaxVersionsPerKey == 0 is a legitimate setting ("head-only, no
	// historical archival"). Only negative values are coerced to the
	// default so YAML/env that explicitly set 0 are honored.
	if policy.MaxVersionsPerKey < 0 {
		policy.MaxVersionsPerKey = DefaultRetentionPolicyMaxVersionsPerKey
	}
	if policy.TTL < 0 {
		policy.TTL = 0
	}
	return policy
}

// MVCCPruneOptions controls pruning of older MVCC versions.
// Zero values inherit the engine's configured RetentionPolicy.
type MVCCPruneOptions struct {
	MaxVersionsPerKey int
	MinRetentionAge   time.Duration
}

// MVCCVisibilityEngine is an optional extension interface for latest and
// snapshot-visible node and edge reads.
type MVCCVisibilityEngine interface {
	GetNodeLatestVisible(id NodeID) (*Node, error)
	GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error)
	GetEdgeLatestVisible(id EdgeID) (*Edge, error)
	GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error)
}

// MVCCIndexedVisibilityEngine is an optional extension interface for
// snapshot-visible graph queries that resolve label, type, and topology against
// MVCC history instead of only the current materialized indexes.
type MVCCIndexedVisibilityEngine interface {
	GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error)
	GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error)
	GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error)
	GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error)
	GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error)
}

// MVCCHeadEngine is an optional extension interface for persisted head lookups.
type MVCCHeadEngine interface {
	GetNodeCurrentHead(id NodeID) (MVCCHead, error)
	GetEdgeCurrentHead(id EdgeID) (MVCCHead, error)
}

// MVCCAppendEngine is an optional extension interface for immutable MVCC writes.
type MVCCAppendEngine interface {
	AppendNodeVersion(node *Node, version MVCCVersion) error
	AppendNodeTombstone(id NodeID, version MVCCVersion) error
	AppendEdgeVersion(edge *Edge, version MVCCVersion) error
	AppendEdgeTombstone(id EdgeID, version MVCCVersion) error
	UpdateNodeCurrentHead(id NodeID, version MVCCVersion, tombstoned bool) error
	UpdateEdgeCurrentHead(id EdgeID, version MVCCVersion, tombstoned bool) error
}

// MVCCMaintenanceEngine is an optional extension interface for rebuild and prune operations.
type MVCCMaintenanceEngine interface {
	RebuildMVCCHeads(ctx context.Context) error
	PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error)
}

// MVCCLatestEffectiveEngine is an optional extension interface for wrapper-level
// latest reads that merge pending, in-flight, and persisted state.
type MVCCLatestEffectiveEngine interface {
	GetNodeLatestEffective(id NodeID) (*Node, error)
	GetEdgeLatestEffective(id EdgeID) (*Edge, error)
}

// MVCCEnumerationEngine is an optional extension interface for latest-visible iteration.
type MVCCEnumerationEngine interface {
	BatchGetNodesLatestVisible(ids []NodeID) (map[NodeID]*Node, error)
	IterateLatestVisibleNodes(yield func(*Node) error) error
	IterateLatestVisibleEdges(yield func(*Edge) error) error
}

// SnapshotReaderInfo describes an active MVCC snapshot reader.
type SnapshotReaderInfo struct {
	ReaderID        string
	SnapshotVersion MVCCVersion
	StartTime       time.Time
	Namespace       string
}

// PressureBand represents the MVCC storage pressure level.
type PressureBand string

const (
	PressureNormal   PressureBand = "normal"
	PressureHigh     PressureBand = "high"
	PressureCritical PressureBand = "critical"
)

// ErrMVCCResourcePressure is returned when MVCC resource pressure exceeds snapshot lifetime.
var ErrMVCCResourcePressure = errors.New("mvcc: resource pressure exceeded snapshot lifetime")

// Snapshot expiration errors surface when an already-admitted reader is forced to stop.
var (
	ErrMVCCSnapshotGracefulCancel = errors.New("mvcc: snapshot cancelled due to resource pressure")
	ErrMVCCSnapshotHardExpired    = errors.New("mvcc: snapshot forcibly expired due to critical resource pressure")
)

// SnapshotReaderRegistry exposes active snapshot-reader tracking.
type SnapshotReaderRegistry interface {
	Register(info SnapshotReaderInfo) (string, func())
	ActiveCount() int64
	Snapshot() []SnapshotReaderInfo
}

// MVCCLifecycleEngine is an optional extension interface for lifecycle management.
type MVCCLifecycleEngine interface {
	RegisterSnapshotReader(info SnapshotReaderInfo) func()
	LifecycleStatus() map[string]interface{}
	TriggerPruneNow(ctx context.Context) error
	PauseLifecycle()
	ResumeLifecycle()
}

// MVCCLifecycleScheduleEngine is an optional extension for runtime lifecycle cadence control.
type MVCCLifecycleScheduleEngine interface {
	SetLifecycleSchedule(interval time.Duration) error
}

// MVCCLifecycleDebtKey describes one logical key contributing lifecycle debt.
type MVCCLifecycleDebtKey struct {
	LogicalKey       string `json:"logical_key"`
	Namespace        string `json:"namespace,omitempty"`
	DebtBytes        int64  `json:"debt_bytes"`
	TombstoneDepth   int    `json:"tombstone_depth"`
	FloorLagVersions int    `json:"floor_lag_versions"`
	VersionsToDelete int    `json:"versions_to_delete"`
}

// MVCCLifecycleDebtEngine is an optional extension for inspecting the highest-debt logical keys.
type MVCCLifecycleDebtEngine interface {
	TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey
}

// MVCCLifecycleController is the storage-facing control interface used by engines.
// A concrete implementation can live outside the storage package and be injected.
type MVCCLifecycleController interface {
	MVCCLifecycleEngine
	AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error)
	EvaluateSnapshotReader(info SnapshotReaderInfo) (graceful bool, hard bool)
	RunPruneNow(ctx context.Context, opts MVCCPruneOptions) (int64, error)
	StartLifecycle(ctx context.Context)
	StopLifecycle()
	IsLifecycleEnabled() bool
	IsLifecycleRunning() bool
	ReaderRegistry() SnapshotReaderRegistry
}

// NodeEventCallback is called when storage operations complete successfully.
// This allows external services (like search indexes) to stay synchronized with storage.
type NodeEventCallback func(node *Node)

// NodeDeleteCallback is called when a node is successfully deleted from storage.
type NodeDeleteCallback func(nodeID NodeID)

// EdgeEventCallback is called when edge storage operations complete successfully.
type EdgeEventCallback func(edge *Edge)

// EdgeDeleteCallback is called when an edge is successfully deleted from storage.
type EdgeDeleteCallback func(edgeID EdgeID)

// StorageEventNotifier is an optional interface that storage engines can implement
// to notify listeners of storage changes. This enables automatic synchronization
// between storage and external services (search indexes, embeddings, caches, etc.).
//
// Events are fired AFTER the storage operation succeeds, ensuring consistency.
//
// Example:
//
//	if notifier, ok := engine.(storage.StorageEventNotifier); ok {
//		notifier.OnNodeCreated(func(node *storage.Node) {
//			searchService.IndexNode(node)
//		})
//		notifier.OnNodeDeleted(func(nodeID storage.NodeID) {
//			searchService.RemoveNode(nodeID)
//		})
//		notifier.OnEdgeCreated(func(edge *storage.Edge) {
//			graphAnalyzer.UpdateMetrics(edge)
//		})
//	}
type StorageEventNotifier interface {
	// Node events
	OnNodeCreated(callback NodeEventCallback)
	OnNodeUpdated(callback NodeEventCallback)
	OnNodeDeleted(callback NodeDeleteCallback)

	// Edge events
	OnEdgeCreated(callback EdgeEventCallback)
	OnEdgeUpdated(callback EdgeEventCallback)
	OnEdgeDeleted(callback EdgeDeleteCallback)
}

// Neo4jExport represents the Neo4j JSON export format.
// This is compatible with `neo4j-admin database dump` JSON output.
type Neo4jExport struct {
	Nodes         []Neo4jNode         `json:"nodes"`
	Relationships []Neo4jRelationship `json:"relationships"`
}

// Neo4jNode is the Neo4j JSON export format for nodes.
type Neo4jNode struct {
	ID         string         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
}

// Neo4jNodeRef is a reference to a node in Neo4j relationship format.
type Neo4jNodeRef struct {
	ID     string   `json:"id"`
	Labels []string `json:"labels,omitempty"`
}

// Neo4jRelationship is the Neo4j JSON export format for relationships.
// Supports both flat format (startNode/endNode strings) and APOC format (start/end objects).
type Neo4jRelationship struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`

	// Flat format (neo4j-admin dump)
	StartNode string `json:"startNode,omitempty"`
	EndNode   string `json:"endNode,omitempty"`

	// APOC format (apoc.export.json)
	Start Neo4jNodeRef `json:"start,omitempty"`
	End   Neo4jNodeRef `json:"end,omitempty"`
}

// GetStartID returns the start node ID supporting both Neo4j export formats.
//
// Neo4j exports can use two formats:
//  1. Flat format: startNode/endNode as strings (neo4j-admin dump)
//  2. APOC format: start/end as objects (apoc.export.json)
//
// This method abstracts the difference, always returning the start node ID.
//
// Example:
//
//	// Flat format
//	rel := &Neo4jRelationship{
//		StartNode: "user-123",
//	}
//	// rel.GetStartID() returns "user-123"
//
//	// APOC format
//	rel = &Neo4jRelationship{
//		Start: Neo4jNodeRef{ID: "user-456"},
//	}
//	// rel.GetStartID() returns "user-456"
func (r *Neo4jRelationship) GetStartID() string {
	if r.Start.ID != "" {
		return r.Start.ID
	}
	return r.StartNode
}

// GetEndID returns the end node ID regardless of format.
func (r *Neo4jRelationship) GetEndID() string {
	if r.End.ID != "" {
		return r.End.ID
	}
	return r.EndNode
}

// ToNeo4jExport converts NornicDB nodes and edges to Neo4j JSON export format.
//
// This function prepares data for export that can be imported into Neo4j
// using neo4j-admin or APOC procedures. NornicDB-specific fields (decay score,
// embeddings, access counts) are stored with "_" prefix to mark them as
// system properties.
//
// The output is compatible with:
//   - `neo4j-admin database import`
//   - `CALL apoc.import.json()`
//   - Standard Neo4j JSON format
//
// Example:
//
//	// Get all data
//	nodes, _ := engine.GetNodesByLabel("") // All nodes
//	edges, _ := engine.AllEdges()
//
//	// Convert to Neo4j format
//	export := storage.ToNeo4jExport(nodes, edges)
//
//	// Save as JSON
//	data, _ := json.MarshalIndent(export, "", "  ")
//	err := os.WriteFile("neo4j-export.json", data, 0644)
//
//	// Import into Neo4j
//	// $ neo4j-admin database import --nodes=neo4j-export.json full
//	// Or in Cypher:
//	// CALL apoc.import.json("file:///neo4j-export.json")
//
// NornicDB extensions are preserved as properties:
//
//	_decayScore, _lastAccessed, _accessCount, _confidence, _autoGenerated
func ToNeo4jExport(nodes []*Node, edges []*Edge) *Neo4jExport {
	export := &Neo4jExport{
		Nodes:         make([]Neo4jNode, len(nodes)),
		Relationships: make([]Neo4jRelationship, len(edges)),
	}

	for i, n := range nodes {
		export.Nodes[i] = Neo4jNode{
			ID:         string(n.ID),
			Labels:     n.Labels,
			Properties: n.mergeInternalProperties(),
		}
	}

	for i, e := range edges {
		props := make(map[string]any)
		for k, v := range e.Properties {
			props[k] = v
		}
		// Add edge-specific internal properties
		if e.Confidence > 0 {
			props["_confidence"] = e.Confidence
		}
		if e.AutoGenerated {
			props["_autoGenerated"] = e.AutoGenerated
		}
		if !e.CreatedAt.IsZero() {
			props["_createdAt"] = e.CreatedAt.Unix()
		}

		export.Relationships[i] = Neo4jRelationship{
			ID:         string(e.ID),
			StartNode:  string(e.StartNode),
			EndNode:    string(e.EndNode),
			Type:       e.Type,
			Properties: props,
		}
	}

	return export
}

// FromNeo4jExport converts Neo4j JSON export format to NornicDB nodes and edges.
//
// This function imports data exported from Neo4j, extracting NornicDB-specific
// properties (those with "_" prefix) back into their dedicated fields.
//
// Supports both export formats:
//   - neo4j-admin database dump (flat format)
//   - apoc.export.json (nested format)
//
// Example:
//
//	// Load Neo4j export file
//	data, _ := os.ReadFile("neo4j-export.json")
//
//	var export storage.Neo4jExport
//	json.Unmarshal(data, &export)
//
//	// Convert to NornicDB format
//	nodes, edges := storage.FromNeo4jExport(&export)
//
//	// Import into NornicDB
//	if err := engine.BulkCreateNodes(nodes); err != nil {
//		log.Fatal(err)
//	}
//	if err := engine.BulkCreateEdges(edges); err != nil {
//		log.Fatal(err)
//	}
//
//	// emit "Imported N nodes, M edges" via the configured logger
//
// Returns nodes and edges ready for storage engine insertion.
func FromNeo4jExport(export *Neo4jExport) ([]*Node, []*Edge) {
	nodes := make([]*Node, len(export.Nodes))
	edges := make([]*Edge, len(export.Relationships))

	for i, n := range export.Nodes {
		// Copy properties
		props := make(map[string]any)
		for k, v := range n.Properties {
			props[k] = v
		}

		node := &Node{
			ID:         NodeID(n.ID),
			Labels:     n.Labels,
			Properties: props,
		}
		// Extract internal properties from the properties map
		node.ExtractInternalProperties()
		nodes[i] = node
	}

	for i, r := range export.Relationships {
		// Copy properties
		props := make(map[string]any)
		for k, v := range r.Properties {
			props[k] = v
		}

		edge := &Edge{
			ID:         EdgeID(r.ID),
			StartNode:  NodeID(r.GetStartID()),
			EndNode:    NodeID(r.GetEndID()),
			Type:       r.Type,
			Properties: props,
		}

		// Extract edge-specific internal properties
		if conf, ok := props["_confidence"].(float64); ok {
			edge.Confidence = conf
			delete(edge.Properties, "_confidence")
		}
		if auto, ok := props["_autoGenerated"].(bool); ok {
			edge.AutoGenerated = auto
			delete(edge.Properties, "_autoGenerated")
		}
		if created, ok := props["_createdAt"].(float64); ok {
			edge.CreatedAt = time.Unix(int64(created), 0)
			delete(edge.Properties, "_createdAt")
		}

		edges[i] = edge
	}

	return nodes, edges
}

// MarshalNeo4jJSON serializes to Neo4j-compatible JSON.
func (n *Node) MarshalNeo4jJSON() ([]byte, error) {
	neo4j := Neo4jNode{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: n.mergeInternalProperties(),
	}
	return json.Marshal(neo4j)
}

// mergeInternalProperties adds NornicDB-specific fields to properties.
func (n *Node) mergeInternalProperties() map[string]any {
	props := make(map[string]any)
	for k, v := range n.Properties {
		props[k] = v
	}

	// Add internal properties with _ prefix (Neo4j convention for system props)
	props["_createdAt"] = n.CreatedAt.Unix()
	props["_updatedAt"] = n.UpdatedAt.Unix()

	return props
}

// ExtractInternalProperties extracts NornicDB-specific fields from properties.
func (n *Node) ExtractInternalProperties() {
	if n.Properties == nil {
		return
	}

	if v, ok := n.Properties["_createdAt"].(float64); ok {
		n.CreatedAt = time.Unix(int64(v), 0)
		delete(n.Properties, "_createdAt")
	}
	if v, ok := n.Properties["_updatedAt"].(float64); ok {
		n.UpdatedAt = time.Unix(int64(v), 0)
		delete(n.Properties, "_updatedAt")
	}
	// Clean up legacy keys if present (from pre-1.1.0 exports).
	delete(n.Properties, "_decayScore")
	delete(n.Properties, "_lastAccessed")
	delete(n.Properties, "_accessCount")
}

// GetDefaultEmbedding returns the default embedding for a node, implementing
// migration behavior from ChunkEmbeddings to NamedEmbeddings.
//
// Migration behavior (temporary):
//   - If NamedEmbeddings["default"] exists, return it
//   - Otherwise, if ChunkEmbeddings has at least one vector, treat ChunkEmbeddings[0] as "default"
//   - Otherwise, return nil
//
// This allows backward compatibility with existing nodes that only have ChunkEmbeddings
// while transitioning to the new NamedEmbeddings model.
//
// Example:
//
//	node := &storage.Node{
//		ID: storage.NodeID("doc-1"),
//		NamedEmbeddings: map[string][]float32{
//			"default": []float32{0.1, 0.2, 0.3},
//		},
//	}
//	emb := node.GetDefaultEmbedding() // Returns []float32{0.1, 0.2, 0.3}
//
//	// Legacy node with ChunkEmbeddings
//	legacyNode := &storage.Node{
//		ID: storage.NodeID("doc-2"),
//		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
//	}
//	emb = legacyNode.GetDefaultEmbedding() // Returns []float32{0.4, 0.5, 0.6}
func (n *Node) GetDefaultEmbedding() []float32 {
	if n == nil {
		return nil
	}

	// First, check NamedEmbeddings["default"]
	if n.NamedEmbeddings != nil {
		if emb, ok := n.NamedEmbeddings["default"]; ok && len(emb) > 0 {
			return emb
		}
	}

	// Migration behavior: treat ChunkEmbeddings[0] as "default"
	if len(n.ChunkEmbeddings) > 0 && len(n.ChunkEmbeddings[0]) > 0 {
		return n.ChunkEmbeddings[0]
	}

	return nil
}

// =============================================================================
// STREAMING INTERFACE
// =============================================================================

// StreamingEngine extends Engine with streaming iteration support.
// This is optional - engines that don't support streaming will use
// the default AllNodes/AllEdges with chunked processing.
type StreamingEngine interface {
	Engine

	// StreamNodes iterates over all nodes without loading all into memory.
	// The callback is called for each node. Return an error to stop iteration.
	// Returns nil on successful completion, context.Canceled on cancellation.
	StreamNodes(ctx context.Context, fn func(node *Node) error) error

	// StreamEdges iterates over all edges without loading all into memory.
	StreamEdges(ctx context.Context, fn func(edge *Edge) error) error

	// StreamNodeChunks iterates over nodes in chunks for batch processing.
	// More efficient than StreamNodes when processing in batches.
	StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error
}

// PrefixStreamingEngine extends StreamingEngine with namespace/key-prefix-aware
// streaming for efficient bounded scans on engines where IDs embed tenant/db
// prefixes. This avoids full-store scans followed by callback filtering.
type PrefixStreamingEngine interface {
	StreamingEngine

	// StreamNodesByPrefix streams only nodes whose IDs start with the given prefix.
	StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error
}

// NodeVisitor is a function called for each node during streaming.
type NodeVisitor func(node *Node) error

// EdgeVisitor is a function called for each edge during streaming.
type EdgeVisitor func(edge *Edge) error

// StreamNodesWithFallback provides streaming iteration with fallback.
// If the engine supports StreamingEngine, it uses that.
// Otherwise, it loads all nodes but processes them in chunks.
func StreamNodesWithFallback(ctx context.Context, engine Engine, chunkSize int, fn NodeVisitor) error {
	// Try streaming interface first
	if streamer, ok := engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}

	// Fallback: load all but process in chunks to allow GC between
	nodes, err := engine.AllNodes()
	if err != nil {
		return err
	}

	for i, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(node); err != nil {
			return err
		}

		// Hint GC every chunk. We intentionally do not mutate the backing slice
		// returned by the engine because some engines may reuse that storage.
		if chunkSize > 0 && (i+1)%chunkSize == 0 {
			// runtime.GC() // Optional: enable for aggressive GC
		}
	}

	return nil
}

// StreamEdgesWithFallback provides streaming iteration with fallback.
func StreamEdgesWithFallback(ctx context.Context, engine Engine, chunkSize int, fn EdgeVisitor) error {
	// Try streaming interface first
	if streamer, ok := engine.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}

	// Fallback: load all but process in chunks
	edges, err := engine.AllEdges()
	if err != nil {
		return err
	}

	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(edge); err != nil {
			return err
		}

	}

	return nil
}

// NodeNeedsEmbedding checks if a node needs an embedding to be generated.
// Returns true if the node should have an embedding generated, false if it should be skipped.
//
// A node is skipped (returns false) if:
//   - It has an internal label (starts with '_')
//   - It already has an embedding
//   - It has the "embedding_skipped" property set
//   - It has "has_embedding" property explicitly set to false
//
// Example:
//
//	for _, node := range nodes {
//	    if storage.NodeNeedsEmbedding(node) {
//	        generateEmbedding(node)
//	    }
//	}
func NodeNeedsEmbedding(node *Node) bool {
	if node == nil {
		return false
	}

	// Skip internal nodes (labels starting with _)
	for _, label := range node.Labels {
		if len(label) > 0 && label[0] == '_' {
			return false
		}
	}

	// Skip if node already has user-provided named embeddings (e.g., Qdrant vectors).
	// Managed embeddings are generated into ChunkEmbeddings by the embed worker.
	if len(node.NamedEmbeddings) > 0 {
		for _, emb := range node.NamedEmbeddings {
			if len(emb) > 0 {
				return false
			}
		}
	}

	// Skip if already has managed embeddings (ChunkEmbeddings).
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		return false
	}

	// Skip if already processed (marked as skipped due to no embeddable content)
	if _, skipped := node.Properties["embedding_skipped"]; skipped {
		return false
	}

	return true
}

// CountNodesWithLabel counts nodes with a specific label using streaming.
func CountNodesWithLabel(ctx context.Context, engine Engine, label string) (int64, error) {
	if stats, ok := engine.(LabelStatsEngine); ok {
		return stats.NodeCountByLabel(label)
	}

	var count int64

	err := StreamNodesWithFallback(ctx, engine, 1000, func(node *Node) error {
		for _, l := range node.Labels {
			if l == label {
				count++
				break
			}
		}
		return nil
	})

	return count, err
}

// CollectLabels collects all unique labels using streaming.
func CollectLabels(ctx context.Context, engine Engine) ([]string, error) {
	labelSet := make(map[string]struct{})

	err := StreamNodesWithFallback(ctx, engine, 1000, func(node *Node) error {
		for _, l := range node.Labels {
			labelSet[l] = struct{}{}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	labels := make([]string, 0, len(labelSet))
	for l := range labelSet {
		labels = append(labels, l)
	}
	return labels, nil
}

// CollectEdgeTypes collects all unique edge types using streaming.
func CollectEdgeTypes(ctx context.Context, engine Engine) ([]string, error) {
	typeSet := make(map[string]struct{})

	err := StreamEdgesWithFallback(ctx, engine, 1000, func(edge *Edge) error {
		typeSet[edge.Type] = struct{}{}
		return nil
	})

	if err != nil {
		return nil, err
	}

	types := make([]string, 0, len(typeSet))
	for t := range typeSet {
		types = append(types, t)
	}
	return types, nil
}

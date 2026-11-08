// Package storage - BadgerDB transaction wrapper with ACID guarantees.
//
// This file implements atomic transactions for BadgerDB with full constraint
// validation and rollback support.
package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

type snapshotLabelCacheKey struct {
	scope         string
	queryNodes    bool
	projectionKey string
	reveal        bool
	stored        bool
}

// BadgerTransaction wraps Badger's native transaction with constraint validation.
//
// Provides ACID guarantees:
//   - Atomicity: All operations commit together or none do
//   - Consistency: Constraints are validated before commit
//   - Isolation: Changes invisible until commit
//   - Durability: Badger's WAL ensures persistence
type BadgerTransaction struct {
	// Author: NOR-288 lane (Codex), 2026-10-26. Accepted log application is
	// already validated input, not a new transaction admission decision.
	acceptedApplication bool
	// endpointsDecided records a prepared commit's one buffered-aware endpoint
	// check, taken before its durable decision; publication does not repeat it.
	endpointsDecided               bool
	clientDetached                 atomic.Bool
	commitLogicalID                string
	commitReleaseAcceptedAuthority func()
	mu                             trackedRWMutex

	// writeAdmission decides whether the transaction may write, before its
	// first write (SetWriteAdmission); nil once it admitted, or without one.
	writeAdmission func() error

	// changeCapture holds the database's change log while a capturing
	// commit runs (change_capture.go, #937).
	changeCapture *txChangeCapture

	// Transaction identity
	ID        string
	StartTime time.Time
	Status    TransactionStatus
	readTS    MVCCVersion
	// beginSnapshot freezes per-namespace MVCC sequences as of
	// BeginTransaction time. When the transaction lazily pins to a
	// namespace later (via the first prefixed write or SetNamespace),
	// readTS is rebound to beginSnapshot[namespace] rather than the
	// namespace's CURRENT sequence — peer commits that landed between
	// our begin and our pin must remain invisible for snapshot
	// isolation to hold. Namespaces not present in the snapshot (i.e.
	// created after our begin) bind to sequence 0, which correctly
	// treats anything committed under them as post-begin.
	beginSnapshot map[string]MVCCVersion
	// inflightAtBegin is the cut's copy of the reservation ledger: the
	// versions unpublished at the snapshot (mvccReservations); nil when
	// there were none. closeLocked releases it.
	inflightAtBegin *mvccCut
	// lifecycleSeq is the engine's prefix-delete sequence at begin
	// (prefixDroppedSince): a delete of this transaction's namespace, or a
	// maintenance pass over it, that started since fails its commit.
	lifecycleSeq uint64
	// bufferEpoch is the write-behind buffer's read epoch at begin
	// (replayedSinceBeginLocked).
	bufferEpoch uint64
	// CommitVersion is assigned once for a successful commit that mutates storage.
	CommitVersion MVCCVersion

	// bufferedAck is set when Commit acknowledged through the write-behind
	// buffer instead of a durable Badger commit (commitBufferedLocked). The
	// executor uses it to suppress durable receipt metadata on the ACK path.
	bufferedAck atomic.Bool

	// namespace pins this transaction to a single database namespace. It is
	// set lazily on the first prefixed write (or eagerly via SetNamespace by
	// callers that already know the target). Every subsequent mutation must
	// share this namespace; mixed writes return ErrCrossNamespaceTransaction.
	// Per-database MVCC counters and per-namespace lifecycle registries depend
	// on this invariant — without it, two namespaces' versions could collide.
	namespace                 string
	knowledgeSchema           *SchemaManager
	knowledgeSchemaDefinition *SchemaDefinition
	knowledgeSchemaDirty      bool
	schemaRuntime             *SchemaManager
	schemaTransaction         bool
	// schemaAdmission owns the live manager's compound DDL admission from
	// before this transaction's snapshot until native publication or rollback.
	schemaAdmission *SchemaManager

	// Badger's native transaction. It carries the transaction's conflict-
	// tracked reads; statement-time writes are staged in pendingWrites /
	// pendingDeletes instead (see stagedKV), so it stays far below Badger's
	// per-batch limit until commit. During a large commit it is the batch
	// currently being filled (see Commit).
	badgerTx *badger.Txn

	// Independent read-only snapshot keeps planning reads out of writer SSI
	// while excluding MVCC versions reserved but not yet physically committed.
	snapshotTx    *badger.Txn
	beginPresence *transactionBeginPresence

	// badgerDB is the engine's Badger handle the transaction began on; the
	// engine clears its own reference on Close.
	badgerDB *managedBadgerDB

	// badgerReadTs is the published Badger timestamp both badgerTx and
	// snapshotTx read at; it is registered with the engine's commit oracle
	// while badgerReadHeld, so compaction keeps every version they can see.
	badgerReadTs   uint64
	badgerReadHeld bool

	// commitW is the commit writer while Commit writes (see commitWriter).
	commitW *commitWriter
	// commitReleaseWrite releases the write barrier Commit holds until it
	// returns. commitCountsHeld and commitEdgeTypeCounts record the count
	// locks it holds while publishing (acquireCommitPublicationLocked).
	commitReleaseWrite   func()
	commitCountsHeld     bool
	commitEdgeTypeCounts bool

	// Parent engine for constraint validation
	engine *BadgerEngine

	// Track operations for constraint validation
	pendingNodes map[NodeID]*Node
	// nodeHeadCache memoizes commit-time endpoint head lookups within one
	// validation pass: bulk edge commits repeat endpoints per edge and the
	// engine read is a Badger point read each time. Nil until the first
	// cached lookup; discarded with the transaction.
	nodeHeadCache    map[NodeID]MVCCHead
	pendingIndex     *pendingNodeIndex
	pendingEdges     map[EdgeID]*Edge
	pendingAdjacency map[NodeID]map[EdgeID]*Edge
	snapshotEquality map[snapshotLabelCacheKey]*snapshotEqualityPopulation
	// edgeCreateNoOps records CREATE candidates over stored hidden
	// relationships. They stage only a hidden candidate image:
	// same-transaction SET/UPDATE writes replace that image without buffering
	// body, index, count, callback or WAL work, and without entering pendingEdges
	// or any visible read overlay. The commit owner compares the final image
	// with the latest stored predecessor before acknowledging the no-op or
	// rejecting a permanent different-image conflict.
	edgeCreateNoOps     map[EdgeID]*Edge
	deferredEdgeWrites  map[EdgeID]struct{}
	cancelledEdgeWrites map[EdgeID]struct{}
	deletedNodes        map[NodeID]struct{}
	deletedEdges        map[EdgeID]struct{}
	// droppedEmbeddings holds, per node, the managed embeddings an update in
	// this transaction invalidated (reconcileManagedEmbeddingsLocked), so a
	// later update in the same transaction that carries them back from a copy
	// read before the invalidation doesn't store them again. Nil until the
	// first invalidation.
	droppedEmbeddings map[NodeID]*Node
	// connectedDeletes are nodes a non-DETACH DELETE deleted while
	// relationships still connected them (DeleteGraphNode with RejectConnected), with their
	// state before: deleted for scans and lookups by label, still the
	// endpoints of those relationships, and deleted for good at commit when
	// none is left (resolveConnectedDeletesLocked). Nil until the first one.
	connectedDeletes map[NodeID]*Node
	// snapshotLabelNodes retains completed, immutable label streams for the
	// lifetime of this pinned snapshot. Bounded/aborted streams are never
	// cached, so LIMIT preserves early termination and memory proportionality.
	snapshotLabelNodes map[snapshotLabelCacheKey][]*Node
	// snapshotLabelNodeByID indexes the whole nodes of those completed label
	// streams by ID, so a write to a node the statement just read from the
	// snapshot (MATCH (n:L) SET ...) doesn't read the node again
	// (getCommittedNodeLocked). It holds pointers to nodes the streams already
	// keep, not copies.
	snapshotLabelNodeByID  map[NodeID]*Node
	snapshotNodeLabels     map[NodeID][]string
	snapshotNodeLabelBytes int
	// snapshotProjectedLabelNodes applies the same rule to exact property
	// projections. Keeping it separate preserves full-node cache reuse while
	// avoiding a full physical snapshot walk for every repeated projected read.
	snapshotProjectedLabelNodes map[snapshotLabelCacheKey][]*Node
	// snapshotLabelPrefixNodes retains small, early-stopped physical prefixes.
	// These are never treated as complete streams; a later caller resumes the
	// pinned label index after the last cached node when it needs more rows.
	snapshotLabelPrefixNodes     map[snapshotLabelCacheKey][]*Node
	snapshotLabelPrefixNodeBytes map[snapshotLabelCacheKey]int
	snapshotLabelPrefixBytes     int
	snapshotPrefixNodeByID       map[NodeID]*Node
	snapshotPrefixNodeBytesByID  map[NodeID]int
	snapshotPrefixNodeOrder      []NodeID
	snapshotPrefixNodeBytes      int
	snapshotEdgeByID             map[EdgeID]*Edge
	snapshotEdgeBytesByID        map[EdgeID]int
	snapshotEdgeOrder            []EdgeID
	snapshotEdgeBytes            int
	// Snapshot adjacency caches retain only a bounded set of visible edge IDs.
	// Edge bodies are resolved again against snapshotTx on each read so decay
	// filtering and the transaction's pending-edge overlay remain current.
	snapshotOutgoingAdjacency snapshotAdjacencyCache
	snapshotIncomingAdjacency snapshotAdjacencyCache
	operations                []Operation

	// Buffered writes - collected during transaction, flushed at commit
	// This batches all writes together for better performance while maintaining ACID guarantees
	pendingWrites  map[string][]byte // key -> value for Set operations
	pendingDeletes map[string]bool   // key -> true for Delete operations
	// pendingLabelCountDeltas keeps derived count metadata out of Badger's
	// optimistic user transaction. Independent node writes commonly share a
	// label, but they must not conflict merely because their derived counters
	// target the same metadata key.
	pendingLabelCountDeltas map[namespaceLabel]int64
	// pendingEdgeTypeCountDeltas is the edge-type analogue: per-type derived
	// count metadata (issue #638) kept out of the user transaction's conflict
	// set and applied right after commit.
	pendingEdgeTypeCountDeltas map[namespaceEdgeType]int64
	// pendingEdgeTypeLabelCountDeltas is the positional (label, type) tier of
	// the same counters, backing the one-labeled-endpoint count shapes.
	pendingEdgeTypeLabelCountDeltas map[edgeTypeLabelDelta]int64
	// When true, skip per-operation constraint checks and validate at commit only.
	deferConstraintValidation bool
	// Legacy name: a create reads storage for every ID the caller did not
	// mint (CreateMintedNode). When true, UUID-shaped fresh creates may still
	// skip the commit-time MVCC head reload once the ID is known absent.
	skipCreateExistenceCheck bool
	// implicit marks transactions that were auto-opened by the executor for a
	// single Cypher statement outside an explicit BEGIN/COMMIT. The Bolt
	// session coalesces durability at the end of the session via the
	// deferFlush path (pkg/bolt/server.go), so the per-Commit engine.Sync()
	// in Commit() is redundant — dropping it collapses ~N fsyncs/Msyncs to
	// the session-end flush without weakening the durability contract users
	// actually rely on (explicit tx commit and session close).
	commitMode EntityCommitMode
	// bufferedStagingTransferred marks a commit that handed its staged maps
	// and operations to the write-behind buffer (commitBufferedLocked);
	// closeLocked then drops them instead of allocating fresh maps the
	// dead transaction will never use.
	bufferedStagingTransferred bool
	// Prepared images are immutable while the detached native continuation
	// retains entity authority. Reuse them for buffered publication accounting.
	preparedEntityCommit *PreparedEntityCommit

	// Transaction metadata (for logging/debugging)
	Metadata           map[string]interface{}
	snapshotReaderInfo SnapshotReaderInfo

	// observed is set once the transaction reads through its snapshot; from
	// then on the snapshot can't move forward (PrepareMergeKey, #961).
	observed bool
	// mergeKeyReleases release the constraint key locks PrepareMergeKey
	// took, one per key; closeLocked calls them.
	mergeKeyReleases   map[uniqueConstraintLockKey]func()
	snapshotDeregister func()
	closedErr          error
}

// currentMVCCReadVersion returns the read snapshot for a transaction in
// the given namespace. Clamps the wall-clock sample to the namespace's
// high-water mark so a backward NTP step cannot make a new transaction
// observe an earlier timestamp than something already committed, then
// reads the namespace's current commit sequence. If namespace is empty
// (transaction has no pinned namespace yet — i.e. no write has named one
// and no SetNamespace was called) the engine returns a wall-clock-only
// version with sequence 0; the transaction must rebind readTS the moment
// its namespace is pinned via refreshReadVersionForNamespaceLocked.
func (b *BadgerEngine) currentMVCCReadVersion(namespace string) MVCCVersion {
	now := time.Now().UTC()
	if namespace == "" {
		return MVCCVersion{CommitTimestamp: now}
	}
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return MVCCVersion{CommitTimestamp: now}
	}
	highWater := state.highWaterNanos.Load()
	if highWater > now.UnixNano() {
		now = time.Unix(0, highWater).UTC()
	}
	return MVCCVersion{
		CommitTimestamp: now,
		CommitSequence:  state.seq.Load(),
	}
}

// BeginTransaction starts a new Badger transaction with ACID guarantees.
//
// At begin time the namespace is unknown. The unpinned scan boundary includes
// the captured namespaces' logical high-water marks as well as wall time, so
// coarse or backward clocks cannot hide rows in the physical snapshot. The
// first prefixed write, point read, or SetNamespace rebinds this boundary to
// that namespace's exact begin-time version.
func (b *BadgerEngine) BeginTransaction(ctx context.Context) (*BadgerTransaction, error) {
	// Admission is request-owned: a canceled or expired request must not start
	// a transaction. Once begun, the handle keeps its own captured snapshot
	// lifetime and never retains this request context; each later statement
	// supplies its own context.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.RLock()
	if b.ensureOpenLocked(false) != nil {
		b.mu.RUnlock()
		return nil, localizedError(localization.StorageTransactionEngineClosed(), ErrStorageClosed)
	}
	badgerDB := b.db
	b.mu.RUnlock()

	lifecycleSeq := b.prefixDrops.seq.Load()
	var bufferEpoch uint64
	if b.writeBehind != nil {
		bufferEpoch = b.writeBehind.readEpoch.Load()
	}
	// The cut: the physical snapshot and buffer prefix, then the logical
	// boundary, so nothing in the snapshot is newer than the boundary. The
	// reservation ledger is told before the snapshot; after it, in one step,
	// the cut takes its wall clock, reads the boundary and copies the ledger,
	// so the copy holds every version unpublished at the snapshot and every
	// later version compares newer (mvccReservations).
	b.mvccReservations.cutStarted()
	snapshotTx, badgerReadTs, beginPresence, err := b.beginTransactionSnapshot(ctx, badgerDB)
	if err != nil {
		b.mvccReservations.cutAbandoned()
		return nil, err
	}
	startTime, beginSnapshot, inflightAtBegin, err := b.mvccReservations.cutTaken(func(now time.Time) (map[string]MVCCVersion, error) {
		return b.snapshotNamespaceVersionsAt(ctx, now)
	})
	if err != nil {
		snapshotTx.Discard()
		beginPresence.release()
		badgerDB.endRead(badgerReadTs)
		return nil, err
	}
	// Unpinned, the boundary starts at the cut's wall clock; pinning replaces
	// it with the namespace's exact begin-time version
	// (refreshReadVersionForNamespaceLocked).
	readTS := MVCCVersion{CommitTimestamp: startTime}
	// Unpinned scans span namespaces in the already captured physical snapshot.
	// A wall-clock-only version (sequence zero) sorts before a committed head
	// at the same timestamp on coarse clocks, or after a backward clock step.
	// Include every captured namespace's boundary until pinning replaces this
	// scan boundary with that namespace's exact begin-time version.
	for _, version := range beginSnapshot {
		if readTS.Compare(version) < 0 {
			readTS = version
		}
	}
	txID := generateTxID()
	badgerTx := badgerDB.newTxnAt(badgerReadTs, true)

	tx := &BadgerTransaction{
		ID:                      txID,
		StartTime:               startTime,
		Status:                  TxStatusActive,
		readTS:                  readTS,
		beginSnapshot:           beginSnapshot,
		inflightAtBegin:         inflightAtBegin,
		lifecycleSeq:            lifecycleSeq,
		bufferEpoch:             bufferEpoch,
		badgerTx:                badgerTx,
		snapshotTx:              snapshotTx,
		beginPresence:           beginPresence,
		badgerDB:                badgerDB,
		badgerReadTs:            badgerReadTs,
		badgerReadHeld:          true,
		engine:                  b,
		pendingNodes:            make(map[NodeID]*Node),
		pendingEdges:            make(map[EdgeID]*Edge),
		deletedNodes:            make(map[NodeID]struct{}),
		deletedEdges:            make(map[EdgeID]struct{}),
		operations:              make([]Operation, 0),
		pendingWrites:           make(map[string][]byte),
		pendingDeletes:          make(map[string]bool),
		pendingLabelCountDeltas: make(map[namespaceLabel]int64),
		// Derived edge-type counts stay out of Badger's optimistic
		// conflict set, same as label counts (issue #638).
		pendingEdgeTypeCountDeltas:      make(map[namespaceEdgeType]int64),
		pendingEdgeTypeLabelCountDeltas: make(map[edgeTypeLabelDelta]int64),
		Metadata:                        make(map[string]interface{}),
		snapshotReaderInfo:              SnapshotReaderInfo{ReaderID: txID, SnapshotVersion: readTS, StartTime: startTime},
	}
	return tx, nil
}

// refreshReadVersionForNamespaceLocked rebinds tx.readTS once the
// transaction's namespace is known, registering the snapshot reader at
// the namespace-specific version. Called from pinNamespaceFromIDLocked /
// SetNamespace immediately after tx.namespace is set.
//
// Bind to the begin-time snapshot of this namespace's MVCC sequence
// rather than the namespace's CURRENT sequence. Without this, peer
// commits that landed between BeginTransaction and the lazy pin would
// become visible to this transaction — the snapshotIsolationConflict
// check (head.CommitSequence > tx.readTS.CommitSequence) would compare
// our pinned-in-the-future readTS against an equal-or-older head and
// silently miss real conflicts, breaking SI for concurrent CREATE on
// the same ID, RMW retries on contended counters, and the anchored-
// snapshot read invariants in transaction_snapshot_anomalies_test.go.
//
// A namespace not present in the snapshot was created after our begin;
// pin to sequence 0 and a wall-clock-only timestamp so anything
// committed under it shows up as post-begin (correct SI for a
// namespace that did not exist when we started).
func (tx *BadgerTransaction) refreshReadVersionForNamespaceLocked() error {
	readTS, ok := tx.beginSnapshot[tx.namespace]
	if !ok {
		readTS = MVCCVersion{CommitTimestamp: tx.StartTime.UTC()}
	}
	tx.readTS = readTS
	tx.snapshotReaderInfo.SnapshotVersion = readTS
	tx.snapshotReaderInfo.Namespace = tx.namespace
	if readTS.IsZero() {
		return nil
	}
	deregister, err := tx.engine.acquireSnapshotReader(tx.snapshotReaderInfo)
	if err != nil {
		return err
	}
	if tx.snapshotDeregister != nil {
		tx.snapshotDeregister()
	}
	tx.snapshotDeregister = deregister
	return nil
}

// IsActive returns true if the transaction is still active.
func (tx *BadgerTransaction) IsActive() bool {
	if tx.clientOwnershipError(transactionRead) != nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.clientOwnershipError(transactionRead) == nil && tx.Status == TxStatusActive
}

func (tx *BadgerTransaction) closeLocked(status TransactionStatus, discard bool, closedErr error) {
	tx.preparedEntityCommit = nil
	tx.beginPresence.release()
	tx.beginPresence = nil
	tx.inflightAtBegin.release()
	tx.inflightAtBegin = nil
	if tx.badgerTx != nil && tx.engine != nil && tx.engine.idDict != nil {
		// Drop any staged counter state. Safe whether the txn
		// committed (flushTxnCounters already cleared it) or is being
		// rolled back (we don't persist counters for aborted work).
		tx.engine.idDict.discardTxnCounters(tx.badgerTx)
	}
	if tx.badgerTx != nil && tx.engine != nil && tx.engine.propKeyDict != nil {
		tx.engine.propKeyDict.discardTxnCounters(tx.badgerTx)
	}
	if discard && tx.badgerTx != nil {
		tx.badgerTx.Discard()
	}
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
		tx.snapshotTx = nil
	}
	if tx.badgerReadHeld {
		tx.badgerDB.endRead(tx.badgerReadTs)
		tx.badgerReadHeld = false
	}
	// After a commit's publication to the constraint cache, so a MERGE
	// waiting for a key finds the committed node.
	tx.releaseMergeAdmissionLocked()
	tx.releaseWriteBarrierLocked()
	tx.clearSnapshotCachesLocked()
	tx.pendingAdjacency = nil
	if tx.bufferedStagingTransferred {
		// The write-behind buffer owns the staged state; the transaction is
		// dead after close, so leaving the fields nil saves six fresh-map
		// allocations per buffered autocommit.
		tx.pendingWrites = nil
		tx.pendingDeletes = nil
		tx.pendingLabelCountDeltas = nil
		tx.pendingEdgeTypeCountDeltas = nil
		tx.pendingEdgeTypeLabelCountDeltas = nil
	} else {
		tx.pendingWrites = make(map[string][]byte)
		tx.pendingDeletes = make(map[string]bool)
		tx.pendingLabelCountDeltas = make(map[namespaceLabel]int64)
		tx.pendingEdgeTypeCountDeltas = make(map[namespaceEdgeType]int64)
		tx.pendingEdgeTypeLabelCountDeltas = make(map[edgeTypeLabelDelta]int64)
	}
	tx.Status = status
	tx.closedErr = closedErr
	tx.releaseSnapshotReaderLocked()
	if schema := tx.schemaAdmission; schema != nil {
		tx.schemaAdmission = nil
		schema.schemaMutationMu.Unlock()
	}
}

// clearSnapshotCachesLocked drops what the transaction cached from its
// snapshot.
func (tx *BadgerTransaction) clearSnapshotCachesLocked() {
	tx.snapshotEquality = nil
	tx.snapshotLabelNodes = nil
	tx.snapshotProjectedLabelNodes = nil
	tx.snapshotOutgoingAdjacency.clear()
	tx.snapshotIncomingAdjacency.clear()
	tx.snapshotLabelPrefixNodes = nil
	tx.snapshotLabelPrefixNodeBytes = nil
	tx.snapshotLabelPrefixBytes = 0
	tx.snapshotPrefixNodeByID = nil
	tx.snapshotLabelNodeByID = nil
	tx.snapshotNodeLabels = nil
	tx.snapshotNodeLabelBytes = 0
	tx.snapshotPrefixNodeBytesByID = nil
	tx.snapshotPrefixNodeOrder = nil
	tx.snapshotPrefixNodeBytes = 0
	tx.snapshotEdgeByID = nil
	tx.snapshotEdgeBytesByID = nil
	tx.snapshotEdgeOrder = nil
	tx.snapshotEdgeBytes = 0
}

// releaseSnapshotReaderLocked ends the transaction's registration as an
// active snapshot reader. Commit calls it once conflict validation has
// passed, since nothing after that reads at the snapshot; close calls it
// otherwise.
func (tx *BadgerTransaction) releaseSnapshotReaderLocked() {
	if tx.snapshotDeregister != nil {
		tx.snapshotDeregister()
		tx.snapshotDeregister = nil
	}
}

// Namespace returns the database namespace this transaction is pinned to,
// or "" if no namespaced write has been recorded yet. Once set, every
// subsequent write must share this namespace.
func (tx *BadgerTransaction) Namespace() string {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.namespace
}

// SetNamespace eagerly pins the transaction to ns. Callers that already
// know the target namespace at BeginTransaction time (the cypher executor's
// transactionStorageWrapper, for example) use this to fail fast on misrouted
// writes instead of waiting for the first prefixed mutation. Returns an
// error if the transaction is already pinned to a different namespace.
//
// SetNamespace deliberately does not call ensureOperationAllowedLocked
// before the namespace bind: lifecycle expiration is meaningless for a
// transaction that has not yet registered against any namespace. We only
// require Status==active.
func (tx *BadgerTransaction) SetNamespace(ns string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.Status != TxStatusActive {
		if tx.closedErr != nil {
			return tx.closedErr
		}
		return ErrTransactionClosed
	}
	if ns == "" {
		return localizedError(localization.StorageTransactionNamespaceRequired(), nil)
	}
	if tx.namespace != "" && tx.namespace != ns {
		return localizedError(localization.StorageTransactionCrossNamespace(tx.namespace, ns), ErrCrossNamespaceTransaction)
	}
	if tx.engine != nil {
		if err := tx.engine.pendingNamespaceConflict(ns); err != nil {
			return err
		}
	}

	if tx.namespace == "" {
		tx.namespace = ns
		if err := tx.refreshReadVersionForNamespaceLocked(); err != nil {
			tx.namespace = ""
			return err
		}
	}
	return nil
}

// pinNamespaceFromIDLocked extracts the namespace from a prefixed entity ID
// and either pins the transaction (first write) or asserts that the new ID
// shares the existing pin (subsequent writes). Caller must hold tx.mu.
//
// IDs without a namespace prefix are rejected: the storage layer requires
// every transactional write to carry a "<db>:<id>" prefix.
func (tx *BadgerTransaction) pinNamespaceFromIDLocked(id string) error {
	ns, _, ok := ParseDatabasePrefix(id)
	if !ok {
		return localizedError(localization.StorageTransactionIDNamespaceRequired(id), nil)
	}
	if tx.engine != nil {
		if err := tx.engine.pendingNamespaceConflict(ns); err != nil {
			return err
		}
	}

	if tx.namespace == "" {
		tx.namespace = ns
		if err := tx.refreshReadVersionForNamespaceLocked(); err != nil {
			tx.namespace = ""
			return err
		}
		return nil
	}
	if tx.namespace != ns {
		return localizedError(localization.StorageTransactionCrossNamespace(tx.namespace, ns), ErrCrossNamespaceTransaction)
	}
	return nil
}

// pinEdgeNamespaceLocked asserts that an edge's three identifiers — its own
// ID, StartNode, and EndNode — share a single namespace, then pins the
// transaction to that namespace. An edge whose endpoints live in different
// namespaces is rejected even if no other writes have happened in this
// transaction yet, because such an edge could never satisfy the
// per-namespace MVCC ordering invariant. Caller must hold tx.mu.
func (tx *BadgerTransaction) pinEdgeNamespaceLocked(edge *Edge) error {
	if err := tx.pinNamespaceFromIDLocked(string(edge.ID)); err != nil {
		return err
	}
	if edge.StartNode != "" {
		if err := tx.pinNamespaceFromIDLocked(string(edge.StartNode)); err != nil {
			return err
		}
	}
	if edge.EndNode != "" {
		if err := tx.pinNamespaceFromIDLocked(string(edge.EndNode)); err != nil {
			return err
		}
	}
	return nil
}

type transactionOperation uint8

const (
	transactionRead transactionOperation = iota
	transactionDataWrite
	transactionSchemaWrite
	transactionCoordinatorCommit
)

// clientOwnershipError is the atomic ownership check usable before waiting on
// tx.mu and again under that lock. Accepted native completion alone retains
// ownership after the client handle is detached.
func (tx *BadgerTransaction) clientOwnershipError(operation transactionOperation) error {
	if operation != transactionCoordinatorCommit && tx.clientDetached.Load() {
		return ErrTransactionClosed
	}
	return nil
}

// ensureOperationAllowedLocked owns lifecycle and operation admission. Only an
// accepted commit continuation may proceed after client detachment. Data and
// schema writes retain their exclusion and claim a writer before staging.
func (tx *BadgerTransaction) ensureOperationAllowedLocked(operation transactionOperation) error {
	if err := tx.clientOwnershipError(operation); err != nil {
		return err
	}
	if tx.Status != TxStatusActive {
		if tx.closedErr != nil {
			return tx.closedErr
		}
		return ErrTransactionClosed
	}
	if tx.engine != nil && tx.namespace != "" {
		if err := tx.engine.pendingNamespaceConflict(tx.namespace); err != nil {
			return err
		}
	}

	if !tx.snapshotReaderInfo.StartTime.IsZero() && tx.engine != nil {
		graceful, hard := tx.engine.evaluateSnapshotReader(tx.snapshotReaderInfo)
		if hard {
			tx.closeLocked(TxStatusRolledBack, true, ErrMVCCSnapshotHardExpired)
			return ErrMVCCSnapshotHardExpired
		}
		if graceful {
			tx.closeLocked(TxStatusRolledBack, true, ErrMVCCSnapshotGracefulCancel)
			return ErrMVCCSnapshotGracefulCancel
		}
	}
	switch operation {
	case transactionDataWrite:
		if tx.schemaTransaction {
			return mixedSchemaAndDataError(false)
		}
		return tx.admitWriteLocked()
	case transactionSchemaWrite:
		if len(tx.operations) > 0 {
			return mixedSchemaAndDataError(true)
		}
		if err := tx.admitWriteLocked(); err != nil {
			return err
		}
		tx.schemaTransaction = true
	}
	return nil
}

// TransactionValidationSetting selects one independent native validation policy.
type TransactionValidationSetting uint8

const (
	// DeferredConstraintValidation defers constraint checks until commit.
	DeferredConstraintValidation TransactionValidationSetting = iota
	// SkipCreateCommitHeadReload skips only the commit-time head reload for a
	// minted/UUID-shaped create; stored-inclusive identity checks remain required.
	SkipCreateCommitHeadReload
)

// SetValidationSetting changes one policy without resetting the other. For
// example, tx.SetValidationSetting(DeferredConstraintValidation, true) defers
// constraints; tx.SetValidationSetting(SkipCreateCommitHeadReload, true) selects
// the minted-create optimization. Closed transactions reject either request.
func (tx *BadgerTransaction) SetValidationSetting(setting TransactionValidationSetting, enabled bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	switch setting {
	case DeferredConstraintValidation:
		tx.deferConstraintValidation = enabled
	case SkipCreateCommitHeadReload:
		tx.skipCreateExistenceCheck = enabled
	default:
		return fmt.Errorf("invalid transaction validation setting: %d", setting)
	}
	return nil
}

// SetImplicit marks this transaction as implicit (auto-opened by the executor
// for a single Cypher statement, no user BEGIN). Implicit transactions skip
// the per-Commit engine.Sync() because the Bolt session end and the async
// flush loop coalesce durability for them.
func (tx *BadgerTransaction) SetImplicit(implicit bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	tx.commitMode = CommitModeExplicit
	if implicit {
		tx.commitMode = CommitModeImplicit
	}
	return nil
}

func (tx *BadgerTransaction) bufferSet(key []byte, value []byte) {
	keyStr := string(key)
	// Remove from deletes if it was marked for deletion
	delete(tx.pendingDeletes, keyStr)
	// Buffer the write (copy value to avoid aliasing)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	tx.pendingWrites[keyStr] = valueCopy
}

// bufferDelete buffers a delete operation to be applied at commit time.
// If the key was previously buffered for write, it's removed from writes.
func (tx *BadgerTransaction) bufferDelete(key []byte) {
	keyStr := string(key)
	// Remove from writes if it was buffered
	delete(tx.pendingWrites, keyStr)
	// Mark for deletion
	tx.pendingDeletes[keyStr] = true
}

// bufferSetEdgeBetweenIndexes stages both exact relationship lookup indexes.
// Allocates numeric IDs for endpoints + edge via the engine's id
// dictionary. Any allocation failure propagates as a returned error.
func (tx *BadgerTransaction) bufferSetEdgeBetweenIndexes(edge *Edge) error {
	startNum, err := resolveOrAllocateNumericIDInTxn(tx.engine.idDict, tx.staged(), NodeID(edge.StartNode))
	if err != nil {
		return err
	}
	endNum, err := resolveOrAllocateNumericIDInTxn(tx.engine.idDict, tx.staged(), NodeID(edge.EndNode))
	if err != nil {
		return err
	}
	edgeNum, err := resolveOrAllocateNumericIDInTxn(tx.engine.idDict, tx.staged(), EdgeID(edge.ID))
	if err != nil {
		return err
	}
	tx.bufferSet(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum), []byte(edge.ID))
	tx.bufferSet(edgeBetweenHeadKey(startNum, endNum, edge.Type), []byte(edge.ID))
	return nil
}

// bufferDeleteEdgeBetweenIndexes stages set removal and conservatively clears
// the head so later reads can self-heal from the set or legacy outgoing index.
// A missing numID means no index entry exists — nothing to delete.
func (tx *BadgerTransaction) bufferDeleteEdgeBetweenIndexes(edge *Edge) {
	startNum, sOK := tx.engine.idDict.lookupNodeNumID(edge.StartNode)
	endNum, eOK := tx.engine.idDict.lookupNodeNumID(edge.EndNode)
	edgeNum, edgeOK := tx.engine.idDict.lookupEdgeNumID(edge.ID)
	if !sOK || !eOK || !edgeOK {
		return
	}
	tx.bufferDelete(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum))
	tx.bufferDelete(edgeBetweenHeadKey(startNum, endNum, edge.Type))
}

// flushBufferedWrites applies all buffered writes and deletes through the
// commit writer w (see Commit). Each key is its own unit, so the buffered
// writes may span any number of Badger batches.
func (tx *BadgerTransaction) flushBufferedWrites(ctx context.Context, w *batchWriter) error {
	// Apply deletes first (in case a key is both written and deleted, delete wins)
	for keyStr := range tx.pendingDeletes {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := []byte(keyStr)
		if tx.commitW != nil {
			tx.commitW.rawCache.add(tx.engine, key, tx.pendingWrites[keyStr])
		}
		if err := w.write(ctx, func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
			return fmt.Errorf("flushing delete for key %s: %w", keyStr, err)
		}
	}

	// Apply writes (only keys that weren't deleted)
	for keyStr, value := range tx.pendingWrites {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Skip if this key was also deleted (delete wins)
		if tx.pendingDeletes[keyStr] {
			continue
		}
		key := []byte(keyStr)
		if tx.commitW != nil {
			tx.commitW.rawCache.add(tx.engine, key, tx.pendingWrites[keyStr])
		}
		if err := w.write(ctx, func(txn *badger.Txn) error {
			return tx.engine.setKeyWithPendingHint(txn, key, value)
		}); err != nil {
			return fmt.Errorf("flushing write for key %s: %w", keyStr, err)
		}
	}

	// Clear buffers after successful flush
	tx.pendingWrites = make(map[string][]byte)
	tx.pendingDeletes = make(map[string]bool)

	return nil
}

// CreateNode adds a node to the transaction with constraint validation.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (tx *BadgerTransaction) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	return tx.createNode(ctx, node, false)
}

// CreateMintedNode is CreateNode for a node whose ID the caller minted for
// this create from a random UUID, as Cypher CREATE and MERGE do. No stored
// entity, visible or hidden, can hold such an ID, so storage is not read for
// one. A stable or imported ID, UUID-shaped or not, takes CreateNode.
func (tx *BadgerTransaction) CreateMintedNode(ctx context.Context, node *Node) (NodeID, error) {
	return tx.createNode(ctx, node, true)
}

func (tx *BadgerTransaction) createNode(ctx context.Context, node *Node, minted bool) (NodeID, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return "", err
	}
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// Enforce namespace prefix at storage layer and pin the transaction to a
	// single namespace; cross-namespace writes break per-database MVCC
	// counters and per-namespace lifecycle bookkeeping.
	if _, err := tx.engine.admitCreatedNode(node, tx); err != nil {
		return "", err
	}
	// The write uses its key names now; it encodes them only at commit.
	tx.engine.NotePropertyKeysInNamespace(tx.namespace, node.Properties)

	// Validate constraints BEFORE writing. The caller's context owns this
	// staging preparation read, so a canceled statement stages nothing.
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(ctx, node); err != nil {
			return "", err
		}
	}

	// Check for duplicates in pending
	if _, exists := tx.pendingNodes[node.ID]; exists {
		return "", ErrAlreadyExists
	}

	// CREATE identity is physical, not visibility-based. A suppressed node
	// still occupies its ID and must reject every CREATE route, including the
	// write-behind bulk/implicit paths, unless the ID was minted for it.
	if _, deleted := tx.deletedNodes[node.ID]; !deleted && !minted {
		_, err := tx.getCommittedNodeLocked(ctx, node.ID)
		if err == nil {
			return "", ErrAlreadyExists
		}
		if err != ErrNotFound {
			return "", fmt.Errorf("checking node existence: %w", err)
		}
	}

	// PERFORMANCE OPTIMIZATION: Buffer all writes and flush at commit time
	// This batches all writes together for better performance while maintaining ACID guarantees

	// Stamp creation times before encoding, as direct engine creation does (#1228).
	// Author: NOR-288/NOR-292 lane (Codex), 2026-10-27. Accepted replay
	// materializes an image, including zero timestamps, rather than a new create.
	if !tx.acceptedApplication {
		initializeCreationTimes(&node.CreatedAt, &node.UpdatedAt)
	}
	// Serialize node (may store embeddings separately if too large)
	_, embeddingsSeparate, err := tx.engine.writeCreatedNode(tx.badgerTx, tx.staged(), nodeKey(node.ID), node)
	if err != nil {
		return "", err
	}

	if embeddingsSeparate {
		if err := tx.bufferNodeChunksLocked(node); err != nil {
			return "", err
		}
	}

	tx.bufferAdjustNodeLabelCounts(tx.namespace, nil, node.Labels)

	// Add to pending embeddings index if needed
	if tx.engine.shouldIndexPendingEmbed(node) {
		tx.bufferSet(pendingEmbedKey(node.ID), []byte{})
	}

	// Track for read-your-writes and constraint validation
	nodeCopy := copyNode(node)
	tx.pendingNodeChangedLocked(tx.pendingNodes[node.ID], nodeCopy)
	tx.pendingNodes[node.ID] = nodeCopy
	delete(tx.deletedNodes, node.ID)

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
		// FreshID lets the commit loop skip the head reload for an ID this
		// transaction minted (CreateMintedNode): no stored or concurrent
		// write can hold it. A caller-supplied ID keeps the head check
		// whatever its shape; a stable import may well be UUID-shaped.
		FreshID: tx.skipCreateExistenceCheck && minted,
	})

	return node.ID, nil
}

// UpdateGraphNode stages a node using the supplied image interpretation.
// ClientNodeEdits reconciles managed embeddings; StoredNodeImages installs a
// complete authoritative image. Policies may differ between operations in one frame.
func (tx *BadgerTransaction) UpdateGraphNode(ctx context.Context, updateRequest NodeUpdateRequest) error {
	node, policy := updateRequest.After, updateRequest.Policy
	if policy > StoredNodeImages {
		return ErrInvalidData
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if node == nil || node.ID == "" {
		return ErrInvalidID
	}
	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}
	tx.engine.NotePropertyKeysInNamespace(tx.namespace, node.Properties)

	// Validate constraints. The caller's context owns this staging
	// preparation read, as it does for CreateNode.
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(ctx, node); err != nil {
			return err
		}
	}

	// A node this transaction deleted stays deleted: as in Neo4j, a write
	// to it changes nothing (DETACH DELETE n WITH n SET n.q = 1, #907).
	// Writing it would bring it back without counting it.
	if _, deleted := tx.deletedNodes[node.ID]; deleted {
		return nil
	}

	committedImage := policy == StoredNodeImages

	// Check if node exists
	var oldNode *Node
	createOpIdx := -1
	if pending, exists := tx.pendingNodes[node.ID]; exists {
		oldNode = copyNode(pending)
		createOpIdx = tx.pendingCreateNodeOperationIndexLocked(node.ID)
	} else {
		var err error
		if committedImage {
			oldNode, err = tx.getCommittedNodeReadLocked(ctx, node.ID, true, storedRead)
		} else {
			oldNode, err = tx.getCommittedNodeLocked(ctx, node.ID)
		}
		if err == ErrNotFound {
			if updateRequest.Before != nil {
				return localizedError(localization.StorageTransactionNodeChanged(string(node.ID)), ErrConflict)
			}
			return tx.missingWriteTargetLocked(bufferedNodeEntity, string(node.ID))
		}
		if err != nil {
			return fmt.Errorf("reading node: %w", err)
		}
		if err := tx.enrollCommittedWriteLocked(nodeKey(node.ID), "node"); err != nil {
			return err
		}
	}

	// Direct Engine updates historically treat zero times as missing defaults.
	// Keep this at the actual stored predecessor owner so WAL preparation and
	// direct observed writes agree, without trusting a caller's Before metadata.
	// Explicit frames and authoritative images retain their exact supplied times.
	if tx.commitMode == CommitModeDirect && policy == ClientNodeEdits && (node.CreatedAt.IsZero() || node.UpdatedAt.IsZero()) {
		if node == updateRequest.Before {
			// A no-change lock write may reuse its read image as After. Preserve the
			// immutable observation when filling a missing timestamp.
			after := *node
			node = &after
		}
		if node.CreatedAt.IsZero() {
			node.CreatedAt = oldNode.CreatedAt
		}
		if node.UpdatedAt.IsZero() {
			node.UpdatedAt = time.Now()
		}
	}

	// Validate policy constraints as part of the same staging preparation.
	if !tx.deferConstraintValidation && !labelsEqual(node.Labels, oldNode.Labels) {
		if err := tx.validatePolicyOnNodeLabelChange(ctx, node, oldNode); err != nil {
			return err
		}
	}

	var suppliedNewVectors bool
	body := node
	var err error
	if !committedImage {
		body, suppliedNewVectors, err = tx.reconcileManagedEmbeddingsLocked(oldNode, node)
		if err != nil {
			return err
		}
	}

	// Buffer updated node write
	nodeBytes, embeddingsSeparate, err := tx.engine.encodeNodeInTxn(tx.badgerTx, namespaceForNodeID(node.ID), body)
	if err != nil {
		return localizedError(localization.StorageClientNodeEncodeFailed(err), err)
	}
	_, dropped := tx.droppedEmbeddings[node.ID]
	invalidated := dropped && len(node.ChunkEmbeddings) == 0 && node.EmbedMeta == nil
	if committedImage || suppliedNewVectors || (embeddingsSeparate && body == node) || invalidated {
		if invalidated && !committedImage && !suppliedNewVectors {
			// The normal reconciliation owner already cleared committed keys.
			tx.clearPendingNodeEmbeddingRangeLocked(node.ID)
		} else if err := tx.clearNodeEmbeddingRangeLocked(node.ID); err != nil {
			return err
		}
		if embeddingsSeparate && body == node {
			if err := tx.bufferNodeChunksLocked(node); err != nil {
				return err
			}
		}
	}

	key := nodeKey(node.ID)
	tx.bufferSet(key, nodeBytes)

	// Update label indexes if changed
	oldLabelSet := make(map[string]bool)
	for _, label := range oldNode.Labels {
		oldLabelSet[label] = true
	}

	newLabelSet := make(map[string]bool)
	for _, label := range node.Labels {
		newLabelSet[label] = true
		if !oldLabelSet[label] {
			// New label - buffer index write
			indexKey, err := tx.engine.labelIndexKeyString(tx.staged(), label, node.ID)
			if err != nil {
				return fmt.Errorf("label index: %w", err)
			}
			tx.bufferSet(indexKey, []byte{})
		}
	}

	// Remove old labels (lookup-only — they must have existed at write time)
	for _, label := range oldNode.Labels {
		if !newLabelSet[label] {
			indexKey := tx.engine.labelIndexKeyStringLookup(label, node.ID)
			if indexKey == nil {
				continue
			}
			tx.bufferDelete(indexKey)
		}
	}
	tx.bufferAdjustNodeLabelCounts(tx.namespace, oldNode.Labels, node.Labels)

	// Positional (label, type) counters follow the node's relabel: its
	// tx-visible incident edges move between label buckets (issue #638).
	added, removed := nodeLabelChangeDeltas(oldNode.Labels, node.Labels)
	if err := tx.bufferNodeLabelChangeEdgeTypeDeltasLocked(ctx, node.ID, added, removed); err != nil {
		return err
	}

	// Track for read-your-writes
	nodeCopy := copyNode(node)
	tx.pendingNodeChangedLocked(tx.pendingNodes[node.ID], nodeCopy)
	tx.pendingNodes[node.ID] = nodeCopy
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		tx.bufferDelete(pendingEmbedKey(node.ID))
	} else if tx.engine.shouldIndexPendingEmbed(node) {
		tx.bufferSet(pendingEmbedKey(node.ID), []byte{})
	} else {
		tx.bufferDelete(pendingEmbedKey(node.ID))
	}

	if createOpIdx >= 0 {
		tx.operations[createOpIdx].Node = nodeCopy
		tx.operations[createOpIdx].Timestamp = time.Now()
		return nil
	}

	tx.operations = append(tx.operations, Operation{
		Type:               OpUpdateNode,
		Timestamp:          time.Now(),
		NodeID:             node.ID,
		Node:               nodeCopy,
		OldNode:            oldNode,
		nodeObservedBefore: updateRequest.Before,
	})

	return nil
}

// reconcileManagedEmbeddingsLocked keeps or invalidates node's managed
// embeddings across an update from oldNode, and returns the node to encode as
// the stored body (#963):
//   - an update that leaves the embedding source unchanged (every property
//     and label the embedding text policy feeds, sameEmbeddingSource) keeps
//     oldNode's embeddings on node, so readers, the node cache and the search
//     index keep its vectors. When they live in the embedding sidecar, the
//     body is stored without them and the sidecar stays their source;
//   - an update that changes the source invalidates them: node loses them,
//     and the sidecar is deleted with the update, so the embed worker finds
//     the node unembedded and embeds the new text.
//
// Embeddings the caller set on node itself (different vectors) are kept as
// written. The second result reports that replacement so staging can remove
// obsolete chunk keys without comparing the same vectors a second time.
func (tx *BadgerTransaction) reconcileManagedEmbeddingsLocked(oldNode, node *Node) (*Node, bool, error) {
	if len(oldNode.ChunkEmbeddings) == 0 && oldNode.EmbedMeta == nil {
		// Embeddings this transaction already invalidated, carried back by a
		// copy of the node read before (REMOVE n:Embedded, n.text SET n.x =
		// 1 writes the row's node twice), stay invalidated.
		if dropped, invalidated := tx.droppedEmbeddings[node.ID]; invalidated && carriesDroppedEmbeddings(node, dropped) {
			node.ChunkEmbeddings = nil
			node.EmbedMeta = nil
		}
		return node, false, nil
	}
	if len(node.ChunkEmbeddings) > 0 && !sameChunkEmbeddings(node.ChunkEmbeddings, oldNode.ChunkEmbeddings) {
		return node, true, nil
	}
	meta, sidecar, err := tx.retainedSidecarMetadataLocked(oldNode)
	if err != nil {
		return nil, false, err
	}
	if canCarryManagedEmbeddings(tx.engine, oldNode, node) {
		node.ChunkEmbeddings = oldNode.ChunkEmbeddings
		node.EmbedMeta = oldNode.EmbedMeta
		if !sidecar {
			return node, false, nil
		}
		// An unrelated user mutation advances the body version without
		// changing the embedded source. Publish the retained sidecar's new
		// freshness stamp atomically with that body, including repeated writes.
		if !oldNode.UpdatedAt.Equal(node.UpdatedAt) {
			meta[embeddingContentUpdatedAtKey] = node.UpdatedAt.UTC().Format(time.RFC3339Nano)
			data, err := encodeValue(meta)
			if err != nil {
				return nil, false, err
			}
			tx.bufferSet(embeddingMetaKey(node.ID), data)
		}
		body := *node
		body.ChunkEmbeddings = nil
		body.EmbedMeta = nil
		return &body, false, nil
	}
	if tx.droppedEmbeddings == nil {
		tx.droppedEmbeddings = make(map[NodeID]*Node)
	}
	tx.droppedEmbeddings[node.ID] = &Node{ChunkEmbeddings: oldNode.ChunkEmbeddings, EmbedMeta: oldNode.EmbedMeta}
	node.ChunkEmbeddings = nil
	node.EmbedMeta = nil
	if err := tx.scanCommittedEmbeddingKeysLocked(node.ID, func(key []byte) error {
		tx.bufferDelete(key)
		return nil
	}); err != nil {
		return nil, false, err
	}
	return node, false, nil
}

// carriesDroppedEmbeddings reports whether node carries the embeddings an
// earlier update invalidated: their vectors (or none, with their metadata),
// rather than new vectors its caller set.
func carriesDroppedEmbeddings(node, dropped *Node) bool {
	if len(node.ChunkEmbeddings) == 0 {
		return node.EmbedMeta != nil
	}
	return sameChunkEmbeddings(node.ChunkEmbeddings, dropped.ChunkEmbeddings)
}

func (tx *BadgerTransaction) pendingCreateNodeOperationIndexLocked(nodeID NodeID) int {
	for i := len(tx.operations) - 1; i >= 0; i-- {
		op := tx.operations[i]
		if op.NodeID != nodeID {
			continue
		}
		switch op.Type {
		case OpDeleteNode:
			return -1
		case OpCreateNode:
			return i
		}
	}
	return -1
}

// scanCommittedEmbeddingKeysLocked visits the embedding keys of nodeID as of
// the transaction's snapshot. The transaction's own
// writes are buffered (pendingWrites / pendingDeletes) and reach badgerTx only
// at commit, so the snapshot view reads the same keys as an iterator over
// badgerTx; but a badgerTx iterator also merges badgerTx's pending writes
// (MVCC history archives written during the transaction), which Badger
// sorts every time an iterator opens. Deleting N nodes in one transaction
// opened 2-3 iterators per node over up to N such writes: quadratic (#703).
// The callback owns each key and may retain it in the buffered write set.
func (tx *BadgerTransaction) scanCommittedEmbeddingKeysLocked(nodeID NodeID, visit func(key []byte) error) error {
	return tx.withSnapshotViewLocked(func(view *badger.Txn) error {
		return visitEmbeddingKeys(view, nodeID, visit)
	})
}

// incidentEdgesLocked returns every relationship that connects nodeID as the
// transaction sees storage, each once: its own pending relationships, the
// write-behind buffer's acknowledged ones and the committed adjacency, less
// those the transaction or an acknowledged buffered commit deleted.
//
// It is the transaction's one reader of a node's relationships for integrity
// work: a node delete's cascade (executeNodeDeletion), a non-DETACH delete's
// commit check (resolveConnectedDeletesLocked), the positional counts a
// relabel adjusts (incidentEdgeTypesTxLocked) and the DELETE checks above the
// transaction (IncidentEdges). So it reads storage, not the visibility layer:
// a relationship that on-demand suppression or knowledge-policy decay hides
// from reads still connects its endpoints, and a delete that skipped it left
// it dangling. Bodies come from the stored records, as the engine's own
// cascade reads them (nodeDeletionEdges).
func (tx *BadgerTransaction) incidentEdgesLocked(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	var edges []*Edge
	listed := make(map[EdgeID]struct{})
	include := func(edge *Edge) {
		listed[edge.ID] = struct{}{}
		edges = append(edges, edge)
	}
	skip := func(edgeID EdgeID) bool {
		if _, rewritten := tx.pendingEdges[edgeID]; rewritten {
			return true
		}
		if _, ok := listed[edgeID]; ok {
			return true
		}
		// The transaction's deletes are buffered until commit, so a
		// multi-node DETACH DELETE meets the same relationship from each
		// endpoint.
		if _, deleted := tx.deletedEdges[edgeID]; deleted {
			return true
		}
		return false
	}

	// Relationships this transaction created or rewrote: the committed index
	// does not list the new ones (#741).
	pending, err := tx.pendingAdjacencyLocked(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	for edgeID, edge := range pending {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, deleted := tx.deletedEdges[edgeID]; !deleted {
			include(edge)
		}
	}

	// Buffered transactions read committed state through the same capture
	// owner as engine adjacency. Overlay selection and committed IDs must
	// not straddle replay/retirement. Transaction-local rewrites and deletes
	// still take precedence over that captured stored view.
	if tx.engine.writeBehind != nil {
		// Integrity preparation follows the selected staging or commit phase lifetime.
		outgoing, incoming, err := readAdjacency(ctx, tx.engine, adjacencyBoth, nodeID, "", nil, storedRead)
		if err != nil {
			return nil, err
		}
		for _, adjacent := range [...][]*Edge{outgoing, incoming} {
			for _, edge := range adjacent {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if !skip(edge.ID) {
					include(copyEdge(edge))
				}
			}
		}
		return edges, nil
	}

	committed := func(index, bodies *badger.Txn) error {
		var edgeIDs []EdgeID
		for _, prefix := range [...][]byte{tx.engine.outgoingIndexPrefixString(nodeID), tx.engine.incomingIndexPrefixString(nodeID)} {
			// Lookup-only: a missing numID means no relationship was
			// ever indexed there.
			if prefix == nil {
				continue
			}
			opts := badgerIteratorOptions()
			opts.Prefix = prefix
			it := index.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := ctx.Err(); err != nil {
					it.Close()
					return err
				}
				edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().Key())
				if !ok {
					continue
				}
				if edgeID, ok := tx.engine.idDict.lookupEdgeIDByNum(edgeNum); ok {
					edgeIDs = append(edgeIDs, edgeID)
				}
			}
			it.Close()
		}
		for _, edgeID := range edgeIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if skip(edgeID) {
				continue
			}
			edge, err := tx.engine.loadEdgeForAdjacencyTombstoneInTxn(bodies, edgeID)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			include(edge)
		}
		return nil
	}
	// The index is read through the snapshot view, not badgerTx: a badgerTx
	// iterator also merges the transaction's MVCC history archives, which
	// Badger sorts every time an iterator opens, and deleting N nodes opened
	// iterators over up to N of them per node (#703). Bodies are read through
	// badgerTx, so a peer's change to one conflicts with this transaction.
	err = tx.withSnapshotViewLocked(func(view *badger.Txn) error {
		return committed(view, tx.badgerTx)
	})
	return edges, err
}

// bufferRemoveEdgeBookkeepingBuffered buffers an edge's primary-key and index
// deletions, marks it deleted, removes it from the pending set, and adjusts the
// per-type and positional (label, type) counters, for each relationship a
// node delete cascades (incidentEdgesLocked): committed, buffered or staged
// (#741) alike.
func (tx *BadgerTransaction) bufferRemoveEdgeBookkeepingBuffered(ctx context.Context, edge *Edge, deletedNodeID NodeID, deletedNodeLabels []string) error {
	tx.bufferDelete(edgeKey(edge.ID))
	if outKey := tx.engine.outgoingIndexKeyStringLookup(edge.StartNode, edge.ID); outKey != nil {
		tx.bufferDelete(outKey)
	}
	if inKey := tx.engine.incomingIndexKeyStringLookup(edge.EndNode, edge.ID); inKey != nil {
		tx.bufferDelete(inKey)
	}
	if typeKey := tx.engine.edgeTypeIndexKeyStringLookup(edge.Type, edge.ID); typeKey != nil {
		tx.bufferDelete(typeKey)
	}
	tx.bufferDeleteEdgeBetweenIndexes(edge)

	tx.deletedEdges[edge.ID] = struct{}{}
	tx.removePendingEdgeLocked(edge.ID)

	tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), edge.Type, -1)

	// Positional (label, type) counters: the deleted node's labels for its
	// side, the peer's tx-visible labels for the other.
	startLabels := deletedNodeLabels
	endLabels := deletedNodeLabels
	if edge.StartNode == deletedNodeID && edge.EndNode != deletedNodeID {
		peer, err := tx.nodeLabelsTxVisibleLocked(ctx, edge.EndNode)
		if err != nil {
			return err
		}
		endLabels = peer
	} else if edge.EndNode == deletedNodeID && edge.StartNode != deletedNodeID {
		peer, err := tx.nodeLabelsTxVisibleLocked(ctx, edge.StartNode)
		if err != nil {
			return err
		}
		startLabels = peer
	}
	tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), edge.Type, startLabels, endLabels, -1)
	return nil
}

// DeleteGraphNode stages node deletion using the selected relationship policy.
// CascadeEdges removes all incident relationships. RejectConnected defers the
// connectivity check until commit so later clauses may remove relationships.
// Until then the node is absent from scans but remains a relationship endpoint.
// Invalid modes return ErrInvalidData without changing the transaction.
//
// Examples:
//
//	tx.DeleteGraphNode(ctx, id, CascadeEdges) // DETACH DELETE
//	tx.DeleteGraphNode(ctx, id, RejectConnected) // DELETE
func (tx *BadgerTransaction) DeleteGraphNode(ctx context.Context, nodeID NodeID, mode NodeDeletionMode) error {
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if mode != CascadeEdges && mode != RejectConnected {
		return ErrInvalidData
	}
	if nodeID == "" {
		return ErrInvalidID
	}
	if err := tx.pinNamespaceFromIDLocked(string(nodeID)); err != nil {
		return err
	}
	// A node this transaction already deleted is gone, as a read of it
	// reports: deleting it again changes nothing (#827).
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return ErrNotFound
	}
	if err := tx.materializeDeferredEdgesLocked(ctx); err != nil {
		return err
	}

	// Capture old node state for constraint bookkeeping (e.g., unique value unregister).
	var oldNode *Node
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		oldNode = copyNode(pending)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(ctx, nodeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := tx.enrollCommittedWriteLocked(nodeKey(nodeID), "node"); err != nil {
			return err
		}
	}

	if mode == RejectConnected {
		if tx.connectedDeletes == nil {
			tx.connectedDeletes = make(map[NodeID]*Node)
		}
		tx.connectedDeletes[nodeID] = oldNode
		tx.deletedNodes[nodeID] = struct{}{}
		return nil
	}
	result, err := tx.engine.executeNodeDeletion(ctx, tx.staged(), nodeID, oldNode)
	if err != nil {
		return err
	}
	tx.recordNodeDeletionLocked(nodeID, result)
	return nil
}

// enrollCommittedWriteLocked adds key, the primary key of a committed node or
// relationship this transaction updates or deletes, to Badger's conflict
// set. Snapshot body reads use a separate view, so without it a peer that
// commits the same key after this transaction's snapshot validation and
// before its Badger commit would be overwritten or deleted, and both
// transactions would commit. A missing key still enrolls the read.
func (tx *BadgerTransaction) enrollCommittedWriteLocked(key []byte, kind string) error {
	if _, err := tx.badgerTx.Get(key); err != nil && err != badger.ErrKeyNotFound {
		return fmt.Errorf("tracking %s write conflict: %w", kind, err)
	}
	return nil
}

// recordNodeDeletionLocked owns the staged deletion's pending-state and
// Operation publication after the common physical/staged kernel succeeds.
func (tx *BadgerTransaction) recordNodeDeletionLocked(nodeID NodeID, result nodeDeletionResult) {
	tx.pendingNodeChangedLocked(tx.pendingNodes[nodeID], nil)
	delete(tx.pendingNodes, nodeID)
	tx.deletedNodes[nodeID] = struct{}{}
	operation := result.operation(nodeID)
	operation.Timestamp = time.Now()
	tx.operations = append(tx.operations, operation)
}

// IncidentEdges lists every relationship that connects nodeID as the
// transaction sees storage, those the visibility layer hides included
// (IncidentEdgeLister): the relationships a DETACH DELETE of the node
// removes, and the ones that keep a non-DETACH DELETE from removing it.
//
// It is a bulk read, so ctx is the caller's: it admits the transaction's read
// lock and the enumeration polls it, so a canceled or expired request returns
// its own error and no partial relationship list, and the enumeration stops
// instead of materializing the rest.
//
// Admission is cancellable as the transaction's other context-carrying
// admission is: a request canceled or expired while it waits for another
// holder returns its own error instead of waiting for that holder to finish.
// The holder is never preempted, and a canceled waiter leaves no hold behind.
// The transaction's own state keeps its precedence: a released client reports
// ErrTransactionClosed even for a canceled request, and once the read is
// admitted the locked lifecycle guard runs before anything is read. This is
// the same ownership order PublishCommit uses at tx.mu.
func (tx *BadgerTransaction) IncidentEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(nodeID)); err != nil {
		return nil, err
	}
	edges, err := tx.incidentEdgesLocked(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	for i, edge := range edges {
		edges[i] = copyEdge(edge)
	}
	return edges, nil
}

// CreateEdge adds an edge to the transaction.
func (tx *BadgerTransaction) CreateEdge(ctx context.Context, edge *Edge) error {
	return tx.bulkCreateEdges(ctx, []*Edge{edge}, false)
}

// CreateMintedEdge is CreateEdge for a relationship whose ID the caller
// minted for this create, as CreateMintedNode is for a node.
func (tx *BadgerTransaction) CreateMintedEdge(ctx context.Context, edge *Edge) error {
	return tx.bulkCreateEdges(ctx, []*Edge{edge}, true)
}

// EdgeCreateWasNoOp reports whether CreateEdge found a stored hidden
// predecessor and staged only a possible no-op candidate. Callers use this
// disposition to avoid premature CREATE stats; same-transaction writes may
// still complete the candidate image. The candidate never enters a visible
// read overlay, and commit is the sole identity decision.
func (tx *BadgerTransaction) EdgeCreateWasNoOp(id EdgeID) bool {
	if tx == nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	_, ok := tx.edgeCreateNoOps[id]
	return ok
}

// BulkCreateEdges buffers many edges in one pass, amortizing the lock, the
// lifecycle check, and — most importantly — the committed-node reads that
// check each endpoint and supply its labels. A batch of N edges that share K
// distinct endpoint nodes (K ≪ 2N in practice for graph fan-in/fan-out)
// reads K nodes instead of up to 2N.
//
// Every edge is checked as CreateEdge checks one (validateNewEdgeLocked)
// before any is buffered, so a rejected edge leaves the batch unwritten
// instead of the edges before it buffered, and each edge is then buffered as
// CreateEdge buffers one (bufferNewEdgeLocked).
func (tx *BadgerTransaction) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	return tx.bulkCreateEdges(ctx, edges, false)
}

// BulkCreateMintedEdges is BulkCreateEdges for relationships whose IDs the
// caller minted for this create (CreateMintedEdge).
func (tx *BadgerTransaction) BulkCreateMintedEdges(ctx context.Context, edges []*Edge) error {
	return tx.bulkCreateEdges(ctx, edges, true)
}

func (tx *BadgerTransaction) bulkCreateEdges(ctx context.Context, edges []*Edge, minted bool) error {
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(edges) == 0 {
		return nil
	}

	// Endpoints resolved for this batch, read once each from pendingNodes /
	// deletedNodes / committed-storage state.
	type endpoint struct {
		labels  []string
		visible bool
	}
	resolve := tx.visibleNodeLabelsLocked
	var batch map[EdgeID]struct{}
	var singleLabels [1][2][]string
	var singleNoOp [1]bool
	labels, noOps := singleLabels[:], singleNoOp[:]
	if len(edges) > 1 {
		endpoints := make(map[NodeID]endpoint, util.SafePreallocProduct(len(edges), 2))
		resolve = func(ctx context.Context, id NodeID) ([]string, bool, error) {
			if cached, ok := endpoints[id]; ok {
				return cached.labels, cached.visible, nil
			}
			labels, visible, err := tx.visibleNodeLabelsLocked(ctx, id)
			if err != nil {
				return nil, false, err
			}
			endpoints[id] = endpoint{labels: labels, visible: visible}
			return labels, visible, nil
		}
		batch = make(map[EdgeID]struct{}, len(edges))
		labels = make([][2][]string, len(edges))
		noOps = make([]bool, len(edges))
	}
	for i, edge := range edges {
		startLabels, endLabels, noOp, err := tx.validateNewEdgeLocked(ctx, edge, resolve, batch, minted)
		if err != nil {
			return err
		}
		labels[i] = [2][]string{startLabels, endLabels}
		noOps[i] = noOp
		if batch != nil {
			batch[edge.ID] = struct{}{}
		}
	}
	for i, edge := range edges {
		if noOps[i] {
			tx.stageHiddenEdgeCreateLocked(edge)
			continue
		}
		if err := tx.bufferNewEdgeLocked(edge, labels[i][0], labels[i][1], minted); err != nil {
			return err
		}
	}
	return nil
}

// validateNewEdgeLocked checks that edge can be created in the transaction
// and returns its endpoints' labels and whether it is a hidden-relationship
// no-op candidate: it is not nil and has an ID, its
// namespace pins, both endpoints are visible (endpoint; ErrNotFound
// otherwise), and no pending edge, edge earlier in the same batch or
// committed edge has its ID (ErrAlreadyExists). Storage is read for every ID
// but a minted one (CreateMintedEdge): stable imports may use UUID-shaped IDs
// too, so an ID's shape proves nothing.
func (tx *BadgerTransaction) validateNewEdgeLocked(ctx context.Context, edge *Edge, endpoint func(context.Context, NodeID) ([]string, bool, error), batch map[EdgeID]struct{}, minted bool) (startLabels, endLabels []string, noOp bool, err error) {
	// A nil edge is invalid data and an empty ID an invalid ID, as in the
	// engines' CreateEdge.
	if edge == nil {
		return nil, nil, false, ErrInvalidData
	}
	if edge.ID == "" {
		return nil, nil, false, ErrInvalidID
	}
	if err := tx.pinEdgeNamespaceLocked(edge); err != nil {
		return nil, nil, false, err
	}
	if _, exists := tx.pendingEdges[edge.ID]; exists {
		return nil, nil, false, ErrAlreadyExists
	}
	if _, exists := batch[edge.ID]; exists {
		return nil, nil, false, ErrAlreadyExists
	}
	if _, deleted := tx.deletedEdges[edge.ID]; deleted || minted {
		startLabels, visible, err := endpoint(ctx, edge.StartNode)
		if err != nil {
			return nil, nil, false, err
		}
		if !visible {
			return nil, nil, false, localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
		}
		endLabels, visible, err := endpoint(ctx, edge.EndNode)
		if err != nil {
			return nil, nil, false, err
		}
		if !visible {
			return nil, nil, false, localizedError(localization.StorageTransactionEndNodeMissing(string(edge.EndNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.EndNode)))
		}
		return startLabels, endLabels, false, nil
	}
	existing, err := tx.getCommittedEdgeLocked(ctx, edge.ID)
	if err == nil {
		if !tx.engine.filterEntityByDecay(visibleRead, existing, DecayScoringTime()) {
			return nil, nil, false, ErrAlreadyExists
		}
		// A MERGE may add ON CREATE SET properties after CreateEdge. Defer
		// semantic identity to the commit owner, which sees that final staged
		// image. Until then this is a transaction-visible candidate only.
		return nil, nil, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, nil, false, fmt.Errorf("checking edge existence: %w", err)
	}
	startLabels, visible, err := endpoint(ctx, edge.StartNode)
	if err != nil {
		return nil, nil, false, err
	}
	if !visible {
		return nil, nil, false, localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
	}
	endLabels, visible, err = endpoint(ctx, edge.EndNode)
	if err != nil {
		return nil, nil, false, err
	}
	if !visible {
		return nil, nil, false, localizedError(localization.StorageTransactionEndNodeMissing(string(edge.EndNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.EndNode)))
	}
	return startLabels, endLabels, false, nil
}

// stageHiddenEdgeCreateLocked stores a hidden-ID CREATE candidate outside the
// ordinary pendingEdges overlay. Later writes address it by ID through
// UpdateEdge, while MATCH, MERGE matching and transaction reads continue to
// see the predecessor as hidden. Commit compares the final candidate with the
// stored predecessor.
func (tx *BadgerTransaction) stageHiddenEdgeCreateLocked(edge *Edge) {
	if tx.edgeCreateNoOps == nil {
		// Allocated on first use: ordinary transactions never stage one.
		tx.edgeCreateNoOps = make(map[EdgeID]*Edge, 1)
	}
	candidate := copyEdge(edge)
	tx.edgeCreateNoOps[edge.ID] = candidate
}

// sameEdgeCreateIdentityLocked is the product identity of an idempotent
// hidden-edge CREATE. ID equality is established by the conflict lookup. Type,
// endpoints and user properties are semantic. CreatedAt/UpdatedAt are write
// bookkeeping; Confidence, AutoGenerated and VisibilitySuppressed are
// derived/internal bookkeeping and are deliberately excluded so
// retry-generated metadata cannot turn an otherwise identical import into a
// replacement. stored is a decoded body, read from storage or from the
// write-behind overlay, which holds the same image, so the candidate's
// properties are compared as the codec would commit them
// (codecEdgeImageLocked): the same values must not differ only in their Go
// representation, such as a generic list against the typed slice storage
// decodes.
func (tx *BadgerTransaction) sameEdgeCreateIdentityLocked(stored, candidate *Edge) (bool, error) {
	if !sameEdgeIdentity(stored, candidate) {
		return false, nil
	}
	// nil and an allocated empty map are the same empty Neo4j property set.
	if len(stored.Properties) == 0 && len(candidate.Properties) == 0 {
		return true, nil
	}
	image, err := tx.codecEdgeImageLocked(candidate)
	if err != nil {
		return false, err
	}
	return (len(stored.Properties) == 0 && len(image.Properties) == 0) ||
		reflect.DeepEqual(stored.Properties, image.Properties), nil
}

// sameStoredEdgeIdentity compares decoded user images. Storage-normalized
// numeric/list values share the existing equality owner; generated timestamps
// and internal metadata do not turn a no-change property write into a conflict.
func sameStoredEdgeIdentity(before, current *Edge) bool {
	return sameEdgeIdentity(before, current) && sameStoredValue(before.Properties, current.Properties)
}

// sameEdgeIdentity owns the relationship kind/endpoints comparison shared by
// creation identity and mutation predecessor validation. Each caller retains
// its existing property-equality contract.
func sameEdgeIdentity(before, current *Edge) bool {
	return before != nil && current != nil && before.Type == current.Type &&
		before.StartNode == current.StartNode && before.EndNode == current.EndNode
}

// bufferNewEdgeLocked buffers a validated new edge: its record, its
// outgoing, incoming, type and edge-between index entries, the positional
// counters for its endpoints' labels (as validateNewEdgeLocked returned
// them), the read-your-writes copy and the create operation.
func (tx *BadgerTransaction) bufferNewEdgeLocked(edge *Edge, startLabels, endLabels []string, minted bool) error {
	if !tx.acceptedApplication {
		initializeCreationTimes(&edge.CreatedAt, &edge.UpdatedAt)
	}
	tx.engine.NotePropertyKeysInNamespace(namespaceForEdgeID(edge.ID), edge.Properties)
	delete(tx.cancelledEdgeWrites, edge.ID)
	_, recreated := tx.deletedEdges[edge.ID]
	if !recreated && len(edge.Properties) == 0 && !tx.engine.GetSchema().HasWriteRules() {
		if tx.deferredEdgeWrites == nil {
			tx.deferredEdgeWrites = make(map[EdgeID]struct{})
		}
		tx.deferredEdgeWrites[edge.ID] = struct{}{}
	} else if err := tx.bufferEdgePhysicalWritesLocked(edge); err != nil {
		return err
	}
	// Derived per-type counter follows the index entry (issue #638).
	tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), edge.Type, 1)
	// Positional (label, type) counters follow the endpoints' tx-visible labels.
	tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), edge.Type, startLabels, endLabels, 1)

	// Track for read-your-writes
	edgeCopy := copyEdge(edge)
	delete(tx.deletedEdges, edge.ID)
	tx.setPendingEdgeLocked(edgeCopy)
	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
		// As for nodes, only an ID this transaction minted skips the commit
		// head reload; a caller-supplied ID keeps it whatever its shape.
		FreshID: tx.skipCreateExistenceCheck && minted,
	})
	return nil
}

// UpdateEdge updates an existing edge within the transaction.
//
// This is required so Cypher can do CREATE ... SET r.prop = ... in a single query
// while using implicit/explicit transactions (writes must remain isolated until commit).
//
// Error contract:
//   - ErrNotFound: the edge does not exist at this transaction's snapshot
//     and is not live at latest-committed state either (never existed, or a
//     peer deleted it).
//   - ErrConflict ("edge <id> changed after transaction start"): the edge is
//     live at latest-committed state but was committed AFTER this
//     transaction began, so this transaction cannot update it. This is the
//     same retryable conflict shape checkEdgeWriteConflict produces at
//     commit time; callers (and Bolt clients, via
//     Neo.TransientError.Transaction.Outdated) should retry on a fresh
//     snapshot.
func (tx *BadgerTransaction) UpdateEdge(ctx context.Context, updateRequest EdgeUpdateRequest) error {
	edge := updateRequest.After
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}
	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}
	if err := tx.materializeDeferredEdgeLocked(edge.ID); err != nil {
		return err
	}
	if err := tx.pinEdgeNamespaceLocked(edge); err != nil {
		return err
	}
	if _, deleted := tx.deletedEdges[edge.ID]; deleted {
		return ErrNotFound
	}
	if _, hiddenCreate := tx.edgeCreateNoOps[edge.ID]; hiddenCreate {
		// ON CREATE SET and later writes update only the candidate. If its final
		// semantic image differs, commit returns the permanent hidden-ID
		// conflict; if it is identical, no physical update is published.
		candidate := copyEdge(edge)
		tx.edgeCreateNoOps[edge.ID] = candidate
		return nil
	}
	tx.engine.NotePropertyKeysInNamespace(tx.namespace, edge.Properties)

	// Load existing edge (pending or committed) for index maintenance.
	var oldEdge *Edge
	if pending, exists := tx.pendingEdges[edge.ID]; exists {
		oldEdge = copyEdge(pending)
	} else {
		var err error
		oldEdge, err = tx.getCommittedEdgeForUpdateLocked(ctx, edge.ID)
		if err == ErrNotFound {
			if updateRequest.Before != nil {
				return localizedError(localization.StorageTransactionEdgeChanged(string(edge.ID)), ErrConflict)
			}
			return tx.missingWriteTargetLocked(bufferedEdgeEntity, string(edge.ID))
		}
		if errors.Is(err, ErrConflict) {
			return err
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
		if err := tx.enrollCommittedWriteLocked(edgeKey(edge.ID), "edge"); err != nil {
			return err
		}
		// Only the first mutation compares the caller's observation with the
		// committed predecessor. Later edits may reuse that same observation;
		// their own pending image is not an independently committed peer.
		if updateRequest.Before != nil && !sameStoredEdgeIdentity(updateRequest.Before, oldEdge) {
			return localizedError(localization.StorageTransactionEdgeChanged(string(edge.ID)), ErrConflict)
		}
	}

	// If endpoints changed, verify they exist and update outgoing/incoming indexes.
	if oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		if _, visible, err := tx.visibleNodeLabelsLocked(ctx, edge.StartNode); err != nil {
			return err
		} else if !visible {
			return localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
		}
		if _, visible, err := tx.visibleNodeLabelsLocked(ctx, edge.EndNode); err != nil {
			return err
		} else if !visible {
			return localizedError(localization.StorageTransactionEndNodeMissing(string(edge.EndNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.EndNode)))
		}

		if oldOutKey := tx.engine.outgoingIndexKeyStringLookup(oldEdge.StartNode, edge.ID); oldOutKey != nil {
			tx.bufferDelete(oldOutKey)
		}
		if oldInKey := tx.engine.incomingIndexKeyStringLookup(oldEdge.EndNode, edge.ID); oldInKey != nil {
			tx.bufferDelete(oldInKey)
		}
		tx.bufferDeleteEdgeBetweenIndexes(oldEdge)
		newOutKey, err := tx.engine.outgoingIndexKeyString(tx.staged(), edge.StartNode, edge.ID)
		if err != nil {
			return fmt.Errorf("outgoing index: %w", err)
		}
		tx.bufferSet(newOutKey, []byte{})
		newInKey, err := tx.engine.incomingIndexKeyString(tx.staged(), edge.EndNode, edge.ID)
		if err != nil {
			return fmt.Errorf("incoming index: %w", err)
		}
		tx.bufferSet(newInKey, []byte{})
		if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
			return fmt.Errorf("edge-between index: %w", err)
		}
	}

	// If type changed, update edge type index.
	if oldEdge.Type != edge.Type {
		if oldEdge.Type != "" {
			if oldTypeKey := tx.engine.edgeTypeIndexKeyStringLookup(oldEdge.Type, edge.ID); oldTypeKey != nil {
				tx.bufferDelete(oldTypeKey)
			}
		}
		if oldEdge.StartNode == edge.StartNode && oldEdge.EndNode == edge.EndNode {
			tx.bufferDeleteEdgeBetweenIndexes(oldEdge)
		}
		if edge.Type != "" {
			newTypeKey, err := tx.engine.edgeTypeIndexKeyString(tx.staged(), edge.Type, edge.ID)
			if err != nil {
				return fmt.Errorf("edge type index: %w", err)
			}
			tx.bufferSet(newTypeKey, []byte{})
		}
		if oldEdge.StartNode == edge.StartNode && oldEdge.EndNode == edge.EndNode {
			if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
				return fmt.Errorf("edge-between index: %w", err)
			}
		}
	}

	// Serialize and buffer updated edge record.
	edgeBytes, err := tx.engine.encodeEdgeInTxn(tx.staged(), namespaceForEdgeID(edge.ID), edge)
	if err != nil {
		return localizedError(localization.StorageClientEdgeEncodeFailed(err), err)
	}
	tx.bufferSet(edgeKey(edge.ID), edgeBytes)
	// The adjacency entries carry the record's header.
	header := adjacencyValueFromRecord(edgeBytes)
	if outKey := tx.engine.outgoingIndexKeyStringLookup(edge.StartNode, edge.ID); outKey != nil {
		tx.bufferSet(outKey, header)
	}
	if inKey := tx.engine.incomingIndexKeyStringLookup(edge.EndNode, edge.ID); inKey != nil {
		tx.bufferSet(inKey, header)
	}

	// Track for read-your-writes.
	edgeCopy := copyEdge(edge)
	tx.setPendingEdgeLocked(edgeCopy)
	if err := tx.bufferEdgeReplacementCountDeltasLocked(ctx, oldEdge, edge); err != nil {
		return err
	}

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
		OldEdge:   oldEdge,
	})

	return nil
}

// bufferEdgeReplacementCountDeltasLocked owns typed and positional counter
// changes for an UPDATE that changes the stored relationship image.
// An unchanged type and endpoints contribute no delta.
func (tx *BadgerTransaction) bufferEdgeReplacementCountDeltasLocked(ctx context.Context, oldEdge, edge *Edge) error {
	if oldEdge.Type != edge.Type {
		tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), oldEdge.Type, -1)
		tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), edge.Type, 1)
	}
	// Positional (label, type) counters move with type or endpoint changes.
	if oldEdge.Type != edge.Type || oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		oldStartLabels, err := tx.nodeLabelsTxVisibleLocked(ctx, oldEdge.StartNode)
		if err != nil {
			return fmt.Errorf("old start labels: %w", err)
		}
		oldEndLabels, err := tx.nodeLabelsTxVisibleLocked(ctx, oldEdge.EndNode)
		if err != nil {
			return fmt.Errorf("old end labels: %w", err)
		}
		tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), oldEdge.Type, oldStartLabels, oldEndLabels, -1)
		newStartLabels := oldStartLabels
		newEndLabels := oldEndLabels
		if oldEdge.StartNode != edge.StartNode {
			newStartLabels, err = tx.nodeLabelsTxVisibleLocked(ctx, edge.StartNode)
			if err != nil {
				return fmt.Errorf("new start labels: %w", err)
			}
		}
		if oldEdge.EndNode != edge.EndNode {
			newEndLabels, err = tx.nodeLabelsTxVisibleLocked(ctx, edge.EndNode)
			if err != nil {
				return fmt.Errorf("new end labels: %w", err)
			}
		}
		tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), edge.Type, newStartLabels, newEndLabels, 1)
	}

	return nil
}

// DeleteEdge deletes an edge from the transaction.
func (tx *BadgerTransaction) DeleteEdge(ctx context.Context, edgeID EdgeID) error {
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if edgeID == "" {
		return ErrInvalidID
	}
	if err := tx.pinNamespaceFromIDLocked(string(edgeID)); err != nil {
		return err
	}
	// A relationship this transaction already deleted, itself or with its
	// node, is gone: deleting it again changes nothing (#827).
	if _, deleted := tx.deletedEdges[edgeID]; deleted {
		return ErrNotFound
	}
	if _, hiddenCreate := tx.edgeCreateNoOps[edgeID]; hiddenCreate {
		// CREATE followed by DELETE has no physical effect on its hidden
		// predecessor. Remove only the transaction-visible candidate.
		delete(tx.edgeCreateNoOps, edgeID)
		return nil
	}

	// Get edge to delete its indexes
	var edge *Edge
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		edge = pending
	} else {
		var err error
		edge, err = tx.getCommittedEdgeLocked(ctx, edgeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
		if err := tx.enrollCommittedWriteLocked(edgeKey(edgeID), "edge"); err != nil {
			return err
		}
	}

	if _, deferred := tx.deferredEdgeWrites[edgeID]; deferred {
		delete(tx.deferredEdgeWrites, edgeID)
		if tx.cancelledEdgeWrites == nil {
			tx.cancelledEdgeWrites = make(map[EdgeID]struct{})
		}
		tx.cancelledEdgeWrites[edgeID] = struct{}{}
	} else {
		tx.bufferDelete(edgeKey(edgeID))
		if outKey := tx.engine.outgoingIndexKeyStringLookup(edge.StartNode, edgeID); outKey != nil {
			tx.bufferDelete(outKey)
		}
		if inKey := tx.engine.incomingIndexKeyStringLookup(edge.EndNode, edgeID); inKey != nil {
			tx.bufferDelete(inKey)
		}
		if typeKey := tx.engine.edgeTypeIndexKeyStringLookup(edge.Type, edgeID); typeKey != nil {
			tx.bufferDelete(typeKey)
		}
		tx.bufferDeleteEdgeBetweenIndexes(edge)
	}

	// Track deletion
	tx.removePendingEdgeLocked(edgeID)
	tx.deletedEdges[edgeID] = struct{}{}

	// Derived per-type counter follows the index entry removal (issue #638).
	tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edgeID), edge.Type, -1)

	// Positional (label, type) counters follow the endpoints' tx-visible
	// labels read before the edge is removed.
	startLabels, err := tx.nodeLabelsTxVisibleLocked(ctx, edge.StartNode)
	if err != nil {
		return fmt.Errorf("start labels: %w", err)
	}
	endLabels, err := tx.nodeLabelsTxVisibleLocked(ctx, edge.EndNode)
	if err != nil {
		return fmt.Errorf("end labels: %w", err)
	}
	tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edgeID), edge.Type, startLabels, endLabels, -1)

	tx.operations = append(tx.operations, Operation{
		Type:      OpDeleteEdge,
		Timestamp: time.Now(),
		EdgeID:    edgeID,
		OldEdge:   edge,
	})

	return nil
}

// A long-lived explicit transaction may execute many unrelated projections.
// Keep the snapshot replay optimization bounded so retained decoded rows cannot
// grow with the number of distinct query shapes.
const maxSnapshotProjectedLabelStreams = 8

// Commit applies all changes atomically with full constraint validation.
// Explicit transactions get strict ACID durability with immediate fsync.
// CommitContext is Commit, cancellable by ctx while the transaction is
// validated: if ctx is cancelled (TERMINATE TRANSACTIONS, a disconnected
// client) before the writes are applied, the commit fails with ctx's error
// and nothing is stored. Once the writes are being applied the commit runs to
// completion, so a commit is never partially applied.
func (tx *BadgerTransaction) CommitContext(ctx context.Context) error {
	return tx.PublishCommit(ctx, CommitPublication{})
}

// commitCancelPollEvery is how many nodes commit-time validation checks
// between polls of the commit context.
const commitCancelPollEvery = 256

// Commit is the context-free commit: it has no caller to cancel it, so its
// own lifetime (context.Background) is the boundary commit-time validation
// reads run on. CommitContext carries the caller's context instead.
func (tx *BadgerTransaction) Commit() error {
	return tx.PublishCommit(context.Background(), CommitPublication{})
}

// commitConflictError reports a Badger write conflict found while committing
// in the storage commit-conflict shape (ErrConflict, localized).
func commitConflictError(err error) error {
	cause := fmt.Errorf("%w: concurrent transaction modified data before commit: %w", ErrConflict, err)
	return localizedError(localization.StorageTransactionCommitConflict(err), cause)
}

// Rollback discards all changes.
func (tx *BadgerTransaction) Rollback() error {
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return err
	}
	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.closeLocked(TxStatusRolledBack, true, nil)
	return nil
}

// SetMetadata sets transaction metadata (same as Transaction).
func (tx *BadgerTransaction) SetMetadata(metadata map[string]interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}

	// Validate size
	totalSize := 0
	for k, v := range metadata {
		totalSize += len(k)
		if v != nil {
			totalSize += len(fmt.Sprint(v))
		}
	}

	if totalSize > 2048 {
		return localizedError(localization.StorageTransactionMetadataTooLarge(totalSize, 2048), nil)
	}

	// Merge through the one per-entry ownership owner. Each merged value is
	// owned here, so the retained map is the image CDC encodes later.
	tx.Metadata = ownedEntries(tx.Metadata, metadata)

	return nil
}

// GetMetadata returns transaction metadata copy.
func (tx *BadgerTransaction) GetMetadata() map[string]interface{} {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Always an independent, non-nil image: the caller can neither reach nor
	// replace retained metadata state.
	return ownedEntries(nil, tx.Metadata)
}

// OperationCount returns the number of buffered operations.
func (tx *BadgerTransaction) OperationCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	// A RejectConnected deletion left for the commit to finish counts:
	// the commit deletes the node or fails.
	return len(tx.operations) + len(tx.connectedDeletes)
}

// RequiresCommitValidation reports whether a transaction with no physical
// operations still has a staged decision that only the commit owner can make.
// Hidden-edge CREATE candidates are deliberately operation-free until their
// final image is compared, so implicit transactions must not discard them as
// empty before validateSnapshotIsolationConflicts runs.
func (tx *BadgerTransaction) RequiresCommitValidation() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.edgeCreateNoOps) > 0
}

// AcknowledgedBuffered reports whether the commit was acknowledged through
// the write-behind buffer rather than made durable. The executor's implicit
// path reads it after Commit to decide whether durable receipt metadata
// applies; the WAL begin/commit markers still run, only the receipt is
// suppressed on the eventual-consistency ACK.
func (tx *BadgerTransaction) AcknowledgedBuffered() bool {
	return tx.bufferedAck.Load()
}

// uniqueConstraintScanHook lets storage tests assert that indexed UNIQUE
// validation does not regress to the label-scan fallback.
var uniqueConstraintScanHook func()
var uniqueConstraintScanHookMu sync.RWMutex

// commitTailHook lets storage tests interpose between a successful Badger
// commit and the post-commit tail (derived counts, MVCC sequence, ID
// counters, caches, callbacks) that publishes it. It exists to pin the
// Close-versus-commit ordering; production leaves it nil.
var (
	commitTailHook   func()
	commitTailHookMu sync.RWMutex
)

// writeBehindKeyCandidatesLocked returns the newest buffered version of the
// unflushed nodes with label whose property equals value: the nodes an
// index-backed constraint check must compare besides the index, which is
// maintained at flush. nil without write-behind. It reads the buffer's pending
// value index, so the cost is the matching nodes, not the label's unflushed
// rows; a value without an index key falls back to the label's whole overlay.
// Callers compare the full key / interval themselves.

// PrepareMergeKey readies the transaction for a MERGE that looks up the node
// whose key under label's UNIQUE constraint on property is value, so that
// concurrent MERGEs of one key behave as in Neo4j: one creates the node and
// the others match it, without an error or a retry (#961).
//
// It applies to every MERGE of the transaction, before or after it has read
// or written, and to any number of keys (a MERGE per UNWIND row):
//   - when the key's node was committed after the transaction's snapshot,
//     the MERGE can't see it, so it must not create the key again
//     (catchUpToNode): a transaction that hasn't read through its snapshot
//     or written anything moves the snapshot forward to now, so the MERGE
//     sees the node and everything committed with it, and matches; any
//     other fails with ErrConflict, a transient error, and its retry
//     matches the node;
//   - when the key has no node, the transaction locks the key until it ends,
//     so a concurrent MERGE of the key waits for it to finish; a node
//     committed while this one waited is handled as above.
//
// A wait ends with ErrDeadlock, also transient, when it would close a cycle
// of transactions waiting for each other, with ctx's error, or, after the
// server's transaction timeout (EngineOptions.LockWait), with
// ErrLockWaitTimeout.
func (tx *BadgerTransaction) PrepareMergeKey(ctx context.Context, label, property string, value interface{}) error {
	return tx.PrepareMergeNode(ctx, []string{label}, map[string]interface{}{property: value})
}

// ReleaseMergeKeys releases the MERGE key locks the transaction holds
// (PrepareMergeKey). A transaction a statement failed in can no longer
// commit, so its keys protect nothing, and other MERGEs of them must not
// wait until the client rolls it back.
func (tx *BadgerTransaction) ReleaseMergeKeys() {
	drainReleaseCallbacks(&tx.mu, &tx.mergeKeyReleases)
}

// catchUpToNode readies the transaction for a MERGE that will look up node
// nodeID. When the node was committed after the snapshot, a transaction that
// has observed nothing yet moves its snapshot forward; one that has fails
// with ErrConflict, since its MERGE would miss the node and create the key
// again.
func (tx *BadgerTransaction) catchUpToNode(ctx context.Context, nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(ctx, nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.Status != TxStatusActive {
		return nil
	}
	newer, err := tx.snapshotHeadConflict(bufferedNodeEntity, string(nodeID), head.Version)
	if err != nil || !newer {
		return err
	}
	if tx.observedLocked() {
		// Wire contract: substrings "conflict:" and "changed after transaction start" are
		// matched by downstream Bolt classifiers as transient.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.2.
		return localizedError(localization.StorageTransactionNodeChanged(string(nodeID)), ErrConflict)
	}
	return tx.advanceSnapshotLocked(ctx, false)
}

// observedLocked reports whether the transaction has read through its
// snapshot or written anything, so its snapshot can no longer move.
func (tx *BadgerTransaction) observedLocked() bool {
	return tx.observed || len(tx.operations) > 0 || len(tx.pendingWrites) > 0 || len(tx.pendingDeletes) > 0 ||
		len(tx.pendingNodes) > 0 || len(tx.pendingEdges) > 0 || len(tx.deletedNodes) > 0 || len(tx.deletedEdges) > 0
}

// replayedSinceBeginLocked reports whether a write-behind generation may have
// been replayed into Badger since the transaction's Badger snapshot: the
// buffer held one then and has changed since. A replay of a commit the
// transaction already saw through the buffer lands after its snapshot like
// one its own drain applied.
func (tx *BadgerTransaction) replayedSinceBeginLocked() bool {
	w := tx.engine.writeBehind
	return w != nil && tx.bufferEpoch&1 == 1 && w.readEpoch.Load() != tx.bufferEpoch
}

// rebaseBadgerAfterDrainLocked moves a committing transaction onto Badger
// transactions opened after a write-behind drain (orderAfterBufferedCommits)
// or replay (replayedSinceBeginLocked):
// the drained commits landed after its Badger snapshot, so its commit would
// read around them and Badger would report their keys as concurrent writes.
// The MVCC snapshot (readTS) stays its isolation anchor, authoritative under
// write-behind because the flusher backdates its heads to their
// acknowledgement (snapshotHeadConflict); its conflicts are checked again
// against the drained heads, so a commit acknowledged after this
// transaction began still conflicts. Staged ID and property-key counters
// follow the writer transaction.
func (tx *BadgerTransaction) rebaseBadgerAfterDrainLocked(ctx context.Context) error {
	snapshotTx, badgerReadTs, err := tx.badgerDB.beginTxn(false)
	if err != nil {
		return err
	}
	next := tx.badgerDB.newTxnAt(badgerReadTs, true)
	if tx.engine.idDict != nil {
		tx.engine.idDict.moveTxnCounters(tx.badgerTx, next)
	}
	if tx.engine.propKeyDict != nil {
		tx.engine.propKeyDict.moveTxnCounters(tx.badgerTx, next)
	}
	tx.badgerTx.Discard()
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
	}
	if tx.badgerReadHeld {
		tx.badgerDB.endRead(tx.badgerReadTs)
	}
	tx.snapshotTx = snapshotTx
	tx.badgerReadTs = badgerReadTs
	tx.badgerReadHeld = true
	tx.badgerTx = next
	tx.clearSnapshotCachesLocked()
	return tx.validateSnapshotIsolationConflicts(ctx)
}

// ConstraintViolationError is returned when a constraint is violated.
//
// Cause is set when the violation is detected at a non-validation layer
// — currently used by the snapshot-isolation check when a MERGE's
// UpdateNode hits a peer-committed node that already carries the
// matching unique value. Surfacing the underlying ErrConflict via
// Unwrap keeps errors.Is(err, ErrConflict) and the transient classifier
// working so retry-aware drivers still see the transient sentinel even
// though the visible message is the consumer-pinned constraint shape.
type ConstraintViolationError struct {
	Type       ConstraintType
	Label      string
	Properties []string
	Message    string
	Cause      error
}

func (e *ConstraintViolationError) Error() string {
	return fmt.Sprintf("Constraint violation (%s on %s.%v): %s",
		e.Type, e.Label, e.Properties, e.Message)
}

func (e *ConstraintViolationError) Unwrap() error {
	return e.Cause
}

func (tx *BadgerTransaction) releaseMergeAdmissionLocked() {
	for _, release := range tx.mergeKeyReleases {
		release()
	}
	tx.mergeKeyReleases = nil
}

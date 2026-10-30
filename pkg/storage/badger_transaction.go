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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
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
	clientDetached                 atomic.Bool
	commitLogicalID                string
	commitReleaseAcceptedAuthority func()
	mu                             sync.Mutex

	// writeAdmission decides whether the transaction may write, before its
	// first write (SetWriteAdmission); nil once it admitted, or without one.
	writeAdmission func() error

	// commitCtx is the caller's context while CommitContext runs: commit-time
	// constraint validation polls it, so a terminated or abandoned statement
	// stops before anything is written instead of validating to the end
	// (#1115). Nil for Commit.
	commitCtx context.Context

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
func (b *BadgerEngine) BeginTransaction() (*BadgerTransaction, error) {
	b.mu.RLock()
	if b.closed {
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
	snapshotTx, badgerReadTs, beginPresence, err := b.beginTransactionSnapshot(badgerDB)
	if err != nil {
		return nil, err
	}
	readTS := b.currentMVCCReadVersion("")
	beginSnapshot := b.snapshotNamespaceVersions()
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
	startTime := time.Now()
	badgerTx := badgerDB.newTxnAt(badgerReadTs, true)

	return &BadgerTransaction{
		ID:                      txID,
		StartTime:               startTime,
		Status:                  TxStatusActive,
		readTS:                  readTS,
		beginSnapshot:           beginSnapshot,
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
	}, nil
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
	if tx.clientDetached.Load() {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.Status == TxStatusActive
}

func (tx *BadgerTransaction) closeLocked(status TransactionStatus, discard bool, closedErr error) {
	tx.preparedEntityCommit = nil
	tx.beginPresence.release()
	tx.beginPresence = nil
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

// ensureOperationAllowedLocked owns lifecycle and operation admission. Only an
// accepted commit continuation may proceed after client detachment. Data and
// schema writes retain their exclusion and claim a writer before staging.
func (tx *BadgerTransaction) ensureOperationAllowedLocked(operation transactionOperation) error {
	if operation != transactionCoordinatorCommit && tx.clientDetached.Load() {
		return ErrTransactionClosed
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
func (tx *BadgerTransaction) flushBufferedWrites(w *batchWriter) error {
	// Apply deletes first (in case a key is both written and deleted, delete wins)
	for keyStr := range tx.pendingDeletes {
		key := []byte(keyStr)
		if err := w.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
			return fmt.Errorf("flushing delete for key %s: %w", keyStr, err)
		}
	}

	// Apply writes (only keys that weren't deleted)
	for keyStr, value := range tx.pendingWrites {
		// Skip if this key was also deleted (delete wins)
		if tx.pendingDeletes[keyStr] {
			continue
		}
		key := []byte(keyStr)
		if err := w.write(func(txn *badger.Txn) error {
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
func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {
	return tx.createNode(node, false)
}

// CreateMintedNode is CreateNode for a node whose ID the caller minted for
// this create from a random UUID, as Cypher CREATE and MERGE do. No stored
// entity, visible or hidden, can hold such an ID, so storage is not read for
// one. A stable or imported ID, UUID-shaped or not, takes CreateNode.
func (tx *BadgerTransaction) CreateMintedNode(node *Node) (NodeID, error) {
	return tx.createNode(node, true)
}

func (tx *BadgerTransaction) createNode(node *Node, minted bool) (NodeID, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
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

	// Validate constraints BEFORE writing
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
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
		_, err := tx.getCommittedNodeLocked(node.ID)
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
	initializeCreationTimes(&node.CreatedAt, &node.UpdatedAt)
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
		// FreshID propagates the "caller asserts this ID is new and cannot
		// collide with a tombstoned MVCC head" contract into the commit
		// loop so it can skip the head-load round-trip. Gated on the legacy
		// UUID-shape heuristic.
		// The stored-inclusive point read above is skipped only for a minted
		// ID, so a stable imported UUID cannot bypass a hidden predecessor.
		// Once the ID is known absent, retain the generated-ID commit fast path.
		FreshID: tx.skipCreateExistenceCheck && shouldSkipCreateExistenceCheck(string(node.ID)),
	})

	return node.ID, nil
}

// UpdateGraphNode stages a node using the supplied image interpretation.
// ClientNodeEdits reconciles managed embeddings; StoredNodeImages installs a
// complete authoritative image. Policies may differ between operations in one frame.
func (tx *BadgerTransaction) UpdateGraphNode(updateRequest NodeUpdateRequest) error {
	node, policy := updateRequest.After, updateRequest.Policy
	if policy > StoredNodeImages {
		return ErrInvalidData
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
		return err
	}

	if node == nil || node.ID == "" {
		return ErrInvalidID
	}
	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}
	tx.engine.NotePropertyKeysInNamespace(tx.namespace, node.Properties)

	// Validate constraints
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
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
			oldNode, err = tx.getCommittedNodeReadLocked(node.ID, true, storedRead)
		} else {
			oldNode, err = tx.getCommittedNodeLocked(node.ID)
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

	// Validate policy constraints on label changes.
	if !tx.deferConstraintValidation && !labelsEqual(node.Labels, oldNode.Labels) {
		if err := tx.validatePolicyOnNodeLabelChange(node, oldNode); err != nil {
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
	if err := tx.bufferNodeLabelChangeEdgeTypeDeltasLocked(node.ID, added, removed); err != nil {
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
func (tx *BadgerTransaction) incidentEdgesLocked(nodeID NodeID) ([]*Edge, error) {
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
	for edgeID, edge := range tx.pendingAdjacencyLocked(nodeID) {
		if _, deleted := tx.deletedEdges[edgeID]; !deleted {
			include(edge)
		}
	}

	// Buffered transactions read committed state through the same capture
	// owner as engine adjacency. Overlay selection and committed IDs must
	// not straddle replay/retirement. Transaction-local rewrites and deletes
	// still take precedence over that captured stored view.
	if tx.engine.writeBehind != nil {
		outgoing, incoming, err := readAdjacency(tx.engine, adjacencyBoth, nodeID, "", nil, storedRead)
		if err != nil {
			return nil, err
		}
		for _, adjacent := range [...][]*Edge{outgoing, incoming} {
			for _, edge := range adjacent {
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
	err := tx.withSnapshotViewLocked(func(view *badger.Txn) error {
		return committed(view, tx.badgerTx)
	})
	return edges, err
}

// bufferRemoveEdgeBookkeepingBuffered buffers an edge's primary-key and index
// deletions, marks it deleted, removes it from the pending set, and adjusts the
// per-type and positional (label, type) counters, for each relationship a
// node delete cascades (incidentEdgesLocked): committed, buffered or staged
// (#741) alike.
func (tx *BadgerTransaction) bufferRemoveEdgeBookkeepingBuffered(edge *Edge, deletedNodeID NodeID, deletedNodeLabels []string) error {
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
		peer, err := tx.nodeLabelsTxVisibleLocked(edge.EndNode)
		if err != nil {
			return err
		}
		endLabels = peer
	} else if edge.EndNode == deletedNodeID && edge.StartNode != deletedNodeID {
		peer, err := tx.nodeLabelsTxVisibleLocked(edge.StartNode)
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
//	tx.DeleteGraphNode(id, CascadeEdges) // DETACH DELETE
//	tx.DeleteGraphNode(id, RejectConnected) // DELETE
func (tx *BadgerTransaction) DeleteGraphNode(nodeID NodeID, mode NodeDeletionMode) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
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
	if err := tx.materializeDeferredEdgesLocked(); err != nil {
		return err
	}

	// Capture old node state for constraint bookkeeping (e.g., unique value unregister).
	var oldNode *Node
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		oldNode = copyNode(pending)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(nodeID)
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
	result, err := tx.engine.executeNodeDeletion(tx.staged(), nodeID, oldNode)
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
func (tx *BadgerTransaction) IncidentEdges(nodeID NodeID) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(nodeID)); err != nil {
		return nil, err
	}
	edges, err := tx.incidentEdgesLocked(nodeID)
	if err != nil {
		return nil, err
	}
	for i, edge := range edges {
		edges[i] = copyEdge(edge)
	}
	return edges, nil
}

// resolveConnectedDeletesLocked finishes the deletes of DeleteGraphNode with RejectConnected at
// commit: a node no relationship connects any more is deleted for good; one
// that is still connected fails the commit, as Neo4j's DeleteConnectedNode
// constraint does. Nodes are checked in ID order, so the error names the
// same node every time.
func (tx *BadgerTransaction) resolveConnectedDeletesLocked() error {
	if len(tx.connectedDeletes) == 0 {
		return nil
	}
	ids := make([]NodeID, 0, len(tx.connectedDeletes))
	for id := range tx.connectedDeletes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		// Every relationship counts, hidden ones too: the delete would
		// otherwise remove them with the node (incidentEdgesLocked).
		remaining, err := tx.incidentEdgesLocked(id)
		if err != nil {
			return err
		}
		if len(remaining) > 0 {
			return &NodeStillConnectedError{NodeID: id, Namespace: tx.namespace}
		}
	}
	for _, id := range ids {
		oldNode := tx.connectedDeletes[id]
		delete(tx.deletedNodes, id)
		result, err := tx.engine.executeNodeDeletion(tx.staged(), id, oldNode)
		if err != nil {
			return err
		}
		tx.recordNodeDeletionLocked(id, result)
	}
	tx.connectedDeletes = nil
	return nil
}

// CreateEdge adds an edge to the transaction.
func (tx *BadgerTransaction) CreateEdge(edge *Edge) error {
	return tx.bulkCreateEdges([]*Edge{edge}, false)
}

// CreateMintedEdge is CreateEdge for a relationship whose ID the caller
// minted for this create, as CreateMintedNode is for a node.
func (tx *BadgerTransaction) CreateMintedEdge(edge *Edge) error {
	return tx.bulkCreateEdges([]*Edge{edge}, true)
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
func (tx *BadgerTransaction) BulkCreateEdges(edges []*Edge) error {
	return tx.bulkCreateEdges(edges, false)
}

// BulkCreateMintedEdges is BulkCreateEdges for relationships whose IDs the
// caller minted for this create (CreateMintedEdge).
func (tx *BadgerTransaction) BulkCreateMintedEdges(edges []*Edge) error {
	return tx.bulkCreateEdges(edges, true)
}

func (tx *BadgerTransaction) bulkCreateEdges(edges []*Edge, minted bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
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
		resolve = func(id NodeID) ([]string, bool) {
			if cached, ok := endpoints[id]; ok {
				return cached.labels, cached.visible
			}
			labels, visible := tx.visibleNodeLabelsLocked(id)
			endpoints[id] = endpoint{labels: labels, visible: visible}
			return labels, visible
		}
		batch = make(map[EdgeID]struct{}, len(edges))
		labels = make([][2][]string, len(edges))
		noOps = make([]bool, len(edges))
	}
	for i, edge := range edges {
		startLabels, endLabels, noOp, err := tx.validateNewEdgeLocked(edge, resolve, batch, minted)
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
		if err := tx.bufferNewEdgeLocked(edge, labels[i][0], labels[i][1]); err != nil {
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
func (tx *BadgerTransaction) validateNewEdgeLocked(edge *Edge, endpoint func(NodeID) ([]string, bool), batch map[EdgeID]struct{}, minted bool) (startLabels, endLabels []string, noOp bool, err error) {
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
		startLabels, visible := endpoint(edge.StartNode)
		if !visible {
			return nil, nil, false, localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
		}
		endLabels, visible := endpoint(edge.EndNode)
		if !visible {
			return nil, nil, false, localizedError(localization.StorageTransactionEndNodeMissing(string(edge.EndNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.EndNode)))
		}
		return startLabels, endLabels, false, nil
	}
	existing, err := tx.getCommittedEdgeLocked(edge.ID)
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
	startLabels, visible := endpoint(edge.StartNode)
	if !visible {
		return nil, nil, false, localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
	}
	endLabels, visible = endpoint(edge.EndNode)
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
	if stored == nil || candidate == nil ||
		stored.Type != candidate.Type ||
		stored.StartNode != candidate.StartNode ||
		stored.EndNode != candidate.EndNode {
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

// bufferNewEdgeLocked buffers a validated new edge: its record, its
// outgoing, incoming, type and edge-between index entries, the positional
// counters for its endpoints' labels (as validateNewEdgeLocked returned
// them), the read-your-writes copy and the create operation.
func (tx *BadgerTransaction) bufferNewEdgeLocked(edge *Edge, startLabels, endLabels []string) error {
	initializeCreationTimes(&edge.CreatedAt, &edge.UpdatedAt)
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
		// As for nodes, a minted ID or the stored-inclusive read has already
		// ruled out a hidden predecessor before this generated-ID optimization.
		FreshID: tx.skipCreateExistenceCheck && shouldSkipCreateExistenceCheck(string(edge.ID)),
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
func (tx *BadgerTransaction) UpdateEdge(updateRequest EdgeUpdateRequest) error {
	edge := updateRequest.After
	tx.mu.Lock()
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
		oldEdge, err = tx.getCommittedEdgeForUpdateLocked(edge.ID)
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
	}

	// If endpoints changed, verify they exist and update outgoing/incoming indexes.
	if oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		if !tx.nodeExists(edge.StartNode) {
			return localizedError(localization.StorageTransactionStartNodeMissing(string(edge.StartNode)), tx.missingWriteTargetLocked(bufferedNodeEntity, string(edge.StartNode)))
		}
		if !tx.nodeExists(edge.EndNode) {
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
	if err := tx.bufferEdgeReplacementCountDeltasLocked(oldEdge, edge); err != nil {
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
func (tx *BadgerTransaction) bufferEdgeReplacementCountDeltasLocked(oldEdge, edge *Edge) error {
	if oldEdge.Type != edge.Type {
		tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), oldEdge.Type, -1)
		tx.bufferAdjustEdgeTypeCount(namespaceForEdgeID(edge.ID), edge.Type, 1)
	}
	// Positional (label, type) counters move with type or endpoint changes.
	if oldEdge.Type != edge.Type || oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		oldStartLabels, err := tx.nodeLabelsTxVisibleLocked(oldEdge.StartNode)
		if err != nil {
			return fmt.Errorf("old start labels: %w", err)
		}
		oldEndLabels, err := tx.nodeLabelsTxVisibleLocked(oldEdge.EndNode)
		if err != nil {
			return fmt.Errorf("old end labels: %w", err)
		}
		tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), oldEdge.Type, oldStartLabels, oldEndLabels, -1)
		newStartLabels := oldStartLabels
		newEndLabels := oldEndLabels
		if oldEdge.StartNode != edge.StartNode {
			newStartLabels, err = tx.nodeLabelsTxVisibleLocked(edge.StartNode)
			if err != nil {
				return fmt.Errorf("new start labels: %w", err)
			}
		}
		if oldEdge.EndNode != edge.EndNode {
			newEndLabels, err = tx.nodeLabelsTxVisibleLocked(edge.EndNode)
			if err != nil {
				return fmt.Errorf("new end labels: %w", err)
			}
		}
		tx.bufferEdgePositionalLabelDeltas(namespaceForEdgeID(edge.ID), edge.Type, newStartLabels, newEndLabels, 1)
	}

	return nil
}

// DeleteEdge deletes an edge from the transaction.
func (tx *BadgerTransaction) DeleteEdge(edgeID EdgeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureOperationAllowedLocked(transactionDataWrite); err != nil {
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
		edge, err = tx.getCommittedEdgeLocked(edgeID)
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
	startLabels, err := tx.nodeLabelsTxVisibleLocked(edge.StartNode)
	if err != nil {
		return fmt.Errorf("start labels: %w", err)
	}
	endLabels, err := tx.nodeLabelsTxVisibleLocked(edge.EndNode)
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

// GetNode retrieves a node (read-your-writes).
//
// Reads pin the transaction to the node's namespace if it isn't already
// pinned. Without this, a transaction's pre-pin readTS sits at sequence 0
// and visibility checks against a head whose FloorVersion is non-zero
// reject every committed value as "not yet visible" — which manifests as
// lost updates under the Read-Modify-Write retry loop in DB.Update.
func (tx *BadgerTransaction) GetNode(nodeID NodeID) (*Node, error) {
	return readTransactionEntity(tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, nil, transactionNodeRead{mvccEntityRead: mvccEntityRead{vectors: true, visibility: visibleRead}})
}

// GetQueryNode implements QueryNodeReader inside the
// transaction: GetNode's visibility (pending writes, deletes, connected
// deletes, the MVCC snapshot) and the node's settled embedding metadata, the
// worker sidecar's included, without reading separately stored chunk
// vectors. Vectors already in memory (pending nodes, snapshot caches, the
// body) may be present, shared rather than copied; a caller that needs the
// vectors reads with GetNode. Query execution reads nodes this way: rows
// never carry vectors, and an update reconciles embeddings against the stored
// node (reconcileManagedEmbeddingsLocked), not the caller's copy.
func (tx *BadgerTransaction) GetQueryNode(nodeID NodeID) (*Node, error) {
	// Query visibility belongs to this capability. The common point-read owner
	// invokes the completion policy before releasing the transaction lock.
	return readTransactionEntity(tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, func(tx *BadgerTransaction, node *Node, _ transactionNodeRead, err error) (*Node, error) {
		if err == nil && tx.engine.filterEntityByDecay(visibleRead, node, DecayScoringTime()) {
			return nil, ErrNotFound
		}
		return node, err
	}, transactionNodeRead{mvccEntityRead: mvccEntityRead{visibility: visibleRead}})
}

// GetNodeProjected implements ProjectedNodeReader inside the transaction: it
// decodes only the requested user properties (none when properties is an empty
// non-nil slice) while preserving ID, labels, timestamps and embedding
// metadata, and without hydrating stored embedding vectors. A nil properties
// slice means "full node". Visibility is the transaction's own: pending
// writes, deletes, connected deletes and the MVCC snapshot all apply exactly
// as in GetNode.
func (tx *BadgerTransaction) GetNodeProjected(nodeID NodeID, properties []string) (*Node, error) {
	return readTransactionEntity(tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, nil, transactionNodeRead{mvccEntityRead: mvccEntityRead{visibility: visibleRead}, properties: properties, projected: true})
}

// getCommittedNodeProjectedLocked is getCommittedNodeLocked for a projected
// read: the same write-behind overlay, snapshot caches and MVCC resolution,
// but decoding only the requested properties and never hydrating embeddings.
func (tx *BadgerTransaction) getCommittedNodeProjectedLocked(nodeID NodeID, properties []string) (*Node, error) {
	if tx.engine != nil && tx.engine.writeBehind != nil {
		return tx.engine.GetNodeProjected(nodeID, properties)
	}
	if node, ok := tx.snapshotPrefixNodeByID[nodeID]; ok {
		return projectCachedNodeForRead(node, properties, true), nil
	}
	if node, ok := tx.snapshotLabelNodeByID[nodeID]; ok {
		sidecar := false
		_ = tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
			sidecar = tx.nodeHasEmbeddingSidecar(txn, nodeID)
			return nil
		})
		if !sidecar {
			return projectCachedNodeForRead(node, properties, true), nil
		}
	}
	if tx.readTS.IsZero() {
		return tx.getNodeFromBadgerSnapshotProjectedLocked(nodeID, propertyProjectionSet(properties))
	}
	node, err := readEntityAtWithView(tx.engine, nodeID, tx.readTS, properties, tx.withSnapshotViewLocked, (*BadgerEngine).getNodeProjectedVisibleAtInTxn)
	if err == ErrNotVisibleAtSnapshot {
		if tx.engine.writeBehind != nil {
			if fresh, freshErr := tx.engine.GetNodeProjected(nodeID, properties); freshErr == nil {
				return fresh, nil
			}
		}
		return nil, ErrNotFound
	}
	if err == ErrNotFound {
		fallbackNode, fallbackErr := tx.getNodeFromBadgerSnapshotProjectedLocked(nodeID, propertyProjectionSet(properties))
		if fallbackErr == nil {
			return fallbackNode, nil
		}
		if fallbackErr != ErrNotFound {
			return nil, fallbackErr
		}
		if tx.engine.writeBehind != nil {
			if fresh, freshErr := tx.engine.GetNodeProjected(nodeID, properties); freshErr == nil {
				return fresh, nil
			}
		}
	}
	return node, err
}

// getNodeFromBadgerSnapshotProjectedLocked reads and decodes a node from the
// transaction's pinned snapshot, projecting to the requested properties.
func (tx *BadgerTransaction) getNodeFromBadgerSnapshotProjectedLocked(nodeID NodeID, include map[string]struct{}) (*Node, error) {
	key := nodeKey(nodeID)
	var nodeBytes []byte
	err := tx.withSnapshotViewLocked(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading node: %w", err)
	}
	return tx.engine.decodeNodeProjected(namespaceForNodeID(nodeID), nodeBytes, include)
}

// RelationshipEndpointVisible implements RelationshipEndpointChecker with the
// same answer GetNode gives inside this transaction. The transaction's own
// delete or write decides first. A committed node is decided from its MVCC
// head alone when the snapshot is at or after the head's version: visible
// unless the head is a tombstone. An older snapshot, a node without a head,
// a read error and decay filtering leave the answer to GetNode.
func (tx *BadgerTransaction) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	if id == "" {
		return false, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.ensureOperationAllowedLocked(transactionRead) != nil || tx.pinNamespaceFromIDLocked(string(id)) != nil {
		return false, false
	}
	if _, connected := tx.connectedDeletes[id]; connected {
		return true, true
	}
	if _, deleted := tx.deletedNodes[id]; deleted {
		return false, true
	}
	if _, pending := tx.pendingNodes[id]; pending {
		return true, true
	}
	if _, cached := tx.snapshotPrefixNodeByID[id]; cached {
		return true, true
	}
	if tx.readTS.IsZero() || tx.engine.visibilityFiltered() {
		return false, false
	}
	var head MVCCHead
	err := tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
		var headErr error
		head, headErr = tx.engine.loadNodeMVCCHeadInTxn(snapshot, id)
		return headErr
	})
	if err != nil || tx.readTS.Compare(head.Version) < 0 {
		return false, false
	}
	return !head.Tombstoned, true
}

// GetEdge retrieves an edge with read-your-writes semantics.
//
// Like GetNode, reads pin the transaction to the edge's namespace so the
// readTS is bound to the namespace's actual sequence rather than the
// pre-pin sequence-0 placeholder.
func (tx *BadgerTransaction) GetEdge(edgeID EdgeID) (*Edge, error) {
	return readTransactionEntity(tx, edgeID, (*BadgerTransaction).edgeReadState, cloneTransactionEdge, (*BadgerTransaction).getCommittedEdgeReadLocked, nil, visibleRead)
}

// ReadEdges returns the requested native transaction adjacency, including pending
// writes and excluding pending deletions. Directional reads retain the pinned
// snapshot or write-behind view; between reads retain the engine's current view.
// For example, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: id}
// reads outgoing edges, while TransactionReadBetween also supplies Other.
func (tx *BadgerTransaction) ReadEdges(request TransactionEdgeRead) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	var direction EdgeDirection
	switch request.Kind {
	case TransactionReadOutgoing:
		direction = Outgoing
	case TransactionReadIncoming:
		direction = Incoming
	case TransactionReadBetween:
	default:
		return nil, ErrInvalidData
	}
	if err := tx.pinNamespaceFromIDLocked(string(request.Node)); err != nil {
		return nil, err
	}
	if request.Kind == TransactionReadBetween {
		if err := tx.pinNamespaceFromIDLocked(string(request.Other)); err != nil {
			return nil, err
		}
	}
	var committed []*Edge
	var err error
	if request.Kind == TransactionReadBetween {
		committed, err = tx.engine.GetEdgesBetween(request.Node, request.Other)
	} else {
		committed, err = tx.getCommittedAdjacentEdgesLocked(request.Node, direction)
	}
	if err != nil {
		return nil, err
	}
	return tx.mergePendingAdjacentEdgesLocked(committed, request.Node, func(edge *Edge) bool {
		switch request.Kind {
		case TransactionReadOutgoing:
			return edge.StartNode == request.Node
		case TransactionReadIncoming:
			return edge.EndNode == request.Node
		default:
			return edge.StartNode == request.Node && edge.EndNode == request.Other
		}
	}), nil
}

// GetEdgeBetween returns a matching edge including pending transaction writes.
func (tx *BadgerTransaction) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edges, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadBetween, Node: startID, Other: endID})
	if err != nil {
		return nil
	}
	for _, edge := range edges {
		if edgeType == "" || edge.Type == edgeType {
			return edge
		}
	}
	return nil
}

// getCommittedEdgesByTypeLocked reads the committed edges of edgeType ("" is
// every edge) visible at the pinned snapshot through the edge type index, then
// merges the transaction's pending edge writes and deletes as the public reads
// do. The type index in that snapshot is the membership at BEGIN and the read
// retains the global collection contract of AllEdges and GetEdgesByType.
// The caller supplies the original pending-record filter and holds tx.mu.
func (tx *BadgerTransaction) getCommittedEdgesByTypeLocked(edgeType string, includePending func(*Edge) bool) ([]*Edge, error) {
	committed := make([]*Edge, 0)
	err := tx.engine.streamEdgesByTypeAtSnapshotWithView(context.Background(), "", edgeType, tx.readTS, tx.withSnapshotViewLocked, visibleRead, func(edge *Edge) error {
		committed = append(committed, edge)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(committed, includePending), nil
}

// AllEdges returns all visible edges including pending transaction writes.
func (tx *BadgerTransaction) AllEdges() ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, and buffered edges are
		// invisible to the pinned view. Read the latest overlaid view.
		committed, err := tx.engine.AllEdges()
		if err != nil {
			return nil, err
		}
		return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool { return edge != nil }), nil
	}
	return tx.getCommittedEdgesByTypeLocked("", func(edge *Edge) bool { return edge != nil })
}

// GetEdgesByType returns edges of a given type including pending transaction writes.
func (tx *BadgerTransaction) GetEdgesByType(edgeType string) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}

	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, so read the latest
		// (overlaid) view instead.
		committed, err := tx.engine.GetEdgesByType(edgeType)
		if err != nil {
			return nil, err
		}
		return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
			return edge.Type == edgeType
		}), nil
	}
	return tx.getCommittedEdgesByTypeLocked(edgeType, func(edge *Edge) bool { return edge.Type == edgeType })
}

// GetNodesByLabel returns nodes with the given label including pending transaction writes.
func (tx *BadgerTransaction) GetNodesByLabel(label string) ([]*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}

	committed, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingNodesLocked(committed, func(node *Node) bool {
		if node == nil {
			return false
		}
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
				return true
			}
		}
		return false
	}), nil
}

// StreamNodesByLabelProjected visits label matches from the transaction's
// pinned snapshot and overlays pending writes as one stream. The callback can
// stop iteration without forcing the transaction to resolve or copy the rest
// of the label population.
func (tx *BadgerTransaction) StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return tx.streamNodesByLabelProjected(label, properties, visibleRead, visit)
}

func (tx *BadgerTransaction) streamNodesByLabelProjected(label string, properties []string, v readVisibility, visit func(*Node) error, readOptions ...StreamNodesOptions) error {
	opts := nodeScanOptions(readOptions)
	queryNodes := opts.QueryNodes && !opts.WithEmbeddings
	if visit == nil {
		return ErrInvalidData
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	scope := tx.labelScanScopeLocked(opts.Prefix)
	if err := tx.engine.pendingPrefixConflict(scope); err != nil {
		return err
	}
	cacheScope := ""
	if scope != tx.labelScanScopeLocked() {
		cacheScope = scope
	}

	invokeVisit := func(node *Node) error {
		tx.mu.Unlock()
		err := visit(node)
		tx.mu.Lock()
		return err
	}

	hasPending := len(tx.pendingNodes) > 0 || len(tx.deletedNodes) > 0
	var seen map[NodeID]struct{}
	if hasPending {
		seen = make(map[NodeID]struct{}, len(tx.pendingNodes))
	}

	// Write-behind reads are read-committed: the flusher may land a
	// buffered commit after the pinned snapshot, so the engine's latest
	// (overlaid) stream is used and no extra overlay merge runs here.
	engineOverlaid := tx.readTS.IsZero() || (tx.engine != nil && tx.engine.writeBehind != nil)
	var overlay []*Node
	var overlayTouched map[NodeID]bool

	emitOverlay := func() error {
		if engineOverlaid {
			return nil // the engine stream already merged the overlay
		}
		scope := tx.labelScanScopeLocked(opts.Prefix)
		nowNanos := DecayScoringTime()
		for _, n := range overlay {
			if !nodeIDInScope(n.ID, scope) || tx.engine.filterEntityByDecay(v, n, nowNanos) {
				continue
			}
			if err := invokeVisit(projectCachedNodeForRead(n, properties, false, queryNodes)); err != nil {
				if err == ErrIterationStopped {
					return nil
				}
				return err
			}
		}
		return nil
	}
	// A whole node the stream hands out is also the one a later write of
	// the node takes as its old version (snapshotLabelNodeByID), so the
	// caller gets a copy it may change (#965). A projected node is only
	// read: nothing writes it back.
	forCaller := func(node *Node) *Node {
		if properties == nil {
			return copyNodeForCaller(node)
		}
		return node
	}
	emitCommitted := func(node *Node) error {
		if node == nil {
			return nil
		}
		if len(overlayTouched) > 0 && overlayTouched[node.ID] {
			return nil // the write-behind overlay shadows this committed row
		}
		// Eagerly cache scanned labels within the byte/node bound. This is
		// load-bearing for snapshot isolation: later label reads in this
		// transaction must see the scan-time labels, not labels committed
		// afterwards (TestTxReads_LabelCacheBoundPreservesSnapshotLabels).
		if !engineOverlaid && len(tx.snapshotNodeLabels) < maxSnapshotNodeLabelCacheNodes {
			tx.cacheCommittedNodeLabelsLocked(node)
		}
		if !hasPending {
			return invokeVisit(forCaller(node))
		}
		if _, deleted := tx.deletedNodes[node.ID]; deleted {
			seen[node.ID] = struct{}{}
			return nil
		}
		if pending, exists := tx.pendingNodes[node.ID]; exists {
			seen[node.ID] = struct{}{}
			if pending != nil && slices.Contains(pending.Labels, label) {
				return invokeVisit(projectCachedNodeForRead(pending, properties, false, queryNodes))
			}
			return nil
		}
		return invokeVisit(forCaller(node))
	}

	cacheKey := snapshotLabelCacheKey{scope: cacheScope, queryNodes: queryNodes, projectionKey: label, reveal: tx.engine.revealAll.Load(), stored: v == storedRead}
	cache := tx.snapshotLabelNodes
	if properties != nil {
		cacheKey.projectionKey = snapshotLabelProjectionKey(label, properties)
		cache = tx.snapshotProjectedLabelNodes
	}
	// Label streams have already applied entity visibility. A transaction
	// outlives each statement's REVEAL scope, so its filtered population must
	// never answer a reveal read (or the reverse). Keep both views pinned to
	// the same physical snapshot, but cache their populations independently.
	if cached, ok := cache[cacheKey]; ok && !engineOverlaid {
		for _, node := range cached {
			if err := emitCommitted(node); err != nil {
				return err
			}
		}
		if err := emitOverlay(); err != nil {
			return err
		}
		return tx.streamPendingLabelNodesLocked(label, seen, properties, invokeVisit, queryNodes)
	}

	completed := make([]*Node, 0)
	afterNodeID := NodeID("")
	if prefix := tx.snapshotLabelPrefixNodes[cacheKey]; !engineOverlaid && tx.snapshotTx != nil && len(prefix) > 0 {
		completed = append(completed, prefix...)
		for _, node := range prefix {
			if err := emitCommitted(node); err != nil {
				return err
			}
		}
		afterNodeID = prefix[len(prefix)-1].ID
	}

	var err error
	streamVisit := func(node *Node) error {
		completed = append(completed, node)
		return emitCommitted(node)
	}
	if tx.snapshotTx != nil && tx.engine.writeBehind == nil {
		err = tx.engine.streamNodesByLabelFromPhysicalSnapshotAfterRead(scope, label, tx.withSnapshotViewLocked, properties, afterNodeID, v, streamVisit, queryNodes)
	} else if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		err = tx.engine.streamNodesByLabelProjectedInScope(scope, label, properties, v, streamVisit, queryNodes)
	} else {
		err = tx.engine.streamNodesByLabelAtSnapshotWithView(
			scope,
			label, tx.readTS, tx.withSnapshotViewLocked, properties, v, streamVisit, queryNodes,
		)
	}
	if err != nil {
		if err == ErrIterationStopped && !engineOverlaid && tx.snapshotTx != nil {
			cacheEndpointNodes := properties == nil && !tx.engine.visibilityFiltered()
			tx.storeSnapshotLabelPrefixLocked(tx.snapshotTx, cacheKey, completed, cacheEndpointNodes)
		}
		return err
	}
	if properties == nil && !engineOverlaid {
		if tx.snapshotLabelNodes == nil {
			tx.snapshotLabelNodes = make(map[snapshotLabelCacheKey][]*Node)
		}
		tx.snapshotLabelNodes[cacheKey] = completed
		tx.clearSnapshotLabelPrefixLocked(cacheKey)
		if !queryNodes && !tx.engine.visibilityFiltered() {
			if tx.snapshotLabelNodeByID == nil {
				tx.snapshotLabelNodeByID = make(map[NodeID]*Node, len(completed))
			}
			for _, node := range completed {
				tx.snapshotLabelNodeByID[node.ID] = node
			}
		}
	} else if !engineOverlaid && len(tx.snapshotProjectedLabelNodes) < maxSnapshotProjectedLabelStreams {
		if tx.snapshotProjectedLabelNodes == nil {
			tx.snapshotProjectedLabelNodes = make(map[snapshotLabelCacheKey][]*Node)
		}
		tx.snapshotProjectedLabelNodes[cacheKey] = completed
		tx.clearSnapshotLabelPrefixLocked(cacheKey)
	}
	if err := emitOverlay(); err != nil {
		return err
	}
	return tx.streamPendingLabelNodesLocked(label, seen, properties, invokeVisit, queryNodes)
}

// StreamNodesWithOptions streams the transaction's view of every node (its
// snapshot overlaid with its own pending writes) with the engine scan's
// options: prefix, projection, PropertyFilter, embeddings and decay. Committed
// nodes are streamed from the pinned snapshot and never materialised as a
// population: AllNodes did, so a property match without a label inside a
// transaction, which includes every write statement, decoded every node in
// full (#824). A transaction built without a pinned snapshot reads the
// committed view its other reads take (getAllCommittedNodesLocked). Pending
// writes replace their committed versions and are not filtered (callers test
// every node they receive); ErrIterationStopped from visit ends the stream.
func (tx *BadgerTransaction) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, visit func(*Node) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if opts.QueryNodes && opts.Projection == nil && !opts.WithEmbeddings && opts.Label != "" {
		queryVisit := borrowQueryNodeScanVisitor(ctx, opts, visit)
		defer queryVisit.release()
		err := tx.streamNodesByLabelProjected(opts.Label, nil, opts.readVisibility(), queryVisit.callback, opts)
		if err == ErrIterationStopped {
			return nil
		}
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	if err := tx.engine.pendingPrefixConflict(tx.labelScanScopeLocked(opts.Prefix)); err != nil {
		return err
	}

	stopped := false
	invokeVisit := func(node *Node) error {
		tx.mu.Unlock()
		err := visit(node)
		tx.mu.Lock()
		if err == ErrIterationStopped {
			stopped = true
		}
		return err
	}
	inScope := func(id NodeID) bool {
		return opts.Prefix == "" || strings.HasPrefix(string(id), opts.Prefix)
	}
	readPending := func(node *Node) *Node {
		return opts.copyResidentNode(node)
	}

	hasPending := len(tx.pendingNodes) > 0 || len(tx.deletedNodes) > 0
	var seen map[NodeID]struct{}
	if hasPending {
		seen = make(map[NodeID]struct{}, len(tx.pendingNodes))
	}
	emitCommitted := func(node *Node) error {
		if hasPending {
			if _, deleted := tx.deletedNodes[node.ID]; deleted {
				return nil
			}
			if pending, exists := tx.pendingNodes[node.ID]; exists {
				seen[node.ID] = struct{}{}
				return invokeVisit(readPending(pending))
			}
		}
		return invokeVisit(node)
	}

	var err error
	if tx.engine != nil && tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the engine's latest
		// (overlaid) stream includes flushes that landed after the pinned
		// snapshot, with early termination for LIMIT pushdown.
		err = tx.engine.StreamNodesWithOptions(ctx, opts, emitCommitted)
	} else if tx.snapshotTx != nil {
		err = tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
			return tx.engine.streamNodesWithOptionsInTxn(ctx, txn, opts, emitCommitted)
		})
	} else {
		err = tx.streamCommittedNodesLocked(ctx, opts, inScope, emitCommitted)
	}
	if stopped || err == ErrIterationStopped {
		return nil
	}
	if err != nil {
		return err
	}
	// Pending and deleted nodes are disjoint: CreateNode clears a deletion.
	for id, node := range tx.pendingNodes {
		if _, emitted := seen[id]; emitted || !inScope(id) {
			continue
		}
		if err := invokeVisit(readPending(node)); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// streamCommittedNodesLocked is the committed side of StreamNodesWithOptions
// for a transaction built without a pinned physical snapshot: the committed
// view the transaction's other reads take (getAllCommittedNodesLocked),
// scoped, projected and filtered after decoding.
func (tx *BadgerTransaction) streamCommittedNodesLocked(ctx context.Context, opts StreamNodesOptions, inScope func(NodeID) bool, emit func(*Node) error) error {
	if opts.QueryNodes && opts.Projection == nil && !opts.WithEmbeddings {
		if tx.readTS.IsZero() {
			return tx.engine.StreamNodesWithOptions(ctx, opts, emit)
		}
		// Reuse the logical MVCC iterator instead of materializing full nodes
		// before the options-driven scan can choose its read contract.
		return tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
			return tx.engine.iterateNodesVisibleAtInScopeInTxn(txn, tx.labelScanScopeLocked(opts.Prefix), tx.readTS, func(node *Node) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !inScope(node.ID) {
					return nil
				}
				if opts.StripEmbeddings {
					node = opts.copyResidentNode(node)
				}
				return emit(node)
			}, opts)
		})
	}
	nodes, err := tx.getAllCommittedNodesLocked()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !inScope(node.ID) {
			continue
		}
		if opts.Projection != nil || opts.StripEmbeddings {
			node = projectCachedNodeForRead(node, opts.Projection, opts.StripEmbeddings)
		}
		if opts.PropertyFilter != nil && !opts.PropertyFilter(node.Properties) {
			continue
		}
		if err := emit(node); err != nil {
			return err
		}
	}
	return nil
}

// A long-lived explicit transaction may execute many unrelated projections.
// Keep the snapshot replay optimization bounded so retained decoded rows cannot
// grow with the number of distinct query shapes.
const maxSnapshotProjectedLabelStreams = 8

func snapshotLabelProjectionKey(label string, properties []string) string {
	canonical := append([]string(nil), properties...)
	sort.Strings(canonical)
	var key strings.Builder
	key.WriteString(label)
	key.WriteByte(0)
	for _, property := range canonical {
		key.WriteString(strconv.Itoa(len(property)))
		key.WriteByte(':')
		key.WriteString(property)
	}
	return key.String()
}

func (tx *BadgerTransaction) streamPendingLabelNodesLocked(
	label string,
	seen map[NodeID]struct{},
	properties []string,
	visit func(*Node) error,
	queryRead ...bool,
) error {
	if len(tx.pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		return nil
	}
	for id, node := range tx.pendingNodes {
		if _, emitted := seen[id]; emitted {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted || node == nil || !slices.Contains(node.Labels, label) {
			continue
		}
		if err := visit(projectCachedNodeForRead(node, properties, false, queryRead...)); err != nil {
			return err
		}
	}
	return nil
}

// GetFirstNodeByLabel returns the first visible node with the given label.
func (tx *BadgerTransaction) GetFirstNodeByLabel(label string) (*Node, error) {
	nodes, err := tx.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	return nodes[0], nil
}

// AllNodes returns all visible nodes including pending transaction writes.
func (tx *BadgerTransaction) AllNodes() ([]*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}

	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	committed, err := tx.getAllCommittedNodesLocked()
	if err != nil {
		return nil, err
	}
	return tx.mergePendingNodesLocked(committed, func(node *Node) bool {
		return node != nil
	}), nil
}

func (tx *BadgerTransaction) GetAllNodes() []*Node {
	nodes, err := tx.AllNodes()
	if err != nil {
		return nil
	}
	return nodes
}

func (tx *BadgerTransaction) HasPendingNodeMutations() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingNodes) > 0 || len(tx.deletedNodes) > 0
}

// HasPendingEdgeMutations reports whether the transaction has staged any edge
// creates, updates, or deletes. Typed-count fast paths use it to decide
// whether the underlying per-type counters still describe the
// transaction-visible state (issue #638).
func (tx *BadgerTransaction) HasPendingEdgeMutations() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingEdges) > 0 || len(tx.deletedEdges) > 0
}

// mergePendingLocked merges committed records with the transaction's pending
// overlay. It is the shared kernel behind the node and edge twins: a pending
// record shadows a committed record with the same ID, a deleted ID drops its
// committed record, and every surviving record is copied before it escapes so
// callers can never mutate overlay state. Nil records are skipped (idOf
// returns the zero ID for them). When there is no overlay, the committed
// slice is returned unchanged.
func mergePendingLocked[ID comparable, T any](
	committed []T,
	pending map[ID]T,
	deleted map[ID]struct{},
	includePending func(T) bool,
	idOf func(T) ID,
	copyOf func(T) T,
) []T {
	if len(pending) == 0 && len(deleted) == 0 {
		return committed
	}
	merged := make([]T, 0, util.SafePreallocSum(len(committed), len(pending)))
	seen := make(map[ID]struct{}, util.SafePreallocSum(len(committed), len(pending)))
	var zero ID
	for _, record := range committed {
		id := idOf(record)
		if id == zero {
			continue // nil record
		}
		if _, deleted := deleted[id]; deleted {
			continue
		}
		if pendingRecord, exists := pending[id]; exists {
			if includePending(pendingRecord) {
				merged = append(merged, copyOf(pendingRecord))
				seen[id] = struct{}{}
			}
			continue
		}
		if includePending(record) {
			merged = append(merged, copyOf(record))
			seen[id] = struct{}{}
		}
	}

	for id, record := range pending {
		if _, exists := seen[id]; exists {
			continue
		}
		if includePending(record) {
			merged = append(merged, copyOf(record))
		}
	}

	return merged
}

func (tx *BadgerTransaction) mergePendingNodesLocked(committed []*Node, includePending func(*Node) bool) []*Node {
	return mergePendingLocked(committed, tx.pendingNodes, tx.deletedNodes, includePending, func(node *Node) NodeID {
		if node == nil {
			return ""
		}
		return node.ID
	}, copyNode)
}

func (tx *BadgerTransaction) mergePendingEdgesLocked(committed []*Edge, includePending func(*Edge) bool) []*Edge {
	return mergePendingLocked(committed, tx.pendingEdges, tx.deletedEdges, includePending, func(edge *Edge) EdgeID {
		if edge == nil {
			return ""
		}
		return edge.ID
	}, copyEdge)
}

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

// commitCancelledLocked reports the commit context's error once it is done.
func (tx *BadgerTransaction) commitCancelledLocked() error {
	if tx.commitCtx == nil {
		return nil
	}
	return tx.commitCtx.Err()
}

// commitCancelPollEvery is how many nodes commit-time validation checks
// between polls of the commit context.
const commitCancelPollEvery = 256

func (tx *BadgerTransaction) Commit() error { return tx.PublishCommit(nil, CommitPublication{}) }

// commitConflictError reports a Badger write conflict found while committing
// in the storage commit-conflict shape (ErrConflict, localized).
func commitConflictError(err error) error {
	cause := fmt.Errorf("%w: concurrent transaction modified data before commit: %w", ErrConflict, err)
	return localizedError(localization.StorageTransactionCommitConflict(err), cause)
}

// Rollback discards all changes.
func (tx *BadgerTransaction) Rollback() error {
	if tx.clientDetached.Load() {
		return ErrTransactionClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.closeLocked(TxStatusRolledBack, true, nil)
	return nil
}

// StagedMemoryBytes estimates storage-owned data retained by an open
// transaction. It counts the serialized write copies and pending entity
// copies once each. Historical operation bodies are distinct from the
// pending map's current version and remain owned until the transaction ends.
// The value is sampled at statement boundaries while the
// transaction session owns the transaction.
func (tx *BadgerTransaction) StagedMemoryBytes() int64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	var bytes int64
	for key, value := range tx.pendingWrites {
		bytes += int64(len(key) + cap(value) + 64)
	}
	for key := range tx.pendingDeletes {
		bytes += int64(len(key) + 48)
	}
	nodes := make(map[*Node]struct{}, len(tx.pendingNodes))
	for _, node := range tx.pendingNodes {
		bytes += estimateRetainedNodeBytes(node)
		nodes[node] = struct{}{}
	}
	edges := make(map[*Edge]struct{}, len(tx.pendingEdges))
	for _, edge := range tx.pendingEdges {
		bytes += estimateRetainedEdgeBytes(edge)
		edges[edge] = struct{}{}
	}
	for _, op := range tx.operations {
		for _, node := range [2]*Node{op.Node, op.OldNode} {
			if node == nil {
				continue
			}
			if _, counted := nodes[node]; !counted {
				bytes += estimateRetainedNodeBytes(node)
				nodes[node] = struct{}{}
			}
		}
		for _, edge := range [2]*Edge{op.Edge, op.OldEdge} {
			if edge == nil {
				continue
			}
			if _, counted := edges[edge]; !counted {
				bytes += estimateRetainedEdgeBytes(edge)
				edges[edge] = struct{}{}
			}
		}
		bytes += int64(cap(op.DeletedEdgeIDs)) * int64(unsafe.Sizeof(EdgeID("")))
		for _, id := range op.DeletedEdgeIDs {
			bytes += int64(len(id))
		}
	}
	bytes += int64(cap(tx.operations)) * int64(unsafe.Sizeof(Operation{}))
	return bytes
}

func estimateRetainedNodeBytes(node *Node) int64 {
	if node == nil {
		return 0
	}
	bytes := estimateNodeBodyCacheBytes(node)
	for _, vector := range node.NamedEmbeddings {
		bytes += int64(32 + 4*len(vector))
	}
	for _, vector := range node.ChunkEmbeddings {
		bytes += int64(24 + 4*len(vector))
	}
	return bytes
}

func estimateRetainedEdgeBytes(edge *Edge) int64 {
	if edge == nil {
		return 0
	}
	bytes := int64(160 + len(edge.ID) + len(edge.StartNode) + len(edge.EndNode) + len(edge.Type))
	bytes += estimateEntityPropertyBytes(edge.Properties)
	return bytes
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

	// Merge
	if tx.Metadata == nil {
		tx.Metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		tx.Metadata[k] = v
	}

	return nil
}

// GetMetadata returns transaction metadata copy.
func (tx *BadgerTransaction) GetMetadata() map[string]interface{} {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := make(map[string]interface{})
	for k, v := range tx.Metadata {
		result[k] = v
	}
	return result
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

// getCommittedNodeLocked reads a committed node for the transaction's own
// work: a write's target, an endpoint, a constraint check, count
// bookkeeping. It reads what storage holds, so a node the visibility layer
// hides is found (storedRead). GetNode and GetQueryNode read the same way
// through the visibility layer (getCommittedNodeReadLocked).
func (tx *BadgerTransaction) getCommittedNodeLocked(nodeID NodeID) (*Node, error) {
	return tx.getCommittedNodeReadLocked(nodeID, true, storedRead)
}

// getCommittedNodeReadLocked reads a committed node through the visibility
// layer when v applies it. With vectors false it reads no separately stored
// embedding vector (GetQueryNode), and a node already held in memory shares
// its vectors instead of copying them.
func (tx *BadgerTransaction) getCommittedNodeReadLocked(nodeID NodeID, vectors bool, v readVisibility) (*Node, error) {
	copyRead := copyNode
	if !vectors {
		copyRead = copyNodeForCaller
	}
	// Write-behind overlay: buffered-but-unflushed writes are committed and
	// visible to every later transaction, ahead of any snapshot read.
	if tx.engine != nil && tx.engine.writeBehind != nil {
		if vectors {
			return tx.engine.getNode(nodeID, v)
		}
		return tx.engine.readNodeMetadata(nodeID, v, nodeItemQueryMetadata)
	}
	if node, ok := tx.snapshotPrefixNodeByID[nodeID]; ok {
		return copyRead(node), nil
	}
	if node, ok := tx.snapshotLabelNodeByID[nodeID]; ok {
		// Like the prefix cache, a node with worker-sidecar embeddings is
		// read again so their hydration runs.
		sidecar := false
		_ = tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
			sidecar = tx.nodeHasEmbeddingSidecar(txn, nodeID)
			return nil
		})
		if !sidecar {
			return copyRead(node), nil
		}
	}
	if tx.readTS.IsZero() {
		return tx.getNodeFromBadgerSnapshotLocked(nodeID)
	}
	node, err := readEntityAtWithView(tx.engine, nodeID, tx.readTS, mvccEntityRead{vectors: vectors, visibility: v}, tx.withSnapshotViewLocked, (*BadgerEngine).getNodeAtInTxn)
	if err == ErrNotVisibleAtSnapshot {
		// The head exists but isn't visible at the reader's snapshot.
		// Treat as a hard miss — falling back to a primary-key read
		// would expose a peer's post-begin commit and break SI.
		if tx.engine.writeBehind != nil {
			// Write-behind: the write was committed at buffer-append, and
			// its MVCC head only appears when the flusher lands it, so a
			// snapshot pinned before the flush can miss a committed node.
			// The overlay covers still-buffered writes; a fresh
			// latest-committed read covers the flush window.
			if fresh, freshErr := tx.engine.getNode(nodeID, v); freshErr == nil {
				return fresh, nil
			}
		}
		return nil, ErrNotFound
	}
	if err == ErrNotFound {
		// No head record at all — fall through to a primary-key read
		// to serve legacy, pre-MVCC bodies that never had a head
		// allocated. Safe because the absence of a head means no
		// version was ever assigned, so there's no SI invariant to
		// violate.
		fallbackNode, fallbackErr := tx.getNodeFromBadgerSnapshotLocked(nodeID)
		if fallbackErr == nil {
			return fallbackNode, nil
		}
		if fallbackErr != ErrNotFound {
			return nil, fallbackErr
		}
		if tx.engine.writeBehind != nil {
			// The pinned snapshot may predate the flush of a node
			// committed through the buffer (head and record both
			// absent at pin time). A fresh read resolves it.
			if fresh, freshErr := tx.engine.getNode(nodeID, v); freshErr == nil {
				return fresh, nil
			}
		}
	}
	// Seed the transaction label cache from the body this read already
	// paid for: edge-batch validation (visibleNodeLabelsLocked) reads each
	// endpoint's labels from the MVCC head, and a property-index MATCH has
	// just loaded the full body in the same transaction (#1043 write-path
	// measurement 4: that second read was ~15% of friendship-phase server
	// CPU). The cache is byte/node bounded (cacheCommittedNodeLabelsLocked)
	// and committed-only, so pending/deleted nodes keep their precedence.
	if node != nil && err == nil && len(node.Labels) > 0 {
		tx.cacheCommittedNodeLabelsLocked(node)
	}
	return node, err
}

// AcknowledgedBuffered reports whether the commit was acknowledged through
// the write-behind buffer rather than made durable. The executor's implicit
// path reads it after Commit to decide whether durable receipt metadata
// applies; the WAL begin/commit markers still run, only the receipt is
// suppressed on the eventual-consistency ACK.
func (tx *BadgerTransaction) AcknowledgedBuffered() bool {
	return tx.bufferedAck.Load()
}

// getNodeFromBadgerSnapshotLocked reads the primary nodeKey via the pinned
// read-only Badger transaction rather than the writer txn so the read does
// NOT enter the user txn's SSI read set. Without that isolation, the
// MERGE planning path (which probes peer-tx node IDs returned from the
// schema's UNIQUE-constraint cache) would put the peer's nodeKey into
// this tx's read set; when the peer commits its primary key, this tx's
// commit collides with a "Transaction Conflict" instead of receiving
// the consumer-pinned commit-time UNIQUE shape (see
// docs/plans/consumer-pinned-error-contract-plan.md §2.1).
//
// Read-your-writes is preserved by GetNode's pendingNodes check at the
// caller — this fallback is for legacy / pre-MVCC nodes that do not yet
// have an MVCC head record.
func (tx *BadgerTransaction) getNodeFromBadgerSnapshotLocked(nodeID NodeID) (*Node, error) {
	key := nodeKey(nodeID)
	var nodeBytes []byte
	err := tx.withSnapshotViewLocked(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading node: %w", err)
	}
	return tx.engine.decodeNode(namespaceForNodeID(nodeID), nodeBytes)
}

// getCommittedEdgeLocked reads a committed relationship for the
// transaction's own work: a write's target, a constraint check, count
// bookkeeping. It reads what storage holds, so a relationship the visibility
// layer hides is found (storedRead). GetEdge reads the same way through the
// visibility layer (getCommittedEdgeReadLocked).
func (tx *BadgerTransaction) getCommittedEdgeLocked(edgeID EdgeID) (*Edge, error) {
	return tx.getCommittedEdgeReadLocked(edgeID, storedRead)
}

// getCommittedEdgeReadLocked reads a committed relationship through the
// visibility layer when v applies it.
func (tx *BadgerTransaction) getCommittedEdgeReadLocked(edgeID EdgeID, v readVisibility) (*Edge, error) {
	// Write-behind overlay: buffered-but-unflushed writes are committed and
	// visible to every later transaction, ahead of any snapshot read.
	if tx.engine != nil && tx.engine.writeBehind != nil {
		return tx.engine.getEdge(edgeID, v)
	}
	if tx.readTS.IsZero() {
		key := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("reading edge: %w", err)
		}
		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading edge value: %w", err)
		}
		return tx.engine.decodeEdgeBodyByID(edgeBytes, edgeID)
	}
	edge, err := readEntityAtWithView(tx.engine, edgeID, tx.readTS, mvccEntityRead{visibility: v}, tx.withSnapshotViewLocked, (*BadgerEngine).getEdgeAtInTxn)
	if err == ErrNotVisibleAtSnapshot {
		// SI: edge head exists but isn't visible at our snapshot.
		// Surface as ErrNotFound so callers can't observe peer
		// commits that landed after our begin.
		if tx.engine.writeBehind != nil {
			// Write-behind: the edge was committed at buffer-append; its
			// head appears only when the flusher lands it, so a snapshot
			// pinned before the flush can miss a committed edge. A fresh
			// latest-committed read covers the flush window.
			if fresh, freshErr := tx.engine.getEdge(edgeID, v); freshErr == nil {
				return fresh, nil
			}
		}
		return nil, ErrNotFound
	}
	if err == ErrNotFound && tx.engine.writeBehind != nil {
		// The pinned snapshot may predate the flush of an edge committed
		// through the buffer (head and record both absent at pin time).
		if fresh, freshErr := tx.engine.getEdge(edgeID, v); freshErr == nil {
			return fresh, nil
		}
	}
	return edge, err
}

// getCommittedEdgeForUpdateLocked resolves the committed edge body for a
// write (UpdateEdge). It behaves exactly like getCommittedEdgeLocked except
// in one case: when the edge head exists but was committed AFTER this
// transaction's snapshot (ErrNotVisibleAtSnapshot or no physical head) and the edge is live at
// latest-committed state, it returns ErrConflict instead of ErrNotFound.
//
// Rationale: MERGE resolves an existing relationship through a
// latest-committed lookup (GetEdgesBetween), so a peer's post-begin commit
// of the same relationship IS found — but a snapshot-scoped read here would
// report the same edge as missing, failing the statement with a hard
// "not found" that Bolt surfaces as a non-retryable client error. The same
// interleaving succeeds on Neo4j (MERGE blocks on the relationship lock,
// re-reads, and applies the SET); its only sanctioned conflict failure is a
// transient code drivers auto-retry. Classifying this as ErrConflict keeps
// the existing consumer-pinned wire shape ("edge %s changed after
// transaction start" -> Neo.TransientError.Transaction.Outdated) that
// checkEdgeWriteConflict already produces for the same race at commit time —
// it moves that sanctioned disclosure to statement time, revealing nothing
// new.
//
// The reclassification is tombstone-aware: if the peer DELETED the edge
// (nothing live at latest-committed state), ErrNotFound remains correct.
// Read paths (GetEdge) are intentionally untouched — snapshot isolation
// still hides peer commits from reads.
func (tx *BadgerTransaction) getCommittedEdgeForUpdateLocked(edgeID EdgeID) (*Edge, error) {
	// Write-behind overlay: a buffered-but-unflushed edge is committed and
	// read-committed-visible, exactly as getCommittedEdgeLocked reports it.
	// Updating it is therefore not a write-write conflict — it is the same
	// latest-committed state the update read from (monster retest part 1,
	// "SET on a buffered relationship fails with a conflict").
	if tx.engine != nil && tx.engine.writeBehind != nil {
		return tx.engine.getEdge(edgeID, storedRead)
	}
	if tx.readTS.IsZero() {
		return tx.getCommittedEdgeLocked(edgeID)
	}
	edge, err := readEntityAtWithView(tx.engine, edgeID, tx.readTS, mvccEntityRead{visibility: storedRead}, tx.withSnapshotViewLocked, (*BadgerEngine).getEdgeAtInTxn)
	// A peer-created edge has no head at the pinned physical snapshot.
	// Classify it using latest state just like a logically newer edge; never
	// replace an existing snapshot body with that latest body.
	if !errors.Is(err, ErrNotVisibleAtSnapshot) && !errors.Is(err, ErrNotFound) {
		return edge, err
	}
	_, _, latestErr := tx.engine.readLatestEntity(string(edgeID), true, storedRead)
	switch {
	case latestErr == nil:
		// Live at latest-committed state: a write-write conflict, not a
		// missing edge. Wire contract: substrings "conflict:" and
		// "changed after transaction start" are matched by downstream
		// Bolt classifiers as transient.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.2.
		return nil, localizedError(localization.StorageTransactionEdgeChanged(string(edgeID)), ErrConflict)
	case errors.Is(latestErr, ErrNotFound):
		// Tombstoned or absent at latest-committed state — genuinely gone.
		return nil, ErrNotFound
	default:
		return nil, latestErr
	}
}

func (tx *BadgerTransaction) getNodesByLabelLocked(label string) ([]*Node, error) {
	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, so read the latest
		// (overlaid) view instead.
		return tx.engine.GetNodesByLabelInScope(tx.labelScanScopeLocked(), label)
	}
	return tx.engine.getNodesByLabelVisibleAtSnapshotWithView(tx.labelScanScopeLocked(), label, tx.readTS, tx.withSnapshotViewLocked)
}

// labelScanScopeLocked is the node ID prefix of the database the transaction
// is pinned to, or "" before it is pinned. A pinned transaction reads and
// writes only its own database, so its label scans skip other databases'
// label-index entries before reading their nodes (#851); callers still
// filter what they receive by database.
func (tx *BadgerTransaction) labelScanScopeLocked(prefixes ...string) string {
	scope := ""
	if tx.namespace != "" {
		scope = tx.namespace + ":"
	}
	if len(prefixes) > 0 && prefixes[0] != "" && (scope == "" || strings.HasPrefix(prefixes[0], scope)) {
		return prefixes[0]
	}
	return scope
}

// committedConstraintNodesLocked returns the committed nodes with label that
// a constraint check compares a written node with. It reads the latest
// committed state, not the transaction's snapshot: a value another
// transaction committed after this one began still conflicts, as in Neo4j
// (#700), and the constraint keys' commit locks (the canonical commit-lock registry) keep that
// state from changing under the check. Nodes this transaction deletes or
// rewrites are left out; the checks compare their pending versions
// separately.
func (tx *BadgerTransaction) committedConstraintNodesLocked(label string) ([]*Node, error) {
	nodes, err := tx.engine.GetStoredNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	kept := nodes[:0]
	for _, node := range nodes {
		if !tx.committedNodeSupersededLocked(node.ID) {
			kept = append(kept, node)
		}
	}
	return kept, nil
}

// committedNodeSupersededLocked reports whether the transaction no longer sees
// a committed node as stored: it deletes the node, or rewrites it (its pending
// version is what the transaction sees, and is checked as pending).
// Constraint checks against committed data skip such nodes, so a value a
// node gives up in the transaction is free for another (#907).
func (tx *BadgerTransaction) committedNodeSupersededLocked(id NodeID) bool {
	if _, deleted := tx.deletedNodes[id]; deleted {
		return true
	}
	_, rewritten := tx.pendingNodes[id]
	return rewritten
}

// committedEdgeSupersededLocked is committedNodeSupersededLocked for a
// relationship.
func (tx *BadgerTransaction) committedEdgeSupersededLocked(id EdgeID) bool {
	if _, deleted := tx.deletedEdges[id]; deleted {
		return true
	}
	_, rewritten := tx.pendingEdges[id]
	return rewritten
}

// nodeExists checks if a node exists (pending or storage).
func (tx *BadgerTransaction) nodeExists(nodeID NodeID) bool {
	_, visible := tx.visibleNodeLabelsLocked(nodeID)
	return visible
}

// visibleNodeLabelsLocked reports whether a node exists for the transaction
// (pending, or committed and visible at its snapshot, and not deleted by it)
// and returns its labels, from the one read that decides it. The committed
// read is a stored read: a node the visibility layer hides from user reads
// is still a relationship's endpoint. A failed read counts as not visible.
func (tx *BadgerTransaction) visibleNodeLabelsLocked(nodeID NodeID) ([]string, bool) {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, false
	}
	if node, exists := tx.pendingNodes[nodeID]; exists {
		return node.Labels, true
	}
	labels, err := tx.committedNodeLabelsLocked(nodeID)
	if err != nil {
		return nil, false
	}
	return labels, true
}

func (tx *BadgerTransaction) validateSnapshotIsolationConflicts() error {
	// Metadata handlers can publish a peer commit between the optimistic and
	// authoritative passes. Endpoint memoization belongs to one pass only.
	tx.nodeHeadCache = nil
	// The buffer first, the stored heads below after: a buffered delete
	// applied in between is found in its heads.
	if err := tx.bufferedDeleteConflictLocked(); err != nil {
		return err
	}
	// Idempotent hidden-edge CREATEs stage no writes. Re-read them at the
	// commit owner so an explicit transaction cannot acknowledge a no-op
	// against a predecessor another writer changed, revealed or deleted after
	// statement-time classification.
	for edgeID, candidate := range tx.edgeCreateNoOps {
		stored, err := tx.engine.GetStoredEdge(edgeID)
		if err != nil || !tx.engine.filterEntityByDecay(visibleRead, stored, DecayScoringTime()) {
			return ErrAlreadyExists
		}
		same, err := tx.sameEdgeCreateIdentityLocked(stored, candidate)
		if err != nil {
			return err
		}
		if !same {
			return &HiddenEdgeIdentityConflictError{EdgeID: edgeID, Namespace: tx.namespace}
		}
	}
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if !op.FreshID {
				if err := tx.checkNodeCreateConflict(op.NodeID); err != nil {
					return err
				}
			}
		case OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
			if err := tx.checkNodeWriteConflict(op.NodeID); err != nil {
				return err
			}
			if op.Type == OpDeleteNode {
				if err := tx.checkNodeAdjacencyConflict(op.NodeID); err != nil {
					return err
				}
			}
		case OpCreateEdge:
			// Created and deleted in the same transaction: nothing reaches
			// commit, so neither the create conflict nor the endpoint checks
			// apply (#741: DELETE r / DETACH DELETE after CREATE in one tx).
			if _, deleted := tx.deletedEdges[op.EdgeID]; deleted {
				continue
			}
			if !op.FreshID {
				if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
					return err
				}
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpUpdateEdge:
			if _, deleted := tx.deletedEdges[op.EdgeID]; deleted {
				continue
			}
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpDeleteEdge:
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
		}
	}
	return nil
}

// snapshotIsolationConflict reports true when the head version was
// committed STRICTLY AFTER the transaction began, comparing on the
// per-namespace monotonic commit sequence rather than on wall-clock
// timestamps.
//
// Why seq-only and not Compare(): MVCCVersion.Compare orders by
// timestamp first and breaks ties on sequence. Timestamps are wall-
// clock UnixNano and can be non-monotonic across goroutines on
// containerized Linux runners under NTP correction, which leaks into
// the SI conflict check as a false positive: the head was committed
// SAME-SEQ as our readTS but with a slightly LATER wall timestamp,
// and Compare> returns true even though no concurrent tx actually
// wrote between BeginTransaction and Commit. Each namespace's commit
// sequence is an atomic uint64 incremented at every
// allocateMVCCVersion call for that namespace — it cannot move
// non-monotonically. A real concurrent commit in the same namespace
// MUST bump that namespace's seq past tx.readTS.CommitSequence to
// have written anything. Cross-namespace comparisons never reach
// this function because BadgerTransaction is pinned to one namespace.
// When a namespace's sequence saturates at MaxUint64 we can no longer
// distinguish commits by seq, so allocateMVCCVersion forces strictly
// increasing commit timestamps and this check falls back to
// timestamp ordering only for the equal-MaxUint64 case.
// snapshotIsolationConflict reports whether headVersion is logically newer
// than the transaction's snapshot. Timestamps order commits: the
// write-behind flusher backdates its heads to the buffered ACK time, so a
// head the flusher lands after this transaction began can still be older
// than the snapshot — not a conflict.
func (tx *BadgerTransaction) snapshotIsolationConflict(headVersion MVCCVersion) bool {
	return headVersion.Compare(tx.readTS) > 0
}

func (tx *BadgerTransaction) checkNodeCreateConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	conflict, err := tx.snapshotHeadConflict(tx.engine.mvccNodeHeadKeyStringLookup(nodeID), head.Version)
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if conflict {
		// Wire contract: substrings "conflict:" and "changed after transaction start" are
		// matched by downstream Bolt classifiers as transient.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.2.
		return localizedError(localization.StorageTransactionNodeChanged(string(nodeID)), ErrConflict)
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeWriteConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	conflict, err := tx.snapshotHeadConflict(tx.engine.mvccNodeHeadKeyStringLookup(nodeID), head.Version)
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if conflict {
		// MERGE-on-same-unique-value race: this transaction's MERGE
		// matched a node that a peer committed between our begin and
		// our commit. The match → UpdateNode redirect onto the peer's
		// node would normally fire as ErrConflict here, but the
		// observable cause is "another writer claimed the unique
		// value" — the consumer-pinned commit-time UNIQUE shape.
		// Surface that shape directly so retry-aware MERGE clients
		// converge instead of being told to retry on a transient
		// conflict (which would just observe the peer's node, find
		// the constraint already satisfied, and complete cleanly).
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		if cv := tx.classifyUpdateConflictAsUniqueViolation(nodeID); cv != nil {
			return cv
		}
		return localizedError(localization.StorageTransactionNodeChangedDetailed(string(nodeID), head.Version.String(), tx.readTS.String()), ErrConflict)
	}
	return nil
}

// classifyUpdateConflictAsUniqueViolation returns a ConstraintViolationError
// when this transaction's UPDATE on nodeID is the loser of a commit-time
// unique-value race — i.e., a peer claimed a uniquely-constrained value
// this transaction's pending node also carries, and our MERGE-MATCH path
// redirected our UPDATE onto the peer's node. Returns nil for any conflict
// that isn't shaped this way (lost-update on a contended counter, generic
// SI conflict on an unrelated update, etc.); the caller falls back to
// the generic conflict error.
func (tx *BadgerTransaction) classifyUpdateConflictAsUniqueViolation(nodeID NodeID) error {
	pending, ok := tx.pendingNodes[nodeID]
	if !ok || pending == nil || tx.namespace == "" {
		return nil
	}
	// Reuse the retained mutation preimage; ordinary reads retain no claims.
	preimages, distinctUpdates := tx.nodeMutationPreimagesLocked()
	before := preimages[nodeID]
	if distinctUpdates {
		for index := range tx.operations {
			operation := &tx.operations[index]
			if operation.NodeID == nodeID {
				before = operation.nodeClaimPreimage()
				break
			}
		}
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if schema == nil {
		return nil
	}
	for _, c := range schema.GetConstraintsForLabels(pending.Labels) {
		if c.Type != ConstraintUnique || len(c.Properties) != 1 {
			continue
		}
		prop := c.Properties[0]
		val, has := pending.Properties[prop]
		if !has {
			continue
		}
		if sameNodeUniqueClaim(before, pending, c.Label, prop) {
			continue
		}
		existing, found, constrained, complete := schema.LookupUniqueConstraintValue(c.Label, prop, val)
		if !constrained || !complete || !found {
			continue
		}
		if existing != nodeID {
			continue
		}
		// Peer claimed our unique value; the schema cache has already
		// recorded their (label, prop, value) → nodeID mapping, and
		// our pending update targets that same nodeID. This is the
		// commit-time UNIQUE race the contract documents.
		message := localization.StorageValidationNodeUniqueConcurrent(prop, val)
		cause := localizedError(localization.StorageTransactionConcurrentModification(), ErrConflict)
		return newLocalizedConstraintViolation(ConstraintUnique, c.Label, []string{prop}, message, cause)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeWriteConflict(edgeID EdgeID) error {
	head, err := tx.engine.GetEdgeCurrentHead(edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	conflict, err := tx.snapshotHeadConflict(tx.engine.mvccEdgeHeadKeyStringLookup(edgeID), head.Version)
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if conflict {
		return localizedError(localization.StorageTransactionEdgeChanged(string(edgeID)), ErrConflict)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeEndpointConflicts(edge *Edge) error {
	if edge == nil {
		return nil
	}
	// Staging validates both endpoints. Validate the final staged image,
	// rather than endpoints of an intermediate update the transaction replaced.
	staged, stagedTarget := tx.pendingEdges[edge.ID]
	if stagedTarget {
		edge = staged
	}
	missingEndpoint := func(nodeID NodeID) error {
		if stagedTarget {
			// A deletion may reserve its MVCC version before this frame
			// begins, yet publish after endpoint validation. The target's
			// disappearance is a conflict regardless of reservation order.
			return localizedError(localization.StorageTransactionEndpointDeleted(string(nodeID)), ErrConflict)
		}
		return ErrInvalidEdge
	}
	for _, nodeID := range []NodeID{edge.StartNode, edge.EndNode} {
		if pending, exists := tx.pendingNodes[nodeID]; exists {
			if pending != nil {
				continue
			}
		}
		head, err := tx.endpointHead(nodeID)
		if err == ErrNotFound {
			if _, nodeErr := tx.engine.GetStoredNode(nodeID); nodeErr == nil {
				continue
			} else if nodeErr == ErrNotFound {
				return missingEndpoint(nodeID)
			} else {
				return &CommitFailureError{Cause: nodeErr}
			}
		}
		if err != nil {
			return &CommitFailureError{Cause: err}
		}
		if head.Tombstoned {
			// An acknowledged re-create the write-behind buffer has not
			// applied yet is the endpoint's current state.
			if _, nodeErr := tx.engine.GetStoredNode(nodeID); nodeErr == nil {
				continue
			} else if nodeErr != ErrNotFound {
				return &CommitFailureError{Cause: nodeErr}
			}
			conflict, err := tx.snapshotHeadConflict(tx.engine.mvccNodeHeadKeyStringLookup(nodeID), head.Version)
			if err != nil {
				return &CommitFailureError{Cause: err}
			}
			if conflict {
				return localizedError(localization.StorageTransactionEndpointDeleted(string(nodeID)), ErrConflict)
			}
			return missingEndpoint(nodeID)
		}
	}
	return nil
}

// endpointHead returns the committed MVCC head of nodeID, memoized per
// transaction: edge-batch validation reads each endpoint once per distinct
// node instead of twice per edge. The cache is only read within a single
// commit validation pass, which is point-in-time by construction, so the
// memoized value is exactly as fresh as the first read.
func (tx *BadgerTransaction) endpointHead(nodeID NodeID) (MVCCHead, error) {
	if head, ok := tx.nodeHeadCache[nodeID]; ok {
		return head, nil
	}
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == nil {
		if tx.nodeHeadCache == nil {
			tx.nodeHeadCache = make(map[NodeID]MVCCHead, 64)
		}
		tx.nodeHeadCache[nodeID] = head
	}
	return head, err
}

func (tx *BadgerTransaction) checkNodeAdjacencyConflict(nodeID NodeID) error {
	err := tx.engine.withView(func(viewTx *badger.Txn) error {
		var prefixes [][]byte
		if outPrefix := tx.engine.outgoingIndexPrefixString(nodeID); outPrefix != nil {
			prefixes = append(prefixes, outPrefix)
		}
		if inPrefix := tx.engine.incomingIndexPrefixString(nodeID); inPrefix != nil {
			prefixes = append(prefixes, inPrefix)
		}
		for _, prefix := range prefixes {
			opts := badgerIteratorOptions()
			opts.Prefix = prefix
			it := viewTx.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().Key())
				if !ok {
					continue
				}
				edgeID, ok := tx.engine.idDict.lookupEdgeIDByNum(edgeNum)
				if !ok {
					continue
				}
				head, physicalVersion, err := tx.engine.loadEdgeMVCCHeadByNumRawWithPhysicalVersionInTxn(viewTx, edgeNum)
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					it.Close()
					return err
				}
				if tx.snapshotHeadVersionConflict(head.Version, physicalVersion) {
					it.Close()
					return localizedError(localization.StorageTransactionAdjacentEdgeChanged(string(nodeID), string(edgeID)), ErrConflict)
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrConflict) {
		return &CommitFailureError{Cause: err}
	}
	return err
}

// shouldSkipCreateExistenceCheck is the legacy name of the UUID predicate now
// used only for the commit-time MVCC head reload optimization.
func shouldSkipCreateExistenceCheck(id string) bool {
	return hasUUIDShape(id)
}

// hasUUIDShape reports whether an id is a namespace-prefixed UUID. Because
// UUIDv4 collisions are astronomically unlikely, a freshly minted UUID cannot
// refer to a previously deleted entity — so callers that mint IDs this way can
// safely skip the MVCC head load-before-write during commit after the
// stored-inclusive create check already proved the physical ID absent.
func hasUUIDShape(id string) bool {
	_, rawID, ok := ParseDatabasePrefix(id)
	if !ok {
		return false
	}
	_, err := uuid.Parse(rawID)
	return err == nil
}

// validateNodeConstraints checks all constraints for a node.
//
// The transaction is pinned to a single namespace at the first prefixed
// write (see pinNamespaceFromIDLocked); all writes therefore share that
// namespace and the schema lookup is cached on the transaction rather than
// re-derived from each node ID. Direct callers (tests, internal helpers)
// that have not yet performed a pinned write will pin here on first use.
func (tx *BadgerTransaction) validateNodeConstraints(node *Node) error {
	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}
	return (nodeConstraintValidation{schema: tx.engine.GetSchemaForNamespace(tx.namespace), transaction: tx}).validate(node)
}

// checkUniqueConstraint ensures property value is unique across ALL data.
//
// All pending nodes share the transaction's pinned namespace by invariant
// (see pinNamespaceFromIDLocked), so the in-tx scan does not need to
// re-filter by namespace prefix. The committed-data fallback still filters
// because the label index spans all namespaces.
func (tx *BadgerTransaction) checkUniqueConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return nil // NULL doesn't violate uniqueness
	}

	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}

	if err := tx.forEachPendingCandidateLocked(node.ID, c.Label, prop, value, func(*Node) error {
		message := localization.StorageValidationNodeUniqueInTransaction(prop, value)
		return newLocalizedConstraintViolation(ConstraintUnique, c.Label, []string{prop}, message, nil)
	}); err != nil {
		return err
	}

	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if existingNode, found, constrained, cacheComplete := schema.LookupUniqueConstraintValue(c.Label, prop, value); constrained && cacheComplete {
		if found && existingNode != node.ID && !tx.committedNodeSupersededLocked(existingNode) {
			return uniqueConstraintViolation(c.Label, prop, value, existingNode)
		}
		return nil
	}

	// If the derived unique-value cache is not complete, fall back to
	// the label scan. Normal schema creation/reload marks the cache complete
	// once after rebuilding it from stored nodes, so hot writes avoid this path.
	return tx.scanForUniqueViolation(tx.namespace, c.Label, prop, value, node.ID)
}

func uniqueConstraintViolation(label, property string, value interface{}, nodeID NodeID) *ConstraintViolationError {
	message := localization.StorageValidationNodeUniqueExisting(property, value, string(nodeID))
	return newLocalizedConstraintViolation(ConstraintUnique, label, []string{property}, message, nil)
}

// uniqueConstraintScanHook lets storage tests assert that indexed UNIQUE
// validation does not regress to the label-scan fallback.
var uniqueConstraintScanHook func()
var uniqueConstraintScanHookMu sync.RWMutex

func setUniqueConstraintScanHook(hook func()) func() {
	uniqueConstraintScanHookMu.Lock()
	previousHook := uniqueConstraintScanHook
	uniqueConstraintScanHook = hook
	uniqueConstraintScanHookMu.Unlock()

	return func() {
		uniqueConstraintScanHookMu.Lock()
		uniqueConstraintScanHook = previousHook
		uniqueConstraintScanHookMu.Unlock()
	}
}

func getUniqueConstraintScanHook() func() {
	uniqueConstraintScanHookMu.RLock()
	defer uniqueConstraintScanHookMu.RUnlock()
	return uniqueConstraintScanHook
}

// commitTailHook lets storage tests interpose between a successful Badger
// commit and the post-commit tail (derived counts, MVCC sequence, ID
// counters, caches, callbacks) that publishes it. It exists to pin the
// Close-versus-commit ordering; production leaves it nil.
var (
	commitTailHook   func()
	commitTailHookMu sync.RWMutex
)

func setCommitTailHook(hook func()) func() {
	commitTailHookMu.Lock()
	previousHook := commitTailHook
	commitTailHook = hook
	commitTailHookMu.Unlock()

	return func() {
		commitTailHookMu.Lock()
		commitTailHook = previousHook
		commitTailHookMu.Unlock()
	}
}

func runCommitTailHook() {
	commitTailHookMu.RLock()
	hook := commitTailHook
	commitTailHookMu.RUnlock()
	if hook != nil {
		hook()
	}
}

// forEachPendingCandidateLocked calls visit for the transaction's other
// pending nodes with label whose property equals value: the candidates a
// UNIQUE, NODE KEY or TEMPORAL check has to compare against. It reads them
// from the transaction's pending-node index (built on first use, kept current
// on every pending write), so checking each node of an n-node transaction
// visits only the nodes sharing its key instead of every pending node; the
// pass over all pending nodes made a large bulk write O(n^2), once while the
// statement wrote and again at commit. Values the index cannot key (no
// indexValueKey) fall back to that pass. Index keys are never stricter than
// compareValues (they are looser for lists, e.g. [1] and [1.0] share a key),
// so candidates are also checked with compareValues: the result is exactly
// what the full pass found.
// Caller holds tx.mu; visit must not write to the transaction.
func (tx *BadgerTransaction) forEachPendingCandidateLocked(self NodeID, label, property string, value interface{}, visit func(*Node) error) error {
	if valueKey, ok := indexValueKey(value); ok {
		for _, id := range tx.pendingValueMatchesLocked(label, property, valueKey) {
			if id == self {
				continue
			}
			if n := tx.pendingNodes[id]; n != nil && compareValues(n.Properties[property], value) {
				if err := visit(n); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for id, n := range tx.pendingNodes {
		if id == self || !hasLabel(n.Labels, label) || !compareValues(n.Properties[property], value) {
			continue
		}
		if err := visit(n); err != nil {
			return err
		}
	}
	return nil
}

// writeBehindKeyCandidatesLocked returns the newest buffered version of the
// unflushed nodes with label whose property equals value: the nodes an
// index-backed constraint check must compare besides the index, which is
// maintained at flush. nil without write-behind. It reads the buffer's pending
// value index, so the cost is the matching nodes, not the label's unflushed
// rows; a value without an index key falls back to the label's whole overlay.
// Callers compare the full key / interval themselves.

// scanForUniqueViolation performs a full database scan to check for UNIQUE violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForUniqueViolation(namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}

	nodes, err := tx.committedConstraintNodesLocked(label)
	if err != nil {
		return err
	}
	for i, existingNode := range nodes {
		if i%commitCancelPollEvery == 0 {
			if err := tx.commitCancelledLocked(); err != nil {
				return err
			}
		}
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}
		if existingValue, ok := existingNode.Properties[property]; ok && compareValues(existingValue, value) {
			message := localization.StorageValidationNodeUniqueExisting(property, value, string(existingNode.ID))
			return newLocalizedConstraintViolation(ConstraintUnique, label, []string{property}, message, nil)
		}
	}

	return nil
}

// checkNodeKeyConstraint ensures composite key uniqueness across ALL data.
func (tx *BadgerTransaction) checkNodeKeyConstraint(node *Node, c Constraint) error {
	values, missing := nodeConstraintKeyValues(node, c.Properties)
	if missing >= 0 {
		if c.Type == ConstraintUnique {
			return nil
		}
		message := localization.StorageValidationNodeKeyNull(c.Properties[missing])
		return newLocalizedConstraintViolation(ConstraintNodeKey, c.Label, c.Properties, message, nil)
	}

	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}

	// All pending nodes share the transaction's pinned namespace by
	// invariant; the namespace-prefix filter that used to live here is
	// redundant.
	if err := tx.forEachPendingCandidateLocked(node.ID, c.Label, c.Properties[0], values[0], func(n *Node) error {
		for i := 1; i < len(c.Properties); i++ {
			if !compareValues(n.Properties[c.Properties[i]], values[i]) {
				return nil
			}
		}
		message := localization.StorageValidationNodeKeyInTransaction(c.Properties, values)
		return newLocalizedConstraintViolation(c.Type, c.Label, c.Properties, message, nil)
	}); err != nil {
		return err
	}

	// Committed nodes: with a filled equality index on exactly the key's
	// properties, only the nodes it lists under this key can collide; each is
	// re-read at the latest committed state and compared as the scan does.
	// Commits update the index before releasing the key's commit lock, so a
	// concurrent commit of the same key is listed or still holds the lock.
	if idx := tx.engine.GetSchemaForNamespace(tx.namespace).filledKeyIndex(c.Label, c.Properties); idx != nil {
		candidates, err := tx.engine.keyConstraintNodes(idx, c.Label, c.Properties[0], values)
		if err != nil {
			return err
		}
		for _, existing := range candidates {
			if existing.ID == node.ID || tx.committedNodeSupersededLocked(existing.ID) || !nodeHoldsKey(existing, tx.namespace, c.Label, c.Properties, node.Properties) {
				continue
			}
			message := localization.StorageValidationNodeCompositeKeyExisting(c.Properties, values, string(existing.ID))
			return newLocalizedConstraintViolation(c.Type, c.Label, c.Properties, message, nil)
		}
		return nil
	}

	// Full-scan check: scan all existing nodes with this label (namespace-scoped).
	if err := tx.scanForNodeKeyViolation(tx.namespace, c.Label, c.Properties, values, node.ID); err != nil {
		if violation, ok := err.(*ConstraintViolationError); ok {
			violation.Type = c.Type
		}
		return err
	}

	return nil
}

// committedKeyHolder returns candidate id's committed node when it holds the
// key (nodeHoldsKey): the MERGE admission read of a composite equality-index
// candidate.
func (b *BadgerEngine) committedKeyHolder(id NodeID, namespace, label string, properties []string, props map[string]interface{}) *Node {
	existing, err := b.GetNode(id)
	if err != nil || existing == nil || !nodeHoldsKey(existing, namespace, label, properties, props) {
		return nil
	}
	return existing
}

// nodeHoldsKey reports whether node belongs to namespace, carries label and
// equals props exactly on every key property: the one test that turns a
// composite equality-index candidate into the key's holder, for commit
// validation and MERGE admission alike. The index lists a node under its last
// indexed values, so the node's current values decide.
func nodeHoldsKey(node *Node, namespace, label string, properties []string, props map[string]interface{}) bool {
	id := node.ID
	if namespace != "" && !(len(id) > len(namespace) && strings.HasPrefix(string(id), namespace) && id[len(namespace)] == ':') {
		return false
	}
	if !hasLabel(node.Labels, label) {
		return false
	}
	for _, prop := range properties {
		if v, ok := node.Properties[prop]; !ok || !compareValues(v, props[prop]) {
			return false
		}
	}
	return true
}

// scanForNodeKeyViolation performs a full database scan to check for NODE KEY violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForNodeKeyViolation(namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	if hook := nodeKeyScanHook.Load(); hook != nil {
		(*hook)()
	}
	nodes, err := tx.committedConstraintNodesLocked(label)
	if err != nil {
		return err
	}
	for i, existingNode := range nodes {
		if i%commitCancelPollEvery == 0 {
			if err := tx.commitCancelledLocked(); err != nil {
				return err
			}
		}
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}

		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}

		if match {
			message := localization.StorageValidationNodeCompositeKeyExisting(properties, values, string(existingNode.ID))
			return newLocalizedConstraintViolation(ConstraintNodeKey, label, properties, message, nil)
		}
	}

	return nil
}

// checkExistenceConstraint ensures required property exists.
func (tx *BadgerTransaction) checkExistenceConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		message := localization.StorageValidationRequiredPropertyMissing(prop)
		return newLocalizedConstraintViolation(ConstraintExists, c.Label, []string{prop}, message, nil)
	}

	return nil
}

// checkTemporalConstraint enforces TEMPORAL NO OVERLAP constraints within a transaction.
// Every property before the trailing (valid_from, valid_to) pair forms the
// grouping key (see temporalKeySpec).
//
// It must validate against:
// - other pending nodes in this transaction (read-your-writes), and
// - existing committed nodes in storage (via the label index scan).
func (tx *BadgerTransaction) checkTemporalConstraint(node *Node, c Constraint) error {
	spec, ok := splitTemporalKeySpec(c.Properties)
	if !ok {
		return localizedError(localization.StorageValidationTemporalPropertiesAtLeastThree(), nil)
	}

	keyVal, missing := spec.keyValue(node.Properties)
	if missing != "" {
		message := localization.StorageValidationTemporalKeyNull(missing)
		return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
	}

	interval, ok := spec.interval(node.Properties)
	if !ok {
		message := localization.StorageValidationTemporalStartInvalid(spec.startProp)
		return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
	}

	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}
	nsPrefix := tx.namespace + ":"

	collides := func(other *Node) error {
		if !spec.keyMatches(other.Properties, keyVal) {
			return nil
		}
		otherInterval, ok := spec.interval(other.Properties)
		if !ok {
			message := localization.StorageValidationTemporalNodeRequired(spec.startProp, string(other.ID))
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
		}
		if intervalsOverlap(interval, otherInterval) {
			message := localization.StorageValidationTemporalNodeOverlap(string(other.ID), spec.keyLabel(), keyVal)
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
		}
		return nil
	}

	// All pending nodes share the transaction's pinned namespace by
	// invariant; only the committed-data scan still needs the prefix filter
	// because the label index spans namespaces.
	if err := tx.forEachPendingCandidateLocked(node.ID, c.Label, spec.keyProps[0], node.Properties[spec.keyProps[0]], collides); err != nil {
		return err
	}

	// Committed nodes: while the constraint's history index is complete, only
	// the nodes in the history of this node's key can overlap it (#1126);
	// otherwise scan the label. Both read the latest committed state and skip
	// the nodes this transaction rewrites or deletes, whose pending versions
	// were checked above.
	if tx.engine.temporalIndexAuthoritative(tx.engine.GetSchemaForNamespace(tx.namespace), c) {
		history, ok, err := tx.engine.temporalKeyHistoryNodesLatest(tx.commitCtx, tx.namespace, c, node)
		if err != nil {
			return err
		}
		if ok {
			for _, other := range history {
				if other.ID == node.ID || tx.committedNodeSupersededLocked(other.ID) || !strings.HasPrefix(string(other.ID), nsPrefix) {
					continue
				}

				if err := collides(other); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Check overlaps against committed storage using the label index scan.
	runTemporalConstraintScanHook()
	visibleNodes, err := tx.committedConstraintNodesLocked(c.Label)
	if err != nil {
		return err
	}
	for _, other := range visibleNodes {
		if other == nil || other.ID == node.ID || !strings.HasPrefix(string(other.ID), nsPrefix) {
			continue
		}
		if err := collides(other); err != nil {
			return err
		}
	}
	return nil
}

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
// of transactions waiting for each other, or with ctx's error.
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
func (tx *BadgerTransaction) catchUpToNode(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
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
	newer, err := tx.snapshotHeadConflict(tx.engine.mvccNodeHeadKeyStringLookup(nodeID), head.Version)
	if err != nil || !newer {
		return err
	}
	if tx.observedLocked() {
		// Wire contract: substrings "conflict:" and "changed after transaction start" are
		// matched by downstream Bolt classifiers as transient.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.2.
		return localizedError(localization.StorageTransactionNodeChanged(string(nodeID)), ErrConflict)
	}
	return tx.advanceSnapshotLocked(false)
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
func (tx *BadgerTransaction) rebaseBadgerAfterDrainLocked() error {
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
	return tx.validateSnapshotIsolationConflicts()
}

// ResumeAfterIndependentCommit advances an implicit caller's read view while
// retaining its entire pending write set and rollback ownership. Explicit
// transactions never enter this path. Before advancing, old pending mutations
// are checked against their old snapshot so a batch cannot mask a conflict.
func (tx *BadgerTransaction) ResumeAfterIndependentCommit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	if tx.commitMode != CommitModeImplicit {
		return fmt.Errorf("independent-commit continuation requires an implicit transaction")
	}
	return tx.advanceSnapshotLocked(true)
}

func (tx *BadgerTransaction) advanceSnapshotLocked(preserveWrites bool) error {
	lifecycleSeq := tx.engine.prefixDrops.seq.Load()
	var bufferEpoch uint64
	if tx.engine.writeBehind != nil {
		bufferEpoch = tx.engine.writeBehind.readEpoch.Load()
	}
	snapshotTx, badgerReadTs, beginPresence, err := tx.engine.beginTransactionSnapshot(tx.badgerDB)
	if err != nil {
		return err
	}
	// Pin the next view before validating the old footprint. A publication
	// after validation then remains newer than the next view and is checked
	// again at commit; no interval can silently erase a write conflict.
	beginSnapshot := tx.engine.snapshotNamespaceVersions()
	if preserveWrites {
		if len(tx.operations) > 0 && tx.engine.prefixDroppedSince(tx.lifecycleSeq, tx.namespace) {
			err = fmt.Errorf("%w: namespace %q was deleted or rewritten by maintenance before continuation", ErrConflict, tx.namespace)
		} else if tx.schemaRuntime != nil && tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence != tx.readTS.CommitSequence {
			err = fmt.Errorf("schema snapshot changed before continuation: %w", ErrConflict)
		} else {
			err = tx.validateSnapshotIsolationConflicts()
		}
		if err != nil {
			snapshotTx.Discard()
			beginPresence.release()
			tx.badgerDB.endRead(badgerReadTs)
			return err
		}
	}
	nextWriter := tx.badgerDB.newTxnAt(badgerReadTs, true)
	if tx.badgerTx != nil && tx.engine.idDict != nil {
		if preserveWrites {
			tx.engine.idDict.moveTxnCounters(tx.badgerTx, nextWriter)
		} else {
			tx.engine.idDict.discardTxnCounters(tx.badgerTx)
		}
	}
	if tx.badgerTx != nil && tx.engine.propKeyDict != nil {
		if preserveWrites {
			tx.engine.propKeyDict.moveTxnCounters(tx.badgerTx, nextWriter)
		} else {
			tx.engine.propKeyDict.discardTxnCounters(tx.badgerTx)
		}
	}
	if tx.badgerTx != nil {
		tx.badgerTx.Discard()
	}
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
	}
	if tx.badgerReadHeld {
		tx.badgerDB.endRead(tx.badgerReadTs)
		tx.badgerReadHeld = false
	}
	tx.beginPresence.release()
	tx.beginPresence = beginPresence
	tx.snapshotTx = snapshotTx
	tx.badgerReadTs = badgerReadTs
	tx.badgerReadHeld = true
	tx.badgerTx = nextWriter
	tx.lifecycleSeq = lifecycleSeq
	tx.bufferEpoch = bufferEpoch
	tx.beginSnapshot = beginSnapshot
	tx.clearSnapshotCachesLocked()
	return tx.refreshReadVersionForNamespaceLocked()
}

func (tx *BadgerTransaction) validateAllConstraints() error {
	checked := 0
	for _, node := range tx.pendingNodes {
		if checked%commitCancelPollEvery == 0 {
			if err := tx.commitCancelledLocked(); err != nil {
				return err
			}
		}
		checked++
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}
	// Deferred updates skip the per-operation check; revalidate final labels before publishing.
	for _, op := range tx.operations {
		if op.Type != OpUpdateNode || op.OldNode == nil {
			continue
		}
		node, exists := tx.pendingNodes[op.NodeID]
		if !exists || labelsEqual(node.Labels, op.OldNode.Labels) {
			continue
		}
		if err := tx.validatePolicyOnNodeLabelChange(node, op.OldNode); err != nil {
			return err
		}
	}
	for _, edge := range tx.pendingEdges {
		if err := tx.validateEdgeConstraints(edge); err != nil {
			return err
		}
	}
	if err := tx.validateConstraintContracts(); err != nil {
		return err
	}
	return nil
}

// validateEdgeConstraints checks relationship constraints for an edge in a transaction.
func (tx *BadgerTransaction) validateEdgeConstraints(edge *Edge) error {
	if edge == nil || edge.Type == "" {
		return nil
	}
	if tx.namespace == "" {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if schema == nil {
		return nil
	}

	constraints := schema.GetConstraintsForLabels([]string{edge.Type})
	for _, c := range constraints {
		if c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		switch c.Type {
		case ConstraintUnique:
			if err := tx.checkEdgeUniqueness(edge, c, tx.namespace); err != nil {
				return err
			}
		case ConstraintExists:
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
		case ConstraintRelationshipKey:
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
			if err := tx.checkEdgeUniqueness(edge, c, tx.namespace); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := tx.checkEdgeTemporalConstraint(edge, c, tx.namespace); err != nil {
				return err
			}
		case ConstraintDomain:
			if len(c.Properties) == 1 && len(c.AllowedValues) > 0 {
				prop := c.Properties[0]
				value := edge.Properties[prop]
				if value != nil && !isValueInAllowedList(value, c.AllowedValues) {
					message := localization.StorageValidationPropertyDomainInvalid(prop, value, c.AllowedValues)
					return newLocalizedConstraintViolation(ConstraintDomain, edge.Type, []string{prop}, message, nil)
				}
			}
		case ConstraintCardinality:
			if err := tx.checkEdgeCardinality(edge, c, tx.namespace); err != nil {
				return err
			}
		}
	}

	// Policy constraints must be evaluated as a set (all ALLOWED policies form a union).
	if err := tx.checkEdgePolicy(edge, schema, tx.namespace); err != nil {
		return err
	}

	ptConstraints := schema.GetPropertyTypeConstraintsForLabels([]string{edge.Type})
	for _, ptc := range ptConstraints {
		if ptc.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		value := edge.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			message := localization.StorageValidationPropertyTypeRequired(ptc.Property, string(ptc.ExpectedType), err)
			return newLocalizedConstraintViolation(ConstraintPropertyType, edge.Type, []string{ptc.Property}, message, err)
		}
	}

	return nil
}

// checkEdgeUniqueness checks uniqueness constraints for an edge against pending and committed edges.
// An edge with a null constrained property never collides. The namespace
// parameter filters committed edges (which span all namespaces in the
// underlying engine) to the transaction's pinned namespace; all pending edges
// already belong to that namespace by invariant.
func (tx *BadgerTransaction) checkEdgeUniqueness(edge *Edge, c Constraint, namespace string) error {
	values := make([]interface{}, len(c.Properties))
	for i, prop := range c.Properties {
		if values[i] = edge.Properties[prop]; values[i] == nil {
			return nil
		}
	}
	collides := func(other *Edge) error {
		for i, prop := range c.Properties {
			otherVal := other.Properties[prop]
			if otherVal == nil || !compareValues(otherVal, values[i]) {
				return nil
			}
		}
		if len(c.Properties) == 1 {
			message := localization.StorageValidationRelationshipUniqueExisting(c.Properties[0], values[0], string(other.ID))
			return newLocalizedConstraintViolation(c.Type, edge.Type, c.Properties, message, nil)
		}
		message := localization.StorageValidationRelationshipCompositeExisting(string(other.ID))
		return newLocalizedConstraintViolation(c.Type, edge.Type, c.Properties, message, nil)
	}

	for id, otherEdge := range tx.pendingEdges {
		if id == edge.ID || otherEdge.Type != edge.Type {
			continue
		}
		if err := collides(otherEdge); err != nil {
			return err
		}
	}

	if candidates, indexed, err := tx.engine.relationshipUniqueCandidates(edge, c, namespace); err != nil {
		return err
	} else if indexed {
		for _, candidate := range candidates {
			id := EdgeID(candidate)
			if id == edge.ID || tx.committedEdgeSupersededLocked(id) {
				continue
			}
			existing, err := tx.engine.GetStoredEdge(id)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if existing.Type != edge.Type {
				continue
			}
			if err := collides(existing); err != nil {
				return err
			}
		}
		return nil
	}

	// Check against committed edges via the engine
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}
	existingEdges, err := tx.engine.GetStoredEdgesByType(edge.Type)
	if err != nil {
		return err
	}
	nsPrefix := namespace + ":"
	for _, existingEdge := range existingEdges {
		if existingEdge.ID == edge.ID || tx.committedEdgeSupersededLocked(existingEdge.ID) {
			continue
		}
		// Filter to same namespace to avoid cross-database false positives
		if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
			continue
		}
		if err := collides(existingEdge); err != nil {
			return err
		}
	}

	return nil
}

// checkEdgeTemporalConstraint checks temporal no-overlap constraints for an edge in a transaction.
// The namespace parameter filters committed edges to the same database namespace.
// Supports composite key properties: all properties except the last 2 form the key.
func (tx *BadgerTransaction) checkEdgeTemporalConstraint(edge *Edge, c Constraint, namespace string) error {
	if len(c.Properties) < 3 {
		return localizedError(localization.StorageValidationTemporalPropertiesAtLeastThree(), nil)
	}

	keyProps := c.Properties[:len(c.Properties)-2]
	startProp := c.Properties[len(c.Properties)-2]
	endProp := c.Properties[len(c.Properties)-1]

	// Validate all key properties are non-null
	keyVals := make([]interface{}, len(keyProps))
	for i, prop := range keyProps {
		keyVals[i] = edge.Properties[prop]
		if keyVals[i] == nil {
			message := localization.StorageValidationTemporalKeyNull(prop)
			return newLocalizedConstraintViolation(ConstraintTemporal, edge.Type, c.Properties, message, nil)
		}
	}

	start, ok := CoerceTemporalTime(edge.Properties[startProp])
	if !ok {
		message := localization.StorageValidationTemporalStartInvalid(startProp)
		return newLocalizedConstraintViolation(ConstraintTemporal, edge.Type, c.Properties, message, nil)
	}
	end, hasEnd := CoerceTemporalTime(edge.Properties[endProp])
	newInterval := temporalInterval{start: start, end: end, hasEnd: hasEnd}

	// Check against pending edges in this transaction
	nsPrefix := namespace + ":"
	for id, otherEdge := range tx.pendingEdges {
		if id == edge.ID || otherEdge.Type != edge.Type {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !edgeTemporalCompositeKeyMatch(otherEdge, keyProps, keyVals) {
			continue
		}
		otherStart, ok := CoerceTemporalTime(otherEdge.Properties[startProp])
		if !ok {
			continue
		}
		otherEnd, otherHasEnd := CoerceTemporalTime(otherEdge.Properties[endProp])
		if intervalsOverlap(newInterval, temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd}) {
			message := localization.StorageValidationTemporalEdgeOverlap(string(id), keyVals)
			return newLocalizedConstraintViolation(ConstraintTemporal, edge.Type, c.Properties, message, nil)
		}
	}

	// Check against committed edges (filtered by namespace)
	existingEdges, err := tx.engine.GetStoredEdgesByType(edge.Type)
	if err != nil {
		return err
	}
	for _, existingEdge := range existingEdges {
		if existingEdge.ID == edge.ID || tx.committedEdgeSupersededLocked(existingEdge.ID) {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
			continue
		}
		if !edgeTemporalCompositeKeyMatch(existingEdge, keyProps, keyVals) {
			continue
		}
		existingStart, ok := CoerceTemporalTime(existingEdge.Properties[startProp])
		if !ok {
			continue
		}
		existingEnd, existingHasEnd := CoerceTemporalTime(existingEdge.Properties[endProp])
		if intervalsOverlap(newInterval, temporalInterval{start: existingStart, end: existingEnd, hasEnd: existingHasEnd}) {
			message := localization.StorageValidationTemporalEdgeOverlap(string(existingEdge.ID), keyVals)
			return newLocalizedConstraintViolation(ConstraintTemporal, edge.Type, c.Properties, message, nil)
		}
	}

	return nil
}

// checkEdgeCardinality enforces cardinality constraints within a transaction.
// It counts pending + committed edges of the given type connected to the anchor node,
// excluding deleted edges, and rejects the new edge if adding it would exceed MaxCount.
func (tx *BadgerTransaction) checkEdgeCardinality(edge *Edge, c Constraint, namespace string) error {
	// Determine anchor node based on direction.
	var anchorNode NodeID
	if c.Direction == "OUTGOING" {
		anchorNode = NodeID(edge.StartNode)
	} else {
		anchorNode = NodeID(edge.EndNode)
	}

	nsPrefix := namespace + ":"
	count := 0

	// Count pending edges in this transaction that match type + anchor + direction.
	for id, pendingEdge := range tx.pendingEdges {
		if id == edge.ID || pendingEdge.Type != c.Label {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		var pendingAnchor NodeID
		if c.Direction == "OUTGOING" {
			pendingAnchor = NodeID(pendingEdge.StartNode)
		} else {
			pendingAnchor = NodeID(pendingEdge.EndNode)
		}
		if pendingAnchor == anchorNode {
			count++
		}
	}

	// Count committed edges from the engine.
	var committedEdges []*Edge
	var err error
	if c.Direction == "OUTGOING" {
		committedEdges, err = tx.engine.GetStoredOutgoingEdges(anchorNode)
	} else {
		committedEdges, err = tx.engine.GetStoredIncomingEdges(anchorNode)
	}
	if err != nil {
		return err
	}
	for _, existingEdge := range committedEdges {
		if existingEdge.ID == edge.ID || existingEdge.Type != c.Label {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
			continue
		}
		// Edges deleted here don't count; rewritten ones are counted as pending.
		if tx.committedEdgeSupersededLocked(existingEdge.ID) {
			continue
		}
		count++
	}

	if count >= c.MaxCount {
		dir := strings.ToLower(c.Direction)
		message := localization.StorageValidationCardinalityExceeded(dir, c.MaxCount, c.Label, string(anchorNode), count)
		return newLocalizedConstraintViolation(ConstraintCardinality, c.Label, nil, message, nil)
	}

	return nil
}

// checkEdgePolicy validates DISALLOWED and ALLOWED endpoint policies within a transaction.
// Reads node labels from pending nodes first (read-your-writes), then falls back to committed storage.
func (tx *BadgerTransaction) checkEdgePolicy(edge *Edge, schema *SchemaManager, namespace string) error {
	constraints := schema.GetConstraintsForLabels([]string{edge.Type})

	var allowedPolicies []Constraint
	var disallowedPolicies []Constraint
	for _, c := range constraints {
		if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		if c.PolicyMode == "ALLOWED" {
			allowedPolicies = append(allowedPolicies, c)
		} else if c.PolicyMode == "DISALLOWED" {
			disallowedPolicies = append(disallowedPolicies, c)
		}
	}

	if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
		return nil
	}

	// Read source node labels (check pending first for read-your-writes).
	srcLabels, err := tx.getNodeLabels(NodeID(edge.StartNode))
	if err != nil {
		return err
	}
	if srcLabels == nil {
		return nil // Node not found; other validation catches this
	}

	// Read target node labels.
	tgtLabels, err := tx.getNodeLabels(NodeID(edge.EndNode))
	if err != nil {
		return err
	}
	if tgtLabels == nil {
		return nil
	}

	// Check DISALLOWED policies first (they take precedence).
	for _, c := range disallowedPolicies {
		if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
			message := localization.StorageValidationDisallowedPolicy(c.SourceLabel, c.Label, c.TargetLabel, c.Name)
			return newLocalizedConstraintViolation(ConstraintPolicy, c.Label, nil, message, nil)
		}
	}

	// Check ALLOWED policies: if any exist, at least one must match.
	if len(allowedPolicies) > 0 {
		matched := false
		for _, c := range allowedPolicies {
			if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
				matched = true
				break
			}
		}
		if !matched {
			message := localization.StorageValidationAllowedPolicy(strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":"))
			return newLocalizedConstraintViolation(ConstraintPolicy, edge.Type, nil, message, nil)
		}
	}

	return nil
}

// validatePolicyOnNodeLabelChange checks all policy constraints on edges connected to
// a node whose labels are changing within a transaction.
func (tx *BadgerTransaction) validatePolicyOnNodeLabelChange(node *Node, oldNode *Node) error {
	if tx.namespace == "" {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if schema == nil {
		return nil
	}

	nsPrefix := tx.namespace + ":"

	// Scan both outgoing and incoming edges.
	for _, isOutgoing := range []bool{true, false} {
		var edges []*Edge
		var err error
		if isOutgoing {
			edges, err = tx.engine.GetStoredOutgoingEdges(node.ID)
		} else {
			edges, err = tx.engine.GetStoredIncomingEdges(node.ID)
		}
		if err != nil {
			return err
		}

		// Pending updates replace committed edge types and endpoints.
		finalCommittedEdges := edges[:0]
		for _, edge := range edges {
			if _, replaced := tx.pendingEdges[edge.ID]; !replaced {
				finalCommittedEdges = append(finalCommittedEdges, edge)
			}
		}
		edges = finalCommittedEdges

		// Include pending edges in this transaction.
		for _, pendingEdge := range tx.pendingEdges {
			if isOutgoing && NodeID(pendingEdge.StartNode) == node.ID {
				edges = append(edges, pendingEdge)
			} else if !isOutgoing && NodeID(pendingEdge.EndNode) == node.ID {
				edges = append(edges, pendingEdge)
			}
		}

		for _, edge := range edges {
			if _, deleted := tx.deletedEdges[edge.ID]; deleted {
				continue
			}
			if !strings.HasPrefix(string(edge.ID), nsPrefix) {
				continue
			}

			// Get the other node's labels.
			var otherNodeID NodeID
			if isOutgoing {
				otherNodeID = NodeID(edge.EndNode)
			} else {
				otherNodeID = NodeID(edge.StartNode)
			}
			otherLabels, err := tx.getNodeLabels(otherNodeID)
			if err != nil {
				return err
			}
			if otherLabels == nil {
				continue
			}

			var srcLabels, tgtLabels []string
			if isOutgoing {
				srcLabels = node.Labels
				tgtLabels = otherLabels
			} else {
				srcLabels = otherLabels
				tgtLabels = node.Labels
			}

			// Check policy constraints.
			constraints := schema.GetConstraintsForLabels([]string{edge.Type})
			var allowedPolicies []Constraint
			var disallowedPolicies []Constraint
			for _, c := range constraints {
				if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
					continue
				}
				if c.PolicyMode == "ALLOWED" {
					allowedPolicies = append(allowedPolicies, c)
				} else if c.PolicyMode == "DISALLOWED" {
					disallowedPolicies = append(disallowedPolicies, c)
				}
			}

			if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
				continue
			}

			for _, c := range disallowedPolicies {
				if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
					message := localization.StorageValidationLabelChangeDisallowed(c.SourceLabel, c.Label, c.TargetLabel, c.Name)
					return newLocalizedConstraintViolation(ConstraintPolicy, c.Label, nil, message, nil)
				}
			}

			if len(allowedPolicies) > 0 {
				matched := false
				for _, c := range allowedPolicies {
					if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
						matched = true
						break
					}
				}
				if !matched {
					message := localization.StorageValidationLabelChangeAllowed(strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":"))
					return newLocalizedConstraintViolation(ConstraintPolicy, edge.Type, nil, message, nil)
				}
			}
		}
	}

	return nil
}

// getNodeLabels returns labels for a node, checking pending nodes first for read-your-writes.
// Storage failures are preserved for fail-closed policy validation.
func (tx *BadgerTransaction) getNodeLabels(nodeID NodeID) ([]string, error) {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		return pending.Labels, nil
	}
	node, err := tx.getCommittedNodeLocked(nodeID)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return node.Labels, nil
}

// Helper: check if node has label
func hasLabel(labels []string, target string) bool {
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
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

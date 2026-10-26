package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
)

// StoredEntityPublication is the immutable stored image delta of one logical
// commit. A physical replay of an acknowledged commit does not publish it again.
type StoredEntityPublication struct {
	Namespace string
	LogicalID string
	Changes   []StoredEntityChange
}

// StoredEntityObserver applies a commit's accounting before listeners run.
// It must not write to storage, acquire write admission, or mutate the images.
type StoredEntityObserver func(StoredEntityPublication) (publish func(), err error)

// StoredEntityReadKind identifies a snapshot-bound committed-state read.
type StoredEntityReadKind uint8

const (
	StoredEntityNodePriorRead StoredEntityReadKind = iota
	StoredEntityEdgePriorRead
	StoredEntityNodeSnapshotScan
	StoredEntityEdgeSnapshotScan
)

// StoredEntityReadBoundary decorates the actual read continuation. Implementers
// must call read exactly once, or return their read failure without calling it.
// The ID is physical and namespace-qualified; scans receive the namespace.
type StoredEntityReadBoundary interface {
	WithStoredEntityRead(StoredEntityReadKind, string, func() error) error
}

type storedEntityView struct {
	absent       bool
	boundaries   *storedEntityObserverRegistry
	read         embeddingRecordReader
	acknowledged *WriteBehindBuffer
	// selectedState is bound under the existing capture/transaction owner. It
	// avoids reacquiring retirement locks while that owner is already holding them.
	selectedState func(bufferedEntityKind, string) bufferedEntityState
}

// bufferedState selects the stored-inclusive typed overlay under its existing
// read lock. The record reader remains the caller's pinned transaction; hidden
// bodies and tombstones must mask that same committed predecessor.
func (v storedEntityView) bufferedState(b *BadgerEngine, kind bufferedEntityKind, id string) bufferedEntityState {
	if v.selectedState != nil {
		return v.selectedState(kind, id)
	}
	if v.acknowledged == nil {
		return bufferedEntityState{}
	}
	v.acknowledged.mu.RLock()
	defer v.acknowledged.mu.RUnlock()
	return v.acknowledged.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: storedRead})
}

func (v storedEntityView) node(b *BadgerEngine, id NodeID) (*Node, error) {
	var node *Node
	err := v.withSelectedEntity(b, bufferedNodeEntity, string(id), func(state bufferedEntityState) error {
		if !state.deleted {
			node = copyNode(state.node)
		}
		return nil
	}, func() error {
		return v.value(bufferedNodeEntity, string(id), func(data []byte) error { var err error; node, err = v.nodeRecord(b, id, data); return err })
	})
	return node, err
}

func (v storedEntityView) edge(b *BadgerEngine, id EdgeID) (*Edge, error) {
	if v.absent {
		return nil, nil
	}
	edge, _, err := b.readStoredEdgeImage(v.read, v.bufferedState(b, bufferedEdgeEntity, string(id)), id)
	return edge, err
}

// readStoredEdgeImage owns stored-inclusive overlay precedence and physical or
// staged record decoding. Callers retain their own read cut and missing-value
// contract; live visibility is applied after this complete image is resolved.
func (b *BadgerEngine) readStoredEdgeImage(reader embeddingRecordReader, state bufferedEntityState, id EdgeID) (*Edge, bool, error) {
	var edge *Edge
	source := storedEntityView{read: reader, selectedState: func(bufferedEntityKind, string) bufferedEntityState { return state }}
	err := source.withSelectedEntity(b, bufferedEdgeEntity, string(id), func(state bufferedEntityState) error {
		if !state.deleted {
			edge = copyEdge(state.edge)
		}
		return nil
	}, func() error {
		return source.value(bufferedEdgeEntity, string(id), func(data []byte) error { var err error; edge, err = source.edgeRecord(b, id, data); return err })
	})
	return edge, edge != nil, err
}

// captureStoredEntityChanges reads each typed touched entity once. Both views
// belong to this commit, never the latest mutable storage after publication.
func (b *BadgerEngine) captureStoredEntityChanges(ctx context.Context, keys []any, before, after storedEntityView) ([]StoredEntityChange, error) {
	seen := make(map[any]struct{}, len(keys))
	changes := make([]StoredEntityChange, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch key.(type) {
		case NodeID, EdgeID:
		default:
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var change StoredEntityChange
		var err error
		switch id := key.(type) {
		case NodeID:
			read := func() error { var err error; change.BeforeNode, err = before.node(b, id); return err }
			err = before.withBoundary(StoredEntityNodePriorRead, string(id), read)
			if err == nil {
				change.AfterNode, err = after.node(b, id)
			}
		case EdgeID:
			read := func() error { var err error; change.BeforeEdge, err = before.edge(b, id); return err }
			err = before.withBoundary(StoredEntityEdgePriorRead, string(id), read)
			if err == nil {
				change.AfterEdge, err = after.edge(b, id)
			}
		}
		if err != nil {
			return nil, err
		}
		if change.BeforeNode != nil || change.AfterNode != nil || change.BeforeEdge != nil || change.AfterEdge != nil {
			changes = append(changes, change)
		}
	}
	return changes, nil
}

// capturePreparedEntityChangesLocked uses the same stored-image reader as the
// physical writer. Staged maps overlay only the final view; acknowledged writes
// overlay the original committed view. Commit holds tx.mu and entity ownership.
func (tx *BadgerTransaction) capturePreparedEntityChangesLocked() ([]StoredEntityChange, error) {
	ctx := tx.validationContextLocked()
	operations, err := tx.physicalOperationsLocked(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := appendOperationPublicationKeysWithContext(ctx, nil, nil, operations)
	if err != nil {
		return nil, err
	}
	return tx.engine.captureStoredEntityChanges(ctx, keys,
		storedEntityView{read: embeddingRecordReader{txn: tx.badgerTx}, acknowledged: tx.engine.writeBehind, boundaries: tx.engine.storedEntityObservers.Load()},
		storedEntityView{read: embeddingRecordReader{txn: tx.badgerTx, stagedWrites: tx.pendingWrites, stagedDeletes: tx.pendingDeletes}})
}

// ManagedEntityPrepare is the authoritative managed leader's prepare/admit
// capability. Its terminal callback settles the existing usage reservation.
// Preparation never submits or applies the durable command.
type ManagedEntityPrepare func(context.Context, []Operation, EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error)

// ManagedEntityInitialize completes existing lazy usage initialization before
// a native write acquires lifecycle/key authority. Read-only BEGIN never calls it.
type ManagedEntityInitialize func(context.Context) error

type storedEntityObserverEntry struct {
	runtime    any
	initialize ManagedEntityInitialize
	preparer   ManagedEntityPrepare
	observer   StoredEntityObserver
	boundary   StoredEntityReadBoundary
}
type storedEntityObserverRegistry map[string]storedEntityObserverEntry

// StoredEntitySnapshot visits the exact stored entities at observer installation.
// It is valid only synchronously inside InitializeStoredEntityObserver's callback.
type StoredEntitySnapshot struct {
	engine    *BadgerEngine
	txn       *badger.Txn
	namespace string
	boundary  StoredEntityReadBoundary
}

// StoredEntityKind identifies a physical entity without reading its body.
type StoredEntityKind uint8

const (
	StoredEntityNodeKind StoredEntityKind = iota
	StoredEntityEdgeKind
)

// StoredEntitySnapshotEntry is an identity from a pinned installation snapshot.
// It and Read are valid only inside the InitializeStoredEntityObserver callback.
// No entity body or embedding is read until Read is explicitly requested.
type StoredEntitySnapshotEntry struct {
	Kind     StoredEntityKind
	ID       string
	snapshot StoredEntitySnapshot
}

// Read returns the complete stored final image, including separate embeddings,
// from the same pinned reader that produced this identity.
func (entry StoredEntitySnapshotEntry) Read() (StoredEntityChange, error) {
	view := storedEntityView{read: embeddingRecordReader{txn: entry.snapshot.txn}}
	var change StoredEntityChange
	var err error
	switch entry.Kind {
	case StoredEntityNodeKind:
		change.AfterNode, err = view.node(entry.snapshot.engine, NodeID(entry.ID))
	case StoredEntityEdgeKind:
		change.AfterEdge, err = view.edge(entry.snapshot.engine, EdgeID(entry.ID))
	default:
		return StoredEntityChange{}, ErrInvalidData
	}
	return change, err
}

// ForEach streams typed stored identities through the same scan fault boundary.
// Call entry.Read only when a complete stored image is needed.
func (s StoredEntitySnapshot) ForEach(visit func(StoredEntitySnapshotEntry) error) error {
	return s.ForEachWithContext(context.Background(), visit)
}

// ForEachWithContext observes the caller while preparing a cold managed ledger.
// It uses the same pinned identities, body reader and installed fault boundary.
func (s StoredEntitySnapshot) ForEachWithContext(ctx context.Context, visit func(StoredEntitySnapshotEntry) error) error {
	for _, kind := range []byte{prefixNode, prefixEdge} {
		if err := ctx.Err(); err != nil {
			return err
		}
		prefix := append([]byte{kind}, []byte(s.namespace+":")...)
		it := s.txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		scan := func() error {
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				id := string(it.Item().Key()[1:])
				entityKind := StoredEntityNodeKind
				if kind == prefixEdge {
					entityKind = StoredEntityEdgeKind
				}
				if err := visit(StoredEntitySnapshotEntry{Kind: entityKind, ID: id, snapshot: s}); err != nil {
					return err
				}

			}
			return nil
		}
		var err error
		if s.boundary != nil {
			kindOfRead := StoredEntityNodeSnapshotScan
			if kind == prefixEdge {
				kindOfRead = StoredEntityEdgeSnapshotScan
			}
			err = s.boundary.WithStoredEntityRead(kindOfRead, s.namespace, scan)
		} else {
			err = scan()
		}
		it.Close()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// InitializeStoredEntityObserver owns one exclusive stored snapshot/install cut.
// The caller must not hold its admission lock while waiting for this cut. The
// initializer streams initial images, atomically initializes its own ledger and
// returns the observer installed before any subsequent writer is admitted.
// Neither initializer nor observer may reenter storage or write admission.
func (b *BadgerEngine) InitializeStoredEntityObserver(namespace string, initialize func(StoredEntitySnapshot) (StoredEntityObserver, error), boundary StoredEntityReadBoundary) error {
	lease, err := b.beginBarrierWrite(true, true)
	if err != nil {
		return err
	}
	defer func() { lease.release(); b.completeCommitNotification(lease.held) }()
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return err
	}
	if b.writeBehind != nil {
		if err := b.writeBehind.flush(false, false); err != nil {
			return err
		}
	}
	b.db.commitGate.Lock()
	defer b.db.commitGate.Unlock()
	b.db.oracle.stage.apply(0)
	var observer StoredEntityObserver
	err = b.withView(func(txn *badger.Txn) error {
		var err error
		observer, err = initialize(StoredEntitySnapshot{engine: b, txn: txn, namespace: namespace, boundary: boundary})
		return err
	})
	if err != nil {
		return err
	}
	b.updateStoredEntityOwners(func(next storedEntityObserverRegistry) {
		entry := next[namespace]
		entry.observer = observer
		entry.boundary = boundary
		next[namespace] = entry
	})
	return nil
}

func (cw *commitWriter) prepareStoredEntityPublication() error {
	if err := cw.pendingNamespaceWriteConflict(); err != nil {
		return err
	}
	registry := cw.engine.storedEntityObservers.Load()
	if registry == nil {
		return nil
	}
	// Skip physical replay: buffered ACK owns the same logical mutation.
	// The replay owner sets this flag before materializing its writes.
	if cw.stored != nil && cw.stored.skip {
		return nil
	}
	var keys []any
	for _, key := range cw.publishKeys {
		var namespace string
		switch id := key.(type) {
		case NodeID:
			namespace = namespaceForNodeID(id)
		case EdgeID:
			namespace = namespaceForEdgeID(id)
		default:
			continue
		}
		if (*registry)[namespace].observer != nil {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	readTs := cw.txn.ReadTs()
	if cw.large != nil {
		readTs = cw.large.originalReadTs
	}
	prior := cw.db.newTxnAt(readTs, false)
	defer prior.Discard()
	changes, err := cw.engine.captureStoredEntityChanges(context.Background(), keys, storedEntityView{read: embeddingRecordReader{txn: prior}, boundaries: registry}, storedEntityView{read: embeddingRecordReader{txn: cw.txn}})
	if err != nil {
		return err
	}
	if cw.stored == nil {
		cw.stored = &storedEntityCommit{}
	}
	if cw.stored.logicalID == "" {
		cw.stored.logicalID = generateTxID()
	}
	cw.stored.publish, err = cw.engine.prepareStoredEntityChanges(registry, cw.stored.logicalID, changes)
	return err
}

type storedEntityCommit struct {
	logicalID string
	publish   func()
	skip      bool
}

// configureStoredEntityPublication binds logical ACK identity and replay policy
// once at the writer's ingress. No observer means no optional state allocation.
func (cw *commitWriter) configureStoredEntityPublication(logicalID string, skip bool) {
	if cw.engine.storedEntityObservers.Load() == nil {
		return
	}
	if cw.stored == nil {
		cw.stored = &storedEntityCommit{}
	}
	cw.stored.logicalID = logicalID
	cw.stored.skip = skip
}

// updateStoredEntityOwners publishes one immutable namespace-owner registry.
// The pure change may retry after concurrent attachment or registration. It
// never holds storage/admission locks or invokes external callbacks.
func (b *BadgerEngine) updateStoredEntityOwners(change func(storedEntityObserverRegistry)) {
	for {
		previous := b.storedEntityObservers.Load()
		next := make(storedEntityObserverRegistry)
		if previous != nil {
			for key, entry := range *previous {
				next[key] = entry
			}
		}
		change(next)
		updated := &next
		if len(next) == 0 {
			updated = nil
		}
		if b.storedEntityObservers.CompareAndSwap(previous, updated) {
			return
		}
	}
}

// RegisterManagedEntityPreparer registers managed leader admission without
// scanning storage or waiting for the exclusive initialization cut. First
// registration owns both preparation and initialization; later wrappers retain
// their own routing while sharing that namespace owner. For example,
// a database wrapper registers its existing prepare/admit coordinator here.
func (b *BadgerEngine) RegisterManagedEntityPreparer(namespace string, prepare ManagedEntityPrepare, initialize ManagedEntityInitialize) {
	b.updateStoredEntityOwners(func(next storedEntityObserverRegistry) {
		entry := next[namespace]
		if entry.preparer == nil && entry.initialize == nil {
			entry.preparer = prepare
			entry.initialize = initialize
		}
		next[namespace] = entry
	})
}

// GetOrAttachManagedEntityRuntime attaches the namespace's one existing managed
// usage/policy runtime. A later manager receives that same runtime rather than
// installing another ledger. The owning package checks its concrete type.
// A nil candidate only queries the current attachment.
func (b *BadgerEngine) GetOrAttachManagedEntityRuntime(namespace string, candidate any) any {
	if candidate == nil {
		registry := b.storedEntityObservers.Load()
		if registry == nil {
			return nil
		}
		return (*registry)[namespace].runtime
	}
	var attached any
	b.updateStoredEntityOwners(func(next storedEntityObserverRegistry) {
		entry := next[namespace]
		if entry.runtime == nil {
			entry.runtime = candidate
		}
		next[namespace] = entry
		attached = entry.runtime
	})
	return attached
}

func (b *BadgerEngine) initializeManagedEntityNamespace(ctx context.Context, namespace string) error {
	registry := b.storedEntityObservers.Load()
	if registry == nil || (*registry)[namespace].initialize == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return (*registry)[namespace].initialize(ctx)
}

// PrepareManagedEntityChanges discovers the registered namespace owner through
// the canonical transparent-engine accessor and invokes its prepare/admit path.
// The caller must establish leader and wrapper admission before calling; this
// capability does not submit, forward, journal or apply a command. handled stays
// true on admission failure so callers cannot bypass the managed owner.
func PrepareManagedEntityChanges(ctx context.Context, engine Engine, namespace string, operations []Operation, mode EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), bool, error) {
	var native *BadgerEngine
	switch current := UnwrapEngine(engine).(type) {
	case *BadgerEngine:
		native = current
	case *MemoryEngine:
		native = current.BadgerEngine
	default:
		return nil, nil, false, nil
	}
	registry := native.storedEntityObservers.Load()
	if registry == nil {
		return nil, nil, false, nil
	}
	prepare := (*registry)[namespace].preparer
	if prepare == nil {
		return nil, nil, false, nil
	}
	prepared, settle, err := prepare(ctx, operations, mode)
	return prepared, settle, true, err
}

// ManagedEntityUsageResetter is implemented by the canonical attached ledger.
// ResetManagedEntityUsage transitions usage after an empty reset or invalidates
// it after Restore, retaining policy and pending allowances. It must not enter
// storage/admission or dispatch listeners.
type ManagedEntityUsageResetter interface {
	ResetManagedEntityUsage(ManagedEntityUsageTransition)
}

// ManagedEntityUsageTransition distinguishes a known empty graph from replacement
// by arbitrary restored contents. Both retain policy and accepted allowances.
type ManagedEntityUsageTransition uint8

const (
	ManagedEntityUsageEmpty ManagedEntityUsageTransition = iota
	ManagedEntityUsageRestored
)

// invalidateManagedEntityUsage runs under Restore's exclusive lifecycle lease.
// Existing views retain their runtime and initialize against the restored graph.
func (b *BadgerEngine) invalidateManagedEntityUsage() {
	registry := b.storedEntityObservers.Load()
	if registry == nil {
		return
	}
	for _, entry := range *registry {
		if reset, ok := entry.runtime.(ManagedEntityUsageResetter); ok {
			reset.ResetManagedEntityUsage(ManagedEntityUsageRestored)
		}
	}
}

func (b *BadgerEngine) resetManagedEntityUsage(prefix string) {
	whole, ok := namespacePrefixFromID(prefix)
	if !ok || whole != prefix {
		return
	}
	registry := b.storedEntityObservers.Load()
	if registry == nil {
		return
	}
	runtime := (*registry)[prefix[:len(prefix)-1]].runtime
	if reset, ok := runtime.(ManagedEntityUsageResetter); ok {
		reset.ResetManagedEntityUsage(ManagedEntityUsageEmpty)
	}
}

// RetireManagedEntityRuntime is called only after successful catalog DROP
// persistence. It releases callback claims for recreation while retaining the
// canonical runtime object, so previously returned views cannot own orphan usage.
func (b *BadgerEngine) RetireManagedEntityRuntime(namespace string) {
	b.updateStoredEntityOwners(func(next storedEntityObserverRegistry) {
		entry, found := next[namespace]
		if !found {
			return
		}
		if entry.runtime == nil {
			delete(next, namespace)
		} else {
			next[namespace] = storedEntityObserverEntry{runtime: entry.runtime}
		}
	})
}

// prepareDeletedEntityPublication uses the prefix deletion owner's existing
// victim dictionaries. Its final view is known absence; the original snapshot
// hydrates the same complete prior images as ordinary commits.
func (b *BadgerEngine) prepareDeletedEntityPublication(nodes, edges map[uint64]struct{}) (func(), error) {
	registry := b.storedEntityObservers.Load()
	if registry == nil {
		return nil, nil
	}
	var keys []any
	for number := range nodes {
		if id, found := b.idDict.lookupNodeIDByNum(number); found && (*registry)[namespaceForNodeID(id)].observer != nil {
			keys = append(keys, id)
		}
	}
	for number := range edges {
		if id, found := b.idDict.lookupEdgeIDByNum(number); found && (*registry)[namespaceForEdgeID(id)].observer != nil {
			keys = append(keys, id)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	var changes []StoredEntityChange
	err := b.db.View(func(txn *badger.Txn) error {
		var err error
		changes, err = b.captureStoredEntityChanges(context.Background(), keys, storedEntityView{read: embeddingRecordReader{txn: txn}, boundaries: registry}, storedEntityView{absent: true})
		return err
	})
	if err != nil {
		return nil, err
	}
	return b.prepareStoredEntityChanges(registry, generateTxID(), changes)
}

func (b *BadgerEngine) prepareStoredEntityChanges(registry *storedEntityObserverRegistry, logicalID string, changes []StoredEntityChange) (func(), error) {
	events := make(map[string][]StoredEntityChange)
	for _, change := range changes {
		var namespace string
		switch {
		case change.BeforeNode != nil:
			namespace = namespaceForNodeID(change.BeforeNode.ID)
		case change.AfterNode != nil:
			namespace = namespaceForNodeID(change.AfterNode.ID)
		case change.BeforeEdge != nil:
			namespace = namespaceForEdgeID(change.BeforeEdge.ID)
		case change.AfterEdge != nil:
			namespace = namespaceForEdgeID(change.AfterEdge.ID)
		}
		events[namespace] = append(events[namespace], change)
	}
	var publications []func()
	for namespace, changes := range events {
		observer := (*registry)[namespace].observer
		if observer == nil {
			continue
		}
		publish, err := observer(StoredEntityPublication{Namespace: namespace, LogicalID: logicalID, Changes: changes})
		if err != nil {
			return nil, err
		}
		if publish != nil {
			publications = append(publications, publish)
		}
	}
	return func() {
		for _, publish := range publications {
			publish()
		}
	}, nil
}

// value owns typed stored record selection on the caller's pinned reader.
// Missing records are absence. The caller's mode determines how much of a
// present record is decoded; publication requests a complete image explicitly.
func (v storedEntityView) value(kind bufferedEntityKind, id string, visit func([]byte) error) error {
	if v.absent {
		return nil
	}
	var key []byte
	switch kind {
	case bufferedNodeEntity:
		key = nodeKey(NodeID(id))
	case bufferedEdgeEntity:
		key = edgeKey(EdgeID(id))
	default:
		return ErrInvalidData
	}
	err := v.read.value(key, visit)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	return err
}
func (v storedEntityView) nodeRecord(b *BadgerEngine, id NodeID, data []byte) (*Node, error) {
	node, err := b.decodeNodeProjectedWithContext(v.read.ctx, namespaceForNodeID(id), data, nil)
	if err != nil {
		return nil, err
	}
	return b.loadNodeEmbeddingState(v.read, node, id, true)
}

// edgeRecord is the complete stored relationship image on this reader. The
// compact body's endpoint numbers belong to its persisted dictionary cut;
// consulting the live dictionary here would mix captured facts and new IDs.
func (v storedEntityView) edgeRecord(b *BadgerEngine, id EdgeID, data []byte) (*Edge, error) {
	edge, start, end, err := b.decodeEdgeCompactV2WithContext(v.read.ctx, namespaceForEdgeID(id), data)
	if err != nil {
		return nil, err
	}
	edge.ID = id
	edge.StartNode, err = v.read.nodeID(start)
	if err != nil {
		return nil, err
	}
	edge.EndNode, err = v.read.nodeID(end)
	return edge, err
}

func (r embeddingRecordReader) nodeID(num uint64) (NodeID, error) {
	var id NodeID
	err := r.value(append([]byte{prefixIDDictNodeReverse}, encodeNumID(num)...), func(value []byte) error { id = NodeID(value); return nil })
	return id, err
}

// withBoundary is the existing stored-read interception seam for both complete
// publication images and explicit D7 fact/projection modes.
func (v storedEntityView) withBoundary(kind StoredEntityReadKind, id string, read func() error) error {
	if v.boundaries != nil {
		namespace := namespaceForNodeID(NodeID(id))
		if kind == StoredEntityEdgePriorRead {
			namespace = namespaceForEdgeID(EdgeID(id))
		}
		if boundary := (*v.boundaries)[namespace].boundary; boundary != nil {
			return boundary.WithStoredEntityRead(kind, id, read)
		}
	}
	return read()
}

// withSelectedEntity owns absent/ACK/staged/physical precedence for every
// explicit mode. Mode-specific decoders/copies run only after this selection.
func (v storedEntityView) withSelectedEntity(b *BadgerEngine, kind bufferedEntityKind, id string, buffered func(bufferedEntityState) error, physical func() error) error {
	if v.absent {
		return nil
	}
	if state := v.bufferedState(b, kind, id); state.found {
		return buffered(state)
	}
	return physical()
}

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
}

// bufferedState selects the stored-inclusive typed overlay under its existing
// read lock. The record reader remains the caller's pinned transaction; hidden
// bodies and tombstones must mask that same committed predecessor.
func (v storedEntityView) bufferedState(b *BadgerEngine, kind bufferedEntityKind, id string) bufferedEntityState {
	if v.acknowledged == nil {
		return bufferedEntityState{}
	}
	v.acknowledged.mu.RLock()
	defer v.acknowledged.mu.RUnlock()
	return v.acknowledged.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: storedRead})
}

func (v storedEntityView) node(b *BadgerEngine, id NodeID) (*Node, error) {
	if v.absent {
		return nil, nil
	}
	if state := v.bufferedState(b, bufferedNodeEntity, string(id)); state.found {
		if state.deleted {
			return nil, nil
		}
		return copyNode(state.node), nil
	}

	var node *Node
	err := v.read.value(nodeKey(id), func(data []byte) error {
		var err error
		node, err = b.decodeNode(namespaceForNodeID(id), data)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.loadNodeEmbeddingState(v.read, node, id, true)
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
	if state.found {
		if state.deleted {
			return nil, false, nil
		}
		return copyEdge(state.edge), true, nil
	}
	var edge *Edge
	err := reader.value(edgeKey(id), func(data []byte) error { var err error; edge, err = b.decodeEdgeBodyByID(data, id); return err })
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	}
	return edge, err == nil, err
}

// captureStoredEntityChanges reads each typed touched entity once. Both views
// belong to this commit, never the latest mutable storage after publication.
type storedEntityCapture struct {
	keys          []any
	before, after storedEntityView
	// staged is read while the caller holds the transaction mutex and entity
	// authority. It supplies both cuts and reuses an already prepared delta.
	staged *BadgerTransaction
}

func (b *BadgerEngine) captureStoredEntityChanges(request storedEntityCapture) ([]StoredEntityChange, error) {
	keys, before, after := request.keys, request.before, request.after
	if tx := request.staged; tx != nil {
		if tx.preparedEntityCommit != nil {
			return tx.preparedEntityCommit.changes, nil
		}
		var constraints publicationConstraintView
		keys = appendOperationPublicationKeys(nil, &constraints, tx.physicalOperationsLocked())
		before = storedEntityView{read: embeddingRecordReader{txn: tx.badgerTx}, acknowledged: b.writeBehind, boundaries: b.storedEntityObservers.Load()}
		after = storedEntityView{read: embeddingRecordReader{txn: tx.badgerTx, stagedWrites: tx.pendingWrites, stagedDeletes: tx.pendingDeletes}}
	}
	seen := make(map[any]struct{}, len(keys))
	changes := make([]StoredEntityChange, 0, len(keys))
	for _, key := range keys {
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
			if before.boundaries != nil && (*before.boundaries)[namespaceForNodeID(id)].boundary != nil {
				err = (*before.boundaries)[namespaceForNodeID(id)].boundary.WithStoredEntityRead(StoredEntityNodePriorRead, string(id), read)
			} else {
				err = read()
			}
			if err == nil {
				change.AfterNode, err = after.node(b, id)
			}
		case EdgeID:
			read := func() error { var err error; change.BeforeEdge, err = before.edge(b, id); return err }
			if before.boundaries != nil && (*before.boundaries)[namespaceForEdgeID(id)].boundary != nil {
				err = (*before.boundaries)[namespaceForEdgeID(id)].boundary.WithStoredEntityRead(StoredEntityEdgePriorRead, string(id), read)
			} else {
				err = read()
			}
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
	for _, kind := range []byte{prefixNode, prefixEdge} {
		prefix := append([]byte{kind}, []byte(s.namespace+":")...)
		it := s.txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		scan := func() error {
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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
	return nil
}

// InitializeStoredEntityObserver owns one exclusive stored snapshot/install cut.
// The caller must not hold its admission lock while waiting for this cut. The
// initializer streams initial images, atomically initializes its own ledger and
// returns the observer installed before any subsequent writer is admitted.
// Neither initializer nor observer may reenter storage or write admission.
func (b *BadgerEngine) InitializeStoredEntityObserver(namespace string, initialize func(StoredEntitySnapshot) (StoredEntityObserver, error), boundary StoredEntityReadBoundary) error {
	lease, err := b.beginBarrierWrite(context.Background(), true, true)
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
	changes, err := cw.engine.captureStoredEntityChanges(storedEntityCapture{keys: keys, before: storedEntityView{read: embeddingRecordReader{txn: prior}, boundaries: registry}, after: storedEntityView{read: embeddingRecordReader{txn: cw.txn}}})
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

// PrepareEntityChanges stages and validates entity changes through the registered
// namespace coordinator, or the native transaction owner when unmanaged.
// Callers establish routing and wrapper admission before preparation; this
// operation neither journals nor publishes the prepared changes.
func PrepareEntityChanges(ctx context.Context, engine Engine, namespace string, operations []Operation, mode EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var native *BadgerEngine
	switch current := UnwrapEngine(engine).(type) {
	case *BadgerEngine:
		native = current
	case *MemoryEngine:
		native = current.BadgerEngine
	}
	if native != nil {
		if registry := native.storedEntityObservers.Load(); registry != nil {
			if prepare := (*registry)[namespace].preparer; prepare != nil {
				return prepare(ctx, operations, mode)
			}
		}
	}
	// The prepared commit carries the admission its context or operations
	// carry (a request's write); otherwise its first write admits at the
	// transaction's counted owner site.
	var origins []uint64
	if origin, admitted := VectorWorkOriginFromArguments(ctx, operations); admitted {
		origins = append(origins, origin)
	}
	tx, err := BeginEntityTransaction(engine, mode, origins...)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	if err = tx.SetNamespace(namespace); err != nil {
		return nil, nil, err
	}
	if err = tx.SetValidationSetting(DeferredConstraintValidation, true); err != nil {
		return nil, nil, err
	}
	for _, op := range operations {
		if err = ApplyEntityChange(tx, op, func(request NodeUpdateRequest) error { return ApplyNodeUpdate(tx, request) }, func(id NodeID) error { return tx.DeleteGraphNode(id, CascadeEdges) }, tx.DeleteEdge); err != nil {
			return nil, nil, err
		}
	}
	if tx.OperationCount() == 0 {
		return nil, nil, tx.CommitContext(ctx)
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
	return prepared, nil, err
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

// invalidateManagedEntityUsage runs under an exclusive Restore or migration lease.
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
		changes, err = b.captureStoredEntityChanges(storedEntityCapture{keys: keys, before: storedEntityView{read: embeddingRecordReader{txn: txn}, boundaries: registry}, after: storedEntityView{absent: true}})
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

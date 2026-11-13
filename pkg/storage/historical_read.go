package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// OpenHistoricalRead returns a read-only Engine that answers every read at one
// fixed MVCC cutoff over one pinned physical snapshot of engine's native store.
//
// The cutoff is the requested wall-clock time at.UTC() with the maximum commit
// sequence, so a read sees every commit that completed at or before that
// instant and no commit after it. The returned engine holds exactly one pinned
// physical admission (the snapshot capture) and exactly one registered
// historical reader for its whole lifetime; Close releases both and nothing
// else, leaving the engine it reads unaffected.
//
// What a historical read answers:
//
//   - Node and relationship labels, properties and embedding metadata are the
//     stored state at the cutoff. Embedding vectors are not available: a
//     historical read never returns vectors, current or historical.
//   - An entity is only listed while its current live incarnation existed at
//     the cutoff. A currently deleted entity is absent, even for a time before
//     its deletion, and an ID whose current incarnation began after a
//     retention-retained tombstone is absent at every earlier time: the state
//     before that tombstone belonged to a different incarnation and never
//     answers this read.
//   - A relationship is only listed while both of its endpoints are nodes this
//     same read lists at the cutoff.
//   - A time before the entity's creation floor is a legitimate absence
//     (ErrNotFound). A time below a floor advanced by pruning reports
//     ErrHistoryNotRetained. A missing retained record at or after the creation
//     floor reports ErrHistoryUnavailable, never a silent absence.
//
// The population of every scan comes from the MVCC node-head and edge-head
// keyspaces, streamed one candidate at a time: a current label, property, type
// or count index describes the live graph and cannot narrow a historical
// population. The returned schema is empty for the same reason, and the current
// suppression and decay policy is preserved on the entities a read resolves.
//
// Example:
//
//	historical, err := storage.OpenHistoricalRead(ctx, engine, time.Now().Add(-24*time.Hour))
//	if err != nil {
//		return err
//	}
//	defer historical.Close()
//	nodes, err := historical.GetNodesByLabel(ctx, "Person")
func OpenHistoricalRead(ctx context.Context, engine Engine, at time.Time) (Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native, namespace, err := historicalReadTarget(engine)
	if err != nil {
		return nil, err
	}
	version := MVCCVersion{CommitTimestamp: at.UTC(), CommitSequence: ^uint64(0)}
	// The reader registration precedes the snapshot capture: a prune step
	// decides its floors from the registered readers, so every step that starts
	// after this registration keeps the versions the cutoff reads. A step
	// already in flight decided without this reader, which is why the
	// registration waits for it to finish first, as every other pinned
	// snapshot read registers (beginMVCCHistoryRead).
	if err := native.mvccPruneBatchMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	release, err := native.acquireSnapshotReader(SnapshotReaderInfo{
		SnapshotVersion: version,
		StartTime:       time.Now().UTC(),
		Namespace:       namespace,
	})
	native.mvccPruneBatchMu.RUnlock()
	if err != nil {
		return nil, err
	}
	view, err := native.captureReadView(ctx, nil, readViewOptions{
		scopeKind: readNamespaceScope,
		scope:     namespace,
		longLived: true,
	})
	if err != nil {
		release()
		return nil, err
	}
	return &historicalReadEngine{
		engine:    native,
		view:      view,
		schema:    NewSchemaManager(),
		release:   release,
		version:   version,
		namespace: namespace,
	}, nil
}

// historicalReadTarget resolves the native store a historical read pins and the
// database namespace the supplied engine serves. A wrapper chain is followed
// through Namespace and EngineUnwrapper; the outermost namespace scopes the
// read exactly as it scopes that wrapper's own reads. Only a native
// BadgerEngine holds the MVCC history a historical read resolves.
func historicalReadTarget(engine Engine) (*BadgerEngine, string, error) {
	if engine == nil {
		return nil, "", errors.New("historical read: nil engine")
	}
	namespace := ""
	current := engine
	// A wrapper that unwraps to itself would never reach the store; the bound
	// turns that into the unsupported-engine error below.
	for depth := 0; depth < 32; depth++ {
		if native, ok := current.(*BadgerEngine); ok && native != nil {
			return native, namespace, nil
		}
		if scoped, ok := current.(interface{ Namespace() string }); ok && namespace == "" {
			namespace = scoped.Namespace()
		}
		wrapper, ok := current.(EngineUnwrapper)
		if !ok {
			break
		}
		inner := wrapper.GetInnerEngine()
		if inner == nil {
			break
		}
		current = inner
	}
	return nil, "", fmt.Errorf("historical read: engine %T does not unwrap to a %T", engine, &BadgerEngine{})
}

// historicalReadEngine answers the Engine read contract at one fixed cutoff
// over one pinned physical snapshot. Every read resolves through the canonical
// MVCC version resolution of the native store; there is no write path and no
// promoted read of the current graph.
type historicalReadEngine struct {
	engine    *BadgerEngine
	view      *badgerReadView
	schema    *SchemaManager
	release   func()
	version   MVCCVersion
	namespace string
	mu        sync.Mutex
	closed    bool
	active    int
}

var _ Engine = (*historicalReadEngine)(nil)

// Namespace reports the database this historical read is scoped to, as the
// engine it was opened from reports it.
func (h *historicalReadEngine) Namespace() string { return h.namespace }

// Close releases the pinned snapshot and the reader registration this
// historical read holds. It does not close the engine it reads, and a second
// Close is a no-op.
func (h *historicalReadEngine) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	retire := h.active == 0
	h.mu.Unlock()
	if retire {
		h.view.Close()
		h.release()
	}
	return nil
}

func (h *historicalReadEngine) finishRead() {
	h.mu.Lock()
	h.active--
	retire := h.closed && h.active == 0
	h.mu.Unlock()
	if retire {
		h.view.Close()
		h.release()
	}
}

// scopePrefix is the stored-ID prefix of this read's database, empty for a
// read of the whole store. It is the same scope the engine it was opened from
// applies to its own reads.
func (h *historicalReadEngine) scopePrefix() string {
	if h.namespace == "" {
		return ""
	}
	return h.namespace + ":"
}

// withTxn admits one read of the pinned snapshot under the caller's context.
func (h *historicalReadEngine) withTxn(ctx context.Context, read func(*badger.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrStorageClosed
	}
	h.active++
	h.mu.Unlock()
	defer h.finishRead()
	return h.view.readPinned(ctx, read)
}

// historicalIncarnationVerdict is what one entity's retained version chain says
// about the current live incarnation at the cutoff.
type historicalIncarnationVerdict struct {
	// absent reports that the ID's current live incarnation did not exist at
	// the cutoff: a retained tombstone after the cutoff, or a retained
	// tombstone at or before it that still ends the preceding incarnation. The
	// state before either belongs to a different incarnation.
	absent bool
	// recordAtCut reports that a retained record at or before the cutoff holds
	// this entity's state in the current live incarnation.
	recordAtCut bool
}

// historicalRecordTombstoned decodes whether one version record is a tombstone.
// A delete writes a tombstone record outright and an undo record describes a
// live body, so the raw record answers the question without resolving undo
// chains.
type historicalRecordTombstoned func(item *badger.Item) (bool, error)

func historicalNodeRecordTombstoned(item *badger.Item) (bool, error) {
	tombstoned := false
	err := item.Value(func(value []byte) error {
		record, decodeErr := decodeMVCCNodeRecord(value)
		if decodeErr != nil {
			return decodeErr
		}
		tombstoned = record.Tombstoned
		return nil
	})
	return tombstoned, err
}

func historicalEdgeRecordTombstoned(item *badger.Item) (bool, error) {
	tombstoned := false
	err := item.Value(func(value []byte) error {
		record, decodeErr := decodeMVCCEdgeRecord(value)
		if decodeErr != nil {
			return decodeErr
		}
		tombstoned = record.Tombstoned
		return nil
	})
	return tombstoned, err
}

// historicalIncarnationInTxn walks one entity's retained version chain, newest
// first, and reports whether the ID's current live incarnation existed at the
// cutoff. The chain itself decides: the head carries the version of the latest
// commit only, so a tombstone it retained is the one boundary between the
// preceding incarnation and the live one. The walk stops at the first record at
// or before the cutoff, and only runs when the cutoff precedes the head version,
// in which case the answer cannot come from the live body.
func historicalIncarnationInTxn[K mvccVersionKeyer](ctx context.Context, b *BadgerEngine, txn *badger.Txn, id string, head MVCCHead, cutoff MVCCVersion, tombstoned historicalRecordTombstoned) (historicalIncarnationVerdict, error) {
	var keyer K
	prefix := keyer.versionPrefix(b, id)
	if len(prefix) == 0 {
		return historicalIncarnationVerdict{}, nil
	}
	if cutoff.Compare(head.Version) >= 0 {
		// The live head body answers this read; no older record can decide it
		// and no record of a later incarnation exists.
		return historicalIncarnationVerdict{recordAtCut: true}, nil
	}
	upper := append(append([]byte{}, prefix...), encodeMVCCSortVersion(head.Version)...)
	verdict := historicalIncarnationVerdict{}
	err := descendingPrefixKeys(txn, prefix, upper, func(item *badger.Item) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		version, err := extractMVCCVersionFromKey(item.Key())
		if err != nil {
			return false, err
		}
		if version.Compare(head.Version) > 0 {
			return true, nil
		}
		deleted, err := tombstoned(item)
		if err != nil {
			return false, err
		}
		if version.Compare(cutoff) > 0 {
			if deleted {
				// The incarnation this ID has now was deleted after the cutoff
				// and started again since. The state before that delete is a
				// different incarnation and must not answer this read.
				verdict.absent = true
				return false, nil
			}
			return true, nil
		}
		// The greatest retained record at or before the cutoff.
		verdict.recordAtCut = !deleted
		verdict.absent = deleted
		return false, nil
	})
	return verdict, err
}

// belowFloor is the one answer to a cutoff below an entity's floor: a floor a
// prune advanced (its persisted marker says so) means the store discarded the
// requested history, while a floor that is still the entity's creation version
// means the entity did not exist yet.
func (h *historicalReadEngine) belowFloor(txn *badger.Txn, kind byte, numID uint64, found bool) error {
	if !found {
		return ErrNotFound
	}
	var logical [9]byte
	logical[0] = kind
	binary.BigEndian.PutUint64(logical[1:], numID)
	_, err := txn.Get(mvccPruneFloorKey(logical[:]))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return ErrHistoryNotRetained
}

// requirePrimaryInTxn admits an entity only while the pinned snapshot holds its
// current primary body: a head without one is not a live incarnation.
func requirePrimaryInTxn(txn *badger.Txn, key []byte) error {
	if _, err := txn.Get(key); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// historicalNodeAtInTxn is the one node read of this engine. It admits an entity
// only while its current live incarnation existed at the cutoff, applies the
// retained-history rules of the store, and resolves the body through the
// canonical version resolution. A non-nil projection decodes only the named
// properties; a nil projection reads the whole body.
func (h *historicalReadEngine) historicalNodeAtInTxn(ctx context.Context, txn *badger.Txn, id NodeID, projection []string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !nodeIDInScope(id, h.scopePrefix()) {
		return nil, ErrNotFound
	}
	head, err := h.engine.loadNodeMVCCHeadInTxn(txn, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// No head at all: this ID has no incarnation to read.
			return nil, ErrNotFound
		}
		return nil, err
	}
	if head.Tombstoned {
		return nil, ErrNotFound
	}
	if err := requirePrimaryInTxn(txn, nodeKey(id)); err != nil {
		return nil, err
	}
	numID, haveNumID := h.engine.idDict.lookupNodeNumID(id)
	if h.version.Compare(head.FloorVersion) < 0 {
		return nil, h.belowFloor(txn, prefixMVCCNode, numID, haveNumID)
	}
	verdict, err := historicalIncarnationInTxn[nodeMVCCVersionKeyer](ctx, h.engine, txn, string(id), head, h.version, historicalNodeRecordTombstoned)
	if err != nil {
		return nil, err
	}
	if verdict.absent {
		return nil, ErrNotFound
	}
	var node *Node
	if projection != nil {
		node, err = h.engine.getNodeProjectedVisibleAtInTxn(txn, id, h.version, projection)
	} else {
		node, err = h.engine.getNodeAtInTxn(txn, id, h.version, mvccEntityRead{vectors: false, visibility: visibleRead})
	}
	switch {
	case err == nil:
		if node == nil {
			return nil, ErrHistoryUnavailable
		}
		// A historical read has no vectors, and the detached image owns the
		// labels, properties and metadata it returns.
		return copyNodeWithoutEmbeddings(node), nil
	case errors.Is(err, ErrNotVisibleAtSnapshot):
		return nil, h.belowFloor(txn, prefixMVCCNode, numID, haveNumID)
	case errors.Is(err, ErrNotFound):
		if verdict.recordAtCut {
			// The store holds this entity's state at the cutoff and the current
			// suppression and decay policy withholds it: the answer is absence.
			return nil, ErrNotFound
		}
		return nil, ErrHistoryUnavailable
	default:
		return nil, err
	}
}

// historicalEdgeAtInTxn is the one relationship read of this engine. It applies
// the same incarnation and retained-history rules as a node read, and then
// requires both endpoints to be nodes this read lists at the cutoff.
func (h *historicalReadEngine) historicalEdgeAtInTxn(ctx context.Context, txn *badger.Txn, id EdgeID) (*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !edgeIDInScope(id, h.scopePrefix()) {
		return nil, ErrNotFound
	}
	head, err := h.engine.loadEdgeMVCCHeadInTxn(txn, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if head.Tombstoned {
		return nil, ErrNotFound
	}
	if err := requirePrimaryInTxn(txn, edgeKey(id)); err != nil {
		return nil, err
	}
	numID, haveNumID := h.engine.idDict.lookupEdgeNumID(id)
	if h.version.Compare(head.FloorVersion) < 0 {
		return nil, h.belowFloor(txn, prefixMVCCEdge, numID, haveNumID)
	}
	verdict, err := historicalIncarnationInTxn[edgeMVCCVersionKeyer](ctx, h.engine, txn, string(id), head, h.version, historicalEdgeRecordTombstoned)
	if err != nil {
		return nil, err
	}
	if verdict.absent {
		return nil, ErrNotFound
	}
	edge, err := h.engine.getEdgeAtInTxn(txn, id, h.version, mvccEntityRead{visibility: visibleRead})
	switch {
	case err == nil:
		if edge == nil {
			return nil, ErrHistoryUnavailable
		}
	case errors.Is(err, ErrNotVisibleAtSnapshot):
		return nil, h.belowFloor(txn, prefixMVCCEdge, numID, haveNumID)
	case errors.Is(err, ErrNotFound):
		if verdict.recordAtCut {
			return nil, ErrNotFound
		}
		return nil, ErrHistoryUnavailable
	default:
		return nil, err
	}
	for _, endpoint := range [2]NodeID{edge.StartNode, edge.EndNode} {
		eligible, err := h.historicalNodeEligibleInTxn(ctx, txn, endpoint)
		if err != nil {
			return nil, err
		}
		if !eligible {
			return nil, ErrNotFound
		}
	}
	return copyEdge(edge), nil
}

// historicalNodeEligibleInTxn reports whether id is a node this historical read
// lists at the cutoff: present in its current live incarnation, retained, and
// visible under the store's current suppression and decay policy.
func (h *historicalReadEngine) historicalNodeEligibleInTxn(ctx context.Context, txn *badger.Txn, id NodeID) (bool, error) {
	if _, err := h.historicalNodeAtInTxn(ctx, txn, id, nil); err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// streamNodesInTxn visits every node of the scoped population this read lists
// at the cutoff, one candidate at a time and in head order. The candidates are
// the MVCC node heads of the pinned snapshot: the live label, property and
// count indexes cannot narrow a historical population, and no population is
// materialized before it is visited.
func (h *historicalReadEngine) streamNodesInTxn(ctx context.Context, txn *badger.Txn, projection []string, visit func(*Node) error) error {
	reader := h.engine.newNodeIndexReader(ctx, txn, h.scopePrefix(), []byte{prefixMVCCNodeHead}, "")
	defer reader.Close()
	for reader.Next() {
		node, err := h.historicalNodeAtInTxn(ctx, txn, reader.NodeID(), projection)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		if err := visit(node); err != nil {
			return err
		}
	}
	return reader.Err()
}

// streamEdgesInTxn is the relationship analogue of streamNodesInTxn: it visits
// every relationship of the scoped population this read lists at the cutoff,
// streaming the edge heads of the pinned snapshot.
func (h *historicalReadEngine) streamEdgesInTxn(ctx context.Context, txn *badger.Txn, visit func(*Edge) error) error {
	prefix := []byte{prefixMVCCEdgeHead}
	iterator := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer iterator.Close()
	var cancel bulkReadCancellation
	for iterator.Rewind(); iterator.ValidForPrefix(prefix); iterator.Next() {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		key := iterator.Item().Key()
		if len(key) != 1+8 {
			continue
		}
		edgeID, ok := h.engine.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[1:]))
		if !ok || edgeID == "" {
			continue
		}
		edge, err := h.historicalEdgeAtInTxn(ctx, txn, edgeID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := visit(edge); err != nil {
			return err
		}
	}
	return nil
}

// historicalNodeAdmits applies the stream options this engine honors without a
// current index: an ID prefix and a label. A projection narrows the decoded
// properties, embeddings are never available, and the property filter options
// are hints, so the caller still tests every node it receives.
func historicalNodeAdmits(node *Node, opts StreamNodesOptions) bool {
	if node == nil {
		return false
	}
	if opts.Prefix != "" && !nodeIDInScope(node.ID, opts.Prefix) {
		return false
	}
	if opts.Label != "" && !historicalNodeCarriesLabel(node, opts.Label) {
		return false
	}
	return true
}

// historicalNodeCarriesLabel is a nil-safe label test around the package's one
// label membership rule.
func historicalNodeCarriesLabel(node *Node, label string) bool {
	if node == nil {
		return false
	}
	return hasLabel(node.Labels, label)
}

// adjacentEdgesInTxn lists the relationships one directional request selects at
// the cutoff. The MVCC adjacency keyspace of the pinned snapshot supplies the
// candidates, and every candidate is resolved and endpoint-checked by the one
// relationship read.
func (h *historicalReadEngine) adjacentEdgesInTxn(ctx context.Context, txn *badger.Txn, request AdjacentEdgeRead) ([]*Edge, error) {
	if !nodeIDInScope(request.Node, h.scopePrefix()) {
		return nil, nil
	}
	var prefix []byte
	switch request.Direction {
	case AdjacentOutgoing, AdjacentBetween:
		prefix = h.engine.mvccOutgoingAdjacencyPrefixString(request.Node)
	case AdjacentIncoming:
		prefix = h.engine.mvccIncomingAdjacencyPrefixString(request.Node)
	default:
		return nil, ErrInvalidData
	}
	if len(prefix) == 0 {
		return nil, nil
	}
	ids, err := h.engine.collectVisibleAdjacencyEdgeIDsInTxn(ctx, txn, prefix, h.version)
	if err != nil {
		return nil, err
	}
	var edges []*Edge
	var cancel bulkReadCancellation
	for _, id := range ids {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		edge, err := h.historicalEdgeAtInTxn(ctx, txn, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !request.admits(edge) {
			continue
		}
		edges = append(edges, request.header(edge))
	}
	return edges, nil
}

// GetNode returns the stored state of id at the cutoff.
func (h *historicalReadEngine) GetNode(ctx context.Context, id NodeID) (*Node, error) {
	return h.readNode(ctx, id, nil)
}

func (h *historicalReadEngine) readNode(ctx context.Context, id NodeID, projection []string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var node *Node
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		var readErr error
		node, readErr = h.historicalNodeAtInTxn(ctx, txn, id, projection)
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}

// GetEdge returns the stored state of the relationship id at the cutoff. Its
// endpoints must be nodes this read lists at the cutoff.
func (h *historicalReadEngine) GetEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var edge *Edge
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		var readErr error
		edge, readErr = h.historicalEdgeAtInTxn(ctx, txn, id)
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return edge, nil
}

// GetNodesByLabel lists the nodes that carried label at the cutoff, streamed
// from the historical population rather than a current label index.
func (h *historicalReadEngine) GetNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var nodes []*Node
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamNodesInTxn(ctx, txn, nil, func(node *Node) error {
			if !historicalNodeCarriesLabel(node, label) {
				return nil
			}
			nodes = append(nodes, node)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// GetFirstNodeByLabel returns the first node that carried label at the cutoff,
// stopping the historical scan at that node.
func (h *historicalReadEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var first *Node
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		streamErr := h.streamNodesInTxn(ctx, txn, nil, func(node *Node) error {
			if !historicalNodeCarriesLabel(node, label) {
				return nil
			}
			first = node
			return ErrIterationStopped
		})
		if errors.Is(streamErr, ErrIterationStopped) {
			return nil
		}
		return streamErr
	})
	if err != nil {
		return nil, err
	}
	if first == nil {
		return nil, ErrNotFound
	}
	return first, nil
}

// ReadAdjacentEdges lists the relationships one request selects at the cutoff.
func (h *historicalReadEngine) ReadAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Direction == AdjacentBoth {
		return readBothAdjacentEdges(ctx, request, func(direction AdjacentEdgeRead) ([]*Edge, error) {
			return h.ReadAdjacentEdges(ctx, direction)
		})
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	var edges []*Edge
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		var readErr error
		edges, readErr = h.adjacentEdgesInTxn(ctx, txn, request)
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

// GetEdgeBetween returns the relationship of edgeType from startID to endID at
// the cutoff.
func (h *historicalReadEngine) GetEdgeBetween(ctx context.Context, startID, endID NodeID, edgeType string) (*Edge, error) {
	request := AdjacentEdgeRead{Direction: AdjacentBetween, Node: startID, Other: endID}
	if edgeType != "" {
		request.Types = []string{edgeType}
	}
	edges, err := h.ReadAdjacentEdges(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(edges) == 0 {
		return nil, ErrNotFound
	}
	return edges[0], nil
}

// GetEdgesByType lists the relationships of edgeType at the cutoff. An empty
// edgeType lists every relationship.
func (h *historicalReadEngine) GetEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var edges []*Edge
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamEdgesInTxn(ctx, txn, func(edge *Edge) error {
			if edgeType != "" && edge.Type != edgeType {
				return nil
			}
			edges = append(edges, edge)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

// AllNodes lists every node this read lists at the cutoff.
func (h *historicalReadEngine) AllNodes(ctx context.Context) ([]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var nodes []*Node
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamNodesInTxn(ctx, txn, nil, func(node *Node) error {
			nodes = append(nodes, node)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// AllEdges lists every relationship this read lists at the cutoff.
func (h *historicalReadEngine) AllEdges(ctx context.Context) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var edges []*Edge
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamEdgesInTxn(ctx, txn, func(edge *Edge) error {
			edges = append(edges, edge)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return edges, nil
}

// StreamNodesWithOptions streams the nodes this read lists at the cutoff. The ID
// prefix and label options narrow the stream, a projection narrows the decoded
// properties, and every returned image is detached and holds no vectors: a
// historical read cannot supply embeddings, so WithEmbeddings is not honored and
// the caller receives the same vector-free images either way.
func (h *historicalReadEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	if fn == nil {
		return ErrInvalidData
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamNodesInTxn(ctx, txn, opts.Projection, func(node *Node) error {
			if !historicalNodeAdmits(node, opts) {
				return nil
			}
			return fn(node)
		})
	})
}

// adjacentDegree counts the relationships of direction this read lists at the
// cutoff for nodeID.
func (h *historicalReadEngine) adjacentDegree(ctx context.Context, nodeID NodeID, direction AdjacentEdgeDirection) (int, error) {
	edges, err := h.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: direction, Node: nodeID})
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// GetInDegree counts the relationships into nodeID at the cutoff.
func (h *historicalReadEngine) GetInDegree(ctx context.Context, nodeID NodeID) (int, error) {
	return h.adjacentDegree(ctx, nodeID, AdjacentIncoming)
}

// GetOutDegree counts the relationships out of nodeID at the cutoff.
func (h *historicalReadEngine) GetOutDegree(ctx context.Context, nodeID NodeID) (int, error) {
	return h.adjacentDegree(ctx, nodeID, AdjacentOutgoing)
}

// GetSchema returns an empty schema. The current schema's indexes describe the
// live graph, so answering from them would report the historical population
// through an index that cannot hold it: a historical read exposes no index.
func (h *historicalReadEngine) GetSchema() *SchemaManager { return h.schema }

// BatchGetNodes returns the stored state of every listed node that this read
// lists at the cutoff. Absent nodes are omitted.
func (h *historicalReadEngine) BatchGetNodes(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodes := make(map[NodeID]*Node, len(ids))
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		var cancel bulkReadCancellation
		for _, id := range ids {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			node, err := h.historicalNodeAtInTxn(ctx, txn, id, nil)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			nodes[id] = node
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// NodeCount counts the nodes this read lists at the cutoff.
func (h *historicalReadEngine) NodeCount(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var count int64
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamNodesInTxn(ctx, txn, nil, func(*Node) error {
			count++
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// EdgeCount counts the relationships this read lists at the cutoff.
func (h *historicalReadEngine) EdgeCount(ctx context.Context) (int64, error) {
	return h.countEdgesByEndpointLabel(ctx, "", "", false)
}

// EdgeCountByType counts the relationships of edgeType at the cutoff. The live
// per-type counter cannot answer it.
func (h *historicalReadEngine) EdgeCountByType(ctx context.Context, edgeType string) (int64, error) {
	return h.countEdgesByEndpointLabel(ctx, "", edgeType, false)
}

// EdgeCountByStartLabel counts the relationships of edgeType at the cutoff whose
// start endpoint carried label at the cutoff.
func (h *historicalReadEngine) EdgeCountByStartLabel(ctx context.Context, label, edgeType string) (int64, error) {
	return h.countEdgesByEndpointLabel(ctx, label, edgeType, false)
}

// EdgeCountByEndLabel counts the relationships of edgeType at the cutoff whose
// end endpoint carried label at the cutoff.
func (h *historicalReadEngine) EdgeCountByEndLabel(ctx context.Context, label, edgeType string) (int64, error) {
	return h.countEdgesByEndpointLabel(ctx, label, edgeType, true)
}

// countEdgesByEndpointLabel is the one relationship count of this engine: it
// streams the historical population, filters by type, and, when label is named,
// keeps only the relationships whose named endpoint carried that label at the
// cutoff with edgeType empty meaning every type.
func (h *historicalReadEngine) countEdgesByEndpointLabel(ctx context.Context, label, edgeType string, incomingEndpoint bool) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var count int64
	err := h.withTxn(ctx, func(txn *badger.Txn) error {
		return h.streamEdgesInTxn(ctx, txn, func(edge *Edge) error {
			if edgeType != "" && edge.Type != edgeType {
				return nil
			}
			if label == "" {
				count++
				return nil
			}
			endpoint := edge.StartNode
			if incomingEndpoint {
				endpoint = edge.EndNode
			}
			// The endpoint is admitted exactly as a node read admits it, so the
			// positional count agrees with GetNode and with the decay policy.
			node, err := h.historicalNodeAtInTxn(ctx, txn, endpoint, nil)
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if historicalNodeCarriesLabel(node, label) {
				count++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CreateNode reports that a historical read is read-only.
func (h *historicalReadEngine) CreateNode(_ context.Context, _ *Node) (NodeID, error) {
	return "", ErrHistoricalReadOnly
}

// UpdateNode reports that a historical read is read-only.
func (h *historicalReadEngine) UpdateNode(_ context.Context, _ NodeUpdateRequest) error {
	return ErrHistoricalReadOnly
}

// DeleteNode reports that a historical read is read-only.
func (h *historicalReadEngine) DeleteNode(_ context.Context, _ NodeID) error {
	return ErrHistoricalReadOnly
}

// CreateEdge reports that a historical read is read-only.
func (h *historicalReadEngine) CreateEdge(_ context.Context, _ *Edge) error {
	return ErrHistoricalReadOnly
}

// UpdateEdge reports that a historical read is read-only.
func (h *historicalReadEngine) UpdateEdge(_ context.Context, _ EdgeUpdateRequest) error {
	return ErrHistoricalReadOnly
}

// DeleteEdge reports that a historical read is read-only.
func (h *historicalReadEngine) DeleteEdge(_ context.Context, _ EdgeID) error {
	return ErrHistoricalReadOnly
}

// BulkCreateNodes reports that a historical read is read-only.
func (h *historicalReadEngine) BulkCreateNodes(_ context.Context, _ []*Node) error {
	return ErrHistoricalReadOnly
}

// BulkCreateEdges reports that a historical read is read-only.
func (h *historicalReadEngine) BulkCreateEdges(_ context.Context, _ []*Edge) error {
	return ErrHistoricalReadOnly
}

// BulkDeleteNodes reports that a historical read is read-only.
func (h *historicalReadEngine) BulkDeleteNodes(_ context.Context, _ []NodeID) error {
	return ErrHistoricalReadOnly
}

// BulkDeleteEdges reports that a historical read is read-only.
func (h *historicalReadEngine) BulkDeleteEdges(_ context.Context, _ []EdgeID) error {
	return ErrHistoricalReadOnly
}

// DeleteByPrefix reports that a historical read is read-only.
func (h *historicalReadEngine) DeleteByPrefix(_ string) (int64, int64, error) {
	return 0, 0, ErrHistoricalReadOnly
}

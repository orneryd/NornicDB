package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

func (*BadgerEngine) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (*MemoryEngine) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (m *MemoryEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	return m.BadgerEngine.ReadStored(ctx, r)
}

// ReadStored uses the existing capture owner for physical time AND overlay
// retirement. The callback has no externally visible effects; results escape
// only after every required fact and payload has succeeded.
func (b *BadgerEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	var result StoredReadResult
	admissionIDs := append([]NodeID(nil), r.Nodes...)
	for _, id := range r.Edges {
		admissionIDs = append(admissionIDs, NodeID(id))
	}
	for _, a := range r.Adjacency {
		admissionIDs = append(admissionIDs, a.Node)
	}
	view, err := b.captureReadView(nil, readViewOptions{ctx: ctx, scopeKind: readNodeIDsScope, nodeIDs: admissionIDs, captureTxn: func(txn *badger.Txn, w *WriteBehindBuffer) error {
		if r.Work != nil {
			r.Work.Captures++
		}
		var e error
		result, e = b.storedReadInTxn(ctx, txn, r, "", func(id NodeID) (bufferedEntityState, bool) {
			s := w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
			return s, s.found
		}, func(id EdgeID) (bufferedEntityState, bool) {
			s := w.entityStateLocked(bufferedEdgeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
			return s, s.found
		}, func(emit func(EdgeID)) error {
			for _, adjacency := range r.Adjacency {
				captured, err := b.readBufferedEntitiesWithContext(ctx, w, storedRead, bufferedEntityRead{kind: bufferedAdjacency, source: adjacency.Node, outgoing: adjacency.Direction == Outgoing})
				if err != nil {
					return err
				}
				for _, edge := range captured.edges {
					if err := ctx.Err(); err != nil {
						return err
					}
					if edge != nil {
						emit(edge.ID)
					}
				}
			}
			return nil
		}, false)
		return e
	}})
	if view != nil {
		view.Close()
	}
	return result, err
}

type storedNodeImage struct {
	fact       StoredNodeFact
	properties map[string]any
	full       *Node
}
type storedEdgeImage struct {
	fact       StoredEdgeFact
	properties map[string]any
	full       *Edge
}

var storedReadIDs atomic.Uint64

func storedIdentity(b *BadgerEngine, txn *badger.Txn, transaction string) StoredReadIdentity {
	id := b.storedReadID.Load()
	if id == 0 {
		b.storedReadID.CompareAndSwap(0, storedReadIDs.Add(1))
		id = b.storedReadID.Load()
	}
	return StoredReadIdentity{Store: id, Timestamp: txn.ReadTs(), Transaction: transaction}
}

// Reference generation comes from this transaction's persisted dictionary
// entry. Node numbering is #1667's snapshotNodeRef, never the live dictionary.
func (b *BadgerEngine) storedNodeReference(txn *badger.Txn, id NodeID) (StoredReference, error) {
	ref, found, err := b.snapshotNodeRef(txn, id)
	if err != nil || !found {
		return StoredReference{}, err
	}
	item, err := txn.Get(nodeIDForwardKey(id))
	if err != nil {
		return StoredReference{}, err
	}
	return StoredReference{ref.num, item.Version()}, nil
}
func storedEdgeReference(txn *badger.Txn, id EdgeID) (StoredReference, error) {
	item, err := txn.Get(edgeIDForwardKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return StoredReference{}, nil
	}
	if err != nil {
		return StoredReference{}, err
	}
	ref := StoredReference{Generation: item.Version()}
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return ErrInvalidData
		}
		ref.NumericID = binary.BigEndian.Uint64(v)
		return nil
	})
	return ref, err
}
func storedReverseNode(txn *badger.Txn, num uint64) (NodeID, error) {
	return (embeddingRecordReader{txn: txn}).nodeID(num)
}
func storedReverseEdge(txn *badger.Txn, num uint64) (EdgeID, error) {
	item, err := txn.Get(append([]byte{prefixIDDictEdgeReverse}, encodeNumID(num)...))
	if err != nil {
		return "", err
	}
	var id EdgeID
	err = item.Value(func(v []byte) error { id = EdgeID(v); return nil })
	return id, err
}
func storedHead(txn *badger.Txn, key []byte) (MVCCHead, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return MVCCHead{}, nil
	}
	if err != nil {
		return MVCCHead{}, err
	}
	var head MVCCHead
	err = item.Value(func(v []byte) error { var e error; head, e = decodeMVCCHead(v); return e })
	return head, err
}

func (b *BadgerEngine) storedNodeInTxn(ctx context.Context, source storedEntityView, id NodeID, r StoredReadRequest, view StoredReadIdentity) (storedNodeImage, error) {
	txn := source.read.txn
	if err := b.pendingReadConflict(readViewOptions{ctx: ctx, scopeKind: readNodeIDsScope, nodeIDs: []NodeID{id}}); err != nil {
		return storedNodeImage{}, err
	}
	out := storedNodeImage{fact: StoredNodeFact{ID: NodeID(strings.Clone(string(id))), View: view}}
	out.fact.View.Namespace = strings.Clone(namespaceForNodeID(id))
	if err := ctx.Err(); err != nil {
		return out, err
	}
	ref, err := b.storedNodeReference(txn, id)
	if err != nil {
		return out, err
	}
	out.fact.Reference = ref
	if ref.NumericID != 0 {
		out.fact.Head, err = storedHead(txn, mvccNodeHeadKey(ref.NumericID))
		if err != nil {
			return out, err
		}
	}
	err = source.withBoundary(StoredEntityNodePriorRead, string(id), func() error {
		return source.value(bufferedNodeEntity, string(id), func(data []byte) error {
			var e error
			if r.Mode == StoredFullMaterialization {
				if r.Work != nil {
					r.Work.FullMaterializations++
					r.Work.EnvelopeParses++
				}
				out.full, e = source.nodeRecord(b, id, data)
				if e != nil {
					return e
				}
				out.fact.Exists = true
				out.fact.Labels, e = cloneStoredLabels(ctx, out.full.Labels)
				if e != nil {
					return e
				}
				if r.Work != nil {
					r.Work.PropertyValues += uint64(len(out.full.Properties))
					for _, v := range out.full.ChunkEmbeddings {
						r.Work.VectorValues += uint64(len(v))
					}
					for _, v := range out.full.NamedEmbeddings {
						r.Work.VectorValues += uint64(len(v))
					}
				}
				return ctx.Err()
			}
			metadata, e := decodeNodeFactMetadata(ctx, data, r.Work)
			if e != nil {
				return e
			}
			out.fact.Exists = true
			out.fact.Labels = metadata.Labels
			if r.Mode == StoredSelectedProperties {
				out.properties, e = b.storedSelectedProperties(ctx, id, data, r.NodeProperties, true, r.Work)
			}
			return e
		})
	})
	if err == nil && !out.fact.Exists && !out.fact.Head.Version.IsZero() && !out.fact.Head.Tombstoned {
		return out, ErrNotImplemented
	}
	return out, err
}
func (b *BadgerEngine) storedEdgeInTxn(ctx context.Context, source storedEntityView, id EdgeID, r StoredReadRequest, view StoredReadIdentity) (storedEdgeImage, error) {
	txn := source.read.txn
	out := storedEdgeImage{fact: StoredEdgeFact{ID: EdgeID(strings.Clone(string(id))), View: view}}
	out.fact.View.Namespace = strings.Clone(namespaceForEdgeID(id))
	if err := ctx.Err(); err != nil {
		return out, err
	}
	ref, err := storedEdgeReference(txn, id)
	if err != nil {
		return out, err
	}
	out.fact.Reference = ref
	if ref.NumericID != 0 {
		out.fact.Head, err = storedHead(txn, mvccEdgeHeadKey(ref.NumericID))
		if err != nil {
			return out, err
		}
	}
	err = source.withBoundary(StoredEntityEdgePriorRead, string(id), func() error {
		return source.value(bufferedEdgeEntity, string(id), func(data []byte) error {
			header := new(Edge)
			start, end, offset, e := decodeEdgeCompactHeaderInto(data, edgeFormatCompactV2, header)
			if e != nil {
				return e
			}
			out.fact.Exists = true
			out.fact.Type = strings.Clone(header.Type)
			out.fact.StartNode, e = storedReverseNode(txn, start)
			if e != nil {
				return e
			}
			out.fact.EndNode, e = storedReverseNode(txn, end)
			if e != nil {
				return e
			}
			out.fact.StartReference, e = b.storedNodeReference(txn, out.fact.StartNode)
			if e != nil {
				return e
			}
			out.fact.EndReference, e = b.storedNodeReference(txn, out.fact.EndNode)
			if e != nil {
				return e
			}
			if r.Work != nil {
				r.Work.EnvelopeParses++
				r.Work.MetadataBytes += uint64(offset + len(out.fact.StartNode) + len(out.fact.EndNode))
			}
			if r.Mode == StoredSelectedProperties {
				out.properties, e = b.storedSelectedProperties(ctx, NodeID(id), data[offset:], r.EdgeProperties, false, r.Work)
			}
			if r.Mode == StoredFullMaterialization {
				if r.Work != nil {
					r.Work.FullMaterializations++
					r.Work.EnvelopeParses++
				}
				out.full, e = source.edgeRecord(b, id, data)
				if r.Work != nil && out.full != nil {
					r.Work.PropertyValues += uint64(len(out.full.Properties))
				}
			}
			return e
		})
	})
	if err == nil && !out.fact.Exists && !out.fact.Head.Version.IsZero() && !out.fact.Head.Tombstoned {
		return out, ErrNotImplemented
	}
	return out, err
}

// Selected properties reuse the token grammar, token selection and typed-value
// decoder used by projected reads, without decoding Node/EmbedMeta carriers.
func (b *BadgerEngine) storedSelectedProperties(ctx context.Context, id NodeID, data []byte, keys []string, node bool, work *StoredReadWork) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if node {
		envelope, err := SplitNodeEnvelope(data)
		if err != nil {
			return nil, err
		}
		if work != nil {
			work.EnvelopeParses++
		}
		data = envelope.properties
	}
	d := newProjectedNodeDecoder(b, StreamNodesOptions{Projection: keys})
	d.useNamespace([]byte(id))
	d.ctx = ctx
	d.work = work
	d.dec.Reset(util.ReaderWithContext(ctx, &d.reader))
	_, err := d.decodeProperties(data)
	if err != nil {
		return nil, err
	}
	return d.scratch, nil
}

// Copies just the explicitly requested keys. Metadata never calls a payload
// copier; nested selected values are detached through the existing codec.
func storedSelectedMemory(ctx context.Context, props map[string]any, keys []string, work *StoredReadWork) (map[string]any, error) {
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if value, ok := props[key]; ok {
			data, err := encodeValueWithContext(ctx, value)
			if err != nil {
				return nil, err
			}
			var owned any
			if err := decodeValueWithContext(ctx, data, &owned); err != nil {
				return nil, err
			}
			out[key] = owned
			if work != nil {
				work.PropertyValues++
				work.PropertyBytes += uint64(len(data))
			}
		}
	}
	return out, nil
}
func storedNodeFromMemory(ctx context.Context, id NodeID, state bufferedEntityState, r StoredReadRequest, view StoredReadIdentity, pending bool) (storedNodeImage, error) {
	if err := ctx.Err(); err != nil {
		return storedNodeImage{}, err
	}
	out := storedNodeImage{fact: StoredNodeFact{ID: NodeID(strings.Clone(string(id))), View: view, Pending: pending, Buffered: !pending, Exists: !state.deleted && state.node != nil}}
	out.fact.Head.Tombstoned = state.deleted
	out.fact.Head.Version = state.version
	out.fact.View.Namespace = strings.Clone(namespaceForNodeID(id))
	if !out.fact.Exists {
		return out, nil
	}
	var err error
	out.fact.Labels, err = cloneStoredLabels(ctx, state.node.Labels)
	if err != nil {
		return out, err
	}
	if r.Work != nil {
		r.Work.MetadataBytes += uint64(len(id))
		for _, label := range out.fact.Labels {
			r.Work.MetadataBytes += uint64(len(label))
		}
	}
	if r.Mode == StoredSelectedProperties {
		out.properties, err = storedSelectedMemory(ctx, state.node.Properties, r.NodeProperties, r.Work)
	}
	if r.Mode == StoredFullMaterialization {
		if r.Work != nil {
			r.Work.FullMaterializations++
		}
		out.full, err = copyNodeWithContext(ctx, state.node)
	}
	return out, err
}
func storedEdgeFromMemory(ctx context.Context, id EdgeID, state bufferedEntityState, r StoredReadRequest, view StoredReadIdentity, pending bool) (storedEdgeImage, error) {
	if err := ctx.Err(); err != nil {
		return storedEdgeImage{}, err
	}
	out := storedEdgeImage{fact: StoredEdgeFact{ID: EdgeID(strings.Clone(string(id))), View: view, Pending: pending, Buffered: !pending, Exists: !state.deleted && state.edge != nil}}
	out.fact.Head.Tombstoned = state.deleted
	out.fact.Head.Version = state.version
	out.fact.View.Namespace = strings.Clone(namespaceForEdgeID(id))
	if !out.fact.Exists {
		return out, nil
	}
	edge := state.edge
	out.fact.Type = strings.Clone(edge.Type)
	out.fact.StartNode = NodeID(strings.Clone(string(edge.StartNode)))
	out.fact.EndNode = NodeID(strings.Clone(string(edge.EndNode)))
	if r.Work != nil {
		r.Work.MetadataBytes += uint64(len(id) + len(edge.Type) + len(edge.StartNode) + len(edge.EndNode))
	}
	var err error
	if r.Mode == StoredSelectedProperties {
		out.properties, err = storedSelectedMemory(ctx, edge.Properties, r.EdgeProperties, r.Work)
	}
	if r.Mode == StoredFullMaterialization {
		if r.Work != nil {
			r.Work.FullMaterializations++
		}
		out.full, err = copyEdgeWithContext(ctx, edge)
	}
	return out, err
}

// storedReadInTxn is the batched native operation, used with a captured reader
// or the actual transaction's existing pinned reader. Overlay resolvers belong
// to that same capture; they never consult current mutable base state.
func (b *BadgerEngine) storedReadInTxn(ctx context.Context, txn *badger.Txn, r StoredReadRequest, transaction string, nodeState func(NodeID) (bufferedEntityState, bool), edgeState func(EdgeID) (bufferedEntityState, bool), overlayAdjacency func(func(EdgeID)) error, pending bool) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view := storedIdentity(b, txn, transaction)
	requestedNodes := make(map[NodeID]bool, len(r.Nodes))
	for _, id := range r.Nodes {
		requestedNodes[id] = true
	}
	requestedEdges := make(map[EdgeID]bool, len(r.Edges))
	for _, id := range r.Edges {
		requestedEdges[id] = true
	}
	requestedOutgoing, requestedIncoming := map[NodeID]bool{}, map[NodeID]bool{}
	for _, adjacency := range r.Adjacency {
		if adjacency.Direction == Outgoing {
			requestedOutgoing[adjacency.Node] = true
		} else {
			requestedIncoming[adjacency.Node] = true
		}
	}
	physicalSource := storedEntityView{read: embeddingRecordReader{txn: txn, ctx: ctx}, boundaries: b.storedEntityObservers.Load()}
	logicalSource := storedEntityView{selectedState: func(kind bufferedEntityKind, key string) bufferedEntityState {
		switch kind {
		case bufferedNodeEntity:
			if nodeState != nil {
				value, _ := nodeState(NodeID(key))
				return value
			}
		case bufferedEdgeEntity:
			if edgeState != nil {
				value, _ := edgeState(EdgeID(key))
				return value
			}
		}
		return bufferedEntityState{}
	}}

	facts := StoredMetadata{Views: []StoredReadIdentity{view}}
	nodeIDs := slices.Clone(r.Nodes)
	edgeIDs := slices.Clone(r.Edges)
	nodeSeen := map[NodeID]bool{}
	edgeSeen := map[EdgeID]bool{}
	for _, a := range r.Adjacency {
		if a.Direction != Outgoing && a.Direction != Incoming {
			return nil, ErrInvalidData
		}
		nodeIDs = append(nodeIDs, a.Node)
		ref, err := b.storedNodeReference(txn, a.Node)
		if err != nil {
			return nil, err
		}
		if ref.NumericID == 0 {
			continue
		}
		prefix := outgoingIndexPrefix(ref.NumericID)
		if a.Direction == Incoming {
			prefix = incomingIndexPrefix(ref.NumericID)
		}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				it.Close()
				return nil, err
			}
			num, ok := extractEdgeNumIDFromOutgoingKey(it.Item().Key())
			if !ok {
				it.Close()
				return nil, ErrInvalidData
			}
			id, err := storedReverseEdge(txn, num)
			if err != nil {
				it.Close()
				return nil, err
			}
			edgeIDs = append(edgeIDs, id)
		}
		it.Close()
	}
	if overlayAdjacency != nil {
		if err := overlayAdjacency(func(id EdgeID) { edgeIDs = append(edgeIDs, id) }); err != nil {
			return nil, err
		}
	}
	projection := StoredProjection{}
	full := StoredFullRead{}
	for _, id := range edgeIDs {
		if edgeSeen[id] {
			continue
		}
		edgeSeen[id] = true
		var physical, current storedEdgeImage
		source := logicalSource
		err := source.withSelectedEntity(b, bufferedEdgeEntity, string(id), func(state bufferedEntityState) error {
			physicalRequest := r
			if !r.Physical {
				physicalRequest.Mode = StoredMetadataOnly
			}
			var err error
			physical, err = b.storedEdgeInTxn(ctx, physicalSource, id, physicalRequest, view)
			if err != nil {
				return err
			}
			current, err = storedEdgeFromMemory(ctx, id, state, r, view, pending)
			if err != nil {
				return err
			}
			current.fact.Reference = physical.fact.Reference
			current.fact.Head.FloorVersion = physical.fact.Head.FloorVersion
			if pending {
				current.fact.Head.Version = physical.fact.Head.Version
			}
			return nil
		}, func() error {
			var err error
			physical, err = b.storedEdgeInTxn(ctx, physicalSource, id, r, view)
			current = physical
			return err
		})
		if err != nil {
			return nil, err
		}
		if current.fact.Exists {
			if err := b.pendingReadConflict(readViewOptions{ctx: ctx, scopeKind: readNodeIDsScope, nodeIDs: []NodeID{current.fact.StartNode, current.fact.EndNode}}); err != nil {
				return nil, err
			}
			current.fact.StartReference, err = b.storedNodeReference(txn, current.fact.StartNode)
			if err != nil {
				return nil, err
			}
			current.fact.EndReference, err = b.storedNodeReference(txn, current.fact.EndNode)
			if err != nil {
				return nil, err
			}
		}
		// Adjacency candidates can include an edge moved away by an overlay. Keep
		// physical facts when requested, but do not invent current incident rows.
		requested := requestedEdges[id]
		if !requested {
			requested = requestedOutgoing[current.fact.StartNode] || requestedIncoming[current.fact.EndNode]
		}
		if r.Physical {
			facts.PhysicalEdges = append(facts.PhysicalEdges, physical.fact)
			if physical.fact.Exists {
				if r.Mode == StoredSelectedProperties {
					properties, err := storedSelectedMemory(ctx, physical.properties, r.EdgeProperties, r.Work)
					if err != nil {
						return nil, err
					}
					projection.PhysicalEdges = append(projection.PhysicalEdges, StoredEdgeProjection{physical.fact, properties})
				}
				if r.Mode == StoredFullMaterialization {
					if r.Work != nil {
						r.Work.FullMaterializations++
					}
					owned, err := copyEdgeWithContext(ctx, physical.full)
					if err != nil {
						return nil, err
					}
					full.PhysicalEdges = append(full.PhysicalEdges, owned)
				}
			}
		}
		if !requested {
			continue
		}
		facts.Edges = append(facts.Edges, current.fact)
		if current.fact.Exists {
			nodeIDs = append(nodeIDs, current.fact.StartNode, current.fact.EndNode)
			if r.Mode == StoredSelectedProperties {
				projection.Edges = append(projection.Edges, StoredEdgeProjection{current.fact, current.properties})
			}
			if r.Mode == StoredFullMaterialization {
				full.Edges = append(full.Edges, current.full)
			}
		}
	}
	for _, id := range nodeIDs {
		if nodeSeen[id] {
			continue
		}
		nodeSeen[id] = true
		nodeRequest := r
		if !requestedNodes[id] {
			nodeRequest.Mode = StoredMetadataOnly
		}
		var physical, current storedNodeImage
		source := logicalSource
		err := source.withSelectedEntity(b, bufferedNodeEntity, string(id), func(state bufferedEntityState) error {
			physicalRequest := nodeRequest
			if !r.Physical {
				physicalRequest.Mode = StoredMetadataOnly
			}
			var err error
			physical, err = b.storedNodeInTxn(ctx, physicalSource, id, physicalRequest, view)
			if err != nil {
				return err
			}
			current, err = storedNodeFromMemory(ctx, id, state, nodeRequest, view, pending)
			if err != nil {
				return err
			}
			current.fact.Reference = physical.fact.Reference
			current.fact.Head.FloorVersion = physical.fact.Head.FloorVersion
			if pending {
				current.fact.Head.Version = physical.fact.Head.Version
			}
			return nil
		}, func() error {
			var err error
			physical, err = b.storedNodeInTxn(ctx, physicalSource, id, nodeRequest, view)
			current = physical
			return err
		})
		if err != nil {
			return nil, err
		}
		facts.Nodes = append(facts.Nodes, current.fact)
		if r.Physical {
			facts.PhysicalNodes = append(facts.PhysicalNodes, physical.fact)
			if physical.fact.Exists {
				if nodeRequest.Mode == StoredSelectedProperties {
					properties, err := storedSelectedMemory(ctx, physical.properties, r.NodeProperties, r.Work)
					if err != nil {
						return nil, err
					}
					projection.PhysicalNodes = append(projection.PhysicalNodes, StoredNodeProjection{physical.fact, properties})
				}
				if nodeRequest.Mode == StoredFullMaterialization {
					if r.Work != nil {
						r.Work.FullMaterializations++
					}
					owned, err := copyNodeWithContext(ctx, physical.full)
					if err != nil {
						return nil, err
					}
					full.PhysicalNodes = append(full.PhysicalNodes, owned)
				}
			}
		}
		if current.fact.Exists {
			if nodeRequest.Mode == StoredSelectedProperties {
				projection.Nodes = append(projection.Nodes, StoredNodeProjection{current.fact, current.properties})
			}
			if nodeRequest.Mode == StoredFullMaterialization {
				full.Nodes = append(full.Nodes, current.full)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch r.Mode {
	case StoredMetadataOnly:
		return facts, nil
	case StoredSelectedProperties:
		projection.Facts = facts
		return projection, nil
	case StoredFullMaterialization:
		full.Facts = facts
		return full, nil
	}
	return nil, ErrInvalidData
}

func (*BadgerTransaction) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (tx *BadgerTransaction) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}
	// A transaction begun under ACK buffering has no immutable body overlay
	// capture in the existing transaction owner. Refuse rather than read latest.
	if tx.engine.writeBehind != nil {
		return nil, ErrNotImplemented
	}
	if tx.snapshotTx == nil {
		return nil, ErrNotImplemented
	}
	if err := visitStoredReadRequestIDs(r, tx.pinNamespaceFromIDLocked); err != nil {
		return nil, err
	}
	var result StoredReadResult
	err := tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
		if r.Work != nil {
			r.Work.Captures++
		}
		var readErr error
		result, readErr = tx.engine.storedReadInTxn(ctx, txn, r, tx.ID, func(id NodeID) (bufferedEntityState, bool) {
			if _, deleted := tx.deletedNodes[id]; deleted {
				return bufferedEntityState{found: true, deleted: true}, true
			}
			n, ok := tx.pendingNodes[id]
			return bufferedEntityState{node: n, found: ok}, ok
		}, func(id EdgeID) (bufferedEntityState, bool) {
			if _, deleted := tx.deletedEdges[id]; deleted {
				return bufferedEntityState{found: true, deleted: true}, true
			}
			e, ok := tx.pendingEdges[id]
			return bufferedEntityState{edge: e, found: ok}, ok
		}, func(emit func(EdgeID)) error {
			for _, a := range r.Adjacency {
				for id, edge := range tx.pendingAdjacency[a.Node] {
					if err := ctx.Err(); err != nil {
						return err
					}
					if a.Direction == Outgoing && edge.StartNode == a.Node || a.Direction == Incoming && edge.EndNode == a.Node {
						emit(id)
					}
				}
			}
			return nil
		}, true)
		return readErr
	})
	return result, err
}

func (*badgerReadSnapshot) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (s *badgerReadSnapshot) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	// This legacy snapshot owns full node overlays and visible-only edge
	// overlays. Its physical captured view is sufficient only without overlays.
	var result StoredReadResult
	err := s.view.withView(func(txn *badger.Txn) error {
		if len(s.nodesTouched) != 0 || len(s.edgesTouched) != 0 {
			return ErrNotImplemented
		}
		if s.prefix != "" {
			if err := visitStoredReadRequestIDs(r, func(id string) error {
				if !strings.HasPrefix(id, s.prefix) {
					return ErrInvalidID
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if r.Work != nil {
			r.Work.Captures++
		}
		var e error
		result, e = s.b.storedReadInTxn(ctx, txn, r, "", nil, nil, nil, false)
		return e
	})
	return result, err
}

// Clone required strings too: a warm label/ID may itself be a substring of an
// arbitrary property. Metadata results must not keep that backing payload alive.
func cloneStoredLabels(ctx context.Context, labels []string) ([]string, error) {
	if labels == nil {
		return nil, ctx.Err()
	}
	out := make([]string, len(labels))
	for i, label := range labels {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i] = strings.Clone(label)
	}
	return out, ctx.Err()
}

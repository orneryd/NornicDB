package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

// GetNode retrieves a node (read-your-writes).
//
// Reads pin the transaction to the node's namespace if it isn't already
// pinned. Without this, a transaction's pre-pin readTS sits at sequence 0
// and visibility checks against a head whose FloorVersion is non-zero
// reject every committed value as "not yet visible" — which manifests as
// lost updates under the Read-Modify-Write retry loop in DB.Update.
func (tx *BadgerTransaction) GetNode(ctx context.Context, nodeID NodeID) (*Node, error) {
	return readTransactionEntity(ctx, tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, transactionNodeRead{mvccEntityRead: mvccEntityRead{vectors: true, visibility: visibleRead}})
}

// GetQueryNode implements QueryNodeReader inside the
// transaction: GetNode's visibility (pending writes, deletes, connected
// deletes, the MVCC snapshot) and the node's settled embedding metadata, the
// worker sidecar's included, without reading separately stored chunk
// vectors. Vectors already in memory (pending nodes, snapshot caches, the
// body) may be present and are caller-owned; a caller that needs the
// vectors reads with GetNode. Query execution reads nodes this way: rows
// never carry vectors, and an update reconciles embeddings against the stored
// node (reconcileManagedEmbeddingsLocked), not the caller's copy.
func (tx *BadgerTransaction) GetQueryNode(ctx context.Context, nodeID NodeID) (*Node, error) {
	return readTransactionEntity(ctx, tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, transactionNodeRead{mvccEntityRead: mvccEntityRead{visibility: visibleRead}})
}

// GetNodeProjected implements ProjectedNodeReader inside the transaction: it
// decodes only the requested user properties (none when properties is an empty
// non-nil slice) while preserving ID, labels, timestamps and embedding
// metadata, and without hydrating stored embedding vectors. A nil properties
// slice means "full node". Visibility is the transaction's own: pending
// writes, deletes, connected deletes and the MVCC snapshot all apply exactly
// as in GetNode.
func (tx *BadgerTransaction) GetNodeProjected(ctx context.Context, nodeID NodeID, properties []string) (*Node, error) {
	return readTransactionEntity(ctx, tx, nodeID, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, transactionNodeRead{mvccEntityRead: mvccEntityRead{visibility: visibleRead}, properties: properties, projected: true})
}

// getCommittedNodeProjectedLocked is getCommittedNodeLocked for a projected
// read: the same write-behind overlay, snapshot caches and MVCC resolution,
// but decoding only the requested properties and never hydrating embeddings.
func (tx *BadgerTransaction) getCommittedNodeProjectedLocked(ctx context.Context, nodeID NodeID, properties []string) (*Node, error) {
	if tx.engine != nil && (tx.engine.writeBehind != nil || tx.readsNodeLastCommittedLocked(nodeID)) {
		return tx.engine.GetNodeProjected(ctx, nodeID, properties)
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
	node, err := readEntityAtWithView(ctx, tx.engine, nodeID, tx.readTS, properties, tx.withSnapshotViewLocked, (*BadgerEngine).getNodeProjectedVisibleAtInTxn)
	if err == ErrNotVisibleAtSnapshot {
		if tx.engine.writeBehind != nil {
			if fresh, freshErr := tx.engine.GetNodeProjected(ctx, nodeID, properties); freshErr == nil {
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
			if fresh, freshErr := tx.engine.GetNodeProjected(ctx, nodeID, properties); freshErr == nil {
				return fresh, nil
			}
		}
	}
	return node, err
}

// getNodeFromBadgerSnapshotProjectedLocked reads and decodes a node from the
// transaction's pinned snapshot, projecting to the requested properties. The
// decode runs inside the snapshot view so the body's property tokens resolve
// through the names that snapshot holds.
func (tx *BadgerTransaction) getNodeFromBadgerSnapshotProjectedLocked(nodeID NodeID, include map[string]struct{}) (*Node, error) {
	key := nodeKey(nodeID)
	var node *Node
	var decodeErr error
	err := tx.withSnapshotViewLocked(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			node, decodeErr = tx.engine.decodeNodeProjected(rtxn, namespaceForNodeID(nodeID), val, include)
			return nil
		})
	})
	switch {
	case decodeErr != nil:
		return nil, decodeErr
	case err == badger.ErrKeyNotFound:
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("reading node: %w", err)
	}
	return node, nil
}

// RelationshipEndpointVisible implements RelationshipEndpointChecker with the
// same answer GetNode gives inside this transaction. While the visibility layer
// can hide an image (knowledge-policy decay or an entity suppressed on demand,
// with no reveal scope open) the check declines, so GetNode keeps the answer:
// its completion applies that filter to every image it can return, overlay
// images included. Otherwise the transaction's own delete or write decides
// first. A committed node is decided from its MVCC head alone when the
// snapshot is at or after the head's version: visible unless the head is a
// tombstone. An older snapshot, a node without a head and a read error also
// leave the answer to GetNode.
func (tx *BadgerTransaction) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	if id == "" {
		return false, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.ensureOperationAllowedLocked(transactionRead) != nil || tx.pinNamespaceFromIDLocked(string(id)) != nil {
		return false, false
	}
	// One guard for the whole visibility layer, ahead of every shortcut below:
	// any of their images can be hidden at read time, and only the point read
	// applies that filter, so it owns the answer while one is active.
	if tx.engine.visibilityFiltered() {
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
	if tx.readsNodeLastCommittedLocked(id) {
		// Read as last committed: GetNode owns the answer, not the snapshot.
		return false, false
	}
	if _, cached := tx.snapshotPrefixNodeByID[id]; cached {
		return true, true
	}
	if tx.readTS.IsZero() {
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
func (tx *BadgerTransaction) GetEdge(ctx context.Context, edgeID EdgeID) (*Edge, error) {
	return readTransactionEntity(ctx, tx, edgeID, (*BadgerTransaction).edgeReadState, cloneTransactionEdge, loadTransactionEdge, visibleRead)
}

// ReadEdges answers one adjacent-edge request from the transaction's view: its
// snapshot (or write-behind, read-committed) view of the committed adjacency
// overlaid with its own pending writes and excluding its pending deletes, just
// as the transaction's other adjacent-edge reads merge them.
//
// The request's direction selects the adjacency (a between read keeps the
// engine's current view, as the engine's own between read answers it), its type
// set filters the relationships before they are hydrated, and Headers asks for
// ID, type and endpoints without the relationship records.
//
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first.
func (tx *BadgerTransaction) ReadEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(request.Node)); err != nil {
		return nil, err
	}
	if request.Direction == AdjacentBetween {
		if err := tx.pinNamespaceFromIDLocked(string(request.Other)); err != nil {
			return nil, err
		}
	}
	// Decay/promotion can inspect relationship properties. A header hint must
	// never remove those inputs before either committed or pending visibility
	// is decided; the record remains a valid answer to a header request.
	if request.Headers && tx.engine.visibilityFiltered() {
		request.Headers = false
	}
	var committed []*Edge
	var err error
	if request.Direction == AdjacentBetween {
		committed, err = tx.engine.ReadAdjacentEdges(ctx, request)
	} else {
		committed, err = tx.getCommittedAdjacentEdgesLocked(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	now := DecayScoringTime()
	return tx.mergePendingAdjacentEdgesLocked(ctx, committed, request, func(edge *Edge) bool {
		return !tx.engine.filterEntityByDecay(visibleRead, edge, now) && request.admits(edge)
	})
}

// GetEdgeBetween returns a matching edge including pending transaction writes.
func (tx *BadgerTransaction) GetEdgeBetween(ctx context.Context, startID, endID NodeID, edgeType string) (*Edge, error) {
	request := AdjacentEdgeRead{Direction: AdjacentBetween, Node: startID, Other: endID}
	if edgeType != "" {
		request.Types = []string{edgeType}
	}
	edges, err := tx.ReadEdges(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if edgeType == "" || edge.Type == edgeType {
			return edge, nil
		}
	}
	return nil, nil
}

// getCommittedEdgesByTypeLocked reads the committed edges of edgeType ("" is
// every edge) visible at the pinned snapshot through the edge type index, then
// merges the transaction's pending edge writes and deletes as the public reads
// do. The type index in that snapshot is the membership at BEGIN and the read
// retains the global collection contract of AllEdges and GetEdgesByType.
// The caller supplies the original pending-record filter and holds tx.mu.
func (tx *BadgerTransaction) getCommittedEdgesByTypeLocked(ctx context.Context, edgeType string, includePending func(*Edge) bool) ([]*Edge, error) {
	committed := make([]*Edge, 0)
	err := tx.engine.streamEdgesByTypeAtSnapshotWithView(ctx, "", edgeType, tx.readTS, tx.withSnapshotViewLocked, visibleRead, func(edge *Edge) error {
		committed = append(committed, edge)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(ctx, committed, includePending)
}

// AllEdges returns all visible edges including pending transaction writes.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first.
func (tx *BadgerTransaction) AllEdges(ctx context.Context) ([]*Edge, error) {
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
	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, and buffered edges are
		// invisible to the pinned view. Read the latest overlaid view.
		committed, err := tx.engine.AllEdges(ctx)
		if err != nil {
			return nil, err
		}
		return tx.mergePendingEdgesLocked(ctx, committed, func(edge *Edge) bool { return edge != nil })
	}
	return tx.getCommittedEdgesByTypeLocked(ctx, "", func(edge *Edge) bool { return edge != nil })
}

// GetEdgesByType returns edges of a given type including pending transaction writes.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first.
func (tx *BadgerTransaction) GetEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
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

	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, so read the latest
		// (overlaid) view instead.
		committed, err := tx.engine.GetEdgesByType(ctx, edgeType)
		if err != nil {
			return nil, err
		}
		return tx.mergePendingEdgesLocked(ctx, committed, func(edge *Edge) bool {
			return edge.Type == edgeType
		})
	}
	return tx.getCommittedEdgesByTypeLocked(ctx, edgeType, func(edge *Edge) bool { return edge.Type == edgeType })
}

// GetNodesByLabel returns nodes with the given label including pending transaction writes.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first.
func (tx *BadgerTransaction) GetNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
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

	committed, err := tx.getNodesByLabelLocked(ctx, label)
	if err != nil {
		return nil, err
	}
	// A merged row is a delivered row, so the live visibility layer decides it
	// here as it does for a streamed row: the committed population was read
	// under the policy of that read, and the pending image is the
	// transaction's own capture of an earlier one.
	return tx.mergePendingNodesLocked(ctx, committed, func(node *Node) bool {
		if node == nil {
			return false
		}
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
				return true
			}
		}
		return false
	})
}

// streamVisitFrame is the per-stream state a streamed read shares with the one
// owner of its release/retake: whether that owner holds the transaction's lock.
// A stream creates one frame and passes it to every record's visit, so neither
// the per-record path nor the transaction carries a visitor's lifetime.
//
// held is the ownership flag the stream's deferred release reads, and it is
// accurate at every point: false from before the lock is released until the
// retake owns it again. A visitor that panics therefore leaves the deferred
// release with nothing to release, so the visitor's panic propagates unchanged
// and a lock this stream no longer holds is never unlocked.
type streamVisitFrame struct {
	held bool
}

// streamVisitUnlocked is the one owner of the release/retake every streamed
// read performs around a record's visitor. It runs the visitor with the
// transaction's lock released and retakes that lock on the read's own context:
// the lock cannot span the visitor (the caller's callback may write), and the
// retake cannot outlive the read, so a request canceled, closed or detached
// while the visitor ran ends here instead of queueing behind an unrelated
// holder, and no further record is delivered.
//
// The lock is released only after the frame reports it unheld, which keeps the
// caller's deferred release accurate across a visitor panic.
//
// Errors keep their identity. A visitor error is returned unchanged when the
// stream owns the lock again, so ErrIterationStopped still ends a stream as the
// sentinel it is. The client's closure and detachment are checked on their own
// atomic state before the retake waits, and everything else the transaction
// admits is validated on the retaken hold; such an error is reported alone when
// the visitor succeeded and joined with the visitor's error when it did not, so
// a stream whose transaction was closed or canceled while its visitor ran
// reports that instead of losing either failure. Intentional early stop keeps
// its existing sentinel when only the retake's context ended: the visitor has
// already completed its requested prefix. Closing the transaction is still an
// error, even when the visitor requested an early stop.
func streamVisitUnlocked[T any](tx *BadgerTransaction, ctx context.Context, frame *streamVisitFrame, record T, visit func(T) error) error {
	frame.held = false
	tx.mu.Unlock()
	visitErr := visit(record)
	err := tx.clientOwnershipError(transactionRead)
	if err == nil {
		if err = tx.mu.LockContext(ctx); err == nil {
			frame.held = true
			err = tx.ensureOperationAllowedLocked(transactionRead)
		}
	}
	switch {
	case err == nil:
		return visitErr
	case visitErr == ErrIterationStopped && (err == context.Canceled || err == context.DeadlineExceeded):
		// The visitor deliberately completed its prefix. It may cancel its
		// context to retire work, but that does not change the stream's
		// established early-stop result. No hold remains to release here.
		return visitErr
	case visitErr == nil:
		return err
	default:
		return errors.Join(visitErr, err)
	}
}

// StreamNodesByLabelProjected visits label matches from the transaction's
// pinned snapshot and overlays pending writes as one stream. The callback can
// stop iteration without forcing the transaction to resolve or copy the rest
// of the label population.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first. It also ends the retake after each visitor callback, so a canceled
// stream stops when its visitor returns rather than waiting for that lock, and
// a transaction the client closed or detached while the visitor ran is reported
// the same way, before that retake waits.
func (tx *BadgerTransaction) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return tx.streamNodesByLabelProjected(ctx, label, properties, visibleRead, visit)
}

func (tx *BadgerTransaction) streamNodesByLabelProjected(ctx context.Context, label string, properties []string, v readVisibility, visit func(*Node) error, readOptions ...StreamNodesOptions) error {
	opts := nodeScanOptions(readOptions)
	queryNodes := opts.QueryNodes && !opts.WithEmbeddings
	if visit == nil {
		return ErrInvalidData
	}
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	frame := streamVisitFrame{held: true}
	defer func() {
		if frame.held {
			tx.mu.Unlock()
		}
	}()
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

	// The visitor runs with the transaction's lock released; the retake
	// carries the caller's context like the admission above.
	invokeVisit := func(node *Node) error {
		return streamVisitUnlocked(tx, ctx, &frame, node, visit)
	}

	loadImage := tx.engine.GetStoredNode
	if queryNodes || properties != nil {
		loadImage = func(ctx context.Context, id NodeID) (*Node, error) {
			return tx.engine.readNodeMetadata(ctx, id, storedRead, nodeItemQueryMetadata)
		}
	}
	pendingNodes, overlayErr := writeOwnedReadImagesLocked(ctx, tx.pendingNodes, tx.deletedNodes, &tx.entityLocks, nodeWriteLockKind, loadImage)
	if overlayErr != nil {
		return overlayErr
	}
	hasPending := len(pendingNodes) > 0 || len(tx.deletedNodes) > 0
	var seen map[NodeID]struct{}
	if hasPending {
		seen = make(map[NodeID]struct{}, len(pendingNodes))
	}

	// Write-behind reads are read-committed: the flusher may land a
	// buffered commit after the pinned snapshot, so the engine's latest
	// (overlaid) stream is used and no extra overlay merge runs here.
	engineOverlaid := tx.readTS.IsZero() || (tx.engine != nil && tx.engine.writeBehind != nil)

	// A whole node the stream hands out is also the one a later write of
	// the node takes as its old version (snapshotLabelNodeByID), so the
	// caller gets a copy it may change (#965). A projected node is only
	// read: nothing writes it back.
	forCaller := func(node *Node) *Node {
		if properties == nil {
			return copyNode(node)
		}
		return node
	}
	// The live visibility layer settles a row when the stream delivers it, not
	// when the population it comes from was captured. The cached population,
	// the stopped prefix and the transaction's pending image are each a capture
	// of an earlier read of the policy, and another session may have hidden one
	// of their rows since. The point read of the same image takes the same live
	// decision (readTransactionEntity), so the label stream takes it here. A
	// stored read keeps its documented bypass: filterEntityByDecay never hides a
	// node read as stored.
	nowNanos := DecayScoringTime()
	visible := func(node *Node) bool {
		return !tx.engine.filterEntityByDecay(v, node, nowNanos)
	}
	// pendingRow delivers the transaction's own image of a row: settled against
	// the live policy before projection, so the decision reads the whole pending
	// body, and projected for the row afterwards.
	pendingRow := func(pending *Node) error {
		if !visible(pending) {
			return nil
		}
		return invokeVisit(projectCachedNodeForRead(pending, properties, false))
	}
	emitCommitted := func(node *Node) error {
		if node == nil {
			return nil
		}
		// Eagerly cache scanned labels within the byte/node bound. This is
		// load-bearing for snapshot isolation: later label reads in this
		// transaction must see the scan-time labels, not labels committed
		// afterwards (TestTxReads_LabelCacheBoundPreservesSnapshotLabels).
		if !engineOverlaid && len(tx.snapshotNodeLabels) < maxSnapshotNodeLabelCacheNodes {
			tx.cacheCommittedNodeLabelsLocked(node)
		}
		if hasPending {
			if _, deleted := tx.deletedNodes[node.ID]; deleted {
				seen[node.ID] = struct{}{}
				return nil
			}
			if pending, exists := pendingNodes[node.ID]; exists {
				seen[node.ID] = struct{}{}
				if pending != nil && slices.Contains(pending.Labels, label) {
					return pendingRow(pending)
				}
				return nil
			}
		}
		if !visible(node) {
			return nil
		}
		return invokeVisit(forCaller(node))
	}

	cacheKey := snapshotLabelCacheKey{scope: cacheScope, queryNodes: queryNodes, projectionKey: label}
	cache := tx.snapshotLabelNodes
	if properties != nil {
		cacheKey.projectionKey = snapshotLabelProjectionKey(label, properties)
		cache = tx.snapshotProjectedLabelNodes
	}
	// Cache physical membership, not a visibility-filtered answer. A row
	// suppressed during capture can become visible later without changing the
	// data snapshot. Every delivery, including replay and pending overlays,
	// applies the live policy above; stored reads keep their existing bypass.
	if cached, ok := cache[cacheKey]; ok && !engineOverlaid {
		// Replaying the pinned population is a bulk read like the scan that
		// filled it: the caller's cancellation retires it here.
		var cancel bulkReadCancellation
		for _, node := range cached {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			if err := emitCommitted(node); err != nil {
				return err
			}
		}
		return tx.streamPendingLabelNodesLocked(ctx, label, seen, pendingNodes, pendingRow)
	}

	completed := make([]*Node, 0)
	afterNodeID := NodeID("")
	if prefix := tx.snapshotLabelPrefixNodes[cacheKey]; !engineOverlaid && tx.snapshotTx != nil && len(prefix) > 0 {
		completed = append(completed, prefix...)
		var cancel bulkReadCancellation
		for _, node := range prefix {
			if err := cancel.check(ctx); err != nil {
				return err
			}
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
		err = tx.engine.streamNodesByLabelFromPhysicalSnapshotAfterRead(ctx, scope, label, tx.withSnapshotViewLocked, properties, afterNodeID, storedRead, streamVisit, queryNodes)
	} else if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		err = tx.engine.streamNodesByLabelProjectedInScope(ctx, scope, label, properties, storedRead, streamVisit, StreamNodesOptions{QueryNodes: queryNodes})
	} else {
		err = tx.engine.streamNodesByLabelAtSnapshotWithView(
			ctx,
			scope,
			label, tx.readTS, tx.withSnapshotViewLocked, properties, storedRead, streamVisit, queryNodes,
		)
	}
	if err != nil {
		// A retake the caller's context ended leaves no hold, and a stopped
		// prefix is locked transaction state: store it only while still held.
		if frame.held && err == ErrIterationStopped && !engineOverlaid && tx.snapshotTx != nil {
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

	return tx.streamPendingLabelNodesLocked(ctx, label, seen, pendingNodes, pendingRow)
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
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first. It also ends the retake after each visitor callback, so a canceled
// stream stops when its visitor returns rather than waiting for that lock, and
// a transaction the client closed or detached while the visitor ran is reported
// the same way, before that retake waits.
func (tx *BadgerTransaction) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, visit func(*Node) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if opts.QueryNodes && opts.Projection == nil && !opts.WithEmbeddings && opts.Label != "" && !opts.scanDecodesFilter() {
		queryVisit := borrowQueryNodeScanVisitor(ctx, opts, visit)
		defer queryVisit.release()
		err := tx.streamNodesByLabelProjected(ctx, opts.Label, nil, opts.readVisibility(), queryVisit.callback, opts)
		if err == ErrIterationStopped {
			return nil
		}
		return err
	}
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	frame := streamVisitFrame{held: true}
	defer func() {
		if frame.held {
			tx.mu.Unlock()
		}
	}()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	if err := tx.engine.pendingPrefixConflict(tx.labelScanScopeLocked(opts.Prefix)); err != nil {
		return err
	}

	stopped := false
	invokeVisit := func(node *Node) error {
		err := streamVisitUnlocked(tx, ctx, &frame, node, visit)
		if err == ErrIterationStopped {
			stopped = true
		}
		return err
	}
	inScope := func(id NodeID) bool {
		return opts.Prefix == "" || strings.HasPrefix(string(id), opts.Prefix)
	}
	now := DecayScoringTime()
	emitPending := func(node *Node) error {
		if tx.engine.filterEntityByDecay(opts.readVisibility(), node, now) {
			return nil
		}
		return invokeVisit(opts.copyResidentNode(node))
	}

	loadImage := tx.engine.GetStoredNode
	if !opts.WithEmbeddings {
		loadImage = func(ctx context.Context, id NodeID) (*Node, error) {
			return tx.engine.readNodeMetadata(ctx, id, storedRead, nodeItemQueryMetadata)
		}
	}
	pendingNodes, overlayErr := writeOwnedReadImagesLocked(ctx, tx.pendingNodes, tx.deletedNodes, &tx.entityLocks, nodeWriteLockKind, loadImage)
	if overlayErr != nil {
		return overlayErr
	}
	hasPending := len(pendingNodes) > 0 || len(tx.deletedNodes) > 0
	var seen map[NodeID]struct{}
	if hasPending {
		seen = make(map[NodeID]struct{}, len(pendingNodes))
	}
	emitCommitted := func(node *Node) error {
		if hasPending {
			if _, deleted := tx.deletedNodes[node.ID]; deleted {
				return nil
			}
			if pending, exists := pendingNodes[node.ID]; exists {
				seen[node.ID] = struct{}{}
				return emitPending(pending)
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
	for id, node := range pendingNodes {
		if _, emitted := seen[id]; emitted || !inScope(id) {
			continue
		}
		if err := emitPending(node); err != nil {
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
	if opts.QueryNodes && opts.Projection == nil && !opts.WithEmbeddings && !opts.scanDecodesFilter() {
		if tx.readTS.IsZero() {
			return tx.engine.StreamNodesWithOptions(ctx, opts, emit)
		}
		// Reuse the logical MVCC iterator instead of materializing full nodes
		// before the options-driven scan can choose its read contract.
		return tx.withSnapshotViewLocked(func(txn *badger.Txn) error {
			return tx.engine.iterateNodesVisibleAtInScopeInTxn(ctx, txn, tx.labelScanScopeLocked(opts.Prefix), tx.readTS, func(node *Node) error {
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
	nodes, err := tx.getAllCommittedNodesLocked(ctx)
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
	ctx context.Context,
	label string,
	seen map[NodeID]struct{},
	pendingNodes map[NodeID]*Node,
	visit func(*Node) error,
) error {
	if len(pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		return nil
	}
	// The pending overlay is emitted under the read's own context, so a
	// canceled statement stops here instead of projecting the rest.
	var cancel bulkReadCancellation
	for id, node := range pendingNodes {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		if _, emitted := seen[id]; emitted {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted || node == nil || !slices.Contains(node.Labels, label) {
			continue
		}
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// GetFirstNodeByLabel returns the first visible node with the given label.
func (tx *BadgerTransaction) GetFirstNodeByLabel(ctx context.Context, label string) (*Node, error) {
	// Label discovery retains this statement's own admission context.
	nodes, err := tx.GetNodesByLabel(ctx, label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	return nodes[0], nil
}

// AllNodes returns all visible nodes including pending transaction writes.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first.
func (tx *BadgerTransaction) AllNodes(ctx context.Context) ([]*Node, error) {
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

	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	committed, err := tx.getAllCommittedNodesLocked(ctx)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingNodesLocked(ctx, committed, func(node *Node) bool {
		return node != nil
	})
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
//
// ctx is the read's own context, threaded from the route that asked for the
// merged population. Both overlay loops observe its cancellation, so a
// canceled read retires with the caller's error and no merged slice: nothing
// partial escapes the kernel and no caller caches a truncated overlay.
func mergePendingLocked[ID comparable, T any](
	ctx context.Context,
	committed []T,
	pending map[ID]T,
	deleted map[ID]struct{},
	includePending func(T) bool,
	idOf func(T) ID,
	copyOf func(T) T,
) ([]T, error) {
	if len(pending) == 0 && len(deleted) == 0 {
		return committed, nil
	}
	merged := make([]T, 0, util.SafePreallocSum(len(committed), len(pending)))
	seen := make(map[ID]struct{}, util.SafePreallocSum(len(committed), len(pending)))
	var zero ID
	var cancel bulkReadCancellation
	for _, record := range committed {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
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
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		if includePending(record) {
			merged = append(merged, copyOf(record))
		}
	}

	return merged, nil
}

func (tx *BadgerTransaction) mergePendingNodesLocked(ctx context.Context, committed []*Node, includePending func(*Node) bool) ([]*Node, error) {
	pending, err := writeOwnedReadImagesLocked(ctx, tx.pendingNodes, tx.deletedNodes, &tx.entityLocks, nodeWriteLockKind, tx.engine.GetStoredNode)
	if err != nil {
		return nil, err
	}
	now := DecayScoringTime()
	visible := func(image *Node) bool {
		return !tx.engine.filterEntityByDecay(visibleRead, image, now) && includePending(image)
	}
	return mergePendingLocked(ctx, committed, pending, tx.deletedNodes, visible, func(node *Node) NodeID {
		if node == nil {
			return ""
		}
		return node.ID
	}, copyNode)
}

func (tx *BadgerTransaction) mergePendingEdgesLocked(ctx context.Context, committed []*Edge, includePending func(*Edge) bool) ([]*Edge, error) {
	pending, err := writeOwnedReadImagesLocked(ctx, tx.pendingEdges, tx.deletedEdges, &tx.entityLocks, edgeWriteLockKind, tx.engine.GetStoredEdge)
	if err != nil {
		return nil, err
	}
	now := DecayScoringTime()
	visible := func(image *Edge) bool {
		return !tx.engine.filterEntityByDecay(visibleRead, image, now) && includePending(image)
	}
	return mergePendingLocked(ctx, committed, pending, tx.deletedEdges, visible, func(edge *Edge) EdgeID {
		if edge == nil {
			return ""
		}
		return edge.ID
	}, copyEdge)
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
		bytes += estimateAffectedFactsBytes(op.DeletedEdgeFacts)
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

// getCommittedNodeLocked reads a committed node for the transaction's own
// work: a write's target, an endpoint, a constraint check, count
// bookkeeping. It reads what storage holds, so a node the visibility layer
// hides is found (storedRead). GetNode and GetQueryNode read the same way
// through the visibility layer (getCommittedNodeReadLocked).
func (tx *BadgerTransaction) getCommittedNodeLocked(ctx context.Context, nodeID NodeID) (*Node, error) {
	return tx.getCommittedNodeReadLocked(ctx, nodeID, true, storedRead)
}

// getCommittedNodeReadLocked reads a committed node through the visibility
// layer when v applies it. With vectors false it reads no separately stored
// embedding vector (GetQueryNode); vectors already held in memory are
// detached before delivery to a mutable caller.
func (tx *BadgerTransaction) getCommittedNodeReadLocked(ctx context.Context, nodeID NodeID, vectors bool, v readVisibility) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Write-behind overlay: buffered-but-unflushed writes are committed and
	// visible to every later transaction, ahead of any snapshot read. A node
	// whose write lock the transaction took after a later commit is read as
	// last committed too, and so is the other end of a relationship a locked
	// node's last committed read returned (readsNodeLastCommittedLocked).
	if tx.engine != nil && (tx.engine.writeBehind != nil || tx.readsNodeLastCommittedLocked(nodeID)) {
		if vectors {
			return tx.engine.getNode(ctx, nodeID, v)
		}
		return tx.engine.readNodeMetadata(ctx, nodeID, v, nodeItemQueryMetadata)
	}
	if node, ok := tx.snapshotPrefixNodeByID[nodeID]; ok {
		return copyNode(node), nil
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
			return copyNode(node), nil
		}
	}
	if tx.readTS.IsZero() {
		return tx.getNodeFromBadgerSnapshotLocked(nodeID)
	}
	node, err := tx.readCommittedNodeAtSnapshotLocked(ctx, nodeID, vectors, v)
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
			if fresh, freshErr := tx.engine.getNode(ctx, nodeID, v); freshErr == nil {
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
			if fresh, freshErr := tx.engine.getNode(ctx, nodeID, v); freshErr == nil {
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
	if node != nil && err == nil {
		_, previouslyRead := tx.snapshotNodeLabels[nodeID]
		if len(node.Labels) > 0 {
			tx.cacheCommittedNodeLabelsLocked(node)
		}
		// Point reads already decoded a complete image. Reuse the bounded
		// snapshot owner used by prefix reads instead of decoding each traversal
		// endpoint again. Admit after a previous read so one-off point reads
		// keep their existing allocation cost. Admission excludes sidecars,
		// vectors and large images; partial projections never enter here.
		// Live visibility keeps
		// its ordinary read path rather than inheriting a cached answer.
		if previouslyRead && !tx.engine.visibilityFiltered() {
			cacheErr := tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
				tx.cacheSnapshotPrefixNodeByIDLocked(snapshot, node)
				return nil
			})
			if cacheErr != nil {
				return nil, cacheErr
			}
			if retained, ok := tx.snapshotPrefixNodeByID[nodeID]; ok {
				return copyNode(retained), nil
			}
		}
	}
	return node, err
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
	var node *Node
	var decodeErr error
	err := tx.withSnapshotViewLocked(func(rtxn *badger.Txn) error {
		item, err := rtxn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			node, decodeErr = tx.engine.decodeNodeProjected(rtxn, namespaceForNodeID(nodeID), val, nil)
			return nil
		})
	})
	switch {
	case decodeErr != nil:
		return nil, decodeErr
	case err == badger.ErrKeyNotFound:
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("reading node: %w", err)
	}
	return node, nil
}

// getCommittedEdgeLocked reads a committed relationship for the
// transaction's own work: a write's target, a constraint check, count
// bookkeeping. It reads what storage holds, so a relationship the visibility
// layer hides is found (storedRead). GetEdge reads the same way through the
// visibility layer (getCommittedEdgeReadLocked).
func (tx *BadgerTransaction) getCommittedEdgeLocked(ctx context.Context, edgeID EdgeID) (*Edge, error) {
	return tx.getCommittedEdgeReadLocked(ctx, edgeID, storedRead)
}

// getCommittedEdgeReadLocked reads a committed relationship through the
// visibility layer when v applies it.
func (tx *BadgerTransaction) getCommittedEdgeReadLocked(ctx context.Context, edgeID EdgeID, v readVisibility) (*Edge, error) {
	// Write-behind overlay: buffered-but-unflushed writes are committed and
	// visible to every later transaction, ahead of any snapshot read; so is
	// a relationship read as last committed under its write lock, or returned
	// by a locked node's last committed relationship read
	// (readsEdgeLastCommittedLocked).
	if tx.engine != nil && (tx.engine.writeBehind != nil || tx.readsEdgeLastCommittedLocked(edgeID)) {
		return tx.engine.getEdge(ctx, edgeID, v)
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
		return tx.engine.decodeEdgeBodyWithID(tx.badgerTx, namespaceForEdgeID(edgeID), edgeBytes, edgeID)
	}
	edge, err := readEntityAtWithView(ctx, tx.engine, edgeID, tx.readTS, mvccEntityRead{visibility: v}, tx.withSnapshotViewLocked, (*BadgerEngine).getEdgeAtInTxn)
	if err == ErrNotVisibleAtSnapshot {
		// SI: edge head exists but isn't visible at our snapshot.
		// Surface as ErrNotFound so callers can't observe peer
		// commits that landed after our begin.
		if tx.engine.writeBehind != nil {
			// Write-behind: the edge was committed at buffer-append; its
			// head appears only when the flusher lands it, so a snapshot
			// pinned before the flush can miss a committed edge. A fresh
			// latest-committed read covers the flush window.
			if fresh, freshErr := tx.engine.getEdge(ctx, edgeID, v); freshErr == nil {
				return fresh, nil
			}
		}
		return nil, ErrNotFound
	}
	if err == ErrNotFound && tx.engine.writeBehind != nil {
		// The pinned snapshot may predate the flush of an edge committed
		// through the buffer (head and record both absent at pin time).
		if fresh, freshErr := tx.engine.getEdge(ctx, edgeID, v); freshErr == nil {
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
func (tx *BadgerTransaction) getCommittedEdgeForUpdateLocked(ctx context.Context, edgeID EdgeID) (*Edge, error) {
	// Write-behind overlay: a buffered-but-unflushed edge is committed and
	// read-committed-visible, exactly as getCommittedEdgeLocked reports it.
	// Updating it is therefore not a write-write conflict — it is the same
	// latest-committed state the update read from (monster retest part 1,
	// "SET on a buffered relationship fails with a conflict").
	if tx.engine != nil && (tx.engine.writeBehind != nil || tx.readsLatestLocked(bufferedEdgeEntity, string(edgeID))) {
		return tx.engine.getEdge(ctx, edgeID, storedRead)
	}
	if tx.readTS.IsZero() {
		return tx.getCommittedEdgeLocked(ctx, edgeID)
	}
	edge, err := readEntityAtWithView(ctx, tx.engine, edgeID, tx.readTS, mvccEntityRead{visibility: storedRead}, tx.withSnapshotViewLocked, (*BadgerEngine).getEdgeAtInTxn)
	// A peer-created edge has no head at the pinned physical snapshot.
	// Classify it using latest state just like a logically newer edge; never
	// replace an existing snapshot body with that latest body.
	if !errors.Is(err, ErrNotVisibleAtSnapshot) && !errors.Is(err, ErrNotFound) {
		return edge, err
	}
	_, _, latestErr := tx.engine.readLatestEntity(ctx, string(edgeID), true, storedRead)
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

func (tx *BadgerTransaction) getNodesByLabelLocked(ctx context.Context, label string) ([]*Node, error) {
	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the flusher may land a
		// buffered commit after the pinned snapshot, so read the latest
		// (overlaid) view instead.
		return tx.engine.GetNodesByLabelInScope(ctx, tx.labelScanScopeLocked(), label)
	}
	return tx.engine.getNodesByLabelVisibleAtSnapshotWithView(ctx, tx.labelScanScopeLocked(), label, tx.readTS, tx.withSnapshotViewLocked)
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
//
// ctx is the lifetime of that enumeration, like validateNodeConstraints': a
// commit passes the commit preparation context, so a terminated statement stops
// inside the canonical bulk reader instead of after materializing every node of
// the label; a staging write passes its own context, which is the staging
// write's lifetime.
func (tx *BadgerTransaction) committedConstraintNodesLocked(ctx context.Context, label string) ([]*Node, error) {
	nodes, err := tx.engine.GetStoredNodesByLabel(ctx, label)
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
// node gives up in the transaction is free for another (#907). A lock-only
// read refresh does not supersede a committed claim: it stages no replacement
// for the constraint owner to check. Snapshot read consumers separately hide
// their stale row when the write-owned read overlay supplies its latest image.
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

// visibleNodeLabelsLocked reports whether a node exists for the transaction
// (pending, or committed and visible at its snapshot, and not deleted by it).
// Stored endpoints remain valid when user-visible decay hides them. Missing
// endpoints are distinct from failed or canceled storage reads.
func (tx *BadgerTransaction) visibleNodeLabelsLocked(ctx context.Context, nodeID NodeID) ([]string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, false, nil
	}
	if node, exists := tx.pendingNodes[nodeID]; exists {
		return node.Labels, true, nil
	}
	labels, err := tx.committedNodeLabelsLocked(ctx, nodeID)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return labels, true, nil
}

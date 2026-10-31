// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Edge Operations
// ============================================================================

// CreateEdge creates a new edge between two nodes.
func (b *BadgerEngine) CreateEdge(edge *Edge) (err error) {
	return b.createEdgeWithWriteContext(edge, nil)
}

func (b *BadgerEngine) createEdgeWithWriteContext(edge *Edge, inherited *entityWriteContext, origins ...uint64) (err error) {
	edge, origins = takeEdgeArgument(edge, origins)
	if edge != nil {
		if handled, err := b.inMutationTransaction(string(edge.ID), false, func(tx *BadgerTransaction) error {
			return tx.CreateEdge(edge)
		}, origins...); handled {
			return err
		}
	}
	rerouteOrigin := origins
	// Register routing before direct admission so every direct lock and barrier
	// releases before the transaction owner classifies a physical-ID conflict.
	defer func() {
		if errors.Is(err, ErrAlreadyExists) && edge != nil && edge.ID != "" {
			err = b.inBulkTransaction(namespaceForEdgeID(edge.ID), func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) }, rerouteOrigin...)
		}
	}()
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	var entityID string
	var endpoints []NodeID
	if edge != nil {
		entityID = string(edge.ID)
		endpoints = []NodeID{edge.StartNode, edge.EndNode}
	}
	write, releaseWrite, barrierErr := b.beginEntityWrite(context.Background(), entityID, true, inherited, (entityWritePlan{known: writtenEntities{nodes: endpoints}}).withVectorWorkOrigin(origins))
	if barrierErr != nil {
		return barrierErr
	}
	rerouteOrigin = []uint64{write.vectorWorkOrigin}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit, write.vectorWorkOrigin)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(false); err != nil {
		return err
	}
	schema := b.GetSchemaForNamespace(namespaceForEdgeID(edge.ID))
	releaseConstraint, constraintErr := schema.acquireUniqueConstraintCommitLocks(write.ctx, write.owner, schema.appendConstraintValueKeys(nil, nil, []*Edge{edge}))
	if constraintErr != nil {
		return constraintErr
	}
	defer releaseConstraint()

	if edge.CreatedAt.IsZero() {
		edge.CreatedAt = time.Now()
	}
	if edge.UpdatedAt.IsZero() {
		edge.UpdatedAt = edge.CreatedAt
	}

	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), before it reads whether the relationship
	// and its endpoints exist, so it sees them and their replay cannot undo it.
	var order orderHold
	if b.writeBehind != nil {
		_, orderErr := b.orderAfterBufferedCommits(write.ctx, &order, writtenEntities{edges: []EdgeID{edge.ID}, nodes: []NodeID{edge.StartNode, edge.EndNode}})
		if orderErr != nil {
			return orderErr
		}
		defer order.release()
	}
	// PERFORMANCE OPTIMIZATION: Use WriteBatch to batch all writes (edge + indexes)
	// This reduces write amplification from 4 separate writes to 1 batch operation.
	// We still validate existence via a read transaction first.
	var exists bool
	err = b.db.View(func(txn *badger.Txn) error {
		// Check if edge already exists
		key := edgeKey(edge.ID)
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Verify start node exists
		_, err = txn.Get(nodeKey(edge.StartNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Verify end node exists
		_, err = txn.Get(nodeKey(edge.EndNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}

	var image *Edge
	var publicationOps []Operation

	// Capture before physical assembly can acquire the commit gate.
	constraints := capturePublicationConstraintView(schema)
	publish := func(images *commitCacheImages) {
		b.publishCommitOperations(namespaceForEdgeID(edge.ID), schema, publicationOps, true, true, images)
	}
	keys := func() []any {
		publicationOps = []Operation{{Type: OpCreateEdge, Edge: image}}
		capturePublicationOperations(notification, publicationOps)
		return appendOperationPublicationKeys(nil, &constraints, publicationOps)
	}
	err = b.withPublishingUpdate(write.ctx, func(txn *badger.Txn) error {
		// Validate relationship constraints before writing
		dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
		schema := b.GetSchemaForNamespace(dbName)
		if schema != nil {
			if err := b.validateEdgeConstraintsInTxn(txn, edge, schema, dbName, ""); err != nil {
				return err
			}
		}

		version, err := b.allocateMVCCVersion(txn, dbName, time.Now())
		if err != nil {
			return err
		}
		data, err := b.encodeEdgeInTxn(txn, dbName, edge)
		if err != nil {
			return localizedError(localization.StorageClientEdgeEncodeFailed(err), err)
		}
		if err := txn.Set(edgeKey(edge.ID), data); err != nil {
			return err
		}
		if image, err = b.writtenEdgeImage(data, edge); err != nil {
			return err
		}
		outKey, err := b.outgoingIndexKeyString(txn, edge.StartNode, edge.ID)
		if err != nil {
			return err
		}
		header := adjacencyValueFromRecord(data)
		if err := txn.Set(outKey, header); err != nil {
			return err
		}
		inKey, err := b.incomingIndexKeyString(txn, edge.EndNode, edge.ID)
		if err != nil {
			return err
		}
		if err := txn.Set(inKey, header); err != nil {
			return err
		}
		typeKey, err := b.edgeTypeIndexKeyString(txn, edge.Type, edge.ID)
		if err != nil {
			return err
		}
		if err := txn.Set(typeKey, []byte{}); err != nil {
			return err
		}
		// Per-type derived counter (issue #638): same commit as the edge
		// write, so the count can never diverge from the index entry.
		if err := b.adjustEdgeTypeCountInTxn(txn, dbName, edge.Type, 1); err != nil {
			return err
		}
		// Positional (label, type) counters (issue #638, one-labeled shapes).
		startLabels, err := b.readNodeLabelsIfPresentInTxn(txn, edge.StartNode)
		if err != nil {
			return err
		}
		endLabels, err := b.readNodeLabelsIfPresentInTxn(txn, edge.EndNode)
		if err != nil {
			return err
		}
		if err := b.adjustEdgeTypeLabelCountsForEdgeInTxn(txn, dbName, edge.Type, startLabels, endLabels, 1); err != nil {
			return err
		}
		if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
			return err
		}
		if err := b.writeEdgeAdjacencyDeltaInTxn(txn, nil, edge, version); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(edge.ID), &IndexEntryCatalog{
			TargetID:    string(edge.ID),
			TargetScope: "EDGE",
			IndexKeys:   b.collectEdgeIndexKeys(edge.ID, edge.StartNode, edge.EndNode, edge.Type),
		}); err != nil {
			return err
		}
		// Create-only: primary key IS the current head body.
		return b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false)
	}, publish, keys, notification, write.stagePublication)

	return err
}

// readEdgeBodyInTxn reads and decodes the current edge body for id from txn.
// This is the shared direct-read core of GetEdge and getEdgeVisibleAtInTxn:
// key lookup, decode and the visibility layer (when v applies it) are one
// implementation so the read paths cannot drift apart.
//
// A missing key is reported as (nil, false, nil) so MVCC callers can fall
// through to their record path; decoding failures, an absent decoded edge and
// hidden edges are reported as errors (ErrNotFound where the caller should
// observe a not-found edge).
func (b *BadgerEngine) readEdgeBodyInTxn(txn *badger.Txn, id EdgeID, v readVisibility) (*Edge, bool, error) {
	edge, found, err := b.readStoredEdgeImage(embeddingRecordReader{txn: txn}, bufferedEntityState{}, id)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if edge == nil {
		return nil, false, ErrNotFound
	}
	if b.filterEntityByDecay(v, edge, DecayScoringTime()) {
		return nil, false, ErrNotFound
	}
	return edge, true, nil
}

// GetEdge retrieves an edge by ID.
func (b *BadgerEngine) GetEdge(id EdgeID) (*Edge, error) {
	return b.getEdge(id, visibleRead)
}

// GetStoredEdge is GetEdge for integrity work: it also returns a relationship
// the visibility layer hides (StoredEntityReader).
func (b *BadgerEngine) GetStoredEdge(id EdgeID) (*Edge, error) {
	return b.getEdge(id, storedRead)
}

func (b *BadgerEngine) getEdge(id EdgeID, v readVisibility) (*Edge, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurGet)
	if id == "" {
		return nil, ErrInvalidID
	}

	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}

	var state bufferedEntityState
	var edge *Edge
	cached := func(w *WriteBehindBuffer) (bool, error) {
		edge = nil
		if w != nil {
			state := w.entityStateLocked(bufferedEdgeEntity, string(id), bufferedReadVisibility{engine: b, visibility: v})
			if state.found {
				if state.deleted {
					return true, ErrNotFound
				}
				var err error
				edge, _, err = b.readStoredEdgeImage(embeddingRecordReader{}, state, id)
				return true, err
			}
		}
		// Edge point reads retain the authoritative physical-body kernel.
		// The adjacency body cache cannot establish record presence.
		return false, nil
	}
	err := b.withReadView(func(w *WriteBehindBuffer) {
		state = w.entityStateLocked(bufferedEdgeEntity, string(id), bufferedReadVisibility{engine: b, visibility: v})
	}, func(txn *badger.Txn) error {
		if state.found {
			if state.deleted {
				return ErrNotFound
			}
			var err error
			edge, _, err = b.readStoredEdgeImage(embeddingRecordReader{}, state, id)
			return err
		}
		var ok bool
		var readErr error
		edge, ok, readErr = b.readEdgeBodyInTxn(txn, id, v)
		if !ok && readErr == nil {
			return ErrNotFound
		}
		return readErr
	}, readViewOptions{scope: namespaceForEdgeID(id), cached: cached})

	return edge, err
}

// UpdateEdge updates an existing edge.
func (b *BadgerEngine) UpdateEdge(updateRequest EdgeUpdateRequest) error {
	return b.updateEdgeWithOrigin(updateRequest)
}

func (b *BadgerEngine) updateEdgeWithOrigin(updateRequest EdgeUpdateRequest, origins ...uint64) error {
	edge, origins := takeEdgeArgument(updateRequest.After, origins)
	updateRequest.After = edge
	if edge != nil {
		if handled, err := b.inMutationTransaction(string(edge.ID), updateRequest.Before != nil, func(tx *BadgerTransaction) error {
			return tx.UpdateEdge(updateRequest)
		}, origins...); handled {
			return err
		}
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	var entityID string
	var endpoints []NodeID
	if edge != nil {
		entityID = string(edge.ID)
		endpoints = []NodeID{edge.StartNode, edge.EndNode}
	}
	write, releaseWrite, barrierErr := b.beginEntityWrite(context.Background(), entityID, true, nil, (entityWritePlan{known: writtenEntities{nodes: endpoints}, read: func() (writtenEntities, error) {
		stored, err := b.GetStoredEdge(EdgeID(entityID))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return writtenEntities{}, err
		}
		if stored == nil {
			return writtenEntities{}, nil
		}
		return writtenEntities{nodes: []NodeID{stored.StartNode, stored.EndNode}}, nil
	}}).withVectorWorkOrigin(origins))
	if barrierErr != nil {
		return barrierErr
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit, write.vectorWorkOrigin)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(false); err != nil {
		return err
	}
	schema := b.GetSchemaForNamespace(namespaceForEdgeID(edge.ID))
	releaseConstraint, constraintErr := schema.acquireUniqueConstraintCommitLocks(write.ctx, write.owner, schema.appendConstraintValueKeys(nil, nil, []*Edge{edge}))
	if constraintErr != nil {
		return constraintErr
	}
	defer releaseConstraint()

	storedBefore, lookupErr := b.GetStoredEdge(edge.ID)
	if lookupErr == nil && storedBefore != nil {
		if err := schema.requireEntityFootprint(write.owner, writtenEntities{nodes: []NodeID{storedBefore.StartNode, storedBefore.EndNode}}); err != nil {
			return err
		}
	} else if lookupErr != nil && lookupErr != ErrNotFound {
		return lookupErr
	}
	var oldEdge *Edge
	var image *Edge
	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), so their replay cannot undo it.
	var order orderHold
	if b.writeBehind != nil {
		// The relationship's former endpoints too: a move rewrites their adjacency.
		if err := b.orderCovering(write.ctx, &order, writtenEntities{edges: []EdgeID{edge.ID}, nodes: []NodeID{edge.StartNode, edge.EndNode}}, b.storedEdgeEndpoints(edge.ID, nil)); err != nil {
			return err
		}
		defer order.release()
	}
	var publicationOps []Operation
	// Capture before physical assembly can acquire the commit gate.
	constraints := capturePublicationConstraintView(schema)
	publish := func(images *commitCacheImages) {
		b.publishCommitOperations(namespaceForEdgeID(edge.ID), schema, publicationOps, true, true, images)
	}
	keys := func() []any {
		publicationOps = []Operation{{Type: OpUpdateEdge, Edge: image, OldEdge: oldEdge}}
		capturePublicationOperations(notification, publicationOps)
		return appendOperationPublicationKeys(nil, &constraints, publicationOps)
	}
	err := b.withPublishingUpdate(write.ctx, func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, namespaceForEdgeID(edge.ID), time.Now())
		if err != nil {
			return err
		}
		key := edgeKey(edge.ID)

		// Get existing edge
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var existing *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = b.decodeEdgeBodyByID(val, edge.ID)
			return decodeErr
		}); err != nil {
			return err
		}

		oldEdge = existing

		// Validate relationship constraints on updated edge
		dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
		schema := b.GetSchemaForNamespace(dbName)
		if schema != nil {
			if err := b.validateEdgeConstraintsInTxn(txn, edge, schema, dbName, edge.ID); err != nil {
				return err
			}
		}

		// If endpoints changed, update indexes
		if existing.StartNode != edge.StartNode || existing.EndNode != edge.EndNode {
			// Verify new endpoints exist
			if _, err := txn.Get(nodeKey(edge.StartNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if _, err := txn.Get(nodeKey(edge.EndNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}

			// Remove old indexes (lookup-only; existed at write time)
			if oldOut := b.outgoingIndexKeyStringLookup(existing.StartNode, edge.ID); oldOut != nil {
				if err := txn.Delete(oldOut); err != nil {
					return err
				}
			}
			if oldIn := b.incomingIndexKeyStringLookup(existing.EndNode, edge.ID); oldIn != nil {
				if err := txn.Delete(oldIn); err != nil {
					return err
				}
			}
			if err := b.deleteEdgeBetweenIndexesInTxn(txn, existing); err != nil {
				return err
			}

			// Add new indexes (allocate/resolve num IDs for new endpoints)
			outKey, err := b.outgoingIndexKeyString(txn, edge.StartNode, edge.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(outKey, []byte{}); err != nil {
				return err
			}
			inKey, err := b.incomingIndexKeyString(txn, edge.EndNode, edge.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(inKey, []byte{}); err != nil {
				return err
			}
			if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
				return err
			}
			if err := b.writeEdgeAdjacencyDeltaInTxn(txn, existing, edge, version); err != nil {
				return err
			}
		}

		// If type changed, update edge type index.
		if existing.Type != edge.Type {
			if existing.Type != "" {
				if oldTypeKey := b.edgeTypeIndexKeyStringLookup(existing.Type, edge.ID); oldTypeKey != nil {
					if err := txn.Delete(oldTypeKey); err != nil {
						return err
					}
				}
				// Per-type derived counter follows the index entry.
				if err := b.adjustEdgeTypeCountInTxn(txn, dbName, existing.Type, -1); err != nil {
					return err
				}
			}
			if existing.StartNode == edge.StartNode && existing.EndNode == edge.EndNode {
				if err := b.deleteEdgeBetweenIndexesInTxn(txn, existing); err != nil {
					return err
				}
			}
			if edge.Type != "" {
				newTypeKey, err := b.edgeTypeIndexKeyString(txn, edge.Type, edge.ID)
				if err != nil {
					return err
				}
				if err := txn.Set(newTypeKey, []byte{}); err != nil {
					return err
				}
				if err := b.adjustEdgeTypeCountInTxn(txn, dbName, edge.Type, 1); err != nil {
					return err
				}
			}
			if existing.StartNode == edge.StartNode && existing.EndNode == edge.EndNode {
				if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
					return err
				}
			}
		}

		// Archive the superseded body BEFORE we overwrite the primary key;
		// edge is the state written at version. No-op when no history or
		// snapshot reader needs it.
		if err := b.archiveEdgeOnUpdateInTxn(txn, edge.ID, edge, version); err != nil {
			return err
		}

		// Store updated edge
		data, err := b.encodeEdgeInTxn(txn, dbName, edge)
		if err != nil {
			return localizedError(localization.StorageClientEdgeEncodeFailed(err), err)
		}
		if image, err = b.writtenEdgeImage(data, edge); err != nil {
			return err
		}

		// Positional (label, type) counters move with type or endpoint changes.
		if existing.Type != edge.Type || existing.StartNode != edge.StartNode || existing.EndNode != edge.EndNode {
			oldStartLabels, err := b.readNodeLabelsIfPresentInTxn(txn, existing.StartNode)
			if err != nil {
				return err
			}
			oldEndLabels, err := b.readNodeLabelsIfPresentInTxn(txn, existing.EndNode)
			if err != nil {
				return err
			}
			if err := b.adjustEdgeTypeLabelCountsForEdgeInTxn(txn, dbName, existing.Type, oldStartLabels, oldEndLabels, -1); err != nil {
				return err
			}
			newStartLabels := oldStartLabels
			newEndLabels := oldEndLabels
			if existing.StartNode != edge.StartNode {
				newStartLabels, err = b.readNodeLabelsIfPresentInTxn(txn, edge.StartNode)
				if err != nil {
					return err
				}
			}
			if existing.EndNode != edge.EndNode {
				newEndLabels, err = b.readNodeLabelsIfPresentInTxn(txn, edge.EndNode)
				if err != nil {
					return err
				}
			}
			if err := b.adjustEdgeTypeLabelCountsForEdgeInTxn(txn, dbName, edge.Type, newStartLabels, newEndLabels, 1); err != nil {
				return err
			}
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}
		// The adjacency entries carry the record's header.
		if err := b.setAdjacencyValuesInTxn(txn, edge, data); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(edge.ID), &IndexEntryCatalog{
			TargetID:    string(edge.ID),
			TargetScope: "EDGE",
			IndexKeys:   b.collectEdgeIndexKeys(edge.ID, edge.StartNode, edge.EndNode, edge.Type),
		}); err != nil {
			return err
		}
		// Primary key now holds the new body; archival above handled history.
		return b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false)
	}, publish, keys, notification, write.stagePublication)

	return err
}

func (b *BadgerEngine) DeleteEdge(id EdgeID, origins ...uint64) error {
	return b.deleteEdgeWithOrigin(id, origins...)
}

func (b *BadgerEngine) deleteEdgeWithOrigin(id EdgeID, origins ...uint64) error {
	if handled, err := b.inMutationTransaction(string(id), false, func(tx *BadgerTransaction) error {
		return tx.DeleteEdge(id)
	}, origins...); handled {
		return err
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurDelete)
	write, releaseWrite, barrierErr := b.beginEntityWrite(context.Background(), string(id), true, nil, (entityWritePlan{read: func() (writtenEntities, error) { return b.nativeDeletionFootprint(string(id), true) }}).withVectorWorkOrigin(origins))
	if barrierErr != nil {
		return barrierErr
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit, write.vectorWorkOrigin)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(false); err != nil {
		return err
	}

	err := b.validateNativeDeletionFootprint(write, string(id), true)
	if err != nil {
		return err
	}

	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), so their replay cannot undo it.
	var order orderHold
	var edge *Edge
	if b.writeBehind != nil {
		// The stored image the order read last, after the order: the newest.
		if err := b.orderCovering(write.ctx, &order, writtenEntities{edges: []EdgeID{id}}, b.storedEdgeEndpoints(id, &edge)); err != nil {
			return err
		}
		defer order.release()
	} else {
		edge, _ = b.GetEdge(id)
	}

	var publicationOps []Operation
	schema := b.GetSchemaForNamespace(namespaceForEdgeID(id))
	// Capture before physical assembly can acquire the commit gate.
	constraints := capturePublicationConstraintView(schema)
	publish := func(images *commitCacheImages) {
		b.publishCommitOperations(namespaceForEdgeID(id), schema, publicationOps, true, true, images)
	}
	keys := func() []any {
		publicationOps = []Operation{{Type: OpDeleteEdge, EdgeID: id, OldEdge: edge}}
		capturePublicationOperations(notification, publicationOps)
		return appendOperationPublicationKeys(nil, &constraints, publicationOps)
	}
	err = b.withPublishingUpdate(write.ctx, func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, namespaceForEdgeID(id), time.Now())
		if err != nil {
			return err
		}
		edgeForAdjacency, err := b.loadEdgeForAdjacencyTombstoneInTxn(txn, id)
		if err != nil && err != ErrNotFound {
			return err
		}
		edge = edgeForAdjacency
		// deleteEdgeInTxn archives the pre-delete body into its version
		// record (no-op when retention is head-only) before removing
		// the primary key.
		if err := b.deleteEdgeInTxn(txn, id); err != nil {
			return err
		}
		// Per-type derived counter (issue #638) follows the index entry
		// removal in the same commit. The edge body read inside this txn
		// supplies the type, so the decrement matches the deleted row.
		if edgeForAdjacency != nil && edgeForAdjacency.Type != "" {
			if err := b.adjustEdgeTypeCountInTxn(txn, namespaceForEdgeID(id), edgeForAdjacency.Type, -1); err != nil {
				return err
			}
			// Positional (label, type) counters follow the endpoints' labels
			// read before the edge body is removed.
			startLabels, err := b.readNodeLabelsIfPresentInTxn(txn, edgeForAdjacency.StartNode)
			if err != nil {
				return err
			}
			endLabels, err := b.readNodeLabelsIfPresentInTxn(txn, edgeForAdjacency.EndNode)
			if err != nil {
				return err
			}
			if err := b.adjustEdgeTypeLabelCountsForEdgeInTxn(txn, namespaceForEdgeID(id), edgeForAdjacency.Type, startLabels, endLabels, -1); err != nil {
				return err
			}
		}
		if err := b.writeEdgeAdjacencyDeltaInTxn(txn, edgeForAdjacency, nil, version); err != nil {
			return err
		}
		// Tombstone marker (tiny, no body) preserved at the deletion
		// version so snapshot reads at that version observe the delete
		// rather than stumbling onto a stale pre-delete body. This is
		// O(1) bytes and independent of retention policy.
		if err := b.writeEdgeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, id, version, true)
	}, publish, keys, notification, write.stagePublication)

	return err
}

// deleteEdgeInTxn is the internal helper for deleting an edge within a transaction.
// Archives the pre-delete edge body into its version record before removing
// the primary key so snapshot reads at prior versions can still resolve it.
func (b *BadgerEngine) deleteEdgeInTxn(txn *badger.Txn, id EdgeID) error {
	key := edgeKey(id)

	// Get edge for index cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyByID(val, id)
		return decodeErr
	}); err != nil {
		return err
	}
	if edge != nil {
		edge.ID = id
	}

	// Archive the old body at the current head's version so snapshot
	// reads at that version still see the edge after the primary key
	// goes away. No-op if no head exists yet.
	head, headErr := b.loadEdgeMVCCHeadInTxn(txn, id)
	if err := archiveAtLiveHead(head, headErr, func(atVersion MVCCVersion) error {
		return b.archiveEdgeBodyInTxn(txn, id, edge, atVersion)
	}); err != nil {
		return err
	}

	// Delete indexes. Lookup-based because the num IDs already exist for
	// any edge that made it into the primary key store.
	if oldOut := b.outgoingIndexKeyStringLookup(edge.StartNode, id); oldOut != nil {
		if err := txn.Delete(oldOut); err != nil {
			return err
		}
	}
	if oldIn := b.incomingIndexKeyStringLookup(edge.EndNode, id); oldIn != nil {
		if err := txn.Delete(oldIn); err != nil {
			return err
		}
	}
	if oldType := b.edgeTypeIndexKeyStringLookup(edge.Type, id); oldType != nil {
		if err := txn.Delete(oldType); err != nil {
			return err
		}
	}
	if err := b.deleteEdgeBetweenIndexesInTxn(txn, edge); err != nil {
		return err
	}

	deleteIndexEntryCatalogInTxn(txn, string(id))

	// Delete edge primary key. Dict cleanup deferred to the prune
	// pipeline — the caller still writes MVCC tombstone + head which
	// reuses the numID.
	return txn.Delete(key)
}

// BulkDeleteNodes deletes nodes, with their relationships, as one
// transaction of any size (#703). IDs that do not exist, or repeat within
// the call, are skipped.
func (b *BadgerEngine) BulkDeleteNodes(ids []NodeID, origins ...uint64) error {
	return b.bulkDeleteNodesWithOrigin(ids, origins...)
}

func (b *BadgerEngine) bulkDeleteNodesWithOrigin(ids []NodeID, origins ...uint64) error {
	return deleteBulkEntities(b, ids, func(tx *BadgerTransaction, id NodeID) error { return tx.DeleteGraphNode(id, CascadeEdges) }, origins...)
}

// BulkDeleteEdges deletes relationships as one transaction of any size
// (#703). IDs that do not exist, or repeat within the call, are skipped.
func (b *BadgerEngine) BulkDeleteEdges(ids []EdgeID, origins ...uint64) error {
	return b.bulkDeleteEdgesWithOrigin(ids, origins...)
}

func (b *BadgerEngine) bulkDeleteEdgesWithOrigin(ids []EdgeID, origins ...uint64) error {
	return deleteBulkEntities(b, ids, func(tx *BadgerTransaction, id EdgeID) error { return tx.DeleteEdge(id) }, origins...)
}

// ============================================================================

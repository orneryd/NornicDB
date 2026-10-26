// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Node Operations
// ============================================================================

// CreateNode creates a new node in persistent storage.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (b *BadgerEngine) CreateNode(node *Node) (NodeID, error) {
	return b.createNodeWithWriteContext(node, nil)
}

func (b *BadgerEngine) createNodeWithWriteContext(node *Node, inherited *entityWriteContext) (NodeID, error) {
	if node != nil {
		if handled, err := b.inCaptureTransaction(string(node.ID), func(tx *BadgerTransaction) error {
			_, err := tx.CreateNode(node)
			return err
		}); handled {
			if err != nil {
				return "", err
			}
			return node.ID, nil
		}
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	var entityID string
	if node != nil {
		entityID = string(node.ID)
	}
	write, releaseWrite, barrierErr := b.beginEntityWrite(entityID, false, inherited)
	if barrierErr != nil {
		return "", barrierErr
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	dbName, err := b.admitCreatedNode(node, nil)
	if err != nil {
		return "", err
	}
	schema := b.GetSchemaForNamespace(dbName)

	// The constraint keys' commit locks cover the check, the write and the
	// publication to the constraint cache, as for a transaction's commit.
	releaseUniqueLocks, lockErr := lockEngineWriteKeys(schema, node, write.owner)
	if lockErr != nil {
		return "", lockErr
	}
	defer releaseUniqueLocks()
	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	var image *Node
	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), so their replay cannot undo it.
	var order orderHold
	if b.writeBehind != nil {
		_, orderErr := b.orderAfterBufferedCommits(&order, writtenEntities{nodes: []NodeID{node.ID}})
		if orderErr != nil {
			return "", orderErr
		}
		defer order.release()
	}
	var publicationOps []Operation
	publish := func() { b.publishCommitOperations(dbName, schema, publicationOps, true, true) }
	keys := func() []any {
		publicationOps = []Operation{{Type: OpCreateNode, Node: image}}
		capturePublicationOperations(notification, publicationOps)
		return appendNodePublicationKeys(nil, schema, node.ID, nil, image)
	}
	err = b.withPublishingUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, namespaceForNodeID(node.ID), time.Now())
		if err != nil {
			return err
		}
		key := nodeKey(node.ID)
		_, getErr := txn.Get(key)
		if getErr == nil {
			return ErrAlreadyExists
		}
		if getErr != badger.ErrKeyNotFound {
			return getErr
		}
		if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
			return err
		}
		data, embeddingsSeparate, err := b.writeCreatedNode(txn, txn, key, node)
		if err != nil {
			return err
		}
		if image, err = b.writtenNodeImage(data, node); err != nil {
			return err
		}
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		}
		if err := b.adjustNodeLabelCountsInTxn(txn, dbName, nil, node.Labels); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(node.ID), &IndexEntryCatalog{
			TargetID:    string(node.ID),
			TargetScope: "NODE",
			IndexKeys:   b.collectNodeIndexKeys(node.ID, node.Labels),
		}); err != nil {
			return localizedError(localization.StorageClientNodeIndexCatalogWriteFailed(err), err)
		}
		if b.shouldIndexPendingEmbed(node) {
			if err := b.setPendingEmbeddingInTxn(txn, node.ID); err != nil {
				return localizedError(localization.StorageClientNodePendingEmbedIndexWriteFailed(err), err)
			}
		}
		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, nil, node); err != nil {
			return err
		}
		// Create-only path: primary key IS the current head body.
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	}, publish, keys, notification, func(cw *commitWriter) error {
		if persistSeparateEmbeddings {
			if err := cw.replaceEmbeddingRange(node.ID, embeddingsToPersist, nil); err != nil {
				return err
			}
		}
		return write.stagePublication(cw)
	})
	if err != nil {
		return "", err
	}

	return node.ID, nil
}

// admitCreatedNode owns the shared node-create input/namespace admission.
// The transactional caller holds tx.mu and already checked write admission;
// its snapshot pin and its later timestamp position remain transactional.
func (b *BadgerEngine) admitCreatedNode(node *Node, tx *BadgerTransaction) (string, error) {
	if node == nil {
		if tx != nil {
			return "", ErrInvalidID
		}
		return "", ErrInvalidData
	}
	if node.ID == "" {
		return "", ErrInvalidID
	}
	if tx == nil {
		if !strings.Contains(string(node.ID), ":") {
			return "", localizedError(localization.StorageClientNodeIDNamespaceUnprefixed(string(node.ID)), nil)
		}
		if err := b.ensureOpen(false); err != nil {
			return "", err
		}
		initializeCreationTimes(&node.CreatedAt, &node.UpdatedAt)
	} else {
		if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
			return "", err
		}
		return tx.namespace, nil
	}
	namespace, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return "", localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
	}
	return namespace, nil
}

// writeCreatedNode owns the encoded body and label writes of a node create.
// Direct writes and transaction staging supply their existing KV owner; their
// admission, counts, sidecar persistence and publication remain in that owner.
func (b *BadgerEngine) writeCreatedNode(codec *badger.Txn, target kvWriter, key []byte, node *Node) ([]byte, bool, error) {
	data, separate, err := b.encodeNodeInTxn(codec, namespaceForNodeID(node.ID), node)
	if err != nil {
		return nil, false, localizedError(localization.StorageClientNodeEncodeFailed(err), err)
	}
	if err := target.Set(key, data); err != nil {
		return nil, false, localizedError(localization.StorageClientNodeWriteFailed(err), err)
	}
	for _, label := range node.Labels {
		key, err := b.labelIndexKeyString(target, label, node.ID)
		if err != nil {
			if _, staged := target.(*stagedKV); staged {
				return nil, false, fmt.Errorf("label index: %w", err)
			}
			return nil, false, localizedError(localization.StorageClientNodeLabelIndexKeyBuildFailed(err), err)
		}
		if err := target.Set(key, []byte{}); err != nil {
			return nil, false, localizedError(localization.StorageClientNodeLabelIndexWriteFailed(err), err)
		}
	}
	return data, separate, nil
}

type nodeReadCacheMode uint8

const (
	nodeReadCacheFull nodeReadCacheMode = iota
	nodeReadCacheQuery
	nodeReadCacheSnapshot
	nodeReadCacheProjection // full-cache reads, never publish a partial decoded body
)

// getNodeByID is the shared cached node read: cache hit (copy via hitCopy),
// miss decode via decode, per-read visibility and optional cache store. GetNode and
// GetNodeWithoutEmbeddings differ only in the decode function, the cache-hit
// copy shape and whether the decoded node is cached.
func (b *BadgerEngine) getNodeByID(id NodeID, v readVisibility, decode func(txn *badger.Txn, item *badger.Item) (*Node, error), hitCopy func(*Node) *Node, cacheMode nodeReadCacheMode) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidID
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurGet)
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}

	gen := b.nodeCacheGen.current()
	var node *Node
	cached := func(w *WriteBehindBuffer) (bool, error) {
		node = nil
		if w != nil {
			state := w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: v})
			if state.found {
				if state.deleted {
					return true, ErrNotFound
				}
				node = state.node
				return true, nil
			}
		}
		if cacheMode == nodeReadCacheSnapshot {
			return false, nil
		}
		b.nodeCacheMu.RLock()
		value, found := b.nodeCache[id]
		if !found && cacheMode == nodeReadCacheQuery {
			value, found = b.queryNodeCache[id]
		}
		if found {
			node = value
		}
		b.nodeCacheMu.RUnlock()
		return found, nil
	}
	// The cached callback resolves the requested buffered key while the owner
	// holds the retirement lock. Only a miss needs a transaction; no overlay
	// for this key remains to capture, and no decode closure is built on a hit.
	view, err := b.captureReadView(nil, readViewOptions{scope: namespaceForNodeID(id), cached: cached})
	loaded := view != nil
	if err == nil && view != nil {
		err = view.readAndClose(func(txn *badger.Txn) error {
			item, err := txn.Get(nodeKey(id))
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			node, err = decode(txn, item)
			if node != nil {
				node.embeddingRevision = EmbeddingBodyRevision{physical: item.Version()}
			}
			return err
		})
	}
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, ErrNotFound
	}
	if !loaded {
		// Captured cache and overlay versions are immutable. Materialize after
		// capture so copying does not extend either read lock or its hot frame.
		node = hitCopy(node)
	}
	node.embeddingOwner, node.embeddingOwnerID = b, id
	if b.filterEntityByDecay(v, node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	if loaded {
		atomic.AddInt64(&b.cacheMisses, 1)
	} else {
		atomic.AddInt64(&b.cacheHits, 1)
	}
	if loaded && cacheMode != nodeReadCacheProjection {
		b.cacheFillNode(gen, node, cacheMode)
	}
	return node, nil
}

// GetNode retrieves a node by ID.
func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
	return b.getNode(id, visibleRead)
}

// GetStoredNode returns the physical body through the same captured read view.
func (b *BadgerEngine) GetStoredNode(id NodeID) (*Node, error) {
	return b.getNode(id, storedRead)
}

func (b *BadgerEngine) getNode(id NodeID, v readVisibility) (*Node, error) {
	return b.getNodeSnapshotForRead(id, false, v)
}

func (b *BadgerEngine) getNodeSnapshot(id NodeID, force bool) (*Node, error) {
	return b.getNodeSnapshotForRead(id, force, visibleRead)
}

func (b *BadgerEngine) getNodeSnapshotForRead(id NodeID, force bool, v readVisibility) (*Node, error) {
	mode := nodeReadCacheFull
	if force {
		mode = nodeReadCacheSnapshot
	}
	return b.getNodeByID(id, v, func(txn *badger.Txn, item *badger.Item) (*Node, error) {
		return b.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings)
	}, copyNode, mode)
}

// GetQueryNode implements QueryNodeReader with GetNode's reads (the
// write-behind overlay, the node cache, the stored body) and its embedding
// metadata, without reading separately stored chunk vectors. In-memory
// vectors are shared with the returned copy, not copied. The vector-free
// node is cached apart from the node cache (queryNodeCache), so full reads
// never see it and still hydrate.
func (b *BadgerEngine) GetQueryNode(id NodeID) (*Node, error) {
	return b.readNodeMetadata(id, visibleRead, nodeItemQueryMetadata)
}

// readNodeMetadata binds hydration, copy ownership and cache publication together.
// Query metadata keeps its own cache and caller-owned view; body-only reads
// strip vectors from overlay/cache hits and never publish partial bodies.
func (b *BadgerEngine) readNodeMetadata(id NodeID, v readVisibility, hydration nodeItemHydration) (*Node, error) {
	copyImage, cache := copyNodeForCaller, nodeReadCacheQuery
	if hydration == nodeItemBodyOnly {
		copyImage, cache = copyNodeWithoutEmbeddings, nodeReadCacheProjection
	}
	return b.getNodeByID(id, v, func(txn *badger.Txn, item *badger.Item) (*Node, error) {
		return b.readNodeItemInTxn(txn, item, id, hydration)
	}, copyImage, cache)
}

// RelationshipEndpointVisible implements RelationshipEndpointChecker with the
// lookups GetNode starts with: the node caches, else whether the node's record
// key exists, without reading or decoding the record. It doesn't answer while
// decay filtering can hide a node at read time, or when the lookup fails.
func (b *BadgerEngine) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	if err := b.pendingNamespaceConflict(namespaceForNodeID(id)); err != nil {
		return false, false
	}

	if id == "" || b.visibilityFiltered() || b.ensureOpen(false) != nil {
		return false, false
	}
	if b.writeBehind != nil {
		var state bufferedEntityState
		err := b.withReadView(func(w *WriteBehindBuffer) {
			state = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}, func(txn *badger.Txn) error {
			if state.found {
				if state.deleted {
					return ErrNotFound
				}
				return nil
			}
			_, err := txn.Get(nodeKey(id))
			return err
		})
		if err == ErrNotFound || err == badger.ErrKeyNotFound {
			return false, true
		}
		return err == nil, err == nil
	}
	b.nodeCacheMu.RLock()
	_, cached := b.nodeCache[id]
	if !cached {
		// GetQueryNode's entries are invalidated with nodeCache's.
		_, cached = b.queryNodeCache[id]
	}
	b.nodeCacheMu.RUnlock()
	if cached {
		return true, true
	}
	err := b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(nodeKey(id))
		return err
	})
	if err == badger.ErrKeyNotFound {
		return false, true
	}
	return err == nil, err == nil
}

// GetNodeProjected retrieves a node while decoding only the requested user
// properties. Metadata fields such as ID, labels, timestamps, and embedding
// metadata are still decoded from the node body. A nil properties slice falls
// back to the full GetNode path; an empty non-nil slice returns no user
// properties.
func (b *BadgerEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	return b.getNodeProjected(id, properties, visibleRead)
}

func (b *BadgerEngine) getNodeProjected(id NodeID, properties []string, v readVisibility) (*Node, error) {
	if properties == nil {
		return b.getNode(id, v)
	}
	include := propertyProjectionSet(properties)
	return b.getNodeByID(id, v, func(txn *badger.Txn, item *badger.Item) (*Node, error) {
		return b.readProjectedNodeItemInTxn(txn, item, id, properties, include)
	}, func(node *Node) *Node { return copyNodeProjectedWithoutEmbeddings(node, properties) }, nodeReadCacheProjection)
}

// GetNodeWithoutEmbeddings retrieves a node without following separately
// stored embedding vectors. User properties and node metadata are preserved.
// The decoded node is not cached, so an embedding-free read can never poison
// the cache for later full reads.
func (b *BadgerEngine) GetNodeWithoutEmbeddings(id NodeID) (*Node, error) {
	return b.readNodeMetadata(id, visibleRead, nodeItemBodyOnly)
}

// UpdateNode updates an existing node or creates it if it doesn't exist (upsert).
func (b *BadgerEngine) UpdateNode(node *Node) error { return b.updateNodeWithWriteContext(node, nil) }

func (b *BadgerEngine) updateNodeWithWriteContext(node *Node, inherited *entityWriteContext) error {
	if node != nil {
		if handled, err := b.inCaptureTransaction(string(node.ID), func(tx *BadgerTransaction) error {
			return ApplyNodeUpdate(tx, node)
		}); handled {
			return err
		}
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	var entityID string
	if node != nil {
		entityID = string(node.ID)
	}
	write, releaseWrite, barrierErr := b.beginEntityWrite(entityID, false, inherited)
	if barrierErr != nil {
		return barrierErr
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if !strings.Contains(string(node.ID), ":") {
		return localizedError(localization.StorageClientNodeIDNamespaceUnprefixed(string(node.ID)), nil)
	}

	if err := b.ensureOpen(false); err != nil {
		return err
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
	}
	schema := b.GetSchemaForNamespace(dbName)

	// The constraint keys' commit locks cover the check, the write and the
	// publication to the constraint cache, as for a transaction's commit.
	releaseUniqueLocks, lockErr := lockEngineWriteKeys(schema, node, write.owner)
	if lockErr != nil {
		return lockErr
	}
	defer releaseUniqueLocks()

	// Track if this is an insert (new node) or update (existing node)
	wasInsert := false
	var existingNode *Node

	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	var written []byte
	var image *Node
	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), so their replay cannot undo it.
	var order orderHold
	if b.writeBehind != nil {
		_, orderErr := b.orderAfterBufferedCommits(&order, writtenEntities{nodes: []NodeID{node.ID}})
		if orderErr != nil {
			return orderErr
		}
		defer order.release()
	}
	var publicationOps []Operation
	publish := func() { b.publishCommitOperations(dbName, schema, publicationOps, true, true) }
	keys := func() []any {
		publicationOps = []Operation{{Type: OpUpdateNode, Node: image, OldNode: existingNode}}
		if wasInsert {
			publicationOps[0].Type = OpCreateNode
		}
		capturePublicationOperations(notification, publicationOps)
		return appendNodePublicationKeys(nil, schema, node.ID, existingNode, image)
	}
	err := b.withPublishingUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, namespaceForNodeID(node.ID), time.Now())
		if err != nil {
			return err
		}
		key := nodeKey(node.ID)

		// Get existing node for label index updates (if exists)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist - do an insert (upsert behavior)
			wasInsert = true
			if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
				return err
			}
			data, embeddingsSeparate, err := b.encodeNodeInTxn(txn, dbName, node)
			if err != nil {
				return localizedError(localization.StorageClientNodeEncodeFailed(err), err)
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
			written = data
			if image, err = b.writtenNodeImage(written, node); err != nil {
				return err
			}

			// Persist separate embeddings in bounded txns after this txn commits.
			if embeddingsSeparate {
				persistSeparateEmbeddings = true
				embeddingsToPersist = node.ChunkEmbeddings
			}
			// Create label indexes
			for _, label := range node.Labels {
				lblKey, err := b.labelIndexKeyString(txn, label, node.ID)
				if err != nil {
					return err
				}
				if err := txn.Set(lblKey, []byte{}); err != nil {
					return err
				}
			}
			if err := b.adjustNodeLabelCountsInTxn(txn, dbName, nil, node.Labels); err != nil {
				return err
			}
			// Add to pending embeddings index if needed (same as CreateNode)
			if b.shouldIndexPendingEmbed(node) {
				if err := b.setPendingEmbeddingInTxn(txn, node.ID); err != nil {
					return err
				}
			}
			// Insert-branch of upsert: primary key IS the current head body.
			return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
		}
		if err != nil {
			return err
		}

		// Node exists - update it.
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existingNode, decodeErr = b.decodeNodeWithEmbeddings(txn, val, node.ID)
			return decodeErr
		}); err != nil {
			return err
		}

		if node.CreatedAt.IsZero() {
			node.CreatedAt = existingNode.CreatedAt
		}
		if node.UpdatedAt.IsZero() {
			node.UpdatedAt = time.Now()
		}
		// The body write is authoritative: nil embeddings clear them and new
		// ones replace them; ones carried over unchanged are dropped when the
		// embedding source changed, and the sidecar cleanup below removes
		// them from storage too (#963).
		dropStaleCarriedEmbeddings(b, existingNode, node)

		if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
			return err
		}

		// Validate policy constraints on label changes.
		if err := b.validatePolicyOnNodeLabelChangeInTxn(txn, node, existingNode, schema, dbName); err != nil {
			return err
		}

		// Remove old label indexes (lookup-only; numID must have existed)
		for _, label := range existingNode.Labels {
			oldLblKey := b.labelIndexKeyStringLookup(label, node.ID)
			if oldLblKey == nil {
				continue
			}
			if err := txn.Delete(oldLblKey); err != nil {
				return err
			}
		}

		// Archive the previous head body into a version record BEFORE we
		// overwrite the primary key so snapshot reads at the old version can
		// still resolve it. node is the state written at version.
		if err := b.archiveNodeOnUpdateInTxn(txn, node.ID, node, version); err != nil {
			return err
		}

		// Serialize and store updated node (may store embeddings separately if too large)
		data, embeddingsSeparate, err := b.encodeNodeInTxn(txn, dbName, node)
		if err != nil {
			return localizedError(localization.StorageClientNodeEncodeFailed(err), err)
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}
		written = data

		// If embeddings are stored separately, write them after commit in bounded txns.
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		} else {
			// Node fits inline - clean up any old separately stored embeddings
			if err := visitEmbeddingKeys(txn, node.ID, txn.Delete); err != nil {
				return localizedError(localization.StorageClientNodeEmbeddingChunkDeleteFailed(err), err)
			}
		}

		// Create new label indexes
		for _, label := range node.Labels {
			lblKey, err := b.labelIndexKeyString(txn, label, node.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(lblKey, []byte{}); err != nil {
				return err
			}
		}
		if err := b.adjustNodeLabelCountsInTxn(txn, dbName, existingNode.Labels, node.Labels); err != nil {
			return err
		}
		// Positional (label, type) counters follow the node's relabel: its
		// incident edges move between label buckets (issue #638).
		added, removed := nodeLabelChangeDeltas(existingNode.Labels, node.Labels)
		if err := b.adjustEdgeTypeLabelCountsForNodeLabelChangeInTxn(txn, dbName, string(node.ID), added, removed); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(node.ID), &IndexEntryCatalog{
			TargetID:    string(node.ID),
			TargetScope: "NODE",
			IndexKeys:   b.collectNodeIndexKeys(node.ID, node.Labels),
		}); err != nil {
			return err
		}

		// Manage pending embeddings index atomically
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			// Node has embedding - remove from pending index
			txn.Delete(pendingEmbedKey(node.ID))
		} else if b.shouldIndexPendingEmbed(node) {
			// Node needs embedding - ensure it's in pending index
			b.setPendingEmbeddingInTxn(txn, node.ID)
		} else {
			// Never embed system database nodes.
			txn.Delete(pendingEmbedKey(node.ID))
		}

		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, existingNode, node); err != nil {
			return err
		}
		// Label changes may rebind the node to a different decay profile (or
		// none at all). The body we just wrote carries node.VisibilitySuppressed
		// from the caller; if labels changed we must rescore against the new
		// label set and rewrite the body so the persisted flag reflects the
		// rebind. Without this, a node previously suppressed under one binding
		// stays suppressed forever after rotating to an unbound label.
		if !labelsEqual(node.Labels, existingNode.Labels) {
			mutation, err := b.evaluateSuppressionInTxn(txn, suppressionEvaluation{staged: true, node: node})
			if err != nil {
				return err
			}
			if mutation.written != nil {
				written = mutation.written
			}
		}
		if image, err = b.writtenNodeImage(written, node); err != nil {
			return err
		}
		// Primary key now holds the new head body; archive of old body
		// into mvccNodeVersionKey(oldHead.Version) happened above.
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	}, publish, keys, notification, func(cw *commitWriter) error {
		if persistSeparateEmbeddings {
			if err := cw.replaceEmbeddingRange(node.ID, embeddingsToPersist, nil); err != nil {
				return err
			}
		}
		return write.stagePublication(cw)
	})

	return err
}

// replaceSeparateEmbeddingChunks replaces a node's separately stored
// embedding chunks with embeddings as one atomic commit of any size: the old
// chunks' deletions come first, then the new chunks, each key its own unit
// (commitEngineWrite repeatable units). Readers see the old chunks or the new ones, never a mix
// (#703).
func (b *BadgerEngine) replaceSeparateEmbeddingChunks(nodeID NodeID, embeddings [][]float32, inherited *entityWriteContext) error {
	write, releaseWrite, err := b.beginEntityWrite(string(nodeID), false, inherited)
	if err != nil {
		return err
	}
	defer releaseWrite()
	return b.commitEngineWrite(func(cw *commitWriter) error {
		if err := cw.replaceEmbeddingRange(nodeID, embeddings, nil); err != nil {
			return err
		}
		return write.stagePublication(cw)
	})
}

// DeleteNode removes a node and all its edges.
func (b *BadgerEngine) DeleteNode(id NodeID) error {
	if handled, err := b.inCaptureTransaction(string(id), func(tx *BadgerTransaction) error {
		return tx.DeleteGraphNode(id, CascadeEdges)
	}); handled {
		return err
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurDelete)
	write, releaseWrite, barrierErr := b.beginEntityWrite(string(id), false, nil, entityWritePlan{read: func() (writtenEntities, error) { return b.nativeDeletionFootprint(string(id), false) }})
	if barrierErr != nil {
		return barrierErr
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	defer func() { releaseWrite(); b.completeCommitNotification(notification) }()
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(false); err != nil {
		return err
	}

	var result nodeDeletionResult

	err := b.validateNativeDeletionFootprint(write, string(id), false)
	if err != nil {
		return err
	}

	// Ordered after the write-behind commits acknowledged before it
	// (orderAfterBufferedCommits), so their replay cannot undo it.
	var order orderHold
	if b.writeBehind != nil {
		if err := b.orderCovering(&order, writtenEntities{deletedNodes: []NodeID{id}}, b.cascadeEndpoints(id)); err != nil {
			return err
		}
		defer order.release()
	}
	var publicationOps []Operation
	schema := b.schemaForNodeID(id)
	publish := func() {
		b.publishCommitOperations(namespaceForNodeID(id), schema, publicationOps, true, true)
	}
	keys := func() []any {
		publicationOps = []Operation{result.operation(id)}
		capturePublicationOperations(notification, publicationOps)
		return appendOperationPublicationKeys(nil, schema, publicationOps)
	}

	err = b.withPublishingUpdate(func(txn *badger.Txn) error {
		var err error
		result, err = b.executeNodeDeletion(txn, id, nil)
		return err
	}, publish, keys, notification, write.stagePublication)

	return err
}

// removeNodeRecordKeys is the durable node-record deletion owner for direct
// and staged writers. Cascades and history use their existing snapshot owners;
// numeric IDs stay until pruning because tombstones and heads still use them.
func (b *BadgerEngine) removeNodeRecordKeys(w kvWriter, node *Node) error {
	for _, label := range node.Labels {
		if key := b.labelIndexKeyStringLookup(label, node.ID); key != nil {
			if err := w.Delete(key); err != nil {
				return err
			}
		}
	}
	if err := w.Delete(pendingEmbedKey(node.ID)); err != nil {
		return err
	}
	return w.Delete(nodeKey(node.ID))
}

// ============================================================================

// lockEngineWriteKeys takes node's constraint key locks for a direct engine
// write, under an owner of its own (a variable so tests can make it fail).
var lockEngineWriteKeys = func(schema *SchemaManager, node *Node, owner string) (func(), error) {
	return schema.acquireUniqueConstraintCommitLocks(context.Background(), owner, schema.appendConstraintValueKeys(nil, []*Node{node}, nil))
}

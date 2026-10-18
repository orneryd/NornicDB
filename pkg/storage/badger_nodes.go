// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
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
	notification, release, barrierErr := b.beginNotifyingWrite()
	if barrierErr != nil {
		return "", barrierErr
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	if node == nil {
		return "", ErrInvalidData
	}
	if node.ID == "" {
		return "", ErrInvalidID
	}
	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if !strings.Contains(string(node.ID), ":") {
		return "", localizedError(localization.StorageClientNodeIDNamespaceUnprefixed(string(node.ID)), nil)
	}

	if err := b.ensureOpen(); err != nil {
		return "", err
	}
	if node.CreatedAt.IsZero() {
		node.CreatedAt = time.Now()
	}
	if node.UpdatedAt.IsZero() {
		node.UpdatedAt = node.CreatedAt
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return "", localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
	}
	schema := b.GetSchemaForNamespace(dbName)

	// The constraint keys' commit locks cover the check, the write and the
	// publication to the constraint cache, as for a transaction's commit.
	releaseUniqueLocks, lockErr := lockEngineWriteKeys(schema, node)
	if lockErr != nil {
		return "", lockErr
	}
	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	writeBody := func(txn *badger.Txn) error {
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
		data, embeddingsSeparate, err := b.encodeNodeInTxn(txn, dbName, node)
		if err != nil {
			return localizedError(localization.StorageClientNodeEncodeFailed(err), err)
		}
		if err := txn.Set(key, data); err != nil {
			return localizedError(localization.StorageClientNodeWriteFailed(err), err)
		}
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		}
		for _, label := range node.Labels {
			lblKey, err := b.labelIndexKeyString(txn, label, node.ID)
			if err != nil {
				return localizedError(localization.StorageClientNodeLabelIndexKeyBuildFailed(err), err)
			}
			if err := txn.Set(lblKey, []byte{}); err != nil {
				return localizedError(localization.StorageClientNodeLabelIndexWriteFailed(err), err)
			}
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
	}
	err := b.commitEngineWrite(func(cw *commitWriter) error {
		if err := cw.writeOnce(writeBody); err != nil {
			return err
		}
		if persistSeparateEmbeddings {
			if err := cw.replaceEmbeddingKeys(node.ID, embeddingsToPersist); err != nil {
				return err
			}
		}
		// The unique-value registry and the caches, in commit order
		// (badger_publication_stage.go).
		cw.publish = func() {
			node.embeddingKeysPresent = persistSeparateEmbeddings
			node.embeddingKeysKnown = true
			b.publishUniqueValues(schema, node.ID, nil, node)
			b.cacheOnNodeCreated(node)
		}
		cw.publishKeys = appendNodePublicationKeys(cw.publishKeys[:0], schema, node.ID, nil, node)
		return nil
	}, notification)
	releaseUniqueLocks()
	if err != nil {
		return "", err
	}

	// Notify listeners (e.g., search service) to index the new node
	notification.ops = []Operation{{Type: OpCreateNode, Node: node}}

	return node.ID, nil
}

// getNodeByID is the shared cached node read: cache hit (copy via hitCopy),
// miss decode via decode, decay filter and optional cache store. GetNode and
// GetNodeWithoutEmbeddings differ only in the decode function, the cache-hit
// copy shape and whether the decoded node is cached.
func (b *BadgerEngine) getNodeByID(id NodeID, decode func(txn *badger.Txn, val []byte) (*Node, error), hitCopy func(*Node) *Node, cacheFill func(gen uint64, node *Node)) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidID
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurGet)
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}

	b.nodeCacheMu.RLock()
	if cached, ok := b.nodeCache[id]; ok {
		b.nodeCacheMu.RUnlock()
		atomic.AddInt64(&b.cacheHits, 1)
		nodeCopy := hitCopy(cached)
		if b.filterNodeByDecay(nodeCopy, DecayScoringTime()) {
			return nil, ErrNotFound
		}
		return nodeCopy, nil
	}
	b.nodeCacheMu.RUnlock()
	atomic.AddInt64(&b.cacheMisses, 1)

	gen := b.nodeCacheGen.current()
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decode(txn, val)
			return decodeErr
		})
	})
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, ErrNotFound
	}
	if b.filterNodeByDecay(node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	if cacheFill != nil {
		cacheFill(gen, node)
	}
	return node, nil
}

// GetNode retrieves a node by ID.
func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
	// Write-behind overlay: acknowledged buffered writes are visible before
	// the background flush lands them in Badger.
	if b.writeBehind != nil {
		if n, found, deleted := b.writeBehind.GetNode(id); found {
			if deleted {
				return nil, ErrNotFound
			}
			return CopyNode(n), nil
		}
	}
	return b.getNodeByID(id,
		func(txn *badger.Txn, val []byte) (*Node, error) {
			return b.decodeNodeWithEmbeddings(txn, val, id)
		},
		copyNode,
		b.cacheFillNode,
	)
}

// GetQueryNode implements QueryNodeReader with GetNode's reads (the
// write-behind overlay, the node cache, the stored body) and its embedding
// metadata, without reading separately stored chunk vectors. In-memory
// vectors are shared with the returned copy, not copied. The vector-free
// node is cached apart from the node cache (queryNodeCache), so full reads
// never see it and still hydrate.
func (b *BadgerEngine) GetQueryNode(id NodeID) (*Node, error) {
	if b.writeBehind != nil {
		if n, found, deleted := b.writeBehind.GetNode(id); found {
			if deleted {
				return nil, ErrNotFound
			}
			return copyNodeForCaller(n), nil
		}
	}
	if node, ok := b.cacheLoadQueryNode(id); ok {
		atomic.AddInt64(&b.cacheHits, 1)
		if b.filterNodeByDecay(node, DecayScoringTime()) {
			return nil, ErrNotFound
		}
		return node, nil
	}
	return b.getNodeByID(id,
		func(txn *badger.Txn, val []byte) (*Node, error) {
			return b.decodeNodeEmbeddingState(txn, val, id, false)
		},
		copyNodeForCaller,
		b.cacheFillQueryNode,
	)
}

// RelationshipEndpointVisible implements RelationshipEndpointChecker with the
// lookups GetNode starts with: the node caches, else whether the node's record
// key exists, without reading or decoding the record. It doesn't answer while
// decay filtering can hide a node at read time, or when the lookup fails.
func (b *BadgerEngine) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	if id == "" || b.visibilityFiltered() || b.ensureOpen() != nil {
		return false, false
	}
	// Write-behind overlay: a buffered-but-unflushed node is committed and
	// therefore a visible traversal endpoint; a buffered delete hides it
	// (PR #1043 read-overlay parity — anonymous-endpoint traversal must see
	// buffered nodes, monster retest part 1).
	if b.writeBehind != nil {
		if b.writeBehind.NodeDeleted(id) {
			return false, true
		}
		if _, ok := b.writeBehind.LookupNode(id); ok {
			return true, true
		}
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
	if id == "" {
		return nil, ErrInvalidID
	}
	if properties == nil {
		return b.GetNode(id)
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurGet)
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}

	include := propertyProjectionSet(properties)
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			namespace := namespaceForNodeID(id)
			decoded, decodeErr := b.decodeNodeProjected(namespace, val, include)
			if decodeErr != nil {
				return decodeErr
			}
			node = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, ErrNotFound
	}
	if b.filterNodeByDecay(node, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return node, nil
}

// GetNodeWithoutEmbeddings retrieves a node without following separately
// stored embedding vectors. User properties and node metadata are preserved.
// The decoded node is not cached, so an embedding-free read can never poison
// the cache for later full reads.
func (b *BadgerEngine) GetNodeWithoutEmbeddings(id NodeID) (*Node, error) {
	return b.getNodeByID(id,
		func(_ *badger.Txn, val []byte) (*Node, error) {
			return b.decodeNode(namespaceForNodeID(id), val)
		},
		copyNodeWithoutEmbeddings,
		nil,
	)
}

// UpdateNode updates an existing node or creates it if it doesn't exist (upsert).
func (b *BadgerEngine) UpdateNode(node *Node) error {
	if node != nil {
		if handled, err := b.inCaptureTransaction(string(node.ID), func(tx *BadgerTransaction) error {
			return upsertNodeInTransaction(tx, node)
		}); handled {
			return err
		}
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	notification, release, barrierErr := b.beginNotifyingWrite()
	if barrierErr != nil {
		return barrierErr
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
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

	if err := b.ensureOpen(); err != nil {
		return err
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
	}
	schema := b.GetSchemaForNamespace(dbName)

	// The constraint keys' commit locks cover the check, the write and the
	// publication to the constraint cache, as for a transaction's commit.
	releaseUniqueLocks, lockErr := lockEngineWriteKeys(schema, node)
	if lockErr != nil {
		return lockErr
	}
	defer releaseUniqueLocks()

	// Track if this is an insert (new node) or update (existing node)
	wasInsert := false
	var existingNode *Node

	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	writeBody := func(txn *badger.Txn) error {
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

			// The commit writer publishes the body and separate chunks together.
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

		// Separate chunks join this body's atomic commit, including large commits.
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		} else if existingNode.embeddingKeysPresent {
			// Node fits inline - clean up any old separately stored embeddings
			if err := deleteEmbeddingKeysInTxn(txn, node.ID); err != nil {
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
			if err := b.rescoreSuppressionAfterLabelChangeInTxn(txn, dbName, node); err != nil {
				return err
			}
		}
		// Primary key now holds the new head body; archive of old body
		// into mvccNodeVersionKey(oldHead.Version) happened above.
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	}
	err := b.commitEngineWrite(func(cw *commitWriter) error {
		if err := cw.writeOnce(writeBody); err != nil {
			return err
		}
		if persistSeparateEmbeddings {
			if err := cw.replaceEmbeddingKeys(node.ID, embeddingsToPersist); err != nil {
				return err
			}
		}
		// The unique-value registry and the caches, in commit order
		// (badger_publication_stage.go).
		cw.publish = func() {
			node.embeddingKeysPresent = persistSeparateEmbeddings
			node.embeddingKeysKnown = true
			if wasInsert {
				b.publishUniqueValues(schema, node.ID, nil, node)
				b.cacheOnNodeCreated(node)
				return
			}
			b.publishUniqueValues(schema, node.ID, existingNode, node)
			b.cacheOnNodeUpdatedWithOldNode(node, existingNode)
		}
		cw.publishKeys = appendNodePublicationKeys(cw.publishKeys[:0], schema, node.ID, existingNode, node)
		return nil
	}, notification)

	if err == nil {
		if existingNode != nil && existingNode.embeddingKeysPresent {
			b.embeddingSidecarWrites.Delete(node.ID)
		}
		if wasInsert {
			// Notify listeners about the new node
			notification.ops = []Operation{{Type: OpCreateNode, Node: node}}
		} else {
			// Notify listeners to re-index the updated node
			notification.ops = []Operation{{Type: OpUpdateNode, Node: node}}
		}
	}

	return err
}

// replaceSeparateEmbeddingChunks replaces a node's separately stored
// embedding chunks with embeddings as one atomic commit of any size: the old
// chunks' deletions come first, then the new chunks, each key its own unit
// (commitWriter). Readers see the old chunks or the new ones, never a mix
// (#703).
func (b *BadgerEngine) replaceSeparateEmbeddingChunks(nodeID NodeID, embeddings [][]float32) error {
	return b.commitEngineWrite(func(cw *commitWriter) error {
		return cw.replaceEmbeddingKeys(nodeID, embeddings)
	})
}

// DeleteNode removes a node and all its edges.
func (b *BadgerEngine) DeleteNode(id NodeID) error {
	if handled, err := b.inCaptureTransaction(string(id), func(tx *BadgerTransaction) error {
		return tx.DeleteNode(id)
	}); handled {
		return err
	}
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurDelete)
	notification, release, barrierErr := b.beginNotifyingWrite()
	if barrierErr != nil {
		return barrierErr
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Track edge deletions for counter update after transaction
	var totalEdgesDeleted int64
	var deletedEdgeIDs []EdgeID
	var deletedEdges []*Edge
	var deletedNode *Node

	// The unique-value registry and the caches, in commit order
	// (badger_publication_stage.go).
	var uniqueSchema *SchemaManager
	if dbName, _, ok := ParseDatabasePrefix(string(id)); ok {
		uniqueSchema = b.GetSchemaForNamespace(dbName)
	}
	publish := func() {
		if deletedNode == nil {
			b.cacheOnNodeDeleted(id, totalEdgesDeleted)
			return
		}
		b.publishUniqueValues(uniqueSchema, deletedNode.ID, deletedNode, nil)
		b.cacheOnNodeDeletedWithNode(id, deletedNode, totalEdgesDeleted)
	}
	keys := func(buf []publicationKey) []publicationKey {
		keys := appendNodePublicationKeys(buf, uniqueSchema, id, deletedNode, nil)
		for _, edgeID := range deletedEdgeIDs {
			keys = append(keys, edgePublicationKey(edgeID))
		}
		return keys
	}
	err := b.withPublishingUpdate(func(txn *badger.Txn) error {
		version, allocErr := b.allocateMVCCVersion(txn, namespaceForNodeID(id), time.Now())
		if allocErr != nil {
			return allocErr
		}
		// deleteNodeInTxn archives the pre-delete node body and each
		// cascade-deleted edge body into their version records BEFORE
		// removing the primary keys — see that function's doc.
		edgesDeleted, edgeIDs, edges, node, err := b.deleteNodeInTxn(txn, id)
		totalEdgesDeleted = edgesDeleted
		deletedEdgeIDs = edgeIDs
		deletedEdges = edges
		deletedNode = node
		if err != nil {
			return err
		}
		if deletedNode == nil {
			return nil
		}
		dbName, _, ok := ParseDatabasePrefix(string(deletedNode.ID))
		if !ok {
			return nil
		}
		schema := b.GetSchemaForNamespace(dbName)
		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, deletedNode, nil); err != nil {
			return err
		}
		if err := b.adjustNodeLabelCountsInTxn(txn, dbName, deletedNode.Labels, nil); err != nil {
			return err
		}
		// Tombstone markers (tiny, no body) preserve delete semantics
		// for snapshot reads at the deletion version.
		if err := b.writeNodeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		if err := b.writeNodeMVCCHeadInTxn(txn, id, version, true); err != nil {
			return err
		}
		for i, edgeID := range deletedEdgeIDs {
			if err := b.writeEdgeAdjacencyDeltaInTxn(txn, deletedEdges[i], nil, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCTombstoneInTxn(txn, edgeID, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, edgeID, version, true); err != nil {
				return err
			}
			// Per-type derived counters follow the cascaded index removal.
			if deletedEdges[i] != nil && deletedEdges[i].Type != "" {
				if err := b.adjustEdgeTypeCountInTxn(txn, namespaceForEdgeID(edgeID), deletedEdges[i].Type, -1); err != nil {
					return err
				}
				// Positional (label, type) counters: the deleted node's labels
				// are known; the peer endpoint's labels are read while its body
				// is still live.
				startLabels := deletedNode.Labels
				endLabels := deletedNode.Labels
				if deletedEdges[i].StartNode == deletedNode.ID && deletedEdges[i].EndNode != deletedNode.ID {
					peer, err := b.readNodeLabelsIfPresentInTxn(txn, deletedEdges[i].EndNode)
					if err != nil {
						return err
					}
					endLabels = peer
				} else if deletedEdges[i].EndNode == deletedNode.ID && deletedEdges[i].StartNode != deletedNode.ID {
					peer, err := b.readNodeLabelsIfPresentInTxn(txn, deletedEdges[i].StartNode)
					if err != nil {
						return err
					}
					startLabels = peer
				}
				if err := b.adjustEdgeTypeLabelCountsForEdgeInTxn(txn, namespaceForEdgeID(edgeID), deletedEdges[i].Type, startLabels, endLabels, -1); err != nil {
					return err
				}
			}
		}
		return nil
	}, publish, keys, notification)

	if err == nil {
		// Notify listeners about deleted edges
		if deletedNode != nil && deletedNode.embeddingKeysPresent {
			b.embeddingSidecarWrites.Delete(id)
		}

		// Notify listeners (e.g., search service) to remove from indexes
		notification.ops = []Operation{{Type: OpDeleteNode, NodeID: id, DeletedEdgeIDs: deletedEdgeIDs}}
	}

	return err
}

// deleteEdgesWithPrefix deletes all edges matching a prefix (helper for DeleteNode).
// deleteEdgesWithPrefix deletes all edges matching the given prefix.
// Returns the count of edges actually deleted for accurate stats tracking.
// IMPORTANT: The returned count MUST be used to decrement edgeCount after txn commits.
func (b *BadgerEngine) deleteEdgesWithPrefix(txn *badger.Txn, prefix []byte) (int64, []EdgeID, []*Edge, error) {
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		edgeIDs = append(edgeIDs, edgeID)
	}

	var deletedCount int64
	var deletedIDs []EdgeID
	var deletedEdges []*Edge
	for _, edgeID := range edgeIDs {
		edgeForAdjacency, err := b.loadEdgeForAdjacencyTombstoneInTxn(txn, edgeID)
		if err != nil && err != ErrNotFound {
			return 0, nil, nil, err
		}
		err = b.deleteEdgeInTxn(txn, edgeID)
		if err == nil {
			deletedCount++
			deletedIDs = append(deletedIDs, edgeID)
			// ErrNotFound from the pre-delete load leaves this nil; the later adjacency-delta write
			// intentionally no-ops because there was no live adjacency membership left to tombstone.
			deletedEdges = append(deletedEdges, edgeForAdjacency)
		} else if err != ErrNotFound {
			return 0, nil, nil, err
		}
	}

	return deletedCount, deletedIDs, deletedEdges, nil
}

// ============================================================================

// lockEngineWriteKeys takes node's constraint key locks for a direct engine
// write, under an owner of its own (a variable so tests can make it fail).
var lockEngineWriteKeys = func(schema *SchemaManager, node *Node) (func(), error) {
	return schema.lockConstraintKeysOf(context.Background(), newEngineWriteLockOwner(), node)
}

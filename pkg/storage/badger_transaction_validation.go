package storage

import (
	"context"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func (tx *BadgerTransaction) validateSnapshotIsolationConflicts(ctx context.Context) error {
	// Metadata handlers can publish a peer commit between the optimistic and
	// authoritative passes. Endpoint memoization belongs to one pass only.
	tx.nodeHeadCache = nil
	// Check the buffer first, then stored heads: an acknowledged peer write
	// drained between these passes is still detected in its persisted head.
	if err := tx.bufferedWriteConflictLocked(ctx); err != nil {
		return err
	}
	// Idempotent hidden-edge CREATEs stage no writes. Re-read them at the
	// commit owner so an explicit transaction cannot acknowledge a no-op
	// against a predecessor another writer changed, revealed or deleted after
	// statement-time classification.
	for edgeID, candidate := range tx.edgeCreateNoOps {
		stored, err := tx.engine.GetStoredEdge(ctx, edgeID)
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
		if err := ctx.Err(); err != nil {
			return err
		}
		switch op.Type {
		case OpCreateNode:
			if !op.FreshID {
				if err := tx.checkNodeCreateConflict(ctx, op.NodeID); err != nil {
					return err
				}
			}
		case OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
			if err := tx.checkNodeWriteConflict(ctx, op.NodeID); err != nil {
				return err
			}
			if op.Type == OpDeleteNode {
				// The relationships a deletion cascades are this commit's write
				// set: each carries a derived per-type count delta, and one a
				// concurrent commit already removed is a conflict on a
				// relationship this commit writes (see
				// checkNodeAdjacencyConflict).
				if err := tx.checkNodeAdjacencyConflict(ctx, op.NodeID, op.DeletedEdgeIDs); err != nil {
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
				if err := tx.checkEdgeWriteConflict(ctx, op.EdgeID, false); err != nil {
					return err
				}
			}
			if err := tx.checkEdgeEndpointConflicts(ctx, op.Edge); err != nil {
				return err
			}
		case OpUpdateEdge:
			if _, deleted := tx.deletedEdges[op.EdgeID]; deleted {
				continue
			}
			if err := tx.checkEdgeWriteConflict(ctx, op.EdgeID, true); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(ctx, op.Edge); err != nil {
				return err
			}
		case OpDeleteEdge:
			if err := tx.checkEdgeWriteConflict(ctx, op.EdgeID, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// validatePreparedBodyConflictsLocked closes the physical conflict boundary
// before an outer owner can durably accept the prepared command. Policy
// suppression rewrites bodies/adjacency without changing logical MVCC heads;
// head validation alone can therefore miss a native SSI conflict. The existing
// entity authority is held here, after any acknowledged-buffer rebase, so these
// bodies cannot change again between this check and accepted publication.
func (tx *BadgerTransaction) validatePreparedBodyConflictsLocked(ctx context.Context, keys []uniqueConstraintLockKey) error {
	return tx.engine.withView(func(view *badger.Txn) error {
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if key.shared {
				// Endpoint existence is validated separately; merely reading
				// one does not put its body in the native writer's SSI set.
				continue
			}
			var body []byte
			switch key.entityKind {
			case 1:
				body = nodeKey(NodeID(key.entityID))
			case 2:
				body = edgeKey(EdgeID(key.entityID))
			default:
				continue
			}
			if (key.entityKind == 1 && tx.readsLatestLocked(bufferedNodeEntity, key.entityID)) || (key.entityKind == 2 && tx.readsLatestLocked(bufferedEdgeEntity, key.entityID)) {
				// Its body was read as last committed under the write lock
				// this transaction has held since.
				continue
			}
			item, err := view.Get(body)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return &CommitFailureError{Cause: err}
			}
			if item.Version() > tx.badgerTx.ReadTs() {
				if key.entityKind == 1 {
					return localizedError(localization.StorageTransactionNodeChanged(key.entityID), ErrConflict)
				}
				return localizedError(localization.StorageTransactionEdgeChanged(key.entityID), ErrConflict)
			}
		}
		return nil
	})
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
// than the transaction's boundary. Timestamps order commits: the write-behind
// flusher backdates its heads to the buffered ACK time, so a head the flusher
// lands after this transaction began can still be older than the boundary.
// Whether such a head was nevertheless published after the cut is the second
// half of the rule, in snapshotHeadConflict (mvccReservations).
func (tx *BadgerTransaction) snapshotIsolationConflict(headVersion MVCCVersion) bool {
	return headVersion.Compare(tx.readTS) > 0
}

func (tx *BadgerTransaction) checkNodeCreateConflict(ctx context.Context, nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(ctx, nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	conflict, err := tx.snapshotHeadConflict(bufferedNodeEntity, string(nodeID), head.Version)
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

func (tx *BadgerTransaction) checkNodeWriteConflict(ctx context.Context, nodeID NodeID) error {
	if tx.readsLatestLocked(bufferedNodeEntity, string(nodeID)) {
		// Written on its latest committed image under its write lock, which no
		// peer has held since: nothing committed can be newer.
		return nil
	}
	head, err := tx.engine.GetNodeCurrentHead(ctx, nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if err := tx.validateMutationPredecessor(ctx, bufferedNodeEntity, string(nodeID), head); err != nil {
		return err
	}
	conflict, err := tx.snapshotHeadConflict(bufferedNodeEntity, string(nodeID), head.Version)
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
		if cv := tx.classifyUpdateConflictAsUniqueViolation(ctx, nodeID); cv != nil {
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
func (tx *BadgerTransaction) classifyUpdateConflictAsUniqueViolation(ctx context.Context, nodeID NodeID) error {
	pending, ok := tx.pendingNodes[nodeID]
	if !ok || pending == nil || tx.namespace == "" {
		return nil
	}
	// Reuse the retained mutation preimage; ordinary reads retain no claims.
	preimages, distinctUpdates, err := mutationPreimagesLocked(ctx, tx.operations, len(tx.pendingNodes), OpUpdateNode, nodeClaimPreimage)
	if err != nil {
		return err
	}
	before := preimages[nodeID]
	if distinctUpdates {
		for index := range tx.operations {
			operation := &tx.operations[index]
			if operation.NodeID == nodeID {
				_, before = nodeClaimPreimage(operation)
				break
			}
		}
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if schema == nil {
		return nil
	}
	// Conflict classification is preparation and uses the current publication owner.
	constraints, err := schema.GetConstraintsForLabels(ctx, pending.Labels)
	if err != nil {
		return err
	}
	for _, c := range constraints {
		if c.EffectiveEntityType() != ConstraintEntityNode {
			continue
		}
		if c.Type != ConstraintUnique || len(c.Properties) != 1 {
			continue
		}
		prop := c.Properties[0]
		val, has := pending.Properties[prop]
		if !has {
			continue
		}
		same, err := sameNodeUniqueClaim(ctx, before, pending, c.Label, prop)
		if err != nil {
			return err
		}
		if same {
			continue
		}
		// Snapshot continuations explicitly supply their detached native lifetime.
		existing, found, constrained, complete, err := schema.LookupUniqueConstraintValue(ctx, c.Label, prop, val)
		if err != nil {
			return err
		}
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

// validateMutationPredecessor checks presence independently of reserved MVCC
// ordering. A peer can reserve its version before BEGIN and publish its delete
// afterward; comparing that version alone would count the removal twice. A
// frame-created identity has no committed predecessor, so CREATE followed by
// DELETE (including reuse of an old tombstoned ID) remains a valid no-op.
func (tx *BadgerTransaction) validateMutationPredecessor(ctx context.Context, kind bufferedEntityKind, id string, head MVCCHead) error {
	if !head.Tombstoned {
		return nil
	}
	present, err := tx.engine.readStoredPresence(ctx, kind, id)
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if present {
		// A pre-BEGIN acknowledged re-create may still cover this stored
		// tombstone. Presence belongs to the current coherent read owner.
		return nil
	}
operations:
	for index := range tx.operations {
		if err := ctx.Err(); err != nil {
			return err
		}
		op := &tx.operations[index]
		if kind == bufferedNodeEntity && string(op.NodeID) == id {
			if op.Type == OpCreateNode {
				return nil
			}
			break
		}
		if kind == bufferedEdgeEntity {
			if string(op.EdgeID) == id {
				if op.Type == OpCreateEdge {
					return nil
				}
				break
			}
			if op.Type == OpDeleteNode {
				for _, cascaded := range op.DeletedEdgeIDs {
					if err := ctx.Err(); err != nil {
						return err
					}
					if string(cascaded) == id {
						break operations
					}
				}
			}
		}
	}
	if kind == bufferedNodeEntity {
		return localizedError(localization.StorageTransactionNodeChanged(id), ErrConflict)
	}
	return localizedError(localization.StorageTransactionEdgeChanged(id), ErrConflict)
}

func (tx *BadgerTransaction) checkEdgeWriteConflict(ctx context.Context, edgeID EdgeID, requiresPredecessor bool) error {
	if tx.readsLatestLocked(bufferedEdgeEntity, string(edgeID)) {
		return nil
	}
	head, err := tx.engine.GetEdgeCurrentHead(ctx, edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if requiresPredecessor {
		if err := tx.validateMutationPredecessor(ctx, bufferedEdgeEntity, string(edgeID), head); err != nil {
			return err
		}
	}
	conflict, err := tx.snapshotHeadConflict(bufferedEdgeEntity, string(edgeID), head.Version)
	if err != nil {
		return &CommitFailureError{Cause: err}
	}
	if conflict {
		return localizedError(localization.StorageTransactionEdgeChanged(string(edgeID)), ErrConflict)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeEndpointConflicts(ctx context.Context, edge *Edge) error {
	if edge == nil || !tx.revalidatesEndpoints() {
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
		head, err := tx.endpointHead(ctx, nodeID)
		if err != nil && err != ErrNotFound {
			return &CommitFailureError{Cause: err}
		}
		if err == ErrNotFound || head.Tombstoned {
			present, readErr := tx.engine.readStoredPresence(ctx, bufferedNodeEntity, string(nodeID))
			if readErr != nil {
				return &CommitFailureError{Cause: readErr}
			}
			if present {
				continue
			}
			if err == nil {
				conflict, readErr := tx.snapshotHeadConflict(bufferedNodeEntity, string(nodeID), head.Version)
				if readErr != nil {
					return &CommitFailureError{Cause: readErr}
				}
				if conflict {
					return localizedError(localization.StorageTransactionEndpointDeleted(string(nodeID)), ErrConflict)
				}
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
func (tx *BadgerTransaction) endpointHead(ctx context.Context, nodeID NodeID) (MVCCHead, error) {
	if head, ok := tx.nodeHeadCache[nodeID]; ok {
		return head, nil
	}
	head, err := tx.engine.GetNodeCurrentHead(ctx, nodeID)
	if err == nil {
		if tx.nodeHeadCache == nil {
			tx.nodeHeadCache = make(map[NodeID]MVCCHead, 64)
		}
		tx.nodeHeadCache[nodeID] = head
	}
	return head, err
}

// checkNodeAdjacencyConflict validates the relationships adjacent to a node a
// deletion removes: each may not have changed since this transaction read it.
// cascaded is the relationship set the deletion removes (its Op.DeletedEdgeIDs,
// empty for a check that owns no cascade).
//
// The index walk sees only relationships the committed adjacency index still
// holds. A relationship a concurrent commit removed since this transaction read
// it no longer has an entry there, so it is checked by its own head, which that
// commit left as a tombstone newer than this transaction's snapshot. It is a
// write conflict on a relationship this commit writes, exactly the rejection
// Badger's conflict detection raises for the same commit on the synchronous
// path. Accepting it would apply this commit's derived per-type count delta for
// a relationship an earlier commit already deleted and counted, driving the
// persisted counter below the relationships it counts; a restore or read that
// folds the deltas then reports the counter underflow.
func (tx *BadgerTransaction) checkNodeAdjacencyConflict(ctx context.Context, nodeID NodeID, cascaded []EdgeID) error {
	// seen: the cascaded relationships the index walk validated.
	var seen map[EdgeID]struct{}
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
				if cascaded != nil {
					if seen == nil {
						seen = make(map[EdgeID]struct{}, len(cascaded))
					}
					seen[edgeID] = struct{}{}
				}
				head, physicalVersion, err := tx.engine.loadEdgeMVCCHeadByNumRawWithPhysicalVersionInTxn(viewTx, edgeNum)
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					it.Close()
					return err
				}
				// Only a staged removal requires this predecessor. An obsolete
				// index entry without a body is not a relationship we delete.
				if _, removes := tx.deletedEdges[edgeID]; removes {
					if err := tx.validateMutationPredecessor(ctx, bufferedEdgeEntity, string(edgeID), head); err != nil {
						it.Close()
						return err
					}
				}
				conflict, err := tx.snapshotHeadVersionConflict(bufferedEdgeEntity, string(edgeID), head.Version, physicalVersion)
				if err != nil {
					it.Close()
					return err
				}
				if conflict {
					it.Close()
					return localizedError(localization.StorageTransactionAdjacentEdgeChanged(string(nodeID), string(edgeID)), ErrConflict)
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		if !errors.Is(err, ErrConflict) {
			return &CommitFailureError{Cause: err}
		}
		return err
	}
	for _, edgeID := range cascaded {
		if _, indexed := seen[edgeID]; indexed {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tx.checkEdgeWriteConflict(ctx, edgeID, true); err != nil {
			return err
		}
	}
	return nil
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
//
// ctx is the lifetime of the read this validation performs: a staging write
// passes its own (context.Background), a commit passes the commit's caller
// context so a terminated statement stops before anything is written.
func (tx *BadgerTransaction) validateNodeConstraints(ctx context.Context, node *Node) error {
	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}
	return (nodeConstraintValidation{ctx: ctx, schema: tx.engine.GetSchemaForNamespace(tx.namespace), transaction: tx}).validate(node)
}

// checkUniqueConstraint ensures property value is unique across ALL data.
//
// All pending nodes share the transaction's pinned namespace by invariant
// (see pinNamespaceFromIDLocked), so the in-tx scan does not need to
// re-filter by namespace prefix. The committed-data fallback still filters
// because the label index spans all namespaces.
func (tx *BadgerTransaction) checkUniqueConstraint(ctx context.Context, node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return nil // NULL doesn't violate uniqueness
	}

	if err := tx.pinNamespaceFromIDLocked(string(node.ID)); err != nil {
		return err
	}

	if err := tx.forEachPendingCandidateLocked(ctx, node.ID, c.Label, prop, value, func(*Node) error {
		message := localization.StorageValidationNodeUniqueInTransaction(prop, value)
		return newLocalizedConstraintViolation(ConstraintUnique, c.Label, []string{prop}, message, nil)
	}); err != nil {
		return err
	}

	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if existingNode, found, constrained, cacheComplete, err := schema.LookupUniqueConstraintValue(ctx, c.Label, prop, value); err != nil {
		return err
	} else if constrained && cacheComplete {
		if found && existingNode != node.ID && !tx.committedNodeSupersededLocked(existingNode) {
			return uniqueConstraintViolation(c.Label, prop, value, existingNode)
		}
		return nil
	}

	// If the derived unique-value cache is not complete, fall back to
	// the label scan. Normal schema creation/reload marks the cache complete
	// once after rebuilding it from stored nodes, so hot writes avoid this path.
	return tx.scanForUniqueViolation(ctx, tx.namespace, c.Label, prop, value, node.ID)
}

func uniqueConstraintViolation(label, property string, value interface{}, nodeID NodeID) *ConstraintViolationError {
	message := localization.StorageValidationNodeUniqueExisting(property, value, string(nodeID))
	return newLocalizedConstraintViolation(ConstraintUnique, label, []string{property}, message, nil)
}

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
func (tx *BadgerTransaction) forEachPendingCandidateLocked(ctx context.Context, self NodeID, label, property string, value interface{}, visit func(*Node) error) error {
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil {
		return err
	}
	if ok {
		ids, err := tx.pendingValueMatchesLocked(ctx, label, property, valueKey)
		if err != nil {
			return err
		}
		var cancellation bulkReadCancellation
		for _, id := range ids {
			if err := cancellation.check(ctx); err != nil {
				return err
			}
			if id == self {
				continue
			}
			if n := tx.pendingNodes[id]; n != nil && compareValues(n.Properties[property], value) {
				if err := visit(n); err != nil {
					return err
				}
			}
		}
		return ctx.Err()
	}
	var cancellation bulkReadCancellation
	for id, n := range tx.pendingNodes {
		if err := cancellation.check(ctx); err != nil {
			return err
		}
		if id == self || !hasLabel(n.Labels, label) || !compareValues(n.Properties[property], value) {
			continue
		}
		if err := visit(n); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// scanForUniqueViolation performs a full database scan to check for UNIQUE violations
// within a single namespace (database).
// ctx is the lifetime of the committed candidate read (see
// validateNodeConstraints).
func (tx *BadgerTransaction) scanForUniqueViolation(ctx context.Context, namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}

	nodes, err := tx.committedConstraintNodesLocked(ctx, label)
	if err != nil {
		return err
	}
	for i, existingNode := range nodes {
		if i%commitCancelPollEvery == 0 {
			if err := ctx.Err(); err != nil {
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
// ctx is the lifetime of the committed candidate read (see
// validateNodeConstraints).
func (tx *BadgerTransaction) checkNodeKeyConstraint(ctx context.Context, node *Node, c Constraint) error {
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
	if err := tx.forEachPendingCandidateLocked(ctx, node.ID, c.Label, c.Properties[0], values[0], func(n *Node) error {
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
	idx, err := tx.engine.GetSchemaForNamespace(tx.namespace).filledKeyIndex(ctx, c.Label, c.Properties)
	if err != nil {
		return err
	}
	if idx != nil {
		candidates, err := tx.engine.keyConstraintNodes(ctx, idx, c.Label, c.Properties[0], values)
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
	if err := tx.scanForNodeKeyViolation(ctx, tx.namespace, c.Label, c.Properties, values, node.ID); err != nil {
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
func (b *BadgerEngine) committedKeyHolder(ctx context.Context, id NodeID, namespace, label string, properties []string, props map[string]interface{}) (*Node, error) {
	existing, err := b.GetNode(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if existing == nil || !nodeHoldsKey(existing, namespace, label, properties, props) {
		return nil, nil
	}
	return existing, nil
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
// ctx is the lifetime of the committed candidate read (see
// validateNodeConstraints).
func (tx *BadgerTransaction) scanForNodeKeyViolation(ctx context.Context, namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	if hook := nodeKeyScanHook.Load(); hook != nil {
		(*hook)()
	}
	nodes, err := tx.committedConstraintNodesLocked(ctx, label)
	if err != nil {
		return err
	}
	for i, existingNode := range nodes {
		if i%commitCancelPollEvery == 0 {
			if err := ctx.Err(); err != nil {
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
//
// ctx is the lifetime of that committed read: the staging write's own for a
// staged node, the commit's caller context during commit validation.
func (tx *BadgerTransaction) checkTemporalConstraint(ctx context.Context, node *Node, c Constraint) error {
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
	if err := tx.forEachPendingCandidateLocked(ctx, node.ID, c.Label, spec.keyProps[0], node.Properties[spec.keyProps[0]], collides); err != nil {
		return err
	}

	// Committed nodes: while the constraint's history index is complete, only
	// the nodes in the history of this node's key can overlap it (#1126);
	// otherwise scan the label. Both read the latest committed state and skip
	// the nodes this transaction rewrites or deletes, whose pending versions
	// were checked above.
	if tx.engine.temporalIndexAuthoritative(tx.engine.GetSchemaForNamespace(tx.namespace), c) {
		history, ok, err := tx.engine.temporalKeyHistoryNodesLatest(ctx, tx.namespace, c, node)
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

	// Check overlaps against committed storage using the label index scan on
	// the read's lifetime (ctx), like the pending candidate pass above.
	runTemporalConstraintScanHook()
	visibleNodes, err := tx.committedConstraintNodesLocked(ctx, c.Label)
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

func (tx *BadgerTransaction) validateAllConstraints(ctx context.Context) error {
	checked := 0
	for _, node := range tx.pendingNodes {
		if checked%commitCancelPollEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		checked++
		if err := tx.validateNodeConstraints(ctx, node); err != nil {
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
		if err := tx.validatePolicyOnNodeLabelChange(ctx, node, op.OldNode); err != nil {
			return err
		}
	}
	for _, edge := range tx.pendingEdges {
		if err := tx.validateEdgeConstraints(ctx, edge); err != nil {
			return err
		}
	}
	if err := tx.validateConstraintContracts(ctx); err != nil {
		return err
	}
	return nil
}

// validateEdgeConstraints checks relationship constraints for an edge in a transaction.
// ctx is the lifetime of the committed enumerations the checks perform, like
// validateNodeConstraints': a commit passes the commit preparation context and
// a terminated statement stops inside the canonical bulk reader.
func (tx *BadgerTransaction) validateEdgeConstraints(ctx context.Context, edge *Edge) error {
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

	constraints, err := schema.GetConstraintsForLabels(ctx, []string{edge.Type})
	if err != nil {
		return err
	}
	for _, c := range constraints {
		if c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		switch c.Type {
		case ConstraintUnique:
			if err := tx.checkEdgeUniqueness(ctx, edge, c, tx.namespace); err != nil {
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
			if err := tx.checkEdgeUniqueness(ctx, edge, c, tx.namespace); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := tx.checkEdgeTemporalConstraint(ctx, edge, c, tx.namespace); err != nil {
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
			if err := tx.checkEdgeCardinality(ctx, edge, c, tx.namespace); err != nil {
				return err
			}
		}
	}

	// Policy constraints must be evaluated as a set (all ALLOWED policies form a union).
	if err := tx.checkEdgePolicy(ctx, edge, schema, tx.namespace); err != nil {
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
// ctx is the lifetime of the committed edge enumeration (see
// validateEdgeConstraints).
func (tx *BadgerTransaction) checkEdgeUniqueness(ctx context.Context, edge *Edge, c Constraint, namespace string) error {
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

	if candidates, indexed, err := tx.engine.relationshipUniqueCandidates(ctx, edge, c, namespace); err != nil {
		return err
	} else if indexed {
		for _, candidate := range candidates {
			id := EdgeID(candidate)
			if id == edge.ID || tx.committedEdgeSupersededLocked(id) {
				continue
			}
			existing, err := tx.engine.GetStoredEdge(ctx, id)
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

	// Check against committed edges via the engine, on the read's lifetime
	// (ctx).
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}
	existingEdges, err := tx.engine.GetStoredEdgesByType(ctx, edge.Type)
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
// ctx is the lifetime of the committed edge enumeration (see
// validateEdgeConstraints).
func (tx *BadgerTransaction) checkEdgeTemporalConstraint(ctx context.Context, edge *Edge, c Constraint, namespace string) error {
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

	// Check against committed edges (filtered by namespace) on the read's
	// lifetime (ctx).
	existingEdges, err := tx.engine.GetStoredEdgesByType(ctx, edge.Type)
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
// excluding deleted and rewritten edges, and rejects the new edge if adding it would
// exceed MaxCount.
//
// Its input authority is the transaction's staged view: pending edge images
// selected from the pending map, over the engine's committed adjacency of the
// anchor, where a rewritten committed image counts once as its pending version and
// a deleted one does not count at all. Population selection is all this route
// keeps; the counter owns the population predicate, the threshold and the
// violation that the engine's publishing-transaction route uses as well, and this
// route counts the whole population before reporting, so the violation shows the
// real total.
// ctx is the lifetime of the committed adjacency enumeration (see
// validateEdgeConstraints).
func (tx *BadgerTransaction) checkEdgeCardinality(ctx context.Context, edge *Edge, c Constraint, namespace string) error {
	counter := newEdgeCardinalityCounter(c, edge, namespace, edge.ID)

	// Count pending edges in this transaction that match type + anchor + direction.
	for id, pendingEdge := range tx.pendingEdges {
		if counter.anchored(pendingEdge) && counter.admits(id) {
			counter.counts(pendingEdge.Type)
		}
	}

	// Count committed edges from the engine on the read's lifetime (ctx).
	var committedEdges []*Edge
	var err error
	adjacency := AdjacentEdgeRead{Node: counter.anchor}
	if c.Direction != "OUTGOING" {
		adjacency.Direction = AdjacentIncoming
	}
	committedEdges, err = tx.engine.ReadStoredAdjacentEdges(ctx, adjacency)
	if err != nil {
		return err
	}
	for _, existingEdge := range committedEdges {
		// Edges deleted here don't count; rewritten ones are counted as pending.
		if tx.committedEdgeSupersededLocked(existingEdge.ID) {
			continue
		}
		if counter.admits(existingEdge.ID) {
			counter.counts(existingEdge.Type)
		}
	}

	if counter.reached() {
		return counter.violation()
	}

	return nil
}

// checkEdgePolicy validates DISALLOWED and ALLOWED endpoint policies within a transaction.
// Reads node labels from pending nodes first (read-your-writes), then falls back to committed storage.
func (tx *BadgerTransaction) checkEdgePolicy(ctx context.Context, edge *Edge, schema *SchemaManager, namespace string) error {
	constraints, err := schema.GetConstraintsForLabels(ctx, []string{edge.Type})
	if err != nil {
		return err
	}

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
	srcLabels, err := tx.getNodeLabels(ctx, NodeID(edge.StartNode))
	if err != nil {
		return err
	}
	if srcLabels == nil {
		return nil // Node not found; other validation catches this
	}

	// Read target node labels.
	tgtLabels, err := tx.getNodeLabels(ctx, NodeID(edge.EndNode))
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
// ctx is the lifetime of the committed adjacency enumerations: commit
// validation passes the commit preparation context, a staging write its own
// staging lifetime (see validateNodeConstraints).
func (tx *BadgerTransaction) validatePolicyOnNodeLabelChange(ctx context.Context, node *Node, oldNode *Node) error {
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
		adjacency := AdjacentEdgeRead{Node: node.ID}
		if !isOutgoing {
			adjacency.Direction = AdjacentIncoming
		}
		edges, err = tx.engine.ReadStoredAdjacentEdges(ctx, adjacency)
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
			otherLabels, err := tx.getNodeLabels(ctx, otherNodeID)
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
			constraints, err := schema.GetConstraintsForLabels(ctx, []string{edge.Type})
			if err != nil {
				return err
			}
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
func (tx *BadgerTransaction) getNodeLabels(ctx context.Context, nodeID NodeID) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		return pending.Labels, nil
	}
	node, err := tx.getCommittedNodeLocked(ctx, nodeID)
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

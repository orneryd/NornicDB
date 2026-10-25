package storage

// Buffered commit admission, conflict and value-order rules.
// Entity exclusion is owned by buffered_write_order.go.

import (
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// errBufferedValidationStale reports that a buffer generation retired between
// a buffered commit's Badger validation and its reservation, so the Badger
// heads it checked may predate the retired writes: validate again.
var errBufferedValidationStale = errors.New("write-behind generation retired during validation")

// bufferedTargetUse says what a buffered commit's write depends on.
type bufferedTargetUse uint8

const (
	// bufferedTargetWrite: the entity is created under a reused ID, updated
	// or deleted, so any newer commit of it conflicts.
	bufferedTargetWrite bufferedTargetUse = iota
	// bufferedTargetDelete: a node delete; also no newer incident
	// relationship may exist.
	bufferedTargetDelete
	// bufferedTargetEndpoint: a relationship endpoint, which must not have
	// been deleted since the snapshot.
	bufferedTargetEndpoint
)

type bufferedTargetKey struct {
	kind bufferedEntityKind
	id   string
}

// bufferedTarget is one entity a buffered commit's writes depend on.
type bufferedTarget struct {
	bufferedTargetKey
	use bufferedTargetUse
}

// bufferedHold is the in-flight claim on one entity: at most one writer, any
// number of endpoint users. A commit lists each entity once and checks before
// it claims, so every claim it finds belongs to another commit.
type bufferedHold struct {
	writing   bool
	deleting  bool
	endpoints int
}

// bufferedReservation is a buffered commit's admission, from its conflict
// check to its append (or failure): the targets it holds and its in-flight
// version.
type bufferedReservation struct {
	namespace string
	version   MVCCVersion
	targets   []bufferedTarget
}

// bufferedHoldsRetainLimit bounds the hold map kept for reuse once it drains;
// a bulk commit's larger one is released.
const bufferedHoldsRetainLimit = 1024

// bufferedConflict names the newer commit a buffered commit lost to.
type bufferedConflict struct {
	target  bufferedTarget
	version MVCCVersion
	edgeID  EdgeID // the newer incident relationship, for a node delete
}

// bufferedCommitTargets lists the entities the transaction's writes depend on,
// exactly the set validateSnapshotIsolationConflicts checks against Badger.
// ownEdges are relationships the transaction deletes itself.
func (tx *BadgerTransaction) bufferedCommitTargetsLocked() ([]bufferedTarget, map[EdgeID]struct{}) {
	targets := tx.bufferedTargetBuf[:0]
	var byKey map[bufferedTargetKey]int // built once a bulk commit outgrows scanning
	var ownEdges map[EdgeID]struct{}
	find := func(key bufferedTargetKey) (int, bool) {
		if byKey != nil {
			i, ok := byKey[key]
			return i, ok
		}
		for i := range targets {
			if targets[i].bufferedTargetKey == key {
				return i, true
			}
		}
		return 0, false
	}
	add := func(kind bufferedEntityKind, id string, use bufferedTargetUse) {
		if id == "" {
			return
		}
		key := bufferedTargetKey{kind: kind, id: id}
		if i, ok := find(key); ok {
			// A write supersedes an endpoint use; a delete supersedes both.
			if use < bufferedTargetEndpoint && (targets[i].use == bufferedTargetEndpoint || use == bufferedTargetDelete) {
				targets[i].use = use
			}
			return
		}
		targets = append(targets, bufferedTarget{bufferedTargetKey: key, use: use})
		switch {
		case byKey != nil:
			byKey[key] = len(targets) - 1
		case len(targets) > 16:
			byKey = make(map[bufferedTargetKey]int, len(tx.operations))
			for i := range targets {
				byKey[targets[i].bufferedTargetKey] = i
			}
		}
	}
	ownEdge := func(id EdgeID) {
		if ownEdges == nil {
			ownEdges = make(map[EdgeID]struct{})
		}
		ownEdges[id] = struct{}{}
	}
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if !op.FreshID {
				add(bufferedNodeEntity, string(op.NodeID), bufferedTargetWrite)
			}
		case OpUpdateNode, OpUpdateEmbedding:
			add(bufferedNodeEntity, string(op.NodeID), bufferedTargetWrite)
		case OpDeleteNode:
			add(bufferedNodeEntity, string(op.NodeID), bufferedTargetDelete)
			for _, edgeID := range op.DeletedEdgeIDs {
				ownEdge(edgeID)
			}
		case OpCreateEdge, OpUpdateEdge:
			if _, deleted := tx.deletedEdges[op.EdgeID]; deleted {
				continue
			}
			if op.Type == OpUpdateEdge || !op.FreshID {
				add(bufferedEdgeEntity, string(op.EdgeID), bufferedTargetWrite)
			}
			if op.Edge != nil {
				for _, nodeID := range []NodeID{op.Edge.StartNode, op.Edge.EndNode} {
					if pending, exists := tx.pendingNodes[nodeID]; exists && pending != nil {
						continue
					}
					add(bufferedNodeEntity, string(nodeID), bufferedTargetEndpoint)
				}
			}
		case OpDeleteEdge:
			ownEdge(op.EdgeID)
			add(bufferedEdgeEntity, string(op.EdgeID), bufferedTargetWrite)
		}
	}
	return targets, ownEdges
}

// admitBufferedCommit checks a buffered commit's targets against every newer
// buffered state and every other in-flight admission, then claims them and
// allocates the commit's version, all under the buffer lock. epoch is
// retireEpoch before the commit's Badger validation.
func (w *WriteBehindBuffer) admitBufferedCommit(namespace string, newer func(MVCCVersion) bool, epoch uint64, targets []bufferedTarget, ownEdges map[EdgeID]struct{}, allocate func() (MVCCVersion, error)) (bufferedReservation, *bufferedConflict, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.admitMu.Lock()
	defer w.admitMu.Unlock()
	if w.admissionClosed {
		return bufferedReservation{}, nil, errWriteBehindAdmissionClosed
	}
	if w.retireEpoch.Load() != epoch {
		return bufferedReservation{}, nil, errBufferedValidationStale
	}
	for _, target := range targets {
		if conflict := w.bufferedTargetConflictLocked(newer, target, ownEdges); conflict != nil {
			return bufferedReservation{}, conflict, nil
		}
	}
	w.inflightCount.Add(1)
	version, err := allocate()
	if err != nil {
		w.inflightCount.Add(-1)
		return bufferedReservation{}, nil, err
	}
	// Allocation runs under admitMu, so each namespace's list stays in
	// sequence order.
	if w.inflight == nil {
		w.inflight = make(map[string][]MVCCVersion)
	}
	w.inflight[namespace] = append(w.inflight[namespace], version)
	if len(targets) == 0 {
		return bufferedReservation{namespace: namespace, version: version}, nil, nil
	}
	if w.holds == nil {
		w.holds = make(map[bufferedTargetKey]bufferedHold, len(targets))
	}
	for _, target := range targets {
		hold := w.holds[target.bufferedTargetKey]
		if target.use == bufferedTargetEndpoint {
			hold.endpoints++
		} else {
			hold.writing = true
			hold.deleting = target.use == bufferedTargetDelete
		}
		w.holds[target.bufferedTargetKey] = hold
	}
	return bufferedReservation{namespace: namespace, version: version, targets: targets}, nil, nil
}

// newer reports whether a commit version is newer than the committer's
// snapshot (BadgerTransaction.snapshotIsolationConflict).
func (w *WriteBehindBuffer) bufferedTargetConflictLocked(newer func(MVCCVersion) bool, target bufferedTarget, ownEdges map[EdgeID]struct{}) *bufferedConflict {
	if hold, held := w.holds[target.bufferedTargetKey]; held {
		if hold.writing && (target.use != bufferedTargetEndpoint || hold.deleting) {
			return &bufferedConflict{target: target}
		}
		if target.use == bufferedTargetDelete && hold.endpoints > 0 {
			return &bufferedConflict{target: target}
		}
	}
	state, version := w.entityVersionLocked(target.kind, target.id)
	changed := state.found && !version.IsZero() && newer(version)
	if changed && (target.use != bufferedTargetEndpoint || state.deleted) {
		return &bufferedConflict{target: target, version: version}
	}
	if target.use != bufferedTargetDelete {
		return nil
	}
	// A node delete must not orphan a relationship acknowledged after the
	// snapshot that the delete could not see.
	nodeID := NodeID(target.id)
	seen := make(map[EdgeID]struct{})
	check := func(buf *CommitBuffer) *bufferedConflict {
		for _, ids := range [2][]EdgeID{buf.outgoingByNode[nodeID], buf.incomingByNode[nodeID]} {
			for _, edgeID := range ids {
				if _, done := seen[edgeID]; done {
					continue
				}
				seen[edgeID] = struct{}{}
				if _, own := ownEdges[edgeID]; own {
					continue
				}
				edgeState, edgeVersion := w.entityVersionLocked(bufferedEdgeEntity, string(edgeID))
				if edgeState.found && !edgeState.deleted && !edgeVersion.IsZero() && newer(edgeVersion) {
					return &bufferedConflict{target: target, version: edgeVersion, edgeID: edgeID}
				}
			}
		}
		return nil
	}
	if conflict := check(w.active); conflict != nil {
		return conflict
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if conflict := check(w.draining[i]); conflict != nil {
			return conflict
		}
	}
	return nil
}

// releaseReservationLocked ends an admission: its claims and its in-flight
// version. Caller holds w.admitMu.
func (w *WriteBehindBuffer) releaseReservationLocked(r bufferedReservation) {
	if versions := w.inflight[r.namespace]; !r.version.IsZero() {
		for i := range versions {
			if versions[i].Compare(r.version) == 0 {
				versions = append(versions[:i], versions[i+1:]...)
				w.publicationEpoch.Add(1)
				break
			}
		}
		// An emptied list keeps its array for the namespace's next admission.
		w.inflight[r.namespace] = versions
	}
	for _, target := range r.targets {
		hold, held := w.holds[target.bufferedTargetKey]
		if !held {
			continue
		}
		if target.use == bufferedTargetEndpoint {
			hold.endpoints--
		} else {
			hold.writing, hold.deleting = false, false
		}
		if !hold.writing && hold.endpoints == 0 {
			delete(w.holds, target.bufferedTargetKey)
		} else {
			w.holds[target.bufferedTargetKey] = hold
		}
	}
	if len(w.holds) == 0 && len(r.targets) > bufferedHoldsRetainLimit {
		w.holds = nil
	}
}

// releaseReservation drops a reservation whose commit failed before its
// append.
func (w *WriteBehindBuffer) releaseReservation(r bufferedReservation) {
	if r.version.IsZero() && len(r.targets) == 0 {
		return
	}
	w.admitMu.Lock()
	w.releaseReservationLocked(r)
	w.uncountInflight(r)
	w.admitMu.Unlock()
}

// uncountInflight lowers inflightCount for r's version once it left inflight
// and, for an appended commit, became visible.
func (w *WriteBehindBuffer) uncountInflight(r bufferedReservation) {
	if !r.version.IsZero() {
		w.inflightCount.Add(-1)
	}
}

// appendReservedCommit makes an admitted commit visible and ends its
// admission in the same critical section.
func (w *WriteBehindBuffer) appendReservedCommit(c bufferedCommit, r bufferedReservation) *commitNotification {
	w.mu.Lock()
	rotate := w.appendCommitLocked(c)
	var notification *commitNotification
	if w.reserve != nil {
		i := len(w.active.commits) - 1
		notification = w.reserve(w.active.commits[i].ops)
		w.active.commits[i].notification = notification
	}
	w.admitMu.Lock()
	w.releaseReservationLocked(r)
	w.admitMu.Unlock()
	w.uncountInflight(r)
	w.mu.Unlock()
	if rotate {
		w.signal()
	}
	return notification
}

// inflightVersions copies the buffered versions admitted but not yet
// visible, per namespace; nil when there are none. A snapshot taken now
// covers their sequences without seeing their writes.
func (w *WriteBehindBuffer) inflightVersions() map[string][]MVCCVersion {
	if w.inflightCount.Load() == 0 {
		return nil
	}
	w.admitMu.Lock()
	defer w.admitMu.Unlock()
	return w.captureInflightLocked()
}

// bufferedTouchesLocked reports whether the buffer holds a state for any
// listed node or relationship, or a relationship incident to a listed node
// when incident is set. Caller holds w.mu.
func (w *WriteBehindBuffer) bufferedTouchesLocked(nodes []NodeID, edges []EdgeID, incident bool) bool {
	for _, id := range nodes {
		if w.entityStateLocked(bufferedNodeEntity, string(id)).found {
			return true
		}
		if !incident {
			continue
		}
		if len(w.active.outgoingByNode[id]) > 0 || len(w.active.incomingByNode[id]) > 0 {
			return true
		}
		for _, buf := range w.draining {
			if len(buf.outgoingByNode[id]) > 0 || len(buf.incomingByNode[id]) > 0 {
				return true
			}
		}
	}
	for _, id := range edges {
		if w.entityStateLocked(bufferedEdgeEntity, string(id)).found {
			return true
		}
	}
	return false
}

// valueOrderedLocked reports whether one of the commit's writes is subject to
// a uniqueness constraint (SchemaManager.valueOrdered); a node delete is
// checked against every node constraint.
func (tx *BadgerTransaction) valueOrderedLocked() bool {
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil && schema.valueOrdered(ConstraintEntityNode, op.Node.Labels) {
				return true
			}
			if op.OldNode != nil && schema.valueOrdered(ConstraintEntityNode, op.OldNode.Labels) {
				return true
			}
		case OpDeleteNode:
			if schema.valueOrdered(ConstraintEntityNode, nil) {
				return true
			}
		case OpCreateEdge, OpUpdateEdge, OpDeleteEdge:
			var types []string
			if op.Edge != nil {
				types = []string{op.Edge.Type}
			}
			if schema.valueOrdered(ConstraintEntityRelationship, types) {
				return true
			}
		}
	}
	return false
}

// reserveBufferedCommitLocked admits the buffered commit: its atomic conflict
// check and its version. A generation retired after the Badger validation
// sends the commit back through it, since the retired writes are only in
// Badger now.
func (tx *BadgerTransaction) reserveBufferedCommitLocked() (bufferedReservation, error) {
	targets, ownEdges := tx.bufferedCommitTargetsLocked()
	buffer := tx.engine.writeBehind
	allocate := func() (MVCCVersion, error) {
		return tx.engine.allocateMVCCVersionRaw(tx.badgerTx, tx.namespace, time.Now())
	}
	for {
		reservation, conflict, err := buffer.admitBufferedCommit(tx.namespace, tx.snapshotIsolationConflict, tx.bufferedRetireEpoch, targets, ownEdges, allocate)
		if errors.Is(err, errBufferedValidationStale) {
			tx.bufferedRetireEpoch = buffer.retireEpoch.Load()
			clear(tx.nodeHeadCache)
			if err := tx.validateSnapshotIsolationConflicts(); err != nil {
				return bufferedReservation{}, snapshotConflictCommitError(err)
			}
			continue
		}
		if conflict != nil {
			return bufferedReservation{}, snapshotConflictCommitError(tx.bufferedConflictError(conflict))
		}
		if errors.Is(err, errWriteBehindAdmissionClosed) {
			return bufferedReservation{}, localizedError(localization.StorageTransactionEngineClosed(), ErrStorageClosed)
		}
		return reservation, err
	}
}

// bufferedConflictError reports a lost buffered race with the error the
// Badger snapshot checks give for the same race.
func (tx *BadgerTransaction) bufferedConflictError(conflict *bufferedConflict) error {
	id := conflict.target.id
	if conflict.target.kind == bufferedEdgeEntity {
		return localizedError(localization.StorageTransactionEdgeChanged(id), ErrConflict)
	}
	switch {
	case conflict.edgeID != "":
		return localizedError(localization.StorageTransactionAdjacentEdgeChanged(id, string(conflict.edgeID)), ErrConflict)
	case conflict.target.use == bufferedTargetEndpoint:
		return localizedError(localization.StorageTransactionEndpointDeleted(id), ErrConflict)
	}
	// The node write conflict checkNodeWriteConflict reports, including its
	// MERGE unique-value classification; an in-flight peer has no version yet.
	if cv := tx.classifyUpdateConflictAsUniqueViolation(NodeID(id)); cv != nil {
		return cv
	}
	if conflict.version.IsZero() {
		return localizedError(localization.StorageTransactionNodeChanged(id), ErrConflict)
	}
	return localizedError(localization.StorageTransactionNodeChangedDetailed(id, conflict.version.String(), tx.readTS.String()), ErrConflict)
}

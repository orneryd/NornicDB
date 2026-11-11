package storage

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// A transaction's contribution to the positional (type, start label) and
// (type, end label) relationship counts depends on two things that can both
// change until it commits: the relationships it leaves at a node, and that
// node's labels. Another transaction can relabel, delete or recreate an end
// node this one only read (NOR-213: a locked node's relationship reads return
// their other ends as last committed), and it can commit a relationship at a
// node this one relabels while the relabel is still open. So no statement
// decides these counts. The commit derives them once, from the transaction's
// operations and the stored graph it publishes onto, while it holds every
// node involved (derivePositionalCountDeltasLocked).

// positionalEdgeChange is one signed relationship change of a transaction:
// +1 for a relationship it leaves in the graph, -1 for one it removes.
type positionalEdgeChange struct {
	namespace  string
	edgeType   string
	start, end NodeID
	sign       int64
}

// positionalNetKey is one node's share of relationship multiplicities at one
// position, label-free: the stored relationships of edgeType that start
// (end false) or end (end true) at node.
type positionalNetKey struct {
	namespace string
	node      NodeID
	edgeType  string
	end       bool
}

// newPositionalEdgeChange is the change of one relationship, by its ID, type
// and ends, with sign.
func newPositionalEdgeChange(id EdgeID, edgeType string, start, end NodeID, sign int64) positionalEdgeChange {
	return positionalEdgeChange{namespace: namespaceForEdgeID(id), edgeType: edgeType, start: start, end: end, sign: sign}
}

func positionalEdgeChangeOf(edge *Edge, sign int64) positionalEdgeChange {
	return newPositionalEdgeChange(edge.ID, edge.Type, edge.StartNode, edge.EndNode, sign)
}

// netKey is the change's share at one of its ends: its start (end false) or
// its end (end true).
func (c positionalEdgeChange) netKey(end bool) positionalNetKey {
	node := c.start
	if end {
		node = c.end
	}
	return positionalNetKey{namespace: c.namespace, node: node, edgeType: c.edgeType, end: end}
}

// visitPositionalChangesLocked walks the transaction's operations in order and
// reports what they do to the positional counts: each signed relationship
// change (a create, a delete, a rewrite of type or end, a relationship a
// node delete removes) to edge, and to node each node whose labels may end
// up other than its stored ones (a label change, or a delete, which also
// covers a delete followed by a create of the same ID). Operations are only
// appended, so a relationship created and later deleted or rewritten appears
// as both changes. A visitor that returns false stops the walk. Caller holds
// tx.mu.
func (tx *BadgerTransaction) visitPositionalChangesLocked(ctx context.Context, edge func(positionalEdgeChange) bool, node func(NodeID) bool) error {
	for i := range tx.operations {
		if i%commitCancelPollEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		op := &tx.operations[i]
		switch op.Type {
		case OpCreateEdge:
			if op.Edge != nil && !edge(positionalEdgeChangeOf(op.Edge, 1)) {
				return nil
			}
		case OpDeleteEdge:
			if op.OldEdge != nil && !edge(positionalEdgeChangeOf(op.OldEdge, -1)) {
				return nil
			}
		case OpUpdateEdge:
			if op.OldEdge == nil || op.Edge == nil || sameEdgeIdentity(op.OldEdge, op.Edge) {
				continue
			}
			if !edge(positionalEdgeChangeOf(op.OldEdge, -1)) || !edge(positionalEdgeChangeOf(op.Edge, 1)) {
				return nil
			}
		case OpUpdateNode:
			if op.OldNode != nil && op.Node != nil && !sameLabelSet(op.OldNode.Labels, op.Node.Labels) && !node(op.NodeID) {
				return nil
			}
		case OpDeleteNode:
			if !node(op.NodeID) {
				return nil
			}
			for _, fact := range op.DeletedEdgeFacts {
				if !edge(newPositionalEdgeChange(fact.EdgeID, fact.Type, fact.StartNode, fact.EndNode, -1)) {
					return nil
				}
			}
		}
	}
	return nil
}

// positionalChangesPendingLocked reports whether the transaction holds a
// change that can move a positional count before it commits, so a count read
// inside the transaction counts rows instead of taking the committed counter
// (PendingCountStoreDelta). Caller holds tx.mu (read or write).
func (tx *BadgerTransaction) positionalChangesPendingLocked(ctx context.Context) (bool, error) {
	found := false
	stop := func() bool { found = true; return false }
	err := tx.visitPositionalChangesLocked(ctx, func(positionalEdgeChange) bool { return stop() }, func(NodeID) bool { return stop() })
	return found, err
}

// derivePositionalCountDeltasLocked stages the transaction's whole
// contribution to the positional counts, once, at commit: for each node,
// type and position, the stored multiplicity after the commit times the
// node's labels after it, minus the stored multiplicity now times its stored
// labels now.
//
//   - A node whose labels the transaction leaves as stored contributes its
//     net relationship change under those labels. An end node the
//     transaction only read takes its stored labels as they are at this
//     commit, whatever its statements read; a relationship created and
//     deleted again nets to nothing, whatever happened to its end node.
//   - An existing node the transaction leaves with other labels also moves
//     the relationships stored at it now, read here
//     (storedIncidentEdgesLocked), including those another transaction
//     committed while this one was open.
//   - A node the transaction deletes takes its stored labels: its
//     relationships all go with it (the delete's cascade, checked at commit).
//
// It runs under the commit's entity authority, which holds every node
// involved: exclusive for a node the transaction writes or deletes, shared
// for the other end of a relationship it creates, deletes or rewrites, or
// that a node delete removes (pendingEntityAuthorityKeysLocked). No other
// commit can then change those nodes' labels, or the relationships stored at
// a node held exclusively, until this one is published.
//
// A non-zero contribution to a node that has neither a stored nor a pending
// image cannot be filed: the commit is refused as a conflict before anything
// is written. An accepted application, already decided, files nothing for
// it instead.
//
// It returns the nodes whose stored relationships it read: with write-behind,
// a commit the buffer does not take orders after the buffered commits of
// those relationships (orderAfterBufferedCommits), as it does for a node it
// deletes, so their counts are applied before its own. Caller holds tx.mu.
func (tx *BadgerTransaction) derivePositionalCountDeltasLocked(ctx context.Context) (relabelled []NodeID, err error) {
	var nets map[positionalNetKey]int64
	var labelChanges map[NodeID]struct{}
	add := func(key positionalNetKey, sign int64) {
		if key.edgeType == "" || key.node == "" {
			return
		}
		if nets == nil {
			nets = make(map[positionalNetKey]int64)
		}
		nets[key] += sign
	}
	if err := tx.visitPositionalChangesLocked(ctx, func(change positionalEdgeChange) bool {
		add(change.netKey(false), change.sign)
		add(change.netKey(true), change.sign)
		return true
	}, func(id NodeID) bool {
		if labelChanges == nil {
			labelChanges = make(map[NodeID]struct{})
		}
		labelChanges[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if len(nets) == 0 && len(labelChanges) == 0 {
		return nil, nil
	}

	type storedLabels struct {
		labels []string
		exists bool
	}
	stored := make(map[NodeID]storedLabels, len(labelChanges))
	storedLabelsOf := func(id NodeID) (storedLabels, error) {
		if known, ok := stored[id]; ok {
			return known, nil
		}
		node, err := tx.engine.getNodeProjected(ctx, id, []string{}, storedRead)
		var current storedLabels
		switch {
		case err == nil:
			current = storedLabels{labels: node.Labels, exists: true}
		case !errors.Is(err, ErrNotFound):
			return storedLabels{}, err
		}
		stored[id] = current
		return current, nil
	}

	// An existing node left with other labels moves what is stored at it now.
	for id := range labelChanges {
		after, pending := tx.pendingNodes[id]
		if !pending {
			continue
		}
		before, err := storedLabelsOf(id)
		if err != nil {
			return nil, err
		}
		if !before.exists || sameLabelSet(before.labels, after.Labels) {
			continue
		}
		incidence, err := tx.storedIncidenceLocked(ctx, id)
		if err != nil {
			return nil, err
		}
		relabelled = append(relabelled, id)
		for key, multiplicity := range incidence {
			tx.bufferPositionalLabelCountsLocked(key, before.labels, -multiplicity)
			tx.bufferPositionalLabelCountsLocked(key, after.Labels, multiplicity)
		}
	}

	// Every net change under the node's labels after this commit.
	for key, net := range nets {
		if net == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if after, pending := tx.pendingNodes[key.node]; pending {
			tx.bufferPositionalLabelCountsLocked(key, after.Labels, net)
			continue
		}
		current, err := storedLabelsOf(key.node)
		if err != nil {
			return nil, err
		}
		if !current.exists {
			if tx.acceptedApplication {
				continue
			}
			return nil, localizedError(localization.StorageTransactionNodeChanged(string(key.node)), ErrConflict)
		}
		tx.bufferPositionalLabelCountsLocked(key, current.labels, net)
	}
	return relabelled, nil
}

// storedIncidenceLocked counts the relationships stored at nodeID now, by
// namespace, type and position, from one current read of both directions
// (storedIncidentEdgesLocked); a self-loop counts once at each position. The
// caller holds nodeID's exclusive commit authority, which keeps that
// population fixed until the commit is published. Caller holds tx.mu.
func (tx *BadgerTransaction) storedIncidenceLocked(ctx context.Context, nodeID NodeID) (map[positionalNetKey]int64, error) {
	outgoing, incoming, err := tx.storedIncidentEdgesLocked(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	counts := make(map[positionalNetKey]int64)
	for _, edge := range outgoing {
		if edge.StartNode == nodeID && edge.Type != "" {
			counts[positionalEdgeChangeOf(edge, 1).netKey(false)]++
		}
	}
	for _, edge := range incoming {
		if edge.EndNode == nodeID && edge.Type != "" {
			counts[positionalEdgeChangeOf(edge, 1).netKey(true)]++
		}
	}
	return counts, nil
}

// bufferPositionalLabelCountsLocked stages multiplicity relationships of
// key's type at key's position under each of labels.
func (tx *BadgerTransaction) bufferPositionalLabelCountsLocked(key positionalNetKey, labels []string, multiplicity int64) {
	sub := byte(prefixMVCCMetaEdgeTypeStartLabelCount)
	if key.end {
		sub = prefixMVCCMetaEdgeTypeEndLabelCount
	}
	for _, label := range edgePositionalLabelDeltaKeys(key.namespace, key.edgeType, labels) {
		tx.bufferAdjustEdgeTypeLabelCount(sub, label.namespace, label.label, label.edgeType, multiplicity)
	}
}

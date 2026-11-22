package storage

import "context"

type cardinalityScope struct {
	anchor    NodeID
	outgoing  bool
	namespace string
	edgeType  string
}

type cardinalityCount struct {
	published uint64
	overlay   uint64
	total     int
}

// cardinalityValidationRead owns counts for one validation pass over the
// transaction's immutable staged view. A count includes the candidate until the
// caller applies the counter's self-exclusion.
type cardinalityValidationRead map[cardinalityScope]cardinalityCount

func (tx *BadgerTransaction) validateEdgeCardinality(
	ctx context.Context, edge *Edge, constraint Constraint, namespace string, read *cardinalityValidationRead,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	counter := newEdgeCardinalityCounter(constraint, edge, namespace, edge.ID)
	engine := tx.engine
	scope := cardinalityScope{counter.anchor, constraint.Direction == "OUTGOING", namespace, constraint.Label}
	readScope := namespaceForNodeID(scope.anchor)
	oracle, _ := nativePublicationOracle(engine)
	if oracle == nil || engine.work.Closing() {
		return ErrStorageClosed
	}
	published := oracle.published.Load()
	epoch, idle := engine.writeBehind.idleReadEpoch(readScope)

	includesCandidate := false
	// Sharing an inclusive count is valid only for a real staged image in
	// this population, with a stable, empty ACKed-write overlay at the anchor.
	if read != nil && len(tx.pendingEdges) > 1 && idle && tx.pendingEdges[edge.ID] == edge && edge.Type == scope.edgeType {
		counter.excludeID = ""
		includesCandidate = counter.admits(edge.ID)
		if !includesCandidate {
			counter.excludeID = edge.ID
		}
	}

	if includesCandidate {
		cached, found := (*read)[scope]
		if found && cached.published == published && cached.overlay == epoch {
			if engine.work.Closing() {
				return ErrStorageClosed
			}
			if err := engine.pendingNamespaceConflict(readScope); err != nil {
				return err
			}
			after, stillIdle := engine.writeBehind.idleReadEpoch(readScope)
			if stillIdle && after == epoch && oracle.published.Load() == published {
				counter.count = cached.total - 1
				if counter.reached() {
					return counter.violation()
				}
				return nil
			}
		}
	}

	if err := tx.countEdgeCardinality(ctx, &counter, scope.outgoing); err != nil {
		return err
	}
	if includesCandidate {
		after, stillIdle := engine.writeBehind.idleReadEpoch(readScope)
		if stillIdle && after == epoch && oracle.published.Load() == published {
			if *read == nil {
				*read = make(cardinalityValidationRead)
			}
			(*read)[scope] = cardinalityCount{published: published, overlay: epoch, total: counter.count}
		}
		counter.count--
	}
	if counter.reached() {
		return counter.violation()
	}
	return nil
}

func (tx *BadgerTransaction) countEdgeCardinality(ctx context.Context, counter *edgeCardinalityCounter, outgoing bool) error {
	var cancel bulkReadCancellation
	for id, pending := range tx.pendingEdges {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		if counter.anchored(pending) && counter.admits(id) {
			counter.counts(pending.Type)
		}
	}

	adjacency := AdjacentEdgeRead{Node: counter.anchor}
	if !outgoing {
		adjacency.Direction = AdjacentIncoming
	}
	committedEdges, err := tx.engine.ReadStoredAdjacentEdges(ctx, adjacency)
	if err != nil {
		return err
	}
	for _, existing := range committedEdges {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		if !tx.committedEdgeSupersededLocked(existing.ID) && counter.admits(existing.ID) {
			counter.counts(existing.Type)
		}
	}
	return ctx.Err()
}

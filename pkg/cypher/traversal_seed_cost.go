package cypher

import (
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// traversalSeedStatistics is one MATCH invocation's ephemeral planner
// statistics memo: the count-store answers its seed estimates read, keyed by
// the exact request they answered. The store is fixed for one MATCH iterator.
// These sampled estimates may become stale during concurrent publication;
// they only guide orientation and never decide row visibility. The memo never
// outlives its invocation or becomes transaction, store or global state, and
// nothing here is a count(*) answer or a proof that a query is empty: counts
// only guide orientation, while the rows stay authoritative.
type traversalSeedStatistics struct {
	reads []traversalSeedStatisticRead
}

// traversalSeedStatisticRead is one answered count-store read: its exact
// request identity and the answer the counters gave it. It retains no rows and
// no graph images.
type traversalSeedStatisticRead struct {
	request  storage.CountStoreReadRequest
	count    int64
	answered bool
}

// read is the seed planner's single count-store read owner. A request this
// MATCH invocation has not made yet goes to the original countStoreRead, and
// an earlier answer to exactly the same request is reused in place of another
// read of the same counters. A reused answer still observes cancellation, as
// the store read it replaces would, and an error is never stored: a failed
// read answers only its own caller.
func (s *traversalSeedStatistics) read(ctx context.Context, store storage.Engine, request storage.CountStoreReadRequest) (int64, bool, error) {
	if s == nil {
		count, reduction, err := countStoreRead(ctx, store, request)
		return count, reduction.available(), err
	}
	for index := range s.reads {
		previous := &s.reads[index]
		key := previous.request
		if key.Kind != request.Kind || key.Namespace != request.Namespace || key.Name != request.Name ||
			key.HiddenLabelPrefix != request.HiddenLabelPrefix || !slices.Equal(key.Types, request.Types) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		return previous.count, previous.answered, nil
	}
	count, reduction, err := countStoreRead(ctx, store, request)
	if err != nil {
		return 0, false, err
	}
	answered := reduction.available()
	// The requested types are the key's identity, so they are copied instead
	// of retained from a caller's slice.
	request.Types = slices.Clone(request.Types)
	s.reads = append(s.reads, traversalSeedStatisticRead{request: request, count: count, answered: answered})
	return count, answered, nil
}

// chooseTraversalEndSeeds compares candidate reads and first-hop adjacency
// work. Counts guide orientation only: they never decide that a query is empty.
// An actual empty index answer, unlike a statistical estimate, is conclusive.
// statistics is the enclosing MATCH invocation's memo, nil when the caller
// supplies none (then every estimate reads the counters itself).
//
// The end anchor's candidates are prepared, not read: the plan lists the index
// entries the end's route selects, so the comparison, the candidate count and
// the decision to walk from the start all read no end node. The end plan is
// read only when the end is the chosen walk, or when it is a singleton whose
// node a start-side walk still binds (boundEndNodeID).
func (e *StorageExecutor) chooseTraversalEndSeeds(ctx context.Context, match *TraversalMatch, startNodes []*storage.Node, startDecided bool, where string, statistics *traversalSeedStatistics) ([]*storage.Node, bool, error) {
	end := match.EndNode
	if len(end.properties) == 0 && (end.variable == "" || where == "" || !referencesVariable(where, end.variable)) {
		return nil, false, nil
	}
	if startDecided && len(startNodes) == 0 {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	startCost, startKnown, err := traversalSeedPopulation(ctx, store, match.StartNode, len(startNodes), startDecided, statistics)
	if err != nil {
		return nil, false, err
	}
	endPlan, indexed, err := e.planPipelineIndexedNodeCandidates(ctx, end, where, pipelineMatchPhysicalHint{})
	if err != nil || !indexed {
		return nil, false, err
	}
	endCandidates := endPlan.candidateCount()
	if endCandidates == 0 {
		// An index that lists no end candidate is conclusive, unlike a count
		// estimate: the plan holds no node, so reading it is the actual answer
		// and reads none.
		nodes, _, _, err := e.readNodeIndexCandidatePlan(ctx, endPlan)
		if err != nil {
			return nil, false, err
		}
		return nodes, true, nil
	}
	endCost, endKnown, err := traversalSeedPopulation(ctx, store, end, endCandidates, true, statistics)
	if err != nil {
		return nil, false, err
	}
	preferEnd := !startDecided || endCandidates < len(startNodes)
	if startKnown && endKnown {
		endCost, endKnown, err = traversalSeedAdjacencyWork(ctx, store, endCost, reverseDirection(match.Relationship.Direction), match.Relationship.Types, statistics)
		if err != nil {
			return nil, false, err
		}
		if endKnown {
			// Candidate reads alone are a lower bound on start-side work.
			// If the entire end walk is cheaper, no start degree read is needed.
			if endCost.work < startCost.work {
				nodes, _, _, err := e.readNodeIndexCandidatePlan(ctx, endPlan)
				if err != nil {
					return nil, false, err
				}
				return nodes, true, nil
			}
			startCost, startKnown, err = traversalSeedAdjacencyWork(ctx, store, startCost, match.Relationship.Direction, match.Relationship.Types, statistics)
			if err != nil {
				return nil, false, err
			}
			if startKnown {
				preferEnd = endCost.work < startCost.work
			}
		}
	}
	if preferEnd {
		nodes, _, _, err := e.readNodeIndexCandidatePlan(ctx, endPlan)
		if err != nil {
			return nil, false, err
		}
		return nodes, true, nil
	}
	// The start side walks, so the end plan is dropped unread: only a singleton
	// still binds its node, and only after the comparison, because a singleton
	// can own a large adjacency. Knowing both endpoints does not make listing it
	// cheap, and anything wider is dropped without reading a candidate.
	if endCandidates > 1 {
		return nil, false, nil
	}
	endNodes, _, _, err := e.readNodeIndexCandidatePlan(ctx, endPlan)
	if err != nil {
		return nil, false, err
	}
	if len(endNodes) == 1 {
		match.boundEndNodeID = storage.NodeID(endNodes[0].IdentityKey())
	}
	return endNodes, false, nil
}

type traversalSeedEstimate struct {
	label      string
	population int64
	work       float64
}

// traversalSeedPopulation uses the same label that a normal scan would read.
// Its candidate reads are a lower bound before considering adjacency work.
func traversalSeedPopulation(ctx context.Context, store storage.Engine, pattern nodePatternInfo, candidates int, decided bool, statistics *traversalSeedStatistics) (traversalSeedEstimate, bool, error) {
	if len(pattern.labels) == 0 {
		return traversalSeedEstimate{}, false, nil
	}
	if _, eligible := countStoreVisibilityPrefix(ctx, store); !eligible {
		return traversalSeedEstimate{}, false, nil
	}
	label := nodeScanLabels(ctx, store, pattern.labels)[0]
	population, known, err := statistics.read(ctx, store, storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: label})
	if err != nil || !known || population <= 0 {
		return traversalSeedEstimate{}, false, err
	}
	seeds := float64(population)
	if decided {
		seeds = float64(candidates)
	}
	return traversalSeedEstimate{label: label, population: population, work: seeds}, true, nil
}

// traversalSeedAdjacencyWork uses positional counters, not per-node degree
// reads (which themselves enumerate adjacency). A traversal step selects the
// pattern's relationship types at the storage read, so the counters are asked
// for those same types (nil asks for every type, as an untyped read does): the
// estimate counts the relationships the walk really lists. This first-hop
// estimate assumes uniform degree within the selected label.
func traversalSeedAdjacencyWork(ctx context.Context, store storage.Engine, estimate traversalSeedEstimate, direction string, relTypes []string, statistics *traversalSeedStatistics) (traversalSeedEstimate, bool, error) {
	seeds := estimate.work
	for _, kind := range []storage.CountStoreReadKind{storage.CountStoreRelationshipsByStartLabel, storage.CountStoreRelationshipsByEndLabel} {
		if direction == "outgoing" && kind == storage.CountStoreRelationshipsByEndLabel || direction == "incoming" && kind == storage.CountStoreRelationshipsByStartLabel {
			continue
		}
		incidence, answered, err := statistics.read(ctx, store, storage.CountStoreReadRequest{Kind: kind, Name: estimate.label, Types: relTypes})
		if err != nil || !answered || incidence < 0 {
			return traversalSeedEstimate{}, false, err
		}
		estimate.work += seeds * (float64(incidence) / float64(estimate.population))
	}
	return estimate, true, nil
}

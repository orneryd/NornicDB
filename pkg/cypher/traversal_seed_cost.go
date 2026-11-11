package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// chooseTraversalEndSeeds compares candidate reads and first-hop adjacency
// work. Counts guide orientation only: they never decide that a query is empty.
// An actual empty index answer, unlike a statistical estimate, is conclusive.
func (e *StorageExecutor) chooseTraversalEndSeeds(ctx context.Context, match *TraversalMatch, startNodes []*storage.Node, startDecided bool, where string) ([]*storage.Node, bool, error) {
	end := match.EndNode
	if len(end.properties) == 0 && (end.variable == "" || where == "" || !referencesVariable(where, end.variable)) {
		return nil, false, nil
	}
	if startDecided && len(startNodes) == 0 {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	startCost, startKnown, err := traversalSeedPopulation(ctx, store, match.StartNode, len(startNodes), startDecided)
	if err != nil {
		return nil, false, err
	}
	endNodes, _, indexed, err := e.collectPipelineIndexedNodeCandidates(ctx, end, where, pipelineMatchPhysicalHint{})
	if err != nil || !indexed {
		return nil, false, err
	}
	if len(endNodes) == 0 {
		return endNodes, true, nil
	}
	endCost, endKnown, err := traversalSeedPopulation(ctx, store, end, len(endNodes), true)
	if err != nil {
		return nil, false, err
	}
	preferEnd := !startDecided || len(endNodes) < len(startNodes)
	if startKnown && endKnown {
		endCost, endKnown, err = traversalSeedAdjacencyWork(ctx, store, endCost, reverseDirection(match.Relationship.Direction))
		if err != nil {
			return nil, false, err
		}
		if endKnown {
			// Candidate reads alone are a lower bound on start-side work.
			// If the entire end walk is cheaper, no start degree read is needed.
			if endCost.work < startCost.work {
				return endNodes, true, nil
			}
			startCost, startKnown, err = traversalSeedAdjacencyWork(ctx, store, startCost, match.Relationship.Direction)
			if err != nil {
				return nil, false, err
			}
			if startKnown {
				preferEnd = endCost.work < startCost.work
			}
		}
	}
	// Bind only after the cost comparison: a singleton can own a large
	// adjacency, and knowing both endpoints does not make listing it cheap.
	if !preferEnd && len(endNodes) == 1 {
		match.boundEndNodeID = storage.NodeID(endNodes[0].IdentityKey())
	}
	return endNodes, preferEnd, nil
}

type traversalSeedEstimate struct {
	label      string
	population int64
	work       float64
}

// traversalSeedPopulation uses the same label that a normal scan would read.
// Its candidate reads are a lower bound before considering adjacency work.
func traversalSeedPopulation(ctx context.Context, store storage.Engine, pattern nodePatternInfo, candidates int, decided bool) (traversalSeedEstimate, bool, error) {
	if len(pattern.labels) == 0 {
		return traversalSeedEstimate{}, false, nil
	}
	if _, eligible := countStoreVisibilityPrefix(ctx, store); !eligible {
		return traversalSeedEstimate{}, false, nil
	}
	label := nodeScanLabels(ctx, store, pattern.labels)[0]
	population, known, err := countStoreRead(ctx, store, storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: label})
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
// reads (which themselves enumerate adjacency). Traversal reads all relationship
// types before filtering, so typed degree would underestimate actual work.
// This first-hop estimate assumes uniform degree within the selected label.
func traversalSeedAdjacencyWork(ctx context.Context, store storage.Engine, estimate traversalSeedEstimate, direction string) (traversalSeedEstimate, bool, error) {
	seeds := estimate.work
	for _, kind := range []storage.CountStoreReadKind{storage.CountStoreRelationshipsByStartLabel, storage.CountStoreRelationshipsByEndLabel} {
		if direction == "outgoing" && kind == storage.CountStoreRelationshipsByEndLabel || direction == "incoming" && kind == storage.CountStoreRelationshipsByStartLabel {
			continue
		}
		incidence, answered, err := countStoreRead(ctx, store, storage.CountStoreReadRequest{Kind: kind, Name: estimate.label})
		if err != nil || !answered || incidence < 0 {
			return traversalSeedEstimate{}, false, err
		}
		estimate.work += seeds * (float64(incidence) / float64(estimate.population))
	}
	return estimate, true, nil
}

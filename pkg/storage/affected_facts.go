package storage

import "unsafe"

// AffectedFact is immutable, payload-free publication metadata. NodeID names a
// body or label membership; EdgeID names a relationship body, with the known
// before/after endpoints and type. Deleted marks tombstones, including cascades
// for which older accepted WAL records contain only an ID. No visitor reads storage.
type AffectedFact struct {
	NodeID    NodeID
	EdgeID    EdgeID
	StartNode NodeID
	EndNode   NodeID
	Type      string
	Label     string
	Deleted   bool
	Before    bool
}

// AffectedFacts is the contract consumed by cache, order and authority projections.
// The release publication-key producer supplies these facts in this layer. #1676
// completes discovery/recheck from that producer, rather than adding a parallel one.
type AffectedFacts interface {
	VisitAffectedFacts(func(AffectedFact))
}

type publicationKeys []any

func (keys publicationKeys) VisitAffectedFacts(visit func(AffectedFact)) {
	for _, key := range keys {
		switch fact := key.(type) {
		case AffectedFact:
			visit(fact)
		case NodeID:
			visit(AffectedFact{NodeID: fact})
		case EdgeID:
			visit(AffectedFact{EdgeID: fact})
		}
	}
}

// affectedEntities projects metadata already discovered by the producer. It
// deliberately does not accept operations, decode bodies or open a read view.
func affectedEntities(facts AffectedFacts) writtenEntities {
	var result writtenEntities
	facts.VisitAffectedFacts(func(fact AffectedFact) {
		if fact.NodeID != "" && fact.Label == "" {
			if fact.Deleted {
				result.deletedNodes = append(result.deletedNodes, fact.NodeID)
			} else {
				result.node(fact.NodeID)
			}
		}
		if fact.EdgeID != "" {
			result.edges = append(result.edges, fact.EdgeID)
			result.node(fact.StartNode)
			result.node(fact.EndNode)
		}
	})
	return result
}

// estimateAffectedFactsBytes extends the release's retained-memory estimator;
// it is not another durable byte/accounting representation.
func estimateAffectedFactsBytes(facts []AffectedFact) int64 {
	bytes := int64(cap(facts)) * int64(unsafe.Sizeof(AffectedFact{}))
	for _, fact := range facts {
		bytes += int64(len(fact.NodeID) + len(fact.EdgeID) + len(fact.StartNode) + len(fact.EndNode) + len(fact.Type) + len(fact.Label))
	}
	return bytes
}

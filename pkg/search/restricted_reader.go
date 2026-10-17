package search

import "sync"

// RestrictedReader is the one input of a restricted reader's search (#935):
// which nodes and relationships the reader may see. Every search consumer
// takes it, and every ranking population it builds holds only the items the
// reader may see: BM25's plan and statistics (prefix expansions, IDF,
// average length), vector rank lists, the lists reciprocal-rank fusion and
// graph fusion combine, and the term weights of returned matches. Hidden data
// therefore changes no plan, score, rank or result; only latency scales with
// the whole index.
//
// Node and Relationship read storage, so a search asks them only outside
// every index, relationship and score-reader lock (decide*), once per item,
// and remembers each answer; under a lock it only looks the remembered
// answers up (lookup*). An item never decided (indexed after its search
// decided visibility) is not visible. A nil Node or Relationship sees
// nothing of that kind. A nil *RestrictedReader is an unrestricted reader.
type RestrictedReader struct {
	// Node reports whether the reader may see the node with this ID.
	Node func(nodeID string) bool
	// Relationship reports whether the reader may see the relationship
	// with this ID.
	Relationship func(relationshipID string) bool

	mu            sync.Mutex
	nodes         map[string]bool
	relationships map[string]bool
}

// decide answers whether the reader may see id, asking ask once and
// remembering the answer in memo. Never call it under an index lock.
func (r *RestrictedReader) decide(memo *map[string]bool, ask func(string) bool, id string) bool {
	r.mu.Lock()
	if *memo == nil {
		*memo = make(map[string]bool)
	}
	visible, known := (*memo)[id]
	r.mu.Unlock()
	if known {
		return visible
	}
	visible = ask != nil && ask(id)
	r.mu.Lock()
	(*memo)[id] = visible
	r.mu.Unlock()
	return visible
}

// NodeVisible decides whether the reader may see a node (outside locks).
func (r *RestrictedReader) NodeVisible(nodeID string) bool {
	return r.decide(&r.nodes, r.Node, nodeID)
}

// RelationshipVisible decides whether the reader may see a relationship
// (outside locks).
func (r *RestrictedReader) RelationshipVisible(relationshipID string) bool {
	return r.decide(&r.relationships, r.Relationship, relationshipID)
}

// decideNodes decides every node in ids, before an index lock is taken.
func (r *RestrictedReader) decideNodes(ids []string) {
	for _, id := range ids {
		r.NodeVisible(normalizeVectorResultIDToNodeID(id))
	}
}

// decideRelationships decides every relationship in ids, before a
// relationship lock is taken.
func (r *RestrictedReader) decideRelationships(ids []string) {
	for _, id := range ids {
		r.RelationshipVisible(id)
	}
}

// lookup is a remembered answer; an undecided item is not visible. It reads
// no storage, so it may run under an index lock.
func (r *RestrictedReader) lookup(memo *map[string]bool, id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return (*memo)[id]
}

// nodeLookup is lookup for nodes, by indexed document ID.
func (r *RestrictedReader) nodeLookup() func(docID string) bool {
	return func(docID string) bool { return r.lookup(&r.nodes, normalizeVectorResultIDToNodeID(docID)) }
}

// relationshipLookup is lookup for relationships.
func (r *RestrictedReader) relationshipLookup() func(relationshipID string) bool {
	return func(relationshipID string) bool { return r.lookup(&r.relationships, relationshipID) }
}

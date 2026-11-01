package storage

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Relationship cardinality enforcement.
//
// Two write routes enforce CARDINALITY constraints on relationships:
//
//   - BadgerEngine.checkEdgeCardinalityInTxn counts inside the *badger.Txn it is
//     given: it iterates and decodes the anchor node's raw adjacency index, so
//     the count sees exactly the state that transaction is validating against.
//   - BadgerTransaction.checkEdgeCardinality counts inside the transaction's
//     staged view: the pending edge images overlaid on the committed adjacency,
//     with deleted and rewritten committed images excluded.
//
// Those input authorities differ genuinely, and each keeps its own selection:
// index iteration and body decoding on one side, staged overlay selection on the
// other. Everything downstream of selection is owned here — which selected
// images belong to the counted population, whether the population already fills
// the constraint, and the violation that reports it — so the two routes cannot
// drift apart on what a cardinality constraint means.
//
// The counter is a value a caller keeps on its stack and streams candidates
// through: it allocates nothing per candidate and materializes no adjacency
// collection, so no authority has to build one in order to count.

// edgeCardinalityCounter is the single owner of relationship cardinality
// counting and violation formation for the edge under validation.
type edgeCardinalityCounter struct {
	// constraint is the CARDINALITY constraint being enforced.
	constraint Constraint
	// anchor is the node the count is measured on: the start node for an
	// OUTGOING constraint, the end node for an INCOMING one.
	anchor NodeID
	// namespace scopes the count to one database, or is empty when the caller
	// counts every database's edges. It is the bare namespace name: admits
	// matches the scope boundary against it in place, so preparing a counter
	// builds no "name:" key and no scope object is carried beyond the caller's
	// own namespace string.
	namespace string
	// excludeID is the image the caller is validating: an edge never counts
	// towards itself.
	excludeID EdgeID
	// count is how many admitted images of the constraint's relationship type
	// the route selected. counts is its only writer.
	count int
}

// relationshipCardinalityAnchor is the endpoint selection shared by constraint
// creation (a count per existing anchor) and candidate-write validation (one
// anchor, excluding the candidate). Their different count populations remain
// streamed by their own authorities; neither builds the other's scratch state.
func relationshipCardinalityAnchor(direction string, edge *Edge) NodeID {
	if direction == "OUTGOING" {
		return edge.StartNode
	}
	return edge.EndNode
}

// newEdgeCardinalityCounter prepares the counter for one validation.
// excludeEdgeID is the edge under validation — the image being created, or the
// current image of one being rewritten. An empty namespace counts every
// namespace, as the unscoped callers already did.
func newEdgeCardinalityCounter(c Constraint, edge *Edge, namespace string, excludeEdgeID EdgeID) edgeCardinalityCounter {
	counter := edgeCardinalityCounter{constraint: c, namespace: namespace, excludeID: excludeEdgeID}
	counter.anchor = relationshipCardinalityAnchor(c.Direction, edge)
	return counter
}

// anchored reports whether a staged edge image is incident to the counted anchor
// in the constraint's direction. A staged overlay offers the images a node
// touches in both directions, so the route selecting from it asks this per
// image; the raw index route selects by the anchor node's own index prefix
// instead.
func (e *edgeCardinalityCounter) anchored(edge *Edge) bool {
	return relationshipCardinalityAnchor(e.constraint.Direction, edge) == e.anchor
}

// admits reports whether a selected image belongs to the counted population
// before its body is read: the edge under validation never counts towards
// itself, and a scoped count admits only its own namespace. Both routes ask
// this, so a staged image of the edge being rewritten and an index entry of it
// are excluded the same way.
//
// A namespace owns exactly the "name:" prefix of an ID, so the scope is the
// caller's namespace plus a boundary at its length: "test" admits "test:e" but
// not "test2:e", "test" or "tes". Matching in place keeps the check identical to
// the "name:" prefix it replaces while allocating nothing per validation.
func (e *edgeCardinalityCounter) admits(edgeID EdgeID) bool {
	if edgeID == e.excludeID {
		return false
	}
	if e.namespace == "" {
		return true
	}
	id := string(edgeID)
	return len(id) > len(e.namespace) && id[len(e.namespace)] == ':' && strings.HasPrefix(id, e.namespace)
}

// counts records an admitted image, reporting whether it belongs to the
// constraint: only images of the constrained relationship type count.
func (e *edgeCardinalityCounter) counts(edgeType string) bool {
	if edgeType != e.constraint.Label {
		return false
	}
	e.count++
	return true
}

// reached reports whether the counted population already fills the constraint,
// so the edge under validation would exceed it.
func (e *edgeCardinalityCounter) reached() bool {
	return e.count >= e.constraint.MaxCount
}

// violation is the one cardinality violation both routes report, formed from the
// counter's own population and constraint so a route cannot describe a count,
// direction, relationship type or anchor other than the one it enforced.
func (e *edgeCardinalityCounter) violation() *ConstraintViolationError {
	message := localization.StorageValidationCardinalityExceeded(
		strings.ToLower(e.constraint.Direction), e.constraint.MaxCount, e.constraint.Label, string(e.anchor), e.count)
	return newLocalizedConstraintViolation(ConstraintCardinality, e.constraint.Label, nil, message, nil)
}

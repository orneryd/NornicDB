package cypher

// The node IDs the canonical ID resolvers list.
//
// The resolvers that prepare a node pattern's candidates (id()/elementId()
// equality, its IN list and its OR, the WHERE point seek, the pattern's inline
// property seek and the indexed property reads) answer with the IDs they
// listed, and the plan keeps that listing until the read hydrates it. An
// ordinary lookup names one ID and a small IN or OR a handful; those stay in
// the result's own array, so listing them costs no heap allocation, and only a
// longer listing keeps an owned slice.
//
// A result never stores a slice into its own array: a plan is copied by value
// (prepared, returned, and read by value, and a read may be taken more than
// once from the same prepared plan), so a stored slice would point into
// whichever copy happened to build it. The listing is therefore borrowed at
// the point of use (ids), which the read paths do synchronously and never keep.

import "github.com/orneryd/nornicdb/pkg/storage"

// nodeIDResultsInline is how many IDs an ID result keeps in its own array: an
// ordinary equality's one ID, a small IN list and an OR of equalities fit, and
// a longer listing moves to the owned overflow.
const nodeIDResultsInline = 8

// nodeIDResults is the node IDs one canonical ID resolver listed, in the order
// it listed them: the inline array while they fit there, otherwise the owned
// overflow slice that then holds all of them. It holds no slice into its own
// array, so copying it copies a listing that stays its own.
type nodeIDResults struct {
	inline   [nodeIDResultsInline]storage.NodeID
	inlineN  int
	overflow []storage.NodeID
}

// len is how many IDs the result lists.
func (r *nodeIDResults) len() int {
	if r.overflow != nil {
		return len(r.overflow)
	}
	return r.inlineN
}

// ids is the listed IDs, in the order the resolver listed them: the result's
// own inline array while they fit there, otherwise the owned overflow slice.
// It borrows an array of its receiver, so a caller uses it only synchronously,
// while that result is alive, and stores it nowhere: a copy of the result has
// its own array, and the borrowed slice is never the copy's listing.
func (r *nodeIDResults) ids() []storage.NodeID {
	if r.overflow != nil {
		return r.overflow
	}
	return r.inline[:r.inlineN]
}

// singleIDResult is the result of a resolver that listed the one ID id.
func singleIDResult(id storage.NodeID) nodeIDResults {
	var r nodeIDResults
	r.inline[0], r.inlineN = id, 1
	return r
}

// newIDResults makes a result that lists at least n IDs without moving them: a
// listing that fits the inline array stays there, and a longer one gets the
// owned overflow it fills. A resolver that knows how many IDs it may list makes
// its result with that number, so a long listing costs the one allocation its
// overflow needs and a small lookup costs none.
func newIDResults(n int) nodeIDResults {
	var r nodeIDResults
	if n > len(r.inline) {
		r.overflow = make([]storage.NodeID, 0, n)
	}
	return r
}

// add appends id to the listing.
func (r *nodeIDResults) add(id storage.NodeID) {
	if r.overflow != nil {
		r.overflow = append(r.overflow, id)
		return
	}
	if r.inlineN < len(r.inline) {
		r.inline[r.inlineN] = id
		r.inlineN++
		return
	}
	// The listing outgrew the inline array: from here it is one owned slice,
	// so no reader borrows the result's own array for it.
	r.overflow = make([]storage.NodeID, len(r.inline), 2*len(r.inline))
	copy(r.overflow, r.inline[:r.inlineN])
	r.overflow = append(r.overflow, id)
	r.inlineN = 0
}

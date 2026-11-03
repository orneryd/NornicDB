package storage

import "context"

// CompositeIndexLookup returns one composite-index read — all indexed
// properties equal (full) or the leading properties equal (prefix) — merged
// with the pending writes the schema is attached to (#719, NOR-375).
//
// A composite index's postings are maintained when a write reaches the
// engine, so on an engine with an attached write-behind buffer they describe
// committed state only. An acknowledged-but-unflushed rename is already the
// body every read returns (GetNode, label scans, the pending property-index
// owner) while the index still lists the node under its previous key. A
// reader that resolves an indexed MATCH/MERGE candidate set through the index
// postings alone therefore misses the acknowledged membership, and MERGE
// creates a duplicate of a node that already exists. This owner is the one
// read that combines both populations:
//
//   - committed candidates of nodes the pending writes superseded (created,
//     rewritten or deleted) are dropped;
//   - the pending nodes that hold the same leading values are added, read
//     from the pending index's value populations instead of by copying every
//     buffered node of the label.
//
// The pending view is captured before the index lock, exactly as the scalar
// property-index owner (PropertyIndexLookup) captures it: the buffer's read
// lease stays stable while the postings are copied and merged, so a node a
// drain retires is counted in exactly one of the two populations — never in
// both, never in neither.
//
// values are the raw pattern values; they are canonicalized exactly as
// IndexNode and LookupFull/LookupPrefix canonicalize them, so a pending node
// equals a committed entry precisely when it keys identically. A value no
// index entry can hold yields no pending candidate, matching the committed
// miss.
//
// CompositeIndex.LookupFull and LookupPrefix remain the committed posting
// reads. This merged read must not be called from a caller that already holds
// the buffer's read view (constraint validation's captured view): the pending
// source's read lease is not reentrant, so re-acquiring it there would queue a
// second reader behind a writer that is waiting for the first, and neither
// would return.
func (sm *SchemaManager) CompositeIndexLookup(ctx context.Context, idx *CompositeIndex, values []interface{}, full bool) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if idx == nil || (full && len(values) != len(idx.Properties)) {
		return nil, nil
	}
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return nil, err
	}
	defer endPendingRead(source)

	var committed []NodeID
	if full {
		committed, err = idx.LookupFull(ctx, values...)
	} else {
		committed, err = idx.LookupPrefix(ctx, values...)
	}
	if err != nil {
		return nil, err
	}
	if view.index == nil {
		// Nothing is pending, so the committed postings are the whole answer.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return committed, nil
	}
	// The index's candidate slice is owned here, so superseded entries are
	// compacted in place: one output, no second copy of the committed set.
	kept := committed[:0]
	var cancellation bulkReadCancellation
	for _, id := range committed {
		if err := cancellation.check(ctx); err != nil {
			return nil, err
		}
		if view.keep(id) {
			kept = append(kept, id)
		}
	}
	return view.compositeMatches(ctx, kept, idx.Label, idx.Properties, values)
}

package storage

import "context"

// Count-store reads (NOR177).
//
// The derived counters answer a read only while they count the rows that read
// returns. A user database hides every node carrying an internal label (a label
// starting with "_"), while the counters keep counting such a node's labels and
// its relationships. Asking "does this scope hold a hidden node?" through one
// read and then reading a counter through another answered two different
// observations: a CREATE committed between them made count() report rows no read
// returns (k+1 while the same pattern returned k rows).
//
// CountStoreReadEngine answers both halves from ONE persisted snapshot and ONE
// buffered view, under the existing count-family read holds, so its answer
// describes the rows of one consistent point. No key, state or migration is
// added: the read composes the canonical label, edge-type and positional
// (label, type) counter readers.
//
// A store that cannot establish the answer (a custom view with no counter
// implementation, a transaction whose staged writes it cannot attribute) reports
// DescribesRows false, and the read falls back to the rows themselves.

// CountStoreReadKind selects the counter family a count-store read asks for.
type CountStoreReadKind uint8

const (
	// CountStoreNodesByLabel counts the nodes carrying Name as a label.
	CountStoreNodesByLabel CountStoreReadKind = iota + 1
	// CountStoreRelationshipsByType counts the relationships of Types, or of
	// every relationship type the scope holds when Types is empty.
	CountStoreRelationshipsByType
	// CountStoreRelationshipsByStartLabel counts the relationships of Types
	// whose start endpoint carries Name as a label.
	CountStoreRelationshipsByStartLabel
	// CountStoreRelationshipsByEndLabel counts the relationships of Types
	// whose end endpoint carries Name as a label.
	CountStoreRelationshipsByEndLabel
)

// CountStoreReadRequest is one count-store read.
type CountStoreReadRequest struct {
	// Kind is the counter family to read.
	Kind CountStoreReadKind
	// Namespace is the database to count. An empty Namespace counts across
	// every namespace, as NodeCountByLabel and EdgeCountByType do.
	Namespace string
	// Name is the node label of a CountStoreNodesByLabel read, or the endpoint
	// label of a positional (label, type) read.
	Name string
	// Types are the relationship types to sum, or empty for every type.
	Types []string
	// HiddenLabelPrefix is the label prefix of the nodes the reading scope
	// hides: the counters describe the read's rows only while the scope holds
	// no relevant node carrying such a label. An empty prefix hides none, as
	// the system database's reads do, and the counters answer directly.
	HiddenLabelPrefix string
}

// CountStoreRead is the answer to one count-store read.
type CountStoreRead struct {
	// Count is the raw derived counter value. It is a query result only when
	// DescribesRows is true; otherwise the caller counts visible rows.
	Count int64
	// DescribesRows reports whether Count describes the rows a read in the same
	// scope returns. False leaves the count to the rows themselves.
	DescribesRows bool
}

// CountStoreReadEngine is an optional extension interface for count-store reads
// whose counter value and visibility describe one consistent observation.
//
// Implementations answer Count from one persisted snapshot and one buffered
// view. Endpoints, reads with a decay filter or a temporal viewport, and scopes
// the store cannot prove are the caller's to decline.
//
// Example:
//
//	reader, ok := engine.(storage.CountStoreReadEngine)
//	if ok {
//		answer, err := reader.ReadCountStore(ctx, storage.CountStoreReadRequest{
//			Kind:              storage.CountStoreNodesByLabel,
//			Name:              "Person",
//			HiddenLabelPrefix: "_",
//		})
//		if err == nil && answer.DescribesRows {
//			return answer.Count
//		}
//	}
type CountStoreReadEngine interface {
	ReadCountStore(ctx context.Context, request CountStoreReadRequest) (CountStoreRead, error)
}

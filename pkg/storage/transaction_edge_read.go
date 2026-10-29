package storage

// TransactionEdgeReadKind selects a native transaction adjacency query.
type TransactionEdgeReadKind uint8

const (
	// TransactionReadOutgoing selects edges whose start node is Node.
	TransactionReadOutgoing TransactionEdgeReadKind = iota
	// TransactionReadIncoming selects edges whose end node is Node.
	TransactionReadIncoming
	// TransactionReadBetween selects edges directed from Node to Other.
	TransactionReadBetween
)

// TransactionEdgeRead describes one native transaction adjacency query. Node
// and Other carry namespace-prefixed IDs; Other is used only for between reads.
type TransactionEdgeRead struct {
	Kind  TransactionEdgeReadKind
	Node  NodeID
	Other NodeID
}

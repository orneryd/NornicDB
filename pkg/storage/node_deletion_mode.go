package storage

// NodeDeletionMode selects how transactional node deletion treats relationships.
type NodeDeletionMode uint8

const (
	// CascadeEdges removes the node and all incident relationships (DETACH DELETE).
	CascadeEdges NodeDeletionMode = iota
	// RejectConnected checks at commit that later operations removed every
	// incident relationship; otherwise the complete transaction rolls back.
	RejectConnected
)

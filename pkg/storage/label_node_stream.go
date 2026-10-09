package storage

import "errors"

// streamNodesByLabelForValidation visits the nodes of label through engine,
// decoding only properties, for schema validation and cache rebuilds.
//
// It uses ProjectedLabelNodeReader when engine implements it (Badger, WAL,
// Async, Namespaced — scoped to the database — Composite and the transaction
// wrappers). When the engine lacks it, or reports ErrNotImplemented before
// visiting anything (a wrapper whose inner engine cannot stream), it falls
// back to GetNodesByLabel: still label-scoped, never a whole-graph AllNodes
// scan.
func streamNodesByLabelForValidation(engine Engine, label string, properties []string, visit func(*Node) error) error {
	if reader, ok := engine.(ProjectedLabelNodeReader); ok {
		visited := false
		err := reader.StreamNodesByLabelProjected(label, properties, func(node *Node) error {
			visited = true
			return visit(node)
		})
		if visited || !errors.Is(err, ErrNotImplemented) {
			return err
		}
	}
	nodes, err := engine.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

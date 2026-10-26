package storage

import "context"

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
	return streamNodesByLabelForValidationWithContext(context.Background(), engine, label, properties, visit)
}

// streamNodesByLabelForValidationWithContext carries the caller through this read and its materialization.
func streamNodesByLabelForValidationWithContext(ctx context.Context, engine Engine, label string, properties []string, visit func(*Node) error) error {
	return StreamLabelNodesWithContext(ctx, engine, label, properties, visit)
}

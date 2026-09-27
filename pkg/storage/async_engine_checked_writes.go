package storage

// Checked writes (#700).
//
// The AsyncEngine acknowledges a cached write before the engine below it
// stores it. A write the schema checks (a constraint, property type, contract
// or relationship policy applies to it: SchemaManager.NodeWriteChecked /
// EdgeWriteChecked) is therefore not cached: it goes through to the engine,
// which checks it with the one constraint implementation every write path
// shares, under the same UNIQUE value locks as a transaction's commit, and
// returns a violation to the caller. A cached write is one no rule checks, so
// the flush can't reject it on a constraint and wedge the engine (every later
// transaction start and schema command waits for that flush).
//
// Schema changes hold writeGate exclusively (PauseWritesForSchemaChange)
// while they flush the cache, check the existing data and register the new
// rule; every write holds it shared while it decides whether it's checked and
// caches or writes it. So no write is cached under a rule it was never
// checked against.

// PauseWritesForSchemaChange stops new node and relationship writes until the
// returned function is called, after flushing the cached writes. A schema
// command (CREATE / DROP CONSTRAINT, CREATE INDEX) calls it before it checks
// the stored data against a new rule and registers the rule, and releases it
// after. If the flush fails, the writes stay paused only until the returned
// function is called; the error is returned with it.
func (ae *AsyncEngine) PauseWritesForSchemaChange() (resume func(), err error) {
	ae.writeGate.Lock()
	if ae.HasPendingWrites() {
		if flushErr := ae.Flush(); flushErr != nil {
			return ae.writeGate.Unlock, flushErr
		}
	}
	return ae.writeGate.Unlock, nil
}

// nodeWriteChecked reports whether the schema checks a write of node: one of
// its labels (or, for an update, one of the labels it had) is covered by a
// constraint, property type, contract or relationship policy. A node ID
// without a database prefix, on an engine with no namespace, is an error
// here, before anything is cached: the engine would refuse it at flush.
func (ae *AsyncEngine) nodeWriteChecked(node *Node, previousLabels []string) (bool, error) {
	namespace, _, err := ae.resolveNamespace(node.ID)
	if err != nil {
		return false, err
	}
	schema := ae.GetSchemaForNamespace(namespace)
	return schema.NodeWriteChecked(node.Labels) || schema.NodeWriteChecked(previousLabels), nil
}

// edgeWriteChecked reports whether the schema checks a write of a
// relationship of edge's type.
func (ae *AsyncEngine) edgeWriteChecked(edge *Edge) bool {
	namespace, _, err := ae.resolveNamespace(NodeID(edge.ID))
	if err != nil {
		return false
	}
	return ae.GetSchemaForNamespace(namespace).EdgeWriteChecked(edge.Type)
}

// writeThrough runs a checked write on the engine. The cached writes are
// flushed first: the write may depend on them (an endpoint node or an earlier
// version still in the cache, a cached delete of the node that held a UNIQUE
// value). The caller holds writeGate shared.
func (ae *AsyncEngine) writeThrough(write func() error) error {
	if ae.HasPendingWrites() {
		if err := ae.Flush(); err != nil {
			return err
		}
	}
	return write()
}

// cachedNodeLabels returns the labels node id has before a write: its cached
// version's, else the engine's, else none.
func (ae *AsyncEngine) cachedNodeLabels(id NodeID) []string {
	ae.mu.RLock()
	cached, ok := ae.nodeCache[id]
	ae.mu.RUnlock()
	if ok && cached != nil {
		return cached.Labels
	}
	if existing, err := ae.engine.GetNode(id); err == nil && existing != nil {
		return existing.Labels
	}
	return nil
}

package cypher

// countCreatedEntity records the properties and labels of a node or
// relationship a statement created, as Neo4j's result summary does
// (#651): every non-null property it was created with counts as a property
// set, every label as a label added. The node / relationship itself is
// counted by the caller, and a SET / ON CREATE SET applied to it afterwards
// is counted by the SET rule (applyCountedNodeSet).
func countCreatedEntity(stats *QueryStats, labels []string, properties map[string]interface{}) {
	if stats == nil {
		return
	}
	stats.LabelsAdded += len(labels)
	for _, value := range properties {
		if value != nil {
			stats.PropertiesSet++
		}
	}
}

// mergeQueryStats adds delta to total and returns total. It allocates total
// only when delta records a write, so read-only paths that combine the
// counters of nested executions stay allocation-free.
func mergeQueryStats(total, delta *QueryStats) *QueryStats {
	if delta == nil || *delta == (QueryStats{}) {
		return total
	}
	if total == nil {
		total = &QueryStats{}
	}
	addQueryStats(total, delta)
	return total
}

// addQueryStats accumulates mutation counters from nested execution paths.
func addQueryStats(total, delta *QueryStats) {
	if total == nil || delta == nil {
		return
	}
	total.NodesCreated += delta.NodesCreated
	total.NodesDeleted += delta.NodesDeleted
	total.RelationshipsCreated += delta.RelationshipsCreated
	total.RelationshipsDeleted += delta.RelationshipsDeleted
	total.PropertiesSet += delta.PropertiesSet
	total.LabelsAdded += delta.LabelsAdded
	total.LabelsRemoved += delta.LabelsRemoved
	total.IndexesAdded += delta.IndexesAdded
	total.IndexesRemoved += delta.IndexesRemoved
	total.ConstraintsAdded += delta.ConstraintsAdded
	total.ConstraintsRemoved += delta.ConstraintsRemoved
}

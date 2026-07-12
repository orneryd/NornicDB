package cypher

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
}

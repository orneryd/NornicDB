package localization

const (
	MessageHistoricalNeighborhoodRoute MessageID = "graph.historical_neighborhood_route"
	MessagePathNodeIDsRequired         MessageID = "graph.path_node_ids_required"
	MessageHistoricalPathRoute         MessageID = "graph.historical_path_route"
	MessageGraphDatabaseAccessDenied   MessageID = "graph.database_access_denied"
)

// HistoricalNeighborhoodRoute identifies the supported endpoints for historical neighborhood traversal.
func HistoricalNeighborhoodRoute() Message {
	return Message{ID: MessageHistoricalNeighborhoodRoute, Fallback: "historical neighborhood traversal is exposed via /nornicdb/graph/{database}/temporal or /nornicdb/graph/{database}/diff"}
}

// PathNodeIDsRequired identifies missing source and target node IDs.
func PathNodeIDsRequired() Message {
	return Message{ID: MessagePathNodeIDsRequired, Fallback: "source_node_id and target_node_id are required"}
}

// HistoricalPathRoute identifies the supported endpoint for historical path reconstruction.
func HistoricalPathRoute() Message {
	return Message{ID: MessageHistoricalPathRoute, Fallback: "historical path traversal is not yet exposed on /nornicdb/graph/{database}/path; use /nornicdb/graph/{database}/temporal for snapshot reconstruction"}
}

// GraphDatabaseAccessDenied identifies denied access where the database name must not be disclosed.
func GraphDatabaseAccessDenied() Message {
	return Message{ID: MessageGraphDatabaseAccessDenied, Fallback: "Access to the requested database is not allowed."}
}

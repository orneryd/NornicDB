package localization

const (
	MessageSearchRelationshipsDisabled     MessageID = "search.relationships_disabled"
	MessageSearchRelationshipsContinuation MessageID = "search.relationships_continuation"
	MessageSearchGraphContinuation         MessageID = "search.graph_continuation"
)

// SearchRelationshipsDisabled identifies a relationship search on a database
// that doesn't index relationships (#1065).
func SearchRelationshipsDisabled() Message {
	return Message{ID: MessageSearchRelationshipsDisabled, Fallback: "relationship search is not enabled for this database; set NORNICDB_SEARCH_RELATIONSHIPS_ENABLED=true"}
}

// SearchRelationshipsContinuation identifies a relationship search asking for
// a continuation (qid, n, mode, group_by or ranked_limit), which it doesn't
// support: it returns one page (#1065).
func SearchRelationshipsContinuation() Message {
	return Message{ID: MessageSearchRelationshipsContinuation, Fallback: "relationship search returns a single page and can't be continued"}
}

// SearchGraphContinuation identifies a graph search (db.retrieveGraph) asking
// for a stateful continuation (qid, discard, mode, group_by or
// ranked_limit): it returns one terminal page.
func SearchGraphContinuation() Message {
	return Message{ID: MessageSearchGraphContinuation, Fallback: "db.retrieveGraph returns one page: qid, discard, mode, groupBy and rankedLimit don't apply"}
}

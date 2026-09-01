package localization

const (
	MessageAutoEmbedDisabled        MessageID = "search.auto_embed_not_enabled"
	MessageSearchServiceUnavailable MessageID = "search.service_unavailable"
	MessageQueryChunkingFailed      MessageID = "search.query_chunking_failed"
	MessageNodeHasNoEmbedding       MessageID = "search.node_has_no_embedding"
)

// AutoEmbedNotEnabled identifies an unavailable automatic embedding queue.
func AutoEmbedNotEnabled() Message {
	return Message{ID: MessageAutoEmbedDisabled, Fallback: "Auto-embed not enabled"}
}

// SearchServiceUnavailable identifies an unavailable search service.
func SearchServiceUnavailable() Message {
	return Message{ID: MessageSearchServiceUnavailable, Fallback: "search service unavailable"}
}

// QueryChunkingFailed identifies a query that could not be chunked.
func QueryChunkingFailed() Message {
	return Message{ID: MessageQueryChunkingFailed, Fallback: "failed to chunk query"}
}

// NodeHasNoEmbedding identifies a vector-similarity target without an embedding.
func NodeHasNoEmbedding() Message {
	return Message{ID: MessageNodeHasNoEmbedding, Fallback: "Node has no embedding"}
}

package localization

import "strconv"

const (
	MessageAutoEmbedDisabled               MessageID = "search.auto_embed_not_enabled"
	MessageSearchServiceUnavailable        MessageID = "search.service_unavailable"
	MessageQueryChunkingFailed             MessageID = "search.query_chunking_failed"
	MessageNodeHasNoEmbedding              MessageID = "search.node_has_no_embedding"
	MessageSearchNodeNotFound              MessageID = "search.node_not_found"
	MessageSearchQueryEmbeddingRequired    MessageID = "search.query_embedding_required"
	MessageSearchIndexBuilding             MessageID = "search.index_building"
	MessageSearchVectorEmbeddingRequired   MessageID = "search.vector_embedding_required"
	MessageSearchClusteringNotEnabled      MessageID = "search.clustering_not_enabled"
	MessageSearchClusteringFailed          MessageID = "search.clustering_failed"
	MessageSearchClusterNotClustered       MessageID = "search.cluster_index_not_clustered"
	MessageSearchVectorIndexUnavailable    MessageID = "search.vector_index_unavailable"
	MessageSearchNoClusters                MessageID = "search.no_clusters"
	MessageSearchVectorPipelineFailed      MessageID = "search.vector_pipeline_failed"
	MessageSearchCandidateGenerationFailed MessageID = "search.candidate_generation_failed"
	MessageSearchExactScoringFailed        MessageID = "search.exact_scoring_failed"
	MessageSearchGPUEmbeddingUnavailable   MessageID = "search.gpu_embedding_index_unavailable"
	MessageSearchDimensionsPositive        MessageID = "search.dimensions_must_be_positive"
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

func SearchNodeNotFound(id string) Message {
	return Message{ID: MessageSearchNodeNotFound, Fallback: "Node '" + id + "' not found", Data: map[string]any{"ID": id}}
}

// SearchQueryEmbeddingRequired identifies a vector query without an embedding.
func SearchQueryEmbeddingRequired() Message {
	return Message{ID: MessageSearchQueryEmbeddingRequired, Fallback: "query embedding required"}
}

// SearchIndexBuilding identifies a temporarily unavailable search index.
func SearchIndexBuilding() Message {
	return Message{ID: MessageSearchIndexBuilding, Fallback: "search index being built, please try again when they are complete"}
}

// SearchVectorEmbeddingRequired identifies vector search without an embedding.
func SearchVectorEmbeddingRequired() Message {
	return Message{ID: MessageSearchVectorEmbeddingRequired, Fallback: "vector search requires embedding"}
}

// SearchClusteringNotEnabled identifies a clustering request before configuration.
func SearchClusteringNotEnabled() Message {
	return Message{ID: MessageSearchClusteringNotEnabled, Fallback: "clustering not enabled - call EnableClustering() first"}
}

// SearchClusteringFailed identifies a clustering operation failure.
func SearchClusteringFailed(cause error) Message {
	return Message{ID: MessageSearchClusteringFailed, Fallback: "clustering failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchClusterIndexNotClustered identifies an unavailable clustered index.
func SearchClusterIndexNotClustered() Message {
	return Message{ID: MessageSearchClusterNotClustered, Fallback: "cluster index not clustered"}
}

// SearchVectorIndexUnavailable identifies an unavailable vector index.
func SearchVectorIndexUnavailable() Message {
	return Message{ID: MessageSearchVectorIndexUnavailable, Fallback: "vector index unavailable"}
}

// SearchNoClusters identifies an empty clustering result.
func SearchNoClusters() Message {
	return Message{ID: MessageSearchNoClusters, Fallback: "no clusters"}
}

// SearchVectorPipelineFailed identifies vector pipeline initialization failure.
func SearchVectorPipelineFailed(cause error) Message {
	return Message{ID: MessageSearchVectorPipelineFailed, Fallback: "failed to get vector pipeline: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchCandidateGenerationFailed identifies vector candidate generation failure.
func SearchCandidateGenerationFailed(cause error) Message {
	return Message{ID: MessageSearchCandidateGenerationFailed, Fallback: "candidate generation failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchExactScoringFailed identifies vector exact-scoring failure.
func SearchExactScoringFailed(cause error) Message {
	return Message{ID: MessageSearchExactScoringFailed, Fallback: "exact scoring failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchGPUEmbeddingUnavailable identifies an unavailable GPU embedding index.
func SearchGPUEmbeddingUnavailable() Message {
	return Message{ID: MessageSearchGPUEmbeddingUnavailable, Fallback: "gpu embedding index unavailable"}
}

// SearchDimensionsMustBePositive identifies invalid vector-store dimensions.
func SearchDimensionsMustBePositive(dimensions int) Message {
	return Message{ID: MessageSearchDimensionsPositive, Fallback: "dimensions must be > 0, got " + strconv.Itoa(dimensions), Data: map[string]any{"Dimensions": dimensions}}
}

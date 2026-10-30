package localization

import "strconv"

const (
	MessageSearchGPUKMeansClusteredIndexRequired MessageID = "search.gpu_kmeans_clustered_index_required"
	MessageSearchClusterHNSWLookupNotConfigured  MessageID = "search.cluster_hnsw_lookup_not_configured"
	MessageSearchIVFPQIndexNotConfigured         MessageID = "search.ivfpq_index_not_configured"
	MessageSearchClusterQueryDimensionsMismatch  MessageID = "search.cluster_query_dimensions_mismatch"
	MessageSearchClusterFailed                   MessageID = "search.cluster_search_failed"
	MessageSearchHNSWIndexCreationFailed         MessageID = "search.hnsw_index_creation_failed"
	MessageSearchVectorIndexNil                  MessageID = "search.vector_index_nil"
)

// SearchGPUKMeansClusteredIndexRequired identifies an unavailable clustered index for GPU k-means routing.
func SearchGPUKMeansClusteredIndexRequired() Message {
	return Message{ID: MessageSearchGPUKMeansClusteredIndexRequired, Fallback: "gpu k-means candidate gen requires clustered index"}
}

// SearchClusterHNSWLookupNotConfigured identifies missing per-cluster HNSW lookup configuration.
func SearchClusterHNSWLookupNotConfigured() Message {
	return Message{ID: MessageSearchClusterHNSWLookupNotConfigured, Fallback: "cluster HNSW lookup not configured"}
}

// SearchIVFPQIndexNotConfigured identifies a missing IVF/PQ index.
func SearchIVFPQIndexNotConfigured() Message {
	return Message{ID: MessageSearchIVFPQIndexNotConfigured, Fallback: "ivfpq index not configured"}
}

// SearchClusterQueryDimensionsMismatch identifies incompatible query and cluster-index dimensions.
func SearchClusterQueryDimensionsMismatch(queryDimensions, indexDimensions int) Message {
	return Message{
		ID:       MessageSearchClusterQueryDimensionsMismatch,
		Fallback: "cluster search failed: query dimensions " + strconv.Itoa(queryDimensions) + " != index dimensions " + strconv.Itoa(indexDimensions),
		Data: map[string]any{
			"QueryDimensions": queryDimensions,
			"IndexDimensions": indexDimensions,
		},
	}
}

// SearchClusterFailed identifies a cluster search failure.
func SearchClusterFailed(cause error) Message {
	return Message{ID: MessageSearchClusterFailed, Fallback: "cluster search failed: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchHNSWIndexCreationFailed identifies an HNSW index initialization failure.
func SearchHNSWIndexCreationFailed(cause error) Message {
	return Message{ID: MessageSearchHNSWIndexCreationFailed, Fallback: "failed to create HNSW index: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// SearchVectorIndexNil identifies missing vector-index state during HNSW initialization.
func SearchVectorIndexNil() Message {
	return Message{ID: MessageSearchVectorIndexNil, Fallback: "vector index is nil"}
}

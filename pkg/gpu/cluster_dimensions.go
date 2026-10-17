package gpu

// EmptyWithDimensions creates an empty cluster index for a replacement vector
// dimension, retaining this index's manager, synchronization policy, capacity,
// and k-means settings. The original index is unchanged.
//
// Use it when a database without managed embeddings adopts its first stored
// vector's dimension before populating clustering:
//
//	replacement := index.EmptyWithDimensions(768)
func (ci *ClusterIndex) EmptyWithDimensions(dimensions int) *ClusterIndex {
	config := ci.Config()
	ci.EmbeddingIndex.mu.RLock()
	manager := ci.manager
	embeddings := &EmbeddingIndexConfig{
		Dimensions:     dimensions,
		InitialCap:     cap(ci.nodeIDs),
		AutoSync:       ci.autoSync,
		BatchThreshold: ci.batchThreshold,
	}
	ci.EmbeddingIndex.mu.RUnlock()
	return NewClusterIndex(manager, embeddings, &config)
}

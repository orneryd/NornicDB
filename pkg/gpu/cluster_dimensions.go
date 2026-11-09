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
	return ci.emptyLike(dimensions, true)
}

// Empty creates an empty cluster index with this index's dimensions, manager,
// synchronization policy and k-means settings, without preallocating its
// capacity (the replaced index may stay alive while queries finish on it).
// The original index is unchanged.
func (ci *ClusterIndex) Empty() *ClusterIndex {
	return ci.emptyLike(ci.dimensions, false)
}

func (ci *ClusterIndex) emptyLike(dimensions int, keepCapacity bool) *ClusterIndex {
	config := ci.Config()
	ci.EmbeddingIndex.mu.RLock()
	manager := ci.manager
	embeddings := &EmbeddingIndexConfig{
		Dimensions:     dimensions,
		AutoSync:       ci.autoSync,
		BatchThreshold: ci.batchThreshold,
	}
	if keepCapacity {
		embeddings.InitialCap = cap(ci.nodeIDs)
	}
	ci.EmbeddingIndex.mu.RUnlock()
	return NewClusterIndex(manager, embeddings, &config)
}

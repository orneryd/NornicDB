package search

func vectorSearchMethod(pipeline *VectorSearchPipeline) (string, string) {
	searchMethod := "vector"
	message := "Vector similarity search (cosine)"

	switch pipeline.candidateGen.(type) {
	case *KMeansCandidateGen:
		searchMethod = "vector_clustered"
		message = "K-means routed vector search"
	case *IVFHNSWCandidateGen:
		searchMethod = "vector_ivf_hnsw"
		message = "IVF-HNSW (centroid routing + per-cluster HNSW)"
	case *IVFPQCandidateGen:
		searchMethod = "vector_ivfpq"
		message = "IVFPQ compressed ANN"
	case *GPUBruteForceCandidateGen:
		searchMethod = "vector_gpu_brute"
		message = "GPU brute-force vector search (exact)"
	case *HNSWCandidateGen:
		searchMethod = "vector_hnsw"
		message = "HNSW approximate nearest neighbor search"
	case *BruteForceCandidateGen, *FileStoreBruteForceCandidateGen:
		searchMethod = "vector_brute"
		message = "CPU brute-force vector search (exact)"
	}

	return searchMethod, message
}

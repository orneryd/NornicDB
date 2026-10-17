package search

// publishVectorServing is the one owner of a change to what vector search
// serves: an HNSW graph, cluster index, cluster graphs or routing profiles, a
// compressed index, a GPU copy, or the retirement of any of them. install sets
// the new structures under their own locks; the cached pipeline is then
// retired when retirePipeline is set, so the next read resolves against the
// new structures; finally IndexRevision advances. A search keys its cached
// answer by the revision it read before computing (#945), so an answer
// computed before this change can only be cached under a revision no later
// search reads.
//
// The owner takes no lock of its own except pipelineMu, and only when
// retirePipeline is set: callers may then hold indexMu but not hnswMu,
// pipelineMu or mu (pipelineMu is taken after indexMu and never under
// hnswMu). A publication made while pipelineMu is held, by the pipeline being
// created, passes retirePipeline=false.
func (s *Service) publishVectorServing(install func(), retirePipeline bool) {
	install()
	if retirePipeline {
		s.pipelineMu.Lock()
		s.vectorPipeline = nil
		s.pipelineMu.Unlock()
	}
	s.indexChanged()
}

// cacheDerivedVectorPipelineLocked caches the pipeline resolved from the
// current structures. It changes nothing a search returns, so it does not
// advance IndexRevision. Caller holds pipelineMu.
func (s *Service) cacheDerivedVectorPipelineLocked(pipeline *VectorSearchPipeline) {
	s.vectorPipeline = pipeline
}

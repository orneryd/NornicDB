package search

import "github.com/orneryd/nornicdb/pkg/gpu"

// publishVectorServing is the one owner of a change to what vector search
// serves outside the cluster generation: the HNSW graph, the compressed index,
// the GPU copy, or the retirement of any of them. (The cluster generation has
// its own owner, which publishes through this one: publishClusterServing.)
// install sets the new structures under their own locks; the cached pipeline
// is then retired when retirePipeline is set, so the next read resolves
// against the new structures; finally IndexRevision advances. A search keys
// its cached answer by the revision it read before computing (#945), so an
// answer computed before this change can only be cached under a revision no
// later search reads.
//
// The owner takes no lock of its own except pipelineMu, and only when
// retirePipeline is set: callers must then hold no indexMu, clusterStateMu,
// mu or hnswMu, which pipeline construction takes under pipelineMu. A caller
// publishing under mutation admission takes pipelineMu before indexMu and
// retires the cached pipeline inside install with retirePipeline=false.
// A publication made under mu or
// hnswMu, or while pipelineMu is held (by the pipeline being created, or by
// clearIndexes), passes retirePipeline=false.
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
// current structures, or drops a cached one (nil). It changes nothing a
// search returns, so it does not advance IndexRevision. Caller holds
// pipelineMu.
func (s *Service) cacheDerivedVectorPipelineLocked(pipeline *VectorSearchPipeline) {
	s.vectorPipeline = pipeline
}

// The serving fields are read only through these accessors, which do no
// locking: the caller holds the field's lock as before. Everything else that
// mentions a serving field is a publication (publishVectorServing) or the
// derived-pipeline cache (TestServingFieldsAreWrittenOnlyByTheirOwner).

func (s *Service) servingHNSW() *HNSWIndex { return s.hnswIndex }

func (s *Service) servingPipeline() *VectorSearchPipeline { return s.vectorPipeline }

func (s *Service) servingIVFPQ() *IVFPQIndex { return s.ivfpqIndex }

func (s *Service) servingGPUIndex() *gpu.EmbeddingIndex { return s.gpuEmbeddingIndex }

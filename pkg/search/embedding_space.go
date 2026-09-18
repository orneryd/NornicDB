package search

import "github.com/orneryd/nornicdb/pkg/storage"

// SetEmbeddingSpace selects the managed vector space for this database search
// service. Configure it before building indexes. Named and property vectors
// retain their existing Neo4j-compatible behavior.
func (s *Service) SetEmbeddingSpace(space string) {
	current, loaded := s.embeddingSpace.Load().(string)
	if loaded && current == space {
		return
	}
	s.embeddingSpace.Store(space)
	if s.resultCache != nil {
		s.resultCache.Invalidate()
	}
}

func (s *Service) managedEmbeddingEligible(node *storage.Node) bool {
	expected, configured := s.embeddingSpace.Load().(string)
	if !configured {
		return true
	}
	actual, _ := node.EmbedMeta["embedding_space"].(string)
	return actual == expected
}

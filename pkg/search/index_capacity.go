package search

import (
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ErrIndexMemoryBudgetExceeded identifies a per-database index capacity rejection.
var ErrIndexMemoryBudgetExceeded = errors.New("search: index memory budget exceeded")

type indexCapacityUsage struct {
	bm25Resident   int64
	vectorResident int64
	bm25Metadata   int64
	vectorMetadata int64
}

// nodeFulltext is the BM25 text one IndexNode call writes for a node,
// extracted once and shared by the capacity check and the index update.
type nodeFulltext struct {
	index     bool   // the call updates BM25 for this node
	text      string // the node's searchable text
	unchanged bool   // the index already holds exactly this text for the node
}

func (s *Service) checkNodeIndexCapacityLocked(node *storage.Node, fulltext nodeFulltext) (indexCapacityUsage, error) {
	usage := s.estimateNodeIndexCapacityLocked(node, fulltext)
	previous := s.indexCapacityByNode[string(node.ID)]
	checks := []struct {
		name     string
		current  int64
		previous int64
		next     int64
		limit    int64
	}{
		{name: "bm25", current: s.bm25ResidentBytes, previous: previous.bm25Resident, next: usage.bm25Resident, limit: s.bm25MemoryMaxBytes},
		{name: "vector", current: s.vectorResidentBytes, previous: previous.vectorResident, next: usage.vectorResident, limit: s.vectorMemoryMaxBytes},
		{name: "bm25 metadata", current: s.bm25MetadataBytes, previous: previous.bm25Metadata, next: usage.bm25Metadata, limit: s.metadataMemoryMaxBytes},
		{name: "vector metadata", current: s.vectorMetadataBytes, previous: previous.vectorMetadata, next: usage.vectorMetadata, limit: s.metadataMemoryMaxBytes},
	}
	for _, check := range checks {
		projected := check.current - check.previous + check.next
		if check.limit > 0 && projected > check.limit {
			return indexCapacityUsage{}, fmt.Errorf("%w: %s requires %d bytes, limit is %d", ErrIndexMemoryBudgetExceeded, check.name, projected, check.limit)
		}
	}
	return usage, nil
}

// estimateNodeIndexCapacityLocked sizes the index entries node needs. Text the
// index already holds keeps its recorded BM25 size instead of being analyzed
// again.
func (s *Service) estimateNodeIndexCapacityLocked(node *storage.Node, fulltext nodeFulltext) indexCapacityUsage {
	if node == nil {
		return indexCapacityUsage{}
	}
	usage := indexCapacityUsage{}
	nodeID := string(node.ID)
	previous, recorded := s.indexCapacityByNode[nodeID]
	if fulltext.index && fulltext.unchanged && recorded {
		usage.bm25Resident = previous.bm25Resident
		usage.bm25Metadata = previous.bm25Metadata
	} else if fulltext.index {
		text := fulltext.text
		usage.bm25Resident = int64(len(text))
		if text != "" {
			usage.bm25Metadata = int64(len(nodeID) + 16)
			seenTerms := make(map[string]struct{})
			for _, term := range normalizeAnalyzer(s.bm25Analyzer).Analyze(text) {
				if _, exists := seenTerms[term]; exists {
					continue
				}
				seenTerms[term] = struct{}{}
				usage.bm25Metadata += int64(len(term) + 8)
			}
		}
	}
	if !s.vectorEnabled.Load() {
		return usage
	}
	usage.vectorMetadata = int64(len(nodeID) + 16)
	for _, label := range node.Labels {
		usage.vectorMetadata += int64(len(label) + 8)
	}
	dimensions := 0
	if s.vectorIndex != nil {
		dimensions = s.vectorIndex.GetDimensions()
	}
	diskPayload := s.vectorFileStore != nil || s.vectorStorageMode == "disk"
	addVector := func(id string, vector []float32) {
		if len(vector) == 0 || (dimensions > 0 && len(vector) != dimensions) {
			return
		}
		usage.vectorMetadata += int64(len(id) + 16)
		if !diskPayload {
			usage.vectorResident += int64(len(vector) * 2 * 4)
		}
	}
	for name, vector := range node.NamedEmbeddings {
		addVector(fmt.Sprintf("%s-named-%s", node.ID, name), vector)
	}
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		addVector(nodeID, node.ChunkEmbeddings[0])
		if len(node.ChunkEmbeddings) > 1 {
			for index, vector := range node.ChunkEmbeddings {
				addVector(fmt.Sprintf("%s-chunk-%d", node.ID, index), vector)
			}
		}
	}
	for property, value := range node.Properties {
		if vector, ok := vectorFromPropertyValue(value, dimensions); ok {
			addVector(fmt.Sprintf("%s-prop-%s", node.ID, property), vector)
		}
	}
	return usage
}

func (s *Service) commitNodeIndexCapacityLocked(nodeID string, usage indexCapacityUsage) {
	previous := s.indexCapacityByNode[nodeID]
	s.bm25ResidentBytes += usage.bm25Resident - previous.bm25Resident
	s.vectorResidentBytes += usage.vectorResident - previous.vectorResident
	s.bm25MetadataBytes += usage.bm25Metadata - previous.bm25Metadata
	s.vectorMetadataBytes += usage.vectorMetadata - previous.vectorMetadata
	s.indexCapacityByNode[nodeID] = usage
}

func (s *Service) releaseNodeIndexCapacityLocked(nodeID string) {
	previous, exists := s.indexCapacityByNode[nodeID]
	if !exists {
		return
	}
	s.bm25ResidentBytes -= previous.bm25Resident
	s.vectorResidentBytes -= previous.vectorResident
	s.bm25MetadataBytes -= previous.bm25Metadata
	s.vectorMetadataBytes -= previous.vectorMetadata
	delete(s.indexCapacityByNode, nodeID)
}

func (s *Service) checkEdgeIndexCapacityLocked(edge *storage.Edge, vectors map[string][]float32) (indexCapacityUsage, error) {
	usage := indexCapacityUsage{}
	if len(vectors) > 0 {
		usage.vectorMetadata = int64(len(edge.ID) + len(edge.Type) + 16)
		for property, vector := range vectors {
			usage.vectorResident += int64(len(vector) * 4)
			usage.vectorMetadata += int64(len(property) + 16)
		}
	}
	previous := s.indexCapacityByEdge[string(edge.ID)]
	projectedResident := s.vectorResidentBytes - previous.vectorResident + usage.vectorResident
	if s.vectorMemoryMaxBytes > 0 && projectedResident > s.vectorMemoryMaxBytes {
		return indexCapacityUsage{}, fmt.Errorf("%w: relationship vector requires %d bytes, limit is %d", ErrIndexMemoryBudgetExceeded, projectedResident, s.vectorMemoryMaxBytes)
	}
	projectedMetadata := s.vectorMetadataBytes - previous.vectorMetadata + usage.vectorMetadata
	if s.metadataMemoryMaxBytes > 0 && projectedMetadata > s.metadataMemoryMaxBytes {
		return indexCapacityUsage{}, fmt.Errorf("%w: relationship vector metadata requires %d bytes, limit is %d", ErrIndexMemoryBudgetExceeded, projectedMetadata, s.metadataMemoryMaxBytes)
	}
	return usage, nil
}

func (s *Service) commitEdgeIndexCapacityLocked(edgeID string, usage indexCapacityUsage) {
	previous := s.indexCapacityByEdge[edgeID]
	s.vectorResidentBytes += usage.vectorResident - previous.vectorResident
	s.vectorMetadataBytes += usage.vectorMetadata - previous.vectorMetadata
	s.indexCapacityByEdge[edgeID] = usage
}

func (s *Service) releaseEdgeIndexCapacityLocked(edgeID string) {
	previous, exists := s.indexCapacityByEdge[edgeID]
	if !exists {
		return
	}
	s.vectorResidentBytes -= previous.vectorResident
	s.vectorMetadataBytes -= previous.vectorMetadata
	delete(s.indexCapacityByEdge, edgeID)
}

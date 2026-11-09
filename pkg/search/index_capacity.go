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
	index     bool           // the call updates BM25 for this node
	text      string         // the node's searchable text
	unchanged bool           // the index already holds exactly this text for the node
	terms     []string       // analyzed once for both capacity and the index write
	frequency map[string]int // the same distinct terms and counts the index will write
}

// nodeIndexUsageLocked is the BM25 and vector memory indexing node takes:
// its text, and its vectors with their metadata (the node's identity and
// labels, and each entry's key).
func (s *Service) nodeIndexUsageLocked(node *storage.Node, fulltext nodeFulltext) indexCapacityUsage {
	// Text the index already holds keeps its recorded BM25 size instead of
	// being analyzed again.
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
			for term := range fulltext.frequency {
				usage.bm25Metadata += int64(len(term) + 8)
			}
		}
	}
	if s.vectorEnabled.Load() {
		usage.vectorMetadata = int64(len(nodeID) + 16)
		for _, label := range node.Labels {
			usage.vectorMetadata += int64(len(label) + 8)
		}
		dimensions := 0
		s.mu.RLock()
		if s.vectorIndex != nil {
			dimensions = s.vectorIndex.GetDimensions()
		}
		diskPayload := s.vectorFileStore != nil || s.vectorStorageMode == "disk"
		s.mu.RUnlock()
		s.forEachNodeVectorEntry(node, dimensions, func(item nodeVectorEntry) bool {
			if dimensions > 0 && len(item.vector) != dimensions {
				return true
			}
			usage.vectorMetadata += int64(len(item.key) + 16)
			if !diskPayload {
				usage.vectorResident += int64(len(item.vector) * 2 * 4)
			}
			return true
		})
	}
	return usage
}

// indexCapacityLocked is the one budget decision for the search entries of a
// node or a relationship: usage, the memory its entries take, replacing
// previous, is checked against the index memory budgets. text is the first
// BM25 budget it exceeds, vector the first vector budget, and first the first
// of all, in the order a caller's write reports it. A part the write leaves
// unchanged cannot exceed a budget.
func (s *Service) indexCapacityLocked(previous, usage indexCapacityUsage) (text, vector, first error) {
	checks := []struct {
		name     string
		text     bool
		current  int64
		previous int64
		next     int64
		limit    int64
	}{
		{name: "bm25", text: true, current: s.bm25ResidentBytes, previous: previous.bm25Resident, next: usage.bm25Resident, limit: s.bm25MemoryMaxBytes},
		{name: "vector", current: s.vectorResidentBytes, previous: previous.vectorResident, next: usage.vectorResident, limit: s.vectorMemoryMaxBytes},
		{name: "bm25 metadata", text: true, current: s.bm25MetadataBytes, previous: previous.bm25Metadata, next: usage.bm25Metadata, limit: s.metadataMemoryMaxBytes},
		{name: "vector metadata", current: s.vectorMetadataBytes, previous: previous.vectorMetadata, next: usage.vectorMetadata, limit: s.metadataMemoryMaxBytes},
	}
	for _, check := range checks {
		projected := check.current - check.previous + check.next
		if check.limit <= 0 || check.next == check.previous || projected <= check.limit {
			continue
		}
		err := fmt.Errorf("%w: %s requires %d bytes, limit is %d", ErrIndexMemoryBudgetExceeded, check.name, projected, check.limit)
		if first == nil {
			first = err
		}
		if check.text && text == nil {
			text = err
		} else if !check.text && vector == nil {
			vector = err
		}
	}
	return text, vector, first
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
	if _, _, first := s.indexCapacityLocked(s.indexCapacityByEdge[string(edge.ID)], usage); first != nil {
		return indexCapacityUsage{}, first
	}
	return usage, nil
}

// admittedUsage is the one decision for the stored state of a node or a
// relationship that an index memory budget refuses (indexCapacityLocked
// reports which kind): a refused kind, the BM25 text or the vectors with
// their metadata, is withdrawn whole. Nothing of it stays and nothing of it
// is installed, so the index serves storage's image of the kind or none of
// it, never part of one image or another representation in its place. It is
// the usage of what remains, which the install charges.
func admittedUsage(usage indexCapacityUsage, text, vector error) indexCapacityUsage {
	if text != nil {
		usage.bm25Resident, usage.bm25Metadata = 0, 0
	}
	if vector != nil {
		usage.vectorResident, usage.vectorMetadata = 0, 0
	}
	return usage
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

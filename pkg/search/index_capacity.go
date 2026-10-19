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
	return usage, s.checkIndexCapacityUsageLocked(previous, usage)
}

// checkIndexCapacityUsageLocked is shared by live admission and persisted
// population replacement. Caller holds indexMu.
func (s *Service) checkIndexCapacityUsageLocked(previous, usage indexCapacityUsage) error {
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
			return fmt.Errorf("%w: %s requires %d bytes, limit is %d", ErrIndexMemoryBudgetExceeded, check.name, projected, check.limit)
		}
	}
	return nil
}

func (s *Service) estimateFulltextCapacity(nodeID, text string) indexCapacityUsage {
	usage := indexCapacityUsage{bm25Resident: int64(len(text))}
	if text != "" {
		usage.bm25Metadata = int64(len(nodeID) + 16)
		seenTerms := make(map[string]struct{})
		for _, term := range normalizeAnalyzer(s.bm25Analyzer).Analyze(text) {
			if _, exists := seenTerms[term]; !exists {
				seenTerms[term] = struct{}{}
				usage.bm25Metadata += int64(len(term) + 8)
			}
		}
	}
	return usage
}

func nodeVectorMetadataBytes(nodeID string, labels []string) int64 {
	bytes := int64(len(nodeID) + 16)
	for _, label := range labels {
		bytes += int64(len(label) + 8)
	}
	return bytes
}

func vectorEntryMetadataBytes(id string) int64 { return int64(len(id) + 16) }

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
		usage = s.estimateFulltextCapacity(nodeID, fulltext.text)
	}
	if !s.vectorEnabled.Load() {
		return usage
	}
	usage.vectorMetadata = nodeVectorMetadataBytes(nodeID, node.Labels)
	dimensions := 0
	if s.vectorIndex != nil {
		dimensions = s.vectorIndex.GetDimensions()
	}
	// Policy alone is not a storage destination: persistence-off and a failed
	// file creation both write to memory, including disk-configured services.
	diskPayload := s.vectorFileStore != nil
	s.forEachNodeVectorEntry(node, dimensions, func(item nodeVectorEntry) bool {
		if dimensions > 0 && len(item.vector) != dimensions {
			return true
		}
		usage.vectorMetadata += vectorEntryMetadataBytes(item.key)
		if !diskPayload {
			usage.vectorResident += vectorPayloadBytes(item.vector)
		}
		return true
	})
	return usage
}

// restoreNodeIndexCapacityLocked follows selection of the active base sources.
// It accounts for their installed populations without copying vector payloads
// or iterating graph storage. Fulltext charges use the same
// estimator as ingestion. Even an over-budget loaded population stays charged;
// the caller fails the build before ready, and later writes cannot evade it.
// Caller holds indexMu and has released mu.
func (s *Service) restoreNodeIndexCapacityLocked() error {
	for nodeID := range s.indexCapacityByNode {
		s.releaseNodeIndexCapacityLocked(nodeID)
	}
	// Clear can leave fulltext documents outside every vector metadata map.
	// Reconstruct each installed population independently, merging its charges
	// into the same node record. Do not invent removed vector metadata.
	s.fulltext().forEachDocument(func(nodeID, text string) {
		s.commitNodeIndexCapacityLocked(nodeID, s.estimateFulltextCapacity(nodeID, text))
	})
	for nodeID, labels := range s.nodeLabels {
		usage := s.indexCapacityByNode[nodeID]
		usage.vectorMetadata = nodeVectorMetadataBytes(nodeID, labels)
		s.nodeVectorIDsLocked(nodeID, func(id string) {
			present := false
			if s.vectorFileStore != nil {
				present = s.vectorFileStore.Has(id)
			} else if s.vectorIndex != nil {
				present = s.vectorIndex.HasVector(id)
			}
			if present {
				usage.vectorMetadata += vectorEntryMetadataBytes(id)
			}
		})
		s.commitNodeIndexCapacityLocked(nodeID, usage)
	}
	// Reuse source reconciliation for every actual memory entry, including
	// legacy IDs without query metadata; disk coordinates remain nonresident.
	var memory *VectorIndex
	if s.vectorFileStore == nil {
		memory = s.vectorIndex
	}
	s.reconcileVectorSourceCapacityLocked(memory)
	// The complete replacement is now reflected in the counters. Check those
	// counters through the same projected-usage policy as live admission.
	return s.checkIndexCapacityUsageLocked(indexCapacityUsage{}, indexCapacityUsage{})
}

// vectorPayloadBytes uses the same sparse raw-copy decision as VectorIndex.Add.
func vectorPayloadBytes(value []float32) int64 {
	bytes := int64(len(value)) * 4
	if rawVectorNeeded(value) {
		bytes *= 2
	}
	return bytes
}

// nodeVectorIDsLocked visits the IDs written by the canonical ingestion path.
// Main/chunk zero share one entry. Caller holds indexMu.
func (s *Service) nodeVectorIDsLocked(nodeID string, visit func(string)) {
	mainKey := nodeMainVectorKey(nodeID)
	visit(mainKey)
	for _, id := range s.nodeChunkVectors[nodeID] {
		if id != mainKey {
			visit(id)
		}
	}
	for _, id := range s.nodeNamedVector[nodeID] {
		visit(id)
	}
	for _, id := range s.nodePropVector[nodeID] {
		visit(id)
	}
}

// nodeVectorResidentLocked records the actual destination after ingestion,
// which may have migrated from memory to a file after its admission check.
func (s *Service) nodeVectorResidentLocked(nodeID string) int64 {
	if s.vectorFileStore != nil || s.vectorIndex == nil {
		return 0
	}
	vi := s.vectorIndex
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	var bytes int64
	s.nodeVectorIDsLocked(nodeID, func(id string) {
		bytes += int64(len(vi.vectors[id])+len(vi.rawVectors[id])) * 4
	})
	return bytes
}

// vectorNonNodeResidentLocked preserves relationship and other resident usage
// when replacing the complete node source. Caller holds indexMu.
func (s *Service) vectorNonNodeResidentLocked() int64 {
	bytes := s.vectorResidentBytes
	for _, usage := range s.indexCapacityByNode {
		bytes -= usage.vectorResident
	}
	return bytes
}

func (s *Service) checkVectorSourceCapacityLocked(store *VectorFileStore) error {
	if s.vectorMemoryMaxBytes <= 0 {
		return nil
	}
	available := s.vectorMemoryMaxBytes - s.vectorNonNodeResidentLocked()
	if available < 0 || int64(store.Count()) > available/int64(store.GetDimensions())/4 {
		return fmt.Errorf("%w: transferred vectors and resident relationship payload exceed %d bytes", ErrIndexMemoryBudgetExceeded, s.vectorMemoryMaxBytes)
	}
	return nil
}

// reconcileVectorSourceCapacityLocked moves payload charges with the source.
// A file destination has no resident node payload. A memory destination counts
// the actual normalized/raw slices once, including loaded IDs without a prior
// live-ingestion capacity record. Only ID/size metadata is allocated here.
func (s *Service) reconcileVectorSourceCapacityLocked(memory *VectorIndex) {
	resident := s.vectorNonNodeResidentLocked()
	for id, usage := range s.indexCapacityByNode {
		usage.vectorResident = 0
		s.indexCapacityByNode[id] = usage
	}
	if memory == nil {
		s.vectorResidentBytes = resident
		return
	}
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	owners := make(map[string]string, len(memory.vectors))
	for id := range s.nodeLabels {
		s.nodeVectorIDsLocked(id, func(vectorID string) { owners[vectorID] = id })
	}
	for id := range s.indexCapacityByNode {
		s.nodeVectorIDsLocked(id, func(vectorID string) { owners[vectorID] = id })
	}
	for id, value := range memory.vectors {
		owner, ok := owners[id]
		if !ok {
			if owner, ok = vectorEntryOwner(id); !ok {
				owner = id // an undecodable key is still resident payload
			}
		}
		bytes := int64(len(value)+len(memory.rawVectors[id])) * 4
		usage := s.indexCapacityByNode[owner]
		usage.vectorResident += bytes
		s.indexCapacityByNode[owner] = usage
		resident += bytes
	}
	s.vectorResidentBytes = resident
}

func (s *Service) commitNodeIndexCapacityLocked(nodeID string, usage indexCapacityUsage) {
	previous := s.indexCapacityByNode[nodeID]
	s.bm25ResidentBytes += usage.bm25Resident - previous.bm25Resident
	s.vectorResidentBytes += usage.vectorResident - previous.vectorResident
	s.bm25MetadataBytes += usage.bm25Metadata - previous.bm25Metadata
	s.vectorMetadataBytes += usage.vectorMetadata - previous.vectorMetadata
	s.indexCapacityByNode[nodeID] = usage
}

// releaseNodeVectorCapacityLocked follows removal of one stored/tracked vector,
// preserving the node's other vectors and fulltext charges. Whole-node removal
// releases its complete record instead. Caller holds indexMu.
func (s *Service) releaseNodeVectorCapacityLocked(nodeID, vectorID string) {
	usage, recorded := s.indexCapacityByNode[nodeID]
	if !recorded {
		return
	}
	usage.vectorResident = s.nodeVectorResidentLocked(nodeID)
	// Loaded sources can have payload records without metadata charges. Never
	// subtract an entry charge that this ledger did not previously retain.
	usage.vectorMetadata -= min(usage.vectorMetadata, vectorEntryMetadataBytes(vectorID))
	s.commitNodeIndexCapacityLocked(nodeID, usage)
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

// clearVectorIndexCapacityLocked follows destructive vector/metadata clearing,
// unlike a source move that preserves metadata. Fulltext survives this operation
// and keeps its existing charges until normal node replacement or removal.
// Caller holds indexMu.
func (s *Service) clearVectorIndexCapacityLocked() {
	for nodeID, usage := range s.indexCapacityByNode {
		if usage.bm25Resident == 0 && usage.bm25Metadata == 0 {
			s.releaseNodeIndexCapacityLocked(nodeID)
			continue
		}
		usage.vectorResident = 0
		usage.vectorMetadata = 0
		s.commitNodeIndexCapacityLocked(nodeID, usage)
	}
	for edgeID := range s.indexCapacityByEdge {
		s.releaseEdgeIndexCapacityLocked(edgeID)
	}
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

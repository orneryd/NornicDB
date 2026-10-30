package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
	"unsafe"
)

// enrichResults converts RRF results to SearchResult with full node data.
func (s *Service) enrichResults(ctx context.Context, rrfResults []rrfResult, opts *SearchOptions, seenOrphans map[string]bool, query hitMatchQuery) []SearchResult {
	resultLimit := boundedResultLimit(len(rrfResults), opts.Limit)
	if resultLimit == 0 {
		return nil
	}
	ctx, visible, release := storage.BeginQueryResultProjectionContext(ctx, s.engine)
	defer release()
	ids := make([]storage.NodeID, resultLimit)
	for index := 0; index < resultLimit; index++ {
		ids[index] = storage.NodeID(rrfResults[index].ID)
	}
	nodes, ok := s.hitNodes(ctx, ids, seenOrphans)
	if !ok {
		return nil
	}
	projection := newResultPropertyProjection(opts.IncludeProperties, opts.ExcludeProperties)
	results := make([]SearchResult, 0, resultLimit)
	found := make([]*storage.Node, 0, resultLimit)
	for index, node := range nodes {
		node = visible.Node(node)
		if node == nil {
			continue
		}
		rrf := rrfResults[index]
		result := searchResultFromNode(rrf.ID, node, projection)
		result.Score = rrf.RRFScore
		result.Similarity = rrf.OriginalScore
		result.RRFScore = rrf.RRFScore
		result.VectorRank = rrf.VectorRank
		result.BM25Rank = rrf.BM25Rank
		results = append(results, result)
		found = append(found, node)
	}
	s.attachSearchMatches(ctx, results, found, query, opts)
	if visible.Enabled() {
		for i := range results {
			results[i].Matches = visibleSearchMatches(results[i].Matches, found[i].Properties)
		}
	}
	return results
}

// enrichIndexResults converts raw index results to SearchResult.
// Every result ID is already an owning node ID: vector results were decoded
// once at collapse, and fulltext document IDs are node IDs. Nothing here decodes.
func (s *Service) enrichIndexResults(ctx context.Context, indexResults []indexResult, opts *SearchOptions, seenOrphans map[string]bool, query hitMatchQuery) []SearchResult {
	resultLimit := boundedResultLimit(len(indexResults), opts.Limit)
	if resultLimit == 0 {
		return nil
	}
	ctx, visible, release := storage.BeginQueryResultProjectionContext(ctx, s.engine)
	defer release()
	projection := newResultPropertyProjection(opts.IncludeProperties, opts.ExcludeProperties)

	ids := make([]storage.NodeID, 0, resultLimit)
	scores := make([]float64, 0, resultLimit)
	var seen map[string]struct{}
	for _, ir := range indexResults {
		if len(ids) >= resultLimit {
			break
		}

		nodeIDStr := ir.ID
		if seen != nil {
			if _, exists := seen[nodeIDStr]; exists {
				continue
			}
			seen[nodeIDStr] = struct{}{}
		} else {
			duplicate := false
			for _, id := range ids {
				if string(id) == nodeIDStr {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if len(ids) >= 64 {
				seen = make(map[string]struct{}, len(ids)+1)
				for _, id := range ids {
					seen[string(id)] = struct{}{}
				}
				seen[nodeIDStr] = struct{}{}
			}
		}
		ids = append(ids, storage.NodeID(nodeIDStr))
		scores = append(scores, ir.Score)
	}
	nodes, ok := s.hitNodes(ctx, ids, seenOrphans)
	if !ok {
		return nil
	}
	results := make([]SearchResult, 0, len(ids))
	found := make([]*storage.Node, 0, len(ids))
	for index, node := range nodes {
		node = visible.Node(node)
		if node == nil {
			continue
		}
		result := searchResultFromNode(string(ids[index]), node, projection) // the node ID, not a chunk ID
		result.Score = scores[index]
		result.Similarity = scores[index]
		results = append(results, result)
		found = append(found, node)
	}
	s.attachSearchMatches(ctx, results, found, query, opts)
	if visible.Enabled() {
		for i := range results {
			results[i].Matches = visibleSearchMatches(results[i].Matches, found[i].Properties)
		}
	}
	return results
}

// hitNodes reads the nodes of the hits ids, without their embeddings. A
// node that is gone is nil, and handled as an orphaned embedding; false
// when the batch read fails.
func (s *Service) hitNodes(ctx context.Context, ids []storage.NodeID, seenOrphans map[string]bool) ([]*storage.Node, bool) {
	nodes := make([]*storage.Node, len(ids))
	if s.supportsBatchNodesWithoutEmbeddings() {
		batch, err := s.batchGetNodesWithoutEmbeddings(ids)
		if err != nil {
			return nil, false
		}
		for index, id := range ids {
			if nodes[index] = batch[id]; nodes[index] == nil {
				_ = s.handleOrphanedEmbedding(ctx, string(id), storage.ErrNotFound, seenOrphans)
			}
		}
		return nodes, true
	}
	for index, id := range ids {
		node, err := s.getNodeWithoutEmbeddings(id)
		if err != nil {
			_ = s.handleOrphanedEmbedding(ctx, string(id), err, seenOrphans)
			continue
		}
		nodes[index] = node
	}
	return nodes, true
}

// searchResultFromNode is the result of a hit on node: its identity,
// labels, projected properties, and the display fields read from them.
func searchResultFromNode(id string, node *storage.Node, projection resultPropertyProjection) SearchResult {
	result := SearchResult{
		ID:         id,
		NodeID:     node.ID,
		Labels:     node.Labels,
		Properties: projection.apply(node.Properties),
	}
	result.sourceNode = node
	if len(node.ChunkEmbeddings) > 0 || len(node.NamedEmbeddings) > 0 || len(projection.include) > 0 || len(projection.exclude) > 0 {
		// A fallback reader may return vectors; an explicit projection may leave
		// large excluded properties behind. Retain metadata without either payload.
		metadata := *node
		metadata.Properties = nil
		metadata.ChunkEmbeddings = nil
		metadata.NamedEmbeddings = nil
		result.sourceNode = &metadata
	}
	if t, ok := node.Properties["type"].(string); ok {
		result.Type = t
	}
	if title, ok := node.Properties["title"].(string); ok {
		result.Title = title
	}
	if desc, ok := node.Properties["description"].(string); ok {
		result.Description = desc
	}
	if content, ok := node.Properties["content"].(string); ok {
		result.ContentPreview = truncate(content, 200)
	} else if text, ok := node.Properties["text"].(string); ok {
		result.ContentPreview = truncate(text, 200)
	}
	return result
}

// MaterializedNode returns a detached logical node for a search hit. Preserve
// the source timestamps: later property-visibility checks must score the node
// that was read, rather than a reconstruction with a zero creation time.
// Managed vectors stay in storage; a user vector property remains a property.
// A database context supplies provenance only when the result has no source
// database, as with compact continuation results. Existing provenance wins.
func (result SearchResult) MaterializedNode(database ...string) *storage.Node {
	var node storage.Node
	if result.sourceNode != nil {
		node = *result.sourceNode
	}
	if node.RuntimeMetadata().Database == "" && len(database) > 0 {
		{
			metadata := node.RuntimeMetadata()
			metadata.Database = database[0]
			node.SetRuntimeMetadata(metadata)
		}
	}
	node.ID = result.NodeID
	if node.ID == "" {
		node.ID = storage.NodeID(result.ID)
	}
	node.Labels = result.Labels
	node.Properties = result.Properties
	node.ChunkEmbeddings = nil
	node.NamedEmbeddings = nil
	return &node
}

func (result SearchResult) nodeMetadataBytes() int64 {
	if result.sourceNode == nil {
		return 0
	}
	return int64(unsafe.Sizeof(storage.Node{})) + util.EstimateValueBytes(result.sourceNode.EmbedMeta)
}

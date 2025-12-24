package server

import (
	"context"
	"runtime"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// ==========================================================================
// Heimdall Database/Metrics Wrappers
// ==========================================================================

// heimdallDBReader wraps NornicDB for Heimdall's DatabaseReader interface.
type heimdallDBReader struct {
	db       *nornicdb.DB
	features *nornicConfig.FeatureFlagsConfig
}

func (r *heimdallDBReader) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	result, err := r.db.ExecuteCypher(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	// Convert result to []map[string]interface{}
	var rows []map[string]interface{}
	for _, row := range result.Rows {
		rowMap := make(map[string]interface{})
		for i, col := range result.Columns {
			if i < len(row) {
				rowMap[col] = row[i]
			}
		}
		rows = append(rows, rowMap)
	}
	return rows, nil
}

func (r *heimdallDBReader) Stats() heimdall.DatabaseStats {
	stats := r.db.Stats()
	result := heimdall.DatabaseStats{
		NodeCount:         stats.NodeCount,
		RelationshipCount: stats.EdgeCount,
		LabelCounts:       make(map[string]int64), // Label counts not yet implemented (future enhancement)
	}

	// Add search/cluster stats if available
	if searchStats := r.db.GetSearchStats(); searchStats != nil {
		result.ClusterStats = &heimdall.ClusterStats{
			NumClusters:    searchStats.NumClusters,
			EmbeddingCount: searchStats.EmbeddingCount,
			IsClustered:    searchStats.IsClustered,
			AvgClusterSize: searchStats.AvgClusterSize,
			Iterations:     searchStats.ClusterIterations,
		}
	}

	// Add feature flags
	if r.features != nil {
		// Derive clustering enabled from search stats
		clusteringEnabled := false
		if result.ClusterStats != nil {
			clusteringEnabled = result.ClusterStats.NumClusters > 0 || result.ClusterStats.EmbeddingCount > 0
		}

		result.FeatureFlags = &heimdall.FeatureFlags{
			HeimdallEnabled:          r.features.HeimdallEnabled,
			HeimdallAnomalyDetection: r.features.HeimdallAnomalyDetection,
			HeimdallRuntimeDiagnosis: r.features.HeimdallRuntimeDiagnosis,
			HeimdallMemoryCuration:   r.features.HeimdallMemoryCuration,
			ClusteringEnabled:        clusteringEnabled,
			TopologyEnabled:          r.features.TopologyAutoIntegrationEnabled,
			KalmanEnabled:            r.features.KalmanEnabled,
			AsyncWritesEnabled:       r.db.IsAsyncWritesEnabled(),
		}
	}

	return result
}

// Discover implements semantic search with graph traversal for Graph-RAG.
func (r *heimdallDBReader) Discover(ctx context.Context, query string, nodeTypes []string, limit int, depth int) (*heimdall.DiscoverResult, error) {
	// Use text search (BM25) since we don't have embedder access here
	// For vector search, the full MCP server should be used
	dbResults, err := r.db.Search(ctx, query, nodeTypes, limit)
	if err != nil {
		return nil, err
	}

	results := make([]heimdall.SearchResult, 0, len(dbResults))
	for _, dbr := range dbResults {
		result := heimdall.SearchResult{
			ID:         dbr.Node.ID,
			Type:       getFirstLabel(dbr.Node.Labels),
			Title:      getStringProperty(dbr.Node.Properties, "title"),
			Similarity: dbr.Score,
			Properties: dbr.Node.Properties,
		}

		// Content preview
		if content := getStringProperty(dbr.Node.Properties, "content"); content != "" {
			if len(content) > 200 {
				result.ContentPreview = content[:200] + "..."
			} else {
				result.ContentPreview = content
			}
		}

		// Get related nodes if depth > 1
		if depth > 1 {
			result.Related = r.getRelatedNodes(ctx, dbr.Node.ID, depth)
		}

		results = append(results, result)
	}

	return &heimdall.DiscoverResult{
		Results: results,
		Method:  "keyword",
		Total:   len(results),
	}, nil
}

// getRelatedNodes fetches connected nodes up to the specified depth.
func (r *heimdallDBReader) getRelatedNodes(ctx context.Context, nodeID string, depth int) []heimdall.RelatedNode {
	if depth < 1 {
		return nil
	}

	var related []heimdall.RelatedNode
	visited := make(map[string]bool)
	visited[nodeID] = true

	// BFS traversal
	type queueItem struct {
		id       string
		distance int
	}
	queue := []queueItem{{id: nodeID, distance: 0}}

	for len(queue) > 0 && len(related) < 50 {
		current := queue[0]
		queue = queue[1:]

		if current.distance >= depth {
			continue
		}

		// Get neighbors (depth=1 for immediate neighbors, "" for any edge type)
		neighbors, err := r.db.Neighbors(ctx, current.id, 1, "")
		if err != nil {
			continue
		}

		// Get edges for relationship info
		edges, err := r.db.GetEdgesForNode(ctx, current.id)
		if err != nil {
			edges = nil
		}

		// Build edge lookup by neighbor ID
		edgeMap := make(map[string]*nornicdb.GraphEdge)
		for _, edge := range edges {
			if edge.Source == current.id {
				edgeMap[edge.Target] = edge
			} else {
				edgeMap[edge.Source] = edge
			}
		}

		for _, neighbor := range neighbors {
			neighborID := neighbor.ID
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Get node info
			node, err := r.db.GetNode(ctx, neighborID)
			if err != nil {
				continue
			}

			rel := heimdall.RelatedNode{
				ID:       neighborID,
				Type:     getFirstLabel(node.Labels),
				Title:    getStringProperty(node.Properties, "title"),
				Distance: current.distance + 1,
			}

			// Get relationship info from edge
			if edge, ok := edgeMap[neighborID]; ok {
				rel.Relationship = edge.Type
				if edge.Source == current.id {
					rel.Direction = "outgoing"
				} else {
					rel.Direction = "incoming"
				}
			}

			related = append(related, rel)

			// Add to queue for next level
			if current.distance+1 < depth {
				queue = append(queue, queueItem{id: neighborID, distance: current.distance + 1})
			}
		}
	}

	return related
}

// getFirstLabel returns the first label or empty string.
func getFirstLabel(labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return ""
}

// getStringProperty extracts a string property from a map.
func getStringProperty(props map[string]interface{}, key string) string {
	if props == nil {
		return ""
	}
	if v, ok := props[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// heimdallMetricsReader provides runtime metrics for Heimdall.
type heimdallMetricsReader struct{}

func (r *heimdallMetricsReader) Runtime() heimdall.RuntimeMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return heimdall.RuntimeMetrics{
		GoroutineCount: runtime.NumGoroutine(),
		MemoryAllocMB:  m.Alloc / 1024 / 1024,
		NumGC:          m.NumGC,
	}
}

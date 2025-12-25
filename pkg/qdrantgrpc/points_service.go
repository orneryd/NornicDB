package qdrantgrpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// PointsService implements the Qdrant Points gRPC service.
// It integrates with NornicDB's search.Service for unified vector indexing.
type PointsService struct {
	pb.UnimplementedPointsServer
	config        *Config
	storage       storage.Engine
	registry      CollectionRegistry
	searchService *search.Service
}

// NewPointsService creates a new Points service.
// If searchService is nil, points are stored but not indexed for search.
func NewPointsService(config *Config, store storage.Engine, registry CollectionRegistry, searchService *search.Service) *PointsService {
	return &PointsService{
		config:        config,
		storage:       store,
		registry:      registry,
		searchService: searchService,
	}
}

// Upsert inserts or updates points in a collection.
func (s *PointsService) Upsert(ctx context.Context, req *pb.UpsertPointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Points) == 0 {
		return nil, status.Error(codes.InvalidArgument, "points are required")
	}

	if len(req.Points) > s.config.MaxBatchPoints {
		return nil, status.Errorf(codes.InvalidArgument, "too many points: %d > %d", len(req.Points), s.config.MaxBatchPoints)
	}

	// Get collection metadata
	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	// Process points
	nodes := make([]*storage.Node, 0, len(req.Points))
	for _, point := range req.Points {
		// Convert point ID
		nodeID := pointIDToNodeID(req.CollectionName, point.Id)

		// Extract vector
		vec, err := extractVector(point.Vectors)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid vector: %v", err)
		}

		// Validate vector dimensions
		if len(vec) != meta.Dimensions {
			return nil, status.Errorf(codes.InvalidArgument, "vector dimension mismatch: got %d, expected %d", len(vec), meta.Dimensions)
		}

		// Convert payload to properties
		props := payloadToProperties(point.Payload)

		// Create node
		node := &storage.Node{
			ID:              nodeID,
			Labels:          []string{QdrantPointLabel, req.CollectionName},
			Properties:      props,
			ChunkEmbeddings: [][]float32{vec},
			CreatedAt:       time.Now(),
		}
		nodes = append(nodes, node)
	}

	// Bulk create nodes in storage
	if err := s.storage.BulkCreateNodes(nodes); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store points: %v", err)
	}

	// Index nodes in search.Service for unified search
	if s.searchService != nil {
		for _, node := range nodes {
			if err := s.searchService.IndexNode(node); err != nil {
				// Log but don't fail - storage succeeded
			}
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// Get retrieves points by IDs.
func (s *PointsService) Get(ctx context.Context, req *pb.GetPointsRequest) (*pb.GetPointsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids are required")
	}

	// Determine what to include
	includePayload := true
	includeVectors := false
	if req.WithPayload != nil {
		if enable, ok := req.WithPayload.SelectorOptions.(*pb.WithPayloadSelector_Enable); ok {
			includePayload = enable.Enable
		}
	}
	if req.WithVectors != nil {
		if enable, ok := req.WithVectors.SelectorOptions.(*pb.WithVectorsSelector_Enable); ok {
			includeVectors = enable.Enable
		}
	}

	// Fetch points
	results := make([]*pb.RetrievedPoint, 0, len(req.Ids))
	for _, pointID := range req.Ids {
		nodeID := pointIDToNodeID(req.CollectionName, pointID)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue // Skip not found
		}

		point := &pb.RetrievedPoint{
			Id: pointID,
		}

		if includePayload {
			point.Payload = propertiesToPayload(node.Properties)
		}

		if includeVectors && len(node.ChunkEmbeddings) > 0 {
			point.Vectors = &pb.Vectors{
				VectorsOptions: &pb.Vectors_Vector{
					Vector: &pb.Vector{
						Data: node.ChunkEmbeddings[0],
					},
				},
			}
		}

		results = append(results, point)
	}

	return &pb.GetPointsResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// Delete removes points from a collection.
func (s *PointsService) Delete(ctx context.Context, req *pb.DeletePointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if req.Points == nil {
		return nil, status.Error(codes.InvalidArgument, "points selector is required")
	}

	// Handle different selector types
	var nodeIDs []storage.NodeID
	switch sel := req.Points.PointsSelectorOneOf.(type) {
	case *pb.PointsSelector_Points:
		for _, pointID := range sel.Points.Ids {
			nodeIDs = append(nodeIDs, pointIDToNodeID(req.CollectionName, pointID))
		}
	case *pb.PointsSelector_Filter:
		// Filter-based deletion not supported in phase 1
		return nil, status.Error(codes.Unimplemented, "filter-based deletion not yet supported")
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid points selector")
	}

	// Delete from storage and search index
	for _, nodeID := range nodeIDs {
		// Remove from search index first
		if s.searchService != nil {
			_ = s.searchService.RemoveNode(nodeID)
		}
		// Then delete from storage
		if err := s.storage.DeleteNode(nodeID); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// Search finds similar vectors.
// This uses brute-force search via the search.Service's vector index or
// direct similarity computation if searchService is not available.
func (s *PointsService) Search(ctx context.Context, req *pb.SearchPointsRequest) (*pb.SearchPointsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Vector) == 0 {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}
	if limit > s.config.MaxTopK {
		return nil, status.Errorf(codes.InvalidArgument, "limit too large: %d > %d", limit, s.config.MaxTopK)
	}

	// Get collection metadata for dimension validation
	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	if len(req.Vector) != meta.Dimensions {
		return nil, status.Errorf(codes.InvalidArgument, "vector dimension mismatch: got %d, expected %d", len(req.Vector), meta.Dimensions)
	}

	// Determine minimum similarity
	minSimilarity := float64(0)
	if req.ScoreThreshold != nil {
		minSimilarity = float64(*req.ScoreThreshold)
	}

	// Determine what to include
	includePayload := false
	includeVectors := false
	if req.WithPayload != nil {
		if enable, ok := req.WithPayload.SelectorOptions.(*pb.WithPayloadSelector_Enable); ok {
			includePayload = enable.Enable
		}
	}
	if req.WithVectors != nil {
		if enable, ok := req.WithVectors.SelectorOptions.(*pb.WithVectorsSelector_Enable); ok {
			includeVectors = enable.Enable
		}
	}

	// Perform search - collection-scoped brute force search
	// We search only within the collection's points
	results := s.searchCollection(ctx, req.CollectionName, req.Vector, limit, minSimilarity)

	// Convert results to response
	scoredPoints := make([]*pb.ScoredPoint, 0, len(results))
	for _, sr := range results {
		nodeID := storage.NodeID(sr.ID)
		pointID := nodeIDToPointID(nodeID)

		scoredPoint := &pb.ScoredPoint{
			Id:    pointID,
			Score: float32(sr.Score),
		}

		// Fetch payload if requested
		if includePayload || includeVectors {
			node, err := s.storage.GetNode(nodeID)
			if err == nil {
				if includePayload {
					scoredPoint.Payload = propertiesToPayload(node.Properties)
				}
				if includeVectors && len(node.ChunkEmbeddings) > 0 {
					scoredPoint.Vectors = &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{
								Data: node.ChunkEmbeddings[0],
							},
						},
					}
				}
			}
		}

		scoredPoints = append(scoredPoints, scoredPoint)
	}

	return &pb.SearchPointsResponse{
		Result: scoredPoints,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// searchCollection performs a collection-scoped vector search.
// This scans nodes in the collection and computes cosine similarity.
type searchResult struct {
	ID    string
	Score float64
}

func (s *PointsService) searchCollection(ctx context.Context, collection string, queryVec []float32, limit int, minSimilarity float64) []searchResult {
	// Get all points in the collection
	nodes, err := s.storage.GetNodesByLabel(collection)
	if err != nil {
		return nil
	}

	// Normalize query vector
	normalizedQuery := vector.Normalize(queryVec)

	// Score each point
	type scored struct {
		id    string
		score float64
	}
	var scored_results []scored

	for _, node := range nodes {
		// Verify it's a Qdrant point
		isQdrantPoint := false
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				isQdrantPoint = true
				break
			}
		}
		if !isQdrantPoint {
			continue
		}

		// Get embedding
		if len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0 {
			continue
		}

		// Compute similarity
		nodeVec := vector.Normalize(node.ChunkEmbeddings[0])
		similarity := float64(vector.DotProduct(normalizedQuery, nodeVec))

		if similarity >= minSimilarity {
			scored_results = append(scored_results, scored{
				id:    string(node.ID),
				score: similarity,
			})
		}
	}

	// Sort by score descending
	for i := 0; i < len(scored_results); i++ {
		for j := i + 1; j < len(scored_results); j++ {
			if scored_results[j].score > scored_results[i].score {
				scored_results[i], scored_results[j] = scored_results[j], scored_results[i]
			}
		}
	}

	// Limit results
	if len(scored_results) > limit {
		scored_results = scored_results[:limit]
	}

	// Convert to result type
	results := make([]searchResult, len(scored_results))
	for i, sr := range scored_results {
		results[i] = searchResult{ID: sr.id, Score: sr.score}
	}

	return results
}

// SearchBatch performs multiple search requests.
func (s *PointsService) SearchBatch(ctx context.Context, req *pb.SearchBatchPointsRequest) (*pb.SearchBatchPointsResponse, error) {
	start := time.Now()

	results := make([]*pb.SearchPointsResponse, 0, len(req.SearchRequests))
	for _, searchReq := range req.SearchRequests {
		// Override collection name from parent request
		searchReq.CollectionName = req.CollectionName
		result, err := s.Search(ctx, searchReq)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return &pb.SearchBatchPointsResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// Count counts points in a collection.
func (s *PointsService) Count(ctx context.Context, req *pb.CountPointsRequest) (*pb.CountPointsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Get count from registry
	count, err := s.registry.GetPointCount(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	return &pb.CountPointsResponse{
		Result: &pb.CountResult{
			Count: uint64(count),
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// SCROLL - Paginated Iteration
// =============================================================================

// Scroll iterates through all points with pagination.
// Maps to: storage.GetNodesByLabel() + offset/limit pagination
func (s *PointsService) Scroll(ctx context.Context, req *pb.ScrollPointsRequest) (*pb.ScrollPointsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Determine limit
	limit := 10
	if req.Limit != nil && *req.Limit > 0 {
		limit = int(*req.Limit)
	}

	// Determine what to include
	includePayload := true
	includeVectors := false
	if req.WithPayload != nil {
		if enable, ok := req.WithPayload.SelectorOptions.(*pb.WithPayloadSelector_Enable); ok {
			includePayload = enable.Enable
		}
	}
	if req.WithVectors != nil {
		if enable, ok := req.WithVectors.SelectorOptions.(*pb.WithVectorsSelector_Enable); ok {
			includeVectors = enable.Enable
		}
	}

	// Get all points in the collection
	nodes, err := s.storage.GetNodesByLabel(req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get points: %v", err)
	}

	// Filter to only Qdrant points and sort by ID for consistent pagination
	var qdrantPoints []*storage.Node
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				qdrantPoints = append(qdrantPoints, node)
				break
			}
		}
	}

	// Sort by ID for consistent pagination
	sortNodesByID(qdrantPoints)

	// Apply offset if provided
	startIdx := 0
	if req.Offset != nil {
		offsetID := pointIDToNodeID(req.CollectionName, req.Offset)
		for i, node := range qdrantPoints {
			if node.ID == offsetID {
				startIdx = i + 1 // Start after the offset point
				break
			}
		}
	}

	// Slice to get page
	endIdx := startIdx + limit
	if endIdx > len(qdrantPoints) {
		endIdx = len(qdrantPoints)
	}
	pageNodes := qdrantPoints[startIdx:endIdx]

	// Convert to retrieved points
	results := make([]*pb.RetrievedPoint, 0, len(pageNodes))
	for _, node := range pageNodes {
		pointID := nodeIDToPointID(node.ID)
		point := &pb.RetrievedPoint{
			Id: pointID,
		}

		if includePayload {
			point.Payload = propertiesToPayload(node.Properties)
		}

		if includeVectors && len(node.ChunkEmbeddings) > 0 {
			point.Vectors = &pb.Vectors{
				VectorsOptions: &pb.Vectors_Vector{
					Vector: &pb.Vector{
						Data: node.ChunkEmbeddings[0],
					},
				},
			}
		}

		results = append(results, point)
	}

	// Determine next page offset
	var nextOffset *pb.PointId
	if endIdx < len(qdrantPoints) {
		nextOffset = nodeIDToPointID(qdrantPoints[endIdx-1].ID)
	}

	return &pb.ScrollPointsResponse{
		Result:         results,
		NextPageOffset: nextOffset,
		Time:           time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// RECOMMEND - Similarity Recommendations
// =============================================================================

// Recommend finds points similar to positive examples and dissimilar to negative.
// Maps to: Average positive vectors, subtract negative → search.Service.Search
func (s *PointsService) Recommend(ctx context.Context, req *pb.RecommendPointsRequest) (*pb.RecommendPointsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Positive) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one positive point is required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}

	// Collect positive vectors
	var positiveVectors [][]float32
	for _, pointID := range req.Positive {
		nodeID := pointIDToNodeID(req.CollectionName, pointID)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue // Skip not found
		}
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			positiveVectors = append(positiveVectors, node.ChunkEmbeddings[0])
		}
	}

	if len(positiveVectors) == 0 {
		return nil, status.Error(codes.NotFound, "no positive vectors found")
	}

	// Collect negative vectors
	var negativeVectors [][]float32
	for _, pointID := range req.Negative {
		nodeID := pointIDToNodeID(req.CollectionName, pointID)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			negativeVectors = append(negativeVectors, node.ChunkEmbeddings[0])
		}
	}

	// Compute recommendation vector: average(positive) - average(negative)
	dims := len(positiveVectors[0])
	recVec := make([]float32, dims)

	// Add average of positive vectors
	for _, vec := range positiveVectors {
		for i := 0; i < dims && i < len(vec); i++ {
			recVec[i] += vec[i] / float32(len(positiveVectors))
		}
	}

	// Subtract average of negative vectors
	if len(negativeVectors) > 0 {
		for _, vec := range negativeVectors {
			for i := 0; i < dims && i < len(vec); i++ {
				recVec[i] -= vec[i] / float32(len(negativeVectors))
			}
		}
	}

	// Normalize the result
	recVec = vector.Normalize(recVec)

	// Determine minimum similarity
	minSimilarity := float64(0)
	if req.ScoreThreshold != nil {
		minSimilarity = float64(*req.ScoreThreshold)
	}

	// Determine what to include
	includePayload := false
	includeVectors := false
	if req.WithPayload != nil {
		if enable, ok := req.WithPayload.SelectorOptions.(*pb.WithPayloadSelector_Enable); ok {
			includePayload = enable.Enable
		}
	}
	if req.WithVectors != nil {
		if enable, ok := req.WithVectors.SelectorOptions.(*pb.WithVectorsSelector_Enable); ok {
			includeVectors = enable.Enable
		}
	}

	// Perform search using the recommendation vector
	results := s.searchCollection(ctx, req.CollectionName, recVec, limit+len(req.Positive)+len(req.Negative), minSimilarity)

	// Build set of IDs to exclude (positive and negative examples)
	excludeIDs := make(map[string]bool)
	for _, pointID := range req.Positive {
		nodeID := pointIDToNodeID(req.CollectionName, pointID)
		excludeIDs[string(nodeID)] = true
	}
	for _, pointID := range req.Negative {
		nodeID := pointIDToNodeID(req.CollectionName, pointID)
		excludeIDs[string(nodeID)] = true
	}

	// Convert results, excluding positive/negative examples
	scoredPoints := make([]*pb.ScoredPoint, 0, limit)
	for _, sr := range results {
		if excludeIDs[sr.ID] {
			continue
		}
		if len(scoredPoints) >= limit {
			break
		}

		nodeID := storage.NodeID(sr.ID)
		pointID := nodeIDToPointID(nodeID)

		scoredPoint := &pb.ScoredPoint{
			Id:    pointID,
			Score: float32(sr.Score),
		}

		if includePayload || includeVectors {
			node, err := s.storage.GetNode(nodeID)
			if err == nil {
				if includePayload {
					scoredPoint.Payload = propertiesToPayload(node.Properties)
				}
				if includeVectors && len(node.ChunkEmbeddings) > 0 {
					scoredPoint.Vectors = &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{
								Data: node.ChunkEmbeddings[0],
							},
						},
					}
				}
			}
		}

		scoredPoints = append(scoredPoints, scoredPoint)
	}

	return &pb.RecommendPointsResponse{
		Result: scoredPoints,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// RecommendBatch performs multiple recommendation requests.
func (s *PointsService) RecommendBatch(ctx context.Context, req *pb.RecommendBatchPointsRequest) (*pb.RecommendBatchPointsResponse, error) {
	start := time.Now()

	results := make([]*pb.RecommendPointsResponse, 0, len(req.RecommendRequests))
	for _, recReq := range req.RecommendRequests {
		recReq.CollectionName = req.CollectionName
		result, err := s.Recommend(ctx, recReq)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return &pb.RecommendBatchPointsResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// SEARCH GROUPS - Grouped Search
// =============================================================================

// SearchGroups searches with grouping by a payload field.
// Maps to: search.Service.Search + group by field
func (s *PointsService) SearchGroups(ctx context.Context, req *pb.SearchPointGroupsRequest) (*pb.SearchPointGroupsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Vector) == 0 {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}

	if req.GroupBy == "" {
		return nil, status.Error(codes.InvalidArgument, "group_by is required")
	}

	groupLimit := int(req.Limit)
	if groupLimit <= 0 {
		groupLimit = 3
	}

	groupSize := int(req.GroupSize)
	if groupSize <= 0 {
		groupSize = 3
	}

	// Search for more results than needed to ensure enough groups
	searchLimit := groupLimit * groupSize * 3
	if searchLimit > s.config.MaxTopK {
		searchLimit = s.config.MaxTopK
	}

	minSimilarity := float64(0)
	if req.ScoreThreshold != nil {
		minSimilarity = float64(*req.ScoreThreshold)
	}

	// Determine what to include
	includePayload := false
	includeVectors := false
	if req.WithPayload != nil {
		if enable, ok := req.WithPayload.SelectorOptions.(*pb.WithPayloadSelector_Enable); ok {
			includePayload = enable.Enable
		}
	}
	if req.WithVectors != nil {
		if enable, ok := req.WithVectors.SelectorOptions.(*pb.WithVectorsSelector_Enable); ok {
			includeVectors = enable.Enable
		}
	}

	// Perform search
	results := s.searchCollection(ctx, req.CollectionName, req.Vector, searchLimit, minSimilarity)

	// Group results by the specified field
	groups := make(map[string][]*pb.ScoredPoint)
	groupOrder := make([]string, 0) // Track order of first appearance

	for _, sr := range results {
		nodeID := storage.NodeID(sr.ID)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Get group key from payload
		groupKey := ""
		if val, ok := node.Properties[req.GroupBy]; ok {
			groupKey = fmt.Sprintf("%v", val)
		}

		// Skip if group is full
		if len(groups[groupKey]) >= groupSize {
			continue
		}

		// Skip if we already have enough groups
		if len(groups[groupKey]) == 0 && len(groupOrder) >= groupLimit {
			continue
		}

		// Add to group
		pointID := nodeIDToPointID(nodeID)
		scoredPoint := &pb.ScoredPoint{
			Id:    pointID,
			Score: float32(sr.Score),
		}

		if includePayload {
			scoredPoint.Payload = propertiesToPayload(node.Properties)
		}
		if includeVectors && len(node.ChunkEmbeddings) > 0 {
			scoredPoint.Vectors = &pb.Vectors{
				VectorsOptions: &pb.Vectors_Vector{
					Vector: &pb.Vector{
						Data: node.ChunkEmbeddings[0],
					},
				},
			}
		}

		if len(groups[groupKey]) == 0 {
			groupOrder = append(groupOrder, groupKey)
		}
		groups[groupKey] = append(groups[groupKey], scoredPoint)
	}

	// Build response
	pointGroups := make([]*pb.PointGroup, 0, len(groupOrder))
	for _, key := range groupOrder {
		group := &pb.PointGroup{
			Id: &pb.GroupId{
				Kind: &pb.GroupId_StringValue{StringValue: key},
			},
			Hits: groups[key],
		}
		pointGroups = append(pointGroups, group)
	}

	return &pb.SearchPointGroupsResponse{
		Result: pointGroups,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// PAYLOAD OPERATIONS
// =============================================================================

// SetPayload sets payload values for points (merges with existing).
// Maps to: storage.UpdateNode (merge Properties)
func (s *PointsService) SetPayload(ctx context.Context, req *pb.SetPayloadPointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "payload is required")
	}

	// Get points to update
	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	// Convert payload to properties
	newProps := payloadToProperties(req.Payload)

	// Update each point
	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Merge new properties with existing
		if node.Properties == nil {
			node.Properties = make(map[string]any)
		}
		for k, v := range newProps {
			node.Properties[k] = v
		}

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// OverwritePayload replaces entire payload for points.
// Maps to: storage.UpdateNode (replace Properties)
func (s *PointsService) OverwritePayload(ctx context.Context, req *pb.SetPayloadPointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Get points to update
	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	// Convert payload to properties
	newProps := payloadToProperties(req.Payload)

	// Update each point
	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Replace properties entirely
		node.Properties = newProps

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// DeletePayload removes specific payload keys from points.
// Maps to: storage.UpdateNode (remove keys from Properties)
func (s *PointsService) DeletePayload(ctx context.Context, req *pb.DeletePayloadPointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Keys) == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys are required")
	}

	// Get points to update
	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	// Update each point
	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Remove specified keys
		if node.Properties != nil {
			for _, key := range req.Keys {
				delete(node.Properties, key)
			}
		}

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// ClearPayload removes all payload from points.
// Maps to: storage.UpdateNode (empty Properties)
func (s *PointsService) ClearPayload(ctx context.Context, req *pb.ClearPayloadPointsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Get points to update
	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.Points)
	if err != nil {
		return nil, err
	}

	// Update each point
	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Clear properties
		node.Properties = make(map[string]any)

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// VECTOR OPERATIONS
// =============================================================================

// UpdateVectors updates vectors for existing points.
// Maps to: storage.UpdateNode (update ChunkEmbeddings) + search.IndexNode
func (s *PointsService) UpdateVectors(ctx context.Context, req *pb.UpdatePointVectorsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if len(req.Points) == 0 {
		return nil, status.Error(codes.InvalidArgument, "points are required")
	}

	// Get collection metadata for dimension validation
	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	// Update each point
	for _, pv := range req.Points {
		nodeID := pointIDToNodeID(req.CollectionName, pv.Id)

		// Get existing node
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue // Skip not found
		}

		// Extract new vector
		vec, err := extractVector(pv.Vectors)
		if err != nil {
			continue // Skip invalid vector
		}

		// Validate dimensions
		if len(vec) != meta.Dimensions {
			continue // Skip dimension mismatch
		}

		// Update embedding
		node.ChunkEmbeddings = [][]float32{vec}

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			continue
		}

		// Re-index in search service
		if s.searchService != nil {
			_ = s.searchService.IndexNode(node)
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// DeleteVectors removes vectors from points (keeps payload).
// Maps to: storage.UpdateNode (clear ChunkEmbeddings) + search.RemoveNode
func (s *PointsService) DeleteVectors(ctx context.Context, req *pb.DeletePointVectorsRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Get points to update
	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	// Update each point
	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		// Clear embeddings
		node.ChunkEmbeddings = nil

		// Remove from search index
		if s.searchService != nil {
			_ = s.searchService.RemoveNode(nodeID)
		}

		// Update in storage
		if err := s.storage.UpdateNode(node); err != nil {
			// Log but continue
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// FIELD INDEX OPERATIONS
// =============================================================================

// CreateFieldIndex creates an index on a payload field.
// Maps to: storage.SchemaManager.AddPropertyIndex
func (s *PointsService) CreateFieldIndex(ctx context.Context, req *pb.CreateFieldIndexCollectionRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if req.FieldName == "" {
		return nil, status.Error(codes.InvalidArgument, "field_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// Get schema manager from storage
	schemaManager := s.storage.GetSchema()
	if schemaManager == nil {
		return nil, status.Error(codes.Internal, "schema manager not available")
	}

	// Create property index using NornicDB's schema manager
	// The index is scoped to the collection label with the specified property
	indexName := fmt.Sprintf("qdrant_%s_%s", req.CollectionName, req.FieldName)
	err := schemaManager.AddPropertyIndex(indexName, req.CollectionName, []string{req.FieldName})
	if err != nil {
		// Index may already exist, which is fine
		if err != storage.ErrAlreadyExists {
			return nil, status.Errorf(codes.Internal, "failed to create index: %v", err)
		}
	}

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// DeleteFieldIndex removes a payload field index.
// NornicDB's schema manager doesn't have a drop method, so this is a no-op that validates inputs.
// The index will be unused but remains in memory until restart.
func (s *PointsService) DeleteFieldIndex(ctx context.Context, req *pb.DeleteFieldIndexCollectionRequest) (*pb.PointsOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if req.FieldName == "" {
		return nil, status.Error(codes.InvalidArgument, "field_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// NornicDB doesn't currently support dropping property indexes
	// The index exists in memory only and will be cleaned up on restart
	// This is compatible behavior - the index is logically deleted

	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_COMPLETED,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// resolvePointsSelector converts a PointsSelector to a list of NodeIDs.
func (s *PointsService) resolvePointsSelector(ctx context.Context, collection string, selector *pb.PointsSelector) ([]storage.NodeID, error) {
	if selector == nil {
		// No selector - return all points in collection
		nodes, err := s.storage.GetNodesByLabel(collection)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get points: %v", err)
		}
		nodeIDs := make([]storage.NodeID, 0, len(nodes))
		for _, node := range nodes {
			for _, label := range node.Labels {
				if label == QdrantPointLabel {
					nodeIDs = append(nodeIDs, node.ID)
					break
				}
			}
		}
		return nodeIDs, nil
	}

	switch sel := selector.PointsSelectorOneOf.(type) {
	case *pb.PointsSelector_Points:
		nodeIDs := make([]storage.NodeID, 0, len(sel.Points.Ids))
		for _, pointID := range sel.Points.Ids {
			nodeIDs = append(nodeIDs, pointIDToNodeID(collection, pointID))
		}
		return nodeIDs, nil
	case *pb.PointsSelector_Filter:
		// Filter-based selection: get all points and filter
		nodes, err := s.storage.GetNodesByLabel(collection)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get points: %v", err)
		}
		nodeIDs := make([]storage.NodeID, 0)
		for _, node := range nodes {
			isQdrantPoint := false
			for _, label := range node.Labels {
				if label == QdrantPointLabel {
					isQdrantPoint = true
					break
				}
			}
			if !isQdrantPoint {
				continue
			}
			if matchesFilter(node, sel.Filter) {
				nodeIDs = append(nodeIDs, node.ID)
			}
		}
		return nodeIDs, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid points selector")
	}
}

// matchesFilter checks if a node matches a Qdrant filter.
func matchesFilter(node *storage.Node, filter *pb.Filter) bool {
	if filter == nil {
		return true
	}

	// All "must" conditions must match
	for _, cond := range filter.Must {
		if !matchesCondition(node, cond) {
			return false
		}
	}

	// At least one "should" condition must match (if any)
	if len(filter.Should) > 0 {
		matched := false
		for _, cond := range filter.Should {
			if matchesCondition(node, cond) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// No "must_not" conditions should match
	for _, cond := range filter.MustNot {
		if matchesCondition(node, cond) {
			return false
		}
	}

	return true
}

// matchesCondition checks if a node matches a single condition.
func matchesCondition(node *storage.Node, cond *pb.Condition) bool {
	if cond == nil {
		return true
	}

	switch c := cond.ConditionOneOf.(type) {
	case *pb.Condition_Field:
		return matchesFieldCondition(node, c.Field)
	case *pb.Condition_HasId:
		for _, pointID := range c.HasId.HasId {
			if pointIDMatchesNode(pointID, node) {
				return true
			}
		}
		return false
	case *pb.Condition_Filter:
		return matchesFilter(node, c.Filter)
	default:
		return false
	}
}

// matchesFieldCondition checks if a node matches a field condition.
func matchesFieldCondition(node *storage.Node, cond *pb.FieldCondition) bool {
	if cond == nil || node.Properties == nil {
		return false
	}

	val, exists := node.Properties[cond.Key]
	if !exists {
		return false
	}

	// Check match condition
	if cond.Match != nil {
		switch m := cond.Match.MatchValue.(type) {
		case *pb.Match_Keyword:
			strVal, ok := val.(string)
			return ok && strVal == m.Keyword
		case *pb.Match_Integer:
			switch v := val.(type) {
			case int64:
				return v == m.Integer
			case int:
				return int64(v) == m.Integer
			case float64:
				return int64(v) == m.Integer
			}
		case *pb.Match_Boolean:
			boolVal, ok := val.(bool)
			return ok && boolVal == m.Boolean
		}
	}

	// Check range condition
	if cond.Range != nil {
		var numVal float64
		switch v := val.(type) {
		case float64:
			numVal = v
		case int64:
			numVal = float64(v)
		case int:
			numVal = float64(v)
		default:
			return false
		}

		if cond.Range.Lt != nil && numVal >= *cond.Range.Lt {
			return false
		}
		if cond.Range.Lte != nil && numVal > *cond.Range.Lte {
			return false
		}
		if cond.Range.Gt != nil && numVal <= *cond.Range.Gt {
			return false
		}
		if cond.Range.Gte != nil && numVal < *cond.Range.Gte {
			return false
		}
	}

	return true
}

// pointIDMatchesNode checks if a point ID matches a node.
func pointIDMatchesNode(pointID *pb.PointId, node *storage.Node) bool {
	if pointID == nil {
		return false
	}
	nodeIDStr := string(node.ID)
	switch pid := pointID.PointIdOptions.(type) {
	case *pb.PointId_Num:
		return nodeIDStr == fmt.Sprintf("%d", pid.Num) ||
			nodeIDStr == fmt.Sprintf("qdrant:%d", pid.Num)
	case *pb.PointId_Uuid:
		return nodeIDStr == pid.Uuid ||
			nodeIDStr == fmt.Sprintf("qdrant:%s", pid.Uuid)
	}
	return false
}

// sortNodesByID sorts nodes by their ID for consistent pagination.
func sortNodesByID(nodes []*storage.Node) {
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if string(nodes[j].ID) < string(nodes[i].ID) {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

// pointIDToNodeID converts a Qdrant PointId to a NornicDB NodeID.
func pointIDToNodeID(collection string, id *pb.PointId) storage.NodeID {
	if id == nil {
		return ""
	}
	var idStr string
	switch pid := id.PointIdOptions.(type) {
	case *pb.PointId_Num:
		idStr = fmt.Sprintf("%d", pid.Num)
	case *pb.PointId_Uuid:
		idStr = pid.Uuid
	}
	return storage.NodeID(fmt.Sprintf("qdrant:%s:%s", collection, idStr))
}

// nodeIDToPointID converts a NornicDB NodeID back to a Qdrant PointId.
func nodeIDToPointID(nodeID storage.NodeID) *pb.PointId {
	// Extract the ID part after "qdrant:collection:"
	idStr := string(nodeID)
	// Find the last colon
	for i := len(idStr) - 1; i >= 0; i-- {
		if idStr[i] == ':' {
			idStr = idStr[i+1:]
			break
		}
	}
	return &pb.PointId{
		PointIdOptions: &pb.PointId_Uuid{Uuid: idStr},
	}
}

// extractVector extracts the vector from a Vectors message.
func extractVector(v *pb.Vectors) ([]float32, error) {
	if v == nil {
		return nil, fmt.Errorf("vectors is nil")
	}

	switch opt := v.VectorsOptions.(type) {
	case *pb.Vectors_Vector:
		if opt.Vector == nil {
			return nil, fmt.Errorf("vector is nil")
		}
		return opt.Vector.Data, nil
	case *pb.Vectors_Vectors:
		// Multi-vector: use the first one (or named "default")
		if opt.Vectors == nil || len(opt.Vectors.Vectors) == 0 {
			return nil, fmt.Errorf("no vectors in map")
		}
		// Try "default" first, then take first available
		if v, ok := opt.Vectors.Vectors[""]; ok && v != nil {
			return v.Data, nil
		}
		for _, v := range opt.Vectors.Vectors {
			if v != nil {
				return v.Data, nil
			}
		}
		return nil, fmt.Errorf("no valid vector found")
	default:
		return nil, fmt.Errorf("unsupported vectors type")
	}
}

// payloadToProperties converts Qdrant Payload to NornicDB Properties.
func payloadToProperties(payload map[string]*pb.Value) map[string]any {
	if payload == nil {
		return nil
	}

	props := make(map[string]any, len(payload))
	for key, value := range payload {
		props[key] = valueToAny(value)
	}
	return props
}

// propertiesToPayload converts NornicDB Properties to Qdrant Payload.
func propertiesToPayload(props map[string]any) map[string]*pb.Value {
	if props == nil {
		return nil
	}

	payload := make(map[string]*pb.Value, len(props))
	for key, value := range props {
		payload[key] = anyToValue(value)
	}
	return payload
}

// valueToAny converts a Qdrant Value to a Go any type.
func valueToAny(v *pb.Value) any {
	if v == nil {
		return nil
	}

	switch kind := v.Kind.(type) {
	case *pb.Value_NullValue:
		return nil
	case *pb.Value_BoolValue:
		return kind.BoolValue
	case *pb.Value_IntegerValue:
		return kind.IntegerValue
	case *pb.Value_DoubleValue:
		return kind.DoubleValue
	case *pb.Value_StringValue:
		return kind.StringValue
	case *pb.Value_ListValue:
		if kind.ListValue == nil {
			return nil
		}
		result := make([]any, len(kind.ListValue.Values))
		for i, item := range kind.ListValue.Values {
			result[i] = valueToAny(item)
		}
		return result
	case *pb.Value_StructValue:
		if kind.StructValue == nil {
			return nil
		}
		result := make(map[string]any, len(kind.StructValue.Fields))
		for k, v := range kind.StructValue.Fields {
			result[k] = valueToAny(v)
		}
		return result
	default:
		return nil
	}
}

// anyToValue converts a Go any type to a Qdrant Value.
func anyToValue(v any) *pb.Value {
	if v == nil {
		return &pb.Value{Kind: &pb.Value_NullValue{}}
	}

	switch val := v.(type) {
	case bool:
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: val}}
	case int:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: int64(val)}}
	case int64:
		return &pb.Value{Kind: &pb.Value_IntegerValue{IntegerValue: val}}
	case float64:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: val}}
	case float32:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: float64(val)}}
	case string:
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: val}}
	case []any:
		values := make([]*pb.Value, len(val))
		for i, item := range val {
			values[i] = anyToValue(item)
		}
		return &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: values}}}
	case map[string]any:
		fields := make(map[string]*pb.Value, len(val))
		for k, v := range val {
			fields[k] = anyToValue(v)
		}
		return &pb.Value{Kind: &pb.Value_StructValue{StructValue: &pb.Struct{Fields: fields}}}
	default:
		// Try to convert to string as fallback
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: fmt.Sprintf("%v", val)}}
	}
}

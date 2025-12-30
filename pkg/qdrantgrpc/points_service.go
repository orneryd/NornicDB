package qdrantgrpc

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const qdrantVectorNamesKey = "_qdrant_vector_names"

// PointsService implements the upstream Qdrant Points gRPC service (package `qdrant`).
// It stores points as NornicDB nodes and (optionally) indexes them via search.Service.
type PointsService struct {
	qpb.UnimplementedPointsServer
	config        *Config
	storage       storage.Engine
	registry      CollectionRegistry
	searchService *search.Service
	vecIndex      *vectorIndexCache
}

func NewPointsService(config *Config, store storage.Engine, registry CollectionRegistry, searchService *search.Service, vecIndex *vectorIndexCache) *PointsService {
	return &PointsService{
		config:        config,
		storage:       store,
		registry:      registry,
		searchService: searchService,
		vecIndex:      vecIndex,
	}
}

// Upsert inserts or overwrites points. If a point exists, its payload/vectors are replaced.
func (s *PointsService) Upsert(ctx context.Context, req *qpb.UpsertPoints) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if !s.config.AllowVectorMutations {
		return nil, status.Error(codes.FailedPrecondition, "vector mutations are disabled because NornicDB-managed embeddings are enabled; set NORNICDB_EMBEDDING_ENABLED=false to allow managing vectors via Qdrant gRPC")
	}
	if len(req.GetPoints()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "points are required")
	}
	if len(req.GetPoints()) > s.config.MaxBatchPoints {
		return nil, status.Errorf(codes.InvalidArgument, "too many points: %d > %d", len(req.GetPoints()), s.config.MaxBatchPoints)
	}

	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	nodeIDs := make([]storage.NodeID, 0, len(req.Points))
	for _, point := range req.Points {
		if point == nil || point.Id == nil {
			return nil, status.Error(codes.InvalidArgument, "point id is required")
		}
		nodeIDs = append(nodeIDs, pointIDToNodeID(req.CollectionName, point.Id))
	}

	existing, err := s.storage.BatchGetNodes(nodeIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read existing points: %v", err)
	}

	nodesToCreate := make([]*storage.Node, 0, len(req.Points))
	nodesToUpdate := make([]*storage.Node, 0, len(req.Points))
	oldVectorNamesByID := make(map[string][]string, len(req.Points))

	for i, point := range req.Points {
		nodeID := nodeIDs[i]

		if point.Vectors == nil {
			return nil, status.Error(codes.InvalidArgument, "vectors are required")
		}
		vecNames, vecs, err := extractVectors(point.Vectors)
		if err != nil {
			return nil, err
		}
		for _, v := range vecs {
			if len(v) != meta.Dimensions {
				return nil, status.Errorf(codes.InvalidArgument, "vector dimension mismatch: got %d, expected %d", len(v), meta.Dimensions)
			}
		}

		props := payloadToProperties(point.Payload)
		node := &storage.Node{
			ID:         nodeID,
			Labels:     []string{QdrantPointLabel, req.CollectionName},
			Properties: props,
		}

		// Write vectors to NamedEmbeddings instead of ChunkEmbeddings
		// Qdrant unnamed vector → NamedEmbeddings["default"]
		// Qdrant named vectors → NamedEmbeddings[name]
		if node.NamedEmbeddings == nil {
			node.NamedEmbeddings = make(map[string][]float32)
		}
		for i, name := range vecNames {
			if i >= len(vecs) {
				break
			}
			// Map empty string to "default" for unnamed vectors
			vectorName := name
			if vectorName == "" {
				vectorName = "default"
			}
			node.NamedEmbeddings[vectorName] = vecs[i]
		}

		// Keep ChunkEmbeddings for backward compatibility during migration
		// TODO: Remove ChunkEmbeddings once all code paths use NamedEmbeddings
		if len(vecs) > 0 {
			node.ChunkEmbeddings = vecs
		}
		setNodeVectorNames(node, vecNames)

		if prev := existing[nodeID]; prev != nil {
			oldVectorNamesByID[string(node.ID)] = nodeVectorNames(prev)
			node.CreatedAt = prev.CreatedAt
			node.UpdatedAt = time.Now()
			nodesToUpdate = append(nodesToUpdate, node)
		} else {
			oldVectorNamesByID[string(node.ID)] = nil
			node.CreatedAt = time.Now()
			nodesToCreate = append(nodesToCreate, node)
		}
	}

	if len(nodesToCreate) > 0 {
		if err := s.storage.BulkCreateNodes(nodesToCreate); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create points: %v", err)
		}
	}
	for _, node := range nodesToUpdate {
		if err := s.storage.UpdateNode(node); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to update point %s: %v", node.ID, err)
		}
	}

	if s.searchService != nil {
		for _, node := range nodesToCreate {
			_ = s.searchService.IndexNode(node)
		}
		for _, node := range nodesToUpdate {
			_ = s.searchService.RemoveNode(node.ID)
			_ = s.searchService.IndexNode(node)
		}
	}

	// Maintain a per-collection in-memory vector index for Qdrant gRPC searches.
	// This avoids falling back to storage scans when Qdrant collection dimensions
	// differ from the DB's default embedding dimensions.
	if s.vecIndex != nil {
		for _, node := range append(nodesToCreate, nodesToUpdate...) {
			if node == nil {
				continue
			}
			newNames := nodeVectorNames(node)
			if len(newNames) == 0 {
				newNames = []string{""}
			}

			oldNames := oldVectorNamesByID[string(node.ID)]
			if err := s.vecIndex.replacePoint(req.CollectionName, meta.Dimensions, meta.Distance, string(node.ID), oldNames, newNames, node.ChunkEmbeddings); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to index point %s: %v", node.ID, err)
			}
		}
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Get(ctx context.Context, req *qpb.GetPoints) (*qpb.GetResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids are required")
	}

	includeVectors, requestedVectorNames := withVectorsSelection(req.WithVectors)

	results := make([]*qpb.RetrievedPoint, 0, len(req.Ids))
	for _, pid := range req.Ids {
		if pid == nil {
			continue
		}
		nodeID := pointIDToNodeID(req.CollectionName, pid)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		out := &qpb.RetrievedPoint{
			Id:      pid,
			Payload: propertiesToPayload(withPayloadSelection(node.Properties, req.WithPayload)),
		}
		if includeVectors {
			out.Vectors = vectorsOutputFromNode(node, requestedVectorNames)
		}
		results = append(results, out)
	}

	return &qpb.GetResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Delete(ctx context.Context, req *qpb.DeletePoints) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if req.Points == nil {
		return nil, status.Error(codes.InvalidArgument, "points selector is required")
	}

	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.Points)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
		if s.searchService != nil {
			_ = s.searchService.RemoveNode(nodeID)
		}
		if s.vecIndex != nil {
			node, err := s.storage.GetNode(nodeID)
			if err == nil {
				s.vecIndex.deletePoint(req.CollectionName, string(nodeID), nodeVectorNames(node))
			} else {
				s.vecIndex.deletePoint(req.CollectionName, string(nodeID), nil)
			}
		}
		_ = s.storage.DeleteNode(nodeID)
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Count(ctx context.Context, req *qpb.CountPoints) (*qpb.CountResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if req.Filter != nil {
		nodes, err := s.storage.GetNodesByLabel(req.CollectionName)
		if err != nil {
			if err == storage.ErrNotFound {
				return &qpb.CountResponse{Result: &qpb.CountResult{Count: 0}, Time: time.Since(start).Seconds()}, nil
			}
			return nil, status.Errorf(codes.Internal, "failed to get points: %v", err)
		}

		count := uint64(0)
		for _, node := range nodes {
			if !isQdrantPointNode(node) {
				continue
			}
			if matchesFilter(node, req.Filter) {
				count++
			}
		}
		return &qpb.CountResponse{
			Result: &qpb.CountResult{Count: count},
			Time:   time.Since(start).Seconds(),
		}, nil
	}

	count, err := s.registry.GetPointCount(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	return &qpb.CountResponse{
		Result: &qpb.CountResult{Count: uint64(count)},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Search(ctx context.Context, req *qpb.SearchPoints) (*qpb.SearchResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetVector()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}

	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}
	if len(req.Vector) != meta.Dimensions {
		return nil, status.Errorf(codes.InvalidArgument, "vector dimension mismatch: got %d, expected %d", len(req.Vector), meta.Dimensions)
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}
	if limit > s.config.MaxTopK {
		return nil, status.Errorf(codes.InvalidArgument, "limit too large: %d > %d", limit, s.config.MaxTopK)
	}

	offset := 0
	if req.Offset != nil {
		if *req.Offset > uint64(^uint(0)>>1) {
			return nil, status.Error(codes.InvalidArgument, "offset too large")
		}
		offset = int(*req.Offset)
	}

	ef := 0
	if req.Params != nil && req.Params.HnswEf != nil {
		raw := req.Params.GetHnswEf()
		if raw > uint64(^uint(0)>>1) {
			return nil, status.Error(codes.InvalidArgument, "hnsw_ef too large")
		}
		ef = int(raw)
		// Keep `ef` sane: HNSW ef must be at least the number of candidates we need.
		need := limit + offset
		if ef > 0 && ef < need {
			ef = need
		}
	}

	minSimilarity := float64(-1)
	if req.ScoreThreshold != nil {
		minSimilarity = float64(*req.ScoreThreshold)
	}

	vectorName := ""
	if req.VectorName != nil {
		vectorName = *req.VectorName
	}

	includeVectors, requestedVectorNames := withVectorsSelection(req.WithVectors)
	includePayload := req.WithPayload != nil

	results := s.searchCollection(ctx, req.CollectionName, meta.Distance, vectorName, req.Vector, req.Filter, limit, offset, minSimilarity, ef)

	out := make([]*qpb.ScoredPoint, 0, len(results))
	for _, r := range results {
		var pid *qpb.PointId
		needNode := includePayload || includeVectors
		var nodeID storage.NodeID
		if strings.HasPrefix(r.ID, "qdrant:") {
			pid = nodeIDToPointID(storage.NodeID(r.ID))
			if needNode {
				nodeID = storage.NodeID(r.ID)
			}
		} else {
			// Fast path for vecIndex results: IDs are compact "<id>" (no "qdrant:<collection>:" prefix).
			pid = pointIDFromCompactString(r.ID)
			if needNode {
				nodeID = storage.NodeID(expandPointID(req.CollectionName, r.ID))
			}
		}

		sp := &qpb.ScoredPoint{
			Id:    pid,
			Score: float32(r.Score),
		}

		if needNode {
			node, err := s.storage.GetNode(nodeID)
			if err == nil {
				if includePayload {
					sp.Payload = propertiesToPayload(withPayloadSelection(node.Properties, req.WithPayload))
				}
				if includeVectors {
					sp.Vectors = vectorsOutputFromNode(node, requestedVectorNames)
				}
			}
		}

		out = append(out, sp)
	}

	return &qpb.SearchResponse{
		Result: out,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Scroll(ctx context.Context, req *qpb.ScrollPoints) (*qpb.ScrollResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	limit := 10
	if req.Limit != nil && *req.Limit > 0 {
		limit = int(*req.Limit)
	}

	includeVectors, requestedVectorNames := withVectorsSelection(req.WithVectors)

	nodes, err := s.storage.GetNodesByLabel(req.CollectionName)
	if err != nil {
		if err == storage.ErrNotFound {
			return &qpb.ScrollResponse{Result: nil, Time: time.Since(start).Seconds()}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get points: %v", err)
	}

	var points []*storage.Node
	for _, node := range nodes {
		if !isQdrantPointNode(node) {
			continue
		}
		if req.Filter != nil && !matchesFilter(node, req.Filter) {
			continue
		}
		points = append(points, node)
	}
	sortNodesByID(points)

	startIdx := 0
	if req.Offset != nil {
		offsetID := pointIDToNodeID(req.CollectionName, req.Offset)
		for i, node := range points {
			if node.ID == offsetID {
				startIdx = i + 1
				break
			}
		}
	}
	endIdx := startIdx + limit
	if endIdx > len(points) {
		endIdx = len(points)
	}
	page := points[startIdx:endIdx]

	out := make([]*qpb.RetrievedPoint, 0, len(page))
	for _, node := range page {
		pid := nodeIDToPointID(node.ID)
		p := &qpb.RetrievedPoint{
			Id:      pid,
			Payload: propertiesToPayload(withPayloadSelection(node.Properties, req.WithPayload)),
		}
		if includeVectors {
			p.Vectors = vectorsOutputFromNode(node, requestedVectorNames)
		}
		out = append(out, p)
	}

	var next *qpb.PointId
	if endIdx < len(points) {
		next = nodeIDToPointID(points[endIdx-1].ID)
	}

	return &qpb.ScrollResponse{
		NextPageOffset: next,
		Result:         out,
		Time:           time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) SetPayload(ctx context.Context, req *qpb.SetPayloadPoints) (*qpb.PointsOperationResponse, error) {
	return s.updatePayload(ctx, req, false)
}

func (s *PointsService) OverwritePayload(ctx context.Context, req *qpb.SetPayloadPoints) (*qpb.PointsOperationResponse, error) {
	return s.updatePayload(ctx, req, true)
}

func (s *PointsService) updatePayload(ctx context.Context, req *qpb.SetPayloadPoints, overwrite bool) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if req.Payload == nil {
		return nil, status.Error(codes.InvalidArgument, "payload is required")
	}

	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		if overwrite {
			node.Properties = payloadToProperties(req.Payload)
		} else {
			if node.Properties == nil {
				node.Properties = make(map[string]any)
			}
			for k, v := range payloadToProperties(req.Payload) {
				node.Properties[k] = v
			}
		}
		node.UpdatedAt = time.Now()
		_ = s.storage.UpdateNode(node)
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) DeletePayload(ctx context.Context, req *qpb.DeletePayloadPoints) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetKeys()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys are required")
	}

	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		for _, k := range req.Keys {
			delete(node.Properties, k)
		}
		node.UpdatedAt = time.Now()
		_ = s.storage.UpdateNode(node)
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) ClearPayload(ctx context.Context, req *qpb.ClearPayloadPoints) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if req.Points == nil {
		return nil, status.Error(codes.InvalidArgument, "points selector is required")
	}

	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.Points)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		node.Properties = nil
		node.UpdatedAt = time.Now()
		_ = s.storage.UpdateNode(node)
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) UpdateVectors(ctx context.Context, req *qpb.UpdatePointVectors) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if !s.config.AllowVectorMutations {
		return nil, status.Error(codes.FailedPrecondition, "vector mutations are disabled because NornicDB-managed embeddings are enabled; set NORNICDB_EMBEDDING_ENABLED=false to allow managing vectors via Qdrant gRPC")
	}
	if len(req.GetPoints()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "points are required")
	}

	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	for _, pv := range req.Points {
		if pv == nil || pv.Id == nil || pv.Vectors == nil {
			return nil, status.Error(codes.InvalidArgument, "point id and vectors are required")
		}

		nodeID := pointIDToNodeID(req.CollectionName, pv.Id)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}

		names, vecs, err := extractVectors(pv.Vectors)
		if err != nil {
			return nil, err
		}
		for _, v := range vecs {
			if len(v) != meta.Dimensions {
				return nil, status.Errorf(codes.InvalidArgument, "vector dimension mismatch: got %d, expected %d", len(v), meta.Dimensions)
			}
		}

		upsertNodeVectors(node, names, vecs)
		node.UpdatedAt = time.Now()
		_ = s.storage.UpdateNode(node)

		if s.searchService != nil {
			_ = s.searchService.RemoveNode(node.ID)
			_ = s.searchService.IndexNode(node)
		}
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) DeleteVectors(ctx context.Context, req *qpb.DeletePointVectors) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if !s.config.AllowVectorMutations {
		return nil, status.Error(codes.FailedPrecondition, "vector mutations are disabled because NornicDB-managed embeddings are enabled; set NORNICDB_EMBEDDING_ENABLED=false to allow managing vectors via Qdrant gRPC")
	}
	if req.PointsSelector == nil {
		return nil, status.Error(codes.InvalidArgument, "points_selector is required")
	}
	if req.Vectors == nil || len(req.Vectors.Names) == 0 {
		return nil, status.Error(codes.InvalidArgument, "vectors.names is required")
	}

	nodeIDs, err := s.resolvePointsSelector(ctx, req.CollectionName, req.PointsSelector)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range nodeIDs {
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		deleteNodeVectors(node, req.Vectors.Names)
		node.UpdatedAt = time.Now()
		_ = s.storage.UpdateNode(node)

		if s.searchService != nil {
			_ = s.searchService.RemoveNode(node.ID)
			_ = s.searchService.IndexNode(node)
		}
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) SearchBatch(ctx context.Context, req *qpb.SearchBatchPoints) (*qpb.SearchBatchResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetSearchPoints()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "search_points are required")
	}

	results := make([]*qpb.BatchResult, 0, len(req.SearchPoints))
	for _, sp := range req.SearchPoints {
		if sp == nil {
			results = append(results, &qpb.BatchResult{Result: nil})
			continue
		}
		// Ensure child request uses parent collection name (SDKs typically set it).
		sp.CollectionName = req.CollectionName
		resp, err := s.Search(ctx, sp)
		if err != nil {
			return nil, err
		}
		results = append(results, &qpb.BatchResult{Result: resp.Result})
	}

	return &qpb.SearchBatchResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) Recommend(ctx context.Context, req *qpb.RecommendPoints) (*qpb.RecommendResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	limit := uint64(10)
	if req.Limit > 0 {
		limit = req.Limit
	}
	if limit > uint64(s.config.MaxTopK) {
		return nil, status.Errorf(codes.InvalidArgument, "limit too large: %d > %d", limit, s.config.MaxTopK)
	}

	using := ""
	if req.Using != nil {
		using = *req.Using
	}

	queryVec, err := s.recommendQueryVector(ctx, req.CollectionName, using, req.Positive, req.Negative, req.PositiveVectors, req.NegativeVectors)
	if err != nil {
		return nil, err
	}

	searchResp, err := s.Search(ctx, &qpb.SearchPoints{
		CollectionName: req.CollectionName,
		Vector:         queryVec,
		Filter:         req.Filter,
		Limit:          limit,
		Offset:         req.Offset,
		VectorName:     ptrString(using),
		WithVectors:    req.WithVectors,
		WithPayload:    req.WithPayload,
		ScoreThreshold: req.ScoreThreshold,
	})
	if err != nil {
		return nil, err
	}

	return &qpb.RecommendResponse{
		Result: searchResp.Result,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) RecommendBatch(ctx context.Context, req *qpb.RecommendBatchPoints) (*qpb.RecommendBatchResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetRecommendPoints()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "recommend_points are required")
	}

	results := make([]*qpb.BatchResult, 0, len(req.RecommendPoints))
	for _, r := range req.RecommendPoints {
		if r == nil {
			results = append(results, &qpb.BatchResult{Result: nil})
			continue
		}
		r.CollectionName = req.CollectionName
		resp, err := s.Recommend(ctx, r)
		if err != nil {
			return nil, err
		}
		results = append(results, &qpb.BatchResult{Result: resp.Result})
	}

	return &qpb.RecommendBatchResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) SearchGroups(ctx context.Context, req *qpb.SearchPointGroups) (*qpb.SearchGroupsResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if len(req.GetVector()) == 0 {
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
		groupSize = 10
	}

	// Oversample then group.
	searchResp, err := s.Search(ctx, &qpb.SearchPoints{
		CollectionName: req.CollectionName,
		Vector:         req.Vector,
		Filter:         req.Filter,
		Limit:          uint64(groupLimit * groupSize),
		ScoreThreshold: req.ScoreThreshold,
		VectorName:     req.VectorName,
		WithPayload:    req.WithPayload,
		WithVectors:    req.WithVectors,
	})
	if err != nil {
		return nil, err
	}

	grouped := make(map[string][]*qpb.ScoredPoint)
	for _, sp := range searchResp.Result {
		if sp == nil || sp.Payload == nil {
			continue
		}
		val, ok := sp.Payload[req.GroupBy]
		if !ok {
			continue
		}
		keyAny := valueToAny(val)
		key := fmt.Sprintf("%v", keyAny)
		if key == "" {
			continue
		}
		grouped[key] = append(grouped[key], sp)
	}

	// Stable order: by group key.
	keys := make([]string, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	groups := make([]*qpb.PointGroup, 0, groupLimit)
	for _, k := range keys {
		if len(groups) >= groupLimit {
			break
		}
		hits := grouped[k]
		if len(hits) > groupSize {
			hits = hits[:groupSize]
		}
		groups = append(groups, &qpb.PointGroup{
			Id:   &qpb.GroupId{Kind: &qpb.GroupId_StringValue{StringValue: k}},
			Hits: hits,
		})
	}

	return &qpb.SearchGroupsResponse{
		Result: &qpb.GroupsResult{Groups: groups},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) CreateFieldIndex(ctx context.Context, req *qpb.CreateFieldIndexCollection) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if req.FieldName == "" {
		return nil, status.Error(codes.InvalidArgument, "field_name is required")
	}

	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	schema := s.storage.GetSchema()
	if schema != nil {
		_ = schema.AddPropertyIndex(fmt.Sprintf("qdrant_%s_%s", req.CollectionName, req.FieldName), req.CollectionName, []string{req.FieldName})
	}

	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) DeleteFieldIndex(ctx context.Context, req *qpb.DeleteFieldIndexCollection) (*qpb.PointsOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}
	if req.FieldName == "" {
		return nil, status.Error(codes.InvalidArgument, "field_name is required")
	}

	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// NornicDB schema indexes are managed internally; treat as a no-op.
	return &qpb.PointsOperationResponse{
		Result: &qpb.UpdateResult{Status: qpb.UpdateStatus_Completed},
		Time:   time.Since(start).Seconds(),
	}, nil
}

// Query implements Qdrant's universal query API for the most common variant:
// Query(nearest = VectorInput).
//
// This enables text/inference-shaped queries via VectorInput.Document when
// Config.EmbedQuery is provided.
func (s *PointsService) Query(ctx context.Context, req *qpb.QueryPoints) (*qpb.QueryResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	limit := uint64(10)
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}

	using := ""
	if req.Using != nil {
		using = *req.Using
	}

	if req.Query == nil {
		// No query: treat as scroll-by-id (not implemented here).
		return nil, status.Error(codes.Unimplemented, "query without Query.variant is not implemented")
	}

	switch q := req.Query.Variant.(type) {
	case *qpb.Query_Nearest:
		vec, err := s.vectorFromInput(ctx, req.CollectionName, using, q.Nearest)
		if err != nil {
			return nil, err
		}

		// Reuse Search behavior.
		searchResp, err := s.Search(ctx, &qpb.SearchPoints{
			CollectionName:   req.CollectionName,
			Vector:           vec,
			Filter:           req.Filter,
			Limit:            limit,
			ScoreThreshold:   req.ScoreThreshold,
			Offset:           req.Offset,
			VectorName:       ptrString(using),
			WithVectors:      req.WithVectors,
			WithPayload:      req.WithPayload,
			ReadConsistency:  req.ReadConsistency,
			ShardKeySelector: req.ShardKeySelector,
			Timeout:          req.Timeout,
		})
		if err != nil {
			return nil, err
		}

		// SearchResponse and QueryResponse share the same result type.
		return &qpb.QueryResponse{
			Result: searchResp.Result,
			Time:   time.Since(start).Seconds(),
		}, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "query variant %T is not implemented", q)
	}
}

func (s *PointsService) QueryBatch(ctx context.Context, req *qpb.QueryBatchPoints) (*qpb.QueryBatchResponse, error) {
	start := time.Now()

	results := make([]*qpb.BatchResult, 0, len(req.GetQueryPoints()))
	for _, q := range req.GetQueryPoints() {
		resp, err := s.Query(ctx, q)
		if err != nil {
			return nil, err
		}
		results = append(results, &qpb.BatchResult{Result: resp.Result})
	}

	return &qpb.QueryBatchResponse{
		Result: results,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *PointsService) vectorFromInput(ctx context.Context, collection string, using string, in *qpb.VectorInput) ([]float32, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "vector input is required")
	}
	switch v := in.Variant.(type) {
	case *qpb.VectorInput_Dense:
		if v.Dense == nil || len(v.Dense.Data) == 0 {
			return nil, status.Error(codes.InvalidArgument, "dense vector is required")
		}
		return v.Dense.Data, nil
	case *qpb.VectorInput_Id:
		if v.Id == nil {
			return nil, status.Error(codes.InvalidArgument, "id is required")
		}
		nodeID := pointIDToNodeID(collection, v.Id)
		node, err := s.storage.GetNode(nodeID)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "point not found: %v", err)
		}
		emb, ok := nodeVectorByName(node, using)
		if !ok {
			return nil, status.Error(codes.NotFound, "vector not found for point")
		}
		return emb, nil
	case *qpb.VectorInput_Document:
		if v.Document == nil || v.Document.Text == "" {
			return nil, status.Error(codes.InvalidArgument, "document.text is required")
		}
		if s.config.EmbedQuery == nil {
			return nil, status.Error(codes.FailedPrecondition, "text query requires embeddings; enable NornicDB embeddings and configure EmbedQuery")
		}
		emb, err := s.config.EmbedQuery(ctx, v.Document.Text)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to embed query: %v", err)
		}
		return emb, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "vector input variant %T is not implemented", v)
	}
}

func (s *PointsService) recommendQueryVector(
	ctx context.Context,
	collection string,
	using string,
	positiveIDs []*qpb.PointId,
	negativeIDs []*qpb.PointId,
	positiveVectors []*qpb.Vector,
	negativeVectors []*qpb.Vector,
) ([]float32, error) {
	var vectorsPos [][]float32
	for _, id := range positiveIDs {
		if id == nil {
			continue
		}
		node, err := s.storage.GetNode(pointIDToNodeID(collection, id))
		if err != nil {
			continue
		}
		if emb, ok := nodeVectorByName(node, using); ok {
			vectorsPos = append(vectorsPos, emb)
		}
	}
	for _, v := range positiveVectors {
		emb, err := s.vectorFromVector(ctx, v)
		if err != nil {
			return nil, err
		}
		vectorsPos = append(vectorsPos, emb)
	}

	if len(vectorsPos) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one positive example is required")
	}

	var vectorsNeg [][]float32
	for _, id := range negativeIDs {
		if id == nil {
			continue
		}
		node, err := s.storage.GetNode(pointIDToNodeID(collection, id))
		if err != nil {
			continue
		}
		if emb, ok := nodeVectorByName(node, using); ok {
			vectorsNeg = append(vectorsNeg, emb)
		}
	}
	for _, v := range negativeVectors {
		emb, err := s.vectorFromVector(ctx, v)
		if err != nil {
			return nil, err
		}
		vectorsNeg = append(vectorsNeg, emb)
	}

	pos := averageVectors(vectorsPos)
	if len(vectorsNeg) == 0 {
		return pos, nil
	}
	neg := averageVectors(vectorsNeg)
	for i := range pos {
		pos[i] = pos[i] - neg[i]
	}
	return pos, nil
}

func (s *PointsService) vectorFromVector(ctx context.Context, v *qpb.Vector) ([]float32, error) {
	if v == nil {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}
	if dense := v.GetDense(); dense != nil && len(dense.Data) > 0 {
		return dense.Data, nil
	}
	if len(v.Data) > 0 {
		return v.Data, nil
	}
	if doc := v.GetDocument(); doc != nil && doc.Text != "" {
		if s.config.EmbedQuery == nil {
			return nil, status.Error(codes.FailedPrecondition, "text query requires embeddings; enable NornicDB embeddings and configure EmbedQuery")
		}
		emb, err := s.config.EmbedQuery(ctx, doc.Text)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to embed query: %v", err)
		}
		return emb, nil
	}
	return nil, status.Error(codes.Unimplemented, "unsupported vector kind")
}

func averageVectors(vectors [][]float32) []float32 {
	if len(vectors) == 0 {
		return nil
	}
	dim := len(vectors[0])
	out := make([]float32, dim)
	count := 0
	for _, v := range vectors {
		if len(v) != dim {
			continue
		}
		for i := range v {
			out[i] += v[i]
		}
		count++
	}
	if count == 0 {
		return nil
	}
	inv := float32(1.0 / float32(count))
	for i := range out {
		out[i] *= inv
	}
	return out
}

// =============================================================================
// Helpers
// =============================================================================

type searchResult struct {
	ID    string
	Score float64
}

func (s *PointsService) searchCollection(ctx context.Context, collection string, dist qpb.Distance, vectorName string, queryVec []float32, filter *qpb.Filter, limit int, offset int, minScore float64, ef int) []searchResult {
	if s.vecIndex != nil {
		// Request extra candidates only when we need to apply payload filters.
		// For filter-free searches, returning exactly limit+offset avoids unnecessary
		// ANN work (and is closer to Qdrant's behavior).
		want := limit + offset
		if filter != nil {
			want = (limit + offset) * 10
			if want < limit+offset {
				want = limit + offset
			}
			if want > s.config.MaxTopK*10 {
				want = s.config.MaxTopK * 10
			}
		}

		cands := s.vecIndex.search(ctx, collection, len(queryVec), dist, vectorName, queryVec, want, minScore, ef)
		if len(cands) > 0 {
			if filter == nil {
				if offset > 0 {
					if offset >= len(cands) {
						return nil
					}
					cands = cands[offset:]
				}
				if len(cands) > limit {
					cands = cands[:limit]
				}
				return cands
			}

			filtered := make([]searchResult, 0, len(cands))
			for _, r := range cands {
				node, err := s.storage.GetNode(storage.NodeID(expandPointID(collection, r.ID)))
				if err != nil || !matchesFilter(node, filter) {
					continue
				}
				filtered = append(filtered, r)
				if len(filtered) >= limit+offset {
					break
				}
			}

			if offset > 0 {
				if offset >= len(filtered) {
					return nil
				}
				filtered = filtered[offset:]
			}
			if len(filtered) > limit {
				filtered = filtered[:limit]
			}
			return filtered
		}
	}

	// Use search service for default vector (vectorName == "" or "default")
	// TODO: Once IndexRegistry supports named vectors, use it for all vectorName searches
	// For named vectors, we currently use vecIndex (deprecated) or brute-force fallback
	effectiveVectorName := vectorName
	if effectiveVectorName == "" {
		effectiveVectorName = "default"
	}

	// Try search service for default vector (uses unified pipeline with HNSW/k-means)
	if s.searchService != nil && effectiveVectorName == "default" {
		// Fast path: avoid per-candidate storage lookups for type filtering.
		// Qdrant "collections" map to a stable node ID prefix: qdrant:<collection>:<id>.
		prefix := fmt.Sprintf("qdrant:%s:", collection)

		// Qdrant's score_threshold is optional; if not set, do not apply a threshold.
		// Our search.Service defaults may otherwise prune results unexpectedly.
		minSimilarity := float64(-1)
		if minScore >= 0 {
			minSimilarity = minScore
		}

		// Ask for extra candidates, since we still need to:
		// - filter to this collection by ID prefix (and drop chunk-suffixed IDs)
		// - optionally apply payload filters
		candidateLimit := (limit + offset) * 10
		if candidateLimit < limit+offset {
			candidateLimit = limit + offset
		}
		if candidateLimit > s.config.MaxTopK*10 {
			candidateLimit = s.config.MaxTopK * 10
		}

		cands, err := s.searchService.VectorSearchCandidates(ctx, queryVec, &search.SearchOptions{
			Limit:         candidateLimit,
			MinSimilarity: &minSimilarity,
		})
		if err == nil && len(cands) > 0 {
			results := make([]searchResult, 0, len(cands))
			for _, r := range cands {
				if !strings.HasPrefix(r.ID, prefix) {
					continue
				}
				if strings.Contains(r.ID, "-chunk-") {
					continue
				}
				if filter != nil {
					node, err := s.storage.GetNode(storage.NodeID(r.ID))
					if err != nil || !matchesFilter(node, filter) {
						continue
					}
				}
				results = append(results, searchResult{ID: r.ID, Score: r.Score})
				if len(results) >= limit+offset {
					// Enough results after filtering; keep tail latency bounded.
					break
				}
			}

			if offset > 0 {
				if offset >= len(results) {
					return nil
				}
				results = results[offset:]
			}
			if len(results) > limit {
				results = results[:limit]
			}
			return results
		}
	}

	// Brute-force fallback: scan all nodes and filter by label. This keeps the
	// measured path comparable to the Bolt/Cypher vector procedure and avoids
	// label-index iterator overhead on some storage backends under high concurrency.
	nodes, err := s.storage.AllNodes()
	if err != nil {
		return nil
	}

	normalizedQuery := vector.Normalize(queryVec)
	var scored []searchResult

	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if !isQdrantPointNode(node) {
			continue
		}
		if !nodeHasLabel(node, collection) {
			continue
		}
		if filter != nil && !matchesFilter(node, filter) {
			continue
		}

		emb, ok := nodeVectorByName(node, vectorName)
		if !ok {
			continue
		}

		score := scoreVector(dist, normalizedQuery, emb)
		if minScore >= 0 && score < minScore {
			continue
		}
		scored = append(scored, searchResult{ID: string(node.ID), Score: score})
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })

	if offset > 0 {
		if offset >= len(scored) {
			return nil
		}
		scored = scored[offset:]
	}
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored
}

func scoreVector(dist qpb.Distance, normalizedQuery []float32, candidate []float32) float64 {
	switch dist {
	case qpb.Distance_Dot:
		return float64(vector.DotProduct(normalizedQuery, candidate))
	case qpb.Distance_Euclid:
		return vector.EuclideanSimilarity(normalizedQuery, candidate)
	case qpb.Distance_Cosine, qpb.Distance_UnknownDistance:
		fallthrough
	default:
		// Avoid per-candidate allocations: Normalize(candidate) returns a copy.
		// CosineSimilarity computes dot/(|a||b|) without allocating.
		return vector.CosineSimilarity(normalizedQuery, candidate)
	}
}

func isQdrantPointNode(node *storage.Node) bool {
	if node == nil {
		return false
	}
	for _, label := range node.Labels {
		if label == QdrantPointLabel {
			return true
		}
	}
	return false
}

func resolvePointIDs(sel *qpb.PointsSelector) ([]*qpb.PointId, error) {
	if sel == nil {
		return nil, status.Error(codes.InvalidArgument, "points selector is required")
	}
	switch opt := sel.PointsSelectorOneOf.(type) {
	case *qpb.PointsSelector_Points:
		if opt.Points == nil || len(opt.Points.Ids) == 0 {
			return nil, status.Error(codes.InvalidArgument, "ids are required")
		}
		return opt.Points.Ids, nil
	default:
		return nil, nil
	}
}

func (s *PointsService) resolvePointsSelector(ctx context.Context, collection string, sel *qpb.PointsSelector) ([]storage.NodeID, error) {
	if sel == nil {
		return nil, status.Error(codes.InvalidArgument, "points selector is required")
	}

	switch opt := sel.PointsSelectorOneOf.(type) {
	case *qpb.PointsSelector_Points:
		ids, err := resolvePointIDs(sel)
		if err != nil {
			return nil, err
		}
		out := make([]storage.NodeID, 0, len(ids))
		for _, id := range ids {
			out = append(out, pointIDToNodeID(collection, id))
		}
		return out, nil
	case *qpb.PointsSelector_Filter:
		nodes, err := s.storage.GetNodesByLabel(collection)
		if err != nil {
			if err == storage.ErrNotFound {
				return nil, nil
			}
			return nil, status.Errorf(codes.Internal, "failed to scan points: %v", err)
		}
		out := make([]storage.NodeID, 0, len(nodes))
		for _, node := range nodes {
			if !isQdrantPointNode(node) {
				continue
			}
			if matchesFilter(node, opt.Filter) {
				out = append(out, node.ID)
			}
		}
		return out, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid points selector")
	}
}

func sortNodesByID(nodes []*storage.Node) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
}

func pointIDToNodeID(collection string, pid *qpb.PointId) storage.NodeID {
	idStr := ""
	switch opt := pid.PointIdOptions.(type) {
	case *qpb.PointId_Num:
		idStr = fmt.Sprintf("%d", opt.Num)
	case *qpb.PointId_Uuid:
		idStr = opt.Uuid
	}
	return storage.NodeID(fmt.Sprintf("qdrant:%s:%s", collection, idStr))
}

func nodeIDToPointID(id storage.NodeID) *qpb.PointId {
	nodeIDStr := string(id)
	parts := splitNodeID(nodeIDStr)
	if len(parts) != 3 {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: nodeIDStr}}
	}
	raw := parts[2]
	if n, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: n}}
	}
	return &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: raw}}
}

func pointIDFromCompactString(raw string) *qpb.PointId {
	// Qdrant PointId is either uint64 or UUID string.
	if n, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: n}}
	}
	return &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: raw}}
}

func splitNodeID(nodeID string) []string {
	// Expected: qdrant:<collection>:<id>
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(nodeID); i++ {
		if nodeID[i] == ':' {
			parts = append(parts, nodeID[start:i])
			start = i + 1
			if len(parts) == 2 {
				break
			}
		}
	}
	parts = append(parts, nodeID[start:])
	return parts
}

// extractVectors extracts one or more dense vectors from a Vectors message.
// We support Qdrant's single unnamed vector and named vectors. Other vector
// kinds (sparse/multi/document) are rejected for storage.
func extractVectors(v *qpb.Vectors) ([]string, [][]float32, error) {
	if v == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "vectors is required")
	}

	switch opt := v.VectorsOptions.(type) {
	case *qpb.Vectors_Vector:
		vec, err := extractDenseFromVector(opt.Vector)
		if err != nil {
			return nil, nil, err
		}
		return []string{""}, [][]float32{vec}, nil
	case *qpb.Vectors_Vectors:
		if opt.Vectors == nil || len(opt.Vectors.Vectors) == 0 {
			return nil, nil, status.Error(codes.InvalidArgument, "vectors are required")
		}
		keys := make([]string, 0, len(opt.Vectors.Vectors))
		for k := range opt.Vectors.Vectors {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		names := make([]string, 0, len(keys))
		vecs := make([][]float32, 0, len(keys))
		for _, name := range keys {
			vec := opt.Vectors.Vectors[name]
			dense, err := extractDenseFromVector(vec)
			if err != nil {
				return nil, nil, err
			}
			names = append(names, name)
			vecs = append(vecs, dense)
		}
		return names, vecs, nil
	default:
		return nil, nil, status.Error(codes.InvalidArgument, "unsupported vectors type")
	}
}

func extractDenseFromVector(v *qpb.Vector) ([]float32, error) {
	if v == nil {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}
	if dense := v.GetDense(); dense != nil && len(dense.Data) > 0 {
		return dense.Data, nil
	}
	if len(v.Data) > 0 {
		return v.Data, nil
	}
	return nil, status.Error(codes.InvalidArgument, "dense vector data is required")
}

func nodeVectorNames(node *storage.Node) []string {
	if node == nil || node.Properties == nil {
		return nil
	}
	raw, ok := node.Properties[qdrantVectorNamesKey]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func setNodeVectorNames(node *storage.Node, names []string) {
	if node.Properties == nil {
		node.Properties = make(map[string]any)
	}
	if len(names) == 0 || (len(names) == 1 && names[0] == "") {
		delete(node.Properties, qdrantVectorNamesKey)
		return
	}
	out := make([]string, len(names))
	copy(out, names)
	node.Properties[qdrantVectorNamesKey] = out
}

func nodeVectorIndexByName(node *storage.Node, vectorName string) (int, bool) {
	if node == nil || len(node.ChunkEmbeddings) == 0 {
		return 0, false
	}
	names := nodeVectorNames(node)
	if len(names) == 0 {
		if vectorName == "" {
			return 0, true
		}
		return 0, false
	}
	for i, name := range names {
		if name == vectorName {
			if i < len(node.ChunkEmbeddings) {
				return i, true
			}
			return 0, false
		}
	}
	if vectorName == "" && len(node.ChunkEmbeddings) > 0 {
		return 0, true
	}
	return 0, false
}

func nodeVectorByName(node *storage.Node, vectorName string) ([]float32, bool) {
	// First, try NamedEmbeddings (new data model)
	effectiveName := vectorName
	if effectiveName == "" {
		effectiveName = "default"
	}
	if node.NamedEmbeddings != nil {
		if vec, ok := node.NamedEmbeddings[effectiveName]; ok && len(vec) > 0 {
			return vec, true
		}
	}

	// Fall back to ChunkEmbeddings for backward compatibility
	idx, ok := nodeVectorIndexByName(node, vectorName)
	if !ok || idx >= len(node.ChunkEmbeddings) {
		return nil, false
	}
	vec := node.ChunkEmbeddings[idx]
	if len(vec) == 0 {
		return nil, false
	}
	return vec, true
}

func nodeHasLabel(node *storage.Node, label string) bool {
	if node == nil || label == "" {
		return false
	}
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

func upsertNodeVectors(node *storage.Node, names []string, vectors [][]float32) {
	if node == nil || len(vectors) == 0 {
		return
	}

	// Write vectors to NamedEmbeddings instead of ChunkEmbeddings
	// Qdrant unnamed vector → NamedEmbeddings["default"]
	// Qdrant named vectors → NamedEmbeddings[name]
	if node.NamedEmbeddings == nil {
		node.NamedEmbeddings = make(map[string][]float32)
	}
	for i, name := range names {
		if i >= len(vectors) {
			break
		}
		// Map empty string to "default" for unnamed vectors
		vectorName := name
		if vectorName == "" {
			vectorName = "default"
		}
		node.NamedEmbeddings[vectorName] = vectors[i]
	}

	// Keep ChunkEmbeddings for backward compatibility during migration
	// TODO: Remove ChunkEmbeddings once all code paths use NamedEmbeddings
	node.ChunkEmbeddings = vectors
	setNodeVectorNames(node, names)
}

func deleteNodeVectors(node *storage.Node, names []string) {
	if node == nil || len(node.ChunkEmbeddings) == 0 {
		return
	}

	existingNames := nodeVectorNames(node)
	if len(existingNames) == 0 {
		// Single unnamed vector.
		for _, n := range names {
			if n == "" {
				node.ChunkEmbeddings = nil
				setNodeVectorNames(node, nil)
				return
			}
		}
		return
	}

	toDelete := make(map[string]struct{}, len(names))
	for _, n := range names {
		toDelete[n] = struct{}{}
	}

	newNames := make([]string, 0, len(existingNames))
	newEmbeddings := make([][]float32, 0, len(existingNames))
	for i, n := range existingNames {
		if _, ok := toDelete[n]; ok {
			continue
		}
		if i < len(node.ChunkEmbeddings) {
			newNames = append(newNames, n)
			newEmbeddings = append(newEmbeddings, node.ChunkEmbeddings[i])
		}
	}
	node.ChunkEmbeddings = newEmbeddings
	setNodeVectorNames(node, newNames)
}

func withPayloadSelection(props map[string]any, selector *qpb.WithPayloadSelector) map[string]any {
	if props == nil {
		return nil
	}

	// Strip internal keys.
	clean := func() map[string]any {
		out := make(map[string]any, len(props))
		for k, v := range props {
			if k == qdrantVectorNamesKey {
				continue
			}
			if len(k) >= 8 && k[:8] == "_qdrant_" {
				continue
			}
			out[k] = v
		}
		return out
	}

	if selector == nil {
		return clean()
	}

	switch opt := selector.SelectorOptions.(type) {
	case *qpb.WithPayloadSelector_Enable:
		if !opt.Enable {
			return nil
		}
		return clean()
	case *qpb.WithPayloadSelector_Include:
		out := make(map[string]any)
		if opt.Include == nil || len(opt.Include.Fields) == 0 {
			return clean()
		}
		for _, f := range opt.Include.Fields {
			if v, ok := props[f]; ok {
				out[f] = v
			}
		}
		return out
	case *qpb.WithPayloadSelector_Exclude:
		out := clean()
		if opt.Exclude == nil {
			return out
		}
		for _, f := range opt.Exclude.Fields {
			delete(out, f)
		}
		return out
	default:
		return clean()
	}
}

func withVectorsSelection(selector *qpb.WithVectorsSelector) (include bool, names []string) {
	if selector == nil {
		return false, nil
	}
	switch opt := selector.SelectorOptions.(type) {
	case *qpb.WithVectorsSelector_Enable:
		return opt.Enable, nil
	case *qpb.WithVectorsSelector_Include:
		if opt.Include == nil {
			return true, nil
		}
		return true, opt.Include.Names
	default:
		return false, nil
	}
}

func vectorsOutputFromNode(node *storage.Node, requestedNames []string) *qpb.VectorsOutput {
	if node == nil {
		return nil
	}

	// First, try NamedEmbeddings (new data model)
	if node.NamedEmbeddings != nil && len(node.NamedEmbeddings) > 0 {
		// Determine which names to include
		includeNames := make([]string, 0)
		if len(requestedNames) > 0 {
			// Include only requested names
			for _, reqName := range requestedNames {
				effectiveName := reqName
				if effectiveName == "" {
					effectiveName = "default"
				}
				if _, ok := node.NamedEmbeddings[effectiveName]; ok {
					includeNames = append(includeNames, reqName) // Keep original name for output
				}
			}
		} else {
			// Include all names (map keys)
			for name := range node.NamedEmbeddings {
				// Map "default" back to empty string for unnamed vector
				if name == "default" {
					includeNames = append(includeNames, "")
				} else {
					includeNames = append(includeNames, name)
				}
			}
		}

		if len(includeNames) == 0 {
			return nil
		}

		// Single unnamed vector
		if len(includeNames) == 1 && includeNames[0] == "" {
			vec, ok := node.NamedEmbeddings["default"]
			if !ok || len(vec) == 0 {
				return nil
			}
			return &qpb.VectorsOutput{
				VectorsOptions: &qpb.VectorsOutput_Vector{
					Vector: &qpb.VectorOutput{
						Vector: &qpb.VectorOutput_Dense{Dense: &qpb.DenseVector{Data: vec}},
					},
				},
			}
		}

		// Multiple named vectors
		out := make(map[string]*qpb.VectorOutput, len(includeNames))
		for _, name := range includeNames {
			effectiveName := name
			if effectiveName == "" {
				effectiveName = "default"
			}
			vec, ok := node.NamedEmbeddings[effectiveName]
			if !ok || len(vec) == 0 {
				continue
			}
			out[name] = &qpb.VectorOutput{
				Vector: &qpb.VectorOutput_Dense{Dense: &qpb.DenseVector{Data: vec}},
			}
		}
		if len(out) == 0 {
			return nil
		}
		return &qpb.VectorsOutput{
			VectorsOptions: &qpb.VectorsOutput_Vectors{
				Vectors: &qpb.NamedVectorsOutput{Vectors: out},
			},
		}
	}

	// Fall back to ChunkEmbeddings for backward compatibility
	if len(node.ChunkEmbeddings) == 0 {
		return nil
	}

	names := nodeVectorNames(node)
	if len(names) == 0 {
		// Single unnamed vector.
		for _, requested := range requestedNames {
			if requested != "" {
				return nil
			}
		}
		return &qpb.VectorsOutput{
			VectorsOptions: &qpb.VectorsOutput_Vector{
				Vector: &qpb.VectorOutput{
					Vector: &qpb.VectorOutput_Dense{Dense: &qpb.DenseVector{Data: node.ChunkEmbeddings[0]}},
				},
			},
		}
	}

	includeNames := names
	if len(requestedNames) > 0 {
		includeNames = requestedNames
	}

	out := make(map[string]*qpb.VectorOutput, len(includeNames))
	for _, name := range includeNames {
		idx, ok := nodeVectorIndexByName(node, name)
		if !ok || idx >= len(node.ChunkEmbeddings) {
			continue
		}
		out[name] = &qpb.VectorOutput{
			Vector: &qpb.VectorOutput_Dense{Dense: &qpb.DenseVector{Data: node.ChunkEmbeddings[idx]}},
		}
	}
	if len(out) == 0 {
		return nil
	}

	return &qpb.VectorsOutput{
		VectorsOptions: &qpb.VectorsOutput_Vectors{
			Vectors: &qpb.NamedVectorsOutput{Vectors: out},
		},
	}
}

func payloadToProperties(payload map[string]*qpb.Value) map[string]any {
	if payload == nil {
		return nil
	}
	props := make(map[string]any, len(payload))
	for k, v := range payload {
		props[k] = valueToAny(v)
	}
	return props
}

func propertiesToPayload(props map[string]any) map[string]*qpb.Value {
	if props == nil {
		return nil
	}
	out := make(map[string]*qpb.Value, len(props))
	for k, v := range props {
		if k == qdrantVectorNamesKey {
			continue
		}
		out[k] = anyToValue(v)
	}
	return out
}

func valueToAny(v *qpb.Value) any {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *qpb.Value_NullValue:
		return nil
	case *qpb.Value_BoolValue:
		return k.BoolValue
	case *qpb.Value_IntegerValue:
		return k.IntegerValue
	case *qpb.Value_DoubleValue:
		return k.DoubleValue
	case *qpb.Value_StringValue:
		return k.StringValue
	case *qpb.Value_ListValue:
		out := make([]any, 0, len(k.ListValue.Values))
		for _, item := range k.ListValue.Values {
			out = append(out, valueToAny(item))
		}
		return out
	case *qpb.Value_StructValue:
		m := make(map[string]any, len(k.StructValue.Fields))
		for fk, fv := range k.StructValue.Fields {
			m[fk] = valueToAny(fv)
		}
		return m
	default:
		return nil
	}
}

func anyToValue(v any) *qpb.Value {
	switch x := v.(type) {
	case nil:
		return &qpb.Value{Kind: &qpb.Value_NullValue{NullValue: qpb.NullValue_NULL_VALUE}}
	case bool:
		return &qpb.Value{Kind: &qpb.Value_BoolValue{BoolValue: x}}
	case int:
		return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: int64(x)}}
	case int64:
		return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: x}}
	case float64:
		return &qpb.Value{Kind: &qpb.Value_DoubleValue{DoubleValue: x}}
	case float32:
		return &qpb.Value{Kind: &qpb.Value_DoubleValue{DoubleValue: float64(x)}}
	case string:
		return &qpb.Value{Kind: &qpb.Value_StringValue{StringValue: x}}
	case []any:
		values := make([]*qpb.Value, 0, len(x))
		for _, item := range x {
			values = append(values, anyToValue(item))
		}
		return &qpb.Value{Kind: &qpb.Value_ListValue{ListValue: &qpb.ListValue{Values: values}}}
	case map[string]any:
		fields := make(map[string]*qpb.Value, len(x))
		for k, v := range x {
			fields[k] = anyToValue(v)
		}
		return &qpb.Value{Kind: &qpb.Value_StructValue{StructValue: &qpb.Struct{Fields: fields}}}
	default:
		return &qpb.Value{Kind: &qpb.Value_NullValue{NullValue: qpb.NullValue_NULL_VALUE}}
	}
}

func matchesFilter(node *storage.Node, filter *qpb.Filter) bool {
	if filter == nil {
		return true
	}

	for _, cond := range filter.Must {
		if !matchesCondition(node, cond) {
			return false
		}
	}
	for _, cond := range filter.MustNot {
		if matchesCondition(node, cond) {
			return false
		}
	}
	if len(filter.Should) > 0 {
		anyMatch := false
		for _, cond := range filter.Should {
			if matchesCondition(node, cond) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false
		}
	}
	return true
}

func matchesCondition(node *storage.Node, cond *qpb.Condition) bool {
	if cond == nil {
		return true
	}
	switch opt := cond.ConditionOneOf.(type) {
	case *qpb.Condition_Field:
		return matchesFieldCondition(node, opt.Field)
	case *qpb.Condition_HasId:
		for _, id := range opt.HasId.HasId {
			if id == nil {
				continue
			}
			nodePointID := nodeIDToPointID(node.ID)
			if nodePointID != nil && pointIDsEqual(nodePointID, id) {
				return true
			}
		}
		return false
	case *qpb.Condition_Filter:
		return matchesFilter(node, opt.Filter)
	default:
		// Unsupported condition kinds are treated as non-matching.
		return false
	}
}

func pointIDsEqual(a, b *qpb.PointId) bool {
	if a == nil || b == nil {
		return false
	}
	switch av := a.PointIdOptions.(type) {
	case *qpb.PointId_Num:
		bv, ok := b.PointIdOptions.(*qpb.PointId_Num)
		return ok && bv.Num == av.Num
	case *qpb.PointId_Uuid:
		bv, ok := b.PointIdOptions.(*qpb.PointId_Uuid)
		return ok && bv.Uuid == av.Uuid
	default:
		return false
	}
}

func matchesFieldCondition(node *storage.Node, cond *qpb.FieldCondition) bool {
	if node == nil || cond == nil {
		return false
	}
	if node.Properties == nil {
		return false
	}
	value, ok := node.Properties[cond.Key]
	if !ok {
		return false
	}

	if cond.Match != nil && !matchesMatch(value, cond.Match) {
		return false
	}
	if cond.Range != nil && !matchesRange(value, cond.Range) {
		return false
	}
	return true
}

func matchesMatch(v any, m *qpb.Match) bool {
	if m == nil {
		return true
	}
	switch mv := m.MatchValue.(type) {
	case *qpb.Match_Keyword:
		s, ok := v.(string)
		return ok && s == mv.Keyword
	case *qpb.Match_Integer:
		n, ok := v.(int64)
		return ok && n == mv.Integer
	case *qpb.Match_Boolean:
		b, ok := v.(bool)
		return ok && b == mv.Boolean
	default:
		return false
	}
}

func matchesRange(v any, r *qpb.Range) bool {
	if r == nil {
		return true
	}

	var f float64
	switch n := v.(type) {
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case float64:
		f = n
	case float32:
		f = float64(n)
	default:
		return false
	}

	if r.Lt != nil && !(f < *r.Lt) {
		return false
	}
	if r.Gt != nil && !(f > *r.Gt) {
		return false
	}
	if r.Gte != nil && !(f >= *r.Gte) {
		return false
	}
	if r.Lte != nil && !(f <= *r.Lte) {
		return false
	}
	return true
}

func ptrString(v string) *string { return &v }

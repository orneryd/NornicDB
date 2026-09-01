package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var errGraphForbidden = fmt.Errorf("graph access forbidden")
var errGraphPathLimitExceeded = fmt.Errorf("path search limit exceeded before target was found")

const maxGraphTemporalDiffNodeIDs = 200

type graphRequest struct {
	NodeIDs           []string `json:"node_ids,omitempty"`
	ExistingNodeIDs   []string `json:"existing_node_ids,omitempty"`
	ExistingEdgeIDs   []string `json:"existing_edge_ids,omitempty"`
	SourceNodeID      string   `json:"source_node_id,omitempty"`
	TargetNodeID      string   `json:"target_node_id,omitempty"`
	Depth             int      `json:"depth,omitempty"`
	Limit             int      `json:"limit,omitempty"`
	Labels            []string `json:"labels,omitempty"`
	RelationshipTypes []string `json:"relationship_types,omitempty"`
	AsOf              string   `json:"as_of,omitempty"`
	CompareTo         string   `json:"compare_to,omitempty"`
}

type graphNodePayload struct {
	ID         string                 `json:"id"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
	Score      *float64               `json:"score,omitempty"`
	Status     string                 `json:"status,omitempty"`
}

type graphEdgePayload struct {
	ID         string                 `json:"id"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Semantic   bool                   `json:"semantic,omitempty"`
	Status     string                 `json:"status,omitempty"`
}

type graphMetaPayload struct {
	Database      string `json:"database"`
	GeneratedFrom string `json:"generated_from"`
	Depth         int    `json:"depth,omitempty"`
	AsOf          string `json:"as_of,omitempty"`
	CompareTo     string `json:"compare_to,omitempty"`
	NodeCount     int    `json:"node_count"`
	EdgeCount     int    `json:"edge_count"`
	Truncated     bool   `json:"truncated"`
}

type graphPayload struct {
	Nodes []graphNodePayload `json:"nodes"`
	Edges []graphEdgePayload `json:"edges"`
	Meta  graphMetaPayload   `json:"meta"`
}

type graphFilterSet struct {
	labels            map[string]struct{}
	relationshipTypes map[string]struct{}
}

type graphCollection struct {
	nodes     map[string]graphNodePayload
	edges     map[string]graphEdgePayload
	truncated bool
}

func normalizeGraphNodeIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func (s *Server) handleGraphNeighborhood(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req graphRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}
	req.NodeIDs = normalizeGraphNodeIDs(req.NodeIDs)
	if len(req.NodeIDs) == 0 {
		s.writeRequestFieldRequired(w, r, "node_ids")
		return
	}
	if strings.TrimSpace(req.AsOf) != "" {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.HistoricalNeighborhoodRoute(), ErrBadRequest)
		return
	}

	if req.Depth <= 0 {
		req.Depth = 1
	}
	filterSet := newGraphFilterSet(req.Labels, req.RelationshipTypes)
	dbName, engine, err := s.resolveGraphStorage(r)
	if err != nil {
		s.writeGraphResolveError(w, r, err)
		return
	}

	collection, err := s.collectLatestNeighborhood(r.Context(), engine, req.NodeIDs, req.Depth, req.Limit, filterSet)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, collection.payload(graphMetaPayload{
		Database:      dbName,
		GeneratedFrom: "node",
		Depth:         req.Depth,
	}))
}

func (s *Server) handleGraphExpand(w http.ResponseWriter, r *http.Request) {
	s.handleGraphNeighborhood(w, r)
}

func (s *Server) handleGraphPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req graphRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}
	if strings.TrimSpace(req.SourceNodeID) == "" || strings.TrimSpace(req.TargetNodeID) == "" {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.PathNodeIDsRequired(), ErrBadRequest)
		return
	}
	if strings.TrimSpace(req.AsOf) != "" {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.HistoricalPathRoute(), ErrBadRequest)
		return
	}

	filterSet := newGraphFilterSet(req.Labels, req.RelationshipTypes)
	dbName, engine, err := s.resolveGraphStorage(r)
	if err != nil {
		s.writeGraphResolveError(w, r, err)
		return
	}

	collection, err := s.collectLatestPath(r.Context(), engine, req.SourceNodeID, req.TargetNodeID, req.Limit, filterSet)
	if err != nil {
		status := http.StatusInternalServerError
		if err == storage.ErrNotFound {
			status = http.StatusNotFound
		} else if err == errGraphPathLimitExceeded {
			status = http.StatusBadRequest
		}
		s.writeError(w, status, err.Error(), err)
		return
	}

	s.writeJSON(w, http.StatusOK, collection.payload(graphMetaPayload{
		Database:      dbName,
		GeneratedFrom: "query",
	}))
}

func (s *Server) handleGraphTemporal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req graphRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}
	req.NodeIDs = normalizeGraphNodeIDs(req.NodeIDs)
	if len(req.NodeIDs) == 0 {
		s.writeRequestFieldRequired(w, r, "node_ids")
		return
	}
	if len(req.NodeIDs) > maxGraphTemporalDiffNodeIDs {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.TemporalGraphNodeLimitExceeded(maxGraphTemporalDiffNodeIDs), ErrBadRequest)
		return
	}
	version, err := parseGraphVersion(req.AsOf)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusBadRequest, err, ErrBadRequest)
		return
	}

	filterSet := newGraphFilterSet(req.Labels, req.RelationshipTypes)
	dbName, engine, err := s.resolveGraphStorage(r)
	if err != nil {
		s.writeGraphResolveError(w, r, err)
		return
	}

	collection, err := s.collectSnapshotInducedSubgraph(engine, req.NodeIDs, version, filterSet)
	if err != nil {
		if err == storage.ErrNotImplemented {
			s.writeTemporalGraphReconstructionUnsupported(w, r, err)
			return
		}
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, collection.payload(graphMetaPayload{
		Database:      dbName,
		GeneratedFrom: "node",
		AsOf:          version.CommitTimestamp.UTC().Format(time.RFC3339Nano),
	}))
}

func (s *Server) handleGraphDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req graphRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}
	req.NodeIDs = normalizeGraphNodeIDs(req.NodeIDs)
	if len(req.NodeIDs) == 0 {
		s.writeRequestFieldRequired(w, r, "node_ids")
		return
	}
	if len(req.NodeIDs) > maxGraphTemporalDiffNodeIDs {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.DiffGraphNodeLimitExceeded(maxGraphTemporalDiffNodeIDs), ErrBadRequest)
		return
	}
	if strings.TrimSpace(req.AsOf) == "" {
		s.writeRequestFieldRequired(w, r, "as_of")
		return
	}

	targetVersion, err := parseGraphVersion(req.AsOf)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusBadRequest, err, ErrBadRequest)
		return
	}

	filterSet := newGraphFilterSet(req.Labels, req.RelationshipTypes)
	dbName, engine, err := s.resolveGraphStorage(r)
	if err != nil {
		s.writeGraphResolveError(w, r, err)
		return
	}

	var baseline graphCollection
	var target graphCollection
	compareLabel := "current"
	if strings.TrimSpace(req.CompareTo) != "" {
		baselineVersion, versionErr := parseGraphVersionForField(req.CompareTo, "compare_to")
		if versionErr != nil {
			s.writeBoundaryError(w, r, http.StatusBadRequest, versionErr, ErrBadRequest)
			return
		}
		baseline, err = s.collectSnapshotInducedSubgraph(engine, req.NodeIDs, baselineVersion, filterSet)
		if err != nil {
			if err == storage.ErrNotImplemented {
				s.writeTemporalGraphDiffUnsupported(w, r, err)
				return
			}
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}
		compareLabel = baselineVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano)

		target, err = s.collectSnapshotInducedSubgraph(engine, req.NodeIDs, targetVersion, filterSet)
		if err != nil {
			if err == storage.ErrNotImplemented {
				s.writeTemporalGraphDiffUnsupported(w, r, err)
				return
			}
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}
	} else {
		baseline, err = s.collectLatestInducedSubgraph(engine, req.NodeIDs, filterSet)
		if err != nil {
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}

		target, err = s.collectSnapshotInducedSubgraph(engine, req.NodeIDs, targetVersion, filterSet)
		if err != nil {
			if err == storage.ErrNotImplemented {
				s.writeTemporalGraphDiffUnsupported(w, r, err)
				return
			}
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}
	}

	diff := diffGraphCollections(baseline, target)
	s.writeJSON(w, http.StatusOK, diff.payload(graphMetaPayload{
		Database:      dbName,
		GeneratedFrom: "diff",
		AsOf:          targetVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano),
		CompareTo:     compareLabel,
	}))
}

func (s *Server) resolveGraphStorage(r *http.Request) (string, storage.Engine, error) {
	dbName := strings.TrimSpace(r.PathValue("database"))
	if dbName == "" {
		return "", nil, fmt.Errorf("database path parameter is required")
	}

	claims := getClaims(r)
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		return "", nil, errGraphForbidden
	}
	if claims != nil && !s.getResolvedAccess(claims, dbName).Read {
		return "", nil, errGraphForbidden
	}
	if s.dbManager.IsCompositeDatabase(dbName) {
		return "", nil, fmt.Errorf("graph endpoints on composite database '%s' are not supported; target a constituent database explicitly", dbName)
	}

	engine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return "", nil, err
	}
	return dbName, engine, nil
}

func (s *Server) writeGraphResolveError(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	if err == errGraphForbidden {
		s.writeLocalizedNeo4jError(w, r, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", localization.GraphDatabaseAccessDenied())
		return
	}
	message := err.Error()
	status := http.StatusBadRequest
	if errors.Is(err, multidb.ErrDatabaseNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, multidb.ErrDatabaseOffline) {
		status = http.StatusServiceUnavailable
	}
	s.writeError(w, status, message, err)
}

func newGraphFilterSet(labels, relationshipTypes []string) graphFilterSet {
	set := graphFilterSet{
		labels:            make(map[string]struct{}),
		relationshipTypes: make(map[string]struct{}),
	}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label != "" {
			set.labels[label] = struct{}{}
		}
	}
	for _, relType := range relationshipTypes {
		relType = strings.TrimSpace(relType)
		if relType != "" {
			set.relationshipTypes[relType] = struct{}{}
		}
	}
	return set
}

func (f graphFilterSet) allowNode(node *storage.Node) bool {
	if node == nil {
		return false
	}
	if len(f.labels) == 0 {
		return true
	}
	for _, label := range node.Labels {
		if _, ok := f.labels[label]; ok {
			return true
		}
	}
	return false
}

func (f graphFilterSet) allowEdge(edge *storage.Edge) bool {
	if edge == nil {
		return false
	}
	if len(f.relationshipTypes) == 0 {
		return true
	}
	_, ok := f.relationshipTypes[edge.Type]
	return ok
}

func newGraphCollection() graphCollection {
	return graphCollection{
		nodes: make(map[string]graphNodePayload),
		edges: make(map[string]graphEdgePayload),
	}
}

func (c *graphCollection) addNode(node *storage.Node, status string) {
	if node == nil {
		return
	}
	id := string(node.ID)
	payload := graphNodePayload{
		ID:         id,
		Labels:     append([]string(nil), node.Labels...),
		Properties: cloneInterfaceMap(node.Properties),
		Status:     status,
	}
	if existing, ok := c.nodes[id]; ok {
		if existing.Status == "" && status != "" {
			payload.Status = status
		} else {
			payload.Status = existing.Status
		}
	}
	c.nodes[id] = payload
}

func (c *graphCollection) addEdge(edge *storage.Edge, status string) {
	if edge == nil {
		return
	}
	id := string(edge.ID)
	payload := graphEdgePayload{
		ID:         id,
		Source:     string(edge.StartNode),
		Target:     string(edge.EndNode),
		Type:       edge.Type,
		Properties: cloneInterfaceMap(edge.Properties),
		Semantic:   edge.AutoGenerated,
		Status:     status,
	}
	if existing, ok := c.edges[id]; ok {
		if existing.Status == "" && status != "" {
			payload.Status = status
		} else {
			payload.Status = existing.Status
		}
	}
	c.edges[id] = payload
}

func (c graphCollection) payload(meta graphMetaPayload) graphPayload {
	nodes := make([]graphNodePayload, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	edges := make([]graphEdgePayload, 0, len(c.edges))
	for _, edge := range c.edges {
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })

	meta.NodeCount = len(nodes)
	meta.EdgeCount = len(edges)
	meta.Truncated = c.truncated

	return graphPayload{Nodes: nodes, Edges: edges, Meta: meta}
}

func (s *Server) collectLatestNeighborhood(ctx context.Context, engine storage.Engine, seedIDs []string, depth, limit int, filters graphFilterSet) (graphCollection, error) {
	collection := newGraphCollection()
	type queueEntry struct {
		nodeID string
		depth  int
	}
	queue := make([]queueEntry, 0, len(seedIDs))
	visited := make(map[string]int, len(seedIDs))
	maxNodes := limit
	if maxNodes <= 0 {
		maxNodes = 500
	}

	for _, seedID := range seedIDs {
		seedID = strings.TrimSpace(seedID)
		if seedID == "" {
			continue
		}
		if _, seen := visited[seedID]; seen {
			continue
		}
		if len(collection.nodes) >= maxNodes {
			collection.truncated = true
			continue
		}
		node, err := engine.GetNode(storage.NodeID(seedID))
		if err != nil || node == nil {
			continue
		}
		collection.addNode(node, "")
		queue = append(queue, queueEntry{nodeID: seedID, depth: 0})
		visited[seedID] = 0
	}

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return collection, ctx.Err()
		default:
		}
		current := queue[0]
		queue = queue[1:]
		if current.depth >= depth {
			continue
		}

		edges, err := graphEdgesForNode(ctx, engine, storage.NodeID(current.nodeID))
		if err != nil {
			return collection, err
		}
		for _, edge := range edges {
			select {
			case <-ctx.Done():
				return collection, ctx.Err()
			default:
			}
			if !filters.allowEdge(edge) {
				continue
			}
			otherID := string(edge.StartNode)
			if otherID == current.nodeID {
				otherID = string(edge.EndNode)
			}
			neighbor, err := engine.GetNode(storage.NodeID(otherID))
			if err != nil || neighbor == nil {
				continue
			}
			if !filters.allowNode(neighbor) {
				continue
			}

			nextDepth := current.depth + 1
			prevDepth, seen := visited[otherID]
			if !seen && len(collection.nodes) >= maxNodes {
				collection.truncated = true
				continue
			}

			if !seen {
				collection.addNode(neighbor, "")
			}
			collection.addEdge(edge, "")
			if !seen || nextDepth < prevDepth {
				visited[otherID] = nextDepth
				queue = append(queue, queueEntry{nodeID: otherID, depth: nextDepth})
			}
		}
	}

	return collection, nil
}

func (s *Server) collectLatestPath(ctx context.Context, engine storage.Engine, sourceID, targetID string, limit int, filters graphFilterSet) (graphCollection, error) {
	if sourceID == targetID {
		collection := newGraphCollection()
		node, err := engine.GetNode(storage.NodeID(sourceID))
		if err != nil || node == nil {
			return collection, storage.ErrNotFound
		}
		collection.addNode(node, "")
		return collection, nil
	}

	type predecessor struct {
		from string
		edge *storage.Edge
	}

	queue := []string{sourceID}
	visited := map[string]struct{}{sourceID: {}}
	prev := map[string]predecessor{}
	maxVisited := limit
	if maxVisited <= 0 {
		maxVisited = 500
	}

	found := false
	limitExceeded := false
	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return graphCollection{}, ctx.Err()
		default:
		}
		current := queue[0]
		queue = queue[1:]

		edges, err := graphEdgesForNode(ctx, engine, storage.NodeID(current))
		if err != nil {
			return graphCollection{}, err
		}
		for _, edge := range edges {
			select {
			case <-ctx.Done():
				return graphCollection{}, ctx.Err()
			default:
			}
			if !filters.allowEdge(edge) {
				continue
			}
			nextID := string(edge.StartNode)
			if nextID == current {
				nextID = string(edge.EndNode)
			}
			node, err := engine.GetNode(storage.NodeID(nextID))
			if err != nil || node == nil || !filters.allowNode(node) {
				continue
			}
			if _, ok := visited[nextID]; ok {
				continue
			}
			if len(visited) >= maxVisited {
				limitExceeded = true
				continue
			}
			visited[nextID] = struct{}{}
			prev[nextID] = predecessor{from: current, edge: edge}
			if nextID == targetID {
				found = true
				break
			}
			queue = append(queue, nextID)
		}
		if found {
			break
		}
	}

	if !found {
		if limitExceeded {
			return graphCollection{}, errGraphPathLimitExceeded
		}
		return graphCollection{}, storage.ErrNotFound
	}

	collection := newGraphCollection()
	current := targetID
	for {
		node, err := engine.GetNode(storage.NodeID(current))
		if err == nil && node != nil {
			collection.addNode(node, "")
		}
		if current == sourceID {
			break
		}
		step, ok := prev[current]
		if !ok {
			break
		}
		collection.addEdge(step.edge, "")
		current = step.from
	}

	return collection, nil
}

func (s *Server) collectLatestInducedSubgraph(engine storage.Engine, nodeIDs []string, filters graphFilterSet) (graphCollection, error) {
	collection := newGraphCollection()
	visible := normalizeNodeIDs(nodeIDs)
	for _, nodeID := range visible {
		node, err := engine.GetNode(storage.NodeID(nodeID))
		if err != nil || node == nil || !filters.allowNode(node) {
			continue
		}
		collection.addNode(node, "")
	}

	resolvedIDs := sortedNodeIDs(collection.nodes)
	resolvedSet := make(map[storage.NodeID]struct{}, len(resolvedIDs))
	for _, id := range resolvedIDs {
		resolvedSet[storage.NodeID(id)] = struct{}{}
	}
	for _, startID := range resolvedIDs {
		edges, err := engine.GetOutgoingEdges(storage.NodeID(startID))
		if err != nil {
			return collection, err
		}
		for _, edge := range edges {
			if edge == nil || edge.StartNode == edge.EndNode {
				continue
			}
			if _, ok := resolvedSet[edge.StartNode]; !ok {
				continue
			}
			if _, ok := resolvedSet[edge.EndNode]; !ok {
				continue
			}
			if filters.allowEdge(edge) {
				collection.addEdge(edge, "")
			}
		}
	}

	return collection, nil
}

func (s *Server) collectSnapshotInducedSubgraph(engine storage.Engine, nodeIDs []string, version storage.MVCCVersion, filters graphFilterSet) (graphCollection, error) {
	provider, ok := engine.(storage.MVCCVisibilityEngine)
	if !ok {
		return graphCollection{}, storage.ErrNotImplemented
	}
	indexed, ok := engine.(storage.MVCCIndexedVisibilityEngine)
	if !ok {
		return graphCollection{}, storage.ErrNotImplemented
	}

	collection := newGraphCollection()
	for _, nodeID := range normalizeNodeIDs(nodeIDs) {
		node, err := provider.GetNodeVisibleAt(storage.NodeID(nodeID), version)
		if err != nil || node == nil || !filters.allowNode(node) {
			continue
		}
		collection.addNode(node, "")
	}

	resolvedIDs := sortedNodeIDs(collection.nodes)
	if len(resolvedIDs) == 0 {
		return collection, nil
	}
	resolvedSet := make(map[storage.NodeID]struct{}, len(resolvedIDs))
	for _, id := range resolvedIDs {
		resolvedSet[storage.NodeID(id)] = struct{}{}
	}
	edges, err := indexed.GetEdgesByTypeVisibleAt("", version)
	if err != nil {
		return collection, err
	}
	for _, edge := range edges {
		if edge == nil || edge.StartNode == edge.EndNode {
			continue
		}
		if _, ok := resolvedSet[edge.StartNode]; !ok {
			continue
		}
		if _, ok := resolvedSet[edge.EndNode]; !ok {
			continue
		}
		if filters.allowEdge(edge) {
			collection.addEdge(edge, "")
		}
	}

	return collection, nil
}

func diffGraphCollections(baseline, target graphCollection) graphCollection {
	out := newGraphCollection()

	allNodeIDs := make(map[string]struct{}, len(baseline.nodes)+len(target.nodes))
	for id := range baseline.nodes {
		allNodeIDs[id] = struct{}{}
	}
	for id := range target.nodes {
		allNodeIDs[id] = struct{}{}
	}
	for id := range allNodeIDs {
		before, hadBefore := baseline.nodes[id]
		after, hadAfter := target.nodes[id]
		switch {
		case hadBefore && hadAfter:
			if !sameNodePayload(before, after) {
				after.Status = "changed"
				out.nodes[id] = after
			}
		case hadAfter:
			after.Status = "added"
			out.nodes[id] = after
		default:
			before.Status = "removed"
			out.nodes[id] = before
		}
	}

	allEdgeIDs := make(map[string]struct{}, len(baseline.edges)+len(target.edges))
	for id := range baseline.edges {
		allEdgeIDs[id] = struct{}{}
	}
	for id := range target.edges {
		allEdgeIDs[id] = struct{}{}
	}
	for id := range allEdgeIDs {
		before, hadBefore := baseline.edges[id]
		after, hadAfter := target.edges[id]
		switch {
		case hadBefore && hadAfter:
			if !sameEdgePayload(before, after) {
				after.Status = "changed"
				out.edges[id] = after
			}
		case hadAfter:
			after.Status = "added"
			out.edges[id] = after
		default:
			before.Status = "removed"
			out.edges[id] = before
		}
	}

	return out
}

func sameNodePayload(left, right graphNodePayload) bool {
	return reflect.DeepEqual(left.Labels, right.Labels) && reflect.DeepEqual(left.Properties, right.Properties)
}

func sameEdgePayload(left, right graphEdgePayload) bool {
	return left.Source == right.Source &&
		left.Target == right.Target &&
		left.Type == right.Type &&
		left.Semantic == right.Semantic &&
		reflect.DeepEqual(left.Properties, right.Properties)
}

func parseGraphVersion(raw string) (storage.MVCCVersion, error) {
	return parseGraphVersionForField(raw, "as_of")
}

func parseGraphVersionForField(raw, fieldName string) (storage.MVCCVersion, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return storage.MVCCVersion{}, fmt.Errorf("%s must be a valid datetime", fieldName)
	}
	if unixSeconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return storage.MVCCVersion{CommitTimestamp: time.Unix(unixSeconds, 0).UTC(), CommitSequence: ^uint64(0)}, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return storage.MVCCVersion{CommitTimestamp: parsed.UTC(), CommitSequence: ^uint64(0)}, nil
		}
	}
	return storage.MVCCVersion{}, fmt.Errorf("%s must be a valid datetime", fieldName)
}

func normalizeNodeIDs(nodeIDs []string) []string {
	seen := make(map[string]struct{}, len(nodeIDs))
	out := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	sort.Strings(out)
	return out
}

func sortedNodeIDs(nodes map[string]graphNodePayload) []string {
	out := make([]string, 0, len(nodes))
	for id := range nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func cloneInterfaceMap(input map[string]interface{}) map[string]interface{} {
	if len(input) == 0 {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func graphEdgesForNode(ctx context.Context, engine storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	outgoing, err := engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	incoming, err := engine.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
	seen := make(map[string]struct{}, len(outgoing)+len(incoming))
	for _, edge := range outgoing {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if edge == nil {
			continue
		}
		id := string(edge.ID)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		edges = append(edges, edge)
	}
	for _, edge := range incoming {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if edge == nil {
			continue
		}
		id := string(edge.ID)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		edges = append(edges, edge)
	}
	return edges, nil
}

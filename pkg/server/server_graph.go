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
	// Direction controls the neighborhood walk edge direction:
	// "out" (follow outgoing edges only), "in" (incoming only) or
	// "both" (default, preserves the historical undirected behavior).
	Direction string `json:"direction,omitempty"`
	// Exclusion filters applied after collection: nodes and edges matching
	// any entry are removed from the result, along with edges incident to a
	// removed node. The remaining graph may fragment into several
	// disconnected components, reported in the payload's components field.
	ExcludeLabels            []string `json:"exclude_labels,omitempty"`
	ExcludeRelationshipTypes []string `json:"exclude_relationship_types,omitempty"`
	// ExcludeNames removes nodes whose symbol name (the "label" property)
	// equals an entry exactly, e.g. "context.Context" hides the variable
	// context.Context.
	ExcludeNames []string `json:"exclude_names,omitempty"`
	// ExcludeProperties entries match nodes and edges by explicit fields:
	// scope optionally constrains the node's labels (or the edge's type),
	// property is the property key, and value optionally constrains the
	// property's value.
	ExcludeProperties []graphPropertyFilterRequest `json:"exclude_properties,omitempty"`
	// IncludeNames and IncludeProperties are the inclusion mirrors: when
	// set, nodes are kept only when they match at least one name entry and
	// at least one applicable property entry (a node must also carry the
	// scoped label, an edge the scoped type). They combine with labels /
	// relationship_types with AND semantics and with the exclusion filters
	// with exclude-after-include.
	IncludeNames      []string                     `json:"include_names,omitempty"`
	IncludeProperties []graphPropertyFilterRequest `json:"include_properties,omitempty"`
}

// graphPropertyFilterRequest is one explicit property filter entry.
type graphPropertyFilterRequest struct {
	// Scope optionally constrains the node's labels or the edge's type;
	// empty matches any node or edge.
	Scope string `json:"scope,omitempty"`
	// Property is the property key to match (required).
	Property string `json:"property"`
	// Value optionally constrains the property's value; nil only requires
	// the property to exist.
	Value *string `json:"value,omitempty"`
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
	// ComponentCount is the number of disconnected subgraphs in the
	// filtered result; it is 1 when the graph is connected (or empty).
	ComponentCount int  `json:"component_count,omitempty"`
	Truncated      bool `json:"truncated"`
}

// graphPayload is recursive: each entry of Components is itself a
// graphPayload (nodes + edges + meta) describing one disconnected subgraph
// of a filtered result, so clients can render every component identically.
type graphPayload struct {
	Nodes      []graphNodePayload `json:"nodes"`
	Edges      []graphEdgePayload `json:"edges"`
	Meta       graphMetaPayload   `json:"meta"`
	Components []graphPayload     `json:"components,omitempty"`
}

type graphFilterSet struct {
	labels            map[string]struct{}
	relationshipTypes map[string]struct{}
	includeNames      map[string]struct{}
	includeProperties []graphPropertyFilter
	excludeLabels     map[string]struct{}
	excludeRelTypes   map[string]struct{}
	excludeNames      map[string]struct{}
	excludeProperties []graphPropertyFilter
}

// graphPropertyFilter is one explicit property filter entry: an optional
// scope (a node label or an edge type), the property key, and an optional
// equality constraint on the property value. No string parsing happens at
// this layer; the request carries the fields explicitly.
type graphPropertyFilter struct {
	scope    string
	property string
	value    *string
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
	direction := strings.TrimSpace(strings.ToLower(req.Direction))
	if direction == "" {
		direction = "both"
	}
	if direction != "out" && direction != "in" && direction != "both" {
		s.writeLocalizedError(w, r, http.StatusBadRequest, localization.GraphDirectionInvalid(), ErrBadRequest)
		return
	}
	filterSet := newGraphFilterSet(req.Labels, req.RelationshipTypes).withFilters(req.IncludeNames, req.ExcludeNames, req.IncludeProperties, req.ExcludeProperties, req.ExcludeLabels, req.ExcludeRelationshipTypes)
	dbName, engine, err := s.resolveGraphStorage(r)
	if err != nil {
		s.writeGraphResolveError(w, r, err)
		return
	}

	collection, err := s.collectLatestNeighborhood(r.Context(), engine, req.NodeIDs, req.Depth, req.Limit, direction, filterSet)
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
	if len(f.labels) > 0 {
		matched := false
		for _, label := range node.Labels {
			if _, ok := f.labels[label]; ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(f.includeNames) > 0 {
		name, ok := node.Properties["label"].(string)
		if !ok {
			return false
		}
		if _, in := f.includeNames[name]; !in {
			return false
		}
	}
	if len(f.includeProperties) > 0 {
		// Relevance model: a scoped entry gates only nodes carrying that
		// label; an unscoped entry gates every node. When at least one entry
		// applies to this node it must match at least one of them.
		relevant := 0
		matched := 0
		for _, filter := range f.includeProperties {
			if filter.scope == "" {
				relevant++
				if propertyMatches(node.Properties, filter.property, filter.value) {
					matched++
				}
				continue
			}
			for _, label := range node.Labels {
				if label == filter.scope {
					relevant++
					if propertyMatches(node.Properties, filter.property, filter.value) {
						matched++
					}
					break
				}
			}
		}
		if relevant > 0 && matched == 0 {
			return false
		}
	}
	return true
}

func (f graphFilterSet) allowEdge(edge *storage.Edge) bool {
	if edge == nil {
		return false
	}
	if len(f.relationshipTypes) > 0 {
		if _, ok := f.relationshipTypes[edge.Type]; !ok {
			return false
		}
	}
	// Include property filters gate edges only when scoped to the edge's
	// type (relevance model: type-scoped entries apply to matching edges,
	// and a matching edge must satisfy at least one of them). Unscoped
	// entries are node-oriented and leave edge traversal free so node
	// inclusion can still walk the graph.
	if len(f.includeProperties) > 0 {
		relevant := 0
		matched := 0
		for _, filter := range f.includeProperties {
			if filter.scope != edge.Type {
				continue
			}
			relevant++
			if propertyMatches(edge.Properties, filter.property, filter.value) {
				matched++
			}
		}
		if relevant > 0 && matched == 0 {
			return false
		}
	}
	return true
}

// withFilters returns a copy of the filter set augmented with the inclusion
// and exclusion filters. Name entries are whitespace-trimmed; property
// entries without a property key are ignored.
func (f graphFilterSet) withFilters(includeNames, excludeNames []string, includeProperties, excludeProperties []graphPropertyFilterRequest, excludeLabels, excludeRelationshipTypes []string) graphFilterSet {
	if len(includeNames) == 0 && len(excludeNames) == 0 && len(includeProperties) == 0 && len(excludeProperties) == 0 && len(excludeLabels) == 0 && len(excludeRelationshipTypes) == 0 {
		return f
	}
	out := graphFilterSet{
		labels:            f.labels,
		relationshipTypes: f.relationshipTypes,
		includeNames:      make(map[string]struct{}),
		excludeLabels:     make(map[string]struct{}),
		excludeRelTypes:   make(map[string]struct{}),
		excludeNames:      make(map[string]struct{}),
	}
	for _, name := range includeNames {
		name = strings.TrimSpace(name)
		if name != "" {
			out.includeNames[name] = struct{}{}
		}
	}
	for _, name := range excludeNames {
		name = strings.TrimSpace(name)
		if name != "" {
			out.excludeNames[name] = struct{}{}
		}
	}
	for _, label := range excludeLabels {
		label = strings.TrimSpace(label)
		if label != "" {
			out.excludeLabels[label] = struct{}{}
		}
	}
	for _, relType := range excludeRelationshipTypes {
		relType = strings.TrimSpace(relType)
		if relType != "" {
			out.excludeRelTypes[relType] = struct{}{}
		}
	}
	for _, entry := range includeProperties {
		if filter := newGraphPropertyFilter(entry); filter != nil {
			out.includeProperties = append(out.includeProperties, *filter)
		}
	}
	for _, entry := range excludeProperties {
		if filter := newGraphPropertyFilter(entry); filter != nil {
			out.excludeProperties = append(out.excludeProperties, *filter)
		}
	}
	return out
}

// newGraphPropertyFilter converts an explicit request entry; entries without
// a property key are rejected.
func newGraphPropertyFilter(entry graphPropertyFilterRequest) *graphPropertyFilter {
	property := strings.TrimSpace(entry.Property)
	if property == "" {
		return nil
	}
	filter := &graphPropertyFilter{
		scope:    strings.TrimSpace(entry.Scope),
		property: property,
	}
	if entry.Value != nil {
		value := strings.TrimSpace(*entry.Value)
		if value != "" {
			filter.value = &value
		}
	}
	return filter
}

func (f graphFilterSet) hasExclusions() bool {
	return len(f.excludeLabels) > 0 || len(f.excludeRelTypes) > 0 || len(f.excludeNames) > 0 || len(f.excludeProperties) > 0
}

// propertyMatches reports whether the node or edge carries the property key;
// when want is non-nil the property's value must equal it.
func propertyMatches(props map[string]interface{}, key string, want *string) bool {
	if props == nil || key == "" {
		return false
	}
	value, ok := props[key]
	if !ok {
		return false
	}
	if want == nil {
		return true
	}
	return propertyValueMatches(value, *want)
}

// propertyValueMatches compares a property value against a user-supplied
// string: strings compare directly, booleans and numbers parse the string,
// nil matches "null".
func propertyValueMatches(actual interface{}, want string) bool {
	switch typed := actual.(type) {
	case string:
		return typed == want
	case bool:
		parsed, err := strconv.ParseBool(want)
		return err == nil && parsed == typed
	case int:
		parsed, err := strconv.ParseFloat(want, 64)
		return err == nil && parsed == float64(typed)
	case int64:
		parsed, err := strconv.ParseFloat(want, 64)
		return err == nil && parsed == float64(typed)
	case float64:
		parsed, err := strconv.ParseFloat(want, 64)
		return err == nil && parsed == typed
	case float32:
		parsed, err := strconv.ParseFloat(want, 64)
		return err == nil && parsed == float64(typed)
	case nil:
		return want == "null"
	default:
		return fmt.Sprint(typed) == want
	}
}

// filterPropertyMatchesNode reports whether an explicit property filter
// matches a node: the scope must be empty or one of the node's labels, and
// the property must exist (and equal the value when constrained).
func filterPropertyMatchesNode(filter graphPropertyFilter, labels []string, props map[string]interface{}) bool {
	if filter.scope != "" {
		scopeMatches := false
		for _, label := range labels {
			if label == filter.scope {
				scopeMatches = true
				break
			}
		}
		if !scopeMatches {
			return false
		}
	}
	return propertyMatches(props, filter.property, filter.value)
}

// excludeNodePayload reports whether a collected node matches any exclusion
// filter: an excluded label, an excluded symbol name, or an excluded
// property entry whose scope is empty or one of the node's labels.
func (f graphFilterSet) excludeNodePayload(node graphNodePayload) bool {
	for _, label := range node.Labels {
		if _, ok := f.excludeLabels[label]; ok {
			return true
		}
	}
	if name, ok := node.Properties["label"].(string); ok {
		if _, in := f.excludeNames[name]; in {
			return true
		}
	}
	for _, filter := range f.excludeProperties {
		if filterPropertyMatchesNode(filter, node.Labels, node.Properties) {
			return true
		}
	}
	return false
}

// excludeEdgePayload reports whether a collected edge matches any exclusion
// filter: an excluded relationship type, or an excluded property path whose
// scope is empty or the edge's type.
func (f graphFilterSet) excludeEdgePayload(edge graphEdgePayload) bool {
	if _, ok := f.excludeRelTypes[edge.Type]; ok {
		return true
	}
	for _, filter := range f.excludeProperties {
		if filter.scope != "" && filter.scope != edge.Type {
			continue
		}
		if propertyMatches(edge.Properties, filter.property, filter.value) {
			return true
		}
	}
	return false
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

	components := c.components()

	meta.NodeCount = len(nodes)
	meta.EdgeCount = len(edges)
	meta.ComponentCount = len(components)
	meta.Truncated = c.truncated

	return graphPayload{Nodes: nodes, Edges: edges, Meta: meta, Components: components}
}

// pruneExcluded removes nodes and edges matching the exclusion filters, then
// drops any edge whose endpoint was removed. The remaining graph may
// fragment into several disconnected components.
func (c *graphCollection) pruneExcluded(filters graphFilterSet) {
	if !filters.hasExclusions() {
		return
	}
	for id, node := range c.nodes {
		if filters.excludeNodePayload(node) {
			delete(c.nodes, id)
		}
	}
	for id, edge := range c.edges {
		if filters.excludeEdgePayload(edge) {
			delete(c.edges, id)
			continue
		}
		if _, ok := c.nodes[edge.Source]; !ok {
			delete(c.edges, id)
			continue
		}
		if _, ok := c.nodes[edge.Target]; !ok {
			delete(c.edges, id)
		}
	}
}

// components partitions the collected nodes into connected subgraphs
// (union-find over the remaining edges). Components are ordered by node
// count descending, then by smallest node ID; node and edge lists are
// sorted. Each component is a full graphPayload so clients can render it
// with the same code path as the top-level graph.
func (c graphCollection) components() []graphPayload {
	if len(c.nodes) == 0 {
		return nil
	}
	parent := make(map[string]string, len(c.nodes))
	var find func(id string) string
	find = func(id string) string {
		root, ok := parent[id]
		if !ok || root == id {
			return id
		}
		grand := find(root)
		parent[id] = grand
		return grand
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	for id := range c.nodes {
		parent[id] = id
	}
	for _, edge := range c.edges {
		if _, ok := c.nodes[edge.Source]; !ok {
			continue
		}
		if _, ok := c.nodes[edge.Target]; !ok {
			continue
		}
		union(edge.Source, edge.Target)
	}

	type componentGroup struct {
		nodeIDs []string
		edgeIDs []string
	}
	byRoot := make(map[string]*componentGroup)
	for id := range c.nodes {
		root := find(id)
		group := byRoot[root]
		if group == nil {
			group = &componentGroup{}
			byRoot[root] = group
		}
		group.nodeIDs = append(group.nodeIDs, id)
	}
	for _, edge := range c.edges {
		root := find(edge.Source)
		if group := byRoot[root]; group != nil {
			group.edgeIDs = append(group.edgeIDs, edge.ID)
		}
	}

	result := make([]graphPayload, 0, len(byRoot))
	for _, group := range byRoot {
		sort.Strings(group.nodeIDs)
		sort.Strings(group.edgeIDs)
		nodes := make([]graphNodePayload, 0, len(group.nodeIDs))
		for _, id := range group.nodeIDs {
			nodes = append(nodes, c.nodes[id])
		}
		edges := make([]graphEdgePayload, 0, len(group.edgeIDs))
		for _, id := range group.edgeIDs {
			edges = append(edges, c.edges[id])
		}
		result = append(result, graphPayload{
			Nodes: nodes,
			Edges: edges,
			Meta: graphMetaPayload{
				GeneratedFrom: "component",
				NodeCount:     len(nodes),
				EdgeCount:     len(edges),
			},
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Meta.NodeCount != result[j].Meta.NodeCount {
			return result[i].Meta.NodeCount > result[j].Meta.NodeCount
		}
		return result[i].Nodes[0].ID < result[j].Nodes[0].ID
	})
	return result
}

func (s *Server) collectLatestNeighborhood(ctx context.Context, engine storage.Engine, seedIDs []string, depth, limit int, direction string, filters graphFilterSet) (graphCollection, error) {
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

		edges, err := graphEdgesForNode(ctx, engine, storage.NodeID(current.nodeID), direction)
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

	collection.pruneExcluded(filters)
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

		edges, err := graphEdgesForNode(ctx, engine, storage.NodeID(current), "both")
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

func graphEdgesForNode(ctx context.Context, engine storage.Engine, nodeID storage.NodeID, direction string) ([]*storage.Edge, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var outgoing []*storage.Edge
	var incoming []*storage.Edge
	if direction != "in" {
		var err error
		outgoing, err = engine.GetOutgoingEdges(nodeID)
		if err != nil {
			return nil, err
		}
	}
	if direction != "out" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var err error
		incoming, err = engine.GetIncomingEdges(nodeID)
		if err != nil {
			return nil, err
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
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

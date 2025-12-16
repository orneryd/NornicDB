// Package models provides GraphQL model types for NornicDB.
//
// These types are used to serialize/deserialize GraphQL requests
// and responses. They map to NornicDB's internal storage types and search types.
package models

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

// =============================================================================
// CUSTOM SCALARS
// =============================================================================

// JSON represents arbitrary JSON data.
type JSON map[string]interface{}

// MarshalGQL implements the graphql.Marshaler interface.
func (j JSON) MarshalGQL(w io.Writer) {
	if j == nil {
		w.Write([]byte("null"))
		return
	}
	data, err := json.Marshal(j)
	if err != nil {
		w.Write([]byte("null"))
		return
	}
	w.Write(data)
}

// UnmarshalGQL implements the graphql.Unmarshaler interface.
func (j *JSON) UnmarshalGQL(v interface{}) error {
	if v == nil {
		*j = nil
		return nil
	}
	switch val := v.(type) {
	case map[string]interface{}:
		*j = JSON(val)
		return nil
	case string:
		// Try to parse as JSON string
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(val), &result); err != nil {
			return fmt.Errorf("cannot unmarshal JSON: %w", err)
		}
		*j = JSON(result)
		return nil
	default:
		return fmt.Errorf("cannot unmarshal %T into JSON", v)
	}
}

// FloatArray represents a slice of float32 values (for embeddings).
type FloatArray []float32

// MarshalGQL implements the graphql.Marshaler interface.
func (f FloatArray) MarshalGQL(w io.Writer) {
	if f == nil {
		w.Write([]byte("null"))
		return
	}
	data, err := json.Marshal([]float32(f))
	if err != nil {
		w.Write([]byte("null"))
		return
	}
	w.Write(data)
}

// UnmarshalGQL implements the graphql.Unmarshaler interface.
func (f *FloatArray) UnmarshalGQL(v interface{}) error {
	if v == nil {
		*f = nil
		return nil
	}
	switch val := v.(type) {
	case []interface{}:
		result := make([]float32, len(val))
		for i, item := range val {
			switch n := item.(type) {
			case float64:
				result[i] = float32(n)
			case float32:
				result[i] = n
			case int:
				result[i] = float32(n)
			case int64:
				result[i] = float32(n)
			case json.Number:
				f, err := n.Float64()
				if err != nil {
					return fmt.Errorf("cannot parse float at index %d: %w", i, err)
				}
				result[i] = float32(f)
			default:
				return fmt.Errorf("cannot unmarshal %T at index %d into float32", item, i)
			}
		}
		*f = FloatArray(result)
		return nil
	case string:
		var result []float32
		if err := json.Unmarshal([]byte(val), &result); err != nil {
			return fmt.Errorf("cannot unmarshal FloatArray: %w", err)
		}
		*f = FloatArray(result)
		return nil
	default:
		return fmt.Errorf("cannot unmarshal %T into FloatArray", v)
	}
}

// MarshalDateTime marshals time.Time to RFC3339 string.
func MarshalDateTime(t time.Time, w io.Writer) {
	if t.IsZero() {
		w.Write([]byte("null"))
		return
	}
	w.Write([]byte(strconv.Quote(t.Format(time.RFC3339))))
}

// UnmarshalDateTime unmarshals GraphQL DateTime scalar to time.Time.
func UnmarshalDateTime(v interface{}) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}
	switch val := v.(type) {
	case string:
		return time.Parse(time.RFC3339, val)
	case time.Time:
		return val, nil
	case int64:
		return time.Unix(val, 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot unmarshal %T into DateTime", v)
	}
}

// =============================================================================
// ENUMS
// =============================================================================

// MemoryTier represents memory classification for decay calculation.
type MemoryTier string

const (
	MemoryTierEpisodic   MemoryTier = "EPISODIC"
	MemoryTierSemantic   MemoryTier = "SEMANTIC"
	MemoryTierProcedural MemoryTier = "PROCEDURAL"
)

func (e MemoryTier) IsValid() bool {
	switch e {
	case MemoryTierEpisodic, MemoryTierSemantic, MemoryTierProcedural:
		return true
	}
	return false
}

func (e MemoryTier) String() string {
	return string(e)
}

func (e *MemoryTier) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}
	*e = MemoryTier(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid MemoryTier", str)
	}
	return nil
}

func (e MemoryTier) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// RelationshipDirection represents traversal direction.
type RelationshipDirection string

const (
	RelationshipDirectionOutgoing RelationshipDirection = "OUTGOING"
	RelationshipDirectionIncoming RelationshipDirection = "INCOMING"
	RelationshipDirectionBoth     RelationshipDirection = "BOTH"
)

func (e RelationshipDirection) IsValid() bool {
	switch e {
	case RelationshipDirectionOutgoing, RelationshipDirectionIncoming, RelationshipDirectionBoth:
		return true
	}
	return false
}

func (e RelationshipDirection) String() string {
	return string(e)
}

func (e *RelationshipDirection) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}
	*e = RelationshipDirection(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid RelationshipDirection", str)
	}
	return nil
}

func (e RelationshipDirection) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// SearchSortBy represents search result sorting options.
type SearchSortBy string

const (
	SearchSortByRelevance    SearchSortBy = "RELEVANCE"
	SearchSortByCreatedAt    SearchSortBy = "CREATED_AT"
	SearchSortByLastAccessed SearchSortBy = "LAST_ACCESSED"
	SearchSortByDecayScore   SearchSortBy = "DECAY_SCORE"
	SearchSortByAccessCount  SearchSortBy = "ACCESS_COUNT"
)

func (e SearchSortBy) IsValid() bool {
	switch e {
	case SearchSortByRelevance, SearchSortByCreatedAt, SearchSortByLastAccessed, SearchSortByDecayScore, SearchSortByAccessCount:
		return true
	}
	return false
}

func (e SearchSortBy) String() string {
	return string(e)
}

func (e *SearchSortBy) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}
	*e = SearchSortBy(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid SearchSortBy", str)
	}
	return nil
}

func (e SearchSortBy) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// SearchMethod represents the search algorithm to use.
type SearchMethod string

const (
	SearchMethodHybrid SearchMethod = "HYBRID"
	SearchMethodVector SearchMethod = "VECTOR"
	SearchMethodBm25   SearchMethod = "BM25"
)

func (e SearchMethod) IsValid() bool {
	switch e {
	case SearchMethodHybrid, SearchMethodVector, SearchMethodBm25:
		return true
	}
	return false
}

func (e SearchMethod) String() string {
	return string(e)
}

func (e *SearchMethod) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}
	*e = SearchMethod(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid SearchMethod", str)
	}
	return nil
}

func (e SearchMethod) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// SortOrder represents ascending/descending sort.
type SortOrder string

const (
	SortOrderAsc  SortOrder = "ASC"
	SortOrderDesc SortOrder = "DESC"
)

func (e SortOrder) IsValid() bool {
	switch e {
	case SortOrderAsc, SortOrderDesc:
		return true
	}
	return false
}

func (e SortOrder) String() string {
	return string(e)
}

func (e *SortOrder) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}
	*e = SortOrder(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid SortOrder", str)
	}
	return nil
}

func (e SortOrder) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// =============================================================================
// CORE TYPES
// =============================================================================

// Node represents a graph node.
type Node struct {
	ID                  string     `json:"id"`
	InternalID          string     `json:"internalId"`
	Labels              []string   `json:"labels"`
	Properties          JSON       `json:"properties"`
	CreatedAt           *time.Time `json:"createdAt,omitempty"`
	UpdatedAt           *time.Time `json:"updatedAt,omitempty"`
	DecayScore          *float64   `json:"decayScore,omitempty"`
	LastAccessed        *time.Time `json:"lastAccessed,omitempty"`
	AccessCount         *int       `json:"accessCount,omitempty"`
	HasEmbedding        bool       `json:"hasEmbedding"`
	EmbeddingDimensions int        `json:"embeddingDimensions"`
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID                 string     `json:"id"`
	InternalID         string     `json:"internalId"`
	Type               string     `json:"type"`
	StartNodeElementID string     `json:"startNodeElementId"` // Neo4j-style elementId
	EndNodeElementID   string     `json:"endNodeElementId"`   // Neo4j-style elementId
	StartNodeID        string     `json:"startNodeId"`        // Used for resolution (deprecated)
	EndNodeID          string     `json:"endNodeId"`          // Used for resolution (deprecated)
	Properties         JSON       `json:"properties"`
	CreatedAt          *time.Time `json:"createdAt,omitempty"`
	UpdatedAt          *time.Time `json:"updatedAt,omitempty"`
	Confidence         *float64   `json:"confidence,omitempty"`
	AutoGenerated      bool       `json:"autoGenerated"`
}

// SimilarNode represents a similar node with score.
type SimilarNode struct {
	Node       *Node   `json:"node"`
	Similarity float64 `json:"similarity"`
}

// Subgraph represents a collection of nodes and relationships.
type Subgraph struct {
	Nodes         []*Node         `json:"nodes"`
	Relationships []*Relationship `json:"relationships"`
}

// =============================================================================
// SEARCH TYPES
// =============================================================================

// SearchResult represents a search hit with scores.
type SearchResult struct {
	Node        *Node    `json:"node"`
	Score       float64  `json:"score"`
	RRFScore    *float64 `json:"rrfScore,omitempty"`
	VectorScore *float64 `json:"vectorScore,omitempty"`
	BM25Score   *float64 `json:"bm25Score,omitempty"`
	VectorRank  *int     `json:"vectorRank,omitempty"`
	BM25Rank    *int     `json:"bm25Rank,omitempty"`
	FoundBy     []string `json:"foundBy"`
}

// SearchResponse represents search results with metadata.
type SearchResponse struct {
	Results          []*SearchResult `json:"results"`
	TotalCount       int             `json:"totalCount"`
	Method           string          `json:"method"`
	ExecutionTimeMs  float64         `json:"executionTimeMs"`
	VectorSearchUsed bool            `json:"vectorSearchUsed"`
	BM25SearchUsed   bool            `json:"bm25SearchUsed"`
}

// SearchOptions represents search query options.
type SearchOptions struct {
	Limit           *int          `json:"limit,omitempty"`
	Offset          *int          `json:"offset,omitempty"`
	Labels          []string      `json:"labels,omitempty"`
	Method          *SearchMethod `json:"method,omitempty"`
	MinScore        *float64      `json:"minScore,omitempty"`
	SortBy          *SearchSortBy `json:"sortBy,omitempty"`
	SortOrder       *SortOrder    `json:"sortOrder,omitempty"`
	IncludeDecayed  *bool         `json:"includeDecayed,omitempty"`
	MinDecayScore   *float64      `json:"minDecayScore,omitempty"`
	PropertyFilters JSON          `json:"propertyFilters,omitempty"`
}

// =============================================================================
// CYPHER TYPES
// =============================================================================

// CypherResult represents Cypher query results.
type CypherResult struct {
	Columns         []string        `json:"columns"`
	Rows            [][]interface{} `json:"rows"`
	RowCount        int             `json:"rowCount"`
	Stats           *CypherStats    `json:"stats,omitempty"`
	ExecutionTimeMs float64         `json:"executionTimeMs"`
}

// CypherStats represents query execution statistics.
type CypherStats struct {
	NodesCreated         int  `json:"nodesCreated"`
	NodesDeleted         int  `json:"nodesDeleted"`
	RelationshipsCreated int  `json:"relationshipsCreated"`
	RelationshipsDeleted int  `json:"relationshipsDeleted"`
	PropertiesSet        int  `json:"propertiesSet"`
	LabelsAdded          int  `json:"labelsAdded"`
	LabelsRemoved        int  `json:"labelsRemoved"`
	ContainsUpdates      bool `json:"containsUpdates"`
}

// CypherInput represents input for Cypher execution.
type CypherInput struct {
	Statement  string `json:"statement"`
	Parameters JSON   `json:"parameters,omitempty"`
	TimeoutMs  *int   `json:"timeoutMs,omitempty"`
}

// =============================================================================
// STATS TYPES
// =============================================================================

// DatabaseStats represents database statistics.
type DatabaseStats struct {
	NodeCount          int                      `json:"nodeCount"`
	RelationshipCount  int                      `json:"relationshipCount"`
	Labels             []*LabelStats            `json:"labels"`
	RelationshipTypes  []*RelationshipTypeStats `json:"relationshipTypes"`
	EmbeddedNodeCount  int                      `json:"embeddedNodeCount"`
	UptimeSeconds      float64                  `json:"uptimeSeconds"`
	MemoryUsageBytes   int                      `json:"memoryUsageBytes"`
}

// LabelStats represents statistics for a label.
type LabelStats struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

// RelationshipTypeStats represents statistics for a relationship type.
type RelationshipTypeStats struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// GraphSchema represents schema information.
type GraphSchema struct {
	NodeLabels               []string            `json:"nodeLabels"`
	RelationshipTypes        []string            `json:"relationshipTypes"`
	NodePropertyKeys         []string            `json:"nodePropertyKeys"`
	RelationshipPropertyKeys []string            `json:"relationshipPropertyKeys"`
	Constraints              []*SchemaConstraint `json:"constraints"`
}

// SchemaConstraint represents a schema constraint.
type SchemaConstraint struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	EntityType    string   `json:"entityType"`
	LabelsOrTypes []string `json:"labelsOrTypes"`
	Properties    []string `json:"properties"`
}

// =============================================================================
// INPUT TYPES
// =============================================================================

// CreateNodeInput represents input for creating a node.
type CreateNodeInput struct {
	ID         *string     `json:"id,omitempty"`
	Labels     []string    `json:"labels"`
	Properties JSON        `json:"properties"`
	Tier       *MemoryTier `json:"tier,omitempty"`
	Embedding  FloatArray  `json:"embedding,omitempty"`
}

// UpdateNodeInput represents input for updating a node.
type UpdateNodeInput struct {
	ID               string     `json:"id"`
	Labels           []string   `json:"labels,omitempty"`
	Properties       JSON       `json:"properties,omitempty"`
	RemoveProperties []string   `json:"removeProperties,omitempty"`
	AddLabels        []string   `json:"addLabels,omitempty"`
	RemoveLabels     []string   `json:"removeLabels,omitempty"`
	Embedding        FloatArray `json:"embedding,omitempty"`
}

// CreateRelationshipInput represents input for creating a relationship.
type CreateRelationshipInput struct {
	ID          *string `json:"id,omitempty"`
	StartNodeID string  `json:"startNodeId"`
	EndNodeID   string  `json:"endNodeId"`
	Type        string  `json:"type"`
	Properties  JSON    `json:"properties,omitempty"`
}

// UpdateRelationshipInput represents input for updating a relationship.
type UpdateRelationshipInput struct {
	ID               string   `json:"id"`
	Type             *string  `json:"type,omitempty"`
	Properties       JSON     `json:"properties,omitempty"`
	RemoveProperties []string `json:"removeProperties,omitempty"`
}

// BulkCreateNodesInput represents input for bulk node creation.
type BulkCreateNodesInput struct {
	Nodes          []*CreateNodeInput `json:"nodes"`
	SkipDuplicates *bool              `json:"skipDuplicates,omitempty"`
}

// BulkCreateRelationshipsInput represents input for bulk relationship creation.
type BulkCreateRelationshipsInput struct {
	Relationships []*CreateRelationshipInput `json:"relationships"`
	SkipInvalid   *bool                      `json:"skipInvalid,omitempty"`
}

// =============================================================================
// RESULT TYPES
// =============================================================================

// BulkCreateResult represents bulk creation results.
type BulkCreateResult struct {
	Created int      `json:"created"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors"`
}

// BulkDeleteResult represents bulk deletion results.
type BulkDeleteResult struct {
	Deleted  int      `json:"deleted"`
	NotFound []string `json:"notFound"`
}

// EmbeddingStatus represents embedding worker status.
type EmbeddingStatus struct {
	Pending       int  `json:"pending"`
	Embedded      int  `json:"embedded"`
	Total         int  `json:"total"`
	WorkerRunning bool `json:"workerRunning"`
}

// DecayResult represents decay simulation results.
type DecayResult struct {
	NodesProcessed    int     `json:"nodesProcessed"`
	NodesDecayed      int     `json:"nodesDecayed"`
	AverageDecayScore float64 `json:"averageDecayScore"`
}

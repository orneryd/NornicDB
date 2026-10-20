// Package cypher provides Cypher query execution for NornicDB.
package cypher

// ExecuteResult holds execution results in Neo4j-compatible format.
type ExecuteResult struct {
	Columns  []string
	Rows     [][]interface{}
	Stats    *QueryStats
	Metadata map[string]interface{} // Additional result metadata (e.g., execution plan)
	// MapKeyOrders retains evaluated map traversal order for transaction HTTP metadata.
	MapKeyOrders map[uintptr][]string `json:"-"`

	// gdsProjection is the projection a GDS stream read its rows from, for
	// gdsStreamHandler to render their node ids with; nil otherwise.
	gdsProjection *GraphProjection
	// relationshipMergeNoOp distinguishes a stored hidden relationship's
	// provisional no-op candidate from both a physical create and a visible
	// match. The pipeline applies ON CREATE to finish the candidate image, but
	// commit alone decides identical no-op versus permanent conflict.
	relationshipMergeNoOp bool
}

// QueryStats holds query execution statistics.
type QueryStats struct {
	NodesCreated         int `json:"nodes_created"`
	NodesDeleted         int `json:"nodes_deleted"`
	RelationshipsCreated int `json:"relationships_created"`
	RelationshipsDeleted int `json:"relationships_deleted"`
	PropertiesSet        int `json:"properties_set"`
	LabelsAdded          int `json:"labels_added"`
	LabelsRemoved        int `json:"labels_removed"`
	// Schema counters: a schema command reports the indexes and constraints
	// it created or dropped (countSchemaChanges).
	IndexesAdded       int `json:"indexes_added"`
	IndexesRemoved     int `json:"indexes_removed"`
	ConstraintsAdded   int `json:"constraints_added"`
	ConstraintsRemoved int `json:"constraints_removed"`
}

// nodePatternInfo holds parsed node pattern information
type nodePatternInfo struct {
	variable   string
	labels     []string
	properties map[string]interface{}
	// labelErr is set when the label chain breaks the label rules
	// (parseLabelChain); writers (CREATE, MERGE) reject the pattern.
	labelErr error
}

// returnItem represents a single item in a RETURN clause
type returnItem struct {
	expr  string
	alias string
}

// column is the item's result column: its alias. Every returnItem builder
// names the column (parseProjectionExprAlias names an item without AS after
// its expression), so an empty alias is an explicit empty name, RETURN x AS
// “, whose column is "" as in Neo4j 5.26.30 (#907).
func (item returnItem) column() string { return item.alias }

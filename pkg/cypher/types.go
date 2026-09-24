// Package cypher provides Cypher query execution for NornicDB.
package cypher

// ExecuteResult holds execution results in Neo4j-compatible format.
type ExecuteResult struct {
	Columns  []string
	Rows     [][]interface{}
	Stats    *QueryStats
	Metadata map[string]interface{} // Additional result metadata (e.g., execution plan)
}

// QueryStats holds query execution statistics.
type QueryStats struct {
	NodesCreated         int `json:"nodes_created"`
	NodesDeleted         int `json:"nodes_deleted"`
	RelationshipsCreated int `json:"relationships_created"`
	RelationshipsDeleted int `json:"relationships_deleted"`
	PropertiesSet        int `json:"properties_set"`
	LabelsAdded          int `json:"labels_added"`
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

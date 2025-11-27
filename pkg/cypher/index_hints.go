// Package cypher provides index hint support for Neo4j-compatible query optimization.
//
// Index hints allow users to specify which indexes should be used during query execution,
// overriding the query planner's automatic index selection.
//
// # Supported Hint Types
//
//   - USING INDEX variable:Label(property) - Force use of a specific property index
//   - USING SCAN variable:Label - Force a label scan instead of index lookup
//   - USING JOIN ON variable - Force specific join strategy (planned)
//
// # Neo4j Compatibility
//
// This implementation follows Neo4j's index hint syntax:
//
//	MATCH (n:Person)
//	USING INDEX n:Person(name)
//	WHERE n.name = 'Alice'
//	RETURN n
//
// Multiple hints can be specified:
//
//	MATCH (a:Person)-[:KNOWS]->(b:Person)
//	USING INDEX a:Person(name)
//	USING INDEX b:Person(email)
//	WHERE a.name = 'Alice' AND b.email = 'bob@example.com'
//	RETURN a, b
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you have a huge phone book (database). Normally, the database decides
// the fastest way to find someone - maybe by their name index, or by scanning.
// Index hints are like saying "I KNOW the name index is best - use that one!"
// It's like telling a librarian exactly which catalog to use.
package cypher

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// IndexHintType represents the type of index hint.
type IndexHintType int

const (
	// HintIndex forces use of a specific property index.
	HintIndex IndexHintType = iota
	// HintScan forces a label scan instead of index lookup.
	HintScan
	// HintJoin forces a specific join strategy.
	HintJoin
)

// IndexHint represents a parsed index hint from a Cypher query.
//
// Example:
//
//	USING INDEX n:Person(name)
//	→ IndexHint{Type: HintIndex, Variable: "n", Label: "Person", Property: "name"}
type IndexHint struct {
	Type     IndexHintType
	Variable string   // The variable name (e.g., "n" in "n:Person")
	Label    string   // The label (e.g., "Person")
	Property string   // The property for index hints (e.g., "name")
	Properties []string // Multiple properties for composite hints
}

// String returns a human-readable representation of the hint.
func (h *IndexHint) String() string {
	switch h.Type {
	case HintIndex:
		if len(h.Properties) > 1 {
			return fmt.Sprintf("USING INDEX %s:%s(%s)", h.Variable, h.Label, strings.Join(h.Properties, ", "))
		}
		return fmt.Sprintf("USING INDEX %s:%s(%s)", h.Variable, h.Label, h.Property)
	case HintScan:
		return fmt.Sprintf("USING SCAN %s:%s", h.Variable, h.Label)
	case HintJoin:
		return fmt.Sprintf("USING JOIN ON %s", h.Variable)
	default:
		return "UNKNOWN HINT"
	}
}

// Regex patterns for parsing index hints
var (
	// USING INDEX n:Label(property) or USING INDEX n:Label(prop1, prop2)
	indexHintPattern = regexp.MustCompile(`(?i)USING\s+INDEX\s+(\w+):(\w+)\s*\(\s*([^)]+)\s*\)`)
	
	// USING SCAN n:Label
	scanHintPattern = regexp.MustCompile(`(?i)USING\s+SCAN\s+(\w+):(\w+)`)
	
	// USING JOIN ON n
	joinHintPattern = regexp.MustCompile(`(?i)USING\s+JOIN\s+ON\s+(\w+)`)
)

// ParseIndexHints extracts all index hints from a Cypher query.
//
// Parameters:
//   - query: The full Cypher query string
//
// Returns:
//   - Slice of parsed IndexHint structs
//   - The query with hints removed (for further processing)
//
// Example:
//
//	query := "MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'Alice' RETURN n"
//	hints, cleanQuery := ParseIndexHints(query)
//	// hints = [{Type: HintIndex, Variable: "n", Label: "Person", Property: "name"}]
//	// cleanQuery = "MATCH (n:Person)  WHERE n.name = 'Alice' RETURN n"
func ParseIndexHints(query string) ([]IndexHint, string) {
	var hints []IndexHint
	cleanQuery := query

	// Parse USING INDEX hints
	indexMatches := indexHintPattern.FindAllStringSubmatch(query, -1)
	for _, match := range indexMatches {
		if len(match) >= 4 {
			props := strings.Split(match[3], ",")
			for i := range props {
				props[i] = strings.TrimSpace(props[i])
			}
			
			hint := IndexHint{
				Type:       HintIndex,
				Variable:   match[1],
				Label:      match[2],
				Property:   props[0],
				Properties: props,
			}
			hints = append(hints, hint)
		}
	}
	// Remove index hints from query
	cleanQuery = indexHintPattern.ReplaceAllString(cleanQuery, "")

	// Parse USING SCAN hints
	scanMatches := scanHintPattern.FindAllStringSubmatch(cleanQuery, -1)
	for _, match := range scanMatches {
		if len(match) >= 3 {
			hint := IndexHint{
				Type:     HintScan,
				Variable: match[1],
				Label:    match[2],
			}
			hints = append(hints, hint)
		}
	}
	// Remove scan hints from query
	cleanQuery = scanHintPattern.ReplaceAllString(cleanQuery, "")

	// Parse USING JOIN hints
	joinMatches := joinHintPattern.FindAllStringSubmatch(cleanQuery, -1)
	for _, match := range joinMatches {
		if len(match) >= 2 {
			hint := IndexHint{
				Type:     HintJoin,
				Variable: match[1],
			}
			hints = append(hints, hint)
		}
	}
	// Remove join hints from query
	cleanQuery = joinHintPattern.ReplaceAllString(cleanQuery, "")

	// Clean up extra whitespace
	cleanQuery = strings.Join(strings.Fields(cleanQuery), " ")

	return hints, cleanQuery
}

// IndexHintContext holds index hints for a query execution.
type IndexHintContext struct {
	Hints      []IndexHint
	HintsByVar map[string][]IndexHint // Variable -> hints for that variable
}

// NewIndexHintContext creates a new context from parsed hints.
func NewIndexHintContext(hints []IndexHint) *IndexHintContext {
	ctx := &IndexHintContext{
		Hints:      hints,
		HintsByVar: make(map[string][]IndexHint),
	}
	
	for _, hint := range hints {
		ctx.HintsByVar[hint.Variable] = append(ctx.HintsByVar[hint.Variable], hint)
	}
	
	return ctx
}

// GetHintsForVariable returns all hints for a specific variable.
func (ctx *IndexHintContext) GetHintsForVariable(variable string) []IndexHint {
	if ctx == nil || ctx.HintsByVar == nil {
		return nil
	}
	return ctx.HintsByVar[variable]
}

// HasIndexHint checks if there's an index hint for a variable/label/property combination.
func (ctx *IndexHintContext) HasIndexHint(variable, label, property string) bool {
	if ctx == nil {
		return false
	}
	
	hints := ctx.HintsByVar[variable]
	for _, hint := range hints {
		if hint.Type == HintIndex && 
			strings.EqualFold(hint.Label, label) && 
			strings.EqualFold(hint.Property, property) {
			return true
		}
	}
	return false
}

// ShouldForceScan checks if a label scan should be forced for a variable.
func (ctx *IndexHintContext) ShouldForceScan(variable, label string) bool {
	if ctx == nil {
		return false
	}
	
	hints := ctx.HintsByVar[variable]
	for _, hint := range hints {
		if hint.Type == HintScan && strings.EqualFold(hint.Label, label) {
			return true
		}
	}
	return false
}

// ApplyIndexHint attempts to use the specified index for node lookup.
// Returns nodes matching the index lookup, or nil if index doesn't exist.
//
// Parameters:
//   - storage: The storage engine to query
//   - schema: The schema manager for index lookup
//   - hint: The index hint to apply
//   - propertyValue: The value to look up in the index
//
// Returns:
//   - Matching nodes if index exists and value found
//   - nil, nil if index doesn't exist (fall back to scan)
//   - nil, error if lookup fails
func ApplyIndexHint(store storage.Engine, schema *storage.SchemaManager, hint IndexHint, propertyValue interface{}) ([]*storage.Node, error) {
	if hint.Type != HintIndex {
		return nil, nil // Not an index hint
	}

	// Check if the index exists
	if schema == nil {
		return nil, nil // No schema manager, fall back to scan
	}

	// Try to use the property index for O(1) lookup
	nodeIDs := schema.PropertyIndexLookup(hint.Label, hint.Property, propertyValue)
	if nodeIDs != nil {
		// Index exists and was used! Get the actual nodes.
		var result []*storage.Node
		for _, nodeID := range nodeIDs {
			node, err := store.GetNode(nodeID)
			if err == nil && node != nil {
				result = append(result, node)
			}
		}
		return result, nil
	}

	// No index found - fall back to label scan with property filter
	nodes, err := store.GetNodesByLabel(hint.Label)
	if err != nil {
		return nil, err
	}

	// Filter by property value
	var result []*storage.Node
	for _, node := range nodes {
		if val, ok := node.Properties[hint.Property]; ok {
			if compareValues(val, propertyValue) {
				result = append(result, node)
			}
		}
	}

	return result, nil
}

// Note: compareValues and toFloat64 are defined in case_expression.go and helpers.go

// ValidateIndexHints checks if all specified index hints can be satisfied.
// Returns an error if any hint references a non-existent index.
//
// Parameters:
//   - schema: The schema manager to validate against
//   - hints: The index hints to validate
//
// Returns:
//   - nil if all hints are valid
//   - Error describing which hint cannot be satisfied
func ValidateIndexHints(schema *storage.SchemaManager, hints []IndexHint) error {
	if schema == nil || len(hints) == 0 {
		return nil
	}

	indexes := schema.GetIndexes()
	indexMap := make(map[string]bool)
	
	for _, idx := range indexes {
		if m, ok := idx.(map[string]interface{}); ok {
			label, _ := m["label"].(string)
			props, _ := m["properties"].([]string)
			if len(props) > 0 {
				key := fmt.Sprintf("%s:%s", strings.ToLower(label), strings.ToLower(props[0]))
				indexMap[key] = true
			}
		}
	}

	for _, hint := range hints {
		if hint.Type == HintIndex {
			key := fmt.Sprintf("%s:%s", strings.ToLower(hint.Label), strings.ToLower(hint.Property))
			if !indexMap[key] {
				// Neo4j returns a specific error for missing indexes
				return fmt.Errorf("no index found for hint: %s (index on :%s(%s) does not exist)", 
					hint.String(), hint.Label, hint.Property)
			}
		}
	}

	return nil
}

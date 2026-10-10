package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
)

// patternNodeVariables caches the node variables of a MATCH body's pattern
// (the text before WHERE), by body text.
var patternNodeVariables = newBoundedCache[string, []string](1024)

// validateBoundPatternNodes is Neo4j's run-time check of the node variables
// of a MATCH or OPTIONAL MATCH pattern that the input rows already bind: a
// value that is neither null nor a node is a TypeError
// (WITH x.node AS m MATCH (m) where m is a string). A variable whose type is
// known before the statement runs is rejected then (projectedExpressionSemanticKind);
// a relationship position bound to another value only matches nothing, as in
// Neo4j. A map or a value of no Cypher type is not rejected here.
func validateBoundPatternNodes(rows []pipelineRow, body string) error {
	if len(rows) == 0 || len(rows) == 1 && len(rows[0]) == 0 {
		return nil
	}
	variables, cached := patternNodeVariables.get(body)
	if !cached {
		pattern := body
		if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
			pattern = body[:where]
		}
		variables = extractNodeVariables(pattern)
		patternNodeVariables.put(body, variables)
	}
	for _, row := range rows {
		for _, variable := range variables {
			if value, bound := row[variable]; bound {
				if err := boundPatternNodeValueError(variable, value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// boundPatternNodeValueError is the TypeError for a pattern's node variable
// bound to value, or nil when value is a node, null, a map, or a value of no
// Cypher type.
func boundPatternNodeValueError(variable string, value interface{}) error {
	switch cypherValueKindOf(value) {
	case valueKindNode, valueKindNull, valueKindMap, valueKindOther:
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCorePatternNodeValueType(variable, cypherTypeName(value)))
}

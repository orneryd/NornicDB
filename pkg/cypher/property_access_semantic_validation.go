package cypher

import (
	"fmt"
	"strings"
)

// validateStaticPropertyAccessTypes rejects property access on aliases whose
// projection establishes a non-map scalar or list type. Entity, map, null,
// parameter, and otherwise unknown values remain valid because their property
// capability is either known or must be resolved at runtime.
func validateStaticPropertyAccessTypes(cypher string) error {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	invalidPropertySources := make(map[string]bool)
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseWith, pipelineClauseReturn:
			keyword := "WITH"
			if clause.kind == pipelineClauseReturn {
				keyword = "RETURN"
			}
			body := projectionSemanticBody(clause.text, keyword)
			for _, raw := range splitTopLevelComma(body) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
				if variable, _, propertyAccess := parseVarPropertyRef(expression); propertyAccess && invalidPropertySources[normalizeProjectionColumnName(variable)] {
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"InvalidArgumentType",
						fmt.Sprintf("property access is not supported on %s", variable),
					)
				}
			}
			if clause.kind == pipelineClauseWith {
				next := make(map[string]bool)
				for _, raw := range splitTopLevelComma(body) {
					expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
					if expression == "*" {
						for variable, invalid := range invalidPropertySources {
							next[variable] = invalid
						}
						continue
					}
					name := alias
					if name == "" {
						name = simpleSemanticIdentifier(expression)
					}
					if name == "" {
						continue
					}
					invalid := staticallyRejectsPropertyAccess(expression)
					if source := simpleSemanticIdentifier(expression); source != "" && invalidPropertySources[source] {
						invalid = true
					}
					next[normalizeProjectionColumnName(name)] = invalid
				}
				invalidPropertySources = next
			}
		}
	}
	return nil
}

func projectionSemanticBody(clause, keyword string) string {
	body := strings.TrimSpace(clause[len(keyword):])
	body, _ = cutDistinct(body)
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	return strings.TrimSpace(body[:end])
}

func staticallyRejectsPropertyAccess(expression string) bool {
	expression = strings.TrimSpace(expression)
	if strings.EqualFold(expression, "null") || strings.HasPrefix(expression, "$") {
		return false
	}
	if _, list := stripEnclosingRowDelimiter(expression, '[', ']'); list {
		return true
	}
	value, literal := parseLiteralValueFromComputedRow(expression)
	if !literal || value == nil {
		return false
	}
	switch value.(type) {
	case map[string]interface{}:
		return false
	default:
		return true
	}
}

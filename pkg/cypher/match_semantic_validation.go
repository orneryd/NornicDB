package cypher

import (
	"fmt"
	"strings"
)

type matchBindingKind uint8

const (
	matchBindingUnknown matchBindingKind = iota
	matchBindingNode
	matchBindingRelationship
	matchBindingPath
	matchBindingValue
)

type matchSemanticScope map[string]matchBindingKind

// validateMatchSemanticScopes applies entity-type rules before physical query
// routing. MATCH variables may be reused only when their binding kind remains
// stable; a node name cannot already denote a relationship, path, or scalar.
func (e *StorageExecutor) validateMatchSemanticScopes(cypher string) error {
	if e.matchSemanticValidationCache.contains(cypher) {
		return nil
	}
	if findKeywordIndexInContext(cypher, "MATCH") < 0 {
		return nil
	}
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		for _, branch := range branches {
			if err := e.validateMatchSemanticScopes(branch); err != nil {
				return err
			}
		}
		e.matchSemanticValidationCache.add(cypher)
		return nil
	}

	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	scope := make(matchSemanticScope)
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			if err := validateMatchClauseBindings(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseWith:
			scope = projectMatchSemanticScope(scope, clause.text)
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope[alias] = matchBindingValue
			}
		case pipelineClauseCreate, pipelineClauseMerge:
			addMatchPatternBindingKinds(scope, clause.text)
		}
	}
	e.matchSemanticValidationCache.add(cypher)
	return nil
}

func validateMatchClauseBindings(scope matchSemanticScope, clause string) error {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	if where := findKeywordIndexInContext(pattern, "WHERE"); where >= 0 {
		pattern = strings.TrimSpace(pattern[:where])
	}
	if mergePatternUsesParameterPredicate(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidParameterUse",
			"MATCH pattern predicates must use a property map, not a parameter map",
		)
	}

	for _, variable := range extractNodeVariables(pattern) {
		if err := bindMatchSemanticKind(scope, variable, matchBindingNode); err != nil {
			return err
		}
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if err := bindMatchSemanticKind(scope, variable, matchBindingRelationship); err != nil {
			return err
		}
	}
	for _, patternPart := range splitTopLevelComma(pattern) {
		if variable := extractPathAssignmentVariable(strings.TrimSpace(patternPart)); variable != "" {
			if err := bindMatchSemanticKind(scope, variable, matchBindingPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func bindMatchSemanticKind(scope matchSemanticScope, variable string, kind matchBindingKind) error {
	variable = normalizeProjectionColumnName(strings.TrimSpace(variable))
	if variable == "" {
		return nil
	}
	if existing, found := scope[variable]; found && existing != matchBindingUnknown && existing != kind {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"VariableTypeConflict",
			fmt.Sprintf("variable %s is already bound to a different entity type", variable),
		)
	}
	scope[variable] = kind
	return nil
}

func projectMatchSemanticScope(input matchSemanticScope, clause string) matchSemanticScope {
	body := strings.TrimSpace(clause[len("WITH"):])
	for _, keyword := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := findKeywordIndexInContext(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}

	output := make(matchSemanticScope)
	for _, raw := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expression == "*" {
			for variable, kind := range input {
				output[variable] = kind
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
		kind := matchBindingValue
		if source := simpleSemanticIdentifier(expression); source != "" {
			if sourceKind, found := input[source]; found {
				kind = sourceKind
			}
		}
		output[normalizeProjectionColumnName(name)] = kind
	}
	return output
}

func simpleSemanticIdentifier(expression string) string {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok || strings.TrimSpace(expression[next:]) != "" {
		return ""
	}
	return normalizeProjectionColumnName(name)
}

func addMatchPatternBindingKinds(scope matchSemanticScope, clause string) {
	for _, variable := range extractNodeVariables(clause) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingNode
		}
	}
	for _, variable := range extractRelationshipVariables(clause) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingRelationship
		}
	}
	for _, patternPart := range splitTopLevelComma(clause) {
		if variable := extractPathAssignmentVariable(strings.TrimSpace(patternPart)); variable != "" {
			if _, found := scope[variable]; !found {
				scope[variable] = matchBindingPath
			}
		}
	}
}

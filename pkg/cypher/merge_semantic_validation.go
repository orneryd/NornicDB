package cypher

import (
	"fmt"
	"strings"
	"sync"
)

type mergeSemanticValidationCache struct {
	mu      sync.RWMutex
	entries map[string]struct{}
	max     int
}

func newMergeSemanticValidationCache(max int) *mergeSemanticValidationCache {
	return &mergeSemanticValidationCache{entries: make(map[string]struct{}, max), max: max}
}

func (cache *mergeSemanticValidationCache) contains(query string) bool {
	if cache == nil {
		return false
	}
	cache.mu.RLock()
	_, exists := cache.entries[query]
	cache.mu.RUnlock()
	return exists
}

func (cache *mergeSemanticValidationCache) add(query string) {
	if cache == nil || cache.max <= 0 {
		return
	}
	cache.mu.Lock()
	if len(cache.entries) >= cache.max {
		clear(cache.entries)
	}
	cache.entries[query] = struct{}{}
	cache.mu.Unlock()
}

// validateMergeSemanticScopes applies the MERGE binding and pattern-value
// contract before routing. Keeping this validation ahead of every physical
// executor prevents optimized and general MERGE paths from accepting different
// syntax or producing effects before a semantic failure.
func (e *StorageExecutor) validateMergeSemanticScopes(cypher string) error {
	if findKeywordIndexInContext(cypher, "MERGE") < 0 {
		return nil
	}
	if e.mergeSemanticValidationCache.contains(cypher) {
		return nil
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}

	scope := newSemanticBindingScope()
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			addMergePatternBindings(scope, clause.text)
		case pipelineClauseCreate:
			addMergePatternBindings(scope, clause.text)
		case pipelineClauseWith:
			scope = projectedBindingScope(scope, clause.text)
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope.bind(alias)
			}
		case pipelineClauseMerge:
			if err := e.validateMergeClause(scope, clause.text); err != nil {
				return err
			}
		}
	}
	e.mergeSemanticValidationCache.add(cypher)
	return nil
}

func addMergePatternBindings(scope *semanticBindingScope, clause string) {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH", "CREATE"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	for _, name := range extractNodeVariables(pattern) {
		scope.bind(name)
	}
	for _, name := range extractRelationshipVariables(pattern) {
		scope.bind(name)
	}
	if name := extractPathAssignmentVariable(pattern); name != "" {
		scope.bind(name)
	}
}

func (e *StorageExecutor) validateMergeClause(scope *semanticBindingScope, clause string) error {
	pattern := mergeClausePattern(clause)
	if pattern == "" {
		return nil
	}
	if mergePatternUsesParameterPredicate(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidParameterUse",
			"MERGE pattern predicates must use a property map, not a parameter map",
		)
	}
	if e.mergePatternContainsNullProperty(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SemanticError",
			"MergeReadOwnWrites",
			"MERGE cannot match or create an entity using a null property value",
		)
	}

	relationshipPattern := containsOutsideStrings(pattern, "-[")
	for _, nodePattern := range e.splitNodePatterns(pattern) {
		variable := createNodePatternVariable(nodePattern)
		if variable == "" {
			continue
		}
		if scope.contains(variable) {
			if !relationshipPattern || createNodePatternHasDecoration(nodePattern) {
				return mergeVariableAlreadyBoundError(variable)
			}
			continue
		}
		scope.bind(variable)
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if scope.contains(variable) {
			return mergeVariableAlreadyBoundError(variable)
		}
		scope.bind(variable)
	}
	if pathVariable := extractPathAssignmentVariable(pattern); pathVariable != "" {
		if scope.contains(pathVariable) {
			return mergeVariableAlreadyBoundError(pathVariable)
		}
		scope.bind(pathVariable)
	}
	return nil
}

func mergeClausePattern(clause string) string {
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "MERGE") {
		body = strings.TrimSpace(body[len("MERGE"):])
	}
	end := len(body)
	for _, keyword := range []string{"ON CREATE SET", "ON MATCH SET"} {
		if index := findKeywordIndexInContext(body, keyword); index >= 0 && index < end {
			end = index
		}
	}
	return strings.TrimSpace(body[:end])
}

func mergePatternUsesParameterPredicate(pattern string) bool {
	depth := 0
	quote := byte(0)
	for index := 0; index < len(pattern); index++ {
		character := pattern[index]
		if quote != 0 {
			if character == '\\' && quote != '`' {
				index++
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case '$':
			if depth == 0 {
				return true
			}
		}
	}
	return false
}

func (e *StorageExecutor) mergePatternContainsNullProperty(pattern string) bool {
	for cursor := 0; cursor < len(pattern); cursor++ {
		if pattern[cursor] != '{' {
			continue
		}
		end := e.findMatchingBrace(pattern, cursor)
		if end < 0 {
			return false
		}
		for _, entry := range splitTopLevelComma(pattern[cursor+1 : end]) {
			colon := topLevelColonIndex(entry)
			if colon >= 0 && strings.EqualFold(strings.TrimSpace(entry[colon+1:]), "null") {
				return true
			}
		}
		cursor = end
	}
	return false
}

func mergeVariableAlreadyBoundError(variable string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"VariableAlreadyBound",
		fmt.Sprintf("variable %s is already bound and cannot be redeclared by MERGE", variable),
	)
}

package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"

	math "github.com/orneryd/nornicdb/pkg/math/libm"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type semanticValidationCache struct {
	shards [boundedCacheShardCount]semanticValidationCacheShard
	max    int
}

type semanticValidationCacheShard struct {
	mu      sync.RWMutex
	entries map[string]semanticValidationEntry
}

// Admission and its immutable lowering facts have the same bounded owner. A
// recording alone does not imply that every semantic check has admitted a query.
type semanticValidationEntry struct {
	valid     bool
	recording *scopeRecording
}

func newSemanticValidationCache(max int) *semanticValidationCache {
	c := &semanticValidationCache{max: max}
	shardLimit := max / boundedCacheShardCount
	for i := range c.shards {
		c.shards[i].entries = make(map[string]semanticValidationEntry, shardLimit/4+1)
	}
	return c
}

func (cache *semanticValidationCache) contains(query string) bool {
	if cache == nil {
		return false
	}
	sh := &cache.shards[boundedCacheStringShardIndex(query)]
	sh.mu.RLock()
	entry := sh.entries[query]
	sh.mu.RUnlock()
	return entry.valid
}

func (cache *semanticValidationCache) add(query string) {
	if cache == nil || cache.max <= 0 {
		return
	}
	sh := &cache.shards[boundedCacheStringShardIndex(query)]
	sh.mu.Lock()
	if len(sh.entries) >= cache.max/boundedCacheShardCount {
		clear(sh.entries)
	}
	entry := sh.entries[query]
	entry.valid = true
	sh.entries[query] = entry
	sh.mu.Unlock()
}

func (cache *semanticValidationCache) recording(query string) *scopeRecording {
	if cache == nil {
		return nil
	}
	sh := &cache.shards[boundedCacheStringShardIndex(query)]
	sh.mu.RLock()
	record := sh.entries[query].recording
	sh.mu.RUnlock()
	return record
}

func (cache *semanticValidationCache) addRecording(query string, record *scopeRecording) {
	if cache == nil || cache.max <= 0 {
		return
	}
	sh := &cache.shards[boundedCacheStringShardIndex(query)]
	sh.mu.Lock()
	if len(sh.entries) >= cache.max/boundedCacheShardCount {
		clear(sh.entries)
	}
	entry := sh.entries[query]
	entry.recording = record
	sh.entries[query] = entry
	sh.mu.Unlock()
}

// validateMergeSemanticScopes applies the MERGE binding and pattern-value
// contract before routing. Keeping this validation ahead of every physical
// executor prevents optimized and general MERGE paths from accepting different
// syntax or producing effects before a semantic failure.
func (e *StorageExecutor) validateMergeSemanticScopes(cypher string, cypher25 bool, inputs ...*semanticStatementInput) error {
	if findKeywordIndexInContext(cypher, "MERGE") < 0 {
		return nil
	}
	ctx := context.Background()
	if cypher25 {
		ctx = context.WithValue(ctx, cypherVersionKey{}, "25")
	}
	var input *semanticStatementInput
	if len(inputs) > 0 {
		input = inputs[0]
	}
	return e.validateSemanticScopesWithInput(ctx, cypher, input)
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

// cypher25 adds Neo4j 2026.09's rule for a Cypher 25 statement: a property
// value may not read a node or relationship the same MERGE creates
// (MERGE (a {x: 1})-[:T]->(b {x: a.x})).
func (e *StorageExecutor) validateMergeClause(scope *semanticBindingScope, clause string, cypher25 bool) error {
	// Neo4j rejects EXISTS / COUNT / COLLECT { … } anywhere in a MERGE clause:
	// its pattern, property maps and ON CREATE / ON MATCH SET actions.
	if mayContainSubqueryExpression(clause) && len(findSubqueryExpressions(clause)) > 0 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"SubqueryInMerge",
			"Subquery expressions are not allowed in a MERGE clause.",
		)
	}
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

	relationshipPattern := patternHasRelationship(pattern)
	relationshipVariables := extractRelationshipVariables(pattern)
	for _, variable := range relationshipVariables {
		if scope.contains(variable) {
			return mergeVariableAlreadyBoundError(variable)
		}
	}
	if cypher25 {
		if err := e.validateCreateSamePatternReferences(scope, pattern, relationshipVariables, make(map[string]string), "MERGE"); err != nil {
			return err
		}
	}
	if relationshipPattern {
		// Every relationship of a MERGE path (mergePathSegments).
		segments := mergePathSegments(pattern)
		if len(segments) < 2 {
			segments = []string{pattern}
		}
		for _, segment := range segments {
			if err := validateMergeRelationshipShape(segment); err != nil {
				return err
			}
		}
	}
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
	for _, variable := range relationshipVariables {
		scope.bind(variable)
	}
	if pathVariable := extractPathAssignmentVariable(pattern); pathVariable != "" {
		if scope.contains(pathVariable) {
			return mergeVariableAlreadyBoundError(pathVariable)
		}
		scope.bind(pathVariable)
	}
	if err := e.validateMergeActionScopes(scope, clause); err != nil {
		return err
	}
	for _, nodePattern := range e.splitNodePatterns(pattern) {
		if err := e.validateCreatePatternExpressions(scope, nodePattern); err != nil {
			return err
		}
	}
	if err := e.validateCreateRelationshipExpressions(scope, pattern); err != nil {
		return err
	}
	return nil
}

func (e *StorageExecutor) validateMergeActionScopes(scope *semanticBindingScope, clause string) error {
	parts := splitMergeClauseActions(mergeClauseBody(clause))
	for _, clauses := range [...]mergeActionClauses{parts.onCreate, parts.onMatch} {
		for index := 0; index < clauses.len(); index++ {
			if err := e.validateSetClauseScope(scope, clauses.set(index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateMergeRelationshipShape(pattern string) error {
	open, close := firstRelationshipBracket(pattern)
	if open < 0 || close < 0 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			"MERGE relationships require exactly one relationship type",
		)
	}

	return validateSingleRelationshipDeclaration("MERGE", "merged", pattern[open+1:close])
}

func mergeClausePattern(clause string) string {
	return splitMergeClauseActions(mergeClauseBody(clause)).pattern
}

// mergeClauseBody is a MERGE clause's text after the MERGE keyword.
func mergeClauseBody(clause string) string {
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "MERGE") {
		body = strings.TrimSpace(body[len("MERGE"):])
	}
	return body
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
			if dynamicLabelStartsAt(pattern, index) {
				// A dynamic label or type ($(e), $all(e), $any(e)), not a
				// parameter map.
				if closing := findMatchingDelimiter(pattern, strings.IndexByte(pattern[index:], '(')+index, '(', ')'); closing > 0 {
					index = closing
				}
				continue
			}
			// A $ inside a name (n:L$M) is no parameter.
			if depth == 0 && isParameterStart(pattern, index) {
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

// validateMergePatternProperties rejects a MERGE pattern's property whose
// value can't identify an entity (null or NaN) with Neo4j's SemanticError,
// then checks the values are storable (validatePropertyValues). entity
// ("node" or "relationship") names the pattern element in the message.
func validateMergePatternProperties(properties map[string]interface{}, entity string) error {
	for property, value := range properties {
		switch {
		case value == nil:
			return localizedStatusError("Neo.ClientError.Statement.SemanticError", "MergeNullPropertyValue",
				localization.CypherMergePropertyValueNotMergeable(entity, "null", property))
		case isNaNValue(value):
			// NaN equals nothing, itself included, so it can't identify an
			// entity; a NaN inside a list can (Neo4j 5.26, #907).
			return localizedStatusError("Neo.ClientError.Statement.SemanticError", "MergeNaNPropertyValue",
				localization.CypherMergePropertyValueNotMergeable(entity, "NaN", property))
		}
	}
	return validatePropertyValues(properties)
}

func mergeVariableAlreadyBoundError(variable string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"VariableAlreadyBound",
		fmt.Sprintf("variable %s is already bound and cannot be redeclared by MERGE", variable),
	)
}

// isNaNValue reports whether value is a floating-point NaN.
func isNaNValue(value interface{}) bool {
	switch typed := value.(type) {
	case float64:
		return math.IsNaN(typed)
	case float32:
		return math.IsNaN(float64(typed))
	}
	return false
}

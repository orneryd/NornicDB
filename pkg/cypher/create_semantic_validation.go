package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// bindingScope is the shared compile-time name set used while validating
// clause composition. Runtime values deliberately do not live here: semantic
// validation must finish before a mutation handler can produce effects.
type semanticBindingScope struct {
	names map[string]struct{}
}

func newSemanticBindingScope() *semanticBindingScope {
	return &semanticBindingScope{names: make(map[string]struct{})}
}

// bind and contains consume decoded identities. Written pattern adapters use
// bindWritten; projection aliases and inherited bindings must not be decoded twice.
func (s *semanticBindingScope) bind(name string) {
	if name != "" {
		s.names[name] = struct{}{}
	}
}

func (s *semanticBindingScope) contains(name string) bool {
	_, exists := s.names[name]
	return exists
}

func (s *semanticBindingScope) bindWritten(written string) {
	s.bind(normalizeProjectionColumnName(written))
}

// validateCreateSemanticScopes applies one symbol contract before routing to
// any of the specialized CREATE executors. It intentionally consumes the same
// top-level clause decomposition as the semantic pipeline so fast paths cannot
// disagree about whether a name is new, bound, or undefined.
func (e *StorageExecutor) validateCreateSemanticScopes(ctx context.Context, cypher string, cypher25 bool) error {
	if findKeywordIndexInContext(cypher, "CREATE") < 0 || isMetadataCommand(cypher, metadataCreateAdmission) {
		return nil
	}
	clauses, ok := splitPipelineClauses(ctx, cypher)
	if !ok {
		return nil
	}

	scope := newSemanticBindingScope()
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			for _, name := range extractNodeVariables(clause.text) {
				scope.bindWritten(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bindWritten(name)
			}
		case pipelineClauseWith:
			scope = projectedBindingScope(ctx, scope, clause.text)
		case pipelineClauseLet:
			if err := bindPipelineLet(scope, clause.text); err != nil {
				return err
			}
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope.bind(alias)
			}
		case pipelineClauseMerge:
			for _, name := range extractNodeVariables(clause.text) {
				scope.bindWritten(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bindWritten(name)
			}
		case pipelineClauseCreate:
			if err := e.validateCreateClauseBindings(scope, clause.text, cypher25); err != nil {
				return err
			}
		}
	}
	return nil
}

func projectedBindingScope(ctx context.Context, input *semanticBindingScope, clause string) *semanticBindingScope {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
	output := newSemanticBindingScope()
	for _, raw := range splitTopLevelComma(body) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(raw), projectionAdmitted)
		expr, alias := itemSource.expression, itemSource.alias
		if expr == "*" {
			for name := range input.names {
				output.bind(name)
			}
			continue
		}
		if alias != "" {
			output.bind(alias)
		} else if name, ok := cyphertext.ScanIdentifier(expr, 0); ok && strings.TrimSpace(expr[name.End:]) == "" {
			output.bind(name.Value)
		}
	}
	return output
}

// validateUnwindAlias rejects an UNWIND clause whose alias isn't exactly one
// variable, as Neo4j does: UNWIND takes no WHERE, so "UNWIND l AS x WHERE …"
// and "UNWIND l AS x y" are SyntaxErrors (Invalid input 'WHERE').
func validateUnwindAlias(clause string) error {
	_, alias, ok := parsePipelineIteration(clause)
	if !ok {
		return sharedClauseSyntaxError("iteration requires a variable and an expression")
	}
	next, identifier := cyphertext.ScanIdentifier(alias, 0)
	if !identifier {
		return nil
	}
	if rest := strings.TrimSpace(alias[next.End:]); rest != "" {
		token := strings.Fields(rest)[0]
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInput(token))
	}
	return nil
}

// unwindBindingName is the variable an UNWIND clause binds (splitUnwindBody).
func unwindBindingName(clause string) string {
	if _, alias, ok := parsePipelineIteration(clause); ok {
		if name, _, symbolic := scanSymbolicName(alias, 0); symbolic {
			return normalizeProjectionColumnName(name)
		}
	}
	return ""
}

func (e *StorageExecutor) validateCreateClauseBindings(scope *semanticBindingScope, clause string, cypher25 bool) error {
	body := strings.TrimSpace(clause[len("CREATE"):])
	created := make(map[string]string)
	for _, pattern := range e.splitCreatePatterns(body) {
		if !cypher25 {
			clear(created)
		}
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		isRelationshipPattern := patternHasRelationship(pattern)
		relationshipVariables := extractRelationshipVariables(pattern)
		for _, variable := range relationshipVariables {
			variable = normalizeProjectionColumnName(variable)
			if scope.contains(variable) {
				return createVariableAlreadyBoundError(variable)
			}
		}
		if isRelationshipPattern {
			if err := validateRelationshipMutation(pattern, createRelationshipMutation); err != nil {
				return err
			}
		}

		if err := e.validateCreateSamePatternReferences(scope, pattern, relationshipVariables, created, "CREATE"); err != nil {
			return err
		}

		for _, nodePattern := range e.splitNodePatterns(pattern) {
			variable := createNodePatternVariable(nodePattern)
			if err := e.validateCreatePatternExpressions(scope, nodePattern); err != nil {
				return err
			}
			if variable == "" {
				continue
			}
			if scope.contains(variable) {
				if !isRelationshipPattern || createNodePatternHasDecoration(nodePattern) {
					return createVariableAlreadyBoundError(variable)
				}
				continue
			}
			scope.bind(variable)
		}

		if err := e.validateCreateRelationshipExpressions(scope, pattern); err != nil {
			return err
		}
		for _, variable := range relationshipVariables {
			scope.bindWritten(variable)
		}
	}
	return nil
}

func createNodePatternVariable(pattern string) string {
	trimmed := strings.TrimSpace(pattern)
	if strings.HasPrefix(trimmed, "(") {
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	name, ok := cyphertext.ScanIdentifier(trimmed, 0)
	if !ok {
		return ""
	}
	return name.Value
}

func createNodePatternHasDecoration(pattern string) bool {
	trimmed := strings.TrimSpace(pattern)
	if strings.HasPrefix(trimmed, "(") {
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	next, ok := cyphertext.ScanIdentifier(trimmed, 0)
	if !ok {
		return false
	}
	remainder := strings.TrimSpace(trimmed[next.End:])
	return strings.HasPrefix(remainder, ":") || strings.HasPrefix(remainder, "{")
}

func createVariableAlreadyBoundError(variable string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"VariableAlreadyBound",
		fmt.Sprintf("variable %s is already bound and cannot be redeclared by CREATE", variable),
	)
}

func createUndefinedVariableError(variable string) error {
	message := localization.CypherAdminVariableNotDefined(variable)
	// Preserve the semantic error in the chain for callers that inspect its
	// detail, while the client-visible text uses the localized constructor.
	semantic := newSemanticError("Neo.ClientError.Statement.SyntaxError", "UndefinedVariable", message.Fallback)
	return &classifiedCypherError{
		cause:  localizedError(message, semantic),
		code:   "Neo.ClientError.Statement.SyntaxError",
		detail: "UndefinedVariable",
	}
}

func (e *StorageExecutor) validateCreatePatternExpressions(scope *semanticBindingScope, pattern string) error {
	properties, ok := createPropertyMapBody(pattern)
	if !ok {
		return nil
	}
	return e.validateCreatePropertyExpressions(scope, properties)
}

func (e *StorageExecutor) validateCreateRelationshipExpressions(scope *semanticBindingScope, pattern string) error {
	for _, properties := range createRelationshipPropertyMapBodies(pattern) {
		if err := e.validateCreatePropertyExpressions(scope, properties); err != nil {
			return err
		}
	}
	return nil
}

// createPropertyMapBody returns the text between the braces of a node (or
// relationship) pattern's property map.
func createPropertyMapBody(pattern string) (string, bool) {
	open := indexByteOutsideBackticks(pattern, '{')
	close := strings.LastIndex(pattern, "}")
	if open < 0 || close <= open {
		return "", false
	}
	return pattern[open+1 : close], true
}

// createRelationshipPropertyMapBodies returns the property map text of every
// relationship ([...]) in a CREATE path pattern, read by the declaration
// owner: a '{' in a dynamic type is not the map.
func createRelationshipPropertyMapBodies(pattern string) []string {
	var bodies []string
	for open, close := readRelationshipBracket(pattern, 0, requireClosedRelationship); open >= 0; open, close = readRelationshipBracket(pattern, close+1, requireClosedRelationship) {
		if properties, ok := relationshipDeclarationHead(pattern[open+1 : close]).propertyMapBody(); ok {
			bodies = append(bodies, properties)
		}
	}
	return bodies
}

// validateCreateSamePatternReferences rejects a property value that reads a
// node or relationship created by the same CREATE path pattern, including a
// node's own map ((a {x: a.y})), as Neo4j does: the entity does not exist yet
// when its pattern's properties are evaluated. In a Cypher 5 statement a
// later comma-separated pattern may read it; names bound inside the
// expression (list comprehension iterators, reduce / all / any / none /
// single) are not references.
//
// created holds the clause's entities already read (and receives this
// pattern's): a fresh map per pattern for a Cypher 5 statement, one map per
// clause for a Cypher 25 statement, where a pattern may not read an entity an
// earlier pattern of the same CREATE creates either (Neo4j 2026.09).
func (e *StorageExecutor) validateCreateSamePatternReferences(scope *semanticBindingScope, pattern string, relationshipVariables []string, created map[string]string, clause string) error {
	nodePatterns := e.splitNodePatterns(pattern)
	for _, nodePattern := range nodePatterns {
		if variable := createNodePatternVariable(nodePattern); variable != "" && !scope.contains(variable) {
			created[variable] = "Node"
		}
	}
	for _, variable := range relationshipVariables {
		created[normalizeProjectionColumnName(variable)] = "Relationship"
	}
	if len(created) == 0 {
		return nil
	}
	maps := createRelationshipPropertyMapBodies(pattern)
	for _, nodePattern := range nodePatterns {
		if properties, ok := createPropertyMapBody(nodePattern); ok {
			maps = append(maps, properties)
		}
	}
	for _, properties := range maps {
		for _, pair := range splitTopLevelComma(properties) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				continue
			}
			for _, variable := range expressionFreeVariables(pair[separator+1:]) {
				if kind, ok := created[variable]; ok {
					return createSamePatternReferenceError(variable, kind, clause)
				}
			}
		}
	}
	return nil
}

func createSamePatternReferenceError(variable, kind, clause string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableCreatedInSameClause",
		localization.CypherCoreEntityReferencedInSameClause(kind, variable, clause))
}

func (e *StorageExecutor) validateCreatePropertyExpressions(scope *semanticBindingScope, properties string) error {
	for _, pair := range splitTopLevelComma(properties) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		expression := strings.TrimSpace(pair[separator+1:])
		if err := validateExpressionOperandCompleteness(expression); err != nil {
			return err
		}
		variable, isReference := simpleCreatePropertyReference(expression)
		if isReference && !scope.contains(variable) {
			return createUndefinedVariableError(variable)
		}
	}
	return nil
}

func simpleCreatePropertyReference(expression string) (string, bool) {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.HasPrefix(expression, "$") ||
		strings.HasPrefix(expression, "'") || strings.HasPrefix(expression, "\"") ||
		strings.HasPrefix(expression, "[") || strings.HasPrefix(expression, "{") ||
		looksLikeFunctionCall(expression) {
		return "", false
	}
	if strings.EqualFold(expression, "null") || strings.EqualFold(expression, "true") || strings.EqualFold(expression, "false") {
		return "", false
	}
	if _, err := strconv.ParseInt(expression, 10, 64); err == nil {
		return "", false
	}
	if _, err := strconv.ParseFloat(expression, 64); err == nil {
		return "", false
	}
	root, ok := cyphertext.ScanIdentifier(expression, 0)
	if !ok {
		return "", false
	}
	// A qualified call can be followed by a property/subscript access. Its
	// namespace is not a variable, as in the shared free-reference reader.
	if _, call := qualifiedFunctionNameEnd(expression, 0, root.End); call {
		return "", false
	}
	remainder := strings.TrimSpace(expression[root.End:])
	if remainder == "" || strings.HasPrefix(remainder, ".") || strings.HasPrefix(remainder, "[") {
		return root.Value, true
	}
	return "", false
}

// singleRelationshipTypeError is Neo4j's error for alternative types
// ([:R|S]) in a CREATE or MERGE relationship.
func singleRelationshipTypeError(clause string) error {
	message := localization.CypherMatchingSingleRelationshipTypeRequired(clause)
	return localizedError(message, newSemanticError("Neo.ClientError.Statement.SyntaxError", "NoSingleRelationshipType", message.Fallback))
}

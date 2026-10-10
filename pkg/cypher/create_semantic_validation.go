package cypher

import (
	"fmt"
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

func (s *semanticBindingScope) bind(name string) {
	name = normalizeProjectionColumnName(strings.TrimSpace(name))
	if name != "" {
		s.names[name] = struct{}{}
	}
}

func (s *semanticBindingScope) contains(name string) bool {
	_, exists := s.names[normalizeProjectionColumnName(strings.TrimSpace(name))]
	return exists
}

// validateCreateSemanticScopes applies one symbol contract before routing to
// any of the specialized CREATE executors. It intentionally consumes the same
// top-level clause decomposition as the semantic pipeline so fast paths cannot
// disagree about whether a name is new, bound, or undefined.
func (e *StorageExecutor) validateCreateSemanticScopes(cypher string) error {
	if findKeywordIndexInContext(cypher, "CREATE") < 0 || isCreateSchemaOrAdministrationCommand(cypher) {
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
			for _, name := range extractNodeVariables(clause.text) {
				scope.bind(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bind(name)
			}
		case pipelineClauseWith:
			scope = projectedBindingScope(scope, clause.text)
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
				scope.bind(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bind(name)
			}
		case pipelineClauseCreate:
			if err := e.validateCreateClauseBindings(scope, clause.text); err != nil {
				return err
			}
		}
	}
	return nil
}

func isCreateSchemaOrAdministrationCommand(cypher string) bool {
	return isSystemCommandNoGraph(cypher) || isCreateProcedureCommand(cypher) ||
		startsWithKeywords(cypher, "CREATE", "DECAY PROFILE") ||
		startsWithKeywords(cypher, "CREATE", "PROMOTION PROFILE") ||
		startsWithKeywords(cypher, "CREATE", "PROMOTION POLICY") ||
		startsWithKeywords(cypher, "CREATE", "CONSTRAINT") ||
		startsWithKeywords(cypher, "CREATE", "INDEX") ||
		startsWithKeywords(cypher, "CREATE", "RANGE INDEX") ||
		startsWithKeywords(cypher, "CREATE", "FULLTEXT INDEX") ||
		startsWithKeywords(cypher, "CREATE", "VECTOR INDEX") ||
		startsWithKeywords(cypher, "CREATE", "TEXT INDEX") ||
		startsWithKeywords(cypher, "CREATE", "POINT INDEX") ||
		startsWithKeywords(cypher, "CREATE", "LOOKUP INDEX")
}

func projectedBindingScope(input *semanticBindingScope, clause string) *semanticBindingScope {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
	output := newSemanticBindingScope()
	for _, raw := range splitTopLevelComma(body) {
		expr, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expr == "*" {
			for name := range input.names {
				output.bind(name)
			}
			continue
		}
		if alias != "" {
			output.bind(alias)
		} else if name, next, ok := scanIdentifierToken(expr, 0); ok && strings.TrimSpace(expr[next:]) == "" {
			output.bind(name)
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
	_, next, identifier := scanIdentifierToken(alias, 0)
	if !identifier {
		return nil
	}
	if rest := strings.TrimSpace(alias[next:]); rest != "" {
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

func (e *StorageExecutor) validateCreateClauseBindings(scope *semanticBindingScope, clause string) error {
	body := strings.TrimSpace(clause[len("CREATE"):])
	for _, pattern := range e.splitCreatePatterns(body) {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		isRelationshipPattern := patternHasRelationship(pattern)
		relationshipVariables := extractRelationshipVariables(pattern)
		for _, variable := range relationshipVariables {
			if scope.contains(variable) {
				return createVariableAlreadyBoundError(variable)
			}
		}
		if isRelationshipPattern {
			if err := validateCreateRelationshipShape(pattern); err != nil {
				return err
			}
		}

		if err := e.validateCreateSamePatternReferences(scope, pattern, relationshipVariables); err != nil {
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
			scope.bind(variable)
		}
	}
	return nil
}

func validateCreateRelationshipShape(pattern string) error {
	open, close := firstRelationshipBracket(pattern)
	if open < 0 || close < 0 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			"CREATE relationships require exactly one relationship type",
		)
	}

	left := strings.TrimSpace(pattern[:open])
	right := strings.TrimSpace(pattern[close+1:])
	leftDirected := strings.HasSuffix(left, "<-")
	rightDirected := strings.HasPrefix(right, "->")
	if leftDirected == rightDirected {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"RequiresDirectedRelationship",
			"CREATE relationships require exactly one direction",
		)
	}

	return validateSingleRelationshipDeclaration("CREATE", "created", pattern[open+1:close])
}

// validateSingleRelationshipDeclaration applies CREATE's and MERGE's rules to
// a relationship's inside (relationshipDeclarationOf), read outside
// backticks: no variable length (verb is the clause's action, "created" or
// "merged"), and exactly one type, unless the type is dynamic ($(e), whose
// types are counted per row by resolveRowDynamicTokens). Alternatives
// ([:R|S]) are Neo4j's NoSingleRelationshipType error naming the clause.
func validateSingleRelationshipDeclaration(clause, verb, content string) error {
	declaration := relationshipDeclarationOf(content)
	if declaration.hasLength {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"CreatingVarLength",
			"variable-length relationships cannot be "+verb,
		)
	}
	if declaration.hasColon && hasDynamicToken(declaration.typeText) {
		return nil
	}
	if declaration.hasColon && indexByteOutsideBackticks(declaration.typeText, '|') >= 0 {
		return singleRelationshipTypeError(clause)
	}
	if declaration.invalidType != "" {
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherMutationsInvalidRelationshipType(declaration.invalidType), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "InvalidRelationshipType",
		}
	}
	if len(declaration.types) != 1 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			clause+" relationships require exactly one relationship type",
		)
	}
	return nil
}

func createNodePatternVariable(pattern string) string {
	trimmed := strings.TrimSpace(pattern)
	if strings.HasPrefix(trimmed, "(") {
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	name, _, ok := scanIdentifierToken(trimmed, 0)
	if !ok {
		return ""
	}
	return name
}

func createNodePatternHasDecoration(pattern string) bool {
	trimmed := strings.TrimSpace(pattern)
	if strings.HasPrefix(trimmed, "(") {
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	_, next, ok := scanIdentifierToken(trimmed, 0)
	if !ok {
		return false
	}
	remainder := strings.TrimSpace(trimmed[next:])
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
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UndefinedVariable",
		fmt.Sprintf("variable %s is not defined", variable),
	)
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
// relationship ([...]) in a CREATE path pattern.
func createRelationshipPropertyMapBodies(pattern string) []string {
	var bodies []string
	for offset := 0; offset < len(pattern); {
		open := strings.Index(pattern[offset:], "[")
		if open < 0 {
			break
		}
		open += offset
		close := findMatchingBracket(pattern, open)
		if close < 0 {
			break
		}
		if properties, ok := createPropertyMapBody(pattern[open+1 : close]); ok {
			bodies = append(bodies, properties)
		}
		offset = close + 1
	}
	return bodies
}

// validateCreateSamePatternReferences rejects a property value that reads a
// node or relationship created by the same CREATE path pattern, including a
// node's own map ((a {x: a.y})), as Neo4j does: the entity does not exist yet
// when its pattern's properties are evaluated. A later comma-separated pattern
// may read it, and names bound inside the expression (list comprehension
// iterators, reduce / all / any / none / single) are not references.
func (e *StorageExecutor) validateCreateSamePatternReferences(scope *semanticBindingScope, pattern string, relationshipVariables []string) error {
	created := make(map[string]string)
	nodePatterns := e.splitNodePatterns(pattern)
	for _, nodePattern := range nodePatterns {
		if variable := createNodePatternVariable(nodePattern); variable != "" && !scope.contains(variable) {
			created[variable] = "Node"
		}
	}
	for _, variable := range relationshipVariables {
		created[variable] = "Relationship"
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
		for _, pair := range e.splitPropertyPairs(properties) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 {
				continue
			}
			for _, variable := range expressionFreeVariables(pair[separator+1:]) {
				if kind, ok := created[variable]; ok {
					return createSamePatternReferenceError(variable, kind)
				}
			}
		}
	}
	return nil
}

func createSamePatternReferenceError(variable, kind string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"VariableCreatedInSameClause",
		fmt.Sprintf("The %s variable '%s' is referencing a %s that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.", kind, variable, kind),
	)
}

func (e *StorageExecutor) validateCreatePropertyExpressions(scope *semanticBindingScope, properties string) error {
	for _, pair := range e.splitPropertyPairs(properties) {
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
	root, next, ok := scanIdentifierToken(expression, 0)
	if !ok {
		return "", false
	}
	remainder := strings.TrimSpace(expression[next:])
	if remainder == "" || strings.HasPrefix(remainder, ".") || strings.HasPrefix(remainder, "[") {
		return root, true
	}
	return "", false
}

// singleRelationshipTypeError is Neo4j's error for alternative types
// ([:R|S]) in a CREATE or MERGE relationship.
func singleRelationshipTypeError(clause string) error {
	message := localization.CypherMatchingSingleRelationshipTypeRequired(clause)
	return localizedError(message, newSemanticError("Neo.ClientError.Statement.SyntaxError", "NoSingleRelationshipType", message.Fallback))
}

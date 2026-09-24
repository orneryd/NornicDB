package cypher

import (
	"fmt"
	"strconv"
	"strings"
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
		findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "LOOKUP INDEX") == 0
}

func projectedBindingScope(input *semanticBindingScope, clause string) *semanticBindingScope {
	body := strings.TrimSpace(clause[len("WITH"):])
	for _, keyword := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := findKeywordIndexInContext(body, keyword); index >= 0 {
			body = strings.TrimSpace(body[:index])
		}
	}
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
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

func unwindBindingName(clause string) string {
	if index := findKeywordIndexInContext(clause, "AS"); index >= 0 {
		start := index + len("AS")
		for start < len(clause) && isWhitespace(clause[start]) {
			start++
		}
		name, _, ok := scanIdentifierToken(clause, start)
		if ok {
			return name
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
		isRelationshipPattern := containsOutsideStrings(pattern, "->") ||
			containsOutsideStrings(pattern, "<-") || containsOutsideStrings(pattern, "-[")
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

	content := strings.TrimSpace(pattern[open+1 : close])
	declaration := content
	if props := strings.Index(declaration, "{"); props >= 0 {
		declaration = strings.TrimSpace(declaration[:props])
	}
	if strings.Contains(declaration, "*") {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"CreatingVarLength",
			"variable-length relationships cannot be created",
		)
	}
	colon := strings.Index(declaration, ":")
	if colon < 0 {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			"CREATE relationships require exactly one relationship type",
		)
	}
	typeDeclaration := strings.TrimSpace(declaration[colon+1:])
	if typeDeclaration == "" || strings.Contains(typeDeclaration, "|") || strings.Contains(typeDeclaration, ":") {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NoSingleRelationshipType",
			"CREATE relationships require exactly one relationship type",
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
	open := strings.Index(pattern, "{")
	close := strings.LastIndex(pattern, "}")
	if open < 0 || close <= open {
		return nil
	}
	return e.validateCreatePropertyExpressions(scope, pattern[open+1:close])
}

func (e *StorageExecutor) validateCreateRelationshipExpressions(scope *semanticBindingScope, pattern string) error {
	for offset := 0; offset < len(pattern); {
		open := strings.Index(pattern[offset:], "[")
		if open < 0 {
			return nil
		}
		open += offset
		close := findMatchingBracket(pattern, open)
		if close < 0 {
			return nil
		}
		content := pattern[open+1 : close]
		propsOpen := strings.Index(content, "{")
		propsClose := strings.LastIndex(content, "}")
		if propsOpen >= 0 && propsClose > propsOpen {
			if err := e.validateCreatePropertyExpressions(scope, content[propsOpen+1:propsClose]); err != nil {
				return err
			}
		}
		offset = close + 1
	}
	return nil
}

func (e *StorageExecutor) validateCreatePropertyExpressions(scope *semanticBindingScope, properties string) error {
	for _, pair := range e.splitPropertyPairs(properties) {
		separator := findTopLevelMapKeyValueSeparator(pair)
		if separator <= 0 {
			continue
		}
		expression := strings.TrimSpace(pair[separator+1:])
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

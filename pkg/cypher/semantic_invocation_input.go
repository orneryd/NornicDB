package cypher

import "strings"

// validateInvocationParameterTypes applies invocation types through the SAME
// semantic walk used for syntax-only admission. It never publishes a verdict
// or a typed frame into the text-only syntax/plan caches.
func (e *StorageExecutor) validateInvocationParameterTypes(source string, params map[string]interface{}, cypher25 bool, inputs ...*semanticStatementInput) error {
	if len(params) == 0 || !strings.Contains(source, "$") || (!mayNeedStaticTypeCheck(source) && !strings.Contains(source, "(")) {
		return nil
	}
	input := matchingSemanticInput(source, inputs...)
	invocation := *input
	invocation.parameters = params
	return e.validateMatchSemanticScopesUncachedInput(source, cypher25, &invocation)
}

// propertyStatementAdmission is the compatibility entrance for isolated
// property-reader callers. Scope transitions still belong to semanticScopeWalk.
func propertyStatementAdmission(source string, outer map[string]string, params map[string]interface{}, inputs ...*semanticStatementInput) error {
	input := matchingSemanticInput(source, inputs...)
	invocation := *input
	invocation.parameters = params
	invocation.outerTypes = outer
	return (&StorageExecutor{}).validateMatchSemanticScopesUncachedInput(source, false, &invocation)
}

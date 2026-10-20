package cypher

// staticOperatorChecker has no independent parameter source. Every typed
// property, operator and function read observes this admitted frame only.
type staticOperatorChecker struct {
	scope staticTypeScope
}

func (scope staticTypeScope) operatorChecker() staticOperatorChecker {
	return staticOperatorChecker{scope: scope}
}

// withTypes is a local binding transition of the same admitted frame. It
// replaces entity/value/member facts while preserving invocation parameters,
// native provenance, version, completeness and the no-demand decision.
func (scope staticTypeScope) withTypes(types typeScope) staticTypeScope {
	local := types.static(scope.cypher25)
	local.params = scope.params
	local.complete = scope.complete
	local.native = scope.native
	local.noLocalScopes = scope.noLocalScopes
	return local
}

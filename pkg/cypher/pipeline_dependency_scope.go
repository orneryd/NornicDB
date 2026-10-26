package cypher

import (
	"context"
	"strings"
)

// Dependency consumers retain the whole query composition. UNION branches and
// nested bodies are resolved only by the shared semantic scope walk.
func pipelineDependencyClauses(body string) []pipelineClause {
	return []pipelineClause{{kind: -1, text: body}}
}

func pipelineReadsBinding(ctx context.Context, clauses []pipelineClause, name string) bool {
	if name == "" || strings.HasPrefix(name, "$") {
		return false
	}
	return !typeScope{name: staticOperand{}}.pipelineIndependent(ctx, clauses)
}

func (imports typeScope) pipelineIndependent(ctx context.Context, clauses []pipelineClause) bool {
	if len(imports) == 0 {
		return true
	}
	var buffer [8]string
	names := buffer[:0]
	for name := range imports {
		names = append(names, name)
	}
	return !semanticRuntimeDependencies(ctx, clauses, names, false)
}

func (imports typeScope) pipelineDependencies(ctx context.Context, clauses []pipelineClause) (*semanticDependencyScope, bool) {
	return imports.pipelineDependenciesIn(ctx, clauses, false)
}

func (imports typeScope) pipelineDependenciesIn(ctx context.Context, clauses []pipelineClause, cypher25 bool, parent ...*semanticBodyScope) (*semanticDependencyScope, bool) {
	if len(clauses) == 0 {
		return nil, false
	}
	texts := make([]string, len(clauses))
	for index, clause := range clauses {
		texts[index] = clause.text
	}
	proof := newSemanticDependencies(imports)
	frame := subqueryScope{dependencies: proof}
	if len(parent) != 0 && parent[0] != nil {
		frame.bindings = parent[0].clone(imports)
		frame.bindings.bare = false // a continuation, not a fresh CALL header
		frame.outer = parent[0].scalar
	}
	model, err := (&StorageExecutor{}).walkQueryScope(ctx, strings.Join(texts, " "), imports, frame, cypher25)
	return proof, err == nil && model.complete && proof.complete
}

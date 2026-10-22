package cypher

import "strings"

// Dependency consumers retain the whole query composition. UNION branches and
// nested bodies are resolved only by the shared semantic scope walk.
func pipelineDependencyClauses(body string) []pipelineClause {
	return []pipelineClause{{kind: -1, text: body}}
}

func pipelineReadsBinding(clauses []pipelineClause, name string) bool {
	if name == "" || strings.HasPrefix(name, "$") {
		return false
	}
	return !typeScope{name: staticOperand{}}.pipelineIndependent(clauses)
}

func (imports typeScope) pipelineIndependent(clauses []pipelineClause) bool {
	if len(imports) == 0 {
		return true
	}
	var buffer [8]string
	names := buffer[:0]
	for name := range imports {
		names = append(names, name)
	}
	return !semanticRuntimeDependencies(clauses, names, false)
}

func (imports typeScope) pipelineDependencies(clauses []pipelineClause) (*semanticDependencyScope, bool) {
	return imports.pipelineDependenciesIn(clauses, false)
}

func (imports typeScope) pipelineDependenciesIn(clauses []pipelineClause, cypher25 bool) (*semanticDependencyScope, bool) {
	if len(clauses) == 0 {
		return nil, false
	}
	texts := make([]string, len(clauses))
	for index, clause := range clauses {
		texts[index] = clause.text
	}
	proof := newSemanticDependencies(imports)
	model, err := (&StorageExecutor{}).walkQueryScope(strings.Join(texts, " "), imports, subqueryScope{dependencies: proof}, cypher25)
	return proof, err == nil && model.complete && proof.complete
}

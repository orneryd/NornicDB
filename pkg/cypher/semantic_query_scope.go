package cypher

import (
	"context"
	"maps"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// semanticDependencyScope records reads in the canonical constructor. active names
// still denote the input binding; a declaration or a dropping WITH removes
// that provenance. Nested walks share reads, never their mutable local scope.
type semanticDependencyScope struct {
	identities map[string]statementBindingID
	active     map[string]bool
	reads      map[string]bool
	parent     *semanticDependencyScope
	complete   bool
}

func newSemanticDependencies(input typeScope) *semanticDependencyScope {
	proof := &semanticDependencyScope{active: make(map[string]bool, len(input)), reads: make(map[string]bool), complete: true}
	for name := range input {
		proof.active[name] = true
	}
	return proof
}

func (proof *semanticDependencyScope) nested() *semanticDependencyScope {
	if proof == nil {
		return nil
	}
	return &semanticDependencyScope{identities: proof.identities, active: maps.Clone(proof.active), reads: make(map[string]bool), parent: proof, complete: proof.complete}
}

func (proof *semanticDependencyScope) read(name string) {
	if proof == nil {
		return
	}
	proof.readIdentity(name, proof.identities[name])
}

func (proof *semanticDependencyScope) readIdentity(name string, identity statementBindingID) {
	if proof == nil || !proof.active[name] {
		return
	}
	if proof.identities != nil && proof.identities[name] != identity {
		return
	}
	proof.reads[name] = true
	proof.parent.readIdentity(name, identity)
}

func (proof *semanticDependencyScope) expression(ctx context.Context, expression string) {
	if proof == nil {
		return
	}
	for _, name := range expressionFreeVariables(ctx, expression) {
		proof.read(name)
	}
}

func (proof *semanticDependencyScope) incomplete() {
	if proof != nil {
		proof.complete = false
		proof.parent.incomplete()
	}
}

// semanticQueryScope is a read-only typed view of an admitted body. Physical consumers
// use imports and projection clauses from this result; rows do not establish
// which names exist. A partial walk never proves an input can be omitted.
type semanticQueryScope struct {
	query    string
	clauses  []pipelineClause
	branches []*semanticQueryScope
	columns  []string
	returned typeScope
	imports  typeScope
	bindings *semanticBodyScope
	complete bool
	proof    *semanticDependencyScope

	// Runtime instantiation uses only these local names, never their types
	// or literal values. stableNames excludes value-named projections from
	// literal-normalized metadata reuse.
	stableNames bool
}

func semanticRuntimeInput(names []string) typeScope {
	input := make(typeScope, len(names))
	for _, name := range names {
		input[name] = staticOperand{}
	}
	return input
}

func scopeVisibleTypes(local, enclosing typeScope) typeScope {
	if len(enclosing) == 0 {
		return local
	}
	visible := maps.Clone(enclosing)
	maps.Copy(visible, local)
	return visible
}

type subqueryScope struct{ bindings *semanticBodyScope }

// walkQueryScope is a compatibility view of the admitted program's type
// enrichment. It schedules no branches, declarations, projections or exports.
func (e *StorageExecutor) walkQueryScope(ctx context.Context, query string, initial typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	_, model, err := e.enrichBoundTypes(ctx, query, initial, enclosing.bindings, "", cypher25)
	return model, err
}
func (e *StorageExecutor) scalarQueryScope(ctx context.Context, kind, source string, initial typeScope, _ *semanticDependencyScope, parent *semanticBodyScope, cypher25 bool) (*semanticQueryScope, error) {
	if strings.HasPrefix(strings.TrimSpace(source), "(") {
		source = "MATCH " + source
	}
	_, model, err := e.enrichBoundTypes(ctx, source, initial, parent, kind, cypher25)
	return model, err
}

// callWithImports resolves legacy WITH's ordinary locals. Unlike CALL(*), its
// star does not include a CALL frame's global imports. Explicit expressions may
// read named ancestors. A constant-only WITH is the existing implicit-import
// extension, rather than an importing clause.
func callWithImports(ctx context.Context, plan *returnProjectionPlan, local, visible typeScope) (typeScope, bool) {
	imports := make(typeScope)
	importing := local.forEachProjectedBinding(plan, func(name, expression, text string, inherited bool) {
		if inherited {
			imports[name] = local[name]
			return
		}
		for _, source := range expressionFreeVariables(ctx, expression) {
			if operand, exists := visible[source]; exists {
				imports[source] = operand
			}
		}
	})
	return imports, importing || len(imports) != 0
}

// callQueryScope selects a frozen CALL body and enriches its existing IDs.
func (e *StorageExecutor) callQueryScope(ctx context.Context, source string, initial typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	values, _, err := e.enrichBoundTypes(ctx, source, initial, enclosing.bindings, "", cypher25)
	if err != nil {
		return nil, err
	}
	if values == nil || values.program == nil {
		return nil, nil
	}
	for _, branch := range values.program.root.branches {
		for _, source := range branch.shape.clauses {
			clause := values.program.clauses[source.syntax.ID()]
			if clause.step.Call != nil {
				return values.model(clause.body), nil
			}
		}
	}
	return nil, nil
}

// readCallScopeClause is the import-header reader for validation, semantic
// scope construction and execution. nil names denotes (*); an empty non-nil
// list denotes (). Unscoped CALL has scoped == false.
func readCallScopeClause(ctx context.Context, call string) (names []string, scoped bool, err error) {
	parsed := readCallSubqueryScope(ctx, call)
	if parsed.invalid {
		return nil, parsed.scoped, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(parsed.text))
	}
	return parsed.names, parsed.scoped, nil
}

package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// projectedScopeFacts is an immutable execution projection program's result.
// Neither incomplete scope nor incomplete output has an accessible names payload.
type projectedScopeFacts struct {
	after      resultmetadata.Names
	outputs    resultmetadata.Names
	err        error
	generation uint64
	statement  *resultmetadata.Statement
	call       *resultmetadata.Call
	reason     resultmetadata.UnresolvedReason
	position   int
	complete   bool
	facts      resultmetadata.Facts
	bound      *boundStatementProgram
}

type projectionFactsKey struct {
	query, incoming, retained              string
	incomingKnown, retainedKnown, cypher25 bool
}

var projectionFactsCache = newBoundedCache[projectionFactsKey, *projectedScopeFacts](4096, func(key projectionFactsKey) int {
	return boundedCacheStringShardIndex(key.query)
})

// ProjectedScopeAfterClause owns which variables are visible after a clause.
// Visit requires a policy for both known names (fresh, sorted) and unknown facts.
// USE selects a graph without binding a variable. CALL bodies contribute only
// their projected outputs; registry outputs include implicit CALL and YIELD *.
// This read-only API consumes the bound projection program without starting a
// semantic/type walk or depending on header consumers.
func (e *StorageExecutor) ProjectedScopeAfterClause(clause string, incoming []string, cypher25 bool) resultmetadata.Names {
	names := resultmetadata.Unknown()
	e.StatementFacts(clause, resultmetadata.KnownScope(incoming), cypher25).Visit(func(statement resultmetadata.Statement) {
		statement.After.Visit(func(known []string) { names = resultmetadata.Known(known) }, func() { names = resultmetadata.Unknown() })
	}, func() { names = resultmetadata.Unknown() }, func(resultmetadata.Unresolved) { names = resultmetadata.Unknown() })
	return names
}

func namesKey(names resultmetadata.Names) (text string, known bool) {
	names.Visit(func(values []string) {
		slices.Sort(values)
		text, known = strings.Join(values, "\x00"), true
	}, func() { text, known = "", false })
	return
}

// projectionFactsFor is the statement cache for execution's declared fields.
// Compilation reads the same clause, pattern-binding and projection plans as
// execution. It never invokes the semantic/type-validation walk or a handler.
func projectionKey(query string, incoming, retained resultmetadata.Names, cypher25 bool) projectionFactsKey {
	inputKey, inputKnown := namesKey(incoming)
	retainedKey, retainedKnown := namesKey(retained)
	return projectionFactsKey{query: query, incoming: inputKey, retained: retainedKey, incomingKnown: inputKnown, retainedKnown: retainedKnown, cypher25: cypher25}
}
func (e *StorageExecutor) projectionFactsFor(query string, incoming, retained resultmetadata.Names, cypher25 bool) *projectedScopeFacts {
	key := projectionKey(query, incoming, retained, cypher25)
	generation := globalProcedureRegistry.generation.Load()
	if facts, found := projectionFactsCache.get(key); found && facts.generation == generation {
		return facts
	}
	facts := e.compileStatementProgram(query, incoming, retained, cypher25)
	immutable := *facts
	immutable.generation = generation
	if immutable.statement != nil && immutable.err == nil {
		known, err := resultmetadata.KnownFacts(*immutable.statement)
		if err == nil {
			immutable.complete, immutable.facts = true, known
		} else {
			immutable.reason = resultmetadata.ReasonDynamicSchema
		}
	}
	facts = &immutable
	if generation == globalProcedureRegistry.generation.Load() {
		projectionFactsCache.put(key, facts)
	}
	return facts
}

func (e *StorageExecutor) projectedStatementScope(query string, incoming, retained resultmetadata.Names, cypher25 bool) *projectedScopeFacts {
	return e.projectionFactsFor(query, incoming, retained, cypher25)
}

func projectedNameMap(names resultmetadata.Names) map[string]struct{} {
	var result map[string]struct{}
	names.Visit(func(values []string) {
		result = make(map[string]struct{}, len(values))
		for _, name := range values {
			result[name] = struct{}{}
		}
	}, func() { result = nil })
	return result
}

func projectedMapNames(scope map[string]struct{}) resultmetadata.Names {
	if scope == nil {
		return resultmetadata.Unknown()
	}
	names := make([]string, 0, len(scope))
	for name := range scope {
		names = append(names, name)
	}
	slices.Sort(names)
	return resultmetadata.Known(names)
}

func addProjectedNames(incoming, added resultmetadata.Names) resultmetadata.Names {
	first, second := projectedNameMap(incoming), projectedNameMap(added)
	if first == nil || second == nil {
		return resultmetadata.Unknown()
	}
	for name := range second {
		first[name] = struct{}{}
	}
	return projectedMapNames(first)
}

func mergeProjectedNames(incoming, retained resultmetadata.Names) resultmetadata.Names {
	return addProjectedNames(incoming, retained)
}

func projectedProcedureNames(source string) resultmetadata.Names {
	clause, ok := canonicalQueryClause(source)
	if !ok {
		return resultmetadata.Unknown()
	}
	return projectedProcedureNodeNames(clause)
}

func projectedProcedureNodeNames(clause cypherantlr.SyntaxNode) resultmetadata.Names {
	if yield := yieldFromSyntax(clause); yield != nil && !yield.yieldAll {
		columns := make([]string, 0, len(yield.items))
		for _, item := range yield.items {
			name := item.name
			if item.alias != "" {
				name = item.alias
			}
			columns = append(columns, name)
		}
		return resultmetadata.Known(columns)
	}
	name := clause.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
	procedure, found := globalProcedureRegistry.Get(name)
	if !found {
		return resultmetadata.Unknown()
	}
	var columns []string
	for _, output := range procedureDeclaredReturns(procedure.Spec) {
		columns = append(columns, output.Name)
	}
	return resultmetadata.Known(columns)
}

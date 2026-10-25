package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNestedScopeVariableUses(t *testing.T) {
	for _, sample := range []struct {
		body string
		want syntaxVariableUse
	}{
		{"CALL () { RETURN 2 AS value } WITH 1 AS keep RETURN value AS x", syntaxVariableInvalid},
		{"CALL { RETURN 2 AS value } WITH 1 AS keep RETURN value AS x", syntaxVariableInvalid},
		{"CALL () { WITH 2 AS value RETURN 1 AS kept } RETURN kept", syntaxVariableUnused},
		{"CALL (value) { RETURN 1 AS kept } RETURN kept", syntaxVariableRead},
		{"CALL { WITH value RETURN 1 AS kept } RETURN kept", syntaxVariableRead},
		{"CALL () { WITH 2 AS value RETURN * } RETURN value", syntaxVariableDefined},
		{"CALL (*) { WITH 2 AS value RETURN * } RETURN value", syntaxVariableRead},
		{"RETURN EXISTS { WITH 2 AS value RETURN value } AS x", syntaxVariableUnused},
		{"RETURN COUNT { WITH 2 AS value RETURN value } AS x", syntaxVariableUnused},
		{"RETURN COLLECT { WITH 2 AS value RETURN value } AS x", syntaxVariableUnused},
		{"RETURN EXISTS { MATCH (value) } AS x", syntaxVariableUnused},
		{"RETURN COUNT { (value)-->() } AS x", syntaxVariableUnused},
		{"RETURN [(value)-->() | value] AS x", syntaxVariableRead},
		{"RETURN CASE WHEN (value)-->() THEN 1 ELSE 0 END AS x", syntaxVariableRead},
		{"RETURN [value = ()-->() | length(value)] AS x", syntaxVariableUnused},
		{"RETURN [value IN [2] | value] AS x", syntaxVariableUnused},
		{"RETURN [value IN [value] | value] AS x", syntaxVariableRead},
		{"RETURN all(value IN [2] WHERE value=2) AS x", syntaxVariableUnused},
		{"RETURN any(item IN [value] WHERE item=2) AS x", syntaxVariableRead},
		{"RETURN reduce(value=0, item IN [2] | value+item) AS x", syntaxVariableUnused},
		{"RETURN reduce(value=value, item IN [2] | value+item) AS x", syntaxVariableRead},
		{"RETURN reduce(total=0, value IN [value] | total+value) AS x", syntaxVariableRead},
		{"FOREACH (value IN [2] | CREATE (:NestedScope {n:value})) RETURN 1 AS x", syntaxVariableUnused},
		{"FOREACH (item IN [2] | CREATE (value:NestedScope)) RETURN 1 AS x", syntaxVariableUnused},
		{"FOREACH (item IN [value] | CREATE (:NestedScope {n:item})) RETURN 1 AS x", syntaxVariableRead},
		{"FOREACH (item IN [1] | FOREACH (value IN [2] | CREATE (:NestedScope {n:value}))) RETURN 1 AS x", syntaxVariableUnused},
		{"RETURN [value IN [2] | EXISTS { RETURN value }] AS x", syntaxVariableUnused},
		{"RETURN EXISTS { CALL () { RETURN 2 AS value } RETURN value } AS x", syntaxVariableUnused},
		{"RETURN EXISTS { CALL () { MATCH (value) RETURN * } RETURN value } AS x", syntaxVariableUnused},
		{"RETURN EXISTS { CALL (value) { RETURN value AS kept } RETURN kept } AS x", syntaxVariableRead},
	} {
		t.Run(sample.body, func(t *testing.T) {
			body := readQueryBody(context.Background(), sample.body, 0, len(sample.body))
			require.Equal(t, queryBodyRead, body.status)
			require.Len(t, body.branches, 1)
			require.Equal(t, sample.want, branchSyntaxVariableUses(body.branches[0], map[string]struct{}{"value": {}})["value"])
		})
	}
}

// Valid private-scope cases must not transport an unused outer value even if
// their final columns happen to agree with an accidentally accepted program.
func TestNestedScopePrivateFacts(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested-scopes"))
	for _, sample := range statementfactstest.NestedScopePrivateCases() {
		if sample.Code != "" {
			continue
		}
		t.Run(sample.Name, func(t *testing.T) {
			executor.StatementFacts(sample.Query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
				for _, clause := range statement.Clauses {
					if clause.Call == nil {
						continue
					}
					if strings.Contains(sample.Name, "/call_all/") {
						require.Equal(t, []string{"value"}, clause.Call.Imports, "CALL(*) reads the declared current header before its body declares a shadow")
					} else {
						require.Empty(t, clause.Call.Imports)
					}
				}
			}, func() { t.Fatal("facts declined") }, func(reason resultmetadata.Unresolved) { t.Fatalf("facts unresolved: %v", reason.Err) })
		})
	}
}

func TestNestedScope25VariableUses(t *testing.T) {
	ctx := context.WithValue(context.Background(), cypherVersionKey{}, "25")
	for _, sample := range []struct {
		expression string
		want       syntaxVariableUse
	}{
		{"allReduce(value=0, item IN [2] | value+item, value>0)", syntaxVariableUnused},
		{"allReduce(value=value, item IN [2] | value+item, value>0)", syntaxVariableRead},
		{"allReduce(total=0, value IN [value] | total+value, total>0)", syntaxVariableRead},
	} {
		t.Run(sample.expression, func(t *testing.T) {
			query := "RETURN " + sample.expression + " AS x"
			body := readQueryBody(ctx, query, 0, len(query))
			require.Equal(t, queryBodyRead, body.status)
			require.Len(t, body.branches, 1)
			require.Equal(t, sample.want, branchSyntaxVariableUses(body.branches[0], map[string]struct{}{"value": {}})["value"])
		})
	}
}

func TestNestedScopeBoundaryVariants(t *testing.T) {
	for _, sample := range []struct {
		body string
		want syntaxVariableUse
	}{
		{"OPTIONAL CALL () { RETURN 2 AS value } WITH 1 AS keep RETURN value", syntaxVariableInvalid},
		{"WITH value CALL (*) { RETURN 1 AS kept } RETURN kept", syntaxVariableRead},
		{"WITH value CALL { WITH * RETURN 1 AS kept } RETURN kept", syntaxVariableRead},
		{"CALL { WITH * RETURN 2 AS value } RETURN value", syntaxVariableRead},
		{"CALL () { RETURN 2 AS value UNION ALL RETURN 3 AS value } RETURN value", syntaxVariableDefined},
		{"CALL { RETURN 1 AS kept UNION WITH value RETURN 2 AS kept } RETURN kept", syntaxVariableRead},
		{"CALL { RETURN 1 AS kept } IN TRANSACTIONS OF value ROWS RETURN kept", syntaxVariableRead},
		{"RETURN EXISTS { (value)-->() } AS x", syntaxVariableUnused},
		{"RETURN COUNT { (value)-->() } AS x", syntaxVariableUnused},
		{"RETURN [(n)-[value]->() | value] AS x", syntaxVariableRead},
		{"RETURN COLLECT { RETURN 2 AS value UNION RETURN 3 AS value } AS x", syntaxVariableUnused},
	} {
		t.Run(sample.body, func(t *testing.T) {
			body := readQueryBody(context.Background(), sample.body, 0, len(sample.body))
			require.Equal(t, queryBodyRead, body.status)
			require.Len(t, body.branches, 1)
			require.Equal(t, sample.want, branchSyntaxVariableUses(body.branches[0], map[string]struct{}{"value": {}})["value"])
		})
	}
}

func TestNestedScopeStarInputs(t *testing.T) {
	for _, sample := range []struct {
		body string
		want syntaxVariableUse
	}{
		{"CALL (*) { RETURN value AS kept } RETURN kept", syntaxVariableRead},
		{"CALL (*) { WITH value WITH 1 AS keep RETURN value AS kept } RETURN kept", syntaxVariableRead},
		{"CALL (*) { RETURN 2 AS value } WITH 1 AS keep RETURN value", syntaxVariableInvalid},
		{"CALL (*) { WITH 2 AS value RETURN 1 AS kept } RETURN kept", syntaxVariableRead},
		{"WITH 2 AS local CALL (*) { RETURN local AS value } WITH 1 AS keep RETURN value", syntaxVariableInvalid},
	} {
		t.Run(sample.body, func(t *testing.T) {
			body := readQueryBody(context.Background(), sample.body, 0, len(sample.body))
			require.Equal(t, queryBodyRead, body.status)
			require.Len(t, body.branches, 1)
			require.Equal(t, sample.want, branchSyntaxVariableUses(body.branches[0], map[string]struct{}{"value": {}})["value"])
		})
	}
}

// These compatibility assertions project admitted facts. Production has no
// first-use import classifier and never publishes a partial rejected program.
type syntaxVariableUse uint8

const (
	syntaxVariableUnused syntaxVariableUse = iota
	syntaxVariableRead
	syntaxVariableDefined
	syntaxVariableInvalid
)

func branchSyntaxVariableUses(branch queryBodyBranch, outer map[string]struct{}) map[string]syntaxVariableUse {
	source := branch.clauses[0].syntax.Source()[branch.start:branch.end]
	return canonicalTestBranchUses(context.Background(), source, outer)
}
func canonicalTestBranchUses(ctx context.Context, source string, outer map[string]struct{}) map[string]syntaxVariableUse {
	local := bindingTypes(outer)
	_, frame, _ := callBodyScope(nil, false, local, nil, false)
	program := (&StorageExecutor{}).compileStatementProgram(ctx, source, projectedMapNames(outer), resultmetadata.Known(nil), cypherVersionFromContext(ctx) == "25", frame)
	uses := make(map[string]syntaxVariableUse)
	if program.statement == nil {
		for name := range outer {
			uses[name] = syntaxVariableInvalid
		}
		return uses
	}
	weak := map[string]bool{}
	program.statement.Exit.Weak.Visit(func(names []string) {
		for _, name := range names {
			weak[name] = true
		}
	}, func() {})
	for name := range outer {
		if !weak[name] {
			uses[name] = syntaxVariableDefined
		}
	}
	program.statement.Incoming.Visit(func(names []string) {
		for _, name := range names {
			uses[name] = syntaxVariableRead
		}
	}, func() {})
	return uses
}
func branchDeclaresVariable(ctx context.Context, source, variable string) bool {
	return canonicalTestBranchUses(ctx, source, map[string]struct{}{variable: {}})[variable] == syntaxVariableDefined
}

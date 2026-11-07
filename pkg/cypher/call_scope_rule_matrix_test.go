package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Retained imports remain readable without becoming wildcard members. Binding
// identity, not a spelling guess, connects the body's read to its outer value.
func TestCallScopeUsesBindingValue(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	query := "WITH 1 AS a CALL (a) { WITH 5 AS v RETURN a+v AS r } RETURN a,r"
	program := exec.projectedStatementScope(ctx, query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	require.True(t, program.complete)
	require.NoError(t, program.err)
	require.NotNil(t, program.bound)
	call := program.statement.Clauses[1].Call
	require.NotNil(t, call)
	call.Retained.Visit(func(names []string) { require.Equal(t, []string{"a"}, names) }, func() { t.Fatal("retained inputs must be known") })
	call.Statement.Incoming.Visit(func(names []string) { require.Empty(t, names) }, func() { t.Fatal("current inputs must be known") })
	call.Statement.Clauses[0].After.Visit(func(names []string) { require.Equal(t, []string{"v"}, names) }, func() { t.Fatal("WITH membership must be known") })
	found := false
	for _, use := range program.bound.uses {
		require.NotZero(t, use.binding)
		if use.name == "a" && use.start >= call.Statement.Start && use.end <= call.Statement.End {
			definition := program.bound.bindings[use.binding-1]
			require.Equal(t, "a", definition.name)
			require.Less(t, definition.start, call.Statement.Start)
			found = true
		}
	}
	require.True(t, found, "the body must read the original imported identity")
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "r"}, result.Columns)
	require.Equal(t, [][]any{{int64(1), int64(6)}}, result.Rows)
}

func TestCallScopeRuleMatrix(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
		if uri == "" {
			t.Skip("requires the Neo4j scope reference")
		}
		ctx := context.Background()
		driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		defer driver.Close(ctx)
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
		defer session.Close(ctx)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-rules"))
		reference := func(query string) callScopeOracleOutcome {
			result, err := session.Run(ctx, query, nil)
			out := callScopeOracleOutcome{}
			if err == nil {
				out.Columns, err = result.Keys()
			}
			if err == nil {
				var records []*neo4j.Record
				records, err = result.Collect(ctx)
				for _, record := range records {
					out.Rows = append(out.Rows, record.Values)
				}
			}
			if err != nil {
				var status *neo4j.Neo4jError
				require.True(t, errors.As(err, &status), "reference transport failure: %v", err)
				out.Code, out.Message = status.Code, status.Msg
			}
			return out
		}
		setup := "CREATE (:CallScopeRuleTarget {n:1}),(:CallScopeRuleTarget {n:2})"
		require.Empty(t, reference(setup).Code)
		_, err = exec.Execute(ctx, setup, nil)
		require.NoError(t, err)
		defer reference("MATCH (n:CallScopeRuleTarget) DETACH DELETE n")
		mismatches := 0
		cases := callscopetest.RuleCases()
		for _, item := range cases {
			original := reference(item.Reference)
			want := original
			if item.Compatibility != "" {
				want = reference(item.Compatibility)
			}
			result, err := exec.Execute(ctx, item.Query, nil)
			got := callScopeOracleOutcome{}
			if err != nil {
				got.Code, got.Message = nornicerrors.Neo4jStatus(err)
			} else if result != nil {
				got.Columns, got.Rows = result.Columns, result.Rows
			}
			if !got.equal(want) {
				mismatches++
				record, _ := json.Marshal(map[string]any{"case": item.Name, "query": item.Query, "control": item.Compatibility, "extension": item.Extension, "original": original, "expected": want, "actual": got})
				fmt.Println("CALL_SCOPE_RULE_MISMATCH " + string(record))
			}
		}
		fmt.Printf("CALL_SCOPE_RULES cases=%d mismatches=%d\n", len(cases), mismatches)
		require.Zero(t, mismatches)
	})
}

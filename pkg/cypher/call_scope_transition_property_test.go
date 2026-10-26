package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func transitionNamesOf(scope typeScope) []string {
	names := make([]string, 0, len(scope))
	for name := range scope {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func transitionStatus(err error) string {
	if err == nil {
		return ""
	}
	var reference *neo4j.Neo4jError
	if errors.As(err, &reference) {
		return reference.Code
	}
	if !nornicerrors.HasNeo4jStatus(err) {
		return "UNCLASSIFIED: " + err.Error()
	}
	code, _ := nornicerrors.Neo4jStatus(err)
	if code == "" {
		return err.Error()
	}
	return code
}

// Compare ordered headers independently of row values. The negative control
// exercises this exact comparator so an omission/reordering cannot pass.
func transitionColumns(want, got []string) error {
	if !slices.Equal(want, got) {
		return fmt.Errorf("columns: want %v, got %v", want, got)
	}
	return nil
}

func TestCallScopeTransitionColumnNegativeControls(t *testing.T) {
	for _, got := range [][]string{{"a"}, {"x", "a"}} {
		require.Error(t, transitionColumns([]string{"a", "x"}, got))
	}
}

// The generator is independent of all production scope code. Every consumer
// must agree with it, including terminal errors when no row reaches CALL.
func TestCallScopeTransitionProperty(t *testing.T) {
	ctx := context.Background()
	streamed := 0
	for _, tc := range callscopetest.TransitionCases() {
		t.Run(tc.Name, func(t *testing.T) {
			fresh := func() *StorageExecutor {
				return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "transition-property"))
			}
			exec := fresh()
			check := func(consumer string, columns []string, err error) {
				t.Helper()
				require.Equal(t, tc.Code, transitionStatus(err), "%s: %s: %v", consumer, tc.Query, err)
				if tc.Code == "" {
					require.NoError(t, transitionColumns(tc.Columns, columns), consumer)
				}
			}
			model, err := exec.walkQueryScope(ctx, tc.Query, typeScope{}, subqueryScope{}, false)
			var typed []string
			if model != nil {
				typed = model.columns
			}
			check("typed", typed, err)
			program := exec.statementProjectionProgram(ctx, tc.Query, resultmetadata.KnownScope(nil), false, nil)
			var statement resultmetadata.Statement
			var factsErr error
			exec.StatementFacts(tc.Query, resultmetadata.KnownScope(nil), false).Visit(func(value resultmetadata.Statement) { statement = value }, func() { t.Fatal("facts declined") }, func(reason resultmetadata.Unresolved) { factsErr = reason.Err })
			var boundColumns []string
			statement.Columns.Visit(func(names []string) { boundColumns = names }, func() {})
			check("bound/facts", boundColumns, factsErr)
			columns, err := exec.StatementResultColumns(tc.Query)
			check("strict headers", columns, err)
			if tc.Code == "" && tc.Call != "" {
				local := typeScope{"a": staticOperand{}}
				if strings.Contains(tc.Query, "CREATE (n:") {
					local["n"] = operandOfKind(matchBindingNode)
				}
				body, err := exec.callQueryScope(ctx, tc.Call, local, subqueryScope{}, false)
				require.NoError(t, err)
				require.Equal(t, tc.BodyColumns, body.columns, "typed CALL columns")
				var call *resultmetadata.Call
				var callInput resultmetadata.Scope
				for _, clause := range statement.Clauses {
					if clause.Call != nil {
						call = clause.Call
						callInput = clause.Before
						break
					}
				}
				require.NotNil(t, call)
				callInput.Visit(func(names []string) { require.Equal(t, tc.Incoming, names, "caller Incoming") }, func() { t.Fatal("unknown caller Incoming") })
				call.Statement.Incoming.Visit(func(names []string) { require.Equal(t, tc.BodyIncoming, names, "body Incoming") }, func() { t.Fatal("unknown body Incoming") })
				call.Retained.Visit(func(names []string) { require.Equal(t, tc.Retained, names, "CALL Retained") }, func() { t.Fatal("unknown CALL Retained") })
				call.After.Visit(func(names []string) { require.Equal(t, tc.After, names, "CALL After") }, func() { t.Fatal("unknown CALL After") })
				require.Equal(t, tc.Retained, transitionNamesOf(body.branches[0].bindings.retained), "typed Retained")
				call.Body.Visit(func(names []string) { require.Equal(t, tc.BodyColumns, names, "bound CALL columns") }, func() { t.Fatal("unknown CALL columns") })
				branch := call.Statement
				if len(branch.Branches) != 0 {
					branch = &branch.Branches[0]
				}
				require.Len(t, branch.Clauses, len(tc.Steps))
				for i, want := range tc.Steps {
					typedScope := body.branches[0].clauses[i].bindings
					require.Equal(t, want.Current, transitionNamesOf(typedScope.entry), "typed step %d: %s", i, want.Text)
					require.Equal(t, want.Readable, transitionNamesOf(typedScope.visible(typedScope.entry)), "typed readable step %d", i)
					boundStep := branch.Clauses[i]
					boundStep.Before.Visit(func(names []string) { require.Equal(t, want.Readable, names, "bound readable step %d", i) }, func() { t.Fatal("unknown clause scope") })
					found := false
					for id, step := range program.bound.scopes {
						if step.Start == boundStep.Start && step.End == boundStep.End {
							frame := program.bound.bodies[id]
							require.NotNil(t, frame)
							require.Equal(t, want.Current, transitionNamesOf(frame.entry), "bound current step %d", i)
							found = true
						}
					}
					require.True(t, found, "missing bound transition %s", want.Text)
				}
			}
			for _, mode := range []string{"EXPLAIN", "PROFILE", "Execute", "stream"} {
				runner := fresh()
				query := tc.Query
				if mode == "EXPLAIN" || mode == "PROFILE" {
					query = mode + " " + query
				}
				var result *ExecuteResult
				if mode == "stream" {
					query = strings.TrimSuffix(query, " ORDER BY x")
					run := executeStreamed(ctx, runner, query, 1)
					rows, _ := run.stream.Next(-1)
					<-run.stream.Done()
					result, err = run.result, run.err
					if result != nil && run.stream.streamed() {
						streamed++
						result.Rows = rows
						result.Columns = run.stream.Columns()
					}
				} else {
					result, err = runner.Execute(ctx, query, nil)
				}
				columns = nil
				if result != nil {
					columns = result.Columns
				}
				check(mode, columns, err)
				if tc.Code == "" && mode != "EXPLAIN" {
					require.ElementsMatch(t, tc.Rows, result.Rows, mode)
					if tc.Name == "delete_retained" {
						remaining, err := runner.Execute(ctx, "MATCH (n:R73Delete) RETURN count(n) AS remaining", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(0)}}, remaining.Rows, mode)
					}
				}
			}
		})
	}
	require.Positive(t, streamed, "the generated population must actually cross the streaming threshold")
	t.Logf("TRANSITION_PROPERTY cases=%d streamed=%d", len(callscopetest.TransitionCases()), streamed)
}

func transitionRowsJSON(rows [][]interface{}) string {
	if len(rows) == 0 {
		return "[]"
	}
	data, err := json.Marshal(rows)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestCallScopeTransitionFabricProperty(t *testing.T) {
	ctx := context.Background()
	for _, tc := range callscopetest.TransitionCases() {
		t.Run(tc.Name, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			result, err := exec.Execute(ctx, "USE comp.one "+tc.Query, nil)
			require.Equal(t, tc.Code, transitionStatus(err), "%s: %v", tc.Query, err)
			if tc.Code == "" {
				require.NoError(t, transitionColumns(tc.Columns, result.Columns))
				require.JSONEq(t, transitionRowsJSON(tc.Rows), transitionRowsJSON(result.Rows))
				if tc.Name == "delete_retained" {
					remaining, err := exec.Execute(ctx, "USE comp.one MATCH (n:R73Delete) RETURN count(n) AS remaining", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(0)}}, remaining.Rows)
				}
			}
		})
	}
}

func TestCallScopeTransitionNeo4j(t *testing.T) {
	driver := fabricCallRowsReference(t)
	require.NotNil(t, driver, "submit with --ref neo-ref")
	checked := 0
	for _, tc := range callscopetest.TransitionCases() {
		if !tc.Neo4jKnown {
			continue
		}
		checked++
		t.Run(tc.Name, func(t *testing.T) {
			result, err := neo4j.ExecuteQuery(context.Background(), driver, tc.Query, nil, neo4j.EagerResultTransformer)
			require.Equal(t, tc.Code, transitionStatus(err), "%s: %v", tc.Query, err)
			if tc.Code == "" {
				require.NoError(t, transitionColumns(tc.Columns, result.Keys))
				var rows [][]interface{}
				for _, record := range result.Records {
					rows = append(rows, record.Values)
				}
				require.JSONEq(t, transitionRowsJSON(tc.Rows), transitionRowsJSON(rows))
			}
		})
	}
	t.Logf("TRANSITION_NEO4J cases=%d version=5.26.30", checked)
}

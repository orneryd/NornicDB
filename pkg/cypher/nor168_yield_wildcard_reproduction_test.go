package cypher

// NOR-168 reproduction against the pinned Neo4j 5.26.30 reference.
//
// The expected outcomes in this file are transcribed from the recorded
// reference run attached to NOR-168 / N152-54, not inferred
// (image neo4j:5.26.30-community, digest
// sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c),
// produced with the real custom Java procedure FrameProcedure.java
// (review.frame(n :: INTEGER) :: (x :: INTEGER), Mode.READ, x = n).
//
// The reference cases this file reproduces are:
//
//	02-exact           CALL review.frame($n) YIELD * WITH x RETURN x AS k
//	03-return          CALL review.frame($n) YIELD * RETURN x AS k
//	04-missing         CALL review.frame($n) YIELD * WITH missing RETURN missing
//	05-standalone      CALL review.frame($n) YIELD *
//	06-named           CALL review.frame($n) YIELD x RETURN x AS k
//	07-undefined       CALL review.frame($n) YIELD x WITH missing RETURN missing
//	08-seed            CREATE (:Nor168)
//	09-builtin-with    CALL db.labels() YIELD * WITH label RETURN label
//	10-builtin-return  CALL db.labels() YIELD * RETURN count(*) AS results
//	11-builtin-standalone CALL db.labels() YIELD *
//	12-builtin-named   CALL db.labels() YIELD label RETURN label
//
// Both parser modes are exercised, and both supported public routes: the direct
// executor for the statement's own database, and the composite (USE <composite
// database>) route. Every case goes through the public Execute entry point, so
// the assertions hold on the product path a client reaches rather than on a
// validation helper alone. Neo4j 5.26 Community has no composite databases, so
// the approved direct reference is the oracle the composite route is compared
// against.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// nor168WildcardHeadline is the diagnostic text the reference reports for the
// five rejected cases. The Neo4j message continues with the statement's source
// position and source echo ("(line 1, column 1 (offset: 0))" followed by the
// quoted statement and a caret), which describes where the rejected form is;
// the rule's own text is this headline, so the assertion below compares the
// headline and accepts only positional detail after it. Asserting this text,
// not just the SyntaxError category, is what distinguishes the wildcard
// restriction from the downstream binding diagnostic it must precede.
const nor168WildcardHeadline = "Cannot use `YIELD *` outside standalone call"

// nor168FrameParameters is the reference's parameter map for every case:
// $n = 7, so the custom procedure yields x = 7.
func nor168FrameParameters() map[string]interface{} {
	return map[string]interface{}{"n": int64(7)}
}

// registerNor168FrameProcedure publishes the reference procedure through the
// package's compiled registration entry point, with the real metadata the Java
// procedure declares (name, READ mode, one INTEGER parameter, one INTEGER
// result column x). The handler counts invocations so a rejected statement can
// be shown not to reach the procedure, and the registry is restored through the
// existing scoped registration fixture after the case.
func registerNor168FrameProcedure(t *testing.T, invocations *atomic.Int64) {
	t.Helper()
	registerProcedureForTest(t, ProcedureSpec{
		Name:        "review.frame",
		Signature:   "review.frame(n :: INTEGER) :: (x :: INTEGER)",
		Description: "NOR-168 reference procedure: returns the input as x",
		Mode:        ProcedureModeRead,
		Params:      []ProcedureParam{{Name: "n", Type: "INTEGER"}},
		Returns:     []ProcedureColumn{{Name: "x", Type: "INTEGER"}},
		MinArgs:     1,
		MaxArgs:     1,
	}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		invocations.Add(1)
		return &ExecuteResult{Columns: []string{"x"}, Rows: [][]interface{}{{args[0]}}}, nil
	})
}

// TestNor168YieldWildcardReferenceReproduction runs the recorded reference cases
// on both parsers and both supported routes.
func TestNor168YieldWildcardReferenceReproduction(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, route := range []string{"direct", "composite"} {
			t.Run(parser+"/"+route, func(t *testing.T) {
				previous := config.GetParserType()
				config.SetParserType(parser)
				t.Cleanup(func() { config.SetParserType(previous) })

				exec, prefix := nor168RouteExecutor(t, route)
				var invocations atomic.Int64
				registerNor168FrameProcedure(t, &invocations)

				// 08-seed: the reference database holds exactly one :Nor168 node
				// and no other label, which is what makes 11/12 return one row.
				seed := []string{"MATCH (p:P) DETACH DELETE p", "CREATE (:Nor168)"}
				for _, statement := range seed {
					_, err := exec.Execute(context.Background(), prefix+statement, nil)
					require.NoError(t, err, statement)
				}
				count, err := exec.Execute(context.Background(), prefix+"MATCH (n:Nor168) RETURN count(*) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows, "one :Nor168 node")

				// The reference rejects all five of these statements with the
				// YIELD * restriction, before the statement runs and before any
				// downstream clause reports what it cannot bind.
				for _, testCase := range []struct {
					name  string
					query string
					// countsInvocation is set for the custom procedure's cases:
					// the reference rejects the statement without running it.
					countsInvocation bool
				}{
					{"02-exact-wildcard-with", "CALL review.frame($n) YIELD * WITH x RETURN x AS k", true},
					{"03-wildcard-return", "CALL review.frame($n) YIELD * RETURN x AS k", true},
					{"04-wildcard-missing-downstream", "CALL review.frame($n) YIELD * WITH missing RETURN missing", true},
					{"09-builtin-wildcard-with", "CALL db.labels() YIELD * WITH label RETURN label", false},
					{"10-builtin-wildcard-return", "CALL db.labels() YIELD * RETURN count(*) AS results", false},
				} {
					t.Run(testCase.name, func(t *testing.T) {
						before := invocations.Load()
						result, err := exec.Execute(context.Background(), prefix+testCase.query, nor168FrameParameters())
						require.Error(t, err, testCase.query)
						require.Nil(t, result, testCase.query)

						code, message := nornicerrors.Neo4jStatus(err)
						t.Logf("actual: code=%q message=%q invocations=%d error=%q",
							code, message, invocations.Load()-before, err.Error())
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, testCase.query)
						if testCase.countsInvocation {
							require.Equal(t, int64(0), invocations.Load()-before,
								"a statement the contract already rejects never invokes the procedure")
						}
						require.NotContains(t, message, "not defined",
							"a downstream binding diagnostic must not obscure the wildcard restriction: %q", message)
						if route == "composite" {
							// The composite route delivers a constituent's failure
							// through the fabric's routing envelope
							// ("local execution on '<alias>' failed: ..."), so the
							// approved diagnostic is required to be the one that
							// envelope carries rather than the message's first bytes.
							require.Contains(t, message, nor168WildcardHeadline,
								"the YIELD * restriction owns this diagnostic, got %q", message)
							return
						}
						require.True(t, hasNor168WildcardHeadline(message),
							"the YIELD * restriction owns this diagnostic, got %q", message)
					})
				}

				// The reference's accepted cases: one row, the procedure's own
				// column, and the parameter's value.
				for _, testCase := range []struct {
					name    string
					query   string
					columns []string
					rows    [][]interface{}
				}{
					{
						name:    "05-standalone-wildcard",
						query:   "CALL review.frame($n) YIELD *",
						columns: []string{"x"},
						rows:    [][]interface{}{{int64(7)}},
					},
					{
						name:    "06-named-yield",
						query:   "CALL review.frame($n) YIELD x RETURN x AS k",
						columns: []string{"k"},
						rows:    [][]interface{}{{int64(7)}},
					},
					{
						name:    "11-builtin-standalone-wildcard",
						query:   "CALL db.labels() YIELD *",
						columns: []string{"label"},
						rows:    [][]interface{}{{"Nor168"}},
					},
					{
						name:    "12-builtin-named-yield",
						query:   "CALL db.labels() YIELD label RETURN label",
						columns: []string{"label"},
						rows:    [][]interface{}{{"Nor168"}},
					},
				} {
					t.Run(testCase.name, func(t *testing.T) {
						result, err := exec.Execute(context.Background(), prefix+testCase.query, nor168FrameParameters())
						require.NoError(t, err, testCase.query)
						t.Logf("actual: columns=%v rows=%v", result.Columns, result.Rows)
						require.Equal(t, testCase.columns, result.Columns, testCase.query)
						require.Equal(t, testCase.rows, result.Rows, testCase.query)
					})
				}

				// 07-undefined: a valid named-yield statement keeps its own
				// undefined-variable diagnostic.
				t.Run("07-genuine-undefined-variable", func(t *testing.T) {
					_, err := exec.Execute(context.Background(),
						prefix+"CALL review.frame($n) YIELD x WITH missing RETURN missing", nor168FrameParameters())
					require.Error(t, err)
					code, message := nornicerrors.Neo4jStatus(err)
					t.Logf("actual: code=%q message=%q error=%q", code, message, err.Error())
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.Contains(t, message, "Variable `missing` not defined")
					require.False(t, hasNor168WildcardHeadline(message),
						"a named yield is valid, so the wildcard restriction cannot own this diagnostic: %q", message)
				})
			})
		}
	}
}

// hasNor168WildcardHeadline reports whether message starts with the reference's
// wildcard restriction text, allowing the source position Neo4j appends to it.
func hasNor168WildcardHeadline(message string) bool {
	return strings.HasPrefix(message, nor168WildcardHeadline)
}

// nor168RouteExecutor returns the executor that serves the statement and the
// USE prefix that selects the route. The composite route reuses the existing
// multi-database fixture (composite database "comp" over local databases "one"
// and "two") and reaches the constituent through the composite database.
func nor168RouteExecutor(t *testing.T, route string) (*StorageExecutor, string) {
	t.Helper()
	exec := newFabricCallRowsExecutor(t)
	if route == "direct" {
		return exec, ""
	}
	return fabricCallRowsCompositeExecutor(t, exec), "USE comp.one "
}

// Composition includes clauses before CALL, not just a following projection.
// These admission controls exercise the existing composition and named-binding
// contracts independently of the recorded Neo4j reference matrix above.
func TestProcedureYieldAdmissionComposition(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, route := range []string{"direct", "composite"} {
			t.Run(parser+"/"+route, func(t *testing.T) {
				previous := config.GetParserType()
				config.SetParserType(parser)
				t.Cleanup(func() { config.SetParserType(previous) })
				exec, prefix := nor168RouteExecutor(t, route)
				var invocations atomic.Int64
				registerNor168FrameProcedure(t, &invocations)
				for _, query := range []string{
					"WITH 1 AS before CALL review.frame($n) YIELD *",
					"WITH 1 AS before CALL review.frame($n) YIELD * RETURN missing",
					"CALL { CALL review.frame($n) YIELD * WITH missing RETURN missing } RETURN missing",
					"RETURN 1 AS missing UNION CALL review.frame($n) YIELD * WITH missing RETURN missing",
					"CALL review.frame($n) YIELD * UNION RETURN missing AS x",
					"CALL review.frame($n) YIELD * UNION CALL review.frame($n) YIELD *",
				} {
					result, err := exec.Execute(context.Background(), prefix+query, nor168FrameParameters())
					require.ErrorContains(t, err, nor168WildcardHeadline)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.Nil(t, result)
				}
				result, err := exec.Execute(context.Background(), prefix+"CALL review.frame($n) YIELD x AS value, x AS value RETURN value", nor168FrameParameters())
				require.ErrorContains(t, err, "already bound")
				require.Nil(t, result)
				require.Zero(t, invocations.Load())
				result, err = exec.Execute(context.Background(), prefix+"WITH 1 AS before CALL review.frame($n) YIELD x RETURN before, x", nor168FrameParameters())
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1), int64(7)}}, result.Rows)
				require.Equal(t, int64(1), invocations.Load())
			})
		}
	}
}

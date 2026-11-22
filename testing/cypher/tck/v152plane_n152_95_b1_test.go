package tck

// NOR-66 / NOR-167 transport matrix.
//
// The release corpus reports transaction modes at the Bolt boundary: an
// autocommit RUN and a driver explicit transaction. This focused test replays
// the malformed map forms and the reported escaped-map-key expression through
// that same BoltBackend on both parsers, and proves the malformed statements
// are classified as compile-time SyntaxError/UnexpectedSyntax without
// publishing the write they stage.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nor66NOR167MalformedMapQueries are the recorded malformed map shapes plus
// statements that stage a write before the invalid map.
var nor66NOR167MalformedMapQueries = []string{
	"RETURN {key: 1,} AS m",
	"RETURN {, key: 1} AS m",
	"RETURN {key: 1,, other: 2} AS m",
	"RETURN {,} AS m",
	"RETURN {key:} AS m",
	"RETURN {key 1} AS m",
	"RETURN {outer: {inner: 1,}} AS m",
	"WITH {a: 1} AS n RETURN n{.a,} AS m",
	"CREATE (:Nor66BoltStage {id: 1}) RETURN {key: 1,} AS m",
	"MERGE (n:Nor66BoltStage {id: 2}) SET n += {key: 1,}",
}

func TestNOR66NOR167MapLiteralBoltMatrix(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			driver, shutdown := startConformanceServer(t)
			defer shutdown()

			for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := t.Context()
					backend := newDifferentialBackend(t, driver, "nornic", mode)
					require.NoError(t, backend.Reset(ctx))

					for _, query := range nor66NOR167MalformedMapQueries {
						_, err := backend.Execute(ctx, query, nil)
						var classified *QueryError
						require.ErrorAs(t, err, &classified, query)
						require.Equal(t, "SyntaxError", classified.Type, "%s: %v", query, err)
						require.Equal(t, "compile time", classified.Phase, query)
						require.Equal(t, "UnexpectedSyntax", classified.Detail, query)
					}
					// A staged write in a rejected statement is not published.
					snapshot, err := backend.Snapshot(ctx)
					require.NoError(t, err)
					require.Empty(t, snapshot.Nodes, "malformed map statements must not publish nodes")
					require.Empty(t, snapshot.Relationships, "malformed map statements must not publish relationships")

					// NOR-167: the reported map expression and an identifier
					// alias of the same written name decode to ab.
					result, err := backend.Execute(ctx, "RETURN {`a\\u0062`: 1} AS m, 1 AS `a\\u0062`", nil)
					require.NoError(t, err)
					assert.Equal(t, []string{"m", "ab"}, result.Columns)
					require.Equal(t, [][]any{{map[string]any{"ab": int64(1)}, int64(1)}}, result.Rows)

					// Healthy controls, including the write path, still run
					// after the rejected statements.
					result, err = backend.Execute(ctx, "WITH {a: 1, b: 2} AS n RETURN n{.a, .b} AS m", nil)
					require.NoError(t, err)
					require.Equal(t, [][]any{{map[string]any{"a": int64(1), "b": int64(2)}}}, result.Rows)

					result, err = backend.Execute(ctx, "CREATE (n:Nor66BoltHealthy) SET n = {`a\\u0062`: 1} RETURN properties(n) AS p", nil)
					require.NoError(t, err)
					require.Equal(t, [][]any{{map[string]any{"ab": int64(1)}}}, result.Rows)

					// NOR-167: variable headers must decode identically through Bolt.
					for _, tc := range []struct {
						query string
						rows  [][]any
					}{
						{"UNWIND ['value'] AS `ab` RETURN `ab`", [][]any{{"value"}}},
						{"UNWIND [] AS `ab` RETURN `ab`", nil},
						{"RETURN 1 AS `ab`", [][]any{{int64(1)}}},
					} {
						result, err := backend.Execute(ctx, tc.query, nil)
						require.NoError(t, err, tc.query)
						assert.Equal(t, []string{"ab"}, result.Columns, tc.query)
						if tc.rows == nil {
						assert.Empty(t, result.Rows, tc.query)
					} else {
						assert.Equal(t, tc.rows, result.Rows, tc.query)
					}
						t.Logf("statement=%q columns=%v rows=%v", tc.query, result.Columns, result.Rows)
					}
				})
			}
		})
	}
}

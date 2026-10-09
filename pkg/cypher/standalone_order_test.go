package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ORDER BY, SKIP / OFFSET and LIMIT as clauses of their own, and OFFSET as
// SKIP, answer as Neo4j 5.26.30 and 2026.09 do (#907).
func TestStandaloneOrderClausesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "standalone_order"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SQ {v: 3}), (:SQ {v: 1}), (:SQ {v: 2})", nil)
	require.NoError(t, err)
	inRolledBackTransaction := func(query string) ([][]interface{}, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			return nil, err
		}
		return result.Rows, nil
	}
	ints := func(values ...int64) [][]interface{} {
		rows := make([][]interface{}, len(values))
		for i, v := range values {
			rows[i] = []interface{}{v}
		}
		return rows
	}
	for query, rows := range map[string][][]interface{}{
		"MATCH (n:SQ) ORDER BY n.v RETURN n.v AS v":                                  ints(1, 2, 3),
		"MATCH (n:SQ) ORDER BY n.v DESC LIMIT 2 RETURN n.v AS v":                     ints(3, 2),
		"MATCH (n:SQ) LIMIT 2 RETURN count(*) AS c":                                  ints(2),
		"MATCH (n:SQ) ORDER BY n.v SKIP 1 RETURN n.v AS v":                           ints(2, 3),
		"MATCH (n:SQ) ORDER BY n.v OFFSET 1 LIMIT 1 RETURN n.v AS v":                 ints(2),
		"MATCH (n:SQ) SKIP 1 LIMIT 1 RETURN count(*) AS c":                           ints(1),
		"MATCH (n:SQ) WHERE n.v > 1 ORDER BY n.v RETURN n.v AS v":                    ints(2, 3),
		"MATCH (n:SQ) ORDER BY n.v SET n.seen = true RETURN n.v AS v":                ints(1, 2, 3),
		"MATCH (n:SQ) ORDER BY n.v ORDER BY n.v DESC RETURN n.v AS v":                ints(3, 2, 1),
		"ORDER BY 1 RETURN 1 AS v":                                                   ints(1),
		"CYPHER 25 MATCH (n:SQ) LET w = n.v * 10 ORDER BY w DESC RETURN w":           ints(30, 20, 10),
		"CYPHER 25 FOR x IN [3, 1, 2] ORDER BY x RETURN collect(x) AS l":             {{[]interface{}{int64(1), int64(2), int64(3)}}},
		"CYPHER 25 FOR x IN [3, 1, 2] LIMIT 1 FOR y IN [x, x] RETURN y":              ints(3, 3),
		"MATCH (n:SQ) RETURN n.v AS v ORDER BY v OFFSET 1":                           ints(2, 3),
		"MATCH (n:SQ) WITH n ORDER BY n.v LIMIT 1 ORDER BY n.v DESC RETURN n.v AS v": ints(1),
		"CALL { MATCH (n:SQ) ORDER BY n.v LIMIT 1 RETURN n.v AS v } RETURN v":        ints(1),
		"RETURN COUNT { MATCH (n:SQ) LIMIT 2 RETURN n } AS c":                        ints(2),
		"MATCH (n:%) WHERE n.v > 1 RETURN n.v AS v ORDER BY v":                       ints(2, 3),
		// A variable named skip, limit or offset stays a variable.
		"WITH 2 AS limit MATCH (n:SQ) WHERE n.v >= limit RETURN count(*) AS c": ints(2),
		"WITH 1 AS skip, 2 AS offset RETURN skip + offset AS v":                ints(3),
	} {
		got, err := inRolledBackTransaction(query)
		require.NoError(t, err, query)
		require.Equal(t, rows, got, query)
	}
	for _, query := range []string{
		"MATCH (n:SQ) ORDER BY m.v RETURN n.v AS v",
		"MATCH (n:SQ) ORDER BY count(*) RETURN 1 AS v",
		"MATCH (n:SQ) LIMIT -1 RETURN 1 AS v",
	} {
		_, err := inRolledBackTransaction(query)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}

func TestDesugarStandaloneOrderClauses(t *testing.T) {
	for query, want := range map[string]string{
		"MATCH (n) RETURN n":                                         "MATCH (n) RETURN n",
		"MATCH (n) RETURN n ORDER BY n.v SKIP 1 LIMIT 2":             "MATCH (n) RETURN n ORDER BY n.v SKIP 1 LIMIT 2",
		"MATCH (n) ORDER BY n.v RETURN n":                            "MATCH (n) WITH * ORDER BY n.v RETURN n",
		"MATCH (n) RETURN n OFFSET 1":                                "MATCH (n) RETURN n SKIP 1",
		"MATCH (n) LIMIT 1 ORDER BY n.v RETURN n":                    "MATCH (n) WITH * LIMIT 1 WITH * ORDER BY n.v RETURN n",
		"MATCH (n) RETURN n.order AS skip":                           "MATCH (n) RETURN n.order AS skip",
		"MATCH (n {limit: 1}) WHERE n.v > $skip RETURN n":            "MATCH (n {limit: 1}) WHERE n.v > $skip RETURN n",
		"MATCH (n) WHERE n.s = 'ORDER BY x' RETURN n":                "MATCH (n) WHERE n.s = 'ORDER BY x' RETURN n",
		"SHOW INDEXES YIELD name ORDER BY name RETURN name":          "SHOW INDEXES YIELD name ORDER BY name RETURN name",
		"MATCH (n) WHERE EXISTS { MATCH (n)--(m) LIMIT 1 } RETURN n": "MATCH (n) WHERE EXISTS { MATCH (n)--(m) WITH * LIMIT 1 } RETURN n",
		"MATCH (n) /* c */ ORDER BY n.v RETURN n":                    "MATCH (n) /* c */ WITH * ORDER BY n.v RETURN n",
		"WITH * MATCH (m) ORDER BY m.v RETURN m":                     "WITH * MATCH (m) WITH * ORDER BY m.v RETURN m",
		// A keyword where an operand is expected is a variable.
		"WITH 1 AS finish RETURN finish ORDER BY finish":         "WITH 1 AS finish RETURN finish ORDER BY finish",
		"MATCH (n) RETURN n.v AS optional ORDER BY optional":     "MATCH (n) RETURN n.v AS optional ORDER BY optional",
		"WITH 1 AS a, 2 AS match RETURN a, match ORDER BY a":     "WITH 1 AS a, 2 AS match RETURN a, match ORDER BY a",
		"UNWIND [2] AS distinct RETURN distinct AS v ORDER BY v": "UNWIND [2] AS distinct RETURN distinct AS v ORDER BY v",
		"WITH 1 AS limit MATCH (m) ORDER BY limit RETURN m":      "WITH 1 AS limit MATCH (m) WITH * ORDER BY limit RETURN m",
		// The label wildcard is an operand, not the modulo operator.
		"MATCH (a)-->(b) WHERE a:% RETURN a.id AS x ORDER BY x": "MATCH (a)-->(b) WHERE a:% RETURN a.id AS x ORDER BY x",
		"MATCH (a) WHERE a:A|% ORDER BY a.id RETURN a":          "MATCH (a) WHERE a:A|% WITH * ORDER BY a.id RETURN a",
		"WITH 7 AS v RETURN v % 2 AS m ORDER BY m":              "WITH 7 AS v RETURN v % 2 AS m ORDER BY m",
		// Unbalanced text is left for the parser to report.
		"MATCH (n ORDER BY n.v RETURN n":                      "MATCH (n ORDER BY n.v RETURN n",
		"MATCH (n) WHERE EXISTS { MATCH (m) LIMIT 1 RETURN n": "MATCH (n) WHERE EXISTS { MATCH (m) LIMIT 1 RETURN n",
	} {
		got, _ := desugarStandaloneOrderClauses(query)
		require.Equal(t, want, got, query)
	}
}

// A statement the rewrite leaves as it is costs no allocation to scan.
func TestDesugarStandaloneOrderClausesAllocatesNothingWithoutEdits(t *testing.T) {
	query := "MATCH (n:Person)-[:KNOWS]->(friend) WHERE n.age > $min RETURN friend.name AS name ORDER BY name SKIP 5 LIMIT 10"
	allocations := testing.AllocsPerRun(100, func() {
		if _, rewrite := desugarStandaloneOrderClauses(query); rewrite != nil {
			t.Fatal("unexpected rewrite")
		}
	})
	require.Zero(t, allocations)
}

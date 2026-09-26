package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCanonicalizeQueryText: every run of whitespace and comments outside
// quoted text becomes one space; quoted text stays, and NornicDB's
// own-syntax statements lose only their comments (#740).
func TestCanonicalizeQueryText(t *testing.T) {
	for input, want := range map[string]string{
		"RETURN 1 AS value // note\nRETURN 2":                 "RETURN 1 AS value RETURN 2",
		"RETURN 1 /* note */ AS value":                        "RETURN 1 AS value",
		"RETURN 1 /* a\nb */ AS value":                        "RETURN 1 AS value",
		"RETURN 1\r\n\t AS value":                             "RETURN 1 AS value",
		"RETURN 1/*c*/AS value":                               "RETURN 1 AS value",
		"MATCH (n)\n\n   WHERE n.p\n= 1\nRETURN n":            "MATCH (n) WHERE n.p = 1 RETURN n",
		"MATCH (n) RETURN n order\t\n by n.id":                "MATCH (n) RETURN n order by n.id",
		"MATCH (n) RETURN n ORDER // c\nBY n.id":              "MATCH (n) RETURN n ORDER BY n.id",
		"MATCH (n) RETURN n ORDER\u00a0BY n.id":               "MATCH (n) RETURN n ORDER BY n.id",
		"MATCH (n) RETURN n ORDER\u2003\fBY n.id":             "MATCH (n) RETURN n ORDER BY n.id",
		"CREATE INDEX i IF\n  NOT\tEXISTS FOR (n:L) ON (n.p)": "CREATE INDEX i IF NOT EXISTS FOR (n:L) ON (n.p)",
		// Quoted text keeps its spacing and comment markers.
		"RETURN 'ORDER  BY /* x */\n' AS s, `ORDER  BY` AS t, \"a  // b\" AS u": "RETURN 'ORDER  BY /* x */\n' AS s, `ORDER  BY` AS t, \"a  // b\" AS u",
		// NornicDB's own-syntax statements keep their lines; only comments go.
		"CREATE PROMOTION POLICY p FOR (n:L) {\n  ON ACCESS {\n    SET n.a = 1 // one\n    SET n.b = 2\n  }\n}": "CREATE PROMOTION POLICY p FOR (n:L) {\n  ON ACCESS {\n    SET n.a = 1 \n    SET n.b = 2\n  }\n}",
		"CREATE  PROCEDURE p() AS /* x */ {\n  RETURN 1\n}":                                                     "CREATE  PROCEDURE p() AS   {\n  RETURN 1\n}",
	} {
		got, _ := canonicalizeQueryText(input)
		require.Equal(t, want, got, "%q", input)
	}
}

// TestCanonicalizeQueryTextAllocationFree: a statement that is canonical
// already is returned as is, without allocating.
func TestCanonicalizeQueryTextAllocationFree(t *testing.T) {
	for _, query := range []string{
		"RETURN 'https://example.test/a/*b*/' AS url, `a//b` AS name",
		"MATCH (n:Person {name: $name}) WHERE n.age > 30 RETURN n.name AS name ORDER BY name DESC LIMIT 10",
		"MERGE (n:K {id: 1}) ON CREATE SET n.c = true ON MATCH SET n.m = true",
		"MATCH (ü:Straße) RETURN ü.größe AS g",
	} {
		got, rewrite := canonicalizeQueryText(query)
		require.Equal(t, query, got)
		require.Nil(t, rewrite)
		require.Zero(t, testing.AllocsPerRun(100, func() { _, _ = canonicalizeQueryText(query) }), query)
	}
}

// TestCanonicalQueryKeepsClientText: unaliased column names and messages show
// the statement as sent, as Neo4j 5.26 does, while it runs in canonical form.
func TestCanonicalQueryKeepsClientText(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canon740"))
	ctx := context.Background()
	for query, want := range map[string]struct {
		columns []string
		rows    [][]interface{}
	}{
		"UNWIND [2, 1] AS x RETURN x IS  NOT  NULL":                        {[]string{"x IS  NOT  NULL"}, [][]interface{}{{true}, {true}}},
		"UNWIND ['ab'] AS x RETURN x STARTS  /*c*/  WITH 'a'":              {[]string{"x STARTS  /*c*/  WITH 'a'"}, [][]interface{}{{true}}},
		"UNWIND [2, 1] AS x RETURN x ORDER /* c */ BY x":                   {[]string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
		"UNWIND [2, 1] AS x RETURN x /* the value */ + 1 AS y ORDER  BY y": {[]string{"y"}, [][]interface{}{{int64(2)}, {int64(3)}}},
		"UNWIND [2, 1] AS x RETURN x ORDER BY x":                           {[]string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
		"RETURN COLLECT { UNWIND [3, 1] AS y RETURN y ORDER  BY y }":       {[]string{"COLLECT { UNWIND [3, 1] AS y RETURN y ORDER  BY y }"}, [][]interface{}{{[]interface{}{int64(1), int64(3)}}}},
		"RETURN COLLECT { UNWIND [3, 1] AS y RETURN y ORDER  BY y } AS c":  {[]string{"c"}, [][]interface{}{{[]interface{}{int64(1), int64(3)}}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want.columns, result.Columns, query)
		require.Equal(t, want.rows, result.Rows, query)
	}
	// The canonical text's cached result keeps canonical columns; each
	// client sees its own text.
	first, err := exec.Execute(ctx, "UNWIND [1] AS x RETURN x IS  NULL", nil)
	require.NoError(t, err)
	second, err := exec.Execute(ctx, "UNWIND [1] AS x RETURN x IS\tNULL", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x IS  NULL"}, first.Columns)
	require.Equal(t, []string{"x IS\tNULL"}, second.Columns)
}

// multiWordKeywordStatements is the #740 corpus: statements Neo4j accepts
// with any whitespace between the words of their keywords, each with its
// result on :K {id: 1}, (:K {id: 2})-[:R]->(:K {id: 3}).
var multiWordKeywordStatements = []struct {
	query  string
	rows   [][]interface{}
	params map[string]interface{}
}{
	{query: "MATCH (n:K) WHERE n.id = $id RETURN n.id AS id", rows: [][]interface{}{{int64(1)}}, params: map[string]interface{}{"id": int64(1)}},
	{query: "MATCH (n:K) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id", rows: [][]interface{}{{int64(1)}, {int64(3)}}, params: map[string]interface{}{"ids": []interface{}{int64(1), int64(3)}}},
	{query: "MATCH (n:K) RETURN n.id AS id ORDER BY id", rows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
	{query: "MATCH (n:K) RETURN DISTINCT n.id AS id ORDER BY id DESC", rows: [][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}}},
	{query: "MATCH (n:K) WITH n ORDER BY n.id DESC LIMIT 1 RETURN n.id AS id", rows: [][]interface{}{{int64(3)}}},
	{query: "MATCH (n:K) RETURN n.id AS id ORDER BY id DESC SKIP 1", rows: [][]interface{}{{int64(2)}, {int64(1)}}},
	{query: "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x", rows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
	{query: "CALL db.labels() YIELD label RETURN label ORDER BY label DESC", rows: [][]interface{}{{"K"}}},
	{query: "MERGE (n:K {id: 1}) ON CREATE SET n.c = true ON MATCH SET n.m = true RETURN n.m AS m, n.c AS c", rows: [][]interface{}{{true, nil}}},
	{query: "MERGE (n:K {id: 9}) ON CREATE SET n.c = true ON MATCH SET n.m = true RETURN n.m AS m, n.c AS c", rows: [][]interface{}{{nil, true}}},
	{query: "UNWIND [1, 9] AS i MERGE (n:K {id: i}) ON CREATE SET n.c = true ON MATCH SET n.m = true RETURN n.id AS id, n.m AS m, n.c AS c ORDER BY id", rows: [][]interface{}{{int64(1), true, nil}, {int64(9), nil, true}}},
	{query: "MATCH (n:K {id: 2}) OPTIONAL MATCH (n)-[:R]->(m) RETURN n.id AS id, m.id AS m", rows: [][]interface{}{{int64(2), int64(3)}}},
	{query: "MATCH (n:K {id: 1}) OPTIONAL MATCH (n)-[:R]->(m) RETURN n.id AS id, m.id AS m", rows: [][]interface{}{{int64(1), nil}}},
	{query: "MATCH (n:K) WHERE EXISTS { OPTIONAL MATCH (n)-[:R]->(m) RETURN m } RETURN count(n) AS c", rows: [][]interface{}{{int64(3)}}},
	{query: "MATCH (n:K {id: 3}) DETACH DELETE n RETURN count(*) AS c", rows: [][]interface{}{{int64(1)}}},
	{query: "MATCH (n:K) WITH n WHERE n.id = 3 DETACH DELETE n RETURN count(*) AS c", rows: [][]interface{}{{int64(1)}}},
	{query: "MATCH (n:K) WHERE n.id IS NOT NULL AND NOT n.id IS NULL RETURN count(n) AS c", rows: [][]interface{}{{int64(3)}}},
	{query: "MATCH (n:K) WHERE toString(n.id) STARTS WITH '1' OR toString(n.id) ENDS WITH '3' RETURN n.id AS id ORDER BY id", rows: [][]interface{}{{int64(1)}, {int64(3)}}},
	{query: "RETURN 1 AS x UNION ALL RETURN 1 AS x", rows: [][]interface{}{{int64(1)}, {int64(1)}}},
	{query: "MATCH (n:K {id: 2}) CALL (n) { MATCH (n)-[:R]->(m) RETURN m.id AS m } RETURN m ORDER BY m", rows: [][]interface{}{{int64(3)}}},
	{query: "MATCH (n:K) RETURN n.id AS id, CASE WHEN n.id IS NULL THEN 0 ELSE n.id END AS v ORDER BY v DESC LIMIT 1", rows: [][]interface{}{{int64(3), int64(3)}}},
	{query: "CREATE INDEX k_id IF NOT EXISTS FOR (n:K) ON (n.id)", rows: nil},
	{query: "DROP INDEX missing_index IF EXISTS", rows: nil},
	{query: "DROP CONSTRAINT missing_constraint IF EXISTS", rows: nil},
}

// TestMultiWordKeywordsAcceptAnyWhitespace: every #740 statement gives the
// same rows and columns with any whitespace or comment Neo4j accepts
// between its tokens (outside quoted text), in auto-commit and in an
// explicit transaction.
func TestMultiWordKeywordsAcceptAnyWhitespace(t *testing.T) {
	gaps := []string{"  ", "\t", "\n    ", " /* c */ ", " // c\n", " ", " ", "\r\n", " \f "}
	random := rand.New(rand.NewSource(740))
	for _, statement := range multiWordKeywordStatements {
		variants := []string{statement.query}
		for i := 0; i < 6; i++ {
			variants = append(variants, respaceQuery(statement.query, func() string { return gaps[random.Intn(len(gaps))] }))
		}
		for _, explicit := range []bool{false, true} {
			for _, query := range variants {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ws740"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:K {id: 1}), (:K {id: 2})-[:R]->(:K {id: 3})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, query, statement.params)
				require.NoError(t, err, "%q", query)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				if statement.rows != nil {
					require.Equal(t, statement.rows, result.Rows, "%q", query)
					reference, err := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ws740ref")).Execute(ctx, statement.query, statement.params)
					if err == nil {
						// Named columns are the same; an unaliased column is
						// the text as written (TestCanonicalQueryKeepsClientText).
						require.Len(t, result.Columns, len(reference.Columns), "%q", query)
						for i, column := range reference.Columns {
							if isSimpleIdentifier(column) {
								require.Equal(t, column, result.Columns[i], "%q", query)
							}
						}
					}
				}
			}
		}
	}
}

// respaceQuery replaces each space outside quoted text with gap().
func respaceQuery(query string, gap func() string) string {
	var out strings.Builder
	for index := 0; index < len(query); {
		c := query[index]
		if c == '\'' || c == '"' || c == '`' {
			end := skipCypherQuotedText(query, index, c)
			out.WriteString(query[index:end])
			index = end
			continue
		}
		if c == ' ' {
			out.WriteString(gap())
		} else {
			out.WriteByte(c)
		}
		index++
	}
	return out.String()
}

func BenchmarkCanonicalizeQueryText(b *testing.B) {
	for name, query := range map[string]string{
		"canonical": "MATCH (n:Person {name: $name}) WHERE n.age > 30 RETURN n.name AS name ORDER BY name DESC LIMIT 10",
		"rewritten": "MATCH (n:Person {name: $name}) // who\nWHERE n.age > 30\nRETURN n.name AS name ORDER\n  BY name DESC LIMIT 10",
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = canonicalizeQueryText(query)
			}
		})
	}
}

// TestCanonicalQueryRestoresParserPosition: a parser position in a message
// ("line 1:22") is that of the text the client sent, across the CR LF and
// indentation runs the canonical text collapses.
func TestCanonicalQueryRestoresParserPosition(t *testing.T) {
	original := "MATCH (n)\r\n  WHERE n.x =\r\n  RETURN n"
	canonical, rewrite := canonicalizeQueryText(original)
	require.NotNil(t, rewrite)
	require.Equal(t, "MATCH (n) WHERE n.x = RETURN n", canonical)
	column := strings.Index(canonical, "RETURN")
	message := fmt.Sprintf("Invalid input: line 1:%d mismatched input 'RETURN'", column)
	require.Equal(t, "Invalid input: line 3:2 mismatched input 'RETURN'", rewrite.restoreMessage(message))
	require.Equal(t, "Invalid input: line 9:0 x", rewrite.restoreMessage("Invalid input: line 9:0 x"))
}

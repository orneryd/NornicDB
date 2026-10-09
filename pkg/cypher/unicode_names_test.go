package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Non-ASCII names, and Unicode arrows, as Neo4j 5.26.30 reads them; the
// answers are Neo4j's (#908).
func TestUnicodeNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unicode_names"))
	ctx := context.Background()
	run := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	one := [][]interface{}{{int64(1)}}
	for _, query := range []string{
		"WITH 1 AS ñRETURN RETURN ñRETURN",
		"WITH 1 AS ñWITH RETURN ñWITH",
		"WITH 1 AS éa RETURN éa",
		"WITH 1 AS aé RETURN aé",
		"WITH 1 AS 中文 RETURN 中文",
		"WITH 1 AS a€ RETURN a€",
		"WITH 1 AS ‿a RETURN ‿a",
		"WITH 1 AS Ⅰa RETURN Ⅰa",
		"WITH 1 AS a\u0300 RETURN a\u0300",
		"WITH 1 AS a\u0660 RETURN a\u0660",
		"WITH 1 AS a\u00adb RETURN a\u00adb",
		"WITH {ñ: 1} AS m RETURN m.ñ",
		"WITH 1 AS `a—b` RETURN `a—b`",
		"RETURN 1 AS c // — €",
		"WITH 1 AS `x y` RETURN {ñk: `x y`}.ñk",
	} {
		require.Equal(t, one, run(query), query)
	}
	require.Equal(t, [][]interface{}{{int64(1), []interface{}{"Ñandú"}}}, run("CREATE (n:Ñandú {ñ: 1}) RETURN n.ñ, labels(n)"))
	require.Equal(t, one, run("MATCH (n:Ñandú) WHERE n.ñ = 1 RETURN n.ñ AS ñ ORDER BY ñ"))
	require.Equal(t, [][]interface{}{{int64(2)}}, run("MATCH (n:Ñandú) SET n.é = 2 RETURN n.é"))
	require.Equal(t, one, run("MATCH (n:Ñandú) WITH n AS ñ MATCH (ñ) RETURN ñ.ñ"))
	require.Equal(t, [][]interface{}{{int64(3)}}, run("UNWIND [1, 2] AS ñ RETURN sum(ñ) AS ß"))
	run("MATCH (n:Ñandú) DETACH DELETE n")

	// A name starts with a letter, a letter number or a connector, and goes
	// on with those, digits, marks, currency signs and format characters.
	for _, query := range []string{
		"WITH 1 AS €a RETURN €a",
		"WITH 1 AS a· RETURN a·",
		"WITH 1 AS \u0660a RETURN 1",
		"RETURN 1 ★ AS c",
		"RETURN 2—1 AS c",
		"RETURN 2 —",
		"MATCH (a)−[r]−>(b) RETURN count(*)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}

	// Unicode dashes and heads in an arrow.
	run("CREATE (:UA)-[:R]->(:UA)")
	for _, query := range []string{
		"MATCH (a:UA)—[r]—>(b) RETURN count(*)",
		"MATCH (a:UA)‐[r]‐>(b) RETURN count(*)",
		"MATCH (a:UA)\u00ad[r]\u00ad>(b) RETURN count(*)",
		"MATCH (a:UA)－[r]－>(b) RETURN count(*)",
		"MATCH (a:UA)——>(b) RETURN count(*)",
		"MATCH (b)⟨—[r]—(a:UA) RETURN count(*)",
		"MATCH (b)〈—[r]—(a:UA) RETURN count(*)",
		"MATCH (b)﹤—[r]—(a:UA) RETURN count(*)",
		"MATCH (a:UA)—[r]—⟩(b) RETURN count(*)",
		"MATCH (a:UA)—[r]—〉(b) RETURN count(*)",
		"MATCH (a:UA)—[r]—﹥(b) RETURN count(*)",
	} {
		require.Equal(t, one, run(query), query)
	}
}

// A Fabric record's bound names are read by the one name rule too.
func TestUnicodeFabricBindingNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unicode_fabric"))
	exec.fabricRecordBindings = map[string]interface{}{"ñx": int64(7)}
	require.Equal(t, int64(7), exec.parseValue(context.Background(), "ñx"))
	require.Equal(t, "1ñ", exec.parseValue(context.Background(), "1ñ"))
}

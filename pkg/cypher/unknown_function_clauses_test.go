package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An unknown function fails with Neo4j's Unknown function in every clause,
// whether or not a plugin lookup is installed (nornicdb.Open installs one),
// and a dotted name isn't known because its last part is a built-in (#1166).
// Procedure names, USE targets, keywords before a parenthesis, strings and
// comments are not function calls. Expected outcomes checked against Neo4j
// 5.26.30 (apoc.coll.sum is one of NornicDB's own functions).
func TestUnknownFunctionFailsInEveryClause(t *testing.T) {
	unknown := []string{
		"WITH 1 AS x WHERE nosuch.fn(1) IS NULL RETURN x",
		"MATCH (n) WHERE nosuchfn(1) IS NULL RETURN n",
		"UNWIND [nosuch.fn(1)] AS v RETURN v",
		"WITH nosuch.fn(1) AS v RETURN v",
		"CALL { RETURN nosuch.fn(1) AS v } RETURN v",
		"UNWIND [1, 2] AS x RETURN x ORDER BY nosuchfn(x)",
		"RETURN nosuch.fn(1)",
		"RETURN apoc.text.camelCase('hello world')",
		"MERGE (t:ZZU {k: 1}) SET t.v = nosuch.fn(1) RETURN t.v AS v",
		"CREATE (t:ZZU {v: nosuchfn(1)})",
		"FOREACH (x IN [1] | CREATE (:ZZU {v: nosuchfn(x)}))",
		"RETURN EXISTS { UNWIND [1] AS x WITH x WHERE nosuch.fn(1) IS NULL RETURN x }",
		"CALL db.labels() YIELD label WHERE nosuchfn(label) RETURN label",
		"RETURN nosuch.toUpper('a')",
		"RETURN a.b.size([1, 2])",
		"RETURN date.toUpper('a')",
		"WITH date.toUpper('a') AS v RETURN v",
		"RETURN nosuch . fn(1)",
		"SHOW PROCEDURES YIELD name WHERE nosuch(name) RETURN name",
		"SHOW FUNCTIONS YIELD name WHERE nosuch(name) IS NULL",
		"SHOW DATABASES YIELD name WHERE nosuch(name) IS NULL",
	}
	known := []string{
		"CALL db.labels() YIELD label RETURN count(label) AS n",
		"CALL { CALL db.labels() YIELD label RETURN count(label) AS n } RETURN n",
		"CALL () { RETURN 1 AS v } RETURN v",
		"WITH 'nosuch(1)' AS s RETURN s // nosuch(1)",
		"WITH 1 AS x /* nosuch(1) */ RETURN x",
		"RETURN date.truncate('day', date('2026-10-13')) AS d",
		"SHOW PROCEDURES YIELD name WHERE toUpper(name) IS NOT NULL RETURN count(name) AS n",
		"RETURN apoc.coll.sum([1, 2]) AS s",
		"UNWIND ['ab'] AS s WITH s WHERE s CONTAINS ('a') AND s STARTS WITH ('a') RETURN s",
		"UNWIND [3, 1] AS x RETURN x ORDER BY (x) SKIP (0) LIMIT (1)",
		"UNWIND [1] AS x WITH x WHERE x IN ([1]) RETURN x",
	}
	for _, mode := range []struct {
		name   string
		lookup func(string) (interface{}, bool)
	}{
		{"without plugin lookup", nil},
		{"with plugin lookup", func(string) (interface{}, bool) { return nil, false }},
	} {
		t.Run(mode.name, func(t *testing.T) {
			previous := PluginFunctionLookup
			PluginFunctionLookup = mode.lookup
			t.Cleanup(func() { PluginFunctionLookup = previous })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unknown_function_clauses"))
			ctx := context.Background()
			for _, query := range unknown {
				_, err := exec.Execute(ctx, query, nil)
				requireSemanticDetail(t, err, "UnknownFunction")
			}
			for _, query := range known {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			// A graph a USE names is no call, its dots spaced or not, as the
			// USE parser reads it.
			for _, query := range []string{"USE graph.byName('x') RETURN 1", "USE graph . byName('x') RETURN 1"} {
				require.NoError(t, validateKnownFunctions(query), query)
			}
			// The statement's other semantic errors come first, as in Neo4j.
			_, err := exec.Execute(ctx, "WITH 1 AS a RETURN nosuch(b)", nil)
			require.ErrorContains(t, err, "b")
			requireSemanticDetail(t, err, "UndefinedVariable")
		})
	}
}

// A plugin's dotted function passes the check, in any clause.
func TestPluginFunctionPassesTheCheckInEveryClause(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "zzplugin.double" {
			return func(value int64) int64 { return value * 2 }, true
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
	err := validateKnownFunctions("UNWIND [1] AS x WITH x WHERE zzplugin.double(x) = 2 RETURN zzplugin.double(x)")
	require.NoError(t, err)
	requireSemanticDetail(t, validateKnownFunctions("RETURN zzplugin.triple(1)"), "UnknownFunction")
}

// Under NORNICDB_PARSER=antlr, a statement the grammar rejects but NornicDB's
// validator accepts reports an unknown function before the parser's error,
// as it reports the statement's other semantic errors.
func TestUnknownFunctionBeforeANTLRParserError(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	config.SetParserType(config.ParserTypeANTLR)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unknown_function_antlr"))
	requireSemanticDetail(t, exec.validateSyntax(context.Background(), "MATCH (n) RETURN nosuch(n) AS x LIMIT 1 OFFSET 1", true, syntaxStatement), "UnknownFunction")
}

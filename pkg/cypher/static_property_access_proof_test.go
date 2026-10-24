package cypher

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// staticPropertyAccessOracle holds the verdicts for
// staticPropertyAccessPopulation, by position: the queries Neo4j 5.26.30
// and #1552's head (9ea9fb11a) both reject on both parsers and in both
// transaction modes (kept), and the queries either of them rejects anywhere
// (allowed).
//
//go:embed testdata/static_property_access_oracle.json
var staticPropertyAccessOracle []byte

// staticPropertyAccessPopulation is every position a property access or map
// projection can take (projections, WHERE, ORDER BY, pattern properties,
// function arguments, CASE, comprehensions, reduce, CALL and expression
// subquery bodies, SET, MERGE, CREATE, UNWIND, UNION) on bases of every
// kind: values bound by WITH, UNWIND, MATCH, CALL and UNION under plain and
// keyword names (distinct, skip, order, limit), and literals. Every query
// starts with UNWIND [] AS r, so only a static check can reject it.
func staticPropertyAccessPopulation() []string {
	bindings := []string{
		"WITH 5 AS {v}", "WITH 'x' AS {v}", "WITH true AS {v}", "WITH 1.5 AS {v}", "WITH [1] AS {v}",
		"UNWIND [1] AS {v}", "UNWIND ['a'] AS {v}", "WITH date('2026-10-22') AS {v}", "WITH duration('P1D') AS {v}",
		"WITH {a: 1} AS {v}", "MATCH ({v})", "WITH CASE WHEN true THEN 1 ELSE 'a' END AS {v}",
		"WITH CASE WHEN true THEN 1 ELSE {a: 1} END AS {v}", "WITH null AS {v}", "WITH 5 + 1 AS {v}",
		"WITH toUpper('a') AS {v}", "WITH [1][0] AS {v}", "CALL () { RETURN 5 AS {v} }",
		"CALL () { RETURN 5 AS {v} UNION RETURN 'a' AS {v} }", "WITH 5 AS q WITH q AS {v}",
		"WITH [5] AS l UNWIND l AS {v}", "MATCH ()-[{v}]->()", "WITH {a: 1}.a AS {v}",
	}
	keywordBindings := []string{"WITH 5 AS {v}", "WITH {a: 1} AS {v}", "WITH 'x' AS {v}", "UNWIND [1] AS {v}"}
	forms := []string{"{v}.x", "{v}{.a}", "{v}{.*}", "{v}{k: 1}", "({v}).x", "{v}.x.y", "{v}['x']"}
	positions := []string{
		"RETURN {a} AS v",
		"WITH {a} AS v RETURN v",
		"WITH {c} WHERE {a} IS NULL RETURN 1 AS v",
		"WITH {c} ORDER BY {a} RETURN 1 AS v",
		"RETURN 1 AS v ORDER BY {a}",
		"MATCH (m {k: {a}}) RETURN 1 AS v",
		"MATCH (m) WHERE m.k = {a} RETURN 1 AS v",
		"RETURN toString({a}) AS v",
		"RETURN CASE WHEN true THEN {a} END AS v",
		"RETURN CASE {a} WHEN 1 THEN 1 END AS v",
		"RETURN [x IN [1] | {a}] AS v",
		"RETURN [x IN [1] WHERE {a} IS NULL] AS v",
		"MATCH (n) RETURN [(n)-->(m) | {a}] AS v",
		"RETURN reduce(acc = 0, x IN [1] | {a}) AS v",
		"CALL ({c}) { RETURN {a} AS v } RETURN v",
		"CALL { WITH {c} RETURN {a} AS v } RETURN v",
		"RETURN EXISTS { MATCH (m) WHERE {a} = 1 } AS v",
		"RETURN COUNT { MATCH (m) WHERE {a} = 1 } AS v",
		"MATCH (m) SET m.p = {a} RETURN 1 AS v",
		"MATCH (m) SET m += {a} RETURN 1 AS v",
		"MERGE (m:L {k: {a}}) RETURN 1 AS v",
		"CREATE (m:L {k: {a}}) RETURN 1 AS v",
		"UNWIND [{a}] AS v RETURN v",
		"RETURN {a} AS v UNION ALL RETURN 1 AS v",
		"RETURN all(x IN [1] WHERE {a} IS NULL) AS v",
		"RETURN {k: {a}} AS v",
		"WITH {c}, 1 AS z RETURN {a} AS v",
	}
	var population []string
	seen := map[string]bool{}
	emit := func(prefix, base, carry string) {
		for _, form := range forms {
			access := strings.ReplaceAll(form, "{v}", base)
			for _, position := range positions {
				query := "UNWIND [] AS r " + prefix + " " + strings.ReplaceAll(strings.ReplaceAll(position, "{c}", carry), "{a}", access)
				if !seen[query] {
					seen[query] = true
					population = append(population, query)
				}
			}
		}
	}
	for _, binding := range bindings {
		emit(strings.ReplaceAll(binding, "{v}", "s"), "s", "s")
	}
	for _, binding := range keywordBindings {
		for _, name := range []string{"distinct", "skip", "order", "limit"} {
			emit(strings.ReplaceAll(binding, "{v}", name), name, name)
		}
	}
	for _, literal := range []string{"'x'", "5", "[1]", "true"} {
		emit("WITH 1 AS z", literal, "z")
	}
	return population
}

// Moving static property access into the statement's one type scope keeps
// every rejection Neo4j 5.26.30 and #1552's head agree on, and adds none
// that neither makes, for a property access or map projection in every
// position, in auto-commit and in an explicit transaction.
func TestStaticPropertyAccessKeepsProvenRejections(t *testing.T) {
	var oracle struct {
		Population       int    `json:"population"`
		PopulationSha256 string `json:"populationSha256"`
		Kept             []int  `json:"kept"`
		Allowed          []int  `json:"allowed"`
	}
	require.NoError(t, json.Unmarshal(staticPropertyAccessOracle, &oracle))
	population := staticPropertyAccessPopulation()
	digest := sha256.Sum256([]byte(strings.Join(population, "\n")))
	// The verdicts are pinned by position: a changed population needs the
	// oracle taken again from Neo4j and #1552's head.
	require.Equal(t, oracle.Population, len(population))
	require.Equal(t, oracle.PopulationSha256, hex.EncodeToString(digest[:]))
	kept, allowed := map[int]bool{}, map[int]bool{}
	for _, index := range oracle.Kept {
		kept[index] = true
	}
	for _, index := range oracle.Allowed {
		allowed[index] = true
	}
	ctx := context.Background()
	for _, mode := range []string{"auto", "tx"} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_property_access_"+mode))
		for index, query := range population {
			if mode == "tx" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, query, nil)
			if mode == "tx" {
				_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			}
			switch {
			case err != nil && !allowed[index]:
				t.Errorf("%s: rejected without proof: %s: %v", mode, query, err)
			case err == nil && kept[index]:
				t.Errorf("%s: lost rejection: %s", mode, query)
			}
		}
	}
}

package cypher

// H2 variant of edb24ece9 "Preserve required node inputs while narrowing
// scalar reads" (units of the scalar-node demand analysis, label-scan
// projection and traversal endpoint analysis it changed). The fix's own
// assertion (scalar_node_demand_test.go / scalar_node_alias_escape_test.go:
// a statement whose node reads were narrowed to the properties it uses
// returns what the whole node returns) applied to the sibling escapes and
// demand shapes the regression tests do not list: map projections, dynamic
// keys, keys(), CASE, coalesce, list and map wrapping, aggregation, UNWIND,
// CALL subqueries, EXISTS/COUNT subqueries, ORDER BY / WHERE on another
// property, traversal endpoints and relationship properties. Oracle: the
// statement equals its reference, the same statement with a WITH that keeps
// properties(...) of every node variable (an escape the fix keeps whole).

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func h2SortedRows(rows [][]interface{}) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprint(row))
	}
	sort.Strings(out)
	return out
}

func TestHuntH2ScalarNarrowingSiblings(t *testing.T) {
	cases, leads := 0, 0
	const nodeMatch = "MATCH (n:EIBig)"
	const nodeRef = "MATCH (n:EIBig) WITH n, properties(n) AS _whole"
	const pathMatch = "MATCH (n:EIBig)-[r:R]->(m)"
	const pathRef = "MATCH (n:EIBig)-[r:R]->(m) WITH n, r, m, properties(n) AS _wn, properties(m) AS _wm, properties(r) AS _wr"
	shapes := []struct {
		match, ref, tail string
	}{
		{nodeMatch, nodeRef, "RETURN size(keys(n)) AS v"},
		{nodeMatch, nodeRef, "RETURN 'big' IN keys(n) AS v"},
		{nodeMatch, nodeRef, "RETURN n {.*} AS v"},
		{nodeMatch, nodeRef, "RETURN n {.small} AS v"},
		{nodeMatch, nodeRef, "RETURN n {.small, .num, extra: n.big} AS v"},
		{nodeMatch, nodeRef, "RETURN n[$key] AS v"},
		{nodeMatch, nodeRef, "RETURN n[toLower('BIG')] AS v"},
		{nodeMatch, nodeRef, "RETURN coalesce(n.missing, n.big) AS v"},
		{nodeMatch, nodeRef, "RETURN n.big IS NOT NULL AS v"},
		{nodeMatch, nodeRef, "RETURN size(n.big) AS v"},
		{nodeMatch, nodeRef, "RETURN CASE WHEN n.num > 4 THEN n.small ELSE n.big END AS v"},
		{nodeMatch, nodeRef, "RETURN size([k IN keys(n) WHERE n[k] IS NOT NULL]) AS v"},
		{nodeMatch, nodeRef, "RETURN [n][0].big AS v"},
		{nodeMatch, nodeRef, "RETURN {a: n}.a.big AS v"},
		{nodeMatch, nodeRef, "RETURN [x IN collect(n) WHERE x.num = 3][0].big AS v"},
		{nodeMatch, nodeRef, "RETURN max(n.big) AS v"},
		{nodeMatch, nodeRef, "RETURN sum(n.num) AS s, size(collect(n.big)) AS v"},
		{nodeMatch, nodeRef, "UNWIND [n] AS x RETURN x.big AS v"},
		{nodeMatch, nodeRef, "WITH collect(n) AS ns UNWIND ns AS x RETURN x.big AS v"},
		{nodeMatch, nodeRef, "WITH n ORDER BY n.big DESC LIMIT 1 RETURN n.small AS v"},
		{nodeMatch, nodeRef, "WITH n WHERE n.big STARTS WITH 'x' RETURN n.small AS v"},
		{nodeMatch, nodeRef, "WITH n AS a RETURN a.big AS v"},
		{nodeMatch, nodeRef, "WITH n AS a, n.small AS s RETURN a {.num, s: s, b: a.big} AS v"},
		{nodeMatch, nodeRef, "CALL (n) { RETURN n.big AS b } RETURN b AS v"},
		{nodeMatch, nodeRef, "CALL { WITH n RETURN n.big AS b } RETURN b AS v"},
		{nodeMatch, nodeRef, "RETURN EXISTS { MATCH (n) WHERE n.big IS NOT NULL } AS v"},
		{nodeMatch, nodeRef, "RETURN COUNT { MATCH (n)-[:R]->(o) WHERE o.num > 0 } AS v"},
		{nodeMatch, nodeRef, "RETURN [(n)-[:R]->(o) | o.big] AS v"},
		{nodeMatch, nodeRef, "RETURN apoc.map.get(properties(n), 'big', null) AS a, n.big AS v"},
		{nodeMatch, nodeRef, "RETURN n.small + n.big AS v"},
		{nodeMatch, nodeRef, "RETURN reduce(s = '', k IN ['small', 'big'] | s + n[k]) AS v"},
		{nodeMatch, nodeRef, "RETURN n.list[1] AS v"},
		{nodeMatch, nodeRef, "RETURN all(x IN n.list WHERE x > 0) AS v"},
		{nodeMatch, nodeRef, "SET n.touched = true RETURN n.big AS v"},
		{nodeMatch, nodeRef, "SET n.small = n.small + '!' RETURN n {.*} AS v"},
		{nodeMatch, nodeRef, "REMOVE n.num RETURN [k IN ['small', 'big', 'num', 'list', 'touched'] WHERE k IN keys(n)] AS v"},
		{pathMatch, pathRef, "RETURN m.big AS v"},
		{pathMatch, pathRef, "RETURN m {.*} AS v"},
		{pathMatch, pathRef, "RETURN [k IN ['small', 'big', 'num', 'list', 'touched'] WHERE k IN keys(m)] AS v"},
		{pathMatch, pathRef, "RETURN r.w AS v"},
		{pathMatch, pathRef, "RETURN r {.*} AS v"},
		{pathMatch, pathRef, "RETURN properties(r) AS v"},
		{pathMatch, pathRef, "RETURN n.big + m.small AS v"},
		{pathMatch, pathRef, "WITH m ORDER BY m.num RETURN m.big AS v"},
		{pathMatch, pathRef, "RETURN [m][0].big AS v"},
		{"MATCH p = (n:EIBig)-[r:R]->(m)", "MATCH p = (n:EIBig)-[r:R]->(m) WITH p, n, r, m, properties(n) AS _wn, properties(m) AS _wm", "RETURN [x IN nodes(p) | x.big] AS v"},
		{"MATCH p = (n:EIBig)-[r:R]->(m)", "MATCH p = (n:EIBig)-[r:R]->(m) WITH p, n, r, m, properties(n) AS _wn, properties(m) AS _wm", "RETURN last(nodes(p)).big AS v"},
		{"MATCH (n:EIBig) OPTIONAL MATCH (n)-[:NONE]->(o)", "MATCH (n:EIBig) OPTIONAL MATCH (n)-[:NONE]->(o) WITH n, o, properties(n) AS _wn", "RETURN n.big AS v, o.big AS w"},
		{"MATCH (n:EIBig), (m:EIBig)", "MATCH (n:EIBig), (m:EIBig) WITH n, m, properties(n) AS _wn, properties(m) AS _wm", "WHERE n <> m RETURN n.big AS v, m.small AS w"},
	}
	open := func(t *testing.T) *StorageExecutor {
		engine, err := storage.NewMemoryEngineWithOptions(storage.BadgerOptions{HighPerformance: true})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = engine.Close() })
		scoped := storage.NewNamespacedEngine(engine, "h2narrow")
		exec := NewStorageExecutor(scoped, QueryCachePolicy{MaxEntries: 0})
		for _, statement := range []string{
			"CREATE (a:EIBig {small: 'v', big: $big, num: 3, list: [1, 2, 3]})-[:R {w: 1, note: $big}]->(b:EIBig {small: 'w', big: 'yy', num: 5, list: [4]})",
			"CREATE (:EIBig {small: 'z', num: 7})",
		} {
			if _, err := exec.Execute(context.Background(), statement, map[string]interface{}{"big": strings.Repeat("x", 70<<10)}); err != nil {
				t.Fatalf("setup %q: %v", statement, err)
			}
		}
		return exec
	}
	forEachParser(t, func(t *testing.T) {
		for _, shape := range shapes {
			statement := shape.match + " " + shape.tail
			reference := shape.ref + " " + shape.tail
			t.Run(statement, func(t *testing.T) {
				cases++
				params := map[string]interface{}{"key": "big"}
				// Each side on its own store: a write shape must not see the
				// other side's write.
				narrowed, nErr := h2Exec(open(t), context.Background(), statement, params)
				whole, wErr := h2Exec(open(t), context.Background(), reference, params)
				if (nErr == nil) != (wErr == nil) || fmt.Sprint(h2SortedRows(narrowed)) != fmt.Sprint(h2SortedRows(whole)) {
					leads++
					trim := func(rows []string) []string {
						out := make([]string, 0, len(rows))
						for _, row := range rows {
							if len(row) > 200 {
								row = row[:200] + fmt.Sprintf("...(%d bytes)", len(row))
							}
							out = append(out, row)
						}
						return out
					}
					h2Lead(map[string]any{"harness": "narrowing", "unit": "edb24ece9", "statement": statement, "reference": reference,
						"rows": trim(h2SortedRows(narrowed)), "reference_rows": trim(h2SortedRows(whole)),
						"error": errString(nErr), "reference_error": errString(wErr),
						"setup": "CREATE (a:EIBig {small:'v', big:<70 KiB of x>, num:3, list:[1,2,3]})-[:R {w:1, note:<70 KiB>}]->(b:EIBig {small:'w', big:'yy', num:5, list:[4]}); CREATE (:EIBig {small:'z', num:7})"})
				}
			})
		}
	})
	h2Coverage(map[string]any{"harness": "narrowing", "commit": "edb24ece9", "cases": cases, "leads": leads})
}

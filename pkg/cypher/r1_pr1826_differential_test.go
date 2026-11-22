package cypher

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Review lane R1 of PR #1826: the cross product of first MATCH, middle clause
// and RETURN, answered by the ordinary executor and by the same executor over
// whole-node reads of an identical database. '@' is the variable the RETURN
// reads. Each mismatch is one line starting with R1DIFF, so the head's list can
// be compared with the base's.

var r1Firsts = []string{
	"MATCH (n:Doc {key: $k})",
	"MATCH (n:Doc) WHERE n.key = $k",
	"MATCH (n:Doc) WHERE n.key IN [$k]",
	"MATCH (n:Doc) WHERE n.extra > 6 AND n.extra < 8",
	"MATCH (n:Doc WHERE n.key = $k)",
	"MATCH (n:Doc) WHERE n['key'] = $k",
	"MATCH (n:Doc $props)",
	"MATCH pth = (n:Doc {key: $k})",
	"MATCH pth = (n:Doc) WHERE n.key = $k AND length(pth) = 0 AND n.status = 'current'",
	"UNWIND [$k] AS kk MATCH (n:Doc {key: kk})",
	"MATCH (n:Doc {key: $k}), (o:Other)",
	"MATCH (n:Doc {key: $k}), (n:Doc {extra: 7})",
	"MATCH (n:Doc) WHERE n.key = $k AND n.extra = 7",
}

var r1Middles = []struct{ text, variable string }{
	{"", "n"},
	{"WITH n", "n"},
	{"WITH n AS q", "q"},
	{"WITH DISTINCT n", "n"},
	{"WITH n ORDER BY n.extra LIMIT 1", "n"},
	{"WITH n WHERE n.extra = 7", "n"},
	{"WITH n, n.status AS s WHERE s = 'current'", "n"},
	{"WITH n AS q, count(*) AS c", "q"},
	{"MATCH (n:Doc)-[r:REL]->(m)", "n"},
	{"MATCH (n)-[r:REL]->(m)", "n"},
	{"OPTIONAL MATCH (n:Doc {extra: 7})-[r:REL]->(m)", "n"},
	{"MATCH (n:Doc {extra: 7})", "n"},
	{"MATCH (n {extra: 7})", "n"},
	{"MATCH (n:Doc WHERE n.extra = 7)", "n"},
	{"MATCH (q:Doc {key: n.key})", "q"},
	{"MATCH (n:Doc)-[:REL*0..1]->(q)", "q"},
	{"MATCH pp = (n:Doc)-[:REL]->(m) WITH nodes(pp)[0] AS q", "q"},
	{"UNWIND n.list AS x WITH n, x WHERE x = 2", "n"},
	{"UNWIND [n] AS q", "q"},
	{"WITH collect(n) AS ns UNWIND ns AS q", "q"},
	{"WITH n AS end", "end"},
}

var r1Returns = []string{
	"@.key AS a, @.path AS b",
	"@.key AS k ORDER BY @.extra DESC",
	"@.key AS k, @.status AS s ORDER BY s",
	"properties(@) AS p",
	"@ {.*} AS p",
	"@ {.key, .extra} AS p",
	"@ {.key, e: @.extra} AS p",
	"size(keys(@)) AS k",
	"[k IN keys(@) WHERE k = 'extra' | k] AS k",
	"@['extra'] AS e",
	"@[$prop] AS e",
	"@['ex' + 'tra'] AS e",
	"count(@.extra) AS c",
	"count(@) AS c",
	"count(DISTINCT @) AS c",
	"collect(@.path) AS c",
	"sum(@.extra) AS s, min(@.status) AS m",
	"head(collect(@)).extra AS e",
	"[x IN collect(@) | x.extra] AS e",
	"collect(@)[0].extra AS e",
	"size(collect(@)) AS c",
	"[x IN [@] | x.extra] AS e",
	"[@][0].extra AS e",
	"head([@]).extra AS e",
	"CASE WHEN @.extra = 7 THEN @.path ELSE @.status END AS c",
	"CASE @.status WHEN 'current' THEN @.extra ELSE 0 END AS c",
	"CASE WHEN true THEN @ END AS c",
	"labels(@) AS l, @:Doc AS d",
	"@ IS NOT NULL AS a, @ = @ AS b",
	"@.list[0] AS a, @.list[0..2] AS b, @.list[-1] AS c",
	"reduce(a = 0, x IN @.list | a + x) AS r",
	"any(x IN @.list WHERE x = 2) AS a, all(x IN @.list WHERE x > 0) AS b",
	"any(x IN [@] WHERE x.extra = 7) AS a",
	"toString(@.extra) AS s, @.extra + 1 AS p, -@.extra AS m",
	"coalesce(@.missing, @.extra) AS c",
	"coalesce(@, null).extra AS c",
	"@.missing IS NULL AS a, @.extra IS NOT NULL AS b",
	"{k: @.key, e: @.extra} AS m",
	"{n: @} AS m",
	"{n: @}.n.extra AS e",
	"[@.key, @.extra] AS l",
	"EXISTS { (@)-[:REL]->(x) WHERE x.extra > @.extra } AS e",
	"EXISTS { (@:Doc {extra: 7})-[:REL]->() } AS e",
	"COUNT { (@:Doc {extra: 7})-[:REL]->() } AS c",
	"COLLECT { MATCH (@)-[:REL]->(x) RETURN x.extra } AS c",
	"COLLECT { MATCH (@:Doc {extra: 7}) RETURN 1 } AS c",
	"[(@)-[:REL]->(x) | x.extra] AS l",
	"[(@:Doc {extra: 7})-[:REL]->(x) | x.extra] AS l",
	"[(@:Doc)-[:REL]->(x) WHERE x.extra > @.extra | @.path + x.path] AS l",
	"[p = (@:Doc)-[:REL]->(x) | [y IN nodes(p) | y.extra]] AS l",
	"size([(@)-[:REL]->(x) | 1]) AS c",
	"@.extra IS :: INTEGER AS t",
	"valueType(@.extra) AS t",
	"startNode(head([(@)-[r2:REL]->() | r2])).extra AS e",
	"@.key AS k, count(*) AS c",
	"DISTINCT @.status AS s",
	"@.key AS k SKIP 0 LIMIT 1",
	"@",
	"@.key AS k, @ AS v",
	"*",
	"elementId(@) IS NOT NULL AS e, id(@) IS NOT NULL AS i",
	"@.`extra` AS e",
	"@ . extra AS e",
	"@ /* c */ .extra AS e",
	"(@).extra AS e",
	"exists { MATCH (z:Doc) WHERE z.extra = @.extra } AS e",
	"[x IN range(0, size(@.list) - 1) | @.list[x]] AS l",
	"@.key STARTS WITH 'o' AND @.path CONTAINS 'ui' AS b",
	"@.key + '/' + @.path AS s",
	"max(@.extra) - min(@.extra) AS d, collect(DISTINCT @.status) AS s",
	"percentileCont(@.extra, 0.5) AS p",
	"tail(collect(@)) AS l",
	"last(collect(@)).path AS e",
	"reverse(collect(@))[0].path AS e",
}

func TestR1PR1826DifferentialProjectedAgainstWholeNode(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		controlStore, store := r1Fixture(t, indexed), r1Fixture(t, indexed)
		control := NewStorageExecutor(r1FullReads{controlStore, "r1"}, QueryCachePolicy{MaxEntries: 0})
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		ran, bothErrors := 0, 0
		counts := map[string]int{}
		var lines []string
		for f, first := range r1Firsts {
			for m, middle := range r1Middles {
				for r, returned := range r1Returns {
					query := strings.Join(strings.Fields(first+" "+middle.text+" RETURN "+strings.ReplaceAll(returned, "@", middle.variable)), " ")
					ran++
					want := r1Rows(control.Execute(t.Context(), query, r1Params()))
					got := r1Rows(exec.Execute(t.Context(), query, r1Params()))
					wantError := len(want) == 1 && strings.HasPrefix(want[0], "ERROR")
					gotError := len(got) == 1 && strings.HasPrefix(got[0], "ERROR")
					if wantError && gotError {
						bothErrors++
						continue
					}
					if reflect.DeepEqual(want, got) {
						continue
					}
					kind := "rows"
					if wantError != gotError {
						kind = "error-on-one-side"
					}
					counts[fmt.Sprintf("middle %q", middle.text)]++
					counts[fmt.Sprintf("first %q", first)]++
					counts["kind "+kind]++
					lines = append(lines, fmt.Sprintf("R1DIFF indexed=%t f%d m%d r%d %s :: %s :: whole=%q got=%q", indexed, f, m, r, kind, query, want, got))
				}
			}
		}
		keys := make([]string, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			t.Logf("R1COUNT indexed=%t %s = %d", indexed, key, counts[key])
		}
		t.Logf("R1TOTAL indexed=%t ran=%d bothErrors=%d mismatches=%d", indexed, ran, bothErrors, len(lines))
		for _, line := range lines {
			fmt.Println(line)
		}
		if len(lines) > 0 {
			t.Errorf("indexed=%t: %d of %d statements answer differently from whole-node reads", indexed, len(lines), ran)
		}
	}
}

var _ storage.Engine = r1FullReads{}

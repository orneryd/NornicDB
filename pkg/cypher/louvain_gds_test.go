package cypher

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// louvainKarateClub is Zachary's karate club: 34 members, 78 friendships.
const louvainKarateClub = "1-2,1-3,1-4,1-5,1-6,1-7,1-8,1-9,1-11,1-12,1-13,1-14,1-18,1-20,1-22,1-32," +
	"2-3,2-4,2-8,2-14,2-18,2-20,2-22,2-31,3-4,3-8,3-9,3-10,3-14,3-28,3-29,3-33,4-8,4-13,4-14," +
	"5-7,5-11,6-7,6-11,6-17,7-17,9-31,9-33,9-34,10-34,14-34,15-33,15-34,16-33,16-34,19-33,19-34," +
	"20-34,21-33,21-34,23-33,23-34,24-26,24-28,24-30,24-33,24-34,25-26,25-28,25-32,26-32,27-30," +
	"27-34,28-34,29-32,29-34,30-33,30-34,31-33,31-34,32-33,32-34,33-34"

// TestLouvainMatchesGDS: apoc.algo.louvain finds the communities, and the
// modularity, GDS 2.13's gds.louvain finds on the same graphs (concurrency
// 1, undirected), given the nodes in the order GDS reads them, their
// creation order (#1184).
func TestLouvainMatchesGDS(t *testing.T) {
	ringOfCliques := []interface{}{}
	for c := 0; c < 6; c++ {
		for i := 0; i < 4; i++ {
			for j := i + 1; j < 4; j++ {
				ringOfCliques = append(ringOfCliques, []interface{}{clique(c, i), clique(c, j), 1.0})
			}
		}
		ringOfCliques = append(ringOfCliques, []interface{}{clique(c, 0), clique((c+1)%6, 1), 1.0})
	}
	karate := []interface{}{}
	for _, edge := range strings.Split(louvainKarateClub, ",") {
		ends := strings.Split(edge, "-")
		karate = append(karate, []interface{}{ends[0], ends[1], 1.0})
	}
	triangles := []interface{}{}
	for _, pair := range []string{"ab", "bc", "ca", "de", "ef", "fd", "cd"} {
		triangles = append(triangles, []interface{}{pair[:1], pair[1:], 1.0})
	}
	selfLoops := append(append([]interface{}{}, triangles...),
		[]interface{}{"g", "g", 1.0}, []interface{}{"a", "a", 1.0}, []interface{}{"g", "d", 1.0})
	weighted := []interface{}{
		[]interface{}{"p", "q", 5.0}, []interface{}{"q", "r", 5.0}, []interface{}{"r", "p", 1.0}, []interface{}{"r", "s", 1.0},
		[]interface{}{"s", "t", 4.0}, []interface{}{"t", "u", 4.0}, []interface{}{"u", "s", 4.0}, []interface{}{"p", "u", 0.5},
	}

	for _, tc := range []struct {
		label      string
		nodes      []string
		edges      []interface{}
		weight     string
		want       []string
		modularity float64
	}{
		{"K", louvainSequence("", 1, 34), karate, "", []string{
			"1 2 3 4 8 10 12 13 14 18 20 22", "5 6 7 11 17", "24 25 26 28 29 32", "9 15 16 19 21 23 27 30 31 33 34",
		}, 0.41880341880341876},
		{"T", strings.Split("abcdefg", ""), triangles, "", []string{"a b c", "d e f", "g"}, 0.3571428571428571},
		{"G", ringOfCliqueNodes(), ringOfCliques, "", []string{
			"r00 r01 r02 r03", "r10 r11 r12 r13", "r20 r21 r22 r23", "r30 r31 r32 r33", "r40 r41 r42 r43", "r50 r51 r52 r53",
		}, 0.6904761904761906},
		{"W", strings.Split("pqrstu", ""), weighted, "w", []string{"p q r", "s t u"}, 0.43794252394835487},
		{"S", strings.Split("abcdefg", ""), selfLoops, "", []string{"a b c", "d e f", "g"}, 0.41500000000000004},
		// No relationships: every node its own community, as in GDS.
		{"N", strings.Split("xyz", ""), nil, "", []string{"x", "y", "z"}, 0},
	} {
		t.Run(tc.label, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "UNWIND range(0, size($ids) - 1) AS i CREATE (:"+tc.label+" {id: $ids[i], seq: i})", map[string]interface{}{"ids": tc.nodes})
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "UNWIND $edges AS e MATCH (a:"+tc.label+" {id: e[0]}), (b:"+tc.label+" {id: e[1]}) CREATE (a)-[:R {w: e[2]}]->(b)",
				map[string]interface{}{"edges": tc.edges})
			require.NoError(t, err)

			result, err := exec.Execute(ctx, "MATCH (n:"+tc.label+") WITH n ORDER BY n.seq WITH collect(n) AS nodes CALL apoc.algo.louvain(nodes, {weightProperty: '"+tc.weight+"'}) YIELD node, community RETURN node.id, community", nil)
			require.NoError(t, err)
			members := map[interface{}][]string{}
			for _, row := range result.Rows {
				members[row[1]] = append(members[row[1]], row[0].(string))
			}
			var groups []string
			for _, ids := range members {
				sort.Slice(ids, func(i, j int) bool { return louvainIDLess(ids[i], ids[j]) })
				groups = append(groups, strings.Join(ids, " "))
			}
			want := append([]string(nil), tc.want...)
			sort.Strings(groups)
			sort.Strings(want)
			require.Equal(t, want, groups)

			// Modularity, as GDS reports it.
			community := map[string]interface{}{}
			for _, row := range result.Rows {
				community[row[0].(string)] = row[1]
			}
			require.InDelta(t, tc.modularity, louvainModularity(tc.edges, community), 1e-9)
		})
	}
}

func clique(c, i int) string { return "r" + string(rune('0'+c)) + string(rune('0'+i)) }

func ringOfCliqueNodes() []string {
	var nodes []string
	for c := 0; c < 6; c++ {
		for i := 0; i < 4; i++ {
			nodes = append(nodes, clique(c, i))
		}
	}
	return nodes
}

// louvainSequence is prefix+from … prefix+to.
func louvainSequence(prefix string, from, to int) []string {
	var ids []string
	for i := from; i <= to; i++ {
		ids = append(ids, prefix+strconv.Itoa(i))
	}
	return ids
}

// louvainIDLess orders numeric ids by number, others by text.
func louvainIDLess(a, b string) bool {
	if len(a) != len(b) && strings.Trim(a+b, "0123456789") == "" {
		return len(a) < len(b)
	}
	return a < b
}

// louvainModularity is the weighted modularity of the partition.
func louvainModularity(edges []interface{}, community map[string]interface{}) float64 {
	degree := map[string]float64{}
	inside := map[interface{}]float64{}
	total := 0.0
	for _, item := range edges {
		edge := item.([]interface{})
		a, b, w := edge[0].(string), edge[1].(string), edge[2].(float64)
		degree[a] += w
		degree[b] += w
		total += w
		if community[a] == community[b] {
			inside[community[a]] += w
		}
	}
	if total == 0 {
		return 0
	}
	communityDegree := map[interface{}]float64{}
	for node, d := range degree {
		communityDegree[community[node]] += d
	}
	q := 0.0
	for c, d := range communityDegree {
		q += inside[c]/total - (d/(2*total))*(d/(2*total))
	}
	return q
}

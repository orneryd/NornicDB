package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPR1826S1PromotionVisibility decides whether a node that a promotion rule
// keeps visible (or fails to keep visible) is treated the same by a whole-node
// read and by a property-only read. The decay profile hides an old node; every
// property has its own long half-life, so only the node's own visibility is at
// stake. The first statement of each case is the control: RETURN n must show
// exactly the nodes the rule keeps.
func TestPR1826S1PromotionVisibility(t *testing.T) {
	type rule struct {
		name, predicate string
		kept, hidden    map[string]any // extra stored properties of the two nodes
	}
	rules := []rule{
		{"present", "n.keep = true", map[string]any{"keep": true}, map[string]any{"keep": false}},
		{"presentNumber", "n.importance >= 3", map[string]any{"importance": int64(5)}, map[string]any{"importance": int64(1)}},
		{"isNull", "n.archived IS NULL", map[string]any{}, map[string]any{"archived": true}},
		{"equalsNull", "n.archived = null", map[string]any{}, map[string]any{"archived": true}},
		{"coalesce", "coalesce(n.archived, 0) = 0", map[string]any{}, map[string]any{"archived": int64(1)}},
	}
	for _, r := range rules {
		for _, schema := range []string{"none", "index", "unique"} {
			t.Run(r.name+"/"+schema, func(t *testing.T) {
				ctx := context.Background()
				base := newTestMemoryEngine(t)
				base.SetDecayEnabled(true)
				kp := base.GetSchemaForNamespace("s1decay")
				require.NoError(t, kp.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "d", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.3, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				var propertyRules []knowledgepolicy.DecayProfilePropertyRule
				for _, p := range []string{"key", "text", "keep", "importance", "archived", "grp"} {
					propertyRules = append(propertyRules, knowledgepolicy.DecayProfilePropertyRule{PropertyPath: p, HalfLifeSeconds: 3600 * 24 * 36500})
				}
				require.NoError(t, kp.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "b", ProfileRef: "d", TargetLabels: []string{"Fact"}, PropertyRules: propertyRules}))
				store := storage.NewNamespacedEngine(base, "s1decay")
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
				statements := []string{
					"CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.25, scoreFloor: 0.9, scoreCap: 0.95, scope: 'NODE' }",
					"CREATE PROMOTION POLICY pol FOR (n:Fact) APPLY { WHEN " + r.predicate + " APPLY PROFILE p }",
				}
				switch schema {
				case "index":
					statements = append(statements, "CREATE INDEX s1f FOR (n:Fact) ON (n.key)")
				case "unique":
					statements = append(statements, "CREATE CONSTRAINT s1f FOR (n:Fact) REQUIRE n.key IS UNIQUE")
				}
				for _, s := range statements {
					if _, err := exec.Execute(ctx, s, nil); err != nil {
						t.Logf("S1PROMO %s/%s statement refused: %s: %v", r.name, schema, s, err)
						return
					}
				}
				old := time.Now().Add(-48 * time.Hour)
				create := func(key string, extra map[string]any) {
					props := map[string]any{"key": key, "text": "t-" + key, "grp": "g"}
					for k, v := range extra {
						props[k] = v
					}
					_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(key), Labels: []string{"Fact"}, Properties: props, CreatedAt: old, UpdatedAt: old})
					require.NoError(t, err)
				}
				create("kept", r.kept)
				create("gone", r.hidden)
				count := func(q string) int {
					result, err := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}).Execute(ctx, q, nil)
					if err != nil {
						t.Logf("S1PROMO error %s: %v", q, err)
						return -1
					}
					return len(result.Rows)
				}
				for _, key := range []string{"kept", "gone"} {
					whole := count("MATCH (n:Fact {key:'" + key + "'}) RETURN n")
					wholeScan := count("MATCH (n:Fact) WHERE n.grp = 'g' AND n.key = '" + key + "' RETURN n")
					t.Logf("S1PROMO CONTROL rule=%q schema=%s node=%s whole=%d wholeScan=%d", r.predicate, schema, key, whole, wholeScan)
					for _, q := range []string{
						"MATCH (n:Fact {key:'%s'}) RETURN n.text", "MATCH (n:Fact {key:'%s'}) RETURN n.text", "MATCH (n:Fact) WHERE n.key = '%s' RETURN n.text", "MATCH (n:Fact) WHERE n.key IN ['%s'] RETURN n.key", "MATCH (n:Fact) WHERE n.key STARTS WITH '%s' RETURN n.text", "UNWIND ['%s'] AS k MATCH (n:Fact {key:k}) RETURN n.text", "MATCH (n:Fact {key:'%s'}) RETURN n{.text}", "MATCH (n:Fact {key:'%s'}) WITH n AS m RETURN m.text", "MATCH (n:Fact) WHERE n.grp = 'g' AND n.key = '%s' RETURN n.text", "MATCH (n:Fact {key:'%s'}) RETURN properties(n)", "MATCH (n:Fact {key:'%s'}) RETURN n.text, n", "MATCH (n:Fact {key:'%s'}) RETURN n",
					} {
						query := fmt.Sprintf(q, key)
						got := count(query)
						verdict := "same"
						if got != whole {
							verdict = "DIFFERS"
							t.Errorf("S1MISMATCH promotion rule=%q schema=%s node=%s: RETURN n rows=%d, %s rows=%d", r.predicate, schema, key, whole, query, got)
						}
						t.Logf("S1PROMO rule=%q schema=%s node=%s whole=%d narrow=%d %s %s", r.predicate, schema, key, whole, got, verdict, q)
					}
				}
				counts := s1Run(exec, "MATCH (n:Fact) RETURN count(n)", nil) + " " + s1Run(exec, "MATCH (n:Fact) WHERE n.grp='g' RETURN count(n.text)", nil) + " " + s1Run(exec, "MATCH (n:Fact) RETURN n", nil)
				t.Logf("S1PROMO rule=%q schema=%s counts %s", r.predicate, schema, strings.ReplaceAll(counts, "\n", "/"))
			})
		}
	}
}

// TestPR1826S1NamedNodePaths: paths of one node, with and without an index. The
// oracle reads whole nodes.
func TestPR1826S1NamedNodePaths(t *testing.T) {
	seeds := []string{
		"MATCH p = (n:S1Doc {key:'b'})", "MATCH p=(n:S1Doc {key:$key})", "MATCH p = (n:S1Doc) WHERE n.key = 'b'", "MATCH p = (n:S1Doc) WHERE n.rank = 2", "MATCH p = (n:S1Doc) WHERE n.rank IN [2,3]", "MATCH p = (n:S1Doc {category:'x', rank:1})", "MATCH p = (n:S1Doc {key:'b'}) WHERE length(p) = 0 AND n.name = 'Beta'", "MATCH p = (n:S1Doc) WHERE length(p) = 0 AND n.name = 'Beta'", "MATCH p = (n:S1Doc) WHERE nodes(p)[0].name = 'Beta'", "MATCH p = (n:S1Doc {key:'b'}) WHERE nodes(p)[0].name = 'Beta'", "MATCH p = (:S1Doc {key:'b'})", "MATCH p = (n:S1Doc {key:'b'}), (m:S1Doc {key:'a'})", "MATCH p = (n:S1Doc {key:'b'}), q = (m:S1Doc {key:'a'})", "UNWIND ['a','b'] AS k MATCH p = (n:S1Doc {key:k})", "OPTIONAL MATCH p = (n:S1Doc {key:'b'})", "MATCH (z:S1Doc {key:'a'}) MATCH p = (n:S1Doc {key:'b'})", "MATCH p = (n:S1Doc {key:'b'})-[:LINK]->(m)", "MATCH p = (n:S1Doc)-[:LINK*0..1]->(m) WHERE n.key = 'b'", "MATCH p = shortestPath((n:S1Doc {key:'a'})-[:LINK*1..3]->(m:S1Doc {key:'c'}))", "CALL { MATCH p = (n:S1Doc {key:'b'}) RETURN p, n } WITH p, n", "MATCH p = (n:S1Doc {key:'b'}) WITH p, n", "MATCH p = (n:S1Doc {key:'b'}) WITH p AS p, n AS n WHERE length(p) = 0",
	}
	tails := []string{
		"RETURN p", "RETURN nodes(p)[0].name", "RETURN nodes(p)[0]", "RETURN [x IN nodes(p) | x.body]", "RETURN size(keys(nodes(p)[0]))", "RETURN properties(nodes(p)[0])", "RETURN n.key, nodes(p)[0].name", "RETURN nodes(p)[0].name, n", "RETURN length(p), n.key", "RETURN head(nodes(p)).rank", "WITH p RETURN nodes(p)[0].name", "WITH nodes(p) AS ns RETURN [x IN ns | x.name]", "WITH p AS q RETURN nodes(q)[0].rank", "UNWIND nodes(p) AS u RETURN u.name, u.body", "RETURN [x IN nodes(p) WHERE x.rank > 0 | x.key]", "RETURN collect(p)", "RETURN p = p", "RETURN relationships(p), n.key", "RETURN any(x IN nodes(p) WHERE x.name = 'Beta')", "RETURN n.key /*O*/ ORDER BY nodes(p)[0].name, n.key", "CALL (p) { RETURN nodes(p)[0].name AS v } RETURN v", "RETURN reduce(s = '', x IN nodes(p) | s + x.name)", "RETURN nodes(p)[0]{.name, .rank}", "RETURN count(p), n.key", "RETURN *",
	}
	for _, schema := range []string{"none", "range", "unique"} {
		t.Run(schema, func(t *testing.T) {
			store, _ := s1Store(t)
			s1Seed(t, store, schema)
			oracle := NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0})
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			bad, total := 0, 0
			for _, seed := range seeds {
				for _, tail := range tails {
					q := seed + " " + tail
					want, got := s1Run(oracle, q, s1Params()), s1Run(exec, q, s1Params())
					total++
					t.Logf("S1PATH %s %q %s", schema, q, map[bool]string{true: "same", false: "DIFFERS"}[want == got])
					if want != got {
						bad++
						if bad <= 12 {
							t.Errorf("S1MISMATCH path %s\nquery: %s\nfull reads: %.300s\nordinary:   %.300s", schema, q, want, got)
						}
					}
				}
			}
			t.Logf("S1COUNT paths schema=%s statements=%d differ=%d", schema, total, bad)
		})
	}
}

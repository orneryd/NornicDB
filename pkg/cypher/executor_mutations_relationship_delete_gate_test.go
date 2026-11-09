package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These cases exercise supported public routing, not the eligibility booleans
// of the retired private DELETE implementation.
func TestPublicRelationshipDeleteShapes(t *testing.T) {
	cases := []struct {
		name, query string
		deleted     int
		bad         bool
	}{
		{"plain", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R]->() DELETE r`, 1, false},
		{"with", `MATCH(a:N{id:'a'})-[r:R]->() WITH r DELETE r`, 1, false},
		{"non-simple limited base", `MATCH(a:N) WITH a MATCH(a)-[r:R]->() WITH r LIMIT 1 DELETE r`, 1, false},
		{"missing target", `MATCH(a:N{id:'a'})-[r:R]->(target:N{id:'ghost'}) DELETE r`, 0, false},
		{"multiple delete vars", `MATCH(a:N{id:'a'})-[r:R]->(b)-[other:R]->() DELETE r,other`, 2, false},
		{"union property and limited rows", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R|S]->() WHERE r.mark='yes' WITH r LIMIT 1 DELETE r`, 1, false},
		{"unwind", `UNWIND [1] AS x MATCH(a:N{id:'a'})-[r:R]->() DELETE r`, 1, false},
		{"optional", `MATCH(a:N{id:'a'}) OPTIONAL MATCH(a)-[r:Missing]->() DELETE r`, 0, false},
		{"missing source", `MATCH(a:N{id:'missing'}) MATCH(a)-[r]->() DELETE r`, 0, false},
		{"type excluded", `MATCH(a:N{id:'a'}) MATCH(a)-[r:Missing]->() DELETE r`, 0, false},
		{"property excluded", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R{mark:'no'}]->() DELETE r`, 0, false},
		{"target label excluded", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R]->(:Missing) DELETE r`, 0, false},
		{"where excluded", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R]->() WHERE r.mark='no' DELETE r`, 0, false},
		{"unnamed source clause", `MATCH() MATCH(a:N{id:'a'})-[r:R]->() DELETE r`, 1, false},
		{"chained", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R]->(b)-[other:R]->() DELETE r`, 1, false},
		{"independent start binding", `MATCH(source:N{id:'c'}) MATCH(other:N{id:'a'})-[r:R]->() DELETE r`, 1, false},
		{"variable length", `MATCH(a:N{id:'a'}) MATCH(a)-[r:R*1..2]->() DELETE r`, 2, false},
		{"empty", `DELETE`, 0, true},
		{"undefined delete variable", `MATCH(a)-[other:R]->(b) DELETE r`, 0, true},
		{"unknown procedure", `CALL db.no_such_delete_fixture() MATCH(a)-[r]->() DELETE r`, 0, true},
		{"direct match limit", `MATCH(a)-[r]->() LIMIT 1 DELETE r`, 1, false},
		{"direct match skip", `MATCH(a)-[r]->() SKIP 1 DELETE r`, 3, false},
		{"direct match order", `MATCH(a)-[r]->() ORDER BY r DELETE r`, 4, false},
	}
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					exec := seedPublicDeleteGraph(t)
					r, err := exec.Execute(t.Context(), tc.query, nil)
					if tc.bad {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.deleted, r.Stats.RelationshipsDeleted)
					}
					requirePublicDeleteEdges(t, exec, 4-tc.deleted)
				})
			}
		})
	}
}

func seedPublicDeleteGraph(t testing.TB) *StorageExecutor {
	t.Helper()
	exec, _ := newTestExecutor(t, "delete_public")
	_, err := exec.Execute(context.Background(), `CREATE(a:N{id:'a'}),(b:N{id:'b'}),(c:N{id:'c'}),(a)-[:R{mark:'yes'}]->(b),(a)-[:S{mark:'yes'}]->(c),(a)-[:T]->(a),(b)-[:R]->(c)`, nil)
	require.NoError(t, err)
	return exec
}
func requirePublicDeleteEdges(t testing.TB, exec *StorageExecutor, want int) {
	t.Helper()
	r, err := exec.Execute(context.Background(), `MATCH()-[r]->() RETURN count(r) AS edges`, nil)
	require.NoError(t, err)
	require.EqualValues(t, want, r.Rows[0][0])
}

func TestPublicRelationshipDeleteLimits(t *testing.T) {
	base := `MATCH(a:N{id:'a'}) MATCH(a)-[r]->()`
	cases := []struct {
		name, suffix string
		deleted      int
		bad          bool
	}{
		{"valid5", " WITH r LIMIT 5", 3, false}, {"limit1", " WITH r LIMIT 1", 1, false},
		{"order", " WITH r ORDER BY id(r) LIMIT 1", 1, false}, {"skip", " WITH r SKIP 1 LIMIT 1", 1, false},
		{"unwind", " UNWIND [1] AS x WITH r LIMIT 1", 1, false},
		{"optional", " OPTIONAL MATCH(a)-[other:Missing]->() WITH r LIMIT 1", 1, false},
		{"no limit", " WITH r", 3, false},
		{"skip without limit", " WITH r SKIP 1", 2, false}, {"zero", " WITH r LIMIT 0", 0, false},
		{"wrong var", " WITH x LIMIT 1", 0, true}, {"nonnumeric", " WITH r LIMIT abc", 0, true},
		{"negative", " WITH r LIMIT -1", 0, true}, {"trailing tokens", " WITH r LIMIT 1 2", 0, true},
		{"call", " CALL db.no_such_delete_fixture() WITH r LIMIT 1", 0, true},
	}
	for _, p := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(p.name, func(t *testing.T) {
			t.Cleanup(p.setup())
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					e := seedPublicDeleteGraph(t)
					r, err := e.Execute(t.Context(), base+tc.suffix+" DELETE r", nil)
					if tc.bad {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.deleted, r.Stats.RelationshipsDeleted)
					}
					requirePublicDeleteEdges(t, e, 4-tc.deleted)
				})
			}
		})
	}
}

func TestPublicRelationshipDeleteDirectionsAndTypes(t *testing.T) {
	cases := []struct {
		name, pattern string
		deleted       int
	}{
		{"outgoing", `(a:N{id:'a'})-[r:R]->(b:N{id:'b'})`, 1},
		{"outgoing wrong endpoint", `(b:N{id:'b'})-[r:R]->(a:N{id:'a'})`, 0},
		{"incoming", `(b:N{id:'b'})<-[r:R]-(a:N{id:'a'})`, 1},
		{"incoming wrong endpoint", `(a:N{id:'a'})<-[r:R]-(b:N{id:'b'})`, 0},
		{"both start", `(a:N{id:'a'})-[r:R]-(b:N{id:'b'})`, 1},
		{"both end", `(b:N{id:'b'})-[r:R]-(a:N{id:'a'})`, 1},
		{"unrelated", `(c:N{id:'c'})-[r:R]-(a:N{id:'a'})`, 0},
		{"self loop", `(a:N{id:'a'})-[r:T]-(a)`, 1},
		{"no restriction", `(a:N{id:'a'})-[r]->()`, 3},
		{"single mismatch", `(a:N{id:'a'})-[r:Missing]->()`, 0},
		{"union match", `(a:N{id:'a'})-[r:R|S]->()`, 2},
		{"union mismatch", `(a:N{id:'a'})-[r:Missing|Other]->()`, 0},
		{"named end where", `(a:N{id:'a'})-[r:R]->(dest:N) WHERE dest.id='b' AND r.mark='yes'`, 1},
	}
	for _, p := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(p.name, func(t *testing.T) {
			t.Cleanup(p.setup())
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					e := seedPublicDeleteGraph(t)
					r, err := e.Execute(t.Context(), "MATCH "+tc.pattern+" DELETE r", nil)
					require.NoError(t, err)
					require.Equal(t, tc.deleted, r.Stats.RelationshipsDeleted)
					requirePublicDeleteEdges(t, e, 4-tc.deleted)
				})
			}
		})
	}
}

package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Relationship facts that depend only on statement text are read once per
// shape; rows, parameters and graph data are still read on every call.

func TestBoundPatternCountAdmissionReadOncePerBody(t *testing.T) {
	for body, want := range map[string]bool{
		"(n)-[:`A|B`]->(m)":                  true,
		"MATCH (n)-->(m) WHERE m.x > 1":      true,
		"(n)-[:`A WHERE B`]->(m)":            true,
		"(n WHERE n.x > 1)-->(m)":            false,
		"(n)-[r WHERE r.w = 2]->(m)":         false,
		"MATCH (n)-->(m) RETURN m":           false,
		"MATCH (n)-->(m) MATCH (m)-->(o)":    false,
		"(n)-[:R {k: 'x WHERE y'}]->(m:Lbl)": true,
	} {
		require.Equal(t, want, boundPatternCountAdmissionOf(body).admitted, body)
		require.Equal(t, want, boundPatternCountAdmissionOf(body).admitted, body)
		require.Zero(t, testing.AllocsPerRun(20, func() { boundPatternCountAdmissionOf(body) }), body)
	}
	require.ElementsMatch(t, []string{"n", "m", "r"}, boundPatternCountAdmissionOf("(n)-[r:`A|B`]->(m) WHERE m.x > 1").variables)
	long := "(n)-[:R]->(m) WHERE m.k = '" + strings.Repeat("x", 1100) + "'"
	require.True(t, boundPatternCountAdmissionOf(long).admitted)
	_, cached := boundPatternCountAdmissions.get(long)
	require.False(t, cached, "oversized bodies are read on each call, not retained")
}

func TestRelationshipDeclarationOfReadsTypesOncePerInterior(t *testing.T) {
	for _, inner := range []string{"r:`A|B`|C", "r:A|B|C", ":`A``B`", "r:$(t)|A", "r:A*1..2"} {
		first := relationshipDeclarationOf(inner)
		again := relationshipDeclarationOf(inner)
		require.Equal(t, first.types.storageNames(), again.types.storageNames(), inner)
		require.Equal(t, first.variable, again.variable, inner)
		require.Equal(t, first.alternatives, again.alternatives, inner)
		require.Zero(t, testing.AllocsPerRun(20, func() { relationshipDeclarationOf(inner) }), inner)
	}
	require.Equal(t, []string{"A|B", "C"}, relationshipDeclarationOf("r:`A|B`|C").types.storageNames())
	require.Equal(t, "r", relationshipDeclarationHead("r:`A|B`|C").variable)
}

func TestRelationshipOwnerRowControlsReachRetainedFacts(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, time.Minute)
	for _, statement := range []string{
		"CREATE (:P {id: 1})-[:A]->(:Q {id: 1}), (:P {id: 2})-[:A]->(:Q {id: 2}), (:P {id: 3})",
		"CREATE (:P2 {id: 1})-[:C]->(:Q2 {id: 1}), (:P2 {id: 2})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	for query, want := range map[string]int64{
		"MATCH (a:P)-[:A|B|C]->(b) RETURN count(b) AS n":                                   2,
		"MATCH (a:P) WITH a, COUNT { (a)-[:A|B]->(:Q) } AS c RETURN sum(c) AS n":           2,
		"MATCH (a:P2) WHERE EXISTS { (a)-[:C]->(:Q2) } RETURN count(a) AS n":               1,
		"MATCH (a:P) WITH a, COUNT { (a)-[:`A`|B]->(:Q) } AS c RETURN sum(c) AS n":         2,
		"MATCH (a:P) WITH a, COUNT { (a)-[:A WHERE true]->(:Q) } AS c RETURN sum(c) AS n":  2,
		"MATCH (a:P) WITH a, COUNT { (a)-[:A]->(q:Q {id: a.id}) } AS c RETURN sum(c) AS n": 2,
		// Unanchored bodies (no pattern variable bound by the row) take the
		// pipeline's label scan rather than the bound-entity kernel.
		"MATCH (a:P) WHERE EXISTS { MATCH (s:Q {id: a.id}) WHERE any(i IN range(0, 1) WHERE s.id >= i) } RETURN count(a) AS n": 2,
		"MATCH (a:P) WITH a, COUNT { (s:Q {id: a.id}) } AS c RETURN sum(c) AS n":                                               2,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, fmt.Sprint(want), fmt.Sprint(result.Rows[0][0]), query)
	}
	// A map reading the row keeps the body out of the chain kernel, so it
	// reaches the bound-pattern admission, retained per body.
	_, admitted := boundPatternCountAdmissions.get("(a)-[:A]->(q:Q {id: a.id})")
	require.True(t, admitted, "the per-row COUNT control reaches the retained admission")
}

// Executor publication carries the facts it already parsed, so invalidation
// never resolves a deferred entry under the cache lock for executed queries.
func TestExecutorPublicationCarriesResolvedSchemaFacts(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 64, time.Minute)
	_, err := exec.Execute(ctx, "CREATE (:User {id: 1})-[:`A|B`]->(:Item)", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:User) RETURN count(n) AS n",
		"MATCH ()-[:`A|B`]->() RETURN count(*) AS n",
		"MATCH (n) RETURN count(n) AS n",
		"MATCH (`a b`:User)-[r]->(m) RETURN count(`a b`) AS n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	exec.cache.mu.RLock()
	defer exec.cache.mu.RUnlock()
	require.NotEmpty(t, exec.cache.cache)
	for key, entry := range exec.cache.cache {
		require.False(t, entry.dependencies.deferred(), key)
	}
}

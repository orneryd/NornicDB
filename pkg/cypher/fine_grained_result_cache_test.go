package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedReadCacheKeepsPolicyAndGraphRevisionsApart(t *testing.T) {
	store := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 50})
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"public": "visible", "secret": "hidden"}})
	require.NoError(t, err)
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Doc"}}},
			{ID: "public", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"public"}}},
			{ID: "secret", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"secret"}}},
		},
		Assignments: map[string][]string{"reader": {"database", "traverse", "public"}, "all": {"database", "traverse", "public", "secret"}, "no-access": {"traverse", "public"}},
	})
	require.NoError(t, err)
	request := func(role, fingerprint string) *auth.FineGrainedRequest {
		return &auth.FineGrainedRequest{Access: policy.ForRoles([]string{role}), Fingerprint: fingerprint, DatabaseID: func(string) string { return "work-id" }}
	}
	query := "MATCH (n:Doc) RETURN n.public, n.secret ORDER BY n.public"
	reader := auth.WithFineGrainedRequest(t.Context(), request("reader", "reader-revision-1"))
	all := auth.WithFineGrainedRequest(t.Context(), request("all", "all-revision-1"))
	first, err := exec.Execute(reader, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"visible", nil}}, first.Rows)
	hitsBefore, _, _, _, _ := exec.cache.Stats()
	second, err := exec.Execute(reader, query, nil)
	require.NoError(t, err)
	require.Equal(t, first.Rows, second.Rows)
	hitsAfter, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, hitsAfter, hitsBefore, "the same stable FGAC principal should reuse its read result")
	// A recreated database can retain its name but receives a new stable ID.
	// Its result must not reuse an entry from the former database identity.
	recreated := request("reader", "reader-revision-1")
	recreated.DatabaseID = func(string) string { return "recreated-work-id" }
	hitsBefore, _, _, _, _ = exec.cache.Stats()
	recreatedResult, err := exec.Execute(auth.WithFineGrainedRequest(t.Context(), recreated), query, nil)
	require.NoError(t, err)
	require.Equal(t, first.Rows, recreatedResult.Rows)
	hitsAfter, _, _, _, _ = exec.cache.Stats()
	require.Equal(t, hitsBefore, hitsAfter, "a new database identity cannot reuse the old result")
	changedGrants := auth.WithFineGrainedRequest(t.Context(), request("all", "reader-revision-1"))
	hitsBefore, _, _, _, _ = exec.cache.Stats()
	changedResult, err := exec.Execute(changedGrants, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"visible", "hidden"}}, changedResult.Rows)
	hitsAfter, _, _, _, _ = exec.cache.Stats()
	require.Equal(t, hitsBefore, hitsAfter, "different effective grants cannot reuse a colliding fingerprint")
	// Even if an internal caller supplies a stale fingerprint, cache lookup
	// must still check current database admission before it can return rows.
	denied := auth.WithFineGrainedRequest(t.Context(), request("no-access", "reader-revision-1"))
	_, err = exec.Execute(denied, query, nil)
	require.Error(t, err)

	full, err := exec.Execute(all, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"visible", "hidden"}}, full.Rows)
	revised := auth.WithFineGrainedRequest(t.Context(), request("reader", "reader-revision-2"))
	hitsBefore, _, _, _, _ = exec.cache.Stats()
	revisionResult, err := exec.Execute(revised, query, nil)
	require.NoError(t, err)
	require.Equal(t, first.Rows, revisionResult.Rows)
	hitsAfter, _, _, _, _ = exec.cache.Stats()
	require.Equal(t, hitsBefore, hitsAfter, "a new policy revision cannot reuse a prior revision's result")
	readerAgain, err := exec.Execute(reader, query, nil)
	require.NoError(t, err)
	require.Equal(t, first.Rows, readerAgain.Rows)

	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"public": "new", "secret": "new-secret"}})
	require.NoError(t, err)
	afterWrite, err := exec.Execute(reader, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"new", nil}, {"visible", nil}}, afterWrite.Rows)
}

package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedBoundDeleteFixture creates a source Repository with `targets` DEPLOYS_FROM
// edges to distinct target repositories, each carrying
// evidence_source='resolver/cross-repo'. It returns a ready executor.
func seedBoundDeleteFixture(t *testing.T, targets int) *StorageExecutor {
	t.Helper()
	exec, _ := newCountingExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	for i := 0; i < targets; i++ {
		_, err = exec.Execute(ctx, "CREATE (:Repository {id:$id})", map[string]interface{}{"id": "repository:target-" + string(rune('a'+i))})
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:$id})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, map[string]interface{}{"id": "repository:target-" + string(rune('a'+i))})
		require.NoError(t, err)
	}
	return exec
}

// TestBoundRelationshipDelete_RoutingGuards covers the early-exit guards that
// decline the fast path (returning handled=false so the caller falls back to
// the general delete path).
func TestBoundRelationshipDelete_RoutingGuards(t *testing.T) {
	exec := seedBoundDeleteFixture(t, 1)
	ctx := context.Background()
	seg := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)`

	t.Run("detach declines fast path", func(t *testing.T) {
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx, seg, "", "rel", true)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
	t.Run("multiple delete vars decline fast path", func(t *testing.T) {
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx, seg, "", "rel,other", false)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
	t.Run("empty delete var declines fast path", func(t *testing.T) {
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx, seg, "", "  ", false)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
}

// TestBoundRelationshipDelete_FilterBranches covers candidate-filtering paths
// where the fast path is eligible but no edge qualifies, so nothing is deleted.
func TestBoundRelationshipDelete_FilterBranches(t *testing.T) {
	cases := []struct {
		name    string
		segment string
	}{
		{
			name: "no matching source node",
			segment: `
MATCH (source_repo:Repository {id:'repository:missing'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)`,
		},
		{
			name: "relationship type excluded",
			segment: `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:NO_SUCH_TYPE]->(:Repository)`,
		},
		{
			name: "relationship property excluded",
			segment: `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM {evidence_source:'does-not-match'}]->(:Repository)`,
		},
		{
			name: "end node label excluded",
			segment: `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:NotARepository)`,
		},
		{
			name: "where clause excludes",
			segment: `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'does-not-match'`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec := seedBoundDeleteFixture(t, 1)
			res, ok, err := exec.tryExecuteBoundRelationshipDelete(context.Background(), tc.segment, "", "rel", false)
			require.NoError(t, err)
			require.True(t, ok, "fast path should handle the query")
			require.Equal(t, 0, res.Stats.RelationshipsDeleted, "no edge should qualify")
		})
	}
}

// TestBoundRelationshipDelete_LimitStopsAtRowCount covers the WITH <rel> LIMIT n
// path: with two qualifying edges and LIMIT 1, exactly one is deleted.
func TestBoundRelationshipDelete_LimitStopsAtRowCount(t *testing.T) {
	exec := seedBoundDeleteFixture(t, 2)
	seg := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WITH rel LIMIT 1`
	res, ok, err := exec.tryExecuteBoundRelationshipDelete(context.Background(), seg, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, res.Stats.RelationshipsDeleted)

	verify, err := exec.Execute(context.Background(), "MATCH ()-[rel:DEPLOYS_FROM]->() RETURN count(rel) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), verify.Rows[0][0], "one edge should remain")
}

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
		fixtureAdmission1190Context, fixtureAdmission1190 := statementTestContext(ctx, "")
		_ = fixtureAdmission1190Context
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1190, fixtureAdmission1190Context, seg, "", "rel", true)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
	t.Run("multiple delete vars decline fast path", func(t *testing.T) {
		fixtureAdmission1191Context, fixtureAdmission1191 := statementTestContext(ctx, "")
		_ = fixtureAdmission1191Context
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1191, fixtureAdmission1191Context, seg, "", "rel,other", false)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
	t.Run("empty delete var declines fast path", func(t *testing.T) {
		fixtureAdmission1192Context, fixtureAdmission1192 := statementTestContext(ctx, "")
		_ = fixtureAdmission1192Context
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1192, fixtureAdmission1192Context, seg, "", "  ", false)
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
			fixtureAdmission1588Context, fixtureAdmission1588 := statementTestContext(context.Background(), "")
			_ = fixtureAdmission1588Context
			res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1588, fixtureAdmission1588Context, tc.segment, "", "rel", false)
			require.NoError(t, err)
			require.True(t, ok, "fast path should handle the query")
			require.Equal(t, 0, res.Stats.RelationshipsDeleted, "no edge should qualify")
		})
	}
}

func TestBoundRelationshipDelete_EmptySingleClauseRelationshipDeleteSkipsImplicitCommit(t *testing.T) {
	exec := seedBoundDeleteFixture(t, 100)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
MATCH (r:MissingRationale)-[rel:EXPLAINS]->()
WHERE r.repo_id IN $ids AND rel.evidence_source IN $sources
DELETE rel`, map[string]interface{}{
		"ids":     []string{"repository:missing"},
		"sources": []string{"generated"},
	})
	require.NoError(t, err)
	require.Zero(t, result.Stats.RelationshipsDeleted)

	verify, err := exec.Execute(ctx, "MATCH ()-[rel:DEPLOYS_FROM]->() RETURN count(rel)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, verify.Rows[0][0])
}

// TestBoundRelationshipDelete_LimitStopsAtRowCount covers the WITH <rel> LIMIT n
// path: with two qualifying edges and LIMIT 1, exactly one is deleted.
func TestBoundRelationshipDelete_LimitStopsAtRowCount(t *testing.T) {
	exec := seedBoundDeleteFixture(t, 2)
	seg := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WITH rel LIMIT 1`
	fixtureAdmission1589Context, fixtureAdmission1589 := statementTestContext(context.Background(), "")
	_ = fixtureAdmission1589Context
	res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1589, fixtureAdmission1589Context, seg, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, res.Stats.RelationshipsDeleted)

	verify, err := exec.Execute(context.Background(), "MATCH ()-[rel:DEPLOYS_FROM]->() RETURN count(rel) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), verify.Rows[0][0], "one edge should remain")
}

// TestBoundRelationshipDelete_EligibilityGuards covers the pattern-shape guards
// that decline the fast path when the two-clause bound-delete form is not met.
func TestBoundRelationshipDelete_EligibilityGuards(t *testing.T) {
	cases := []struct {
		name      string
		segment   string
		deleteVar string
	}{
		{"unparseable with/limit segment", `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WITH rel SKIP 1`, "rel"},
		{"limit base not simple", `
MATCH (a) WITH a
MATCH (a)-[rel]->(b)
WITH rel LIMIT 1`, "rel"},
		{"source clause has no variable", `
MATCH ()
MATCH (source_repo)-[rel]->(b)`, "rel"},
		{"relationship pattern is chained", `
MATCH (source_repo)
MATCH (source_repo)-[rel]->(b)-[other]->(c)`, "rel"},
		{"start variable not the source", `
MATCH (source_repo)
MATCH (other)-[rel]->(b)`, "rel"},
		{"relationship variable is not the delete var", `
MATCH (source_repo)
MATCH (source_repo)-[other]->(b)`, "rel"},
		{"variable length relationship", `
MATCH (source_repo)
MATCH (source_repo)-[rel*1..2]->(b)`, "rel"},
	}
	exec := seedBoundDeleteFixture(t, 1)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixtureAdmission1590Context, fixtureAdmission1590 := statementTestContext(context.Background(), "")
			_ = fixtureAdmission1590Context
			res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1590, fixtureAdmission1590Context, tc.segment, "", tc.deleteVar, false)
			require.NoError(t, err)
			require.False(t, ok, "fast path should decline this shape")
			require.Nil(t, res)
		})
	}
}

// TestBoundRelationshipDelete_DirectionVariants exercises the incoming and
// both-direction candidate-edge branches.
func TestBoundRelationshipDelete_DirectionVariants(t *testing.T) {
	t.Run("incoming", func(t *testing.T) {
		exec := seedBoundDeleteFixture(t, 1)
		seg := `
MATCH (target:Repository {id:'repository:target-a'})
MATCH (target)<-[rel:DEPLOYS_FROM]-(:Repository)`
		fixtureAdmission1591Context, fixtureAdmission1591 := statementTestContext(context.Background(), "")
		_ = fixtureAdmission1591Context
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1591, fixtureAdmission1591Context, seg, "", "rel", false)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, res.Stats.RelationshipsDeleted)
	})
	t.Run("both", func(t *testing.T) {
		exec := seedBoundDeleteFixture(t, 1)
		seg := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]-(:Repository)`
		fixtureAdmission1592Context, fixtureAdmission1592 := statementTestContext(context.Background(), "")
		_ = fixtureAdmission1592Context
		res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1592, fixtureAdmission1592Context, seg, "", "rel", false)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 1, res.Stats.RelationshipsDeleted)
	})
}

// TestBoundRelationshipDelete_NamedEndNodeWithWhere covers the branch that binds
// a named end-node variable into the WHERE evaluation path.
func TestBoundRelationshipDelete_NamedEndNodeWithWhere(t *testing.T) {
	exec := seedBoundDeleteFixture(t, 1)
	seg := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(dest:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'`
	fixtureAdmission1593Context, fixtureAdmission1593 := statementTestContext(context.Background(), "")
	_ = fixtureAdmission1593Context
	res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1593, fixtureAdmission1593Context, seg, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, res.Stats.RelationshipsDeleted)
}

// TestBoundRelationshipDelete_SelfLoopBothDirectionDedup covers the seen-edge
// dedup branch: a self-loop edge appears in both the outgoing and incoming
// candidate sets for a both-direction match and must be deleted once.
func TestBoundRelationshipDelete_SelfLoopBothDirectionDedup(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:self'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (s:Repository {id:'repository:self'})
CREATE (s)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(s)
`, nil)
	require.NoError(t, err)

	seg := `
MATCH (source_repo:Repository {id:'repository:self'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]-(:Repository)`
	fixtureAdmission1193Context, fixtureAdmission1193 := statementTestContext(ctx, "")
	_ = fixtureAdmission1193Context
	res, ok, err := exec.tryExecuteBoundRelationshipDelete(fixtureAdmission1193, fixtureAdmission1193Context, seg, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, res.Stats.RelationshipsDeleted, "self-loop edge must be deleted exactly once")
}

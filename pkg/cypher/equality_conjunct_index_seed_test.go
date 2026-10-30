// TestBug_EqualityConjunctDoesNotIndexSeed reproduces and locks in the fix
// for a performance bug where a node MATCH whose WHERE clause is a
// conjunction containing an equality on an indexed property never
// index-seeds, even though the same equality alone does.
//
// Discovered: 2026-09-20, while diagnosing eshu-hq/eshu#6822 (a per-generation
// existence probe shaped
// `MATCH (n:Function) WHERE n.repo_id = $repo_id AND
//
//	n.evidence_source = 'projector/canonical' AND n.generation_id <>
//	$generation_id WITH n ORDER BY elementId(n) LIMIT 1
//	RETURN elementId(n) AS __id` costs a full label hydration scan on every
//
// generation for large labels).
//
// Impact: `WHERE repo_id = $r AND <anything>` is correct but hydrates every
// node of the label to find the handful in repo $r. The WITH-shaped probe is
// worse: executeMatchWithClause loads the label via
// loadNodesWithTemporalViewport unconditionally — no index attempt exists on
// that path at all.
//
// Root-Cause: parseSimpleIndexedEquality (match_index_seek.go) rejects every
// clause containing a top-level AND/OR/<>, so a conjunctive predicate never
// reaches tryCollectNodesFromPropertyIndex. The compound helpers added for
// id-equality (tryCollectNodesFromIDEqualityCompound) and IN-list
// (tryCollectNodesFromPropertyIndexInCompound) conjuncts have no
// plain-equality equivalent, and neither the single-clause chain (match.go)
// nor the WITH path (match_with.go) wires one in.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedEqualityConjunctPopulation creates `repos` x `perRepo` :FnProbe nodes
// with {uid, repo_id, evidence_source, generation_id}, indexed via
// CREATE INDEX on repo_id, plus one stale node in repo "repo-stale" (old
// generation) and one current-generation node in repo "repo-current".
func seedEqualityConjunctPopulation(t testing.TB, exec *StorageExecutor, repos, perRepo int) {
	t.Helper()
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE INDEX fnprobe_repo_id IF NOT EXISTS FOR (n:FnProbe) ON (n.repo_id)", nil)
	require.NoError(t, err)

	for r := 0; r < repos; r++ {
		for i := 0; i < perRepo; i++ {
			_, err := exec.Execute(ctx,
				`CREATE (:FnProbe {uid: $uid, repo_id: $repo, evidence_source: 'projector/canonical', generation_id: 'gen-2'})`,
				map[string]interface{}{
					"uid":  fmt.Sprintf("fn-%04d-%04d", r, i),
					"repo": fmt.Sprintf("repo-%04d", r),
				})
			require.NoErrorf(t, err, "seed node %d/%d", r, i)
		}
	}
	_, err = exec.Execute(ctx,
		`CREATE (:FnProbe {uid: 'stale-1', repo_id: 'repo-stale', evidence_source: 'projector/canonical', generation_id: 'gen-1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		`CREATE (:FnProbe {uid: 'current-1', repo_id: 'repo-current', evidence_source: 'projector/canonical', generation_id: 'gen-2'})`, nil)
	require.NoError(t, err)
}

const equalityConjunctProbe = `MATCH (n:FnProbe)
WHERE n.repo_id = $repo_id AND n.evidence_source = 'projector/canonical' AND n.generation_id <> $generation_id
WITH n ORDER BY elementId(n) LIMIT 1
RETURN elementId(n) AS __id`

// withEqualityParams injects query params into ctx the way Execute does, for
// direct helper calls that resolve $param references from context.
func withEqualityParams(ctx context.Context, params map[string]interface{}) context.Context {
	return context.WithValue(ctx, paramsKey, params)
}

// TestBug_EqualityConjunctDoesNotIndexSeed pins the correctness contract: a
// conjunctive existence probe must return the same rows whether or not the
// index-seek fast path fires. This is the safety net for the perf fix — it
// must not change results, only the seed source.
func TestBug_EqualityConjunctDoesNotIndexSeed(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedEqualityConjunctPopulation(t, exec, 4, 25)

	ctx := context.Background()
	tests := []struct {
		name   string
		params map[string]interface{}
		want   int
	}{
		{"zero-match repo", map[string]interface{}{"repo_id": "repo-no-such", "generation_id": "gen-2"}, 0},
		{"stale present", map[string]interface{}{"repo_id": "repo-stale", "generation_id": "gen-2"}, 1},
		{"current only", map[string]interface{}{"repo_id": "repo-current", "generation_id": "gen-2"}, 0},
		{"populated repo all current", map[string]interface{}{"repo_id": "repo-0000", "generation_id": "gen-2"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, equalityConjunctProbe, tt.params)
			require.NoError(t, err)
			require.Len(t, res.Rows, tt.want, "probe rows for %v", tt.params)
		})
	}
}

// TestBug_EqualityConjunctDoesNotIndexSeed_NoLabelScan is the scan-budget
// proof: the conjunctive probe must not hydrate the label. It fails before
// the fix (full GetNodesByLabel scan) and passes after (index seed +
// residual Go filter).
func TestBug_EqualityConjunctDoesNotIndexSeed_NoLabelScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedEqualityConjunctPopulation(t, exec, 4, 25)
	wrapped.reset()

	ctx := context.Background()
	res, err := exec.Execute(ctx, equalityConjunctProbe,
		map[string]interface{}{"repo_id": "repo-0000", "generation_id": "gen-2"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)

	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"conjunctive probe leaked %d GetNodesByLabel() calls — index seed missing",
		wrapped.GetNodesByLabelCalls())
	require.Zerof(t, wrapped.AllNodesCalls(),
		"conjunctive probe leaked %d AllNodes() calls — index seed missing",
		wrapped.AllNodesCalls())
}

// TestBug_EqualityConjunctDoesNotIndexSeed_NoIndex pins the fallback: without
// the property index the same probe stays correct via the scan path.
func TestBug_EqualityConjunctDoesNotIndexSeed_NoIndex(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		`CREATE (:FnProbe {uid: 'stale-1', repo_id: 'repo-stale', evidence_source: 'projector/canonical', generation_id: 'gen-1'})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, equalityConjunctProbe,
		map[string]interface{}{"repo_id": "repo-stale", "generation_id": "gen-2"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	res, err = exec.Execute(ctx, equalityConjunctProbe,
		map[string]interface{}{"repo_id": "repo-no-such", "generation_id": "gen-2"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}

// TestBug_EqualityConjunctDoesNotIndexSeed_MultiLabel pins label parity: the
// index seed checks labels with ANY semantics, so a multi-label pattern must
// still enforce ALL labels. A node carrying only one of the two labels must
// be excluded even though the index seed returns it.
func TestBug_EqualityConjunctDoesNotIndexSeed_MultiLabel(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE INDEX fnprobe_repo_id IF NOT EXISTS FOR (n:FnProbe) ON (n.repo_id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		`CREATE (:FnProbe:FnExtra {uid: 'both-1', repo_id: 'repo-multi', evidence_source: 'projector/canonical', generation_id: 'gen-1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		`CREATE (:FnProbe {uid: 'single-1', repo_id: 'repo-multi', evidence_source: 'projector/canonical', generation_id: 'gen-1'})`, nil)
	require.NoError(t, err)
	wrapped.reset()

	res, err := exec.Execute(ctx,
		`MATCH (n:FnProbe:FnExtra)
WHERE n.repo_id = $repo_id AND n.evidence_source = 'projector/canonical' AND n.generation_id <> $generation_id
WITH n ORDER BY elementId(n) LIMIT 10
RETURN n.uid AS uid`,
		map[string]interface{}{"repo_id": "repo-multi", "generation_id": "gen-2"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "both-1", res.Rows[0][0])

	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"multi-label conjunctive probe leaked %d GetNodesByLabel() calls",
		wrapped.GetNodesByLabelCalls())
}

// TestTryCollectNodesFromPropertyIndexEqualityCompound directly unit-tests
// the new helper's branches: empty clause, whole-clause simple equality,
// AND-conjunct param and literal forms, first-indexed-conjunct wins,
// no-indexed-conjunct, missing index, and the empty-seed-set case.
// Direct calls are used — rather than routing everything through
// exec.Execute — because the param-taking equality branches resolve $param
// references from context, which the ctx-injection helper reproduces
// exactly as Execute wires it.
func TestTryCollectNodesFromPropertyIndexEqualityCompound(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedEqualityConjunctPopulation(t, exec, 2, 10)

	ctx := context.Background()
	pattern := nodePatternInfo{labels: []string{"FnProbe"}, variable: "n"}

	t.Run("empty clause", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(ctx, pattern, "")
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, nodes)
	})

	t.Run("simple equality still seeds", func(t *testing.T) {
		pctx := withEqualityParams(ctx, map[string]interface{}{"repo_id": "repo-0000"})
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			pctx, pattern, "n.repo_id = $repo_id")
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 10)
	})

	t.Run("AND-conjunct param equality seeds", func(t *testing.T) {
		pctx := withEqualityParams(ctx, map[string]interface{}{"repo_id": "repo-0001", "generation_id": "gen-2"})
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			pctx, pattern,
			"n.repo_id = $repo_id AND n.evidence_source = 'projector/canonical' AND n.generation_id <> $generation_id")
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 10)
	})

	t.Run("AND-conjunct literal equality seeds", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			ctx, pattern,
			"n.evidence_source = 'projector/canonical' AND n.generation_id <> 'gen-2' AND n.repo_id = 'repo-0000'")
		require.NoError(t, err)
		require.True(t, used, "literal equality conjunct on an indexed property must seed")
		require.Len(t, nodes, 10)
	})

	t.Run("non-indexed equality conjunct is skipped", func(t *testing.T) {
		pctx := withEqualityParams(ctx, map[string]interface{}{"repo_id": "repo-0000"})
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			pctx, pattern,
			"n.uid = 'fn-0000-0000' AND n.repo_id = $repo_id")
		require.NoError(t, err)
		require.True(t, used, "second conjunct on an indexed property must seed")
		require.Len(t, nodes, 10)
	})

	t.Run("empty conjunct term is skipped", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			ctx, pattern, "() AND n.repo_id = 'repo-0000'")
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 10)
	})

	t.Run("no indexed conjunct falls back", func(t *testing.T) {
		pctx := withEqualityParams(ctx, map[string]interface{}{"generation_id": "gen-2"})
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			pctx, pattern, "n.generation_id <> $generation_id AND n.uid <> 'x'")
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, nodes)
	})

	t.Run("no match still reports used", func(t *testing.T) {
		pctx := withEqualityParams(ctx, map[string]interface{}{"repo_id": "repo-no-such"})
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexEqualityCompound(
			pctx, pattern, "n.repo_id = $repo_id AND n.generation_id <> 'gen-2'")
		require.NoError(t, err)
		require.True(t, used)
		require.Empty(t, nodes)
	})
}

// BenchmarkEqualityConjunctProbeMatch measures the conjunctive existence
// probe over a 50k-node label spread across 50 repos — the shape from the
// bug repro (`WHERE repo_id = $r AND ...`), read-only so repeated b.N
// iterations are apples-to-apples.
//
// Population is 50k rather than 5k: at 5k an in-process MemoryEngine scan is
// cheap enough that the O(n) vs O(repo) shape change is inside measurement
// noise (the same sizing note as BenchmarkInListAnchoredRelMatch). Before
// this fix, this hydrates all 50000 :FnProbe nodes every iteration; after
// the fix it seeks the 1000-entry property index group for the probed repo
// instead.
func BenchmarkEqualityConjunctProbeMatch(b *testing.B) {
	exec, _ := newCountingExecutor(b)
	seedEqualityConjunctPopulation(b, exec, 50, 1000)
	params := map[string]interface{}{"repo_id": "repo-0007", "generation_id": "gen-2"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, equalityConjunctProbe, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != 0 {
			b.Fatalf("expected 0 rows, got %d", len(res.Rows))
		}
	}
}

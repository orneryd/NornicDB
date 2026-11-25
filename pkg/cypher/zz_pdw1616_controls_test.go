package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pdw1616ReadTrace struct {
	*storage.NamespacedEngine

	mu                                            sync.Mutex
	memo                                          map[string]storage.CountStoreRead
	countCalls, countMisses, batchCalls, batchIDs int
	uniqueIDs                                     map[storage.NodeID]struct{}
	batchWall                                     time.Duration
}

func (s *pdw1616ReadTrace) ReadCountStore(ctx context.Context, r storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countCalls++
	key := fmt.Sprintf("%d/%s/%s/%q/%s", r.Kind, r.Namespace, r.Name, r.Types, r.HiddenLabelPrefix)
	if r.Scope == nil && s.memo != nil {
		if answer, ok := s.memo[key]; ok {
			return answer, nil
		}
	}
	s.countMisses++
	answer, err := s.NamespacedEngine.ReadCountStore(ctx, r)
	if err == nil && r.Scope == nil && s.memo != nil {
		s.memo[key] = answer
	}
	return answer, err
}

func (s *pdw1616ReadTrace) BatchGetNodesWithoutEmbeddings(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	start := time.Now()
	nodes, err := s.NamespacedEngine.BatchGetNodesWithoutEmbeddings(ctx, ids)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchCalls++
	s.batchIDs += len(ids)
	s.batchWall += time.Since(start)
	for _, id := range ids {
		s.uniqueIDs[id] = struct{}{}
	}
	return nodes, err
}

func (s *pdw1616ReadTrace) reportAndReset(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("PDW1616 trace case=%s countCalls=%d countMisses=%d batchCalls=%d batchIDs=%d uniqueIDs=%d sumBatchWall=%v",
		name, s.countCalls, s.countMisses, s.batchCalls, s.batchIDs, len(s.uniqueIDs), s.batchWall)
	s.countCalls, s.countMisses, s.batchCalls, s.batchIDs, s.batchWall = 0, 0, 0, 0, 0
	clear(s.uniqueIDs)
}

func TestPDW1616SearchAndInputsControls(t *testing.T) {
	t.Run("Search", pdw1616SearchControls)
	t.Run("Inputs", pdw1616InputsControls)
}

func pdw1616SearchControls(t *testing.T) {
	f := pdw1614Build(t)
	base := f.store.(*storage.NamespacedEngine)
	store := &pdw1616ReadTrace{NamespacedEngine: base, uniqueIDs: make(map[storage.NodeID]struct{})}
	p := &pdw1616Probe{t: t, store: store}
	ctx := t.Context()
	nodes, err := base.GetNodesByLabel(ctx, "PDSearchSource")
	require.NoError(t, err)
	for _, n := range nodes {
		n.ChunkEmbeddings = [][]float32{{1, 0}}
		require.NoError(t, base.UpdateNode(ctx, storage.NodeUpdateRequest{After: n, Policy: storage.StoredNodeImages}))
	}
	svc := search.NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx))
	svc.SetSearchResultCachePolicy(0, 0)
	svc.SetReranker(pdw1616Ranker{})
	makeExec := func() *StorageExecutor {
		e := p.executor()
		e.SetSearchService(svc)
		e.SetEmbedder(rerankTruthfulnessEmbedder{chunks: []string{"lease renewal schedule"}})
		return e
	}
	q := pdw1616Stock["search"].text
	group := regexp.MustCompile("(?s)CALL db\\.retrieve\\(.*?YIELD node AS member, score AS member_score, matches AS member_matches").FindString(q) + " RETURN member.id AS id"
	bind := f.searchParams(30, true)
	delete(bind, "zz_members")
	delete(bind, "zz_page")
	run := func(name, text string, params map[string]any, memo bool) *ExecuteResult {
		t.Helper()
		if memo {
			store.memo = make(map[string]storage.CountStoreRead)
		} else {
			store.memo = nil
		}
		r, err := p.profile(name, makeExec(), text, params, 2)
		store.reportAndReset(t, name)
		require.NoError(t, err)
		return r
	}
	groups := run("controls/search/group-quiet", group, bind, false)
	require.Len(t, groups.Rows, 250)
	first := run("controls/search/full-quiet", q, bind, false)
	memo := run("controls/search/full-count-memo", q, bind, true)
	withoutQID := func(r *ExecuteResult) [][]any {
		for i, col := range r.Columns {
			if col == "qid" {
				for _, row := range r.Rows {
					row[i] = nil
				}
				return r.Rows
			}
		}
		t.Fatal("search qid column absent")
		return nil
	}
	require.ElementsMatch(t, withoutQID(first), withoutQID(memo))
	p.withWriters(f, "search-controls", 2, func() {
		live := run("controls/search/group-writers", group, bind, false)
		require.Len(t, live.Rows, 250)
	})
	after := run("controls/search/group-after", group, bind, false)
	require.Len(t, after.Rows, 250)

	// The original span projection indexes stored string-list properties.
	// Bind those same properties once per correlated call for the control.
	projected := pdw1614SearchWithoutRetrieval(t, q)
	projectedBound := projected
	for _, pair := range [][2]string{{"node", "matches"}, {"member", "member_matches"}} {
		receiver, matches := pair[0], pair[1]
		line := fmt.Sprintf("CALL (%s, %s) {\n", receiver, matches)
		part := fmt.Sprintf("  UNWIND coalesce(%s, []) AS m\n  WITH %s, m, %s,\n", matches, receiver, matches)
		require.Contains(t, projectedBound, line+part)
		projectedBound = strings.Replace(projectedBound, line+part, line+
			fmt.Sprintf("  WITH %s, %s, %s.section_roles AS zz_roles, %s.section_content_keys AS zz_keys\n", receiver, matches, receiver, receiver)+
			strings.Replace(part, ", m, "+matches+",", ", m, "+matches+", zz_roles, zz_keys,", 1), 1)
		placed := fmt.Sprintf("  WITH %s, m, placed, %s,\n", receiver, matches)
		projectedBound = strings.Replace(projectedBound, placed, strings.TrimSuffix(placed, "\n")+" zz_roles, zz_keys,\n", 1)
		projectedBound = strings.ReplaceAll(projectedBound, receiver+".section_roles[i]", "zz_roles[i]")
		projectedBound = strings.ReplaceAll(projectedBound, receiver+".section_content_keys[i]", "zz_keys[i]")
	}
	require.NotEqual(t, projected, projectedBound)
	r, err := p.profile("controls/search/spans-stock", makeExec(), projected, f.searchParams(30, true), 2)
	if err != nil {
		t.Logf("PDW1616 synthetic search span control unavailable: %v", err)
		return
	}
	viaAliases := run("controls/search/spans-bound", projectedBound, f.searchParams(30, true), false)
	require.ElementsMatch(t, withoutQID(r), withoutQID(viaAliases))
}

func pdw1616InputsControls(t *testing.T) {
	f := pdw1614Build(t)
	p := &pdw1616Probe{t: t, store: f.store}
	p.addInputEdges(0, 10)
	q := pdw1616Stock["inputs"].text
	var ids []any
	for i := 0; i < 107; i++ {
		ids = append(ids, fmt.Sprintf("doc-%04d", i))
	}
	bind := map[string]any{"ids": ids}
	measure := func(name, query string) *ExecuteResult {
		t.Helper()
		r, err := p.profile(name, p.executor(), query, bind, 2)
		require.NoError(t, err)
		require.Len(t, r.Rows, len(ids))
		return r
	}
	before := measure("controls/inputs/before-fanout", q)
	pdw1614Exec(t, f.store, "CREATE (:PDGraphNode:PDSummary {id:'padded', document_id:'unrelated'})", nil)
	var pads []any
	for i := 0; i < f.sizes.shared; i++ {
		pads = append(pads, fmt.Sprintf("shared-%03d", i))
	}
	pdw1614Batches(t, f.store, "UNWIND $pads AS id MATCH (s:PDGraphNode {id:id}), (noise:PDGraphNode {id:'padded'}) "+
		"UNWIND range(1,80) AS i CREATE (s)-[:PD_RELATION {id:id + ':pad:' + toString(i), kind:'involves', eff_usable:true}]->(noise)",
		"pads", pads, 15)
	after := measure("controls/inputs/after-24000-filtered-edges", q)
	require.ElementsMatch(t, before.Rows, after.Rows)

	const full = "MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode)\n  WHERE own.document_id = document.id AND NOT shared:PDDefinition"
	const split = "MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)\n  WHERE own.document_id = document.id\n  MATCH (own)-[l2:PD_RELATION]-(shared:PDGraphNode)\n  WHERE l2 <> l AND NOT shared:PDDefinition"
	pushed := strings.Replace(q, full, split, 1)
	require.NotEqual(t, q, pushed)
	earlyFilter := measure("controls/inputs/after-early-own-filter", pushed)
	require.ElementsMatch(t, before.Rows, earlyFilter.Rows)
}

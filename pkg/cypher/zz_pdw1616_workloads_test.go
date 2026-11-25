package cypher

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pdw1616Queries map[string]struct {
	wire string
	text string
}

const pdw1616InputPage = 174

func TestPDW1616Workloads(t *testing.T) {
	for name, q := range pdw1616Stock {
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(q.text)))
		require.Equal(t, q.wire, hash[:16], name)
		t.Logf("PDW1616 exact name=%s wire=%s bytes=%d GOMAXPROCS=%d NumCPU=%d", name, hash[:16], len(q.text), runtime.GOMAXPROCS(0), runtime.NumCPU())
	}
	f := pdw1614Build(t)
	probe := &pdw1616Probe{t: t, store: f.store}
	t.Run("Evaluate", func(t *testing.T) {
		probe.t = t
		probe.evaluate(f)
	})
	t.Run("Inputs", func(t *testing.T) {
		probe.t = t
		probe.inputs(f)
	})
	t.Run("Search", func(t *testing.T) {
		probe.t = t
		probe.search(f)
	})
	t.Run("Mark", func(t *testing.T) {
		probe.t = t
		probe.mark(f)
	})
}

type pdw1616Probe struct {
	t     *testing.T
	store storage.Engine
	nonce int64
}

func (p *pdw1616Probe) executor() *StorageExecutor {
	return NewStorageExecutor(p.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func (p *pdw1616Probe) profile(name string, exec *StorageExecutor, q string, params map[string]any, repeats int) (*ExecuteResult, error) {
	p.t.Helper()
	var result *ExecuteResult
	var err error
	var walls []time.Duration
	w := pdw1614Measure(p.t, func() {
		for i := 0; i < repeats; i++ {
			bind := maps.Clone(params)
			p.nonce++
			bind["pdw1616_nonce"] = p.nonce
			pprof.Do(p.t.Context(), pprof.Labels("pdw", "stmt"), func(ctx context.Context) {
				start := time.Now()
				result, err = exec.Execute(ctx, q, bind)
				walls = append(walls, time.Since(start).Round(time.Microsecond))
			})
			if err != nil {
				break
			}
		}
	})
	p.t.Logf("PDW1616 case=%s wall=%v nonce=%d err=%v", name, walls, p.nonce, err)
	w.log(p.t, name, len(walls), true)
	p.t.Logf("PDW1616 case=%s storage-cum{%s}", name, pdw1614StorageCum(w, 25))
	return result, err
}

func pdw1616Command(t *testing.T, exec *StorageExecutor, q string, bind map[string]any) *ExecuteResult {
	t.Helper()
	r, err := exec.Execute(t.Context(), q, bind)
	require.NoError(t, err)
	return r
}

func (p *pdw1616Probe) withWriters(f *pdw1614Fixture, name string, n int, fn func()) {
	p.t.Helper()
	load := f.startWriters(n, true, name)
	defer load.halt(p.t, name)
	deadline := time.Now().Add(5 * time.Second)
	for load.commits.Load() < int64(n) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Positive(p.t, load.commits.Load(), "writers must be active before query")
	fn()
}

func pdw1616ListCount(v any) int {
	list, _ := v.([]any)
	return len(list)
}

func (p *pdw1616Probe) evaluate(f *pdw1614Fixture) {
	q := pdw1616Stock["evaluate"].text
	var extra []any
	for i := 0; len(f.seeds) < 2712; i++ {
		doc := i % 5
		from := fmt.Sprintf("ent-%04d-%03d", doc, i%50)
		to := fmt.Sprintf("shared-%03d", i%f.sizes.shared)
		id := fmt.Sprintf("pdw-extra-%04d", i)
		f.seeds = append(f.seeds, map[string]any{"from": from, "link": id})
		extra = append(extra, pdw1614Link(id, from, to, fmt.Sprintf("doc-%04d", doc)))
	}
	pdw1614Batches(p.t, p.store, pdw1614CreateLinks, "links", extra, 500)
	bind := f.evaluateParams()
	bind["selected"] = []any{}
	check := func(name string, exec *StorageExecutor, bindings map[string]any, want int) *ExecuteResult {
		p.t.Helper()
		r, err := p.profile(name, exec, q, bindings, 2)
		require.NoError(p.t, err)
		require.Len(p.t, r.Rows, 1)
		p.t.Logf("PDW1616 %s seeds=%d output=%d", name, len(bindings["links"].([]any)), pdw1616ListCount(r.Rows[0][0]))
		require.Equal(p.t, want, pdw1616ListCount(r.Rows[0][0]))
		return r
	}
	half := maps.Clone(bind)
	half["links"] = f.seeds[:1356]
	check("evaluate/quiet/half", p.executor(), half, 1356)
	res := check("evaluate/quiet/full", p.executor(), bind, 2712)
	// Same list order and value, built in one flatten instead of growing
	// the left operand on every iteration.
	flattened := strings.Replace(q, "reduce(effl_all = effl_gone_documents, effl_x IN effl_rows\n                                            | effl_all + effl_x.documents)", "effl_gone_documents + apoc.coll.flatten([effl_x IN effl_rows | effl_x.documents])", 1)
	require.NotEqual(p.t, q, flattened)
	control, err := p.profile("evaluate/quiet/full-flatten", p.executor(), flattened, bind, 2)
	require.NoError(p.t, err)
	require.Equal(p.t, res.Rows, control.Rows)

	p.withWriters(f, "eval-changed", 2, func() {
		check("evaluate/writers/full-auto", p.executor(), bind, 2712)
		other := p.executor()
		pdw1616Command(p.t, other, "BEGIN", nil)
		check("evaluate/writers/full-explicit", other, bind, 2712)
		pdw1616Command(p.t, other, "ROLLBACK", nil)
	})
	applied := make([]any, 0, 2712)
	for _, row := range res.Rows[0][0].([]any) {
		m := row.(map[string]any)
		applied = append(applied, map[string]any{"from": m["from"], "link": m["link"], "inputs": m["inputs"], "base": m["base"], "disputed": m["disputed"], "reason": m["reason"]})
	}
	pdw1614Batches(p.t, p.store, "UNWIND $rows AS row MATCH (s:PDGraphNode {id: row.from})-[r:PD_RELATION]->() WHERE r.id = row.link "+
		"SET r.eff_base = row.base, r.eff_disputed = row.disputed, r.eff_usable = row.base AND NOT row.disputed, r.eff_reason = row.reason, "+
		"r.eff_applied = apoc.convert.toJson([row.inputs, row.base, row.disputed, row.base AND NOT row.disputed, row.reason]) RETURN count(r) AS applied", "rows", applied, 500)
	check("evaluate/quiet/settled-auto", p.executor(), bind, 0)
	fresh := p.executor()
	pdw1616Command(p.t, fresh, "BEGIN", nil)
	check("evaluate/quiet/settled-fresh-tx", fresh, bind, 0)
	pdw1616Command(p.t, fresh, "ROLLBACK", nil)
	old := p.executor()
	pdw1616Command(p.t, old, "BEGIN", nil)
	pdw1616Command(p.t, old, "MATCH (d:PDDocument {id: 'doc-0000'}) RETURN d.id", nil)
	for i := 0; i < 25; i++ {
		f.writeOne(p.t, "eval-stale", i, false)
	}
	check("evaluate/quiet/settled-old-tx-after-25-commits", old, bind, 0)
	pdw1616Command(p.t, old, "ROLLBACK", nil)
	check("evaluate/quiet/settled-auto-after-25-commits", p.executor(), bind, 0)
}

func (p *pdw1616Probe) addInputEdges(lo, hi int) {
	p.t.Helper()
	var rows []any
	for i := 0; i < pdw1616InputPage; i++ {
		doc := fmt.Sprintf("doc-%04d", i)
		src := fmt.Sprintf("src-%04d", i)
		for j := lo; j < hi; j++ {
			rows = append(rows, map[string]any{
				"doc": doc, "src": src, "own": fmt.Sprintf("ent-%04d-%03d", i, j),
				"shared": fmt.Sprintf("shared-%03d", (i*13+j)%300),
				"cite":   fmt.Sprintf("src-%04d", (i+j+1)%556),
				"link":   fmt.Sprintf("input-%04d-%02d", i, j),
			})
		}
	}
	pdw1614Batches(p.t, p.store, "UNWIND $rows AS row MATCH (d:PDDocument {id: row.doc}) MATCH (o:PDGraphNode {id: row.own}) "+
		"MATCH (s:PDGraphNode {id: row.shared}) SET o.source_id = row.src, o.cite_start = 0, o.cite_end = 50, s:PDIdentity "+
		"CREATE (d)-[:PD_RELATION {id: row.link + ':direct', kind:'involves', eff_usable:true, source_id:row.src, cite_start:0, cite_end:60}]->(s) "+
		"CREATE (d)-[:PD_RELATION {id: row.link + ':own', kind:'involves', eff_usable:true, source_id:row.src, cite_start:0, cite_end:50}]->(o) "+
		"CREATE (o)-[:PD_RELATION {id: row.link + ':item', kind:'involves', eff_usable:true, source_id:row.cite, cite_start:0, cite_end:50}]->(s) "+
		"CREATE (o)-[:PD_BINDS {status:'confirmed'}]->(s)", "rows", rows, 300)
}

func (p *pdw1616Probe) inputs(f *pdw1614Fixture) {
	q := pdw1616Stock["inputs"].text
	var ids []any
	for i := 0; i < pdw1616InputPage; i++ {
		ids = append(ids, fmt.Sprintf("doc-%04d", i))
	}
	check := func(name string, ids []any) {
		p.t.Helper()
		r, err := p.profile(name, p.executor(), q, map[string]any{"ids": ids}, 2)
		require.NoError(p.t, err)
		require.Len(p.t, r.Rows, len(ids))
		var candidates [3]int
		var sections int
		for _, row := range r.Rows {
			branches := row[1].([]any)
			require.Len(p.t, branches, len(candidates))
			for i, branch := range branches {
				candidates[i] += pdw1616ListCount(branch)
			}
			sections += pdw1616ListCount(row[2])
		}
		p.t.Logf("PDW1616 %s docs=%d candidates=%v citedSources=%d", name, len(ids), candidates, sections)
	}
	p.addInputEdges(0, 2)
	check("inputs/early/quiet", ids)
	p.withWriters(f, "inputs-early", 2, func() { check("inputs/early/writers", ids) })
	check("inputs/early/quiet-after-writers", ids)
	p.addInputEdges(2, 10)
	check("inputs/settled/quiet", ids)
	check("inputs/settled/quiet-107", ids[:107])
	p.withWriters(f, "inputs-settled", 2, func() { check("inputs/settled/writers", ids) })
	check("inputs/settled/quiet-after-writers", ids)
	f.reopen()
	p.store = f.store
	check("inputs/settled/quiet-after-reopen", ids)
}

type pdw1616Ranker struct{}

func (pdw1616Ranker) Name() string                     { return "pdw1616" }
func (pdw1616Ranker) Enabled() bool                    { return true }
func (pdw1616Ranker) IsAvailable(context.Context) bool { return true }
func (pdw1616Ranker) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	out := make([]search.RerankResult, len(candidates))
	for i, c := range candidates {
		out[i] = search.RerankResult{ID: c.ID, OriginalRank: i + 1, NewRank: i + 1, FinalScore: 1 - float64(i)/float64(len(candidates)+1)}
	}
	return out, nil
}

func (p *pdw1616Probe) search(f *pdw1614Fixture) {
	q := pdw1616Stock["search"].text
	ctx := p.t.Context()
	nodes, err := p.store.GetNodesByLabel(ctx, "PDSearchSource")
	require.NoError(p.t, err)
	for i, n := range nodes {
		n.ChunkEmbeddings = [][]float32{{1, float32(i%50) / 100}}
		require.NoError(p.t, p.store.UpdateNode(ctx, storage.NodeUpdateRequest{After: n, Policy: storage.StoredNodeImages}))
	}
	svc := search.NewService(p.store, 2, nil)
	p.t.Cleanup(func() { require.NoError(p.t, svc.Close()) })
	require.NoError(p.t, svc.BuildIndexes(ctx))
	svc.SetSearchResultCachePolicy(0, 0)
	svc.SetReranker(pdw1616Ranker{})
	makeExec := func() *StorageExecutor {
		exec := p.executor()
		exec.SetSearchService(svc)
		exec.SetEmbedder(rerankTruthfulnessEmbedder{chunks: []string{"lease renewal schedule"}})
		return exec
	}
	p.t.Logf("PDW1616 search indexedSources=%d", len(nodes))
	bind := f.searchParams(30, true)
	delete(bind, "zz_page")
	delete(bind, "zz_members")
	extract := func(proc, tail string) string {
		re := regexp.MustCompile("(?s)CALL db\\." + proc + "\\(.*?" + tail)
		found := re.FindString(q)
		require.NotEmpty(p.t, found)
		return found
	}
	rankOnly := extract("rretrieve", "YIELD page") + " RETURN size(page.results) AS hits"
	groupOnly := extract("retrieve", "YIELD node AS member, score AS member_score, matches AS member_matches") + " RETURN count(member) AS hits"
	run := func(name, text string, bindings map[string]any) bool {
		r, err := p.profile(name, makeExec(), text, bindings, 2)
		if err != nil {
			p.t.Logf("PDW1616 %s failed error=%v", name, err)
			return false
		}
		require.NotEmpty(p.t, r.Rows)
		if len(r.Columns) == 1 {
			p.t.Logf("PDW1616 %s counts=%v", name, r.Rows)
		} else {
			p.t.Logf("PDW1616 %s families=%d columns=%v", name, len(r.Rows), r.Columns)
		}
		return true
	}
	run("search/quiet/ranked-procedure", rankOnly, bind)
	run("search/quiet/group-procedure", groupOnly, bind)
	run("search/quiet/full", q, bind)
	projected := pdw1614SearchWithoutRetrieval(p.t, q)
	run("search/quiet/projection", projected, f.searchParams(30, true))
	p.withWriters(f, "search", 2, func() {
		run("search/writers/full", q, bind)
		run("search/writers/ranked-procedure", rankOnly, bind)
		run("search/writers/group-procedure", groupOnly, bind)
		run("search/writers/projection", projected, f.searchParams(30, true))
	})
}

func (p *pdw1616Probe) mark(f *pdw1614Fixture) {
	q := pdw1616Stock["mark"].text
	var items []any
	for i := 0; i < 333; i++ {
		doc := fmt.Sprintf("doc-%04d", i+180)
		prev, output := "old", "different"
		if i%3 == 0 {
			output = "new"
		}
		if i%9 == 8 {
			prev = "stale"
		}
		items = append(items, map[string]any{"id": doc, "previous": prev, "fingerprint": "new", "output": output, "root": i%3 == 0})
	}
	pdw1614Batches(p.t, p.store, "UNWIND $items AS i MATCH (d:PDDocument {id:i.id}) "+
		"SET d.summary_input='old', d.summary_output=i.output, d.summary_status='ready' "+
		"FOREACH (_ IN CASE WHEN i.root THEN [1] ELSE [] END | CREATE (:PDSummary {owner_kind:'document', owner_id:i.id, summary_kind:'document', current:true, input_fingerprint:'new'}))",
		"items", items, 100)
	check := func(name, query string, items []any, want int) {
		p.t.Helper()
		for i := 0; i < 2; i++ {
			exec := p.executor()
			pdw1616Command(p.t, exec, "BEGIN", nil)
			r, err := p.profile(name, exec, query, map[string]any{"items": items}, 1)
			require.NoError(p.t, err)
			end := time.Now()
			if i == 0 {
				verify := pdw1616Command(p.t, exec, "UNWIND $items AS i MATCH (d:PDDocument {id:i.id}) WHERE d.summary_input='new' RETURN count(d)", map[string]any{"items": items})
				require.Equal(p.t, [][]any{{int64(want)}}, verify.Rows)
			}
			pdw1616Command(p.t, exec, "ROLLBACK", nil)
			p.t.Logf("PDW1616 %s items=%d changed=%d propertiesSet=%d verify-and-rollback=%v", name, len(items), want, r.Stats.PropertiesSet, time.Since(end))
		}
	}
	check("mark/quiet/111", q, items[:111], 99)
	check("mark/quiet/333", q, items, 296)
	check("mark/quiet/no-eager-lock", strings.Replace(q, "SET n += {}\n", "", 1), items, 296)
	p.withWriters(f, "mark", 2, func() { check("mark/writers/333", q, items, 296) })
}

package cypher

// Part 36.5 and the SEARCH half of fixed row F7 (neutral): the first call of a
// long read statement in a fresh process, and later calls.
// In-memory store, namespace "kitchen", written through the storage API:
//   60 :Recipe nodes rc<i> {slug, title, body, published: true, vegetarian:
//   i%2 == 0, spicy: i%3 == 0} with 32 chunk vectors of 2,048 values ([1,0,...]
//   rotated by i%7); 10 :Cook nodes with :WROTE to rc<i> (cook i%10);
//   25 :Ingredient nodes, :USES {grams} from each recipe to 6 of them;
//   8 :Menu nodes, :IN_MENU from each recipe to menu i%8; 3 :Review nodes per
//   recipe with :RATED {..} and stars 1..5.
// Search service (2,048 dimensions) built before measuring; an embedder that
// returns [1,0,...] and keeps text as one chunk; a reranker that keeps order.
// Two long statements: SEARCH_LONG (with db.rretrieve inside a CALL ... UNION
// ALL) and BROWSE_LONG (the same enrichment, no procedure call).
// Measured: TotalAlloc and wall time around each call; allocation profile
// (rate 1) growth attributed to the first non-runtime frame of each sample.
// The order of calls is set by the test name (each test is run alone, so
// each starts in a fresh process).

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type nrpEmbedder struct{}

func (nrpEmbedder) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, 2048)
	v[0] = 1
	return v, nil
}
func (nrpEmbedder) ChunkText(s string, _ int, _ int) ([]string, error) { return []string{s}, nil }

type nrpReranker struct{}

func (nrpReranker) Name() string                       { return "neutral-order" }
func (nrpReranker) Enabled() bool                      { return true }
func (nrpReranker) IsAvailable(context.Context) bool   { return true }
func (nrpReranker) Rerank(_ context.Context, _ string, c []search.RerankCandidate) ([]search.RerankResult, error) {
	r := make([]search.RerankResult, len(c))
	for i, x := range c {
		r[i] = search.RerankResult{ID: x.ID, Content: x.Content, OriginalRank: i + 1, FinalScore: 1 - float64(i)/100}
	}
	return r, nil
}

const nrpEnrich = `
WITH hit, max(score) AS score, collect(DISTINCT origin) AS origins,
     head([m IN collect(meta) WHERE m IS NOT NULL]) AS meta
OPTIONAL MATCH (hit)<-[:WROTE]-(cook:Cook)
OPTIONAL MATCH (hit)-[u:USES]->(ing:Ingredient)
WITH hit, score, origins, meta, cook,
     collect(CASE WHEN ing IS NULL THEN null ELSE {name: ing.name, grams: u.grams, staple: coalesce(ing.staple, false)} END) AS ingredients
CALL (hit) {
  OPTIONAL MATCH (hit)-[:IN_MENU]->(menu:Menu)
  RETURN collect(menu.title)[0..5] AS menus, count(menu) AS menu_count
}
CALL (hit, cook) {
  OPTIONAL MATCH (cook)-[:WROTE]->(other:Recipe)
  WHERE other <> hit AND coalesce(other.published, false)
  WITH other ORDER BY other.slug
  RETURN collect(other.slug)[0..3] AS more_by_cook
}
WITH hit, score, origins, meta, cook, ingredients, menus, menu_count, more_by_cook,
     COLLECT { MATCH (hit)<-[:RATED]-(rv:Review) WHERE rv.stars >= 4 RETURN rv.stars } AS good,
     COLLECT { MATCH (hit)<-[:RATED]-(rv:Review) RETURN rv.stars } AS all_stars,
     [x IN ingredients WHERE x.grams > 100 | x.name] AS heavy,
     [x IN ingredients WHERE x.staple | x.name] AS staples
WITH hit, score, origins, meta, cook, ingredients, menus, menu_count, more_by_cook, heavy, staples,
     CASE WHEN size(all_stars) = 0 THEN null
          ELSE reduce(total = 0, s IN all_stars | total + s) * 1.0 / size(all_stars) END AS rating,
     size(good) AS good_reviews
ORDER BY CASE WHEN 'pinned' IN origins THEN 0 ELSE 1 END, score DESC, hit.slug
WITH collect({hit: hit, score: score, origins: origins, cook: cook, ingredients: ingredients,
              menus: menus, menu_count: menu_count, more: more_by_cook, heavy: heavy,
              staples: staples, rating: rating, good: good_reviews}) AS cards,
     head(collect(meta)) AS meta
UNWIND CASE WHEN size(cards) = 0 THEN [null] ELSE cards[0..$n] END AS card
RETURN card.hit.slug AS slug, card.hit.title AS title, card.score AS score,
       card.origins AS origins, meta.method AS method, meta.total AS total, meta.more AS more,
       {cook: card.cook.name, kitchen: card.cook.kitchen,
        ingredients: [x IN card.ingredients WHERE x IS NOT NULL][0..10],
        heavy: card.heavy, staples: card.staples, menus: card.menus, menu_count: card.menu_count,
        more_by_cook: card.more, rating: card.rating, good_reviews: card.good,
        flags: [f IN [CASE WHEN card.hit.vegetarian THEN 'vegetarian' END,
                      CASE WHEN card.hit.spicy THEN 'spicy' END,
                      CASE WHEN card.rating IS NOT NULL AND card.rating >= 4 THEN 'favourite' END]
                WHERE f IS NOT NULL],
        summary: CASE WHEN size(coalesce(card.hit.body, '')) > 80
                      THEN substring(card.hit.body, 0, 80) + '...'
                      ELSE card.hit.body END} AS detail
`

const nrpSearchLong = `
CALL () {
  UNWIND $pinned AS pin
  OPTIONAL MATCH (pinned:Recipe {slug: pin})
  WITH pinned WHERE pinned IS NOT NULL AND coalesce(pinned.published, false)
  RETURN pinned AS hit, 1.0 AS score, 'pinned' AS origin, null AS meta
  UNION ALL
  CALL db.rretrieve(apoc.map.fromPairs([option IN [['query', $query], ['types', ['Recipe']],
        ['where', 'coalesce(node.published, false) AND NOT coalesce(node.retired, false)'],
        ['n', $n], ['rerankTopK', $window], ['minSimilarity', $min_similarity]]
      WHERE option[1] IS NOT NULL]))
  YIELD page
  UNWIND CASE WHEN size(page.results) = 0 THEN [null] ELSE page.results END AS result
  RETURN result.node AS hit, result.score AS score, 'search' AS origin,
         {method: page.search_method, total: page.total, more: page.has_more} AS meta
}` + nrpEnrich

const nrpBrowseLong = `
CALL () {
  UNWIND $pinned AS pin
  OPTIONAL MATCH (pinned:Recipe {slug: pin})
  WITH pinned WHERE pinned IS NOT NULL AND coalesce(pinned.published, false)
  RETURN pinned AS hit, 1.0 AS score, 'pinned' AS origin, null AS meta
  UNION ALL
  MATCH (listed:Recipe) WHERE coalesce(listed.published, false) AND NOT coalesce(listed.retired, false)
  WITH listed ORDER BY listed.slug LIMIT $window
  RETURN listed AS hit, 0.5 AS score, 'browse' AS origin, {method: 'browse', total: $window, more: false} AS meta
}` + nrpEnrich

func nrpKitchen(t *testing.T) *StorageExecutor {
	t.Helper()
	s := nrpStore(t, false, "kitchen")
	e := nrpExecutor(t, s, true)
	for c := 0; c < 10; c++ {
		nrpNode(t, s, &storage.Node{ID: storage.NodeID(fmt.Sprintf("cook%d", c)), Labels: []string{"Cook"}, Properties: map[string]any{"name": fmt.Sprintf("Cook %d", c), "kitchen": fmt.Sprintf("k%d", c%3)}})
	}
	for g := 0; g < 25; g++ {
		nrpNode(t, s, &storage.Node{ID: storage.NodeID(fmt.Sprintf("ing%d", g)), Labels: []string{"Ingredient"}, Properties: map[string]any{"name": fmt.Sprintf("ingredient %d", g), "staple": g%4 == 0}})
	}
	for m := 0; m < 8; m++ {
		nrpNode(t, s, &storage.Node{ID: storage.NodeID(fmt.Sprintf("menu%d", m)), Labels: []string{"Menu"}, Properties: map[string]any{"title": fmt.Sprintf("Menu %d", m)}})
	}
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("rc%d", i)
		chunks := make([][]float32, 32)
		for k := range chunks {
			v := make([]float32, 2048)
			v[(i%7+k)%2048] = 1
			chunks[k] = v
		}
		nrpNode(t, s, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Recipe"}, Properties: map[string]any{
			"slug": id, "title": fmt.Sprintf("Recipe %d", i), "body": strings.Repeat("simmer the stock slowly and season to taste ", 4),
			"published": true, "vegetarian": i%2 == 0, "spicy": i%3 == 0}, ChunkEmbeddings: chunks})
		nrpEdge(t, s, fmt.Sprintf("wrote%d", i), fmt.Sprintf("cook%d", i%10), id, "WROTE", nil)
		for j := 0; j < 6; j++ {
			nrpEdge(t, s, fmt.Sprintf("uses%d-%d", i, j), id, fmt.Sprintf("ing%d", (i+j*4)%25), "USES", map[string]any{"grams": int64(50 + 30*j)})
		}
		nrpEdge(t, s, fmt.Sprintf("menu%d-%d", i, i%8), id, fmt.Sprintf("menu%d", i%8), "IN_MENU", nil)
		for r := 0; r < 3; r++ {
			rv := fmt.Sprintf("rv%d-%d", i, r)
			nrpNode(t, s, &storage.Node{ID: storage.NodeID(rv), Labels: []string{"Review"}, Properties: map[string]any{"stars": int64(1 + (i+r)%5)}})
			nrpEdge(t, s, "rated-"+rv, rv, id, "RATED", nil)
		}
	}
	svc := nrpService(t, s, 2048)
	if err := svc.BuildIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.SetSearchService(svc)
	e.SetEmbedder(nrpEmbedder{})
	svc.SetReranker(nrpReranker{})
	return e
}

var nrpLongParams = map[string]any{"query": "simmer stock", "pinned": []any{"rc0"}, "n": int64(10), "window": int64(40), "min_similarity": nil}

// nrpAllocByFrame returns bytes allocated so far per first non-runtime frame.
func nrpAllocByFrame() map[string]int64 {
	runtime.GC()
	runtime.GC()
	var records []runtime.MemProfileRecord
	n, _ := runtime.MemProfile(nil, true)
	for {
		records = make([]runtime.MemProfileRecord, n+100)
		var ok bool
		n, ok = runtime.MemProfile(records, true)
		if ok {
			records = records[:n]
			break
		}
	}
	out := map[string]int64{}
	for _, r := range records {
		frames := runtime.CallersFrames(r.Stack())
		name := "?"
		for {
			f, more := frames.Next()
			if f.Function != "" && !strings.HasPrefix(f.Function, "runtime.") {
				name = f.Function
				break
			}
			if !more {
				break
			}
		}
		out[name] += r.AllocBytes
	}
	return out
}

func nrpProfiledCall(t *testing.T, e *StorageExecutor, name, q string, tx bool) {
	t.Helper()
	if os.Getenv("NRP_NOPROFILE") != "" {
		o := nrpMeasure(t, e, "long-noprofile-"+name, q, nrpLongParams, tx)
		t.Logf("NRPSUM long-noprofile %s tx=%v ms=%.1f totalAllocMB=%.2f rows=%d hash=%s", name, tx, float64(o.elapsed.Microseconds())/1000, float64(o.alloc)/1e6, o.rows, o.hash)
		return
	}
	before := nrpAllocByFrame()
	if tx {
		nrpExec(t, e, "BEGIN", nil)
	}
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	start := time.Now()
	r, err := e.Execute(nrpCtx(), q, nrpLongParams)
	elapsed := time.Since(start)
	runtime.ReadMemStats(&b)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if tx {
		nrpExec(t, e, "ROLLBACK", nil)
	}
	after := nrpAllocByFrame()
	type kv struct {
		k string
		v int64
	}
	var diffs []kv
	var antlr, total int64
	for k, v := range after {
		d := v - before[k]
		if d <= 0 {
			continue
		}
		total += d
		if strings.Contains(k, "antlr4-go/antlr") {
			antlr += d
		}
		diffs = append(diffs, kv{k, d})
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].v > diffs[j].v })
	t.Logf("NRPSUM long %s tx=%v ms=%.1f totalAllocMB=%.2f profileMB=%.2f antlrMB=%.2f rows=%d hash=%s",
		name, tx, float64(elapsed.Microseconds())/1000, float64(b.TotalAlloc-a.TotalAlloc)/1e6, float64(total)/1e6, float64(antlr)/1e6, len(r.Rows), nrpHash(r.Rows))
	for i := 0; i < len(diffs) && i < 8; i++ {
		t.Logf("NRP   top %s %.2fMB", diffs[i].k, float64(diffs[i].v)/1e6)
	}
	if len(r.Rows) == 0 {
		t.Fatalf("%s returned no rows", name)
	}
}

// Fresh process: the long search statement is the first Cypher statement
// parsed after the store is built (the store is written through the storage
// API; only BuildIndexes and nothing else runs before it).
func nrpProfileRate() {
	if os.Getenv("NRP_NOPROFILE") == "" {
		runtime.MemProfileRate = 1
	}
}

func TestNRPLongStatementFirst(t *testing.T) {
	nrpProfileRate()
	e := nrpKitchen(t)
	nrpProfiledCall(t, e, "search-first", nrpSearchLong, false)
	nrpProfiledCall(t, e, "search-second", nrpSearchLong, false)
	nrpProfiledCall(t, e, "search-explicit-first", nrpSearchLong, true)
	nrpProfiledCall(t, e, "browse-first", nrpBrowseLong, false)
	nrpProfiledCall(t, e, "browse-second", nrpBrowseLong, false)
	nrpProfiledCall(t, e, "search-third", nrpSearchLong, false)
	t.Logf("NRP statement bytes search=%d browse=%d", len(nrpSearchLong), len(nrpBrowseLong))
}

// Fresh process: the explicit-transaction mode first, then auto-commit.
func TestNRPLongStatementExplicitFirst(t *testing.T) {
	nrpProfileRate()
	e := nrpKitchen(t)
	nrpProfiledCall(t, e, "search-explicit-first", nrpSearchLong, true)
	nrpProfiledCall(t, e, "search-explicit-second", nrpSearchLong, true)
	nrpProfiledCall(t, e, "search-autocommit-after", nrpSearchLong, false)
}

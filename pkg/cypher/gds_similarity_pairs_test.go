package cypher

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pairsParty is one source of a nornicdb.similarity.pairs fixture, as the
// oracle reads it.
type pairsParty struct {
	id          string
	keys        []string
	class       interface{} // string or nil
	identifiers []string
	document    interface{} // string or nil
	allow       bool
	things      []string
	// weights, for a thing, are its rows' explicit weights (repeated rows),
	// in place of the fixture's having.
	weights map[string][]float64
}

// pairsFixture is a fixture: sources, each thing's holder count, and the
// excluded pairs.
type pairsFixture struct {
	parties  []pairsParty
	having   map[string]int
	excluded [][2]string
}

// args is the procedure's arguments for the fixture.
func (f pairsFixture) args(topK, topN int) []interface{} {
	var rows, sources []interface{}
	for _, p := range f.parties {
		for _, thing := range p.things {
			if weights, ok := p.weights[thing]; ok {
				for _, weight := range weights {
					rows = append(rows, map[string]interface{}{"source": p.id, "thing": thing, "weight": weight})
				}
				continue
			}
			rows = append(rows, map[string]interface{}{"source": p.id, "thing": thing, "having": int64(f.having[thing])})
		}
		keys := make([]interface{}, len(p.keys))
		for i, key := range p.keys {
			keys[i] = key
		}
		identifiers := make([]interface{}, len(p.identifiers))
		for i, key := range p.identifiers {
			identifiers[i] = key
		}
		sources = append(sources, map[string]interface{}{
			"source": p.id, "partitionKeys": keys, "classKey": p.class, "identifierKeys": identifiers,
			"documentKey": p.document, "allowUnshared": p.allow,
		})
	}
	excluded := make([]interface{}, len(f.excluded))
	for i, pair := range f.excluded {
		excluded[i] = []interface{}{pair[0], pair[1]}
	}
	return []interface{}{rows, map[string]interface{}{
		"sources": sources, "excludedPairs": excluded, "topK": int64(topK), "topN": int64(topN),
	}}
}

// pairsOraclePair is a scored eligible pair, independently computed.
type pairsOraclePair struct {
	a, b        string
	similarity  float64
	identifiers []interface{}
	conflicts   []interface{}
}

func (p pairsOraclePair) before(q pairsOraclePair) bool {
	if len(p.identifiers) != len(q.identifiers) {
		return len(p.identifiers) > len(q.identifiers)
	}
	if p.similarity != q.similarity {
		return p.similarity > q.similarity
	}
	if p.a != q.a {
		return p.a < q.a
	}
	return p.b < q.b
}

func (p pairsOraclePair) entry() map[string]interface{} {
	return map[string]interface{}{"node1": p.a, "node2": p.b, "similarity": p.similarity, "identifiers": p.identifiers, "conflicts": p.conflicts}
}

// pairsOracle judges and scores one pair straight from the definitions:
// shared partition key, compatible classes, not excluded, and for two
// sources needing a witness, different documents and a shared thing other
// than either; the score adds 1.0/having over the shared things other than
// either, rarest first then by thing id; identifiers are a's keys b has.
type pairsOracle struct {
	fixture     pairsFixture
	things      []map[string]bool
	keys        []map[string]bool
	identifiers []map[string]bool
	excluded    map[[2]string]bool
}

func newPairsOracle(f pairsFixture) *pairsOracle {
	o := &pairsOracle{fixture: f, excluded: make(map[[2]string]bool)}
	for _, p := range f.parties {
		things, keys, identifiers := make(map[string]bool), make(map[string]bool), make(map[string]bool)
		for _, t := range p.things {
			things[t] = true
		}
		for _, k := range p.keys {
			keys[k] = true
		}
		for _, k := range p.identifiers {
			identifiers[k] = true
		}
		o.things = append(o.things, things)
		o.keys = append(o.keys, keys)
		o.identifiers = append(o.identifiers, identifiers)
	}
	for _, pair := range f.excluded {
		o.excluded[[2]string{pair[0], pair[1]}] = true
		o.excluded[[2]string{pair[1], pair[0]}] = true
	}
	return o
}

func (o *pairsOracle) judge(i, j int) (pairsOraclePair, bool) {
	a, b := o.fixture.parties[i], o.fixture.parties[j]
	if a.id > b.id {
		a, b, i, j = b, a, j, i
	}
	shareKey := false
	for k := range o.keys[i] {
		if o.keys[j][k] {
			shareKey = true
			break
		}
	}
	if !shareKey || a.class != nil && b.class != nil && a.class != b.class || o.excluded[[2]string{a.id, b.id}] {
		return pairsOraclePair{}, false
	}
	var shared []float64
	for t := range o.things[i] {
		if !o.things[j][t] || t == a.id || t == b.id {
			continue
		}
		wa, okA := o.weight(a, t)
		wb, okB := o.weight(b, t)
		if okA && okB {
			shared = append(shared, math.Min(wa, wb))
		}
	}
	if !a.allow && !b.allow && (a.document != nil && a.document == b.document || len(shared) == 0) {
		return pairsOraclePair{}, false
	}
	// A pair's shared things, the smaller of its two weights each, added
	// largest first (for weights 1.0/having: rarest first).
	sort.Sort(sort.Reverse(sort.Float64Slice(shared)))
	score := 0.0
	for _, weight := range shared {
		score += weight
	}
	identifiers := []interface{}{}
	for _, k := range a.identifiers {
		if o.identifiers[j][k] {
			identifiers = append(identifiers, k)
		}
	}
	conflicts := []interface{}{}
	if len(a.identifiers) > 0 && len(b.identifiers) > 0 && len(identifiers) == 0 {
		conflicts = []interface{}{"identifiers differ"}
	}
	return pairsOraclePair{a: a.id, b: b.id, similarity: score, identifiers: identifiers, conflicts: conflicts}, true
}

// page is the oracle's answer over every pair: each first source's best
// topK (0: all), then the best topN, and the count of every eligible pair.
func (o *pairsOracle) page(topK, topN int) ([]interface{}, int64) {
	var all []pairsOraclePair
	for i := range o.fixture.parties {
		for j := i + 1; j < len(o.fixture.parties); j++ {
			if pair, ok := o.judge(i, j); ok {
				all = append(all, pair)
			}
		}
	}
	sort.Slice(all, func(x, y int) bool { return all[x].before(all[y]) })
	reached := int64(len(all))
	if topK > 0 {
		kept := all[:0]
		taken := make(map[string]int)
		for _, pair := range all {
			if taken[pair.a] < topK {
				taken[pair.a]++
				kept = append(kept, pair)
			}
		}
		all = kept
	}
	if len(all) > topN {
		all = all[:topN]
	}
	page := make([]interface{}, len(all))
	for i, pair := range all {
		page[i] = pair.entry()
	}
	return page, reached
}

// streamedPage is page with topK 0 for large fixtures: pairs are found
// through the partition keys (each pair once, under its smallest shared
// key) and only the best topN are kept.
func (o *pairsOracle) streamedPage(topN int) ([]interface{}, int64) {
	byKey := make(map[string][]int)
	for i := range o.fixture.parties {
		for k := range o.keys[i] {
			byKey[k] = append(byKey[k], i)
		}
	}
	var best []pairsOraclePair
	var reached int64
	for key, members := range byKey {
		for x := range members {
			for y := x + 1; y < len(members); y++ {
				i, j := members[x], members[y]
				smallest := ""
				for k := range o.keys[i] {
					if o.keys[j][k] && (smallest == "" || k < smallest) {
						smallest = k
					}
				}
				if smallest != key {
					continue
				}
				pair, ok := o.judge(i, j)
				if !ok {
					continue
				}
				reached++
				if len(best) == topN && !pair.before(best[topN-1]) {
					continue
				}
				at := sort.Search(len(best), func(n int) bool { return pair.before(best[n]) })
				best = append(best, pairsOraclePair{})
				copy(best[at+1:], best[at:])
				best[at] = pair
				if len(best) > topN {
					best = best[:topN]
				}
			}
		}
	}
	page := make([]interface{}, len(best))
	for i, pair := range best {
		page[i] = pair.entry()
	}
	return page, reached
}

// runPairs calls the procedure directly with the fixture.
func runPairs(t *testing.T, f pairsFixture, topK, topN int) ([]interface{}, int64, map[string]interface{}) {
	t.Helper()
	result, err := callNornicSimilarityPairs(context.Background(), f.args(topK, topN))
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	page := row[0].([]interface{})
	for _, entry := range page {
		// The oracle's identifier lists hold the strings as given.
		item := entry.(map[string]interface{})
		require.IsType(t, []interface{}{}, item["identifiers"])
	}
	require.Equal(t, row[1].(int64) > int64(topN), row[2])
	return page, row[1].(int64), row[3].(map[string]interface{})
}

// ownerFixture is n documents (n even) with one person mention each, all of
// one name, so every two of them pair; one invoice mention each, the two of
// an invoice pairing by its name and number; one company identity per
// remainder of 200, with an unresolved mention of the first 20; and bank,
// tax and supplier identities that pair with nobody. Every document holds
// the bank and the tax office. Eligible pairs: n(n-1)/2 + n/2 + 20.
func ownerFixture(n int) pairsFixture {
	f := pairsFixture{having: make(map[string]int)}
	docThings := func(d int) []string {
		return []string{"bank", "tax", fmt.Sprintf("party%03d", d%200), fmt.Sprintf("org%02d", d%50),
			fmt.Sprintf("acc%05d", d/3), fmt.Sprintf("inv%05d", d/2)}
	}
	holders := make(map[string][]int)
	for d := 0; d < n; d++ {
		for _, t := range docThings(d) {
			f.having[t]++
			holders[t] = append(holders[t], d)
		}
	}
	union := func(docs []int) []string {
		seen := make(map[string]bool)
		var out []string
		for _, d := range docs {
			for _, t := range docThings(d) {
				if !seen[t] {
					seen[t] = true
					out = append(out, t)
				}
			}
		}
		sort.Strings(out)
		return out
	}
	for d := 0; d < n; d++ {
		doc := fmt.Sprintf("doc%05d", d)
		f.parties = append(f.parties,
			pairsParty{id: fmt.Sprintf("m%05d", d), keys: []string{"maksim petrov"}, class: "person", document: doc, things: docThings(d)},
			pairsParty{id: fmt.Sprintf("i%05d", d), keys: []string{fmt.Sprintf("inv%05d", d/2)}, class: "brand",
				identifiers: []string{fmt.Sprintf("invoice number:INV%d", d/2)}, document: doc, things: docThings(d)})
	}
	for c := 0; c < 200 && c < n; c++ {
		id := fmt.Sprintf("party%03d", c)
		f.parties = append(f.parties, pairsParty{id: id, keys: []string{id}, class: "org",
			identifiers: []string{fmt.Sprintf("company number:P%d", c)}, allow: true, things: union(holders[id])})
	}
	for x := 0; x < 20; x++ {
		f.parties = append(f.parties, pairsParty{id: fmt.Sprintf("u%02d", x), keys: []string{fmt.Sprintf("party%03d", x)}, class: "org",
			identifiers: []string{fmt.Sprintf("company number:P%d", x)}, document: fmt.Sprintf("doc%05d", x), things: docThings(x)})
	}
	for _, id := range []string{"bank", "tax"} {
		f.parties = append(f.parties, pairsParty{id: id, keys: []string{id + " office"}, class: "org", allow: true, things: union(holders[id])})
	}
	for k := 0; k < 50 && k < n; k++ {
		id := fmt.Sprintf("org%02d", k)
		f.parties = append(f.parties, pairsParty{id: id, keys: []string{id + " ltd"}, class: "org", allow: true, things: union(holders[id])})
	}
	return f
}

// TestSimilarityPairsOwnerFixture528: the exact count and the page at 528
// documents, against the oracle over every pair, with the work showing that
// most pairs were counted, not scored.
func TestSimilarityPairsOwnerFixture528(t *testing.T) {
	f := ownerFixture(528)
	oracle := newPairsOracle(f)
	for _, topN := range []int{50, 400} {
		page, reached, work := runPairs(t, f, 0, topN)
		require.Equal(t, int64(139412), reached)
		want, wantReached := oracle.page(0, topN)
		require.Equal(t, wantReached, reached)
		require.Equal(t, want, page, "topN=%d", topN)
		require.GreaterOrEqual(t, work["countedCombinatorially"].(int64), int64(528*527/2))
		require.Less(t, work["pairScores"].(int64), int64(528*527/2))
		require.LessOrEqual(t, work["peakPairMaps"].(int64), int64(topN)+work["representatives"].(int64))
	}
}

// TestSimilarityPairsOwnerFixture5280: 13,939,220 eligible pairs counted
// exactly, the first 50 (and the first 2,700, past the identifier pairs)
// equal to the streamed oracle, and far fewer pair scores than pairs.
func TestSimilarityPairsOwnerFixture5280(t *testing.T) {
	if testing.Short() {
		t.Skip("streams 13.9 million oracle pairs")
	}
	f := ownerFixture(5280)
	oracle := newPairsOracle(f)
	want, wantReached := oracle.streamedPage(2700)
	require.Equal(t, int64(13939220), wantReached)
	for _, topN := range []int{50, 2700} {
		page, reached, work := runPairs(t, f, 0, topN)
		require.Equal(t, int64(13939220), reached)
		require.Equal(t, want[:topN], page, "topN=%d", topN)
		require.GreaterOrEqual(t, work["countedCombinatorially"].(int64), int64(13936560))
		require.Less(t, work["pairScores"].(int64), int64(1500000))
		require.LessOrEqual(t, work["peakPairMaps"].(int64), int64(topN)+work["representatives"].(int64))
		t.Logf("topN=%d work=%v", topN, work)
	}
}

// TestSimilarityPairsRandom compares every page, count and per-source limit
// with the oracle on random fixtures: shared and unknown classes, several
// keys per source, same-document sources, sources holding themselves as a
// thing, repeated identifier keys, excluded pairs in both orders.
func TestSimilarityPairsRandom(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	for round := 0; round < 150; round++ {
		size := 6 + random.Intn(30)
		f := pairsFixture{having: make(map[string]int)}
		thingPool := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "p03", "p07"}
		for _, thing := range thingPool {
			f.having[thing] = 1 + random.Intn(9)
		}
		common := random.Intn(2) == 0
		for i := 0; i < size; i++ {
			p := pairsParty{id: fmt.Sprintf("p%02d", i)}
			for _, key := range []string{"k1", "k2", "k3"} {
				if random.Intn(3) == 0 {
					p.keys = append(p.keys, key)
				}
			}
			switch random.Intn(3) {
			case 0:
				p.class = "A"
			case 1:
				p.class = "B"
			}
			if random.Intn(4) > 0 {
				p.document = fmt.Sprintf("d%d", random.Intn(5))
			}
			p.allow = random.Intn(4) == 0
			for _, key := range []string{"x1", "x2", "x1"} {
				if random.Intn(4) == 0 {
					p.identifiers = append(p.identifiers, key)
				}
			}
			for _, thing := range thingPool {
				if random.Intn(3) == 0 || common && thing == "t1" {
					p.things = append(p.things, thing)
				}
			}
			// Half the rounds give some things per-source weights, repeated
			// rows included, as weighted rows may.
			if round%2 == 1 {
				p.weights = make(map[string][]float64)
				for _, thing := range p.things {
					if random.Intn(3) == 0 {
						choices := []float64{0, 0.125, 0.25, 0.5, 1}
						p.weights[thing] = []float64{choices[random.Intn(len(choices))]}
						if random.Intn(2) == 0 {
							p.weights[thing] = append(p.weights[thing], choices[random.Intn(len(choices))])
						}
					}
				}
			}
			f.parties = append(f.parties, p)
		}
		for e := 0; e < 4; e++ {
			a, b := random.Intn(size), random.Intn(size)
			f.excluded = append(f.excluded, [2]string{f.parties[a].id, f.parties[b].id})
		}
		oracle := newPairsOracle(f)
		for _, topN := range []int{1, 2, 5, 1000} {
			for _, topK := range []int{0, 1, 2, 3} {
				page, reached, _ := runPairs(t, f, topK, topN)
				want, wantReached := oracle.page(topK, topN)
				require.Equal(t, wantReached, reached, "round=%d topK=%d topN=%d", round, topK, topN)
				require.Equal(t, want, page, "round=%d topK=%d topN=%d", round, topK, topN)
			}
		}
	}
}

// TestSimilarityPairsBaseRepresentatives: the first pairs in (a, b) order
// stand only for pairs that tie exactly at the block's base with no
// identifiers. A pair at the base sharing an identifier, a pair below
// another block's base sharing one, and a pair above the base through a
// rarer shared thing must all be found, though none is among the first
// pairs in id order.
func TestSimilarityPairsBaseRepresentatives(t *testing.T) {
	f := pairsFixture{having: map[string]int{"c": 10, "r": 2, "z": 100}}
	for i := 1; i <= 6; i++ {
		p := pairsParty{id: fmt.Sprintf("a%d", i), keys: []string{"g1"}, document: fmt.Sprintf("da%d", i), things: []string{"c"}}
		switch i {
		case 4:
			p.things = append(p.things, "r")
		case 5:
			p.identifiers = []string{"id-x"}
		case 6:
			p.things = append(p.things, "r")
			p.identifiers = []string{"id-x"}
		}
		f.parties = append(f.parties, p)
	}
	for i := 1; i <= 3; i++ {
		p := pairsParty{id: fmt.Sprintf("b%d", i), keys: []string{"g2"}, document: fmt.Sprintf("db%d", i), things: []string{"z"}}
		if i > 1 {
			p.identifiers = []string{"id-y"}
		}
		f.parties = append(f.parties, p)
	}
	page, reached, _ := runPairs(t, f, 0, 3)
	require.Equal(t, int64(15+3), reached)
	base := 1.0 / 10
	require.Equal(t, []interface{}{
		map[string]interface{}{"node1": "a5", "node2": "a6", "similarity": base, "identifiers": []interface{}{"id-x"}, "conflicts": []interface{}{}},
		map[string]interface{}{"node1": "b2", "node2": "b3", "similarity": 1.0 / 100, "identifiers": []interface{}{"id-y"}, "conflicts": []interface{}{}},
		map[string]interface{}{"node1": "a4", "node2": "a6", "similarity": floatSum(1.0/2, base), "identifiers": []interface{}{}, "conflicts": []interface{}{}},
	}, page)
	want, _ := newPairsOracle(f).page(0, 3)
	require.Equal(t, want, page)
}

// TestSimilarityPairsProcedure: the procedure through Cypher, with source
// nodes whose id property orders them opposite to their stored ids, the
// shared things with their holder counts, conflicts, and its errors.
func TestSimilarityPairsProcedure(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns")
	exec := NewStorageExecutor(engine)
	for i, property := range []string{"p-c", "p-b", "p-a"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i+1)), Labels: []string{"P"}, Properties: map[string]interface{}{"id": property}})
		require.NoError(t, err)
	}
	for _, thing := range []string{"t1", "t2"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(thing), Labels: []string{"T"}, Properties: map[string]interface{}{"id": thing}})
		require.NoError(t, err)
	}
	run := func(config string) ([][]interface{}, error) {
		result, err := exec.Execute(context.Background(),
			"MATCH (p:P) WITH collect({source: p, partitionKeys: ['g'], identifierKeys: CASE p.id WHEN 'p-a' THEN ['x'] ELSE ['y'] END}) AS sources "+
				"MATCH (p:P), (t:T) WITH sources, collect({source: p, thing: t, having: CASE t.id WHEN 't1' THEN 3 ELSE 4 END}) AS rows "+
				"CALL nornicdb.similarity.pairs(rows, {sources: sources"+config+"}) YIELD pairs, reached, capped RETURN pairs, reached, capped", nil)
		if err != nil {
			return nil, err
		}
		return result.Rows, nil
	}
	rows, err := run(", tieBreakProperty: 'id', topN: 2, includeShared: true")
	require.NoError(t, err)
	shared := []interface{}{
		map[string]interface{}{"nodeId": "t1", "weight": 1.0 / 3, "having": int64(3)},
		map[string]interface{}{"nodeId": "t2", "weight": 1.0 / 4, "having": int64(4)},
	}
	score := floatSum(1.0/3, 1.0/4)
	require.Equal(t, [][]interface{}{{[]interface{}{
		// p-b (n2) and p-c (n1) share y; p-a's pairs conflict.
		map[string]interface{}{"node1": "n2", "node2": "n1", "similarity": score, "identifiers": []interface{}{"y"}, "conflicts": []interface{}{}, "shared": shared},
		map[string]interface{}{"node1": "n3", "node2": "n2", "similarity": score, "identifiers": []interface{}{}, "conflicts": []interface{}{"identifiers differ"}, "shared": shared},
	}, int64(3), true}}, rows)
	for _, tc := range []struct{ config, want string }{
		{", tieBreakProperty: 'id', topN: 0", "topN"},
		{", topK: -1", "topK"},
		{", similarityMetric: 'JACCARD'", "JACCARD is not a valid metric for nornicdb.similarity.pairs, which scores RESOURCE_ALLOCATION"},
		{", similarityMetric: 1", "similarityMetric"},
		{", excludedPairs: [['a']]", "excludedPairs[0]"},
		{", excludedPairs: 'a'", "excludedPairs"},
	} {
		_, err := run(tc.config)
		require.Error(t, err, tc.config)
		require.Contains(t, err.Error(), tc.want, tc.config)
	}
	for _, tc := range []struct{ call, want string }{
		{"nornicdb.similarity.pairs([], {})", "sources"},
		{"nornicdb.similarity.pairs([], {sources: 'a'})", "sources"},
		{"nornicdb.similarity.pairs([], {sources: ['a']})", "sources[0]"},
		{"nornicdb.similarity.pairs([], {sources: [{partitionKeys: ['g']}]})", "sources[0].source"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', partitionKeys: 'g'}]})", "sources[0].partitionKeys"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', partitionKeys: [1.5]}]})", "sources[0].partitionKeys"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', classKey: true}]})", "sources[0].classKey"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', documentKey: [1]}]})", "sources[0].documentKey"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', allowUnshared: 1}]})", "sources[0].allowUnshared"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a', kind: 1}]})", "kind"},
		{"nornicdb.similarity.pairs([], {sources: [{source: 'a'}, {source: 'a'}]})", "Source 1 of the similarity sources repeats the source a"},
		{"nornicdb.similarity.pairs([{source: 'a', thing: 'x', having: 'many'}], {sources: [{source: 'a'}]})", "Row 0 of the similarity rows has the weight String"},
		{"nornicdb.similarity.pairs(null, {sources: []})", "rows"},
	} {
		_, err := exec.Execute(context.Background(), "CALL "+tc.call, nil)
		require.Error(t, err, tc.call)
		require.Contains(t, err.Error(), tc.want, tc.call)
	}
	// No eligible pair: one summary row all the same.
	result, err := exec.Execute(context.Background(), "CALL nornicdb.similarity.pairs([], {sources: [{source: 'a', partitionKeys: ['g']}]}) YIELD pairs, reached, capped RETURN pairs, reached, capped", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{}, int64(0), false}}, result.Rows)
}

// weight is a source's weight for a thing as the rows give it: the largest
// of its explicit weights, else 1.0/having; false for a thing nobody holds.
func (o *pairsOracle) weight(p pairsParty, thing string) (float64, bool) {
	if weights, ok := p.weights[thing]; ok {
		best := weights[0]
		for _, w := range weights[1:] {
			best = math.Max(best, w)
		}
		return best, true
	}
	having := o.fixture.having[thing]
	return 1.0 / float64(having), having > 0
}

// oneAtom is n sources of one partition key, each in its own document
// unless documents says otherwise, all holding x with weight 1.
func oneAtom(n int, documents func(i int) string) pairsFixture {
	f := pairsFixture{having: map[string]int{}}
	for i := 0; i < n; i++ {
		document := fmt.Sprintf("d%05d", i)
		if documents != nil {
			document = documents(i)
		}
		f.parties = append(f.parties, pairsParty{id: fmt.Sprintf("p%05d", i), keys: []string{"g"}, document: document,
			things: []string{"x"}, weights: map[string][]float64{"x": {1}}})
	}
	return f
}

// TestSimilarityPairsWeightedCommonThings: a common thing whose per-source
// weights differ is not a base all pairs share; the pairs it ranks are found
// and the page stays exact (review R1, bot 4300779657).
func TestSimilarityPairsWeightedCommonThings(t *testing.T) {
	// a, b, c share only x, with weights 1, 0.125, 1: (a, c) is first.
	f := pairsFixture{having: map[string]int{}}
	for _, p := range []struct {
		id     string
		weight float64
	}{{"a", 1}, {"b", 0.125}, {"c", 1}} {
		f.parties = append(f.parties, pairsParty{id: p.id, keys: []string{"g"}, document: "d" + p.id, things: []string{"x"}, weights: map[string][]float64{"x": {p.weight}}})
	}
	oracle := newPairsOracle(f)
	for _, limits := range [][2]int{{0, 1}, {1, 2}, {0, 3}} {
		page, reached, _ := runPairs(t, f, limits[0], limits[1])
		want, wantReached := oracle.page(limits[0], limits[1])
		require.Equal(t, wantReached, reached)
		require.Equal(t, want, page, "topK=%d topN=%d", limits[0], limits[1])
	}
	page, _, _ := runPairs(t, f, 0, 1)
	require.Equal(t, "a", page[0].(map[string]interface{})["node1"])
	require.Equal(t, "c", page[0].(map[string]interface{})["node2"])
	require.Equal(t, 1.0, page[0].(map[string]interface{})["similarity"])
	// Four sources with weights 1, 1, 100, 100 (repeated rows resolve to
	// their largest): the last pair, at 100, is first.
	g := pairsFixture{having: map[string]int{}}
	for i, weights := range [][]float64{{1}, {0.5, 1}, {100}, {3, 100}} {
		g.parties = append(g.parties, pairsParty{id: fmt.Sprintf("q%d", i), keys: []string{"g"}, document: fmt.Sprintf("dq%d", i), things: []string{"x"}, weights: map[string][]float64{"x": weights}})
	}
	page, _, _ = runPairs(t, g, 0, 1)
	require.Equal(t, []interface{}{map[string]interface{}{"node1": "q2", "node2": "q3", "similarity": 100.0, "identifiers": []interface{}{}, "conflicts": []interface{}{}}}, page)
	want, _ := newPairsOracle(g).page(0, 1)
	require.Equal(t, want, page)
}

// TestSimilarityPairsBoundedSelection: every pair sharing one identifier is
// scored, yet the retained pairs stay within the page (review R2, R7).
func TestSimilarityPairsBoundedSelection(t *testing.T) {
	f := oneAtom(1000, nil)
	for i := range f.parties {
		f.parties[i].identifiers = []string{"shared-id"}
	}
	page, reached, work := runPairs(t, f, 0, 50)
	require.Equal(t, int64(1000*999/2), reached)
	require.Equal(t, int64(1000*999/2), work["pairScores"].(int64))
	require.LessOrEqual(t, work["peakPairMaps"].(int64), int64(50)+work["representatives"].(int64))
	want, _ := newPairsOracle(f).streamedPage(50)
	require.Equal(t, want, page)
	// Empty input: an empty page and zero counters, peakPairMaps included.
	_, reached, work = runPairs(t, pairsFixture{having: map[string]int{}}, 0, 10)
	require.Zero(t, reached)
	require.Equal(t, int64(0), work["peakPairMaps"])
}

// TestSimilarityPairsSameDocumentRuns: the representatives skip a source's
// document at once, so a block of one document costs nothing and one whose
// last source differs costs a few visits per source (review R5).
func TestSimilarityPairsSameDocumentRuns(t *testing.T) {
	const n = 2000
	same := oneAtom(n, func(int) string { return "d" })
	_, reached, work := runPairs(t, same, 0, 50)
	require.Zero(t, reached)
	require.Zero(t, work["partnerVisits"].(int64))
	lastDiffers := oneAtom(n, func(i int) string {
		if i == n-1 {
			return "other"
		}
		return "d"
	})
	for _, limits := range [][2]int{{0, 50}, {1, 50}} {
		page, reached, work := runPairs(t, lastDiffers, limits[0], limits[1])
		require.Equal(t, int64(n-1), reached)
		require.Less(t, work["partnerVisits"].(int64), int64(4*n), "topK=%d", limits[0])
		want, _ := newPairsOracle(lastDiffers).page(limits[0], limits[1])
		require.Equal(t, want, page, "topK=%d", limits[0])
	}
}

// TestSimilarityPairsCancellation: a cancelled statement stops the call, at
// once and mid-work (review R3).
func TestSimilarityPairsCancellation(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := callNornicSimilarityPairs(cancelled, oneAtom(10, nil).args(0, 5))
	require.ErrorIs(t, err, context.Canceled)
	// Mid-work: every pair shares an identifier, so the call scores half a
	// million pairs unless it stops.
	f := oneAtom(1000, nil)
	for i := range f.parties {
		f.parties[i].identifiers = []string{"shared-id"}
	}
	args := f.args(0, 50)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	_, err = callNornicSimilarityPairs(ctx, args)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSimilarityPairsMemoryAccounting: the call's working state is charged
// to the statement: over the transaction limit it fails with the memory
// error and gives everything back; under it, it succeeds and gives back its
// temporaries (review R4).
func TestSimilarityPairsMemoryAccounting(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	args := ownerFixture(528).args(0, 50)
	params := map[string]interface{}{"rows": args[0], "config": args[1]}
	query := "CALL nornicdb.similarity.pairs($rows, $config) YIELD reached RETURN reached"
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	_, err := executor.Execute(context.Background(), query, params)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	requirePoolsEmpty(t, "nornic")
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 30})
	result, err := executor.Execute(context.Background(), query, params)
	require.NoError(t, err)
	require.Equal(t, int64(139412), result.Rows[0][0])
	requirePoolsEmpty(t, "nornic")
}

// TestSimilarityPairsMetric: RESOURCE_ALLOCATION may be named, in any case.
func TestSimilarityPairsMetric(t *testing.T) {
	for _, metric := range []string{"RESOURCE_ALLOCATION", "resource_allocation"} {
		args := oneAtom(3, nil).args(0, 5)
		args[1].(map[string]interface{})["similarityMetric"] = metric
		result, err := callNornicSimilarityPairs(context.Background(), args)
		require.NoError(t, err, metric)
		require.Equal(t, int64(3), result.Rows[0][1])
	}
	result, err := callNornicSimilarityPairs(context.Background(), []interface{}{[]interface{}{}, map[string]interface{}{"sources": []interface{}{}, "similarityMetric": "RESOURCE_ALLOCATION"}})
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, result.Rows[0][0])
}

// similarityPairsExample is the user guide's nornicdb.similarity.pairs
// example.
const similarityPairsExample = `WITH [{source: 'm1', thing: 'bank', having: 3}, {source: 'm2', thing: 'bank', having: 3},
      {source: 'm2', thing: 'inv7', having: 2}, {source: 'm3', thing: 'inv7', having: 2},
      {source: 'm3', thing: 'bank', having: 3}] AS rows
CALL nornicdb.similarity.pairs(rows, {
  sources: [{source: 'm1', partitionKeys: ['petrov'], documentKey: 'd1'},
            {source: 'm2', partitionKeys: ['petrov'], documentKey: 'd2'},
            {source: 'm3', partitionKeys: ['petrov'], documentKey: 'd3'}],
  topN: 2})
YIELD pairs, reached, capped
RETURN [p IN pairs | [p.node1, p.node2, p.similarity]] AS best, reached, capped`

// TestSimilarityPairsDocExample pins the user guide's example.
func TestSimilarityPairsDocExample(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
	result, err := exec.Execute(context.Background(), similarityPairsExample, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{
		[]interface{}{"m2", "m3", floatSum(1.0/2, 1.0/3)}, []interface{}{"m1", "m2", 1.0 / 3},
	}, int64(3), true}}, result.Rows)
}

// singletonAtoms is n sources with one partition key and distinct classes:
// every source its own atom, and no two atoms compatible, so preparation
// tries n(n+1)/2 atom pairs and keeps none.
func singletonAtoms(n int) map[string]interface{} {
	sources := make([]interface{}, n)
	for i := range sources {
		sources[i] = map[string]interface{}{"source": fmt.Sprintf("p%05d", i), "partitionKeys": []interface{}{"g"}, "classKey": fmt.Sprintf("c%05d", i)}
	}
	return map[string]interface{}{"sources": sources}
}

// TestSimilarityPairsCancelledPreparation: the call polls cancellation while
// it prepares, also through atom pairs it rejects, and never reports success
// once cancelled.
func TestSimilarityPairsCancelledPreparation(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	_, err := callNornicSimilarityPairs(ctx, []interface{}{[]interface{}{}, singletonAtoms(10000)})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// The rejected block attempts alone poll it.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	probe := newContextCancelProbe(cancelled)
	config, err := readSimilarityPairsConfig(singletonAtoms(100))
	require.NoError(t, err)
	selector := &similarityPairsSelector{config: config, probe: &probe, account: &memoryAccount{}}
	selector.parties = selector.config.parties
	require.NoError(t, selector.prepareAtoms())
	require.ErrorIs(t, selector.prepareBlocks(), context.Canceled)
}

// TestSimilarityPairsConfigurationMemory: the configuration's own state,
// the excluded pairs and identifier lists, is charged to the statement.
func TestSimilarityPairsConfigurationMemory(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	query := "CALL nornicdb.similarity.pairs($rows, $config) YIELD reached RETURN reached"
	excluded := make([]interface{}, 100000)
	for i := range excluded {
		excluded[i] = []interface{}{fmt.Sprintf("a%06d", i), fmt.Sprintf("b%06d", i)}
	}
	identifiers := make([]interface{}, 100000)
	for i := range identifiers {
		identifiers[i] = fmt.Sprintf("id%06d", i)
	}
	for name, config := range map[string]map[string]interface{}{
		"excludedPairs":  {"sources": []interface{}{}, "excludedPairs": excluded},
		"identifierKeys": {"sources": []interface{}{map[string]interface{}{"source": "s", "identifierKeys": identifiers}}},
	} {
		params := map[string]interface{}{"rows": []interface{}{}, "config": config}
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
		_, err := executor.Execute(context.Background(), query, params)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 30})
		result, err := executor.Execute(context.Background(), query, params)
		require.NoError(t, err, name)
		require.Equal(t, int64(0), result.Rows[0][0], name)
		requirePoolsEmpty(t, "nornic")
	}
}

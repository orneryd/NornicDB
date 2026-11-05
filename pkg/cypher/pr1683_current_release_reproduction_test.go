package cypher

// Fail-first reproduction for PR1683 / NOR-148 (with the correctness slice
// folded from NOR-294) on the pristine current-release checkout 31208b17.
// It adds no production hooks and changes no other file.
//
// Both probes run through the public Execute path only:
//
//  1. NOR-148 scan/rebuild defect: pkg/cypher/call_fulltext.go
//     callDbIndexFulltextQueryNodes streams the whole store with
//     storage.StreamNodesWithFallback and rebuilds every candidate's ftDoc on
//     each ranked call, so an unchanged repeated public query pays the scan and
//     document rebuild again. The spy below wraps the production namespace
//     wrapper (storage.NamespacedEngine) and only observes delegation, so every
//     optional capability — including storage.StreamingEngine, which
//     StreamNodesWithFallback type-asserts — stays visible; nothing is stubbed
//     or hidden. The executor is built with the supported zero-entry query
//     cache policy (MaxEntries: 0), so a cached result cannot mask execution.
//
//  2. NOR-148 acceptance criterion "Preserve exact case-sensitive property
//     identity: selecting `Text` must not collide with `text` or silently
//     discard either value" (folded NOR-294, test-matrix row 7).
//     fulltext_query.go ftDoc canonicalizes property names with lowerASCII, so
//     an index declared over [n.Text, n.text] collapses two distinct fields
//     into one map entry and one value is discarded. The probes below use two
//     distinct values on one document plus single-field documents, against the
//     exact schema-declared property list.
//
// The reproduction is expected to FAIL on this release: the failures are the
// recorded defect. No phrase-boundary behavior, scoring contract, or
// unsupported topology is asserted.

import (
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pr1683NewScanSpyExecutor builds an executor over the spied production
// namespace wrapper and native Badger fixture with the result cache disabled, and proves the
// instrumentation is live.
func pr1683NewScanSpyExecutor(t *testing.T) (*fulltextCountingStore, *StorageExecutor) {
	t.Helper()
	store := newTestBadgerNamespacedStore(t)
	namespaced, ok := store.(*storage.NamespacedEngine)
	require.True(t, ok)
	spy := &fulltextCountingStore{NamespacedEngine: namespaced}
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	require.Nil(t, exec.cache,
		"MaxEntries:0 must leave the composite query result cache disabled so a cached result cannot mask execution")

	// Instrumentation self-check: a direct streamed read through the same
	// optional interface the fulltext procedure uses must be observed.
	require.NoError(t, spy.StreamNodes(t.Context(), func(*storage.Node) error { return nil }))
	selfScans, _ := spy.take()
	require.Equal(t, 1, selfScans, "scan spy must observe StreamNodes calls")
	return spy, exec
}

// pr1683SeedOrchardCorpus creates the repeated-query corpus and declares the
// index the probe queries.
func pr1683SeedOrchardCorpus(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE (:Note {name:'n1', body:'orchard apple harvest'})`,
		`CREATE (:Note {name:'n2', body:'orchard pear harvest'})`,
		`CREATE (:Note {name:'n3', body:'meadow river bank'})`,
	} {
		_, err := exec.Execute(t.Context(), stmt, nil)
		require.NoError(t, err, "seed: %s", stmt)
	}
	_, err := exec.Execute(t.Context(),
		`CREATE FULLTEXT INDEX pr1683_repeat_idx IF NOT EXISTS FOR (n:Note) ON EACH [n.body]`, nil)
	require.NoError(t, err)

	ftIdx, ok := exec.storage.GetSchema().GetFulltextIndex("pr1683_repeat_idx")
	require.True(t, ok, "declared fulltext index must be visible to the procedure")
	require.Equal(t, []string{"body"}, ftIdx.Properties)
}

type pr1683Row struct {
	Name  string
	Score float64
}

// pr1683FulltextRows runs the public procedure query for index/query and
// returns name/score rows in the public ranked order.
func pr1683FulltextRows(t *testing.T, exec *StorageExecutor, index, query string) []pr1683Row {
	t.Helper()
	stmt := fmt.Sprintf(
		`CALL db.index.fulltext.queryNodes(%q, $q, {limit:10}) YIELD node, score RETURN node.name AS name, score`, index)
	res, err := exec.Execute(t.Context(), stmt, map[string]interface{}{"q": query})
	require.NoError(t, err, "public fulltext query %q against index %q", query, index)
	rows := make([]pr1683Row, 0, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 2, "procedure projects node name and score")
		name, ok := row[0].(string)
		require.True(t, ok, "node.name projects a string, got %T", row[0])
		score, ok := row[1].(float64)
		require.True(t, ok, "score projects a float64, got %T", row[1])
		require.Positive(t, score, "a matched row keeps a positive score")
		rows = append(rows, pr1683Row{Name: name, Score: score})
	}
	for i := 1; i < len(rows); i++ {
		require.GreaterOrEqual(t, rows[i-1].Score, rows[i].Score, "public fulltext ranking is descending")
	}
	return rows
}

// TestPR1683CurrentRelease_RepeatedFulltextQueryRescansUnchangedDocuments
// probes NOR-148: an unchanged repeated public fulltext query must not stream
// and rebuild the same documents again. On this release the procedure scans on
// every call, so the second execution's counts are expected to be non-zero.
func TestPR1683CurrentRelease_RepeatedFulltextQueryRescansUnchangedDocuments(t *testing.T) {
	t.Run("ranked top-k", func(t *testing.T) {
		spy, exec := pr1683NewScanSpyExecutor(t)
		pr1683SeedOrchardCorpus(t, exec)
		spy.take() // drop any counters from seeding/DDL

		const query = "orchard"
		first := pr1683FulltextRows(t, exec, "pr1683_repeat_idx", query)
		firstScans, firstDocs := spy.take()
		second := pr1683FulltextRows(t, exec, "pr1683_repeat_idx", query)
		secondScans, secondDocs := spy.take()

		require.Len(t, first, 2, "control: both orchard notes are indexed")
		require.Equal(t, first, second,
			"unchanged documents must produce identical rows and scores on a repeated public query")
		t.Logf("ranked probe: first execution streamed %d scan(s) rebuilding %d doc(s); "+
			"unchanged repeat streamed %d scan(s) rebuilding %d doc(s)",
			firstScans, firstDocs, secondScans, secondDocs)

		assert.Zero(t, secondScans,
			"NOR-148: repeating an unchanged fulltext query must not stream the store again; "+
				"the second call issued %d whole-store scan(s)", secondScans)
		assert.Zero(t, secondDocs,
			"NOR-148: repeating an unchanged fulltext query must not rebuild already-current documents; "+
				"the second call visited %d document(s) to rebuild ftDocs", secondDocs)
	})

	t.Run("count only", func(t *testing.T) {
		spy, exec := pr1683NewScanSpyExecutor(t)
		pr1683SeedOrchardCorpus(t, exec)
		spy.take()

		count := func() interface{} {
			res, err := exec.Execute(t.Context(),
				`CALL db.index.fulltext.queryNodes("pr1683_repeat_idx", $q) YIELD node RETURN count(node) AS matched`,
				map[string]interface{}{"q": "orchard"})
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)
			require.Len(t, res.Rows[0], 1)
			return res.Rows[0][0]
		}

		first := count()
		firstScans, firstDocs := spy.take()
		second := count()
		secondScans, secondDocs := spy.take()

		require.EqualValues(t, 2, first, "control: count-only query covers both orchard notes")
		require.EqualValues(t, first, second, "unchanged documents must produce the same count")
		t.Logf("count-only probe: first execution streamed %d scan(s) rebuilding %d doc(s); "+
			"unchanged repeat streamed %d scan(s) rebuilding %d doc(s)",
			firstScans, firstDocs, secondScans, secondDocs)

		assert.Zero(t, secondScans,
			"NOR-148: repeating an unchanged count-only fulltext query must not stream the store again; "+
				"the second call issued %d whole-store scan(s)", secondScans)
		assert.Zero(t, secondDocs,
			"NOR-148: repeating an unchanged count-only fulltext query must not rebuild already-current documents; "+
				"the second call visited %d document(s) to rebuild ftDocs", secondDocs)
	})
}

// pr1683IdentityNames runs the public procedure query over the case-identity
// index and returns the matched node names in deterministic order.
func pr1683IdentityNames(t *testing.T, exec *StorageExecutor, query string) []string {
	t.Helper()
	rows := pr1683FulltextRows(t, exec, "pr1683_case_identity", query)
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.Name)
	}
	sort.Strings(names)
	return names
}

// TestPR1683CurrentRelease_ExactTextAndTextFieldIdentity probes the folded
// NOR-294 acceptance criterion: an index declared over the exact property list
// [n.Text, n.text] must keep the two case-distinct fields selectable, with
// neither value silently discarded. Current fulltext_query.go lowercases field
// names onto a lowercased property map, so one value wins and the other is
// unselectable.
func TestPR1683CurrentRelease_ExactTextAndTextFieldIdentity(t *testing.T) {
	eng := newTestBadgerNamespacedStore(t)
	exec := NewStorageExecutor(eng, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	require.Nil(t, exec.cache, "result cache must be disabled so results cannot mask execution")

	for _, stmt := range []string{
		// One document carries two distinct values under the two case-distinct
		// property names.
		`CREATE (:Doc {name:'mixed', Text:'CaseExactAlpha', text:'lowerExactBeta'})`,
		// Single-field documents make each direction deterministic: an
		// uppercase-field clause must not read a lowercase-only property, and a
		// lowercase-field clause must not read an uppercase-only property.
		`CREATE (:Doc {name:'upperOnly', Text:'UppercaseZulu'})`,
		`CREATE (:Doc {name:'lowerOnly', text:'lowerOnlyGamma'})`,
	} {
		_, err := exec.Execute(t.Context(), stmt, nil)
		require.NoError(t, err, "seed: %s", stmt)
	}

	_, err := exec.Execute(t.Context(),
		`CREATE FULLTEXT INDEX pr1683_case_identity IF NOT EXISTS FOR (n:Doc) ON EACH [n.Text, n.text]`, nil)
	require.NoError(t, err)

	// Premise: the schema keeps the declared property list case-exact, so the
	// index owns two distinct fields named "Text" and "text".
	declared, ok := eng.GetSchema().GetFulltextIndex("pr1683_case_identity")
	require.True(t, ok, "declared fulltext index must be visible to the procedure")
	require.Equal(t, []string{"Text", "text"}, declared.Properties,
		"the index declares two case-distinct fields")

	// Premise: the store keeps both case-distinct values on 'mixed'.
	stored, err := exec.Execute(t.Context(),
		`MATCH (d:Doc {name:'mixed'}) RETURN d.Text AS upperValue, d.text AS lowerValue`, nil)
	require.NoError(t, err)
	require.Len(t, stored.Rows, 1)
	require.Len(t, stored.Rows[0], 2)
	require.Equal(t, "CaseExactAlpha", stored.Rows[0][0], "the store keeps the case-distinct Text value")
	require.Equal(t, "lowerExactBeta", stored.Rows[0][1], "the store keeps the case-distinct text value")

	// Controls: unambiguous exact-field selection already works, so the
	// failures below come from the collision, not from missing fixtures.
	require.Equal(t, []string{"upperOnly"}, pr1683IdentityNames(t, exec, "Text:UppercaseZulu"))
	require.Equal(t, []string{"lowerOnly"}, pr1683IdentityNames(t, exec, "text:lowerOnlyGamma"))

	t.Run("uppercase field must not read the lowercase property", func(t *testing.T) {
		assert.Empty(t, pr1683IdentityNames(t, exec, "Text:lowerOnlyGamma"),
			"NOR-148 folded NOR-294: 'lowerOnly' has no Text property, so a Text-scoped clause must not read its text value")
	})

	t.Run("lowercase field must not read the uppercase property", func(t *testing.T) {
		assert.Empty(t, pr1683IdentityNames(t, exec, "text:UppercaseZulu"),
			"NOR-148 folded NOR-294: 'upperOnly' has no text property, so a text-scoped clause must not read its Text value")
	})

	t.Run("undeclared fields are not searchable", func(t *testing.T) {
		assert.Empty(t, pr1683IdentityNames(t, exec, "name:mixed"), "an index over Text and text must not expose postings for name")
	})

	t.Run("one document exposes both case-distinct fields", func(t *testing.T) {
		// A single document build must satisfy both exact field clauses at
		// once; a collision can satisfy at most one of them.
		assert.Equal(t, []string{"mixed"}, pr1683IdentityNames(t, exec, "+Text:CaseExactAlpha +text:lowerExactBeta"),
			"both declared fields hold stored values, so neither may be discarded")
	})

	t.Run("each exact field selects its own value", func(t *testing.T) {
		upper := pr1683IdentityNames(t, exec, "Text:CaseExactAlpha")
		lower := pr1683IdentityNames(t, exec, "text:lowerExactBeta")
		t.Logf("individual field probes: Text:CaseExactAlpha -> %v; text:lowerExactBeta -> %v", upper, lower)
		assert.Equal(t, []string{"mixed"}, upper, "Text must select the stored Text value")
		assert.Equal(t, []string{"mixed"}, lower, "text must select the stored text value")
	})
}

package search

import (
	"context"
	"fmt"
	"math/bits"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiteralSignatureIndexNeverExcludesAnExactSubstring(t *testing.T) {
	documents := []string{"", "hotel", " leading and trailing ", "x\x00y", "ABC opaque", "αβ界é", "bad\xffbytes"}
	for width := 128; width <= 2048; width *= 2 {
		index := fulltextLiteralIndex{width: width}
		for slot, text := range documents {
			var signature literalSignature
			require.NoError(t, addLiteralText(t.Context(), &signature, text, width, nil))
			require.NoError(t, index.put(t.Context(), slot, &signature))
		}
		for _, text := range documents {
			for start := 0; start <= len(text); start++ {
				for end := start; end <= len(text); end++ {
					term := text[start:end]
					var query literalSignature
					require.NoError(t, addLiteralText(t.Context(), &query, term, width, nil))
					candidates := index.candidates(0, &query)
					for slot, body := range documents {
						if strings.Contains(body, term) {
							require.NotZero(t, candidates&(uint64(1)<<uint(slot)), "width%d document%q term%q", width, body, term)
						}
					}
				}
			}
		}
	}
}

func TestLiteralSignatureIndexReplacementClearsAllOldColumns(t *testing.T) {
	index := fulltextLiteralIndex{width: 128}
	var old, next literalSignature
	require.NoError(t, addLiteralText(t.Context(), &old, "old obsolete alpha", index.width, nil))
	require.NoError(t, addLiteralText(t.Context(), &next, "new present beta", index.width, nil))
	for _, slot := range []int{0, 63, 64, 127, 130} {
		require.NoError(t, index.put(t.Context(), slot, &old))
	}
	require.NoError(t, index.put(t.Context(), 64, &next))
	for bit := 0; bit < index.width; bit++ {
		got := index.columns[index.width+bit] & 1
		want := (next[bit>>6] >> uint(bit&63)) & 1
		require.Equal(t, want, got, "column%d", bit)
	}
	index.clear(64)
	require.Zero(t, index.live[1]&1)
	var empty literalSignature
	require.Equal(t, 1, bits.OnesCount64(index.candidates(1, &empty)))
	before := index.retainedBytes()
	for i := 0; i < 1000; i++ {
		require.NoError(t, index.put(t.Context(), 64, &next))
		index.clear(64)
	}
	require.Equal(t, before, index.retainedBytes(), "reusing a slot does not retain obsolete vocabulary or generations")
}

func TestLiteralSignatureIndexCancellationDoesNotPublishADocument(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	index := fulltextLiteralIndex{width: 128}
	var signature literalSignature
	require.ErrorIs(t, addLiteralText(ctx, &signature, "unadmitted", index.width, nil), context.Canceled)
	require.ErrorIs(t, index.put(ctx, 0, &signature), context.Canceled)
	require.Empty(t, index.columns)
	require.Empty(t, index.live)
}

func BenchmarkLiteralSignatureSelection(b *testing.B) {
	for _, size := range []int{128, 4096, 32768} {
		for _, width := range []int{128, 512} {
			b.Run(fmt.Sprintf("documents%d/width%d", size, width), func(b *testing.B) {
				index := fulltextLiteralIndex{width: width}
				documents := make([]string, size)
				for slot := range documents {
					documents[slot] = fmt.Sprintf("source document marker%06d", slot)
					var signature literalSignature
					require.NoError(b, addLiteralText(b.Context(), &signature, documents[slot], width, nil))
					require.NoError(b, index.put(b.Context(), slot, &signature))
				}
				for _, term := range []string{"marker000001", "source"} {
					for _, indexed := range []bool{false, true} {
						b.Run(fmt.Sprintf("%s/indexed%t", term, indexed), func(b *testing.B) {
							var query literalSignature
							require.NoError(b, addLiteralText(b.Context(), &query, term, width, nil))
							want := 1
							if term == "source" {
								want = size
							}
							restricted, err := index.mayRestrict(b.Context(), &query)
							require.NoError(b, err)
							useIndex := indexed && restricted
							b.ReportAllocs()
							b.ResetTimer()
							count, candidates := 0, 0
							for i := 0; i < b.N; i++ {
								count, candidates = 0, 0
								if useIndex {
									for block := range index.live {
										remaining := index.candidates(block, &query)
										for remaining != 0 {
											bit := bits.TrailingZeros64(remaining)
											remaining &= remaining - 1
											candidates++
											if strings.Contains(documents[block*64+bit], term) {
												count++
											}
										}
									}
								} else {
									for _, text := range documents {
										candidates++
										if strings.Contains(text, term) {
											count++
										}
									}
								}
							}
							b.StopTimer()
							require.Equal(b, want, count)
							b.ReportMetric(float64(candidates), "candidates/op")
							b.ReportMetric(float64(index.retainedBytes())/float64(size), "index-B/document")
						})
					}
				}
			})
		}
	}
}

func TestLiteralSignatureCommonBitsRemainConservativeAfterChurn(t *testing.T) {
	index := fulltextLiteralIndex{width: 512}
	documents := map[int]string{}
	for _, change := range []struct {
		slot int
		text string
	}{{0, "shared alpha"}, {1, "shared beta"}, {2, "shared gamma"}, {0, "other value"}, {1, ""}, {2, "different"}} {
		var signature literalSignature
		require.NoError(t, addLiteralText(t.Context(), &signature, change.text, index.width, nil))
		require.NoError(t, index.put(t.Context(), change.slot, &signature))
		documents[change.slot] = change.text
		for slot, text := range documents {
			var current literalSignature
			require.NoError(t, addLiteralText(t.Context(), &current, text, index.width, nil))
			for word := 0; word < index.width/64; word++ {
				require.Equal(t, index.common[word], index.common[word]&current[word], "slot%d word%d", slot, word)
			}
		}
	}
	index.clear(0)
	index.clear(1)
	var query literalSignature
	require.NoError(t, addLiteralText(t.Context(), &query, "nonexistent", index.width, nil))
	restricted, err := index.mayRestrict(t.Context(), &query)
	require.NoError(t, err)
	require.True(t, restricted)
}

func TestFulltextLiteralSelectionPreservesFieldsAndReplacement(t *testing.T) {
	view := &fulltextDocumentView{}
	view.resetDocuments(0, true)
	view.orderDirty = true
	for i := 0; i < 1024; i++ {
		id := fmt.Sprint(i)
		text := "ordinary text " + id
		fields := map[string]string{"text": text}
		if i == 19 {
			text = "alpha"
			fields = map[string]string{"title": "  alpha"}
		}
		require.NoError(t, view.replace(t.Context(), id, &FulltextDocument{Text: text, MatchFields: fields}))
	}
	documents := FulltextDocuments{view: view}
	selector := FulltextLiteralSelector{Op: FulltextLiteral, Text: "  alpha"}
	check := func(want []string) {
		t.Helper()
		var matched []string
		visited := 0
		require.NoError(t, ReadFulltextCandidates(t.Context(), documents, selector, FulltextLiteralTree{}, func(id string, doc *FulltextDocument) error {
			visited++
			if strings.Contains(doc.MatchFields["title"], selector.Text) {
				matched = append(matched, id)
			}
			return nil
		}))
		require.ElementsMatch(t, want, matched)
		require.Less(t, visited, documents.Count(), "actual candidate visits must narrow the corpus")
		require.NotNil(t, view.literals)
	}
	check([]string{"19"})
	frequencies, err := documents.BodyDocumentFrequencies(t.Context(), []string{"alpha", "  alpha", "a", ""})
	require.NoError(t, err)
	require.Equal(t, map[string]int{"alpha": 1, "  alpha": 0, "a": 1024, "": 1024}, frequencies, "body statistics stay independent of untrimmed field matching")
	// Establish the one-extra-live-row high-water mark before testing repeated
	// slot reuse; growing from 1,024 to 1,025 live rows legitimately adds a block.
	require.NoError(t, view.replace(t.Context(), "capacity-probe", &FulltextDocument{Text: "probe"}))
	require.NoError(t, view.replace(t.Context(), "capacity-probe", nil))
	capacity := view.literals.retainedBytes()
	for i := 0; i < 300; i++ {
		id := fmt.Sprint(i + 2000)
		require.NoError(t, view.replace(t.Context(), id, &FulltextDocument{Text: "alpha", MatchFields: map[string]string{"title": "  alpha"}}))
		require.NoError(t, view.replace(t.Context(), id, nil))
	}
	require.Less(t, view.literals.retainedBytes()-capacity, int64(1024), "churn reuses slots instead of growing with historical IDs")
	require.NoError(t, view.replace(t.Context(), "19", nil))
	check(nil)
	require.NoError(t, view.replace(t.Context(), "19", &FulltextDocument{Text: "alpha", MatchFields: map[string]string{"title": "  alpha"}}))
	check([]string{"19"})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, view.replace(cancelled, "19", &FulltextDocument{Text: "changed"}), context.Canceled)
	check([]string{"19"})
}

// This component control issues real cancellation at a context checkpoint
// during hashing. It does not stand in for public request-lifetime tests.
func TestFulltextLiteralReplacementCancellationKeepsPriorPosting(t *testing.T) {
	view := &fulltextDocumentView{}
	view.resetDocuments(0, true)
	view.orderDirty = true
	for i := 0; i < 128; i++ {
		require.NoError(t, view.replace(t.Context(), fmt.Sprint(i), &FulltextDocument{Text: "original"}))
	}
	index, err := view.literalIndex(t.Context())
	require.NoError(t, err)
	columns := append([]uint64(nil), index.columns...)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	checkpoints := &fulltextCountingContext{Context: ctx, onCheck: func(check int) {
		if check == 16 {
			cancel()
		}
	}}
	err = view.replace(checkpoints, "0", &FulltextDocument{Text: strings.Repeat("new work ", 16384)})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 16, checkpoints.checks)
	require.Equal(t, "original", view.documents["0"].Text)
	require.Equal(t, columns, index.columns, "failed preparation cannot replace live postings")
	require.NoError(t, view.replace(t.Context(), "0", &FulltextDocument{Text: "healthy retry"}))
	require.Equal(t, "healthy retry", view.documents["0"].Text)
}

func TestLiteralChunkWorkspacePreservesCrossBoundaryGrams(t *testing.T) {
	text := strings.Repeat("a", 254) + "XYZ" + strings.Repeat("b", 254) + "LMN" + strings.Repeat("c", 256)
	for _, width := range []int{512, 1024, 2048} {
		workspace := make(map[string]*literalSignature)
		var expected, first, repeat literalSignature
		require.NoError(t, addLiteralText(t.Context(), &expected, text, width, nil))
		require.NoError(t, addLiteralText(t.Context(), &first, text, width, workspace))
		require.NoError(t, addLiteralText(t.Context(), &repeat, text, width, workspace))
		require.Equal(t, expected, first)
		require.Equal(t, expected, repeat)
		index := fulltextLiteralIndex{width: width}
		require.NoError(t, index.put(t.Context(), 0, &first))
		for _, term := range []string{"XYZ", "aXYZb", "LMN", "bLMNc", text[253:258], text[509:515]} {
			require.Contains(t, text, term)
			var query literalSignature
			require.NoError(t, addLiteralText(t.Context(), &query, term, width, nil))
			require.NotZero(t, index.candidates(0, &query)&1, "width%d term%q", width, term)
		}
		require.LessOrEqual(t, len(workspace), 32)
	}
}

func TestFulltextLiteralSelectorValidationDoesNotDependOnCorpusSize(t *testing.T) {
	for _, size := range []int{0, 10, 128} {
		view := &fulltextDocumentView{}
		view.resetDocuments(0, true)
		view.orderDirty = true
		for i := 0; i < size; i++ {
			require.NoError(t, view.replace(t.Context(), fmt.Sprint(i), &FulltextDocument{Text: "ordinary text"}))
		}
		documents := FulltextDocuments{view: view}
		for _, selector := range []FulltextLiteralSelector{
			{Op: 255},
			{Op: FulltextAnyOf, Children: []FulltextLiteralSelector{{}, {Op: 255}}},
		} {
			visits := 0
			err := ReadFulltextCandidates(t.Context(), documents, selector, FulltextLiteralTree{}, func(string, *FulltextDocument) error { visits++; return nil })
			require.Error(t, err)
			require.Zero(t, visits)
		}
		visits := 0
		require.NoError(t, ReadFulltextCandidates(t.Context(), documents, FulltextLiteralSelector{Op: FulltextAnyOf}, FulltextLiteralTree{}, func(string, *FulltextDocument) error { visits++; return nil }))
		require.Zero(t, visits, "empty union is empty on every read representation")
	}
}

func TestLiteralSignatureRecoversBroadQueryProofAfterDeletion(t *testing.T) {
	index := fulltextLiteralIndex{width: 512}
	var shared, other literalSignature
	require.NoError(t, addLiteralText(t.Context(), &shared, "shared unique literal", index.width, nil))
	require.NoError(t, addLiteralText(t.Context(), &other, "other data", index.width, nil))
	require.NoError(t, index.put(t.Context(), 0, &shared))
	require.NoError(t, index.put(t.Context(), 1, &other))
	restricted, err := index.mayRestrict(t.Context(), &shared)
	require.NoError(t, err)
	require.True(t, restricted)
	index.clear(1)
	restricted, err = index.mayRestrict(t.Context(), &shared)
	require.NoError(t, err)
	require.False(t, restricted)
	for word := 0; word < index.width/64; word++ {
		require.Equal(t, shared[word], shared[word]&index.common[word])
	}
	require.NoError(t, index.put(t.Context(), 1, &other))
	restricted, err = index.mayRestrict(t.Context(), &shared)
	require.NoError(t, err)
	require.True(t, restricted, "a newly admitted document retires obsolete universal proofs")
}

func TestLiteralSignatureBroadQueryProofStopsOnCancellation(t *testing.T) {
	index := fulltextLiteralIndex{width: 512}
	var signature literalSignature
	require.NoError(t, addLiteralText(t.Context(), &signature, "shared literal", index.width, nil))
	require.NoError(t, index.put(t.Context(), 0, &signature))
	require.NoError(t, index.put(t.Context(), 4096, &signature))
	index.common = literalSignature{} // conservative state after earlier churn
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	progress := &fulltextCountingContext{Context: ctx, onCheck: func(check int) {
		if check == 16 {
			cancel()
		}
	}}
	_, err := index.mayRestrict(progress, &signature)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, literalSignature{}, index.common, "an interrupted block proof is never cached as complete")
}

// Candidate selection must not allocate once per posting block or per hit.
// These are prepared-index component measurements; public hydration costs are
// measured separately by BenchmarkNOR148DistinctTerms.
func TestFulltextLiteralSelectionAllocationsDoNotScaleWithPopulation(t *testing.T) {
	ctx := t.Context()
	selectors := []FulltextLiteralSelector{
		{Op: FulltextLiteral, Text: "needle"},
		{Op: FulltextAllOf, Children: []FulltextLiteralSelector{
			{Op: FulltextLiteral, Text: "needle"},
			{Op: FulltextAnyOf, Children: []FulltextLiteralSelector{
				{Op: FulltextLiteral, Text: "blue"},
				{Op: FulltextLiteral, Text: "green"},
			}},
		}},
	}
	for _, selector := range selectors {
		var first float64
		for _, size := range []int{1024, 4096} {
			view := &fulltextDocumentView{}
			view.resetDocuments(size, true)
			for i := 0; i < size; i++ {
				text := "ordinary document"
				if i%16 == 0 {
					text = "needle blue"
				}
				require.NoError(t, view.replace(ctx, fmt.Sprint(i), &FulltextDocument{Text: text}))
			}
			documents := FulltextDocuments{view: view}
			_, err := view.literalIndex(ctx)
			require.NoError(t, err)
			visits := 0
			visit := func(_ string, document *FulltextDocument) error {
				if strings.Contains(document.Text, "needle") {
					visits++
				}
				return nil
			}
			require.NoError(t, ReadFulltextCandidates(ctx, documents, selector, FulltextLiteralTree{}, visit))
			require.Equal(t, size/16, visits)
			var readError error
			allocations := testing.AllocsPerRun(100, func() {
				readError = ReadFulltextCandidates(ctx, documents, selector, FulltextLiteralTree{}, visit)
			})
			require.NoError(t, readError)
			require.Zero(t, allocations, "prepared small-query selection must not allocate a plan on each read")
			if size == 1024 {
				first = allocations
			} else {
				require.Equal(t, first, allocations, "selection allocations must depend on query shape, not posting blocks or hit count")
			}
			t.Logf("operation=%d documents=%d allocations=%g", selector.Op, size, allocations)
		}
	}
}

func TestFulltextLiteralSelectionBorrowsIsolatedBoundedClearedScratch(t *testing.T) {
	view := &fulltextDocumentView{}
	view.resetDocuments(128, true)
	for i := 0; i < 128; i++ {
		require.NoError(t, view.replace(t.Context(), fmt.Sprint(i), &FulltextDocument{Text: "ordinary text"}))
	}
	documents := FulltextDocuments{view: view}
	_, err := view.literalIndex(t.Context())
	require.NoError(t, err)
	// This new row has a slot in a later block than the first callback, so
	// corrupting an outer plan during a nested call cannot hide in that block.
	require.NoError(t, view.replace(t.Context(), "sentinel", &FulltextDocument{Text: "unique sentinel"}))
	outer := FulltextLiteralSelector{Op: FulltextAllOf, Children: []FulltextLiteralSelector{{Op: FulltextLiteral, Text: "ordinary"}, {Op: FulltextLiteral, Text: "text"}}}
	var plain, nested []string
	require.NoError(t, ReadFulltextCandidates(t.Context(), documents, outer, FulltextLiteralTree{}, func(id string, _ *FulltextDocument) error { plain = append(plain, id); return nil }))
	called := false
	require.NoError(t, ReadFulltextCandidates(t.Context(), documents, outer, FulltextLiteralTree{}, func(id string, _ *FulltextDocument) error {
		nested = append(nested, id)
		if !called {
			called = true
			inner := FulltextLiteralSelector{Op: FulltextAnyOf, Children: []FulltextLiteralSelector{{Op: FulltextLiteral, Text: "sentinel"}, {Op: FulltextLiteral, Text: "absent"}}}
			var ids []string
			require.NoError(t, ReadFulltextCandidates(t.Context(), documents, inner, FulltextLiteralTree{}, func(id string, _ *FulltextDocument) error { ids = append(ids, id); return nil }))
			require.Equal(t, []string{"sentinel"}, ids)
		}
		return nil
	}))
	require.True(t, called)
	require.Len(t, plain, 128)
	require.ElementsMatch(t, plain, nested, "a nested read must not overwrite its caller's live plan")
	index := view.literals
	require.LessOrEqual(t, cap(index.plans), 16)
	for _, plan := range index.plans[:cap(index.plans)] {
		require.Equal(t, fulltextLiteralPlan{}, plan, "retained compiler scratch contains no previous plan references or masks")
	}
	large := FulltextLiteralSelector{Op: FulltextAnyOf, Children: make([]FulltextLiteralSelector, 80)}
	for i := range large.Children {
		large.Children[i] = FulltextLiteralSelector{Op: FulltextLiteral, Text: fmt.Sprintf("missing-needle-%03d", i)}
	}
	require.NoError(t, ReadFulltextCandidates(t.Context(), documents, large, FulltextLiteralTree{}, func(string, *FulltextDocument) error { return nil }))
	require.LessOrEqual(t, cap(index.plans), 16, "an oversized query cannot enlarge retained compiler scratch")
}

// The compiler, rather than syntax adapters, owns request cancellation.
type fulltextSkippedChildReader struct {
	cancel      context.CancelFunc
	calls       *int
	describeErr error
	childErr    error
}

func (r fulltextSkippedChildReader) Describe(context.Context, int) (FulltextLiteralNode, error) {
	return FulltextLiteralNode{Op: FulltextAllOf, Children: 1000}, r.describeErr
}
func (r fulltextSkippedChildReader) Child(context.Context, int, FulltextLiteralOperation, int) (int, bool, error) {
	*r.calls++
	if r.cancel != nil {
		r.cancel()
	}
	return 0, false, r.childErr
}
func TestFulltextLiteralReaderPropagatesCancellationAndErrors(t *testing.T) {
	view := &fulltextDocumentView{}
	view.resetDocuments(0, true)
	documents := FulltextDocuments{view: view}
	visits := 0
	visit := func(string, *FulltextDocument) error { visits++; return nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	err := ReadFulltextCandidates(ctx, documents, 0, fulltextSkippedChildReader{cancel: cancel, calls: &calls}, visit)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "compiler must stop skipped-child enumeration")
	require.Zero(t, visits)
	for _, atDescribe := range []bool{true, false} {
		failure := fmt.Errorf("reader boundary failed: describe=%t", atDescribe)
		calls = 0
		reader := fulltextSkippedChildReader{calls: &calls}
		if atDescribe {
			reader.describeErr = failure
		} else {
			reader.childErr = failure
		}
		require.ErrorIs(t, ReadFulltextCandidates(t.Context(), documents, 0, reader, visit), failure)
		if atDescribe {
			require.Zero(t, calls)
		} else {
			require.Equal(t, 1, calls)
		}
	}
}

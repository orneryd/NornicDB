package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// clusterTerms keeps counts grouped by level so a profile reads its top terms
// directly; the result must equal ranking the whole vocabulary.
func TestClusterTermsProfileMatchesFullRanking(t *testing.T) {
	terms := newClusterTerms()
	require.Nil(t, (*clusterTerms)(nil).profile(8))
	require.Nil(t, terms.profile(8), "no tokens, no profile")

	for i := 0; i < 20; i++ {
		terms.add(fmt.Sprintf("t%02d", i), float64(i%5+1))
	}
	require.Equal(t, float64(60), terms.total)
	require.Equal(t, []float64{1, 2, 3, 4, 5}, terms.levels)

	// Top 6: the four count-5 terms, then two of the four count-4 terms by name.
	got := terms.profile(6)
	require.Equal(t, map[string]float64{
		"t04": 5.0 / 60, "t09": 5.0 / 60, "t14": 5.0 / 60, "t19": 5.0 / 60,
		"t03": 4.0 / 60, "t08": 4.0 / 60,
	}, got)
	require.Len(t, terms.profile(100), 20, "a small vocabulary is returned whole")

	// Moving counts keeps levels exact; a count reaching zero drops the term.
	terms.add("t04", -5)
	terms.add("t09", -10) // clamps at zero
	terms.add("t00", 9)
	terms.add("t01", 0)
	require.NotContains(t, terms.counts, "t04")
	require.NotContains(t, terms.counts, "t09")
	require.Equal(t, float64(10), terms.counts["t00"])
	require.Equal(t, float64(59), terms.total)
	require.Equal(t, []float64{1, 2, 3, 4, 5, 10}, terms.levels, "count 10 added")
	terms.add("t05", -1)
	terms.add("t10", -1)
	terms.add("t15", -1)
	require.Equal(t, []float64{2, 3, 4, 5, 10}, terms.levels, "an emptied count leaves the levels")
	require.Equal(t, float64(56), terms.total)
	for level, set := range terms.byCount {
		for tok := range set {
			require.Equal(t, level, terms.counts[tok])
		}
	}
	got = terms.profile(3)
	require.Equal(t, map[string]float64{"t00": 10.0 / 56, "t14": 5.0 / 56, "t19": 5.0 / 56}, got)
}

// A node change nets its tokens per cluster, so re-indexing unchanged text
// changes no counts and marks no profile for recomputation.
func TestClusterTokenDeltaNetsUnchangedText(t *testing.T) {
	delta := map[int]map[string]float64{}
	addTokenDelta(delta, 0, map[string]float64{"alpha": 2, "beta": 1}, -1)
	addTokenDelta(delta, 0, map[string]float64{"alpha": 2, "beta": 1}, 1)
	addTokenDelta(delta, 1, nil, 1)
	require.Empty(t, delta[0])
	require.NotContains(t, delta, 1)

	gen := &clusterGeneration{}
	for cluster, tokens := range delta {
		gen.addTerms(cluster, tokens, 1)
	}
	require.Empty(t, gen.dirty)

	addTokenDelta(delta, 0, map[string]float64{"gamma": 1}, 1)
	addTokenDelta(delta, 2, map[string]float64{"alpha": 1}, -1)
	gen.terms = map[int]*clusterTerms{2: newClusterTerms()}
	gen.terms[2].add("alpha", 3)
	for cluster, tokens := range delta {
		gen.addTerms(cluster, tokens, 1)
	}
	require.Equal(t, float64(1), gen.terms[0].counts["gamma"])
	require.Equal(t, float64(2), gen.terms[2].counts["alpha"])
	require.Contains(t, gen.dirty, 0)
	require.Contains(t, gen.dirty, 2)
}

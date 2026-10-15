package cypher

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func coldMapListQuery(count int) string {
	return "RETURN [" + strings.Repeat("{x:1},", count-1) + "{x:1}] AS values"
}

// Calling the validation owner directly bypasses the valid-syntax cache: every
// sample performs the work a first execution must do. Eight times the input
// may take generous linear overhead, but must not approach 64 times the work.
func TestStaticMapKeysColdScaling(t *testing.T) {
	assertColdMapScaling(t, validateStaticMapKeys)
}

func TestPatternDetectionColdMapScaling(t *testing.T) {
	assertColdMapScaling(t, func(query string) error {
		require.False(t, hasPatternOrSubquery(query))
		return nil
	})
}

func assertColdMapScaling(t *testing.T, validate func(string) error) {
	t.Helper()
	small, large := coldMapListQuery(512), coldMapListQuery(4096)
	var smallTimes, largeTimes []time.Duration
	for range 5 {
		start := time.Now()
		require.NoError(t, validate(small))
		smallTimes = append(smallTimes, time.Since(start))
		start = time.Now()
		require.NoError(t, validate(large))
		largeTimes = append(largeTimes, time.Since(start))
	}
	slices.Sort(smallTimes)
	slices.Sort(largeTimes)
	smallMedian, largeMedian := smallTimes[len(smallTimes)/2], largeTimes[len(largeTimes)/2]
	ratio := float64(largeMedian) / float64(smallMedian)
	t.Logf("cold map-list scan: 512=%s 4096=%s ratio=%.2f", smallMedian, largeMedian, ratio)
	require.Less(t, ratio, 24.0, "8x fixed-size maps must not cause quadratic prefix rescanning")
}

func BenchmarkStaticMapKeysColdLists(b *testing.B) {
	for _, count := range []int{64, 512, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			query := coldMapListQuery(count)
			b.ReportAllocs()
			b.SetBytes(int64(len(query)))
			b.ResetTimer()
			for range b.N {
				if err := validateStaticMapKeys(query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSubqueryBracePredecessorConsistency(t *testing.T) {
	for _, position := range []int{-1, 0, 2} {
		require.False(t, precedingSubqueryExpressionKeyword("", position))
	}
	require.False(t, precedingSubqueryExpressionKeyword("RETURN 1", 3))
	for _, gap := range []string{" ", "\f", "\v", " /*gap*/ ", " //gap\n"} {
		query := "RETURN COUNT" + gap + "{ RETURN 1 AS x } AS v"
		brace := strings.IndexByte(query, '{')
		scanner := newSubqueryBraceScanner(query, 0)
		require.True(t, precedingSubqueryExpressionKeyword(query, brace), "%q", query)
		require.True(t, scanner.at(brace), "%q", query)
		require.True(t, scanner.at(brace), "lookahead may revisit the same brace")
		require.True(t, newMapPatternContext(query).subqueries[brace], "%q", query)
	}
}

func TestSubqueryBraceForwardLexicalGuards(t *testing.T) {
	for _, test := range []struct {
		query      string
		subqueries int
	}{
		{"RETURN [{x:'COUNT { RETURN 1 }'}, COUNT/*gap*/{ RETURN 1 }]", 1},
		{"RETURN [/* COUNT { RETURN 1 } */ {x:1}]", 0},
		{"MATCH (n:Count {x:1}) RETURN n", 0},
		{"RETURN n.`count` {x:1}", 0},
		{"RETURN COUNT { RETURN:1 }", 0},
		{"CALL (a,b) /*gap*/ { RETURN a,b } RETURN a", 1},
	} {
		t.Run(test.query, func(t *testing.T) {
			scanner := newSubqueryBraceScanner(test.query, 0)
			context := newMapPatternContext(test.query)
			found := 0
			for index := 0; index < len(test.query); index++ {
				if test.query[index] != '{' {
					continue
				}
				actual := scanner.at(index)
				require.Equal(t, context.subqueries[index], actual)
				if actual {
					found++
				}
			}
			require.Equal(t, test.subqueries, found)
		})
	}
}

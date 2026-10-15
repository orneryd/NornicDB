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
	small, large := coldMapListQuery(512), coldMapListQuery(4096)
	var smallTimes, largeTimes []time.Duration
	for range 5 {
		start := time.Now()
		require.NoError(t, validateStaticMapKeys(small))
		smallTimes = append(smallTimes, time.Since(start))
		start = time.Now()
		require.NoError(t, validateStaticMapKeys(large))
		largeTimes = append(largeTimes, time.Since(start))
	}
	slices.Sort(smallTimes)
	slices.Sort(largeTimes)
	smallMedian, largeMedian := smallTimes[len(smallTimes)/2], largeTimes[len(largeTimes)/2]
	ratio := float64(largeMedian) / float64(smallMedian)
	t.Logf("uncached static-map validation: 512=%s 4096=%s ratio=%.2f", smallMedian, largeMedian, ratio)
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

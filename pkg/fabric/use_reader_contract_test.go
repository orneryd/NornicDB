package fabric

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestUseSplitReaderKeepsOriginalOffsets(t *testing.T) {
	q := "WITH 7 AS use WITH use AS x USE comp.one RETURN x"
	want := strings.LastIndex(q, "USE")
	var positions []int
	reader := func(text string, start, end int, keyword string) bool {
		require.Equal(t, q, text)
		require.Equal(t, "USE", keyword)
		require.Equal(t, start+3, end)
		positions = append(positions, start)
		return start != want
	}
	prefix, suffix, found := splitAtTopLevelUse(q, reader)
	require.True(t, found)
	require.Equal(t, q[:want], prefix)
	require.Equal(t, q[want:], suffix)
	require.Equal(t, []int{strings.Index(q, "use"), strings.Index(q, "use AS x"), want}, positions)
	prefix, suffix, found = splitAtTopLevelUse(q, nil)
	require.True(t, found)
	first := strings.Index(q, "use")
	require.Equal(t, q[:first], prefix)
	require.Equal(t, q[first:], suffix)
	_, _, found = splitAtTopLevelUse("USE GRAPH use RETURN 1", nil)
	require.True(t, found) // nil reader retains its lexical, context-free contract.
}

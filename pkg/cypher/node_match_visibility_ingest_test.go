package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var nodeMatchVisibilitySink bool

func TestNodePatternChecksOnlyRequestedVisibleProperties(t *testing.T) {
	e, be := reviewPropertyExecutor(t)
	node, err := be.GetNode("review:a")
	require.NoError(t, err)
	short := *node
	short.Properties = map[string]interface{}{"title": "a", "body": "hidden"}
	long := short
	long.Properties = map[string]interface{}{"title": "a", "body": "hidden"}
	for i := 0; i < 24; i++ {
		long.Properties[fmt.Sprintf("unselected%d", i)] = i
	}
	match := map[string]interface{}{"title": "a"}
	require.True(t, e.nodeMatchesProps(&long, match))
	require.False(t, e.nodeMatchesProps(&long, map[string]interface{}{"body": "hidden"}))
	require.False(t, e.nodeMatchesProps(&long, map[string]interface{}{"title": nil}))
	shortAllocs := testing.AllocsPerRun(20, func() { nodeMatchVisibilitySink = e.nodeMatchesProps(&short, match) })
	longAllocs := testing.AllocsPerRun(20, func() { nodeMatchVisibilitySink = e.nodeMatchesProps(&long, match) })
	require.LessOrEqual(t, longAllocs, shortAllocs+5, "unselected properties must not cause extra visibility reads")

	for query, want := range map[string]int64{
		"MATCH (n:Doc {title: 'a'}) RETURN count(n)":     1,
		"MATCH (n:Doc {body: 'hidden'}) RETURN count(n)": 0,
	} {
		result, err := e.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{want}}, result.Rows)
	}
}

func BenchmarkNodePatternVisibility(b *testing.B) {
	e, be := reviewPropertyExecutor(b)
	node, err := be.GetNode("review:a")
	require.NoError(b, err)
	for i := 0; i < 8; i++ {
		node.Properties[fmt.Sprintf("unselected%d", i)] = i
	}
	match := map[string]interface{}{"title": "a"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeMatchVisibilitySink = e.nodeMatchesProps(node, match)
	}
}

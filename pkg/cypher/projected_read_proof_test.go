package cypher

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedReadProofMatchesCanonicalAnalysis(t *testing.T) {
	for _, test := range []struct {
		variable, expression string
		inline               map[string]any
	}{
		{"p", "", nil},
		{"p", "RETURN p.a + $amount", map[string]any{"key": 1}},
		{"q", "p.a + q.b", map[string]any{"b": 2, "other": 3}},
		{"p", "RETURN p.`field.name`, p.a, p.a", nil},
		{"p", "RETURN p", map[string]any{"key": 1}},
		{"p", "RETURN properties(p)", nil},
		{"p", "RETURN p['name']", nil},
		{"n", "n.name STARTS WITH $prefix OR n.status = $status", nil},
	} {
		want := parsePipelineNodePredicateProperties(test.variable, test.expression, test.inline)
		for i := 0; i < 2; i++ {
			require.Equal(t, want, pipelineNodePredicateProperties(test.variable, test.expression, test.inline), test.expression)
		}
	}
}

func TestProjectedReadProjectionProofInputsAndOwnership(t *testing.T) {
	text := "p.a + q.b + $amount"
	first := pipelineNodePredicateProperties("p", text, map[string]any{"key": 1})
	require.Equal(t, []string{"a", "key"}, first)
	first[0] = "caller changed it"
	require.Equal(t, []string{"a", "other"}, pipelineNodePredicateProperties("p", text, map[string]any{"other": 2}))
	require.Equal(t, []string{"b"}, pipelineNodePredicateProperties("q", text, nil))
	for i := 0; i < 2; i++ {
		require.Nil(t, pipelineNodePredicateProperties("p", "RETURN p", map[string]any{"key": 1}))
		require.Equal(t, []string{}, pipelineNodePredicateProperties("q", "RETURN p", nil))
		require.Equal(t, []string{"field.name"}, pipelineNodePredicateProperties("p", "p.`field.name`", nil))
	}
}

func TestProjectedReadProjectionProofConcurrentOwnership(t *testing.T) {
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Go(func() {
			for n := 0; n < 50; n++ {
				fields := pipelineNodePredicateProperties("p", "RETURN p.a, p.body", nil)
				if len(fields) != 2 || fields[0] != "a" || fields[1] != "body" {
					t.Error("projection proof changed across callers", fields)
					return
				}
				fields[0] = "caller changed it"
			}
		})
	}
	workers.Wait()
}

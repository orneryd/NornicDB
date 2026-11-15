package cypher

import (
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestExperimentalProjectionProofInputsAndOwnership(t *testing.T) {
	previous := config.IsProjectedReadsEnabled()
	t.Cleanup(func() { config.SetProjectedReadsEnabled(previous) })
	for _, enabled := range []bool{false, true} {
		config.SetProjectedReadsEnabled(enabled)
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
}

func TestExperimentalProjectionProofConcurrentOwnership(t *testing.T) {
	previous := config.IsProjectedReadsEnabled()
	t.Cleanup(func() { config.SetProjectedReadsEnabled(previous) })
	config.SetProjectedReadsEnabled(true)
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

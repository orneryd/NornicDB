package cypher

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNOR120UnchangedGraphTieSelection uses a fixed twelve-node graph and 64
// real uncached executions per parser. x and y share three neighbours with s,
// of degrees 4, 5 and 6. Their mathematical Adamic-Adar scores are identical.
// This bounded reproduction records actual selection/score variants before
// asserting the shipped stable-top-k contract. Production map iteration is
// nondeterministic; not every possible iteration schedule must expose the bug.
func TestNOR120UnchangedGraphTieSelection(t *testing.T) {
	const repeats = 64
	for _, lane := range linkPredictionLanes() {
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor120_noise_"+lane.name)
			linkPredictionMultiTermGraph(t, engine)
			exec := linkPredictionExecutor(engine)
			count := func() {
				result, err := exec.Execute(context.Background(), "MATCH (n:P) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(12)}}, result.Rows)
			}
			count()
			control := linkPredictionAnswerOf(t, exec, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode:'s', topK:1})")
			require.Equal(t, []string{"x"}, control.targets)
			require.Equal(t, []float64{3}, control.scores)
			both := linkPredictionAnswerOf(t, exec, "CALL gds.linkPrediction.adamicAdar.stream({sourceNode:'s', topK:2})")
			require.ElementsMatch(t, []string{"x", "y"}, both.targets)
			wantScore := 1/math.Log(4) + 1/math.Log(5) + 1/math.Log(6)
			for _, score := range both.scores {
				require.InDelta(t, wantScore, score, 1e-12)
			}
			selected := map[string]int{}
			scoreBits := map[string]int{}
			for run := 0; run < repeats; run++ {
				answer := linkPredictionAnswerOf(t, exec, "CALL gds.linkPrediction.adamicAdar.stream({sourceNode:'s', topK:1})")
				require.Len(t, answer.targets, 1)
				require.InDelta(t, wantScore, answer.scores[0], 1e-12)
				selected[answer.targets[0]]++
				scoreBits[fmt.Sprintf("%016x", math.Float64bits(answer.scores[0]))]++
			}
			count()
			t.Logf("unchanged 12-node graph; %d uncached repeats: selected=%v scoreBits=%v", repeats, selected, scoreBits)
			require.Len(t, scoreBits, 1, "unchanged graph must keep identical score bits")
			keys := make([]string, 0, len(selected))
			for key := range selected {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			require.Equal(t, []string{"x"}, keys, "unchanged graph must keep the same mathematically tied top-1 candidate, by target ID")
		})
	}
}

// linkPredictionMultiTermGraph gives s, x and y shared hubs of degrees 4, 5 and 6.
func linkPredictionMultiTermGraph(t *testing.T, engine storage.Engine) {
	t.Helper()
	for _, id := range []string{"s", "x", "y"} {
		linkPredictionNode(t, engine, id)
	}
	for index := 0; index < 3; index++ {
		hub := fmt.Sprintf("h%d", index)
		linkPredictionNode(t, engine, hub)
		for _, id := range []string{"s", "x", "y"} {
			linkPredictionEdge(t, engine, hub+"-"+id, hub, id, "R")
		}
		for pad := 0; pad <= index; pad++ {
			id := fmt.Sprintf("pad%d_%d", index, pad)
			linkPredictionNode(t, engine, id)
			linkPredictionEdge(t, engine, hub+"-"+id, hub, id, "R")
		}
	}
}

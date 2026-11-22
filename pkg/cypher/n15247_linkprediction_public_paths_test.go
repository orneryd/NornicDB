package cypher

import (
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestN15247MultiTermScoresAcrossPublicPaths exercises the common scoring
// owner through scalar functions, raw streams and hybrid/ensemble streams.
// The IDs and graph never change; repeated calls use the existing uncached
// executor. Numeric tolerances check the formula; exact bits check stability.
func TestN15247MultiTermScoresAcrossPublicPaths(t *testing.T) {
	const repeats = 32
	for _, lane := range linkPredictionLanes() {
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "n15247_public_"+lane.name)
			linkPredictionMultiTermGraph(t, engine)
			exec := linkPredictionExecutor(engine)
			var aa, ra float64
			for _, degree := range []float64{4, 5, 6} {
				aa += 1 / math.Log(degree)
				ra += 1 / degree
			}
			assertGraph := func() {
				result, err := exec.Execute(t.Context(), "MATCH (n:P) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(12)}}, result.Rows)
			}
			assertGraph()

			t.Run("scalar", func(t *testing.T) {
				query := `MATCH (s:P {key:'s'}), (x:P {key:'x'}), (y:P {key:'y'})
					RETURN gds.alpha.linkprediction.adamicAdar(s,x) AS aax,
					gds.alpha.linkprediction.adamicAdar(s,y) AS aay,
					gds.alpha.linkprediction.resourceAllocation(s,x) AS rax,
					gds.alpha.linkprediction.resourceAllocation(s,y) AS ray`
				var firstBits []uint64
				for run := 0; run < repeats; run++ {
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"aax", "aay", "rax", "ray"}, result.Columns)
					require.Len(t, result.Rows, 1)
					require.Len(t, result.Rows[0], 4)
					bits := make([]uint64, 4)
					for i, want := range []float64{aa, aa, ra, ra} {
						got, ok := result.Rows[0][i].(float64)
						require.True(t, ok)
						require.InDelta(t, want, got, 1e-12)
						bits[i] = math.Float64bits(got)
					}
					require.Equal(t, bits[0], bits[1], "AA candidates must tie exactly")
					require.Equal(t, bits[2], bits[3], "RA candidates must tie exactly")
					if run == 0 {
						firstBits = bits
					}
					require.Equal(t, firstBits, bits, "scalar run %d", run)
				}
			})

			// Public predict keeps each algorithm's own scale, with no
			// candidate-relative rescaling. No fixture node has embeddings;
			// explicit weights isolate its topological score.
			paa, pra := math.Tanh(aa/5), math.Tanh(ra/5)
			ensemble := 0.1*(1-1/(1+3.0/2)) + 0.2 + 0.3*paa + 0.15*(math.Log10(9)/4) + 0.25*pra
			for _, tc := range []struct {
				name, procedure, config string
				want                    float64
			}{
				{"raw_aa", "adamicAdar", "", aa},
				{"raw_ra", "resourceAllocation", "", ra},
				{"predict_aa", "predict", ", algorithm:'adamic_adar', topologyWeight:1.0, semanticWeight:0.0", paa},
				{"predict_ra", "predict", ", algorithm:'resource_allocation', topologyWeight:1.0, semanticWeight:0.0", pra},
				{"predict_ensemble", "predict", ", algorithm:'ensemble', topologyWeight:1.0, semanticWeight:0.0", ensemble},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var firstBits uint64
					for run := 0; run < repeats; run++ {
						for _, k := range []int{1, 2} {
							query := fmt.Sprintf("CALL gds.linkPrediction.%s.stream({sourceNode:'s', topK:%d%s})", tc.procedure, k, tc.config)
							answer := linkPredictionAnswerOf(t, exec, query)
							require.Equal(t, []string{"x", "y"}[:k], answer.targets, "run %d topK %d", run, k)
							require.Len(t, answer.scores, k)
							for _, score := range answer.scores {
								require.InDelta(t, tc.want, score, 1e-12)
								bits := math.Float64bits(score)
								if run == 0 && k == 1 {
									firstBits = bits
								}
								require.Equal(t, firstBits, bits, "run %d topK %d", run, k)
							}
						}
					}
				})
			}
			assertGraph()
		})
	}
}

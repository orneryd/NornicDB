package search

// Author: hnsw-stall implementation lane (Claude Fable 5.2), 10 Nov 2026.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// catchUpSeed is the i-th of 40 directions spread evenly over the sphere: the
// vectors a catch-up case starts with. Any two of them, and any of them and a
// vector the case writes later, differ by several int8 steps, so the graph
// check tells every key from every other.
func catchUpSeed(i int) []float32 {
	z := 1 - float64(2*i+1)/40
	r := math.Sqrt(1 - z*z)
	angle := float64(i) * 2.399963
	return []float32{float32(r * math.Cos(angle)), float32(r * math.Sin(angle)), float32(z)}
}

func hnswHoldsLive(idx *HNSWIndex, id string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	internalID, ok := idx.idToInternal[id]
	return ok && validHNSWIndex(internalID, len(idx.deleted)) && !idx.deleted[internalID]
}

// hnswCoordinates is what a graph itself keeps of one key's vector: its
// stored coordinates, the int8 codes of a scalar-quantized graph, or both. A
// graph the search service publishes looks its vectors up in the vector store
// and keeps only the codes.
type hnswCoordinates struct {
	stored []float32
	codes  []byte
}

func hnswOwnCoordinates(graph *HNSWIndex, id string) (hnswCoordinates, error) {
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	internalID, ok := graph.idToInternal[id]
	if !ok || !validHNSWIndex(internalID, len(graph.deleted)) || graph.deleted[internalID] {
		return hnswCoordinates{}, fmt.Errorf("the graph lost %s", id)
	}
	var own hnswCoordinates
	if validHNSWIndex(internalID, len(graph.vecOff)) {
		if off := int(graph.vecOff[internalID]); off >= 0 && off+graph.dimensions <= len(graph.vectors) {
			own.stored = slices.Clone(graph.vectors[off : off+graph.dimensions])
		}
	}
	if q := graph.quantized; q != nil {
		if q.config.Mode != HNSWQuantizationScalar {
			return own, fmt.Errorf("the graph check does not read %q coordinates", q.config.Mode)
		}
		start, end := int(internalID)*q.stride, (int(internalID)+1)*q.stride
		if end > len(q.codes) {
			return own, fmt.Errorf("the graph has no int8 coordinates for %s", id)
		}
		own.codes = slices.Clone(q.codes[start:end])
	}
	if own.stored == nil && own.codes == nil {
		return own, fmt.Errorf("the graph keeps no coordinates of its own for %s", id)
	}
	return own, nil
}

// hnswScaledCoordinates are vec's coordinates on the int8 scale of a
// scalar-quantized graph, before rounding: the normalized vector, scaled so
// that its largest coordinate is 127. The graph stores each one rounded.
func hnswScaledCoordinates(vec []float32) []float64 {
	normalized := vector.Normalize(vec)
	var largest float64
	for _, x := range normalized {
		largest = max(largest, math.Abs(float64(x)))
	}
	scaled := make([]float64, len(normalized))
	if largest == 0 {
		return scaled
	}
	for i, x := range normalized {
		scaled[i] = float64(x) / largest * 127
	}
	return scaled
}

// hnswCodesAre reports whether row holds scaled rounded to int8. A coordinate
// on a rounding boundary may round either way, so each code must lie within
// half a step of its coordinate; the 0.001 allows for float32 arithmetic.
func hnswCodesAre(row []byte, scaled []float64) bool {
	if len(row) != len(scaled) {
		return false
	}
	for i, code := range row {
		if math.Abs(float64(int8(code))-scaled[i]) > 0.501 {
			return false
		}
	}
	return true
}

func int8Codes(row []byte) []int8 {
	out := make([]int8, len(row))
	for i, code := range row {
		out[i] = int8(code)
	}
	return out
}

// hnswGraphMismatch reports every way graph differs from want, which maps each
// key the graph must hold to that key's vector; nil means none.
//   - The graph's live keys must be exactly want's: a key it lacks is a lost
//     change, a key it holds beyond them is a removal that came back.
//   - What the graph keeps of each key's vector must be that key's vector:
//     its int8 codes are the wanted coordinates rounded, and its stored
//     coordinates equal the wanted ones to 1e-6. Coordinates are compared,
//     not similarity, so a key holding another key's vector is reported
//     however close the two vectors are.
//   - Any two wanted vectors must differ by more than one int8 step in some
//     coordinate: otherwise one code row could pass for both keys.
func hnswGraphMismatch(graph *HNSWIndex, want map[string][]float32) error {
	if graph == nil {
		return errors.New("there is no graph")
	}
	var problems []error
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	scaled := make(map[string][]float64, len(want))
	for _, id := range ids {
		scaled[id] = hnswScaledCoordinates(want[id])
	}
	for i, id := range ids {
		for _, other := range ids[i+1:] {
			apart := false
			for k := range scaled[id] {
				apart = apart || math.Abs(scaled[id][k]-scaled[other][k]) > 1.002
			}
			if !apart {
				problems = append(problems, fmt.Errorf("the vectors wanted for %s and %s are within one int8 step: the check could not tell them apart", id, other))
			}
		}
	}
	graph.mu.RLock()
	var unexpected []string
	for id, internalID := range graph.idToInternal {
		if !validHNSWIndex(internalID, len(graph.deleted)) || graph.deleted[internalID] {
			continue
		}
		if _, ok := want[id]; !ok {
			unexpected = append(unexpected, id)
		}
	}
	graph.mu.RUnlock()
	slices.Sort(unexpected)
	for _, id := range unexpected {
		problems = append(problems, fmt.Errorf("the graph holds %s, which it must not", id))
	}
	if size := graph.Size(); size != len(want) {
		problems = append(problems, fmt.Errorf("the graph holds %d vectors, want %d", size, len(want)))
	}
	for _, id := range ids {
		own, err := hnswOwnCoordinates(graph, id)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		if own.codes != nil && !hnswCodesAre(own.codes, scaled[id]) {
			problems = append(problems, fmt.Errorf("%s holds int8 coordinates %v, want %.1f rounded, those of its vector %v", id, int8Codes(own.codes), scaled[id], want[id]))
		}
		if own.stored != nil {
			wanted := vector.Normalize(want[id])
			for i := range wanted {
				if math.Abs(float64(own.stored[i]-wanted[i])) > 1e-6 {
					problems = append(problems, fmt.Errorf("%s holds coordinates %v, want %v", id, own.stored, wanted))
					break
				}
			}
		}
	}
	return errors.Join(problems...)
}

// The graph check must reject each way a graph can hold the wrong vectors, in
// both shapes a graph has: one that stores its vectors, and the shape the
// search service publishes, with int8 codes in the graph and the vectors
// looked up. The swapped pairs include the closest vectors of the fixtures
// that use the check. Every case names the break it must report.
func TestHNSWGraphOracleRejectsWrongGraphs(t *testing.T) {
	want := map[string][]float32{"a": {1, 0, 0}, "b": {0, 1, 0}, "c": {0, 0, 1}}
	with := func(base map[string][]float32, changes map[string][]float32) map[string][]float32 {
		out := make(map[string][]float32, len(base)+len(changes))
		for id, vec := range base {
			out[id] = vec
		}
		for id, vec := range changes {
			if vec == nil {
				delete(out, id)
				continue
			}
			out[id] = vec
		}
		return out
	}
	// The closest pairs of the fixtures: the transition test's v-08 and v-09
	// (cosine 0.995), and the catch-up tests' seed-19 and "added" (0.999).
	transition := map[string][]float32{"v-07": {-8, 2, 1}, "v-08": {-9, 2, 0}, "v-09": {-10, 2, 1}}
	seeds := map[string][]float32{"seed-19": catchUpSeed(19), "added": {0, 1, 0}, "seed-21": catchUpSeed(21)}
	shapes := []struct {
		name  string
		build func(t *testing.T, given, truth map[string][]float32) *HNSWIndex
	}{
		{"stored vectors", func(t *testing.T, given, _ map[string][]float32) *HNSWIndex {
			graph := NewHNSWIndex(3, DefaultHNSWConfig())
			for id, vec := range given {
				require.NoError(t, graph.Add(id, vec))
			}
			return graph
		}},
		{"int8 codes and looked-up vectors", func(t *testing.T, given, truth map[string][]float32) *HNSWIndex {
			graph := NewHNSWIndex(3, DefaultHNSWConfig())
			for id, vec := range given {
				require.NoError(t, graph.Add(id, vec))
			}
			require.NoError(t, graph.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}))
			// The vector store holds the true vectors; the graph was given
			// `given`. Updating a key re-inserts it without stored coordinates.
			graph.SetVectorLookup(func(id string) ([]float32, bool) {
				vec, ok := truth[id]
				return vec, ok
			})
			for id, vec := range given {
				require.NoError(t, graph.Update(id, vec))
			}
			return graph
		}},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			require.NoError(t, hnswGraphMismatch(shape.build(t, want, want), want), "a graph holding exactly the wanted vectors")
			require.NoError(t, hnswGraphMismatch(shape.build(t, transition, transition), transition), "the transition fixture's closest vectors, each on its own key")
			require.NoError(t, hnswGraphMismatch(shape.build(t, seeds, seeds), seeds), "the catch-up fixture's closest vectors, each on its own key")

			removed := shape.build(t, with(want, map[string][]float32{"gone": {1, 1, 0}}), want)
			removed.Remove("gone")
			require.NoError(t, hnswGraphMismatch(removed, want), "a removed key leaves a tombstone, not a vector")

			updated := shape.build(t, with(want, map[string][]float32{"a": {0, 1, 1}}), want)
			require.NoError(t, updated.Update("a", []float32{1, 0, 0}))
			require.NoError(t, hnswGraphMismatch(updated, want), "an updated key holds its new vector")

			for _, wrong := range []struct {
				name   string
				given  map[string][]float32
				want   map[string][]float32
				report string
			}{
				{"two keys hold each other's vectors", with(want, map[string][]float32{"a": {0, 1, 0}, "b": {1, 0, 0}}), want, "a holds "},
				{"the transition fixture's closest keys hold each other's vectors", with(transition, map[string][]float32{"v-08": {-10, 2, 1}, "v-09": {-9, 2, 0}}), transition, "v-08 holds "},
				{"the catch-up fixture's closest keys hold each other's vectors", with(seeds, map[string][]float32{"seed-19": {0, 1, 0}, "added": catchUpSeed(19)}), seeds, "seed-19 holds "},
				{"a key holds its vector from before an update", want, with(want, map[string][]float32{"a": {0, 1, 1}}), "a holds "},
				{"a key holds a vector no key should have", with(want, map[string][]float32{"c": {-1, 0, 0}}), want, "c holds "},
				{"a key is missing", with(want, map[string][]float32{"c": nil}), want, "the graph lost c"},
				{"a removed key is back", with(want, map[string][]float32{"gone": {1, 1, 0}}), want, "the graph holds gone, which it must not"},
				{"a missing key is replaced by another", with(want, map[string][]float32{"c": nil, "other": {0, 0, 1}}), want, "the graph lost c"},
			} {
				t.Run(wrong.name, func(t *testing.T) {
					err := hnswGraphMismatch(shape.build(t, wrong.given, wrong.want), wrong.want)
					require.Error(t, err, "the check accepted this graph")
					require.ErrorContains(t, err, wrong.report)
				})
			}
		})
	}
	require.ErrorContains(t, hnswGraphMismatch(nil, want), "there is no graph")
	twins := map[string][]float32{"a": {1, 0, 0}, "b": {2, 0, 0}}
	require.ErrorContains(t, hnswGraphMismatch(shapes[0].build(t, twins, twins), twins), "could not tell them apart")
}

package linkpredict

// Root-cause diagnostic round 3, campaign lane B (test-only).
// G-linkpredict-tie-rank: the maintainer's tie graph built directly.

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCBAdamicAdarSummationOrder(t *testing.T) {
	g := Graph{}
	link := func(a, b string) {
		for _, pair := range [][2]string{{a, b}, {b, a}} {
			if g[storage.NodeID(pair[0])] == nil {
				g[storage.NodeID(pair[0])] = NodeSet{}
			}
			g[storage.NodeID(pair[0])][storage.NodeID(pair[1])] = struct{}{}
		}
	}
	for index := 0; index < 3; index++ {
		hub := fmt.Sprintf("h%d", index)
		for _, id := range []string{"s", "x", "y"} {
			link(hub, id)
		}
		for pad := 0; pad <= index; pad++ {
			link(hub, fmt.Sprintf("pad%d_%d", index, pad))
		}
	}
	bits := map[string]map[string]int{"x": {}, "y": {}}
	differ := 0
	const calls = 2000
	for i := 0; i < calls; i++ {
		scores := TopologyScores("adamic_adar", g, nil, "s")
		bx, by := math.Float64bits(scores["x"]), math.Float64bits(scores["y"])
		bits["x"][fmt.Sprintf("%016x", bx)]++
		bits["y"][fmt.Sprintf("%016x", by)]++
		if bx != by {
			differ++
		}
	}
	t.Logf("RCB120 product TopologyScores x %d calls: bits x=%v y=%v; calls where x and y differ=%d", calls, bits["x"], bits["y"], differ)
	terms := []float64{1 / math.Log(4), 1 / math.Log(5), 1 / math.Log(6)}
	orders := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for _, order := range orders {
		sum := 0.0
		for _, index := range order {
			sum += terms[index]
		}
		t.Logf("RCB120 summation order of hub degrees %v -> %016x", []int{4 + order[0], 4 + order[1], 4 + order[2]}, math.Float64bits(sum))
	}
	// Variant: the same score with the common neighbours summed in sorted ID order.
	sortedScore := func(candidate storage.NodeID) float64 {
		var common []string
		for neighbor := range g["s"] {
			if _, shared := g[candidate][neighbor]; shared {
				common = append(common, string(neighbor))
			}
		}
		sort.Strings(common)
		sum := 0.0
		for _, neighbor := range common {
			sum += 1 / math.Log(float64(g.Degree(storage.NodeID(neighbor))))
		}
		return sum
	}
	variantBits := map[string]int{}
	for i := 0; i < calls; i++ {
		variantBits[fmt.Sprintf("x=%016x y=%016x", math.Float64bits(sortedScore("x")), math.Float64bits(sortedScore("y")))]++
	}
	t.Logf("RCB120 variant sorted-order sum x %d calls: %v", calls, variantBits)
}

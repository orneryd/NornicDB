package louvain

import (
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
)

// The Cypher sweep separately verifies every projection and stream option.
// Here its full-size, unseeded simple graphs exercise the engine directly,
// without reproducing native projection aggregation or stream filtering.
func TestRunMatchesProjectedGDSReferences(t *testing.T) {
	var cases []struct {
		Name        string
		N           int
		Edges       [][]interface{}
		Orientation string
		Seeds       []*int64
		Config      map[string]interface{}
		Expected    [][]interface{}
	}
	data, err := os.ReadFile("../cypher/testdata/gds_louvain_weighted_round3.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	compared := 0
	for _, tc := range cases {
		if tc.Seeds != nil || len(tc.Expected) != tc.N {
			continue
		}
		seen := map[[2]int]bool{}
		simple := true
		for _, edge := range tc.Edges {
			a, b := int(edge[0].(float64)), int(edge[1].(float64))
			if tc.Orientation == "UNDIRECTED" && a > b {
				a, b = b, a
			}
			key := [2]int{a, b}
			if seen[key] {
				simple = false
				break
			}
			seen[key] = true
		}
		if !simple {
			continue
		}
		compared++
		t.Run(tc.Name, func(t *testing.T) {
			ids := make([]string, tc.N)
			for i := range ids {
				ids[i] = strconv.Itoa(i)
			}
			sort.Strings(ids)
			position := make(map[int]int, len(ids))
			for i, id := range ids {
				original, _ := strconv.Atoi(id)
				position[original] = i
			}
			graph := NewGraph(tc.N)
			for _, edge := range tc.Edges {
				a, b := position[int(edge[0].(float64))], position[int(edge[1].(float64))]
				weight := 1.0
				if _, weighted := tc.Config["relationshipWeightProperty"]; weighted {
					switch value := edge[2].(type) {
					case nil:
						weight = math.NaN()
					case float64:
						weight = value
					case string:
						switch value {
						case "NaN":
							weight = math.NaN()
						case "Infinity":
							weight = math.Inf(1)
						case "-Infinity":
							weight = math.Inf(-1)
						default:
							t.Fatalf("unknown weight %q", value)
						}
					}
				}
				switch tc.Orientation {
				case "UNDIRECTED":
					graph.Link(a, b, weight)
				case "REVERSE":
					graph.Arc(b, a, weight)
				default:
					graph.Arc(a, b, weight)
				}
			}
			options := DefaultOptions()
			if v, ok := tc.Config["maxLevels"].(float64); ok {
				options.MaxLevels = int(v)
			}
			if v, ok := tc.Config["maxIterations"].(float64); ok {
				options.MaxIterations = int(v)
			}
			if v, ok := tc.Config["tolerance"].(float64); ok {
				options.Tolerance = v
			}
			actual := Final(Run(graph, options))
			expected := make([]float64, tc.N)
			for _, row := range tc.Expected {
				expected[position[int(row[0].(float64))]] = row[1].(float64)
			}
			for i := range actual {
				for j := range actual {
					if (actual[i] == actual[j]) != (expected[i] == expected[j]) {
						t.Fatalf("GDS partition %v, engine %v", expected, actual)
					}
				}
			}
		})
	}
	if compared < 200 {
		t.Fatalf("only %d engine reference cases", compared)
	}
	t.Logf("compared %d simple full-size GDS reference cases", compared)
}

func TestRunSeedsResolutionAndEmptyGraph(t *testing.T) {
	empty := Run(NewGraph(0), DefaultOptions())
	if len(Final(empty)) != 0 || len(empty.Levels) != 1 {
		t.Fatalf("empty graph result: %+v", empty)
	}
	graph := NewGraph(4)
	graph.Link(0, 1, 1)
	graph.Link(2, 3, 1)
	options := DefaultOptions()
	options.Seed = []int{0, 0, 2, 2}
	actual := Final(Run(graph, options))
	for i, want := range []int{0, 0, 1, 1} {
		if actual[i] != want {
			t.Fatalf("seeded communities %v", actual)
		}
	}
	pair := NewGraph(2)
	pair.Link(0, 1, 1)
	for _, resolution := range []float64{0, 1, 2, 0.25} {
		options := DefaultOptions()
		options.Resolution = resolution
		actual := Final(Run(pair, options))
		together := resolution != 0.25
		if (actual[0] == actual[1]) != together {
			t.Fatalf("resolution %v: communities %v", resolution, actual)
		}
	}
}

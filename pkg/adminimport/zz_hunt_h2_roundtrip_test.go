package adminimport

// Hunt ground H2 (v1.5.2 open hunt, "around the fixes"), variant of
// 1cbd3ddeb "fix(adminimport): preserve CSV property values through round
// trips" (units H2-0021, H2-0027, H2-0039, H2-0040 and the other changed
// functions of importer.go and neo4j_csv.go). The fix's own assertion
// (neo4j_csv_roundtrip_fidelity_test.go: after the admin CSV export and
// import, every entity's properties equal the source's) applied to the
// sibling value shapes the regression test does not cover: other scalar
// types, list element types, list elements and labels holding the array
// delimiter, empty lists and empty list elements, one key holding different
// types on different entities, large and unsigned integers, temporal values,
// multiple labels. The source's own read-back is the expected value, so
// storage normalization is not reported. Prints "HUNT_LEAD <json>" per
// disagreement and one "HUNT_COVERAGE <json>"; never fails on a disagreement.
// Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func h2RoundTripLead(lead map[string]any) {
	lead["ground"] = "H2"
	data, err := json.Marshal(lead)
	if err != nil {
		data = []byte(fmt.Sprintf("%q", fmt.Sprint(lead)))
	}
	fmt.Fprintf(os.Stdout, "HUNT_LEAD %s\n", data)
}

type h2RoundTripCase struct {
	name  string
	nodes []*storage.Node
	edges []*storage.Edge
}

func h2Node(id string, labels []string, props map[string]any) *storage.Node {
	if labels == nil {
		labels = []string{"Item"}
	}
	props["uid"] = id
	return &storage.Node{ID: storage.NodeID(id), Labels: labels, Properties: props}
}

func h2RoundTripCases() []h2RoundTripCase {
	when := time.Date(2026, 11, 20, 10, 30, 0, 123456789, time.FixedZone("CET", 3600))
	return []h2RoundTripCase{
		{name: "string-list-element-with-array-delimiter", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []string{"a;b", "c"}})}},
		{name: "string-list-empty-element", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []string{"", "x"}})}},
		{name: "string-list-single-empty-element", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []string{""}})}},
		{name: "empty-string-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []string{}})}},
		{name: "empty-any-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []any{}})}},
		{name: "string-list-with-quote-and-newline", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tags": []string{"say \"hi\"", "line1\nline2", "comma,here"}})}},
		{name: "int-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"xs": []int64{1, -2, math.MaxInt64, math.MinInt64}})}},
		{name: "bool-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"xs": []bool{true, false}})}},
		{name: "any-list-of-strings", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"xs": []any{"a", "b;c"}})}},
		{name: "any-list-mixed-int-string", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"xs": []any{int64(1), "a"}})}},
		{name: "any-list-mixed-int-float", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"xs": []any{int64(1), 2.5}})}},
		{name: "same-key-int-and-string", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"code": int64(7)}), h2Node("n2", nil, map[string]any{"code": "007"})}},
		{name: "same-key-int-and-float", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"score": int64(3)}), h2Node("n2", nil, map[string]any{"score": 2.5})}},
		{name: "same-key-bool-and-string", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"flag": true}), h2Node("n2", nil, map[string]any{"flag": "yes"})}},
		{name: "same-key-scalar-and-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"v": "x"}), h2Node("n2", nil, map[string]any{"v": []string{"x", "y"}})}},
		{name: "same-key-int-list-and-string-list", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"v": []int64{1}}), h2Node("n2", nil, map[string]any{"v": []string{"a"}})}},
		{name: "int64-extremes", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"big": int64(math.MaxInt64), "small": int64(math.MinInt64), "odd": int64(9007199254740993)})}},
		{name: "float-specials", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"tiny": 5e-324, "huge": 1.7976931348623157e308, "neg": -0.5, "third": 1.0 / 3.0})}},
		{name: "float-integral", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"f": 2.0})}},
		{name: "string-that-looks-like-number", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"s": "123", "t": "true", "e": "1e5"})}},
		{name: "datetime-with-zone", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"at": when})}},
		{name: "datetime-utc", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"at": when.UTC()})}},
		{name: "label-with-array-delimiter", nodes: []*storage.Node{h2Node("n1", []string{"A;B"}, map[string]any{})}},
		{name: "multiple-labels", nodes: []*storage.Node{h2Node("n1", []string{"A", "B", "C"}, map[string]any{})}},
		{name: "label-with-comma-and-space", nodes: []*storage.Node{h2Node("n1", []string{"My Label,x"}, map[string]any{})}},
		{name: "property-key-with-colon", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"a:b": "v"})}},
		{name: "property-key-with-space", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"first name": "v"})}},
		{name: "unicode-values", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{"s": "Zoë — 日本語 🚀", "tags": []string{"ä", "ß"}})}},
		{name: "relationship-type-with-delimiter-and-props", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{}), h2Node("n2", nil, map[string]any{})},
			edges: []*storage.Edge{{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "HAS;PART", Properties: map[string]any{"uid": "e1", "w": []string{"a;b"}, "n": int64(1)}}}},
		{name: "relationship-same-key-mixed-types", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{}), h2Node("n2", nil, map[string]any{})},
			edges: []*storage.Edge{
				{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "R", Properties: map[string]any{"uid": "e1", "w": int64(1)}},
				{ID: "e2", StartNode: "n2", EndNode: "n1", Type: "R", Properties: map[string]any{"uid": "e2", "w": "one"}},
			}},
		{name: "self-loop-relationship", nodes: []*storage.Node{h2Node("n1", nil, map[string]any{})},
			edges: []*storage.Edge{{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "SELF", Properties: map[string]any{"uid": "e1"}}}},
		{name: "node-id-with-comma-and-quote", nodes: []*storage.Node{h2Node(`n,1"x`, nil, map[string]any{}), h2Node("n2", nil, map[string]any{})},
			edges: []*storage.Edge{{ID: "e1", StartNode: `n,1"x`, EndNode: "n2", Type: "R", Properties: map[string]any{"uid": "e1"}}}},
	}
}

func h2SortedLabels(labels []string) []string {
	out := append([]string(nil), labels...)
	sort.Strings(out)
	return out
}

func TestHuntH2CSVRoundTripSiblings(t *testing.T) {
	cases, leads := 0, 0
	for _, tc := range h2RoundTripCases() {
		t.Run(tc.name, func(t *testing.T) {
			cases++
			ctx := context.Background()
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			source := storage.NewNamespacedEngine(base, "source")
			lead := map[string]any{"harness": "csv-roundtrip", "unit": "1cbd3ddeb", "case": tc.name}
			for _, node := range tc.nodes {
				if _, err := source.CreateNode(ctx, node); err != nil {
					fmt.Printf("HUNT_NOTE H2 csv-roundtrip %s source CreateNode refused: %v\n", tc.name, err)
					return
				}
			}
			for _, edge := range tc.edges {
				if err := source.CreateEdge(ctx, edge); err != nil {
					fmt.Printf("HUNT_NOTE H2 csv-roundtrip %s source CreateEdge refused: %v\n", tc.name, err)
					return
				}
			}
			dest, outDir, err := h2RoundTrip(t, source)
			if err != nil {
				lead["kind"], lead["error"] = "roundtrip-error", err.Error()
				lead["source"] = fmt.Sprintf("%#v", tc.nodes)
				leads++
				h2RoundTripLead(lead)
				return
			}
			var diffs []string
			for _, want := range tc.nodes {
				src, err := source.GetNode(ctx, want.ID)
				if err != nil {
					diffs = append(diffs, fmt.Sprintf("source read-back of %s: %v", want.ID, err))
					continue
				}
				got, err := dest.GetNode(ctx, want.ID)
				if err != nil {
					diffs = append(diffs, fmt.Sprintf("node %q missing after round trip: %v", want.ID, err))
					continue
				}
				if !reflect.DeepEqual(src.Properties, got.Properties) {
					diffs = append(diffs, fmt.Sprintf("node %q properties: source %#v, imported %#v", want.ID, src.Properties, got.Properties))
				}
				if !reflect.DeepEqual(h2SortedLabels(src.Labels), h2SortedLabels(got.Labels)) {
					diffs = append(diffs, fmt.Sprintf("node %q labels: source %q, imported %q", want.ID, src.Labels, got.Labels))
				}
			}
			if len(tc.edges) > 0 {
				srcEdges, err1 := storage.CollectStoredEdges(ctx, source)
				gotEdges, err2 := storage.CollectStoredEdges(ctx, dest)
				if err1 != nil || err2 != nil {
					diffs = append(diffs, fmt.Sprintf("edge collection: %v / %v", err1, err2))
				} else {
					byID := map[storage.EdgeID]*storage.Edge{}
					for _, edge := range gotEdges {
						byID[edge.ID] = edge
					}
					if len(gotEdges) != len(srcEdges) {
						diffs = append(diffs, fmt.Sprintf("relationship count: source %d, imported %d", len(srcEdges), len(gotEdges)))
					}
					for _, src := range srcEdges {
						got := byID[src.ID]
						if got == nil {
							diffs = append(diffs, fmt.Sprintf("relationship %q missing", src.ID))
							continue
						}
						if got.Type != src.Type || got.StartNode != src.StartNode || got.EndNode != src.EndNode || !reflect.DeepEqual(src.Properties, got.Properties) {
							diffs = append(diffs, fmt.Sprintf("relationship %q: source %s %s->%s %#v, imported %s %s->%s %#v", src.ID, src.Type, src.StartNode, src.EndNode, src.Properties, got.Type, got.StartNode, got.EndNode, got.Properties))
						}
					}
				}
			}
			if len(diffs) > 0 {
				lead["kind"] = "roundtrip-changed-values"
				lead["diffs"] = diffs
				if data, err := os.ReadFile(outDir + "/nodes.csv"); err == nil {
					text := string(data)
					if len(text) > 600 {
						text = text[:600]
					}
					lead["nodes_csv"] = text
				}
				leads++
				h2RoundTripLead(lead)
			}
		})
	}
	data, _ := json.Marshal(map[string]any{"ground": "H2", "harness": "csv-roundtrip", "commit": "1cbd3ddeb", "cases": cases, "leads": leads})
	fmt.Fprintf(os.Stdout, "HUNT_COVERAGE %s\n", data)
}

// h2RoundTrip is roundTripNeo4jCSV with its errors returned instead of
// required, so a refused export or import is a lead and the batch goes on.
func h2RoundTrip(t *testing.T, source storage.Engine) (storage.Engine, string, error) {
	t.Helper()
	outDir := t.TempDir() + "/neo4j-csv"
	if err := ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: outDir}); err != nil {
		return nil, outDir, fmt.Errorf("export: %w", err)
	}
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	if err != nil {
		return nil, outDir, fmt.Errorf("discover: %w", err)
	}
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	opts := Options{DatabaseName: "roundtrip", NodeSources: nodeSources, RelSources: relSources, BuildIndexes: false, Now: fixedImportTime}
	if _, err := ImportFull(context.Background(), base, opts); err != nil {
		return nil, outDir, fmt.Errorf("import: %w", err)
	}
	return storage.NewNamespacedEngine(base, "roundtrip"), outDir, nil
}

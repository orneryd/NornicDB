// Labeled-scan benchmarks for the apoc.search helpers.
//
// The storage stub hands back one fixed, ordered node slice on every read, so a
// measurement covers the labeled scan and its predicates rather than a storage
// map copy. The workloads are named so the same population can be measured
// against the pre-convergence per-helper bodies and against the single scan
// owner: small is 256 nodes, large is 16384.
package search_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc/search"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
)

// scanBenchNodes is the population one benchmark size reads: a fifth of the
// nodes carry the status value the membership predicate lists, an eighth carry a
// name the prefix predicate matches, every fourth node has no email property and
// an eighth carry a null note, so each predicate keeps a stable share of the
// population.
func scanBenchNodes(size int) []*apocstorage.Node {
	nodes := make([]*apocstorage.Node, 0, size)
	for index := 0; index < size; index++ {
		properties := map[string]interface{}{
			"name":   fmt.Sprintf("Person%d", index),
			"status": "archived",
			"tags":   []interface{}{"tag0", "tag1"},
			"age":    int64(index % 90),
		}
		if index%5 == 0 {
			properties["status"] = "active"
		}
		if index%8 == 0 {
			properties["name"] = fmt.Sprintf("Alice%d", index)
			properties["note"] = nil
		}
		if index%4 != 0 {
			properties["email"] = fmt.Sprintf("person%d@example.com", index)
		}
		nodes = append(nodes, &apocstorage.Node{
			ID:         int64(index + 1),
			Labels:     []string{"Person"},
			Properties: properties,
		})
	}
	return nodes
}

// multiLabelBenchNodes is the population the multi-label helpers read: even
// indexes carry the Person label and odd indexes the Company label, so a
// two-label search scans the same whole graph twice and keeps a stable order.
func multiLabelBenchNodes(size int) []*apocstorage.Node {
	nodes := make([]*apocstorage.Node, 0, size)
	for index := 0; index < size; index++ {
		label := "Person"
		if index%2 == 1 {
			label = "Company"
		}
		properties := map[string]interface{}{
			"name":   fmt.Sprintf("Name%d", index),
			"status": "archived",
		}
		if index%3 == 0 {
			properties["status"] = "active"
		}
		nodes = append(nodes, &apocstorage.Node{
			ID:         int64(index + 1),
			Labels:     []string{label},
			Properties: properties,
		})
	}
	return nodes
}

// scanBenchStorage returns one fixed node slice from every whole-graph read.
type scanBenchStorage struct {
	nodes []*apocstorage.Node
}

func (s *scanBenchStorage) AllNodes(context.Context) ([]*apocstorage.Node, error) {
	return s.nodes, nil
}

func (s *scanBenchStorage) GetNode(context.Context, int64) (*apocstorage.Node, error) {
	return nil, apocstorage.ErrNodeNotFound
}

func (s *scanBenchStorage) CreateNode(context.Context, []string, map[string]interface{}) (*apocstorage.Node, error) {
	return &apocstorage.Node{}, nil
}

func (s *scanBenchStorage) UpdateNode(context.Context, int64, apocstorage.PropertyUpdate) error {
	return nil
}

func (s *scanBenchStorage) DeleteNode(context.Context, int64) error { return nil }

func (s *scanBenchStorage) AddLabels(context.Context, int64, []string) error { return nil }

func (s *scanBenchStorage) RemoveLabels(context.Context, int64, []string) error { return nil }

func (s *scanBenchStorage) UpdateNodeLabels(context.Context, int64, []string) error { return nil }

func (s *scanBenchStorage) GetRelationship(context.Context, int64) (*apocstorage.Relationship, error) {
	return nil, apocstorage.ErrRelationshipNotFound
}

func (s *scanBenchStorage) CreateRelationship(context.Context, int64, int64, string, map[string]interface{}) (*apocstorage.Relationship, error) {
	return &apocstorage.Relationship{}, nil
}

func (s *scanBenchStorage) UpdateRelationship(context.Context, int64, apocstorage.PropertyUpdate) error {
	return nil
}

func (s *scanBenchStorage) DeleteRelationship(context.Context, int64) error { return nil }

func (s *scanBenchStorage) UpdateRelationshipType(context.Context, int64, string) error { return nil }

func (s *scanBenchStorage) AllRelationships(context.Context) ([]*apocstorage.Relationship, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeRelationships(context.Context, int64, string, apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeNeighbors(context.Context, int64, string, apocstorage.Direction) ([]*apocstorage.Node, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeDegree(context.Context, int64, string, apocstorage.Direction) (int, error) {
	return 0, nil
}

func (s *scanBenchStorage) FindShortestPath(context.Context, int64, int64, string, int) (*apocstorage.Path, error) {
	return nil, apocstorage.ErrPathNotFound
}

func (s *scanBenchStorage) FindAllPaths(context.Context, int64, int64, string, int) ([]*apocstorage.Path, error) {
	return nil, nil
}

func (s *scanBenchStorage) BFS(context.Context, int64, string, int, func(*apocstorage.Node) bool) error {
	return nil
}

func (s *scanBenchStorage) DFS(context.Context, int64, string, int, func(*apocstorage.Node) bool) error {
	return nil
}

// scanBenchSizes are the two workloads every labeled-scan benchmark reports.
var scanBenchSizes = []struct {
	name string
	size int
}{
	{name: "small256", size: 256},
	{name: "large16384", size: 16384},
}

// BenchmarkAPOCSearchLabeledScan measures every apoc.search helper whose whole
// body is one labeled scan: string predicates, equality and criteria
// predicates, a range, membership lists of two, four, eight and sixteen values,
// and the property-presence predicates.
func BenchmarkAPOCSearchLabeledScan(b *testing.B) {
	ctx := context.Background()
	for _, population := range scanBenchSizes {
		// The backend is fixed per instance, so the benchmark builds the one
		// instance every helper call below answers from.
		functions := search.New(&scanBenchStorage{nodes: scanBenchNodes(population.size)})
		for _, helper := range []struct {
			name string
			call func() []*search.NodeType
		}{
			{name: "prefix", call: func() []*search.NodeType {
				apocCallValue1, apocCallError1 := functions.Prefix(ctx, "Person", "name", "Al")
				if apocCallError1 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError1)
				}
				return apocCallValue1
			}},
			{name: "suffix", call: func() []*search.NodeType {
				apocCallValue2, apocCallError2 := functions.Suffix(ctx, "Person", "email", "@example.com")
				if apocCallError2 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError2)
				}
				return apocCallValue2
			}},
			{name: "contains", call: func() []*search.NodeType {
				apocCallValue3, apocCallError3 := functions.Contains(ctx, "Person", "name", "erson1")
				if apocCallError3 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError3)
				}
				return apocCallValue3
			}},
			{name: "fullText", call: func() []*search.NodeType {
				apocCallValue4, apocCallError4 := functions.FullText(ctx, "Person", "name", "Alice Person7")
				if apocCallError4 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError4)
				}
				return apocCallValue4
			}},
			{name: "fuzzy", call: func() []*search.NodeType {
				apocCallValue5, apocCallError5 := functions.Fuzzy(ctx, "Person", "name", "Alise1", 2)
				if apocCallError5 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError5)
				}
				return apocCallValue5
			}},
			{name: "regex", call: func() []*search.NodeType {
				apocCallValue6, apocCallError6 := functions.Regex(ctx, "Person", "email", `^person1.*@example\.com$`)
				if apocCallError6 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError6)
				}
				return apocCallValue6
			}},
			{name: "match", call: func() []*search.NodeType {
				apocCallValue7, apocCallError7 := functions.Match(ctx, "Person", "name", "Alice*")
				if apocCallError7 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError7)
				}
				return apocCallValue7
			}},
			{name: "node", call: func() []*search.NodeType {
				apocCallValue8, apocCallError8 := functions.Node(ctx, "Person", "status", "active")
				if apocCallError8 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError8)
				}
				return apocCallValue8
			}},
			{name: "nodeAll", call: func() []*search.NodeType {
				apocCallValue9, apocCallError9 := functions.NodeAll(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(5)})
				if apocCallError9 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError9)
				}
				return apocCallValue9
			}},
			{name: "nodeAny", call: func() []*search.NodeType {
				apocCallValue10, apocCallError10 := functions.NodeAny(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(7)})
				if apocCallError10 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError10)
				}
				return apocCallValue10
			}},
			{name: "range", call: func() []*search.NodeType {
				apocCallValue11, apocCallError11 := functions.Range(ctx, "Person", "age", int64(20), int64(30))
				if apocCallError11 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError11)
				}
				return apocCallValue11
			}},
			{name: "in2", call: func() []*search.NodeType {
				apocCallValue12, apocCallError12 := functions.In(ctx, "Person", "status", []interface{}{"active", "pending"})
				if apocCallError12 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError12)
				}
				return apocCallValue12
			}},
			{name: "in4", call: func() []*search.NodeType {
				apocCallValue13, apocCallError13 := functions.In(ctx, "Person", "status", []interface{}{"v0", "v1", "v2", "active"})
				if apocCallError13 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError13)
				}
				return apocCallValue13
			}},
			{name: "in8", call: func() []*search.NodeType {
				apocCallValue14, apocCallError14 := functions.In(ctx, "Person", "status", []interface{}{
					"v0", "v1", "v2", "v3", "v4", "v5", "v6", "active",
				})
				if apocCallError14 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError14)
				}
				return apocCallValue14
			}},
			{name: "in16", call: func() []*search.NodeType {
				apocCallValue15, apocCallError15 := functions.In(ctx, "Person", "status", []interface{}{
					"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
					"v8", "v9", "v10", "v11", "v12", "v13", "v14", "active",
				})
				if apocCallError15 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError15)
				}
				return apocCallValue15
			}},
			{name: "notIn", call: func() []*search.NodeType {
				apocCallValue16, apocCallError16 := functions.NotIn(ctx, "Person", "status", []interface{}{"archived", "pending"})
				if apocCallError16 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError16)
				}
				return apocCallValue16
			}},
			{name: "exists", call: func() []*search.NodeType {
				apocCallValue17, apocCallError17 := functions.Exists(ctx, "Person", "email")
				if apocCallError17 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError17)
				}
				return apocCallValue17
			}},

			{name: "missing", call: func() []*search.NodeType {
				apocCallValue18, apocCallError18 := functions.Missing(ctx, "Person", "email")
				if apocCallError18 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError18)
				}
				return apocCallValue18
			}},
			{name: "null", call: func() []*search.NodeType {
				apocCallValue19, apocCallError19 := functions.Null(ctx, "Person", "note")
				if apocCallError19 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError19)
				}
				return apocCallValue19
			}},
			{name: "notNull", call: func() []*search.NodeType {
				apocCallValue20, apocCallError20 := functions.NotNull(ctx, "Person", "email")
				if apocCallError20 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError20)
				}
				return apocCallValue20
			}},
		} {
			b.Run(population.name+"/"+helper.name, func(b *testing.B) {
				if matches := helper.call(); len(matches) == 0 {
					b.Fatal("benchmark population must produce matches")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					helper.call()
				}
			})
		}
	}
}

// BenchmarkAPOCSearchMultiLabel measures the helpers that search several labels
// in the order they were given: every label performs its own whole-graph read,
// so a two-label search reads the same population twice.
//
// multiSearchAny carries one criterion, where "any" and "all" select the same
// nodes, so its cost is comparable across the semantics change; the criteria
// that differ between any and all are pinned by the behaviour tests instead.
func BenchmarkAPOCSearchMultiLabel(b *testing.B) {
	ctx := context.Background()
	labels := []string{"Person", "Company"}
	for _, population := range scanBenchSizes {
		functions := search.New(&scanBenchStorage{nodes: multiLabelBenchNodes(population.size)})
		for _, helper := range []struct {
			name string
			call func() []*search.NodeType
		}{
			{name: "multiSearchAll", call: func() []*search.NodeType {
				apocCallValue21, apocCallError21 := functions.MultiSearchAll(ctx, labels, map[string]interface{}{"status": "active"})
				if apocCallError21 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError21)
				}
				return apocCallValue21
			}},
			{name: "multiSearchAny", call: func() []*search.NodeType {
				apocCallValue22, apocCallError22 := functions.MultiSearchAny(ctx, labels, map[string]interface{}{"status": "active"})
				if apocCallError22 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError22)
				}
				return apocCallValue22
			}},
			{name: "parallel", call: func() []*search.NodeType {
				apocCallValue23, apocCallError23 := functions.Parallel(ctx, labels, "status", "active")
				if apocCallError23 != nil {
					b.Fatalf("APOC helper failed: %v", apocCallError23)
				}
				return apocCallValue23
			}},
		} {
			b.Run(population.name+"/"+helper.name, func(b *testing.B) {
				if matches := helper.call(); len(matches) == 0 {
					b.Fatal("benchmark population must produce matches")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					helper.call()
				}
			})
		}
	}
}

// membershipBenchNodes returns size ordered nodes that a property-presence scan
// and a membership scan both match, so the two helpers' allocations compare
// directly.
func membershipBenchNodes(size int) []*apocstorage.Node {
	nodes := make([]*apocstorage.Node, 0, size)
	for index := 0; index < size; index++ {
		nodes = append(nodes, &apocstorage.Node{
			ID:     int64(index + 1),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"email":  fmt.Sprintf("person%d@example.com", index),
				"status": "active",
			},
		})
	}
	return nodes
}

// TestAPOCSearchMembershipScanAllocatesLikePropertyPresence pins that the
// membership helpers keep no per-call lookup table for a short list: over a
// population where every helper matches every node, In and NotIn allocate
// exactly what a property-presence scan allocates. A long list builds its table
// once per call, which is what keeps its comparison constant-time.
func TestAPOCSearchMembershipScanAllocatesLikePropertyPresence(t *testing.T) {
	const population = 256
	functions := search.New(&scanBenchStorage{nodes: membershipBenchNodes(population)})
	ctx := context.Background()
	values := []interface{}{"active", "pending"}
	absent := []interface{}{"absent"}

	apocCallValue24, apocCallError24 := functions.Exists(ctx, "Person", "email")
	if apocCallError24 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError24)
	}
	if matched := len(apocCallValue24); matched != population {
		t.Fatalf("property presence must match every node, matched %d", matched)
	}
	apocCallValue25, apocCallError25 := functions.In(ctx, "Person", "status", values)
	if apocCallError25 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError25)
	}
	if matched := len(apocCallValue25); matched != population {
		t.Fatalf("membership must match every node, matched %d", matched)
	}

	presence := testing.AllocsPerRun(20, func() {
		_, apocCallError26 := functions.Exists(ctx, "Person", "email")
		if apocCallError26 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError26)
		}
	})
	for _, testCase := range []struct {
		name string
		call func()
	}{
		{name: "in", call: func() {
			_, apocCallError27 := functions.In(ctx, "Person", "status", values)
			if apocCallError27 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError27)
			}
		}},
		{name: "notIn", call: func() {
			_, apocCallError28 := functions.NotIn(ctx, "Person", "status", absent)
			if apocCallError28 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError28)
			}
		}},
	} {
		membership := testing.AllocsPerRun(20, testCase.call)
		if membership != presence {
			t.Fatalf("%s allocates %.0f times per call, property presence %.0f: a short list carries no table",
				testCase.name, membership, presence)
		}
	}

	long := []interface{}{
		"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
		"v8", "v9", "v10", "v11", "v12", "v13", "v14", "active",
	}
	apocCallValue29, apocCallError29 := functions.In(ctx, "Person", "status", long)
	if apocCallError29 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError29)
	}
	if matched := len(apocCallValue29); matched != population {
		t.Fatalf("a long membership list must match every node, matched %d", matched)
	}
	indexed := testing.AllocsPerRun(20, func() {
		_, apocCallError30 := functions.In(ctx, "Person", "status", long)
		if apocCallError30 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError30)
		}
	})
	if indexed <= presence {
		t.Fatalf("a long list builds its table once per call, so it allocates more than property presence (%.0f vs %.0f): "+
			"the table is what keeps a long list's comparison constant-time", indexed, presence)
	}
}

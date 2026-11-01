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

func (s *scanBenchStorage) GetNode(int64) (*apocstorage.Node, error) {
	return nil, apocstorage.ErrNodeNotFound
}

func (s *scanBenchStorage) CreateNode([]string, map[string]interface{}) (*apocstorage.Node, error) {
	return &apocstorage.Node{}, nil
}

func (s *scanBenchStorage) UpdateNode(int64, map[string]interface{}) error { return nil }

func (s *scanBenchStorage) DeleteNode(int64) error { return nil }

func (s *scanBenchStorage) AddLabels(int64, []string) error { return nil }

func (s *scanBenchStorage) RemoveLabels(int64, []string) error { return nil }

func (s *scanBenchStorage) UpdateNodeLabels(int64, []string) error { return nil }

func (s *scanBenchStorage) GetRelationship(int64) (*apocstorage.Relationship, error) {
	return nil, apocstorage.ErrRelationshipNotFound
}

func (s *scanBenchStorage) CreateRelationship(int64, int64, string, map[string]interface{}) (*apocstorage.Relationship, error) {
	return &apocstorage.Relationship{}, nil
}

func (s *scanBenchStorage) UpdateRelationship(int64, map[string]interface{}) error { return nil }

func (s *scanBenchStorage) DeleteRelationship(int64) error { return nil }

func (s *scanBenchStorage) UpdateRelationshipType(int64, string) error { return nil }

func (s *scanBenchStorage) AllRelationships(context.Context) ([]*apocstorage.Relationship, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeRelationships(int64, string, apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeNeighbors(int64, string, apocstorage.Direction) ([]*apocstorage.Node, error) {
	return nil, nil
}

func (s *scanBenchStorage) GetNodeDegree(int64, string, apocstorage.Direction) (int, error) {
	return 0, nil
}

func (s *scanBenchStorage) FindShortestPath(int64, int64, string, int) (*apocstorage.Path, error) {
	return nil, apocstorage.ErrPathNotFound
}

func (s *scanBenchStorage) FindAllPaths(int64, int64, string, int) ([]*apocstorage.Path, error) {
	return nil, nil
}

func (s *scanBenchStorage) BFS(int64, string, int, func(*apocstorage.Node) bool) error { return nil }

func (s *scanBenchStorage) DFS(int64, string, int, func(*apocstorage.Node) bool) error {
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
				return functions.Prefix(ctx, "Person", "name", "Al")
			}},
			{name: "suffix", call: func() []*search.NodeType {
				return functions.Suffix(ctx, "Person", "email", "@example.com")
			}},
			{name: "contains", call: func() []*search.NodeType {
				return functions.Contains(ctx, "Person", "name", "erson1")
			}},
			{name: "fullText", call: func() []*search.NodeType {
				return functions.FullText(ctx, "Person", "name", "Alice Person7")
			}},
			{name: "fuzzy", call: func() []*search.NodeType {
				return functions.Fuzzy(ctx, "Person", "name", "Alise1", 2)
			}},
			{name: "regex", call: func() []*search.NodeType {
				return functions.Regex(ctx, "Person", "email", `^person1.*@example\.com$`)
			}},
			{name: "match", call: func() []*search.NodeType {
				return functions.Match(ctx, "Person", "name", "Alice*")
			}},
			{name: "node", call: func() []*search.NodeType {
				return functions.Node(ctx, "Person", "status", "active")
			}},
			{name: "nodeAll", call: func() []*search.NodeType {
				return functions.NodeAll(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(5)})
			}},
			{name: "nodeAny", call: func() []*search.NodeType {
				return functions.NodeAny(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(7)})
			}},
			{name: "range", call: func() []*search.NodeType {
				return functions.Range(ctx, "Person", "age", int64(20), int64(30))
			}},
			{name: "in2", call: func() []*search.NodeType {
				return functions.In(ctx, "Person", "status", []interface{}{"active", "pending"})
			}},
			{name: "in4", call: func() []*search.NodeType {
				return functions.In(ctx, "Person", "status", []interface{}{"v0", "v1", "v2", "active"})
			}},
			{name: "in8", call: func() []*search.NodeType {
				return functions.In(ctx, "Person", "status", []interface{}{
					"v0", "v1", "v2", "v3", "v4", "v5", "v6", "active",
				})
			}},
			{name: "in16", call: func() []*search.NodeType {
				return functions.In(ctx, "Person", "status", []interface{}{
					"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
					"v8", "v9", "v10", "v11", "v12", "v13", "v14", "active",
				})
			}},
			{name: "notIn", call: func() []*search.NodeType {
				return functions.NotIn(ctx, "Person", "status", []interface{}{"archived", "pending"})
			}},
			{name: "exists", call: func() []*search.NodeType {
				return functions.Exists(ctx, "Person", "email")
			}},

			{name: "missing", call: func() []*search.NodeType {
				return functions.Missing(ctx, "Person", "email")
			}},
			{name: "null", call: func() []*search.NodeType {
				return functions.Null(ctx, "Person", "note")
			}},
			{name: "notNull", call: func() []*search.NodeType {
				return functions.NotNull(ctx, "Person", "email")
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
				return functions.MultiSearchAll(ctx, labels, map[string]interface{}{"status": "active"})
			}},
			{name: "multiSearchAny", call: func() []*search.NodeType {
				return functions.MultiSearchAny(ctx, labels, map[string]interface{}{"status": "active"})
			}},
			{name: "parallel", call: func() []*search.NodeType {
				return functions.Parallel(ctx, labels, "status", "active")
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

	if matched := len(functions.Exists(ctx, "Person", "email")); matched != population {
		t.Fatalf("property presence must match every node, matched %d", matched)
	}
	if matched := len(functions.In(ctx, "Person", "status", values)); matched != population {
		t.Fatalf("membership must match every node, matched %d", matched)
	}

	presence := testing.AllocsPerRun(20, func() { functions.Exists(ctx, "Person", "email") })
	for _, testCase := range []struct {
		name string
		call func()
	}{
		{name: "in", call: func() { functions.In(ctx, "Person", "status", values) }},
		{name: "notIn", call: func() { functions.NotIn(ctx, "Person", "status", absent) }},
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
	if matched := len(functions.In(ctx, "Person", "status", long)); matched != population {
		t.Fatalf("a long membership list must match every node, matched %d", matched)
	}
	indexed := testing.AllocsPerRun(20, func() { functions.In(ctx, "Person", "status", long) })
	if indexed <= presence {
		t.Fatalf("a long list builds its table once per call, so it allocates more than property presence (%.0f vs %.0f): "+
			"the table is what keeps a long list's comparison constant-time", indexed, presence)
	}
}

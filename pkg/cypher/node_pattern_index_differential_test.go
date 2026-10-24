package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const nodePatternDifferentialFixture = `CREATE
 (a:R73Label {id:1,bucket:1,rank:2,flag:true,value:1}),
 (b:R73Label:R73Wanted {id:2,bucket:1,rank:4,flag:true,value:3}),
 (c:R73Label {id:3,bucket:2,rank:2,flag:true,value:3}),
 (d:R73Label:R73Wanted {id:4,bucket:2,rank:1,flag:false,value:1}),
 (e:R73Label:R73Wanted {id:2,bucket:1,rank:3,flag:true,value:3}),
 (f:R73Label:R73Wanted {bucket:2,rank:0,flag:true}),
 (g:R73Label {id:9,bucket:3,rank:0,flag:true,value:3}),
 (h:R73Wanted {id:0,bucket:0,rank:0,flag:true,value:3}),
 (q:R73End {id:99}),
 (a)-[:R73Forward {value:1}]->(q), (q)-[:R73Reverse {value:1}]->(a),
 (b)-[:R73Forward {value:3}]->(q), (q)-[:R73Reverse {value:3}]->(b),
 (c)-[:R73Forward {value:3}]->(q), (q)-[:R73Reverse {value:3}]->(c),
 (d)-[:R73Forward {value:1}]->(q), (q)-[:R73Reverse {value:1}]->(d),
 (e)-[:R73Forward {value:3}]->(q), (q)-[:R73Reverse {value:3}]->(e),
 (f)-[:R73Forward]->(q), (q)-[:R73Reverse]->(f),
 (g)-[:R73Forward {value:3}]->(q), (q)-[:R73Reverse {value:3}]->(g),
 (h)-[:R73Forward {value:3}]->(q), (q)-[:R73Reverse {value:3}]->(h)`

type nodePatternDifferentialCase struct {
	name, query string
}

// This product is intentionally generated, rather than an inventory of past
// failures: every acceptance dimension is combined with each access direction
// and ordering/window shape. The unindexed query is the execution oracle.
func nodePatternDifferentialCases() []nodePatternDifferentialCase {
	labels := []string{":R73Label", ":R73Label:R73Wanted", ":R73Wanted:R73Label"}
	maps := []struct{ prefix, text string }{
		{"", ""}, {"", " {flag:true}"}, {"", " {value:null}"},
		{"UNWIND [null,3] AS x ", " {value:x}"},
	}
	wheres := []string{"", " WHERE n.id>0", " WHERE n.id IS NOT NULL", " WHERE (n.id>0 OR n.id IS NULL) AND coalesce(n.flag,false)=true"}
	orders := []string{"n.id", "n.id DESC", "n.id,n.rank DESC", "n.bucket,n.id DESC,n.rank"}
	windows := []string{"LIMIT 1", "LIMIT 2", "SKIP 1 LIMIT 2", "SKIP $skip LIMIT $limit"}
	var cases []nodePatternDifferentialCase
	for li, label := range labels {
		for mi, properties := range maps {
			for wi, where := range wheres {
				for oi, order := range orders {
					for pi, window := range windows {
						for shape := 0; shape < 3; shape++ {
							pattern := "(n" + label + properties.text + ")"
							switch shape {
							case 1:
								pattern += "-[:R73Forward]->(q:R73End)"
							case 2:
								pattern = "(q:R73End)-[:R73Reverse]->" + pattern
							}
							cases = append(cases, nodePatternDifferentialCase{
								name:  fmt.Sprintf("labels%d/map%d/where%d/order%d/window%d/shape%d", li, mi, wi, oi, pi, shape),
								query: properties.prefix + "MATCH " + pattern + where + " RETURN n.id ORDER BY " + order + " " + window,
							})
						}
					}
				}
			}
		}
	}
	// Path bindings are produced after node candidate collection. Generate
	// their predicates over the same labels/maps/order/window dimensions, so
	// a collector cannot silently treat a deferred WHERE as an empty WHERE.
	for li, label := range labels {
		for mi, properties := range maps {
			for wi, where := range []string{"nodes(p)[0].id>0", "nodes(p)[0].bucket=2"} {
				for oi, order := range orders {
					for pi, window := range windows {
						cases = append(cases, nodePatternDifferentialCase{
							name:  fmt.Sprintf("path_binding/labels%d/map%d/where%d/order%d/window%d", li, mi, wi, oi, pi),
							query: properties.prefix + "MATCH p=(n" + label + properties.text + ") WHERE " + where + " RETURN n.id ORDER BY " + order + " " + window,
						})
					}
				}
			}
		}
	}
	for _, extra := range []nodePatternDifferentialCase{
		{"row_null", "UNWIND [null] AS x MATCH (n:R73Label:R73Wanted {value:x}) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"},
		{"mixed_null_map", "MATCH (n:R73Label:R73Wanted {flag:true,value:null}) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"},
		{"parameter_map", "MATCH (n:R73Label:R73Wanted {value:$value}) WHERE n.id>0 RETURN n.id ORDER BY n.id DESC LIMIT 1"},
		{"label_where", "MATCH (n:R73Label) WHERE n:R73Wanted AND n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"},
		{"skip_zero", "MATCH (n:R73Label:R73Wanted) WHERE n.id>0 RETURN n.id ORDER BY n.id SKIP 0 LIMIT 1"},
		{"limit_zero", "MATCH (n:R73Label:R73Wanted) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 0"},
		{"unlabelled", "MATCH (n {flag:true}) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 2"},
		{"optional", "OPTIONAL MATCH (n:R73Label:R73Wanted {value:null}) RETURN n.id ORDER BY n.id LIMIT 1"},
		{"later_match", "MATCH (n:R73Label:R73Wanted) MATCH (n)-[:R73Forward {value:3}]->(q) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"},
		{"distinct", "MATCH (n:R73Label:R73Wanted) WHERE n.id>0 RETURN DISTINCT n.id ORDER BY n.id LIMIT 2"},
		{"aggregation", "MATCH (n:R73Label:R73Wanted) WHERE n.id>0 RETURN n.id,count(*) AS c ORDER BY n.id LIMIT 1"},
		{"with_filter", "MATCH (n:R73Label:R73Wanted) WITH n WHERE n.flag=true RETURN n.id ORDER BY n.id DESC LIMIT 1"},
	} {
		cases = append(cases, extra)
	}
	return cases
}

func nodePatternOracleSample(shape nodePatternDifferentialCase, index, total int) bool {
	return index%47 == 0 || index >= total-12 || strings.HasPrefix(shape.name, "path_binding/labels1/map1/where0/order0/")
}

func nodePatternDifferentialStore(t *testing.T, indexed bool) *storage.NamespacedEngine {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "node_pattern_diff")
	executor := NewStorageExecutor(store)
	_, err := executor.Execute(context.Background(), nodePatternDifferentialFixture, nil)
	require.NoError(t, err)
	if indexed {
		for _, property := range []string{"id", "bucket"} {
			_, err := executor.Execute(context.Background(), fmt.Sprintf("CREATE INDEX r73_diff_%s FOR (n:R73Label) ON (n.%s)", property, property), nil)
			require.NoError(t, err)
		}
	}
	return store
}

func executeNodePatternDifferential(t *testing.T, store storage.Engine, query string, explicit, profile bool) *ExecuteResult {
	t.Helper()
	executor := NewStorageExecutor(store)
	ctx := context.Background()
	if explicit {
		_, err := executor.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()
	}
	if profile {
		query = "PROFILE " + query
	}
	result, err := executor.Execute(ctx, query, map[string]interface{}{"skip": int64(2), "limit": int64(3), "value": int64(3)})
	require.NoError(t, err, query)
	if result.Rows == nil {
		result.Rows = [][]interface{}{}
	}
	return result
}

func TestNodePatternIndexDifferential(t *testing.T) {
	unindexed, indexed := nodePatternDifferentialStore(t, false), nodePatternDifferentialStore(t, true)
	for _, explicit := range []bool{false, true} {
		for _, profile := range []bool{false, true} {
			for _, shape := range nodePatternDifferentialCases() {
				t.Run(fmt.Sprintf("transaction=%t/profile=%t/%s", explicit, profile, shape.name), func(t *testing.T) {
					want := executeNodePatternDifferential(t, unindexed, shape.query, explicit, profile)
					got := executeNodePatternDifferential(t, indexed, shape.query, explicit, profile)
					require.Equal(t, want.Columns, got.Columns, shape.query)
					require.Equal(t, want.Rows, got.Rows, shape.query)
				})
			}
		}
	}
}

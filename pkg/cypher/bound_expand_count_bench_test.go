package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// holderBenchExecutor: one :Item hub that 4,000 :Part nodes resolve to, the
// parts spread over 100 :Doc nodes of 40 parts each (a document has 40
// relationships), and a :Source per part; :Doc and :Source keys indexed.
// No relationship is a merge.
func holderBenchExecutor(b testing.TB) *StorageExecutor {
	b.Helper()
	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "bench")
	nodes := []*storage.Node{{ID: "hub", Labels: []string{"Item"}, Properties: map[string]interface{}{"key": "hub"}}}
	var edges []*storage.Edge
	for d := 0; d < 100; d++ {
		doc := fmt.Sprintf("d%d", d)
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(doc), Labels: []string{"Doc"}, Properties: map[string]interface{}{"key": doc}})
		for p := 0; p < 40; p++ {
			part := fmt.Sprintf("%s-p%d", doc, p)
			source := part + "-s"
			nodes = append(nodes,
				&storage.Node{ID: storage.NodeID(part), Labels: []string{"Part"}, Properties: map[string]interface{}{"key": part, "doc": doc}},
				&storage.Node{ID: storage.NodeID(source), Labels: []string{"Source"}, Properties: map[string]interface{}{"key": source, "quoted": false}})
			edges = append(edges,
				&storage.Edge{ID: storage.EdgeID(part + "-in"), Type: "LINK", StartNode: storage.NodeID(doc), EndNode: storage.NodeID(part), Properties: map[string]interface{}{"kind": "contains", "src": source}},
				&storage.Edge{ID: storage.EdgeID(part + "-to"), Type: "LINK", StartNode: storage.NodeID(part), EndNode: "hub", Properties: map[string]interface{}{"kind": "resolves", "src": source}})
		}
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		b.Fatal(err)
	}
	if err := store.BulkCreateEdges(edges); err != nil {
		b.Fatal(err)
	}
	exec := NewStorageExecutor(store)
	for _, ddl := range []string{"CREATE INDEX FOR (d:Doc) ON (d.key)", "CREATE INDEX FOR (s:Source) ON (s.key)"} {
		if _, err := exec.Execute(context.Background(), ddl, nil); err != nil {
			b.Fatal(err)
		}
	}
	return exec
}

const holderReadStatement = `MATCH (t:Item {key: 'hub'})-[l:LINK]-(near:Part)
OPTIONAL MATCH (d:Doc {key: near.doc})
WITH near, d WHERE d IS NOT NULL AND %s
RETURN count(*) AS n`

// holderReadShapes are the per-row shapes of an effective holder read, with
// their one-row answer on holderBenchExecutor's graph: for each relationship
// of a hub, the owning document by an indexed key and a correlated
// [NOT] EXISTS between two bound nodes (Expand(Into)), with a nested
// citation check; and the zero-hop quantified merge path from a bound hub.
var holderReadShapes = []struct {
	name, query string
	want        interface{}
}{
	{"exists_into", fmt.Sprintf(holderReadStatement, "EXISTS { MATCH (d)-[own:LINK]-(near) WHERE own.kind = 'contains' }"), int64(4000)},
	{"not_exists_into", fmt.Sprintf(holderReadStatement, "NOT EXISTS { MATCH (d)-[own:LINK]-(near) WHERE own.kind = 'other' }"), int64(4000)},
	{"exists_into_cited", fmt.Sprintf(holderReadStatement, "EXISTS { MATCH (d)-[own:LINK]-(near) WHERE own.kind = 'contains' AND NOT EXISTS { MATCH (s:Source {key: own.src}) WHERE s.quoted } }"), int64(4000)},
	{"qpp_zero_hop_hub", "MATCH (s:Item {key: 'hub'}) MATCH (s)(()-[r:LINK WHERE r.kind = 'merged']-())*(m:Item) RETURN collect(DISTINCT m.key) AS keys", []interface{}{"hub"}},
	{"qpp_zero_hop_hub_incoming", "MATCH (s:Item {key: 'hub'}) OPTIONAL MATCH (s)(()<-[r:LINK WHERE r.kind = 'merged']-())*(m:Item) RETURN count(m) AS n", int64(1)},
}

func BenchmarkBoundExpandHolderRead(b *testing.B) {
	exec := holderBenchExecutor(b)
	ctx := context.Background()
	for _, tc := range holderReadShapes {
		b.Run(tc.name, func(b *testing.B) {
			if _, err := exec.Execute(ctx, tc.query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec.InvalidateCommittedWriteCaches()
				if _, err := exec.Execute(ctx, tc.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

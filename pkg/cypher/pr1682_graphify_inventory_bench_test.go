package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkPR1682GraphifyInventory profiles the current graphify_local.py
// adjacency query with its 200-source cap. It reuses the existing Graphify
// scenario payload (2K Code, 200 Document, 100 Entity nodes), seeded natively
// outside timing. Ten successive 200-source batches have different parameters,
// as during an ingest; runs longer than ten operations revisit those batches
// and can use default result caching. Every returned edge is checked against
// the fixture's independent edge list. This is in-process query cost, not HTTP
// throughput, importer time or qualification of the remote database.
func BenchmarkPR1682GraphifyInventory(b *testing.B) {
	const database = "graphify_bench"
	const repo = "orneryd/NornicDB"
	const batchSize = 200
	payload := buildGraphifyScenarioPayload(2000, 200, 100)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), database)
	ids := make(map[string]storage.NodeID, len(payload.nodes))
	for _, node := range payload.nodes {
		node.props["repo"], node.props["props_hash"] = repo, "node-hash"
		id, err := store.CreateNode(b.Context(), &storage.Node{ID: storage.NodeID(node.id), Labels: []string{node.label}, Properties: node.props})
		if err != nil {
			b.Fatal(err)
		}
		ids[node.id] = id
	}
	for index, edge := range payload.edges {
		edge.props["props_hash"] = "edge-hash"
		if err := store.CreateEdge(b.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("edge-%d", index)), StartNode: ids[edge.src], EndNode: ids[edge.tgt], Type: edge.relType, Properties: edge.props}); err != nil {
			b.Fatal(err)
		}
	}
	type inventoryBatch struct {
		params map[string]interface{}
		want   map[[3]string]struct{}
		seen   map[[3]string]struct{}
	}
	var batches []inventoryBatch
	for start := 0; start < 2000; start += batchSize {
		elements := make([]interface{}, 0, batchSize)
		sources := make(map[string]bool, batchSize)
		for _, node := range payload.nodes[start : start+batchSize] {
			// Returned node IDs are local to this NamespacedEngine. Use the
			// same element-ID owner as the public Cypher function.
			elements = append(elements, storage.NodeElementID(database, ids[node.id]))
			sources[node.id] = true
		}
		want := make(map[[3]string]struct{})
		for _, edge := range payload.edges {
			if sources[edge.src] {
				want[[3]string{edge.src, edge.tgt, edge.relType}] = struct{}{}
			}
		}
		batches = append(batches, inventoryBatch{map[string]interface{}{"repo": repo, "elements": elements}, want, make(map[[3]string]struct{}, len(want))})
	}
	exec := NewStorageExecutor(store)
	const query = "UNWIND $elements AS element MATCH (a) WHERE elementId(a) = element MATCH (a)-[r]->(b) WHERE a.repo = $repo AND b.repo = $repo RETURN a.id AS src, b.id AS tgt, type(r) AS rel, r.props_hash AS hash"
	defer pr1682ProfileQuery(b)()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch := batches[index%len(batches)]
		result, err := exec.Execute(b.Context(), query, batch.params)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(batch.want) {
			b.Fatalf("inventory rows: got %d, want %d", len(result.Rows), len(batch.want))
		}
		clear(batch.seen)
		for _, row := range result.Rows {
			if len(row) != 4 || row[3] != "edge-hash" {
				b.Fatalf("inventory row: %#v", row)
			}
			src, srcOK := row[0].(string)
			tgt, tgtOK := row[1].(string)
			relation, relationOK := row[2].(string)
			if !srcOK || !tgtOK || !relationOK {
				b.Fatalf("inventory identity types: %#v", row)
			}
			key := [3]string{src, tgt, relation}
			if _, exists := batch.want[key]; !exists {
				b.Fatalf("unexpected inventory edge: %v", key)
			}
			batch.seen[key] = struct{}{}
		}
		if len(batch.seen) != len(batch.want) {
			b.Fatal("inventory duplicated or omitted edges")
		}
	}
}

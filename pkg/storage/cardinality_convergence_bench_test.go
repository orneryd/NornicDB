package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// Relationship cardinality enforcement runs on two read authorities. These
// benchmarks measure both on the same population — the engine's publishing txn
// (raw adjacency index iteration and body decoding) and a transaction's staged
// view (pending images over committed adjacency) — and also measure the rejected
// alternative: counting the staged population through the transaction's
// current-adjacency owner, which materializes the overlay. Fixture creation is
// excluded and every call is checked.

// benchmarkCardinalityEngine builds an engine whose hub node has size committed
// OWNS relationships, plus a constraint that the population cannot yet fill, so
// each measured call counts the whole population.
func benchmarkCardinalityEngine(b *testing.B, size, pending int) (*BadgerEngine, Constraint) {
	b.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"test:hub", "test:leaf"} {
		if _, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}}); err != nil {
			b.Fatalf("CreateNode(%q): %v", id, err)
		}
	}
	for i := 0; i < size; i++ {
		edge := &Edge{
			ID: EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:hub", EndNode: "test:leaf", Type: "OWNS",
			Properties: map[string]any{},
		}
		if err := engine.CreateEdge(edge); err != nil {
			b.Fatalf("CreateEdge(%q): %v", edge.ID, err)
		}
	}
	return engine, Constraint{
		Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: size + pending + 1,
	}
}

// benchmarkCardinalityTx stages pending relationships over the committed hub
// adjacency: half rewrite a committed relationship, half are new, so a staged
// read must exclude the rewritten committed images and count both additions.
func benchmarkCardinalityTx(b *testing.B, engine *BadgerEngine, size, pending int) *BadgerTransaction {
	b.Helper()
	tx := &BadgerTransaction{
		engine:       engine,
		pendingEdges: make(map[EdgeID]*Edge, pending),
		deletedEdges: make(map[EdgeID]struct{}),
	}
	for i := 0; i < pending; i++ {
		id := EdgeID(fmt.Sprintf("test:e%d", i))
		if i%2 == 1 {
			id = EdgeID(fmt.Sprintf("test:p%d", i))
		}
		tx.pendingEdges[id] = &Edge{ID: id, StartNode: "test:hub", EndNode: "test:leaf", Type: "OWNS", Properties: map[string]any{}}
	}
	return tx
}

func benchmarkCardinalityProbe() *Edge {
	return &Edge{ID: "test:probe", StartNode: "test:hub", EndNode: "test:leaf", Type: "OWNS", Properties: map[string]any{}}
}

func BenchmarkCardinalityRoutes(b *testing.B) {
	for _, size := range []int{8, 2048} {
		size := size
		b.Run(fmt.Sprintf("direct/%d", size), func(b *testing.B) {
			engine, constraint := benchmarkCardinalityEngine(b, size, 0)
			probe := benchmarkCardinalityProbe()
			calls := 0
			b.ReportAllocs()
			b.ResetTimer()
			err := engine.withView(func(txn *badger.Txn) error {
				for i := 0; i < b.N; i++ {
					if err := engine.checkEdgeCardinalityInTxn(txn, probe, constraint, "test", ""); err != nil {
						return err
					}
					calls++
				}
				return nil
			})
			if err != nil {
				b.Fatalf("checkEdgeCardinalityInTxn: %v", err)
			}
			b.StopTimer()
			if calls != b.N {
				b.Fatalf("calls = %d, want %d", calls, b.N)
			}
		})

		for _, pending := range []int{0, size / 2} {
			pending := pending
			b.Run(fmt.Sprintf("staged/pending=%d/%d", pending, size), func(b *testing.B) {
				engine, constraint := benchmarkCardinalityEngine(b, size, pending)
				tx := benchmarkCardinalityTx(b, engine, size, pending)
				probe := benchmarkCardinalityProbe()
				calls := 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := tx.checkEdgeCardinality(probe, constraint, "test"); err != nil {
						b.Fatalf("checkEdgeCardinality: %v", err)
					}
					calls++
				}
				b.StopTimer()
				if calls != b.N {
					b.Fatalf("calls = %d, want %d", calls, b.N)
				}
			})

			b.Run(fmt.Sprintf("staged-current-adjacency/pending=%d/%d", pending, size), func(b *testing.B) {
				engine, constraint := benchmarkCardinalityEngine(b, size, pending)
				tx := benchmarkCardinalityTx(b, engine, size, pending)
				probe := benchmarkCardinalityProbe()
				calls := 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := stagedCardinalityViaCurrentAdjacency(tx, probe, constraint, "test"); err != nil {
						b.Fatalf("stagedCardinalityViaCurrentAdjacency: %v", err)
					}
					calls++
				}
				b.StopTimer()
				if calls != b.N {
					b.Fatalf("calls = %d, want %d", calls, b.N)
				}
			})
		}
	}
}

package storage

import (
	"fmt"
	"testing"
)

// BenchmarkPublicationWritePath is the write path's post-commit publication
// cost (F-PUBLISH) under one property index and one uniqueness constraint:
// direct and transactional create, update and delete, a read right after a
// write, and a 10,000-node transaction.
func BenchmarkPublicationWritePath(b *testing.B) {
	setup := func(b *testing.B) (*BadgerEngine, *SchemaManager) {
		b.Helper()
		engine, err := NewBadgerEngineInMemory()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = engine.Close() })
		sm := engine.GetSchemaForNamespace("db")
		if err := sm.AddPropertyIndex("bench_l_p", "L", []string{"p"}); err != nil {
			b.Fatal(err)
		}
		if err := sm.AddUniqueConstraint("bench_l_u", "L", "u"); err != nil {
			b.Fatal(err)
		}
		sm.MarkPropertyIndexesFilled()
		if err := RefreshUniqueConstraintValuesForEngine(engine, sm); err != nil {
			b.Fatal(err)
		}
		return engine, sm
	}
	node := func(i int, value string) *Node {
		return &Node{ID: NodeID(fmt.Sprintf("db:n%d", i)), Labels: []string{"L"}, Properties: map[string]any{"p": value, "u": fmt.Sprintf("u%d", i)}}
	}
	inTx := func(b *testing.B, engine *BadgerEngine, write func(tx *BadgerTransaction) error) {
		tx, err := engine.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := write(tx); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	const population = 1000
	populate := func(b *testing.B, engine *BadgerEngine, count int) {
		for i := 0; i < count; i++ {
			if _, err := engine.CreateNode(node(i, "v0")); err != nil {
				b.Fatal(err)
			}
		}
	}

	for _, explicit := range []bool{false, true} {
		mode := "direct"
		if explicit {
			mode = "tx"
		}
		b.Run("create/"+mode, func(b *testing.B) {
			engine, _ := setup(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if explicit {
					inTx(b, engine, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(node(i, "v")); return err })
				} else if _, err := engine.CreateNode(node(i, "v")); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("update/"+mode, func(b *testing.B) {
			engine, _ := setup(b)
			populate(b, engine, population)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				updated := node(i%population, fmt.Sprintf("v%d", i))
				if explicit {
					inTx(b, engine, func(tx *BadgerTransaction) error { return tx.UpdateNode(updated) })
				} else if err := engine.UpdateNode(updated); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("delete/"+mode, func(b *testing.B) {
			engine, _ := setup(b)
			populate(b, engine, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := NodeID(fmt.Sprintf("db:n%d", i))
				if explicit {
					inTx(b, engine, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
				} else if err := engine.DeleteNode(id); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("read_after_write", func(b *testing.B) {
		engine, _ := setup(b)
		populate(b, engine, population)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			updated := node(i%population, fmt.Sprintf("v%d", i))
			if err := engine.UpdateNode(updated); err != nil {
				b.Fatal(err)
			}
			for read := 0; read < 3; read++ {
				if _, err := engine.GetNode(updated.ID); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("tx_create_10k", func(b *testing.B) {
		engine, _ := setup(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inTx(b, engine, func(tx *BadgerTransaction) error {
				for j := 0; j < 10000; j++ {
					if _, err := tx.CreateNode(node(i*10000+j, "v")); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})
}

package storage

import (
	"fmt"
	"testing"
)

// BenchmarkConstraintContractLifetime measures contract work where it runs:
// commit validation of one entity, and the creation scan of existing data.
// Neither should pay predicate compilation per checked entity.
func BenchmarkConstraintContractLifetime(b *testing.B) {
	contract := ConstraintContract{
		Name:              "owns_item",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "COUNT { (n)-[:OWNS]->(:Item) } >= 1"}},
	}
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	const people = 64
	for i := 0; i < people; i++ {
		person, item := NodeID(fmt.Sprintf("test:p%d", i)), NodeID(fmt.Sprintf("test:i%d", i))
		if _, err := engine.CreateNode(&Node{ID: person, Labels: []string{"Person"}, Properties: map[string]any{}}); err != nil {
			b.Fatal(err)
		}
		if _, err := engine.CreateNode(&Node{ID: item, Labels: []string{"Item"}, Properties: map[string]any{}}); err != nil {
			b.Fatal(err)
		}
		if err := engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:o%d", i)), StartNode: person, EndNode: item, Type: "OWNS"}); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("creation_scan_64", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := ValidateConstraintContractOnCreationForEngine(engine, contract); err != nil {
				b.Fatal(err)
			}
		}
	})
	if err := engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.SetNamespace("test"); err != nil {
		b.Fatal(err)
	}
	node, err := tx.currentNodeLocked("test:p0")
	if err != nil {
		b.Fatal(err)
	}
	b.Run("commit_count_pattern", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := tx.validateConstraintContractsForNodeLocked(node); err != nil {
				b.Fatal(err)
			}
		}
	})
}

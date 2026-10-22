package cypher

import (
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var relationshipCrossPackageTargets []string

// These public operations also compile on the original bases, so the final
// paired A/B can use byte-identical fixtures without an internal API shim.
func BenchmarkRelationshipOwnerCrossPackage(b *testing.B) {
	b.Run("routing", func(b *testing.B) {
		analyzer := storage.NewQueryAnalyzer()
		analyzer.SetLabelRouting("End", []string{"a", "b"})
		all := []string{"a", "b"}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			info := analyzer.AnalyzeQuery("MATCH ( :End ) RETURN n")
			relationshipCrossPackageTargets = analyzer.RouteQuery(info, all)
		}
	})
	for _, cold := range []bool{false, true} {
		name := "contract_compile_hot"
		if cold {
			name = "contract_compile_cold"
		}
		b.Run(name, func(b *testing.B) {
			contract := storage.ConstraintContract{Name: "owner_cost", TargetEntityType: "node", TargetLabelOrType: "Start", Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindBooleanNode, Expression: "COUNT { (n)-[:R]->(:End) } >= 1"}}}
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				if cold {
					contract.Entries[0].Expression = "COUNT { (n)-[:R" + strconv.Itoa(index) + "]->(:End) } >= 1"
				}
				if err := storage.ValidateConstraintContractExpressions(contract); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Backfill must apply the write-side property equivalence contract, without
// hashing unhashable Go list values or widening adjacent exact integers.
func TestConstraintCreationValueEquivalence(t *testing.T) {
	restoreParserTypeAfter(t)
	cases := []struct {
		name      string
		a, b      interface{}
		duplicate bool
		ka, kb    string
	}{
		{"byte integer lists", []byte{1, 2}, []int64{1, 2}, true, "", ""},
		{"integer byte lists", []int64{1, 2}, []byte{1, 2}, true, "", ""},
		{"integer lists", []int64{1, 2}, []int64{1, 2}, true, "", ""},
		{"string lists", []string{"a", "b"}, []string{"a", "b"}, true, "", ""},
		{"numeric lists", []int64{1, 2}, []float64{1, 2}, true, "", ""},
		{"scalar numeric", int64(1), float64(1), true, "", ""},
		{"integer string distinction", int64(1), "1", false, "", ""},
		{"large integer distinction", int64(9007199254740993), int64(9007199254740992), false, "", ""},
		{"large mixed numeric distinction", int64(9007199254740993), float64(9007199254740992), false, "", ""},
		{"large lists distinction", []int64{9007199254740993}, []int64{9007199254740992}, false, "", ""},
		{"missing values", nil, nil, false, "", ""},
		{"delimiter distinction", "a\x00b", "a", false, "c", "b\x00c"},
	}
	for _, parser := range []string{"nornic", "antlr"} {
		for _, engine := range []string{"memory", "badger"} {
			for _, entity := range []string{"node", "relationship"} {
				for _, composite := range []bool{false, true} {
					for _, tc := range cases {
						t.Run(fmt.Sprintf("%s/%s/%s/composite=%t/%s", parser, engine, entity, composite, tc.name), func(t *testing.T) {
							config.SetParserType(parser)
							var base storage.Engine
							if engine == "badger" {
								b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
								if b != nil {
									fixtureowner.Own(t, b)
								}
								require.NoError(t, err)
								base = b
							} else {
								base = newTestMemoryEngine(t)
							}
							e := NewStorageExecutor(storage.NewNamespacedEngine(base, "ddl_values"))
							ctx := context.Background()
							seed := "CREATE (:DDLProbe {v:$a,k:$ka}),(:DDLProbe {v:$b,k:$kb})"
							pattern := "(n:DDLProbe)"
							expr := "n.v"
							if entity == "relationship" {
								seed = "CREATE ()-[:DDLProbe {v:$a,k:$ka}]->(),()-[:DDLProbe {v:$b,k:$kb}]->()"
								pattern = "()-[n:DDLProbe]-()"
							}
							_, err := e.Execute(ctx, seed, map[string]interface{}{"a": tc.a, "b": tc.b, "ka": tc.ka, "kb": tc.kb})
							require.NoError(t, err)
							if composite {
								expr = "(n.v,n.k)"
							}
							var ddlErr error
							require.NotPanics(t, func() {
								_, ddlErr = e.Execute(ctx, "CREATE CONSTRAINT ddl_values_unique FOR "+pattern+" REQUIRE "+expr+" IS UNIQUE", nil)
							})
							if tc.duplicate {
								require.Error(t, ddlErr, "duplicate pre-existing values must reject constraint creation")
							} else {
								require.NoError(t, ddlErr, "distinct/missing values must allow constraint creation")
							}
						})
					}
				}
			}
		}
	}
}

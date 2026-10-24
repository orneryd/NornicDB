package bolt

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestTypedNumericBoltBoundaries(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "numeric_wire")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, input := range listinputs.NumericInputs() {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"NumericWireProperty"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.Property}})
		require.NoError(t, err)
	}
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	for _, explicit := range []bool{false, true} {
		for _, input := range listinputs.NumericInputs() {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					records, err := runListInputBoltStatement(ctx, driver, "numeric_wire", explicit, query, params)
					require.NoError(t, err, query)
					require.Len(t, records, 1, query)
					return records[0].Values[0]
				}
				params := map[string]interface{}{"name": input.Name}
				forms := []struct{ Name, Prefix, List string }{{"typed-property", "MATCH (n:NumericWireProperty {name:$name}) ", "n.xs"}}
				if listinputs.NumericWireParameter(input, false) {
					params["xs"] = input.List
					forms = append(forms, struct{ Name, Prefix, List string }{"parameter", "", "$xs"},
						struct{ Name, Prefix, List string }{"with", "WITH $xs AS xs ", "xs"},
						struct{ Name, Prefix, List string }{"nested", "WITH {items:[$xs]} AS box ", "head(box.items)"})
					read(t, "CREATE (n:NumericWireWritten {xs:$xs}) RETURN size(n.xs) AS v", params)
					forms = append(forms, struct{ Name, Prefix, List string }{"written-property", "MATCH (n:NumericWireWritten) ", "n.xs"})
				}
				for _, form := range forms {
					for _, check := range listinputs.NumericWireChecks(input) {
						t.Run(form.Name+"/"+check.Expression, func(t *testing.T) {
							query := form.Prefix + listinputs.Query(check.Expression, form.List)
							require.Equal(t, check.Want, read(t, query, params), query)
						})
					}
					t.Run(form.Name+"/context-string", func(t *testing.T) {
						want := read(t, form.Prefix+listinputs.Query("toString(head(%s))", form.List), params)
						got := read(t, form.Prefix+listinputs.Query("apoc.map.setKey({},'v',toString(coalesce(%s,[])[0]))", form.List), params)
						require.Equal(t, map[string]interface{}{"v": want}, got)
					})
				}
				_, err := runListInputBoltStatement(ctx, driver, "numeric_wire", explicit, "MATCH (n:NumericWireWritten) DETACH DELETE n", nil)
				require.NoError(t, err)
			})
		}
	}
}

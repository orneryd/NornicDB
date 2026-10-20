package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricExactNumbersPublicComposite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		outer, inner interface{}
	}{
		{"string", "x", "x"},
		{"mixed_integer_float", int64(9007199254740992), float64(9007199254740992)},
		{"negative_zero", math.Copysign(0, -1), int64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			defer mgr.Close()
			require.NoError(t, mgr.CreateDatabase("a"))
			require.NoError(t, mgr.CreateDatabase("b"))
			require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
				{Alias: "a", DatabaseName: "a", Type: "local", AccessMode: "read_write"},
				{Alias: "b", DatabaseName: "b", Type: "local", AccessMode: "read_write"},
			}))
			a, err := mgr.GetStorage("a")
			require.NoError(t, err)
			b, err := mgr.GetStorage("b")
			require.NoError(t, err)
			_, err = a.CreateNode(&storage.Node{ID: "outer", Labels: []string{"Outer"}, Properties: map[string]interface{}{"k": tc.outer}})
			require.NoError(t, err)
			for i, v := range []int64{9007199254740992, 9007199254740993} {
				_, err = b.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint("inner", i)), Labels: []string{"Inner"}, Properties: map[string]interface{}{"k": tc.inner, "v": v}})
				require.NoError(t, err)
			}
			defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
			require.NoError(t, err)
			exec := NewStorageExecutor(defaultStore)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
			exec.cache = nil
			for _, direction := range []string{"ASC", "DESC"} {
				t.Run(direction, func(t *testing.T) {
					query := fmt.Sprintf(`USE cmp
CALL { USE cmp.a MATCH (a:Outer) RETURN a.k AS k }
CALL {
  WITH k USE cmp.b
  MATCH (b:Inner) WHERE b.k = k
  RETURN b.v AS v ORDER BY b.v %s LIMIT 1
}
RETURN v`, direction)
					result, err := exec.Execute(context.Background(), query, nil)
					require.NoError(t, err)
					want := int64(9007199254740992)
					if direction == "DESC" {
						want++
					}
					require.Equal(t, [][]interface{}{{want}}, result.Rows)
					require.True(t, exec.LastHotPathTrace().FabricBatchedApplyRows)
				})
			}
		})
	}
}

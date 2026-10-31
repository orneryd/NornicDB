package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericWireSchemaDDLTransactionRestart(t *testing.T) {
	for _, format := range []struct {
		name, header, token string
		want, rejected      interface{}
	}{
		{"v1_large_float", `"version":1,`, "1152921504606847000", float64(1 << 60), int64(1152921504606847000)},
		{"v1_out_of_range_float", `"version":1,`, "100000000000000000000", float64(1e20), "100000000000000000000"},
		{"v2_exact_integer", `"version":2,`, "9007199254740993", int64(9007199254740993), int64(9007199254740992)},
	} {
		t.Run(format.name, func(t *testing.T) {
			dir := t.TempDir()
			// Seed the historical schema bytes, including the old FLOAT writer's
			// rounded integral token, without passing through today's encoder.
			db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
			require.NoError(t, err)
			raw := []byte(fmt.Sprintf(`{%s"constraints":[{"name":"value_domain","type":"DOMAIN","entity_type":"NODE","label":"Value","properties":["v"],"allowed_values":[%s]}]}`, format.header, format.token))
			err = db.Update(func(txn *badger.Txn) error { return txn.Set([]byte("\x09test\x00"), raw) })
			closeErr := db.Close()
			require.NoError(t, err)
			require.NoError(t, closeErr)
			open := func() *storage.BadgerEngine {
				engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				require.NoError(t, err)
				return engine
			}
			engine := open()
			executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
			ctx := context.Background()
			for _, query := range []string{"BEGIN", "CREATE INDEX other_index FOR (n:Other) ON (n.v)", "COMMIT"} {
				_, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			require.NoError(t, engine.Close())
			engine = open()
			require.True(t, engine.GetSchemaForNamespace("test").MaintainsPropertyIndex("Other", "v"))
			require.Equal(t, []interface{}{format.want}, engine.GetSchemaForNamespace("test").ExportDefinition().Constraints[0].AllowedValues)
			executor = NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
			_, err = executor.Execute(ctx, "CREATE (:Value {v:$v})", map[string]interface{}{"v": format.want})
			require.NoError(t, err)
			_, err = executor.Execute(ctx, "CREATE (:Value {v:$v})", map[string]interface{}{"v": format.rejected})
			require.Error(t, err)
		})
	}
}

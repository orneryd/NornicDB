package cypher_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestEngineDecoratorsSizeTrackedMatch(t *testing.T) {
	for _, name := range []string{"direct", "T", "WT"} {
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			manager, err := multidb.NewDatabaseManager(t.Context(), base, multidb.NewConfigFromDefaultDatabase("tenant"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			engine, err := manager.GetStorage("tenant")
			require.NoError(t, err)
			_, err = engine.CreateNode(&storage.Node{ID: "record", Labels: []string{"DecoratorSize"}, Properties: map[string]interface{}{"id": int64(1)}})
			require.NoError(t, err)
			// Insert instrumentation beneath the real sizeTrackingEngine. The exported
			// Engine field is set by reflection because the concrete type is private.
			field := reflect.ValueOf(engine).Elem().FieldByName("Engine")
			require.True(t, field.CanSet())
			view := field.Interface().(storage.Engine)
			if name == "T" {
				field.Set(reflect.ValueOf(storage.NewTracedEngine(view)))
			}
			if name == "WT" {
				field.Set(reflect.ValueOf(storage.NewWALEngine(storage.NewTracedEngine(view), nil)))
			}
			result, err := cypher.NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n:DecoratorSize) RETURN n.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type inferenceR3Storage struct{ storage.Engine }

func (r inferenceR3Storage) GetStorage(string) (storage.Engine, error) { return r.Engine, nil }

func TestInferenceManagerR3SharedPublication(t *testing.T) {
	store := newTestMemoryEngine(t)
	defer store.Close()
	for _, kind := range []string{"override", "USE", "protocol"} {
		t.Run(kind, func(t *testing.T) {
			parent := NewStorageExecutor(store)
			var scoped *StorageExecutor
			switch kind {
			case "override":
				scoped = parent.cloneWithStorage(store)
			case "USE":
				scoped = parent.cloneForStorage(store)
			case "protocol":
				var err error
				scoped, err = NewDatabaseScopedExecutor(DatabaseScopedExecutorOptions{Storage: inferenceR3Storage{store}, Name: "nornic", BaseExecutor: parent})
				require.NoError(t, err)
			}
			// Scopes exist while the asynchronously constructed provider is absent.
			parent.SetInferenceManager(testInferenceManager{})
			result, err := scoped.Execute(context.Background(), "CALL db.infer({prompt:'scope'}) YIELD text RETURN text", nil)
			require.NoError(t, err)
			require.Equal(t, "ok", result.Rows[0][0])
			parent.SetInferenceManager(nil)
			require.Nil(t, scoped.GetInferenceManager(), "scoped aliases must observe withdrawal too")
		})
	}
}

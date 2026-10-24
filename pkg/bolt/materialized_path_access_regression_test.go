package bolt

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type materializedPathAccessEngine struct {
	*storage.MemoryEngine
	mu       sync.Mutex
	accessed []string
}

func (e *materializedPathAccessEngine) RecordMaterializedAccess(entityID string) {
	e.mu.Lock()
	e.accessed = append(e.accessed, entityID)
	e.mu.Unlock()
}

func (e *materializedPathAccessEngine) take() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	accessed := e.accessed
	e.accessed = nil
	sort.Strings(accessed)
	return accessed
}

func TestMaterializedPathAccessBolt(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "auto-commit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			inner := &materializedPathAccessEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
			t.Cleanup(func() { _ = inner.Close() })
			store := storage.NewNamespacedEngine(inner, "materialized")
			_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "a"}})
			require.NoError(t, err)
			_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"MaterializedPathAccess"}, Properties: map[string]interface{}{"id": "b"}})
			require.NoError(t, err)
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "MATERIALIZED_PATH_ACCESS"}))

			_, port := startBoltIntegrationServerWithExplicitTx(t, store)
			conn := openBoltTestConn(t, port)
			if explicit {
				require.NoError(t, SendBegin(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
			}
			rows := runBoltQueryAndCollectRecords(t, conn, "MATCH p=(:MaterializedPathAccess {id:'a'})-[:MATERIALIZED_PATH_ACCESS]->(:MaterializedPathAccess {id:'b'}) RETURN p")
			require.Len(t, rows, 1)
			require.Equal(t, []string{"materialized:a", "materialized:b", "materialized:e"}, inner.take())
			if explicit {
				require.NoError(t, SendRollback(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			}
		})
	}
}

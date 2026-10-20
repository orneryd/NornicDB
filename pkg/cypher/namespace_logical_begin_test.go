package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type namespaceBeginGate struct {
	*storage.BadgerEngine
	begin           func() (*storage.BadgerTransaction, error)
	entered, resume chan struct{}
	once            sync.Once
}

func (g *namespaceBeginGate) BeginTransaction() (*storage.BadgerTransaction, error) {
	g.once.Do(func() { close(g.entered); <-g.resume })
	return g.begin()
}

func TestNamespaceDropLogicalBeginPrecedesPhysicalSnapshot(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%v/explicit=%v", buffered, explicit), func(t *testing.T) {
				f := openTypedCountScope(t, t.TempDir(), buffered)
				typedCountRun(t, f.admin, "CREATE DATABASE logical_a")
				gate := &namespaceBeginGate{BadgerEngine: f.base, begin: f.base.BeginTransaction, entered: make(chan struct{}), resume: make(chan struct{})}
				var once sync.Once
				unblock := func() { once.Do(func() { close(gate.resume) }) }
				defer unblock()
				exec := NewStorageExecutor(storage.NewNamespacedEngine(gate, "logical_a"))
				exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: f.manager})
				done := make(chan error, 1)
				query := "CREATE (:StaleLogical)"
				if explicit {
					query = "BEGIN"
				}
				go func() { _, err := exec.Execute(context.Background(), query, nil); done <- err }()
				select {
				case <-gate.entered:
				case <-time.After(time.Second):
					t.Fatal("logical BEGIN did not reach snapshot admission")
				}
				typedCountRun(t, f.admin, "DROP DATABASE logical_a")
				typedCountRun(t, f.admin, "CREATE DATABASE logical_a")
				unblock()
				err := <-done
				if explicit && err == nil {
					_, err = exec.Execute(context.Background(), "CREATE (:StaleLogical)", nil)
					if err == nil {
						_, err = exec.Execute(context.Background(), "COMMIT", nil)
					}
				}
				require.Error(t, err, "the logical BEGIN must predate any wait to open its physical snapshot")
				if exec.HasActiveTransaction() {
					_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
				}
				typedCountWant(t, f.executor(t, "logical_a"), "MATCH (n:StaleLogical) RETURN count(n) AS n", 0)
			})
		}
	}
}

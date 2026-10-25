package multidb

// Dropping a database must not deliver storage listeners while the manager
// holds its lock: a listener that commits through the manager takes it too.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDropDatabaseWithWriteBackListener(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, event := range []string{"created", "reset", "metadata"} {
			t.Run(fmt.Sprintf("writeBehind=%v/%s", writeBehind, event), func(t *testing.T) {
				b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				m, err := NewDatabaseManager(b, nil)
				require.NoError(t, err)
				deadlocked := false
				defer func() {
					if !deadlocked {
						require.NoError(t, m.Close())
					}
				}()
				for _, name := range []string{"victim", "audit"} {
					require.NoError(t, m.CreateDatabase(name))
					require.NoError(t, m.SetDatabaseLimits(name, &Limits{Storage: StorageLimits{MaxNodes: 100}}))
				}
				audit, err := m.GetStorage("audit")
				require.NoError(t, err)
				victim, err := m.GetStorage("victim")
				require.NoError(t, err)

				var mu sync.Mutex
				var results []error
				var once sync.Once
				var armed atomic.Bool // metadata updates before the drop do not count
				writeBack := func() {
					once.Do(func() {
						tx, err := b.BeginTransaction()
						if err == nil {
							_, err = tx.CreateNode(&storage.Node{ID: "audit:written-back"})
						}
						if err == nil {
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							err = audit.(*sizeTrackingEngine).CommitTransaction(ctx, tx)
						}
						mu.Lock()
						results = append(results, err)
						mu.Unlock()
					})
				}
				switch event {
				case "created":
					b.OnNodeCreated(func(n *storage.Node) {
						if n.ID == "victim:prior" {
							writeBack()
						}
					})
				case "reset":
					b.OnNamespaceReset(func(prefix string) {
						if prefix == "victim:" {
							writeBack()
						}
					})
				case "metadata":
					b.OnNodeUpdated(func(n *storage.Node) {
						if armed.Load() && strings.HasPrefix(string(n.ID), "system:") {
							writeBack()
						}
					})
				}

				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				_, err = tx.CreateNode(&storage.Node{ID: "victim:prior"})
				require.NoError(t, err)
				require.NoError(t, victim.(*sizeTrackingEngine).CommitTransaction(context.Background(), tx))

				armed.Store(true)
				dropped := make(chan error, 1)
				go func() { dropped <- m.DropDatabase("victim") }()
				select {
				case err := <-dropped:
					require.NoError(t, err)
				case <-time.After(20 * time.Second):
					deadlocked = true
					t.Fatal("DropDatabase deadlocked delivering a listener that commits through the manager")
				}
				require.NoError(t, b.FlushWriteBehind())
				mu.Lock()
				defer mu.Unlock()
				require.Len(t, results, 1, "the write-back listener ran once")
				require.NoError(t, results[0])
			})
		}
	}
}

package multidb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type accountingPublicationPause struct {
	*storage.BadgerTransaction
	published chan struct{}
	resume    chan struct{}
}

func (p *accountingPublicationPause) CommitContextWithPublication(ctx context.Context, done func()) error {
	return p.BadgerTransaction.CommitContextWithPublication(ctx, func() {
		close(p.published)
		<-p.resume
		done()
	})
}

// Storage publication and quota accounting are one ordered transition. A base
// maintenance delete may finish while the managed caller is still in its tail.
// Its negative delta must never arrive before the creation it removes.
func TestManagedAccountingPublishesBeforeMaintenance(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, entity := range []string{"node", "edge"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, entity), func(t *testing.T) {
				base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
				require.NoError(t, err)
				manager, err := NewDatabaseManager(base, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				const db = "publication_accounting"
				require.NoError(t, manager.CreateDatabase(db))
				engine, err := manager.GetStorage(db)
				require.NoError(t, err)
				if entity == "edge" {
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = engine.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
				}
				before, _, _ := manager.GetStorageSize(db)
				tx, err := base.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace(db))
				require.NoError(t, tx.SetImplicit(buffered))
				if entity == "node" {
					_, err = tx.CreateNode(&storage.Node{ID: db + ":created"})
					require.NoError(t, err)
				} else {
					require.NoError(t, tx.CreateEdge(&storage.Edge{ID: db + ":created", StartNode: db + ":a", EndNode: db + ":b", Type: "LINK"}))
				}
				paused := &accountingPublicationPause{BadgerTransaction: tx, published: make(chan struct{}), resume: make(chan struct{})}
				done := make(chan error, 1)
				go func() { done <- engine.(*sizeTrackingEngine).commitTransaction(context.Background(), paused) }()
				select {
				case <-paused.published:
				case <-time.After(5 * time.Second):
					close(paused.resume)
					t.Fatal("managed publication did not finish")
				}
				if entity == "node" {
					err = base.DeleteNode(db + ":created")
				} else {
					err = base.DeleteEdge(db + ":created")
				}
				close(paused.resume)
				require.NoError(t, err)
				require.NoError(t, <-done)
				after, _, _ := manager.GetStorageSize(db)
				require.Equal(t, before, after)
				nodes, edges, err := manager.calculateStorageSizeFromEngine(engine)
				require.NoError(t, err)
				require.Equal(t, nodes+edges, after)
			})
		}
	}
}

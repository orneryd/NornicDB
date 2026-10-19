package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A policy check can commit without changing a body. Its old read snapshot is
// not the prior image of that commit: another writer may have changed the
// entity meanwhile. Accounting must describe only its own physical transition.
func TestAccountingNoOpDoesNotChargeConcurrentBodyWrite(t *testing.T) {
	for _, entity := range []string{"node", "edge"} {
		for _, operation := range []string{"create", "update", "delete"} {
			t.Run(fmt.Sprintf("%s/%s", entity, operation), func(t *testing.T) {
				b, err := NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				node := &Node{ID: "test:n", Properties: map[string]any{"payload": "before"}}
				edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"payload": "before"}}
				write := func(create bool) error {
					if entity == "edge" {
						if create {
							return b.CreateEdge(edge)
						}
						return b.UpdateEdge(edge)
					}
					if create {
						_, err := b.CreateNode(node)
						return err
					}
					return b.UpdateNode(node)
				}
				if operation != "create" {
					require.NoError(t, write(true))
				}
				type delivery struct {
					pair StoredAccountingPairs
					err  error
				}
				pairs := make(chan delivery, 4)
				b.RegisterAccountingOwner("test", func() func(StoredAccountingPairs, error) {
					return func(pair StoredAccountingPairs, err error) {
						pairs <- delivery{pair: pair, err: err}
					}
				})
				var key any = node.ID
				bodyKey := nodeKey(node.ID)
				if entity == "edge" {
					key, bodyKey = edge.ID, edgeKey(edge.ID)
				}
				read := make(chan struct{})
				resume := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- b.withPublishingUpdate(func(txn *badger.Txn) error {
						_, err := txn.Get(bodyKey)
						if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
							return err
						}
						close(read)
						<-resume
						return nil // a policy evaluation which needs no rewrite
					}, func() {}, func() []any { return []any{key} })
				}()
				select {
				case <-read:
				case <-time.After(5 * time.Second):
					t.Fatal("policy check did not open its snapshot")
				}
				node.Properties["payload"], edge.Properties["payload"] = "after", "after"
				if operation == "delete" {
					if entity == "node" {
						err = b.DeleteNode(node.ID)
					} else {
						err = b.DeleteEdge(edge.ID)
					}
				} else {
					err = write(operation == "create")
				}
				close(resume)
				require.NoError(t, err)
				select {
				case err = <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("policy check did not finish")
				}
				peer := <-pairs // the peer's actual body transition
				require.NoError(t, peer.err)
				noOp := <-pairs
				require.NoError(t, noOp.err)
				require.Equal(t, noOp.pair.Prior.Nodes, noOp.pair.Final.Nodes)
				require.Equal(t, noOp.pair.Prior.Edges, noOp.pair.Final.Edges)
			})
		}
	}
}

// Author: Lane #1644 (Codex). The public auth callback must retain the same
// pending mutation view and rollback owner for point and bulk node deletion.
package auth_test

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicStoreBulkView(t *testing.T) {
	for _, namespace := range []string{"system", "tenant"} {
		for _, abort := range []bool{false, true} {
			name := namespace + "/commit"
			if abort {
				name = namespace + "/abort"
			}
			t.Run(name, func(t *testing.T) {
				base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
				fixtureowner.OwnChecked(t, base)
				view := storage.NewNamespacedEngine(base, namespace)
				_, err := view.CreateNode(&storage.Node{ID: "kept", Labels: []string{"StoreRecord"}})
				require.NoError(t, err)
				rejected := errors.New("callback rejected")
				err = auth.WithStoreTransaction(context.Background(), view, func(issued storage.Engine) error {
					_, e := issued.CreateNode(&storage.Node{ID: "fresh", Labels: []string{"StoreRecord"}})
					if e != nil {
						return e
					}
					if e = issued.BulkDeleteNodes([]storage.NodeID{"kept", "fresh", "kept", "missing"}); e != nil {
						return e
					}
					if abort {
						return rejected
					}
					for _, id := range []storage.NodeID{"kept", "fresh"} {
						_, e = issued.GetNode(id)
						require.ErrorIs(t, e, storage.ErrNotFound, "bulk must stage deletion in the issued pending view")
					}
					return nil
				})
				if abort {
					require.ErrorIs(t, err, rejected)
					_, err = view.GetNode("kept")
					require.NoError(t, err, "a rejected callback cannot publish its bulk mutation")
				} else {
					require.NoError(t, err)
					_, err = view.GetNode("kept")
					require.ErrorIs(t, err, storage.ErrNotFound)
				}
				_, err = view.GetNode("fresh")
				require.ErrorIs(t, err, storage.ErrNotFound)
			})
		}
	}
}

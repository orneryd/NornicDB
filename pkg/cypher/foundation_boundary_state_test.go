package cypher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicUnboundAndEndedBoundaries(t *testing.T) {
	for _, ended := range []bool{false, true} {
		name := "unbound"
		if ended {
			name = "ended"
		}
		t.Run(name, func(t *testing.T) {
			base := enginePopulationBase(t)
			tenant := storage.NewNamespacedEngine(base, "tenant")
			view := &transactionStorageWrapper{underlying: tenant, namespace: "tenant", separator: ":"}
			want := storage.ErrNotImplemented
			if ended {
				owner := NewStorageExecutor(tenant)
				_, err := owner.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				view = owner.transactionStorageFor(owner.txContext.tx.(*storage.BadgerTransaction))
				_, err = owner.Execute(context.Background(), "ROLLBACK", nil)
				require.NoError(t, err)
				want = storage.ErrTransactionClosed
			}
			calls := &atomic.Int64{}
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "unexpected access beyond retained boundary", 500)
			}))
			defer peer.Close()
			remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "tenant"})
			require.NoError(t, err)
			defer remote.Close()
			subjects := []storage.Engine{view, storage.NewTracedEngine(storage.NewWALEngine(view, nil)), storage.NewCompositeEngine(map[string]storage.Engine{"one": tenant}, map[string]string{"one": "tenant"}, map[string]string{"one": "read_write"}).WithMembership(view), remote.WithMembership(view)}
			for _, subject := range subjects {
				member := NewStorageExecutor(subject)
				for _, query := range []string{"MATCH(n:L) RETURN n", "CREATE (:ForbiddenBoundaryWrite)", "BEGIN"} {
					_, err := member.Execute(context.Background(), query, nil)
					require.ErrorIs(t, err, want)
				}
			}
			require.Zero(t, calls.Load(), "constructor and public consumers cannot read a remote after its owner boundary refuses")
			nodes, err := tenant.GetNodesByLabel("ForbiddenBoundaryWrite")
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}

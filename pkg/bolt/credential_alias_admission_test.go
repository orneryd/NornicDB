package bolt

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialCompositeAliasBoltAdmission(t *testing.T) {
	for _, remoteLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote_last_%t", remoteLast), func(t *testing.T) {
			mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = mgr.Close() })
			require.NoError(t, mgr.CreateDatabase("localdb"))
			local := multidb.ConstituentRef{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "localdb"}
			remote := multidb.ConstituentRef{Alias: "winner", Type: "remote", AccessMode: "read", URI: "http://example.invalid", DatabaseName: "remote"}
			refs := []multidb.ConstituentRef{remote, local}
			want := "localdb"
			if remoteLast {
				refs = []multidb.ConstituentRef{local, remote}
				want = "comp"
			}
			require.NoError(t, mgr.CreateCompositeDatabase("comp", refs))
			var mu sync.Mutex
			var admitted []string
			server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8}, &mockExecutor{}, mgr)
			server.SetDatabaseConnectionAdmission(mgr.ResolveConnectionLimitOwner, func(owner string) error {
				if owner == mgr.DefaultDatabaseName() {
					return nil
				}
				mu.Lock()
				defer mu.Unlock()
				admitted = append(admitted, owner)
				return multidb.ErrConnectionLimitExceeded
			}, func(string) {})
			port := startBoltTestServer(t, server)
			for _, alias := range []string{"Winner", "winner", "WINNER", "wInNeR"} {
				ctx := context.Background()
				driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
				require.NoError(t, err)
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "comp." + alias})
				// Admission occurs at RUN before graph execution; the remote endpoint
				// is deliberately unavailable and its query result is irrelevant.
				_, err = session.Run(ctx, "RETURN 1", nil)
				require.Error(t, err, "actual Bolt admission must enforce the limit before graph execution")
				_ = session.Close(ctx)
				_ = driver.Close(ctx)
			}
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, admitted, 4, "every case variant must reach actual Bolt admission")
			for _, owner := range admitted {
				require.Equal(t, want, owner)
			}
		})
	}
}

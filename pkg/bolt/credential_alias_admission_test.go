package bolt

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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
			mgr, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
			require.NoError(t, err)
			fixtureowner.Own(t, mgr)
			require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "localdb"}))
			local := multidb.ConstituentRef{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "localdb"}
			remote := multidb.ConstituentRef{Alias: "winner", Type: "remote", AccessMode: "read", URI: "http://example.invalid", DatabaseName: "remote"}
			refs := []multidb.ConstituentRef{remote, local}
			want := "localdb"
			if remoteLast {
				refs = []multidb.ConstituentRef{local, remote}
				want = "comp"
			}
			require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "comp", refs))
			spacedLocal, plainRemote := local, remote
			spacedLocal.Alias, plainRemote.Alias = " r ", "r"
			require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "spacedcomp", []multidb.ConstituentRef{spacedLocal, plainRemote}))
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
			selections := []string{"comp.Winner", "comp.winner", "comp.WINNER", "comp.wInNeR", "spacedcomp. r ", "spacedcomp.r"}
			for _, selection := range selections {
				ctx := context.Background()
				driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
				require.NoError(t, err)
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: selection})
				// Admission occurs at RUN before graph execution; the remote endpoint
				// is deliberately unavailable and its query result is irrelevant.
				_, err = session.Run(ctx, "RETURN 1", nil)
				require.Error(t, err, "actual Bolt admission must enforce the limit before graph execution")
				_ = session.Close(ctx)
				_ = driver.Close(ctx)
			}
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{want, want, want, want, "localdb", "spacedcomp"}, admitted,
				"case folding must retain its existing winner while distinct spaced aliases keep their own admission owner")
		})
	}
}

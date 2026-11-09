package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// remoteParticipantFixture is a coordinator whose composite database
// "remoteowner" has one remote constituent, served over transport by another
// server; both servers have a 400 ms transaction timeout.
func remoteParticipantFixture(t *testing.T, transport string) (coordinator, remote *Server) {
	t.Helper()
	remote, token, _ := setupKeyWaitServer(t, 400*time.Millisecond, 0)
	router := remote.buildRouter()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", token)
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	uri := peer.URL
	if transport == "bolt" {
		uri = fabricLifetimeBolt(t, remote)
	}
	coordinator, _, _ = setupKeyWaitServer(t, 400*time.Millisecond, 0)
	require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), "remoteowner", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: uri}}))
	return coordinator, remote
}

// TestCoordinatorGovernsItsRemoteParticipant (review round 3 of #1763): a
// remote participant is never an end owner. The coordinator's transaction
// decides when the transaction ends, on HTTP and Bolt alike:
//   - working: a composite transaction that keeps working on the coordinator
//     longer than the timeout, without touching its remote constituent again,
//     still commits its remote write;
//   - idle: left idle past the coordinator's timeout, it rolls its remote part
//     back, so the remote key is free, and its COMMIT is refused with
//     TransactionTimedOut.
func TestCoordinatorGovernsItsRemoteParticipant(t *testing.T) {
	for _, transport := range []string{"http", "bolt"} {
		t.Run(transport+"/working", func(t *testing.T) {
			coordinator, remote := remoteParticipantFixture(t, transport)
			holder, err := coordinator.newExecutorForDatabase("remoteowner")
			require.NoError(t, err)
			_, err = holder.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
			_, err = holder.Execute(t.Context(), "USE remoteowner.r CREATE (:Doc {id: 'working'}) RETURN 1 AS k", nil)
			require.NoError(t, err)
			for i := 0; i < 12; i++ {
				_, err = holder.Execute(t.Context(), "RETURN 1 AS k", nil)
				require.NoError(t, err, "coordinator operation %d", i)
				time.Sleep(75 * time.Millisecond)
			}
			_, err = holder.Execute(t.Context(), "COMMIT", nil)
			require.NoError(t, err, "a composite transaction that kept working lost its remote participant")
			result, err := remote.db.ExecuteCypher(t.Context(), "MATCH (n:Doc {id: 'working'}) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.Rows[0][0], "the remote write was committed")
		})
		t.Run(transport+"/idle", func(t *testing.T) {
			coordinator, remote := remoteParticipantFixture(t, transport)
			holder, err := coordinator.newExecutorForDatabase("remoteowner")
			require.NoError(t, err)
			_, err = holder.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
			_, err = holder.Execute(t.Context(), "USE remoteowner.r MERGE (:Doc {id: 'idle'}) RETURN 1 AS k", nil)
			require.NoError(t, err)
			time.Sleep(900 * time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, err = remote.db.ExecuteCypher(ctx, "MERGE (:Doc {id: 'idle'})", nil)
			require.NoError(t, err, "the coordinator's ended transaction kept its remote key")
			_, err = holder.Execute(t.Context(), "COMMIT", nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", code, "COMMIT after the coordinator's timeout: %v", err)
		})
	}
}

// TestAbandonedRemoteParticipantReleasesItsKeys (review round 4 of #1763):
// a coordinator's participant on an HTTP server lives on a lease its
// coordinator renews while its composite transaction lives. A participant
// whose coordinator is gone (it opened the participant with the coordinator
// protocol, wrote, then sent nothing more and closed its connections) is
// ended by the remote server when its 30 s session expires unrenewed: its
// key is free afterwards.
func TestAbandonedRemoteParticipantReleasesItsKeys(t *testing.T) {
	remote, token, _ := setupKeyWaitServer(t, 400*time.Millisecond, 0)
	peer := httptest.NewServer(remote.buildRouter())
	defer peer.Close()
	client := &http.Client{Transport: &http.Transport{}}
	engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "nornic", AuthToken: token, HTTPClient: client})
	require.NoError(t, err)
	defer engine.Close()
	tx, err := engine.BeginCypherTx(storage.WithCoordinatorOwnedTransaction(t.Context()), storage.RemoteAccessWrite)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, _, err = tx.QueryCypher(t.Context(), "MERGE (:Doc {id: 'abandoned'})", nil)
	require.NoError(t, err)
	client.CloseIdleConnections()
	time.Sleep(32 * time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = remote.db.ExecuteCypher(ctx, "MERGE (:Doc {id: 'abandoned'})", nil)
	require.NoError(t, err, "an abandoned HTTP participant kept its key past its lease")
}

// TestWorkingCoordinatorKeepsItsRemoteParticipantPastTheSessionExpiry
// (review round 4 of #1763): a coordinator whose composite transaction keeps
// working on other constituents renews its HTTP participant's lease, so the
// participant outlives the remote server's 30 s session expiry without any
// statement of its own, and its write commits.
func TestWorkingCoordinatorKeepsItsRemoteParticipantPastTheSessionExpiry(t *testing.T) {
	coordinator, remote := remoteParticipantFixture(t, "http")
	holder, err := coordinator.newExecutorForDatabase("remoteowner")
	require.NoError(t, err)
	_, err = holder.Execute(t.Context(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	_, err = holder.Execute(t.Context(), "USE remoteowner.r MERGE (:Doc {id: 'working-long'}) RETURN 1 AS k", nil)
	require.NoError(t, err)
	for started := time.Now(); time.Since(started) < 32*time.Second; {
		_, err = holder.Execute(t.Context(), "RETURN 1 AS k", nil)
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	require.NoError(t, err)
	result, err := remote.db.ExecuteCypher(t.Context(), "MATCH (n:Doc {id: 'working-long'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}

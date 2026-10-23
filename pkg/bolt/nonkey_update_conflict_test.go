package bolt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltExplicitTransactionUniqueClaimConflict(t *testing.T) {
	for _, tc := range []struct {
		name       string
		query      string
		wantUnique bool
	}{
		{"non_key_set", "UNWIND $items AS item MATCH (n:Doc {id: item.id}) SET n.summary_input = item.fingerprint", false},
		{"same_key_set", "MATCH (n:Doc {id: 'original'}) SET n.id = 'original', n.summary_input = $fingerprint", false},
		{"changed_key", "MATCH (n:Doc {id: 'original'}) SET n.id = 'claimed', n.summary_input = $fingerprint", true},
		// Both transactions create before commit; a labelled MERGE can
		// instead serialize at key admission before reaching this owner.
		{"merge_create", "MERGE (n {id: 'claimed'}) SET n:Doc, n.summary_input = $fingerprint", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
			setup := openBoltTestConn(t, port)
			runBoltQueryAndCollectRecords(t, setup, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE")
			runBoltQueryAndCollectRecords(t, setup, "CREATE (:Doc {id: 'original', summary_input: 'before'})")
			loser, peer := openBoltTestConn(t, port), openBoltTestConn(t, port)
			for _, conn := range []net.Conn{loser, peer} {
				require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
			}
			require.NoError(t, SendBegin(t, loser, nil))
			require.NoError(t, ReadSuccess(t, loser))
			require.NoError(t, SendBegin(t, peer, nil))
			require.NoError(t, ReadSuccess(t, peer))
			for i, conn := range []net.Conn{loser, peer} {
				fingerprint := []string{"loser", "peer"}[i]
				require.NoError(t, SendRun(t, conn, tc.query, map[string]interface{}{
					"fingerprint": fingerprint,
					"items":       []interface{}{map[string]interface{}{"id": "original", "fingerprint": fingerprint}},
				}, nil))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
			}
			require.NoError(t, SendCommit(t, peer))
			require.NoError(t, ReadSuccess(t, peer))
			require.NoError(t, SendCommit(t, loser))
			code, message, err := AssertFailure(t, loser)
			require.NoError(t, err)
			t.Logf("commit failure: %s: %s", code, message)
			if tc.wantUnique {
				require.Contains(t, message, "Constraint violation (UNIQUE")
			} else {
				require.Equal(t, "Neo.TransientError.Transaction.Outdated", code)
				require.Contains(t, message, "changed after transaction start")
				require.NotContains(t, message, "constraint violation")
			}
		})
	}
}

// Neo4j holds a write lock until commit, so the second non-key update can
// wait rather than fail. Its result must be success or a transient failure,
// never a uniqueness error. Run with tnrun --ref neo-ref.
func TestNeo4jExplicitTransactionNonKeyUpdate(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires Neo4j reference")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())
	setup := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer setup.Close(context.Background())
	label := fmt.Sprintf("NonKeyConflict%d", time.Now().UnixNano())
	run := func(query string) {
		result, err := setup.Run(ctx, query, nil)
		require.NoError(t, err)
		_, err = result.Consume(ctx)
		require.NoError(t, err)
	}
	run(fmt.Sprintf("CREATE CONSTRAINT FOR (n:%s) REQUIRE n.id IS UNIQUE", label))
	run(fmt.Sprintf("CREATE (:%s {id:'original'})", label))
	firstSession := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer firstSession.Close(context.Background())
	secondSession := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer secondSession.Close(context.Background())
	first, err := firstSession.BeginTransaction(ctx)
	require.NoError(t, err)
	defer first.Close(context.Background())
	second, err := secondSession.BeginTransaction(ctx)
	require.NoError(t, err)
	defer second.Close(context.Background())
	query := fmt.Sprintf("UNWIND $items AS item MATCH (n:%s {id:item.id}) SET n.summary_input = item.fingerprint", label)
	write := func(tx neo4j.ExplicitTransaction, fingerprint string) error {
		result, err := tx.Run(ctx, query, map[string]interface{}{"items": []interface{}{map[string]interface{}{"id": "original", "fingerprint": fingerprint}}})
		if err != nil {
			return err
		}
		_, err = result.Consume(ctx)
		return err
	}
	require.NoError(t, write(first, "first"))
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		err := write(second, "second")
		if err == nil {
			err = second.Commit(ctx)
		}
		done <- err
	}()
	<-started
	require.NoError(t, first.Commit(ctx))
	err = <-done
	if err != nil {
		var status *neo4j.Neo4jError
		require.True(t, errors.As(err, &status), "%v", err)
		require.Equal(t, "TransientError", status.Classification())
		t.Logf("Neo4j concurrent non-key update: %s", status.Code)
	} else {
		t.Log("Neo4j concurrent non-key update: both commits succeeded; no error status")
	}
}

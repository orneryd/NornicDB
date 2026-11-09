package bolt

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionOverlayBoltRoutes(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		_, port := startBoltIntegrationServer(t, storage.NewNamespacedEngine(base, "test"), boltIntegrationExplicitTx)
		conn := openBoltTestConn(t, port)
		defer conn.Close()
		if explicit {
			require.NoError(t, SendBegin(t, conn, nil))
			require.NoError(t, ReadSuccess(t, conn))
		}
		const create = `UNWIND range(1,64) AS i MERGE (a:Overlay {id:i % 7}) MERGE (b:Overlay {id:(i+1) % 7}) CREATE (a)-[:R {v:i}]->(b) RETURN count(*)`
		for run := 0; run < 2; run++ {
			require.Equal(t, [][]interface{}{{int64(64)}}, runBoltQueryWithParams(t, conn, create, nil))
		}
		require.Equal(t, [][]interface{}{{int64(7)}}, runBoltQueryWithParams(t, conn, "MATCH (n:Overlay) DETACH DELETE n RETURN count(*)", nil))
		if explicit {
			require.NoError(t, SendMessage(conn, []byte{0xB0, MsgCommit}))
			require.NoError(t, ReadSuccess(t, conn))
		}
		require.Equal(t, [][]interface{}{{int64(0)}}, runBoltQueryWithParams(t, conn, "MATCH (n:Overlay) RETURN count(n)", nil))
		edges, err := base.AllEdges(context.Background())
		require.NoError(t, err)
		require.Empty(t, edges)
	}
}

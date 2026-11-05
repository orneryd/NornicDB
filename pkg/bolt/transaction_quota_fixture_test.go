package bolt

import (
	"net"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newBoltQuotaFixture keeps the real store, database and server in one lifetime
// owner. A scenario can decorate that exact store before the manager is built.
func newBoltQuotaFixture(t *testing.T, limits *multidb.Limits, decorate func(*storage.MemoryEngine) storage.Engine) (*multidb.DatabaseManager, int) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	var engine storage.Engine = base
	if decorate != nil {
		engine = decorate(base)
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "quota"}))
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), "quota", limits))
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
	return manager, startBoltTestServer(t, server)
}

// requireQuotaResetCountsZero checks recovery on the original connection.
func requireQuotaResetCountsZero(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendRun(t, conn, "MATCH (n:Quota) RETURN count(n)", nil, map[string]any{"db": "quota"}))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendPull(t, conn, nil))
	record, err := AssertRecord(t, conn)
	require.NoError(t, err)
	require.Equal(t, []any{int64(0)}, record)
	require.NoError(t, ReadSuccess(t, conn))
}

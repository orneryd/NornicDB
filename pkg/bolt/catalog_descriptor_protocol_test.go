package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testutil/remotegraph"
	"github.com/stretchr/testify/require"
)

func TestBoltCatalogDescriptorPublicViewsAndFabric(t *testing.T) {
	manager, peer := remotegraph.New(t)
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	session := driver.NewSession(t.Context(), neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	t.Cleanup(func() { require.NoError(t, session.Close(context.Background())) })
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := session.Run(t.Context(), query, nil)
		require.NoError(t, err)
		records, err := result.Collect(t.Context())
		require.NoError(t, err)
		rows := make([][]interface{}, len(records))
		for i, record := range records {
			rows[i] = record.Values
		}
		return rows
	}
	remotegraph.CheckPublicRead(t, peer, run)
}

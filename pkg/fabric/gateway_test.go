package fabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryGatewayExecute(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})

	mock := &mockCypherExecutor{results: map[string]*ResultStream{
		"MATCH (n) RETURN 1 AS ok": {
			Columns: []string{"ok"},
			Rows:    [][]interface{}{{int64(1)}},
		},
	}}

	gateway := NewQueryGateway(
		NewFabricPlanner(catalog),
		NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil),
	)

	res, err := gateway.Execute(context.Background(), NewFabricTransaction("tx-1"), "MATCH (n) RETURN 1 AS ok", "db1", nil, "")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"ok"}, res.Columns)
	require.Len(t, res.Rows, 1)
}

func TestQueryGatewayExecuteInvalidConfig(t *testing.T) {
	_, err := (&QueryGateway{}).Execute(context.Background(), nil, "MATCH (n) RETURN n", "nornic", nil, "")
	require.Error(t, err)
}

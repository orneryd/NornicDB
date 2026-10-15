package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

func TestServerExtraTinySettersAndAccessors(t *testing.T) {
	metrics := &observability.HTTPMetrics{}
	manager := &multidb.DatabaseManager{}
	server := &Server{dbManager: manager}

	require.Nil(t, (*Server)(nil).GetDatabaseManager())
	require.Same(t, manager, server.GetDatabaseManager())

	server.SetHTTPMetrics(metrics)
	server.mu.RLock()
	require.Same(t, metrics, server.httpMetrics)
	server.mu.RUnlock()

	server.SetHTTPMetrics(nil)
	server.mu.RLock()
	require.Nil(t, server.httpMetrics)
	server.mu.RUnlock()
}

func TestServerExtraStatusCoercionHelpers(t *testing.T) {
	require.Equal(t, int64(7), int64FromStatus(int64(7)))
	require.Equal(t, int64(8), int64FromStatus(8))
	require.Equal(t, int64(9), int64FromStatus(9.9))
	require.Equal(t, int64(0), int64FromStatus("nope"))

	require.Equal(t, 1.25, float64FromStatus(1.25))
	require.Equal(t, float64(float32(2.5)), float64FromStatus(float32(2.5)))
	require.Equal(t, 3.0, float64FromStatus(int64(3)))
	require.Equal(t, 4.0, float64FromStatus(4))
	require.Equal(t, 0.0, float64FromStatus(struct{}{}))
}

func TestServerExtraStatusClassBoundaries(t *testing.T) {
	cases := map[int]string{
		100: "1xx",
		199: "1xx",
		200: "2xx",
		302: "3xx",
		404: "4xx",
		500: "5xx",
		599: "5xx",
		99:  "5xx",
		700: "5xx",
	}
	for code, want := range cases {
		require.Equal(t, want, statusClass(code), "status %d", code)
	}
}

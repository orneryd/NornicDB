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

func TestServerExtraStatementTargetDatabaseBranches(t *testing.T) {
	cases := []struct {
		name      string
		defaultDB string
		statement string
		want      string
		wantErr   string
	}{
		{name: "empty statement", defaultDB: " neo4j ", statement: "  ", want: "neo4j"},
		{name: "normal statement", defaultDB: "neo4j", statement: "MATCH (n) RETURN n", want: "neo4j"},
		{name: "colon use", defaultDB: "neo4j", statement: ":USE tenant MATCH (n)", want: "tenant"},
		{name: "bare colon use falls through", defaultDB: "neo4j", statement: ":USE", want: "neo4j"},
		{name: "bare use falls through", defaultDB: "neo4j", statement: "USE", want: "neo4j"},
		// A USE clause is routed by the request database's executor (#738).
		{name: "graph by name", defaultDB: "neo4j", statement: "USE graph.byName('tenant-db')", want: "neo4j"},
		{name: "graph reference invalid", defaultDB: "neo4j", statement: "USE graph.byName(", want: "neo4j"},
		{name: "quoted database", defaultDB: "neo4j", statement: "USE `tenant``one` MATCH (n)", want: "neo4j"},
		{name: "plain use", defaultDB: "neo4j", statement: "USE tenant RETURN 1", want: "neo4j"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := statementTargetDatabase(tc.defaultDB, tc.statement)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

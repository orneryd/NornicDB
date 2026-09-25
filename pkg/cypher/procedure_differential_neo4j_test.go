package cypher

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProcedureDifferentialNeo4j(t *testing.T) {
	uri := os.Getenv("NEO4J_URI")
	user := os.Getenv("NEO4J_USERNAME")
	pass := os.Getenv("NEO4J_PASSWORD")
	if uri == "" || user == "" || pass == "" {
		t.Skip("NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD not configured")
	}

	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, pass, ""))
	require.NoError(t, err)
	defer func() { _ = driver.Close(ctx) }()

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	nornic := NewStorageExecutor(store)

	t.Run("dbms.procedures basic discovery", func(t *testing.T) {
		nornicRes, err := nornic.Execute(ctx, "CALL dbms.procedures() YIELD name RETURN name LIMIT 1", nil)
		require.NoError(t, err)
		require.NotEmpty(t, nornicRes.Rows)

		neoRows, err := runNeo4jQuery(ctx, driver, "CALL dbms.procedures() YIELD name RETURN name LIMIT 1")
		require.NoError(t, err)
		require.NotEmpty(t, neoRows)
	})

	t.Run("unknown procedure error parity", func(t *testing.T) {
		_, nornicErr := nornic.Execute(ctx, "CALL does.not.exist()", nil)
		require.Error(t, nornicErr)
		require.Contains(t, nornicErr.Error(), "Neo.ClientError.Procedure.ProcedureNotFound: There is no procedure with the name")

		_, neoErr := runNeo4jQuery(ctx, driver, "CALL does.not.exist()")
		require.Error(t, neoErr)
		require.Contains(t, strings.ToLower(neoErr.Error()), "procedure")
	})

	t.Run("unknown yield column error parity", func(t *testing.T) {
		_, nornicErr := nornic.Execute(ctx, "CALL dbms.procedures() YIELD notAColumn RETURN notAColumn", nil)
		require.Error(t, nornicErr)
		require.Contains(t, strings.ToLower(nornicErr.Error()), "yield")

		_, neoErr := runNeo4jQuery(ctx, driver, "CALL dbms.procedures() YIELD notAColumn RETURN notAColumn")
		require.Error(t, neoErr)
		require.Contains(t, strings.ToLower(neoErr.Error()), "yield")
	})
}

func runNeo4jQuery(ctx context.Context, driver neo4j.DriverWithContext, q string) ([]map[string]interface{}, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() { _ = session.Close(ctx) }()
	result, err := session.Run(ctx, q, nil)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]interface{}, 0)
	for result.Next(ctx) {
		record := result.Record()
		m := make(map[string]interface{}, len(record.Keys))
		for i, k := range record.Keys {
			if i < len(record.Values) {
				m[k] = record.Values[i]
			}
		}
		rows = append(rows, m)
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

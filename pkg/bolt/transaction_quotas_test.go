package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltTransactionQuotas(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
	require.NoError(t, manager.CreateDatabase("quota"))
	require.NoError(t, manager.SetDatabaseLimits("quota", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "quota"})
			defer func() { _ = session.Close(ctx) }()
			query := "UNWIND [1, 2] AS id CREATE (:Quota {id: id})"
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = tx.Close(ctx) }()
				result, err := tx.Run(ctx, query, nil)
				require.NoError(t, err)
				_, err = result.Consume(ctx)
				require.NoError(t, err)
				err = tx.Commit(ctx)
				require.ErrorContains(t, err, "max_nodes")
				var neoErr *neo4jdriver.Neo4jError
				require.True(t, errors.As(err, &neoErr))
				require.Equal(t, multidb.QuotaCapacityStatus, neoErr.Code)
			} else {
				result, err := session.Run(ctx, query, nil)
				if err == nil {
					_, err = result.Consume(ctx)
				}
				require.ErrorContains(t, err, "max_nodes")
				var neoErr *neo4jdriver.Neo4jError
				require.True(t, errors.As(err, &neoErr))
				require.Equal(t, multidb.QuotaCapacityStatus, neoErr.Code)
			}
			engine, err := manager.GetStorage("quota")
			require.NoError(t, err)
			count, err := engine.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestBoltTransactionQuotaCodesAndSameConnectionReuse(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, kind := range []string{"capacity", "rate"} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicit, kind), func(t *testing.T) {
				base := storage.NewMemoryEngine()
				manager, err := multidb.NewDatabaseManager(base, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
				require.NoError(t, manager.CreateDatabase("quota"))
				limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}
				expected := multidb.QuotaCapacityStatus
				if kind == "rate" {
					limits = &multidb.Limits{Rate: multidb.RateLimits{MaxWritesPerSecond: 1}}
					expected = multidb.QuotaRateStatus
				}
				require.NoError(t, manager.SetDatabaseLimits("quota", limits))
				server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
				port := startBoltTestServer(t, server)
				conn := openBoltTestConn(t, port)
				require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
				if explicit {
					beginExplicitTransaction(t, conn, map[string]any{"db": "quota"})
				}
				query := "UNWIND [1,2] AS id CREATE (:Quota {id:id})"
				require.NoError(t, SendRun(t, conn, query, nil, map[string]any{"db": "quota"}))
				if explicit {
					require.NoError(t, ReadSuccess(t, conn))
					require.NoError(t, SendPull(t, conn, nil))
					require.NoError(t, ReadSuccess(t, conn))
					require.NoError(t, SendCommit(t, conn))
				}
				code, _, err := AssertFailure(t, conn)
				require.NoError(t, err)
				require.Equal(t, expected, code)
				require.NoError(t, SendReset(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
				// A read on the very same socket proves RESET recovery after each rejection.
				require.NoError(t, SendRun(t, conn, "MATCH (n:Quota) RETURN count(n)", nil, map[string]any{"db": "quota"}))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				record, err := AssertRecord(t, conn)
				require.NoError(t, err)
				require.Equal(t, []any{int64(0)}, record)
				require.NoError(t, ReadSuccess(t, conn))
			})
		}
	}
}

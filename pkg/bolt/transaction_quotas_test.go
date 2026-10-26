package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var quotaReadFailure = errors.New("injected quota admission read failure")

func TestBoltTransactionQuotas(t *testing.T) {
	t.Run("capacity-driver", func(t *testing.T) {

		ctx := context.Background()
		manager, port := newBoltQuotaFixture(t, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, nil)
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

	})
	t.Run("read-failure-reuse", func(t *testing.T) {

		for _, tc := range []struct {
			name string
			mode storagefault.ReadOperation
		}{{"node", storagefault.NodeRead}, {"edge", storagefault.EdgeRead}, {"nodes", storagefault.NodeCountRead}, {"edges", storagefault.EdgeCountRead}, {"bytes", storagefault.NodeStreamRead}} {
			t.Run(tc.name, func(t *testing.T) {
				limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100, MaxEdges: 100}}
				if tc.mode == storagefault.NodeStreamRead {
					limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 100000}}
				}
				var base *storagefault.ReadFailureEngine
				_, port := newBoltQuotaFixture(t, limits, func(real *storage.MemoryEngine) storage.Engine {
					base = storagefault.NewReadFailureEngine(real, real, quotaReadFailure)
					return base
				})
				conn := openBoltTestConn(t, port)
				require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
				beginExplicitTransaction(t, conn, map[string]any{"db": "quota"})
				query := "CREATE (:Quota)"
				if tc.mode == storagefault.EdgeRead || tc.mode == storagefault.EdgeCountRead {
					query = "CREATE (:Quota)-[:LINK]->(:Quota)"
				}
				require.NoError(t, SendRun(t, conn, query, nil, map[string]any{"db": "quota"}))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
				base.SetFailure(tc.mode)
				require.NoError(t, SendCommit(t, conn))
				_, message, err := AssertFailure(t, conn)
				require.NoError(t, err)
				require.Contains(t, message, quotaReadFailure.Error(), "the admission read seam must be exercised")
				base.SetFailure("")
				requireQuotaResetCountsZero(t, conn)

			})
		}

	})
}

func TestBoltTransactionQuotaCodesAndSameConnectionReuse(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, kind := range []string{"capacity", "rate"} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicit, kind), func(t *testing.T) {
				limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}
				expected := multidb.QuotaCapacityStatus
				if kind == "rate" {
					limits = &multidb.Limits{Rate: multidb.RateLimits{MaxWritesPerSecond: 1}}
					expected = multidb.QuotaRateStatus
				}
				_, port := newBoltQuotaFixture(t, limits, nil)
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
				requireQuotaResetCountsZero(t, conn)

			})
		}
	}
}

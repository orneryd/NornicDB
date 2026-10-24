package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/transactionalcalltest"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltTransactionalCallStructuralMatrix(t *testing.T) {
	ctx := context.Background()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "tenant"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	query := func(t *testing.T, session neo4j.SessionWithContext, statement string) []*neo4j.Record {
		t.Helper()
		result, err := session.Run(ctx, statement, nil)
		require.NoError(t, err)
		rows, err := result.Collect(ctx)
		require.NoError(t, err)
		return rows
	}
	transactionalcalltest.Check(t, func(t *testing.T, sample transactionalcalltest.Case) transactionalcalltest.Outcome {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		query(t, session, transactionalcalltest.Reset)
		var result neo4j.ResultWithContext
		var queryErr error
		var returned []*neo4j.Record
		if sample.Explicit {
			tx, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			if sample.Before != "" {
				before, err := tx.Run(ctx, sample.Before, nil)
				require.NoError(t, err)
				_, err = before.Consume(ctx)
				require.NoError(t, err)
			}
			result, queryErr = tx.Run(ctx, sample.Query, sample.Params)
			if queryErr == nil {
				returned, queryErr = result.Collect(ctx)
			}
			_ = tx.Rollback(ctx)
		} else {
			if sample.Before != "" {
				query(t, session, sample.Before)
			}
			result, queryErr = session.Run(ctx, sample.Query, sample.Params)
			if queryErr == nil {
				returned, queryErr = result.Collect(ctx)
			}
		}
		require.NoError(t, session.Close(ctx))
		got := transactionalcalltest.Outcome{}
		if queryErr == nil {
			for _, record := range returned {
				got.Rows = append(got.Rows, record.Values)
			}
		}
		if queryErr != nil {
			var status *neo4j.Neo4jError
			require.True(t, errors.As(queryErr, &status), "%v", queryErr)
			got.Code = status.Code
		}
		reader := driver.NewSession(ctx, neo4j.SessionConfig{})
		for _, row := range query(t, reader, transactionalcalltest.Read) {
			got.Values = append(got.Values, row.Values[0].(int64))
		}
		require.NoError(t, reader.Close(ctx))
		return got
	})
}

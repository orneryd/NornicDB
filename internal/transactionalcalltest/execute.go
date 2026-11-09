package transactionalcalltest

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// CheckExecute runs the public Execute adapter shared by the fixture and Cypher tests.
func CheckExecute(t *testing.T) {
	ctx := context.Background()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
	Check(t, func(t *testing.T, sample Case) Outcome {
		_, err := exec.Execute(ctx, Reset, nil)
		require.NoError(t, err)
		if sample.Explicit {
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		if sample.Before != "" {
			_, err = exec.Execute(ctx, sample.Before, nil)
			require.NoError(t, err)
		}
		queryResult, queryErr := exec.Execute(ctx, sample.Query, sample.Params)
		got := Outcome{}
		if queryErr == nil && queryResult != nil {
			got.Rows = queryResult.Rows
		}
		if queryErr != nil {
			var classified interface{ BoltErrorCode() string }
			require.ErrorAs(t, queryErr, &classified)
			got.Code = classified.BoltErrorCode()
		}
		if sample.Explicit {
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(ctx, Read, nil)
		require.NoError(t, err)
		for _, row := range result.Rows {
			got.Values = append(got.Values, row[0].(int64))
		}
		return got
	})
}

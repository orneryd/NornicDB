package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type invariantCreateErrEngine struct {
	storage.Engine
	err error
}

func (e *invariantCreateErrEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return "", e.err
}

func TestCypherInvariantErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	t.Run("pipeline create failure", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		store := storage.NewNamespacedEngine(base, "pipeline_invariant")
		seed := NewStorageExecutor(store)
		_, err := seed.Execute(context.Background(), "CREATE (:Person {id:'p1'})", nil)
		require.NoError(t, err)

		cause := errors.New("create failed")
		exec := NewStorageExecutor(&invariantCreateErrEngine{Engine: store, err: cause})
		_, handled, err := exec.executePipeline(context.Background(), "MATCH (n:Person) WITH n CREATE (:Tmp {id:'t1'}) RETURN n")
		require.True(t, handled)
		require.EqualError(t, err, "pipeline CREATE failed: failed to create node: create failed")
		require.ErrorIs(t, err, cause)

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherInvariantsPipelineCreateFailed, localizedErr.Message.ID)
		require.Equal(t, "failed to create node: create failed", localizedErr.Message.Data["Cause"])
	})

	t.Run("transaction prefix deletion", func(t *testing.T) {
		wrapper := &transactionStorageWrapper{}
		_, _, err := wrapper.DeleteByPrefix("tenant:")
		require.EqualError(t, err, "DeleteByPrefix not supported within transaction context")

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, localization.MessageCypherInvariantsDeleteByPrefixTransactionUnsupported, localizedErr.Message.ID)
	})
}

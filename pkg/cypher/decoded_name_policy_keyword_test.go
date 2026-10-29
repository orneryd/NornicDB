package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePolicyKeywordProperty(t *testing.T) {
	for _, key := range []string{"plain", "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR"} {
		t.Run(key, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			engine.SetDecayEnabled(true)
			executor := NewStorageExecutor(engine)
			ctx := context.Background()
			_, err = executor.Execute(ctx, "CREATE DECAY PROFILE binding FOR (n:Target) APPLY { NO DECAY }", nil)
			require.NoError(t, err)
			query := "CREATE PROMOTION POLICY access FOR (n:Target) APPLY { ON ACCESS { SET n." + key + " = 7 SET n.copyValue = coalesce(n." + key + ", 0) + 1 } }"
			_, err = executor.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			id, err := engine.CreateNode(&storage.Node{ID: "nornic:keyword", Labels: []string{"Target"}})
			require.NoError(t, err)
			accumulator := knowledgepolicy.NewAccessAccumulator(true, 0)
			flusher := knowledgepolicy.NewAccessFlusher(accumulator, engine, time.Hour)
			scorer := knowledgepolicy.NewScorer(knowledgepolicy.NewResolver(engine.GetSchema().GetBindingTable(), nil), true)
			flusher.SetPropertySuppression(func(string) *knowledgepolicy.Scorer { return scorer }, engine, nil)
			accumulator.IncrementAccess(string(id))
			flusher.Flush()
			read, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: string(id)})
			entry := read.Entry
			require.NoError(t, err)
			require.NotNil(t, entry)
			require.EqualValues(t, 7, entry.Overflow[key], "the target accepts the unquoted property name")
			require.EqualValues(t, 8, entry.Overflow["copyValue"], "the expression must read the same property identity")
		})
	}
}

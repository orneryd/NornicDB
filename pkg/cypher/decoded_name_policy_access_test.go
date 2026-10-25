package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Property-suppression bookkeeping must not consume a user-chosen custom
// access-property name. This exercises the persisted result after a real flush.
func TestDecodedNamePolicySuppressionNamespace(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetDecayEnabled(true)
	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	_, err = executor.Execute(ctx, "CREATE DECAY PROFILE binding FOR (n:Target) APPLY { NO DECAY }", nil)
	require.NoError(t, err)
	id, err := engine.CreateNode(&storage.Node{ID: "nornic:namespace", Labels: []string{"Target"}, Properties: map[string]interface{}{"bio": "retained"}})
	require.NoError(t, err)
	require.NoError(t, engine.PutAccessMeta(string(id), &knowledgepolicy.AccessMetaEntry{
		TargetID: string(id), TargetScope: knowledgepolicy.ScopeNode,
		Overflow: map[string]interface{}{"_suppress:bio": 7, "bio": 8},
	}))
	accumulator := knowledgepolicy.NewAccessAccumulator(true, 0)
	flusher := knowledgepolicy.NewAccessFlusher(accumulator, engine, time.Hour)
	scorer := knowledgepolicy.NewScorer(knowledgepolicy.NewResolver(engine.GetSchema().GetBindingTable(), nil), true)
	flusher.SetPropertySuppression(func(string) *knowledgepolicy.Scorer { return scorer }, engine, nil)
	accumulator.IncrementAccess(string(id))
	flusher.Flush()
	read, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: string(id)})
	entry := read.Entry
	require.NoError(t, err)
	require.EqualValues(t, 7, entry.Overflow["_suppress:bio"], "a decoded custom property is independent of suppression state")
	require.EqualValues(t, 8, entry.Overflow["bio"])
}

func TestDecodedNamePolicySuppressionStatePersistence(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	// The scorer below evaluates decay; keep the persistence carrier visible
	// so this subject independently tests the flusher's internal state keys.
	engine.SetDecayEnabled(false)
	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	for _, query := range []string{
		"CREATE DECAY PROFILE decay OPTIONS { halfLifeSeconds:3600, function:'exponential', scope:'NODE', scoreFrom:'CREATED', visibilityThreshold:0.1 }",
		"CREATE DECAY PROFILE binding FOR (n:Target) APPLY { DECAY PROFILE decay }",
	} {
		_, err = executor.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	old := time.Now().Add(-720 * time.Hour)
	id, err := engine.CreateNode(&storage.Node{ID: "nornic:decayed", Labels: []string{"Target"}, Properties: map[string]interface{}{" ": "old", "bio": "old"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	accumulator := knowledgepolicy.NewAccessAccumulator(true, 0)
	flusher := knowledgepolicy.NewAccessFlusher(accumulator, engine, time.Hour)
	scorer := knowledgepolicy.NewScorer(knowledgepolicy.NewResolver(engine.GetSchema().GetBindingTable(), nil), true)
	meta, metaErr := engine.GetEntityMeta(string(id))
	require.NoError(t, metaErr)
	require.Contains(t, meta.PropertyKeys, " ")
	resolution := scorer.ScoreProperty(string(id), meta.Labels, " ", nil, meta.CreatedAtNanos, meta.VersionAtNanos, time.Now().UnixNano())
	require.True(t, resolution.SuppressionEligible, "meta=%+v resolution=%+v", meta, resolution)
	flusher.SetPropertySuppression(func(string) *knowledgepolicy.Scorer { return scorer }, engine, nil)
	accumulator.IncrementAccess(string(id))
	flusher.Flush()
	read, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: string(id)})
	entry := read.Entry
	require.NoError(t, err)
	require.True(t, entry.SuppressedProperties[" "])
	require.True(t, entry.SuppressedProperties["bio"])
	require.NotContains(t, entry.Overflow, "_suppress: ")
	require.NotContains(t, entry.Overflow, "_suppress:bio")
}

func TestDecodedNamePolicyAccessMutations(t *testing.T) {
	for _, key := range []string{"plain", " ", "  ", "\t", "a.b", "a=b", "a`b", "TRUE"} {
		t.Run(key, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			engine.SetDecayEnabled(true)
			executor := NewStorageExecutor(engine)
			ctx := context.Background()
			_, err = executor.Execute(ctx, "CREATE DECAY PROFILE binding FOR (n:Target) APPLY { NO DECAY }", nil)
			require.NoError(t, err)
			copyKey := "copy:" + key
			query := "CREATE PROMOTION POLICY access FOR (n:Target) APPLY { ON ACCESS { SET n." + cyphertext.QuoteName(key) + " = 7 SET n." + cyphertext.QuoteName(copyKey) + " = coalesce(n." + cyphertext.QuoteName(key) + ", 0) + 1 } }"
			_, err = executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			id, err := engine.CreateNode(&storage.Node{ID: "nornic:subject", Labels: []string{"Target"}})
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
			require.EqualValues(t, 7, entry.Overflow[key], "the mutation target is a decoded key")
			require.EqualValues(t, 8, entry.Overflow[copyKey], "the expression reads the same decoded key")
			require.NotContains(t, entry.Overflow, cyphertext.QuoteName(key))
		})
	}
}

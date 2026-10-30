package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestKnowledgePolicyStoredSourceStages(t *testing.T) {
	raw := `  CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { WHEN n.name = '\u03bb' APPLY PROFILE 'p' ON ACCESS { SET n.name = '\u005cu0062' } } `
	for _, stage := range []SourceStage{RawSource, AcceptedSource} {
		t.Run(map[SourceStage]string{RawSource: "raw", AcceptedSource: "retained"}[stage], func(t *testing.T) {
			ctx := t.Context()
			query := raw
			if stage == AcceptedSource {
				var view *cyphertext.Source
				ctx, view = cyphertext.WithSource(ctx, raw)
				require.NoError(t, view.Err)
				query = view.Text
			}
			parsed, owned, err := ParseKnowledgePolicyDDL(ctx, query, stage)
			require.NoError(t, err)
			require.True(t, owned)
			policy := parsed.(*CreatePromotionPolicyCmd).Policy
			require.Equal(t, `n.name = '\u03bb'`, policy.WhenClauses[0].Predicate)
			require.Equal(t, `n.name = '\u005cu0062'`, policy.OnAccess.Mutations[0].Expression)
		})
	}
	accepted := `CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.name = '\u0062' } }`
	parsed, owned, err := ParseKnowledgePolicyDDL(t.Context(), accepted, AcceptedSource)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, `n.name = '\u005cu0062'`, parsed.(*CreatePromotionPolicyCmd).Policy.OnAccess.Mutations[0].Expression)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = ParseKnowledgePolicyDDL(ctx, raw, RawSource)
	require.ErrorIs(t, err, context.Canceled)
}

func TestKnowledgePolicyRawSourceNativeReopenAndEvaluation(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNIC_CYPHER_PARSER", parser)
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				return engine
			}
			engine := open()
			executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "policy_source"))
			for _, query := range []string{
				`CREATE DECAY PROFILE d OPTIONS { halfLifeSeconds: 3600, function: 'none', scope: 'NODE', scoreFrom: 'CREATED' }`,
				`CREATE DECAY PROFILE b FOR (n:Fact) APPLY { DECAY PROFILE 'd' }`,
				`CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.literal = '\u005cu0062' } }`,
			} {
				_, err := executor.Execute(t.Context(), query, nil)
				require.NoError(t, err, query)
			}
			require.Equal(t, `n.literal = '\u005cu0062'`, engine.GetSchemaForNamespace("policy_source").ShowPromotionPolicies()[0].OnAccess.Mutations[0].Expression)
			require.NoError(t, engine.Close())
			engine = open()
			require.Equal(t, `n.literal = '\u005cu0062'`, engine.GetSchemaForNamespace("policy_source").ShowPromotionPolicies()[0].OnAccess.Mutations[0].Expression)
			engine.SetDecayEnabled(true)
			_, err := engine.CreateNode(&storage.Node{ID: "policy_source:one", Labels: []string{"Fact"}, Properties: map[string]interface{}{}})
			require.NoError(t, err)
			accumulator := knowledgepolicy.NewAccessAccumulator(true, 0)
			flusher := knowledgepolicy.NewAccessFlusher(accumulator, engine, time.Hour)
			flusher.SetPropertySuppression(func(namespace string) *knowledgepolicy.Scorer { return engine.ScorerForNamespace(namespace) }, engine, func(string) {})
			accumulator.IncrementAccess("policy_source:one")
			flusher.Flush()
			result, err := engine.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: "policy_source:one"})
			require.NoError(t, err)
			require.NotNil(t, result.Entry)
			require.Equal(t, `\u0062`, result.Entry.Overflow["literal"], "persisted raw policy is projected exactly once by the actual on-access evaluator")
		})
	}
}

func TestKnowledgePolicyQuotedExpressionBoundaries(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNIC_CYPHER_PARSER", parser)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "policy_quotes"))
			_, err := executor.Execute(t.Context(), `CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.0 }`, nil)
			require.NoError(t, err)
			query := `CREATE PROMOTION POLICY quoted FOR (n:Fact) APPLY { WHEN n.name = 'APPLY' APPLY PROFILE 'p' ON ACCESS { SET n.name = 'SET; WITH' SET n.other = coalesce(n.with, 'WITH; SET') } }`
			_, err = executor.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			policies := executor.storage.GetSchema().ShowPromotionPolicies()
			require.Len(t, policies, 1)
			require.Equal(t, `n.name = 'APPLY'`, policies[0].WhenClauses[0].Predicate)
			require.Len(t, policies[0].OnAccess.Mutations, 2)
			require.Equal(t, `n.name = 'SET; WITH'`, policies[0].OnAccess.Mutations[0].Expression)
			require.Equal(t, `n.other = coalesce(n.with, 'WITH; SET')`, policies[0].OnAccess.Mutations[1].Expression)
		})
	}
}

func TestKnowledgePolicyStoredSourceValidity(t *testing.T) {
	source := policyDDLSource{Source: cyphertext.NewSource(t.Context(), "", cyphertext.AcceptedSource), original: true}
	text, err := source.storedText(0, 0)
	require.NoError(t, err)
	require.Empty(t, text)
	_, err = source.storedText(1, 1)
	require.ErrorIs(t, err, storage.ErrInvalidData)
	source = policyDDLSource{Source: &cyphertext.Source{Original: "x", Text: "xx"}, original: true}
	_, err = source.storedText(0, 2)
	require.ErrorIs(t, err, storage.ErrInvalidData)
}

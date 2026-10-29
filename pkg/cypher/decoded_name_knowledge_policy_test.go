package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePolicyAnchor(t *testing.T) {
	for _, key := range []string{"anchor", " ", "  ", "\t", "'anchor'", `"anchor"`, `a\b'"c`} {
		for _, alter := range []bool{false, true} {
			t.Run(key+map[bool]string{false: "/create", true: "/alter"}[alter], func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_policy")
				executor := NewStorageExecutor(store)
				ctx := context.Background()
				statement := "CREATE DECAY PROFILE anchor OPTIONS { halfLifeSeconds: 3600, function:'exponential', scope:'NODE', scoreFrom: 'CUSTOM', scoreFromProperty: " + cypherStringLiteral(key) + " }"
				if alter {
					_, err := executor.Execute(ctx, "CREATE DECAY PROFILE anchor OPTIONS { halfLifeSeconds: 3600, function:'exponential', scope:'NODE', scoreFrom: 'CUSTOM', scoreFromProperty: 'old' }", nil)
					require.NoError(t, err)
					statement = "ALTER DECAY PROFILE anchor SET OPTIONS { scoreFromProperty: " + cypherStringLiteral(key) + " }"
				}
				_, err := executor.Execute(ctx, statement, nil)
				require.NoError(t, err)
				bundles, _ := store.GetSchema().ShowDecayProfiles()
				require.Len(t, bundles, 1)
				require.Equal(t, key, bundles[0].ScoreFromProperty)
				_, err = executor.Execute(ctx, "CREATE DECAY PROFILE binding FOR (n:Anchor) APPLY { DECAY PROFILE anchor }", nil)
				require.NoError(t, err)
				now := int64(100) * int64(time.Hour)
				scorer := knowledgepolicy.NewScorer(knowledgepolicy.NewResolver(store.GetSchema().GetBindingTable(), nil), true)
				score := scorer.ScoreNode("node", []string{"Anchor"}, &knowledgepolicy.AccessMetaEntry{Overflow: map[string]interface{}{key: now - int64(time.Hour)}}, now-10*int64(time.Hour), now-10*int64(time.Hour), now)
				require.InDelta(t, 0.5, score.BaseScore, 0.000001, "the custom anchor must read the exact overflow property")
			})
		}
	}
	t.Run("exact_empty", func(t *testing.T) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_policy"))
		_, err := executor.Execute(context.Background(), "CREATE DECAY PROFILE bad OPTIONS { function:'exponential', scope:'NODE', scoreFrom: 'CUSTOM', scoreFromProperty: '' }", nil)
		require.Error(t, err)
		_, err = executor.Execute(context.Background(), "CREATE DECAY PROFILE ordinary OPTIONS { function:'exponential', scope:'NODE', scoreFrom: 'CREATED', scoreFromProperty: '' }", nil)
		require.NoError(t, err)
	})
}

func TestDecodedNamePolicyMetadataRoundTrip(t *testing.T) {
	for _, labels := range [][]string{{"Plain"}, {"a:b"}, {"A", "B"}, {" "}, {"a`b"}} {
		t.Run(fmt.Sprintf("%q", labels), func(t *testing.T) {
			store := newDecodedPolicyScoringStore(t)
			binding := knowledgepolicy.DecayProfileBinding{Name: "binding", TargetLabels: labels, NoDecay: true, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "a.b` c", NoDecay: true}}}
			require.NoError(t, store.GetSchema().CreateDecayProfileBinding(binding))
			executor := NewStorageExecutor(store)
			result, err := executor.Execute(context.Background(), "CALL kp.profiles()", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, labels, result.Rows[0][11])
			apply := result.Rows[0][18].(string)
			require.Contains(t, apply, "n."+cyphertext.QuoteName("a.b` c")+" NO DECAY")
			target := "(n"
			for _, label := range labels {
				target += ":" + cyphertext.QuoteName(label)
			}
			target += ")"
			_, err = executor.Execute(context.Background(), "ALTER DECAY PROFILE binding FOR "+target+" APPLY { "+apply+" }", nil)
			require.NoError(t, err)
			_, bindings := store.GetSchema().ShowDecayProfiles()
			require.Len(t, bindings, 1)
			require.Equal(t, labels, bindings[0].TargetLabels)
			require.Equal(t, binding.PropertyRules, bindings[0].PropertyRules)
		})
	}
}

func TestDecodedNamePolicyResolveLists(t *testing.T) {
	for _, labels := range [][]string{{"Plain"}, {" "}, {"  "}, {"\t"}, {"A,B"}, {"A", "B"}} {
		t.Run(labels[0], func(t *testing.T) {
			store := newDecodedPolicyScoringStore(t)
			require.NoError(t, store.GetSchema().CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "exact", TargetLabels: labels, NoDecay: true}))
			executor := NewStorageExecutor(store)
			result, err := executor.Execute(context.Background(), "CALL kp.resolve('', $labels, '')", map[string]interface{}{"labels": labels})
			require.NoError(t, err)
			require.Equal(t, []string{"exact"}, result.Rows[0][4])
			result, err = executor.Execute(context.Background(), "CALL kp.resolve('', $labels, '')", map[string]interface{}{"labels": []string{"unrelated"}})
			require.NoError(t, err)
			require.Empty(t, result.Rows[0][4])
		})
	}
	for _, edgeType := range []string{"Plain", " ", "  ", "\t", "A|B"} {
		t.Run("edge/"+edgeType, func(t *testing.T) {
			store := newDecodedPolicyScoringStore(t)
			require.NoError(t, store.GetSchema().CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "edge", IsEdge: true, TargetEdgeType: edgeType, NoDecay: true}))
			executor := NewStorageExecutor(store)
			result, err := executor.Execute(context.Background(), "CALL kp.resolve('', '', $type)", map[string]interface{}{"type": edgeType})
			require.NoError(t, err)
			require.Equal(t, []string{"edge"}, result.Rows[0][4])
		})
	}
}

func TestDecodedNamePolicyTargetReader(t *testing.T) {
	for _, name := range []string{"Plain", "a:b", " ", "\t", "a`b"} {
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_policy")
			executor := NewStorageExecutor(store)
			_, err := executor.Execute(context.Background(), "CREATE DECAY PROFILE exact FOR (n:"+cyphertext.QuoteName(name)+") APPLY { NO DECAY n."+cyphertext.QuoteName(name)+" NO DECAY }", nil)
			require.NoError(t, err)
			_, bindings := store.GetSchema().ShowDecayProfiles()
			require.Len(t, bindings, 1)
			require.Equal(t, []string{name}, bindings[0].TargetLabels)
			require.Len(t, bindings[0].PropertyRules, 1)
			require.Equal(t, name, bindings[0].PropertyRules[0].PropertyPath)
		})
	}
}

func newDecodedPolicyScoringStore(t *testing.T) storage.Engine {
	t.Helper()
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetDecayEnabled(true)
	return engine
}

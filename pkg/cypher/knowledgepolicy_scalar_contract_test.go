package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// One fixture/evaluation/assertion owner covers the scalar result contracts.
// Each case owns its engine and preserves the old thresholds and error checks.
func TestKnowledgePolicyScalarContracts(t *testing.T) {
	for _, tc := range []struct {
		name, expression, id, label                  string
		age                                          time.Duration
		disabled, tracked, unhandled, reasonRequired bool
		want                                         interface{}
		scalarBounds, mapBounds                      [2]float64
		absent                                       []string
	}{
		{name: "decayed score", expression: "decayScore(n)", id: "old_episode", label: "MemoryEpisode", age: 720 * time.Hour, scalarBounds: [2]float64{0, .10}},
		{name: "recent score", expression: "decayScore(n)", id: "recent_episode", label: "MemoryEpisode", age: 5 * time.Minute, scalarBounds: [2]float64{.90, 2}},
		{name: "no-decay fact", expression: "decayScore(n)", id: "old_fact", label: "KnowledgeFact", age: 8760 * time.Hour, want: 1.0},
		{name: "unmatched label", expression: "decayScore(n)", id: "custom", label: "CustomLabel", age: 720 * time.Hour, want: 1.0},
		{name: "disabled score", expression: "decayScore(n)", id: "old_episode", label: "MemoryEpisode", age: 720 * time.Hour, disabled: true, want: 1.0},
		{name: "decay map and resolved function", expression: "decay(n)", id: "ep", label: "MemoryEpisode", age: 2 * time.Hour, want: map[string]interface{}{"applies": true, "function": string(knowledgepolicy.DecayFunctionExponential), "floor": 0.0}, mapBounds: [2]float64{0, 1}},
		{name: "no-decay reason", expression: "decay(n)", id: "fact", label: "KnowledgeFact", age: 8760 * time.Hour, want: map[string]interface{}{"reason": "no decay", "score": 1.0}},
		{name: "disabled decay", expression: "decay(n)", id: "ep", label: "MemoryEpisode", age: 720 * time.Hour, disabled: true, want: map[string]interface{}{"applies": false}, reasonRequired: true},
		{name: "policy with metadata", expression: "policy(n)", id: "tracked", label: "MemoryEpisode", age: 2 * time.Hour, tracked: true, want: map[string]interface{}{"accessCount": int64(42), "targetId": "nornic:tracked"}},
		{name: "policy without metadata", expression: "policy(n)", id: "untracked", label: "MemoryEpisode", age: 2 * time.Hour, want: map[string]interface{}{"targetId": "nornic:untracked", "targetScope": "NODE"}, absent: []string{"accessCount"}},
		{name: "disabled policy", expression: "policy(n)", id: "ep", label: "MemoryEpisode", age: 2 * time.Hour, disabled: true, want: map[string]interface{}{"targetId": "nornic:ep"}},
		{name: "unknown option", expression: "decayScore(n, {badKey:'x'})", id: "ep", label: "MemoryEpisode", age: 2 * time.Hour},
		{name: "reveal belongs to registry", expression: "reveal(n)", id: "ep_reveal", label: "MemoryEpisode", age: 720 * time.Hour, unhandled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, exec := setupDecayEngine(t, !tc.disabled)
			id := "nornic:" + tc.id
			if tc.tracked {
				require.NoError(t, engine.PutAccessMeta(id, &knowledgepolicy.AccessMetaEntry{TargetID: id, TargetScope: knowledgepolicy.ScopeNode, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 42, LastAccessedAt: time.Now().Add(-time.Hour).UnixNano()}}))
			}
			node := makeNode(id, []string{tc.label}, tc.age)
			value, handled := exec.evaluateKnowledgePolicyFunction(context.Background(), originalBraceView(tc.expression), strings.ToLower(tc.expression), map[string]*storage.Node{"n": node}, nil)
			require.Equal(t, !tc.unhandled, handled)
			if tc.unhandled {
				return
			}
			if tc.scalarBounds != [2]float64{} {
				score, ok := value.(float64)
				require.True(t, ok, "expected float64, got %T", value)
				require.GreaterOrEqual(t, score, tc.scalarBounds[0])
				require.Less(t, score, tc.scalarBounds[1])
				return
			}
			expected, isMap := tc.want.(map[string]interface{})
			if !isMap {
				require.Equal(t, tc.want, value)
				return
			}
			result, ok := value.(map[string]interface{})
			require.True(t, ok, "expected map, got %T", value)
			for key, want := range expected {
				require.Contains(t, result, key)
				require.Equal(t, want, result[key], key)
			}
			for _, key := range tc.absent {
				require.NotContains(t, result, key)
			}
			if tc.reasonRequired {
				require.IsType(t, "", result["reason"])
				require.NotEmpty(t, result["reason"])
			}
			if tc.mapBounds != [2]float64{} {
				score, ok := result["score"].(float64)
				require.True(t, ok)
				require.Greater(t, score, tc.mapBounds[0])
				require.Less(t, score, tc.mapBounds[1])
			}
		})
	}
}

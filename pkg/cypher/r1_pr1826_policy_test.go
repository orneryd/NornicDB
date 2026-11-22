package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// r1PolicyFixture is a database whose Fact nodes are three days old under a
// one-hour half-life. A promotion policy keeps a node visible while its WHEN
// predicate holds, and the predicate reads a stored property of the node.
func r1PolicyFixture(t testing.TB, indexed bool, predicate string, nodes []*storage.Node) (*StorageExecutor, *storage.BadgerEngine) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	base.SetDecayEnabled(true)
	schema := base.GetSchemaForNamespace("policy")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "age", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "facts", TargetLabels: []string{"Fact"}, ProfileRef: "age",
	}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name: "keep", Scope: knowledgepolicy.ScopeNode, Multiplier: 10, ScoreFloor: 0.5, ScoreCap: 1, Enabled: true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "promote", TargetLabels: []string{"Fact"}, Enabled: true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: predicate, ProfileRef: "keep"}},
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "policy"), QueryCachePolicy{MaxEntries: 0})
	if indexed {
		_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT r1_fact IF NOT EXISTS FOR (n:Fact) REQUIRE n.display IS UNIQUE", nil)
		require.NoError(t, err)
	}
	old := time.Now().Add(-72 * time.Hour)
	for _, node := range nodes {
		node.CreatedAt, node.UpdatedAt = old, old
		_, err := base.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	return exec, base
}

func r1PolicyCheck(t *testing.T, exec *StorageExecutor, query string, want []string) {
	t.Helper()
	ctx := t.Context()
	for _, stage := range []string{"cold", "repeated", "explicit transaction"} {
		if stage == "explicit transaction" {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		got := r1Rows(exec.Execute(ctx, query, nil))
		if stage == "explicit transaction" {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("%s: %s: want %q, got %q", stage, query, want, got)
		}
	}
}

// A node a promotion policy keeps visible must stay visible to a statement that
// reads only some of its properties.
func TestR1PR1826PromotedNodeStaysVisibleToPropertyReads(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		for _, test := range []struct {
			query string
			want  []string
		}{
			{"MATCH (n:Fact {display:'a'}) RETURN n.display, n.promote", []string{"'a' | true"}},
			{"MATCH (n:Fact {display:'a'}) RETURN n.display", []string{"'a'"}},
			{"MATCH (n:Fact) WHERE n.display = 'a' RETURN n.display", []string{"'a'"}},
			{"MATCH (n:Fact) WHERE n.display IN ['a','expired'] RETURN n.display", []string{"'a'"}},
			{"UNWIND ['a','b','expired'] AS d MATCH (n:Fact {display:d}) RETURN n.display", []string{"'a'", "'b'"}},
			{"MATCH (n:Fact {display:'a'}) RETURN count(n) AS c", []string{"1"}},
			{"MATCH (n:Fact {display:'a'}) RETURN count(n.display) AS c", []string{"1"}},
			{"MATCH (n:Fact) RETURN n.display", []string{"'a'", "'b'"}},
			{"MATCH (n:Fact) RETURN count(n.display) AS c", []string{"2"}},
		} {
			t.Run(fmt.Sprintf("indexed=%t/%s", indexed, test.query), func(t *testing.T) {
				exec, base := r1PolicyFixture(t, indexed, "n.promote = true", []*storage.Node{
					{ID: "policy:a", Labels: []string{"Fact"}, Properties: map[string]any{"promote": true, "display": "a"}},
					{ID: "policy:b", Labels: []string{"Fact"}, Properties: map[string]any{"promote": true, "display": "b"}},
					{ID: "policy:expired", Labels: []string{"Fact"}, Properties: map[string]any{"promote": false, "display": "expired"}},
				})
				_, err := base.GetNode(t.Context(), "policy:a")
				require.NoError(t, err, "control: the whole-node read sees the promoted node")
				_, err = base.GetNode(t.Context(), "policy:expired")
				require.ErrorIs(t, err, storage.ErrNotFound, "control: the whole-node read hides the expired node")
				r1PolicyCheck(t, exec, test.query, test.want)
			})
		}
	}
}

// A node the policy hides must not be shown by a statement that reads only some
// of its properties. The promotion predicate promotes nodes without a hidden
// flag, so a projection that omits the flag promotes the node it should hide.
func TestR1PR1826SuppressedNodeStaysHiddenFromPropertyReads(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		for _, test := range []struct {
			query string
			want  []string
		}{
			{"MATCH (n:Fact {display:'secret'}) RETURN n", []string{}},
			{"MATCH (n:Fact {display:'secret'}) RETURN n.display, n.hidden", []string{}},
			{"MATCH (n:Fact {display:'secret'}) RETURN n.display, n.body", []string{}},
			{"MATCH (n:Fact) WHERE n.display = 'secret' RETURN n.body", []string{}},
			{"UNWIND ['secret','open'] AS d MATCH (n:Fact {display:d}) RETURN n.display", []string{"'open'"}},
			{"MATCH (n:Fact {display:'secret'}) RETURN count(n) AS c", []string{"0"}},
			{"MATCH (n:Fact) RETURN n.display, n.body", []string{"'open' | 'open body'"}},
		} {
			t.Run(fmt.Sprintf("indexed=%t/%s", indexed, test.query), func(t *testing.T) {
				exec, base := r1PolicyFixture(t, indexed, "coalesce(n.hidden, 0) = 0", []*storage.Node{
					{ID: "policy:open", Labels: []string{"Fact"}, Properties: map[string]any{"display": "open", "body": "open body"}},
					{ID: "policy:secret", Labels: []string{"Fact"}, Properties: map[string]any{"hidden": int64(1), "display": "secret", "body": "secret body"}},
				})
				_, err := base.GetNode(t.Context(), "policy:open")
				require.NoError(t, err, "control: the whole-node read sees the open node")
				_, err = base.GetNode(t.Context(), "policy:secret")
				require.ErrorIs(t, err, storage.ErrNotFound, "control: the whole-node read hides the hidden node")
				r1PolicyCheck(t, exec, test.query, test.want)
			})
		}
	}
}

// A function replaced while the server runs must get the arguments its new
// registration asks for, as the base does: "A replacement completed before
// statement planning supplies its own demand".
func TestR1PR1826ProofFollowsReplacedFunction(t *testing.T) {
	const name = "r1.probe"
	labelsOnly := func(_ context.Context, call cypherfn.Context, args []string) (interface{}, error) {
		value, err := call.Eval(args[0])
		if err != nil {
			return nil, err
		}
		node, _ := value.(*storage.Node)
		return int64(len(node.Labels)), nil
	}
	properties := func(_ context.Context, call cypherfn.Context, args []string) (interface{}, error) {
		value, err := call.Eval(args[0])
		if err != nil {
			return nil, err
		}
		node, _ := value.(*storage.Node)
		return int64(len(node.Properties)), nil
	}
	store := r1Fixture(t, true)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	query := "MATCH (n:Doc {key:'one'}) RETURN r1.probe(n) AS c"
	cypherfn.Register(name, labelsOnly, cypherfn.CanonicalReads, cypherfn.HeaderInputImage)
	require.Equal(t, []string{"1"}, r1Rows(exec.Execute(t.Context(), query, nil)))
	cypherfn.Register(name, properties, cypherfn.CanonicalReads, cypherfn.MetadataArgumentInputImage)
	require.Equal(t, []string{"6"}, r1Rows(exec.Execute(t.Context(), query, nil)), "the replacement reads the node's six properties")
}

// n.has_embedding of an indexed read must be the value a whole-node read gives,
// whichever image of the node the caches hold.
func TestR1PR1826HasEmbeddingOnIndexedRead(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			ctx := t.Context()
			base := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(base, "r1")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			if indexed {
				_, err := exec.Execute(ctx, "CREATE CONSTRAINT r1_vec IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE", nil)
				require.NoError(t, err)
			}
			_, err := base.CreateNode(ctx, &storage.Node{ID: "r1:vec", Labels: []string{"Doc"}, Properties: map[string]any{"key": "one"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
			require.NoError(t, err)
			control := NewStorageExecutor(r1FullReads{store, "r1"}, QueryCachePolicy{MaxEntries: 0})
			query := "MATCH (n:Doc {key:'one'}) RETURN n.has_embedding AS e"
			for _, stage := range []string{"after the write", "repeated", "after a whole-node read"} {
				if stage == "after a whole-node read" {
					_, err := base.GetNode(ctx, "r1:vec")
					require.NoError(t, err)
				}
				got := r1Rows(exec.Execute(ctx, query, nil))
				want := r1Rows(control.Execute(ctx, query, nil))
				if !reflect.DeepEqual(want, got) {
					t.Errorf("%s: whole-node answer %q, answer %q", stage, want, got)
				}
			}
		})
	}
}

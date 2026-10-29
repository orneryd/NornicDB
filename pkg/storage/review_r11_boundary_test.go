package storage

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
)

// A boundary-negative control for supported nested payloads. The real compiler
// currently supplies strings only; this separately exercises the codec contract.
func TestReviewR11BoundaryPayload(t *testing.T) {
	leaf := localization.NewLocalizedError("Leaf", localization.Message{ID: "review-leaf", Fallback: "leaf"}, nil)
	original := localization.NewLocalizedError("Outer", localization.Message{ID: "review-outer", Fallback: "outer", Data: map[string]any{
		"list": []any{"original", int64(1)}, "nested": map[string]any{"names": []string{"a", "b"}},
		"wrapped": fmt.Errorf("wrapped: %w", leaf),
	}, PluralCount: []any{true}}, fmt.Errorf("cause: %w", leaf))
	report := reportedContractError(original)
	want := renderPublic(report)
	var first, second *localization.LocalizedError
	require.True(t, errors.As(report, &first))
	require.True(t, errors.As(report, &second))
	require.NotSame(t, first, second)
	first.Message.Data["list"].([]any)[0] = "caller changed scalar list"
	mutatePublic(first)
	require.Equal(t, want, renderPublic(report), "a public accessor leaked decoded payload state")
}

// Every compiled-handle API is reachable with an invalid installed revision.
// A zero handle cannot stand in for this path: it has a different error.
func TestReviewR11InstalledHandlesAndWrappers(t *testing.T) {
	for _, scenario := range contractErrorScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			e := newDetachedEngine(t)
			contract := scenarioContract(scenario.expression, scenario.relationship)
			schema := e.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddConstraintContractBundle(contract, nil, nil, false))
			handles := schema.CompiledConstraintContracts()
			require.Len(t, handles, 1)
			scan := newDetachedEngine(t)
			require.NoError(t, commitScenarioEntity(scan, "existing", scenario.relationship))
			call := func() error {
				return ValidateCompiledConstraintContractOnCreationForEngine(NewNamespacedEngine(scan, "test"), handles[0])
			}
			first := call()
			require.Error(t, first)
			var boundary contractError
			require.True(t, errors.As(first, &boundary))
			sentinel := errors.New("review sentinel")
			wrapped := errors.Join(fmt.Errorf("wrap: %w", first), sentinel)
			require.ErrorIs(t, wrapped, sentinel)
			var got *localization.LocalizedError
			require.True(t, errors.As(wrapped, &got))
			want := renderPublic(first)
			mutatePublic(got)
			mutatePublic(first)
			require.Equal(t, want, renderPublic(first))
			require.Equal(t, want, renderPublic(call()))
			other := newDetachedEngine(t)
			require.NoError(t, other.GetSchemaForNamespace("test").AddCompiledConstraintContractBundle(handles[0], nil, nil, false))
			require.Equal(t, renderPublic(commitScenarioEntity(e, "one", scenario.relationship)), renderPublic(commitScenarioEntity(other, "two", scenario.relationship)))
		})
	}
}

type r11ReadFailure struct{ marker string }

func (e *r11ReadFailure) Error() string { return e.marker }

type r11FailView struct{ failure error }

func (v r11FailView) OutgoingEdges(NodeID) ([]*Edge, error) { return nil, v.failure }
func (v r11FailView) IncomingEdges(NodeID) ([]*Edge, error) { return nil, v.failure }
func (v r11FailView) Node(NodeID) (*Node, error)            { return nil, v.failure }
func TestReviewR11RuntimeIdentity(t *testing.T) {
	failure := &r11ReadFailure{"storage read sentinel"}
	c := scenarioContract("COUNT { (n)-[:R]->() } > 0", false)
	revision := compileConstraintContractRevision(c)
	err := revision.validate(r11FailView{failure}, ConstraintContractKindBooleanNode, &Node{ID: "test:n"}, nil)
	require.ErrorIs(t, err, failure)
	var got *r11ReadFailure
	require.True(t, errors.As(err, &got))
	require.Same(t, failure, got)
	leaf := publicLocalized(err)
	require.NotNil(t, leaf)
	leaf.Message.Data["Predicate"] = "caller changed predicate"
	require.Equal(t, c.Entries[0].Expression, revision.description().Entries[0].Expression)
	next := revision.validate(r11FailView{failure}, ConstraintContractKindBooleanNode, &Node{ID: "test:n"}, nil)
	require.Equal(t, c.Entries[0].Expression, publicLocalized(next).Message.Data["Predicate"])
}

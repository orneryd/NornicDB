package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQualifyTemporalNodeID_PreservesAndPrefixes(t *testing.T) {
	require.Equal(t, NodeID(""), qualifyTemporalNodeID("ns", ""))
	require.Equal(t, NodeID("foo"), qualifyTemporalNodeID("", "foo"))
	require.Equal(t, NodeID("ns:already"), qualifyTemporalNodeID("ns", "ns:already"))
	require.Equal(t, NodeID("ns:foo"), qualifyTemporalNodeID("ns", "foo"))
	// Different namespace prefix on the input → engine prepends its
	// own; the wrapper does NOT strip the existing prefix because
	// the contract is "qualify with this namespace", not "rewrite".
	require.Equal(t, NodeID("ns:other:foo"), qualifyTemporalNodeID("ns", "other:foo"))
}

func TestCurrentTemporalNodeByScanInNamespace_FindsLatest(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{
		Name:       "fact_temporal",
		Type:       ConstraintTemporal,
		Label:      "FactScan",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("scan").AddConstraint(constraint))

	v1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	v3 := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)

	_, err := engine.CreateNode(&Node{
		ID: "scan:v1", Labels: []string{"FactScan"},
		Properties: map[string]interface{}{
			"fact_key":   "k",
			"valid_from": v1,
			"valid_to":   v2,
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{
		ID: "scan:v2", Labels: []string{"FactScan"},
		Properties: map[string]interface{}{
			"fact_key":   "k",
			"valid_from": v2,
			"valid_to":   v3,
		},
	})
	require.NoError(t, err)

	// Wrong-namespace candidate must be ignored.
	_, err = engine.CreateNode(&Node{
		ID: "other:v3", Labels: []string{"FactScan"},
		Properties: map[string]interface{}{
			"fact_key":   "k",
			"valid_from": v1,
			"valid_to":   v3,
		},
	})
	require.NoError(t, err)

	got, err := engine.currentTemporalNodeByScanInNamespace("scan", constraint, "k", v2.Add(time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, NodeID("scan:v2"), got.ID, "highest valid_from <= asOf wins")

	// asOf before v1 → no match.
	got, err = engine.currentTemporalNodeByScanInNamespace("scan", constraint, "k",
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Nil(t, got)

	// Different key → no match.
	got, err = engine.currentTemporalNodeByScanInNamespace("scan", constraint, "different_key", v2)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestIsCurrentTemporalNodeInNamespace_NoConstraintsTrue(t *testing.T) {
	engine := createTestBadgerEngine(t)
	// No temporal constraints registered → the function returns true
	// because there's nothing to gate on.
	got, err := engine.IsCurrentTemporalNodeInNamespace("test", &Node{
		ID: "test:n", Labels: []string{"L"},
	}, time.Now())
	require.NoError(t, err)
	require.True(t, got)
}

func TestIsCurrentTemporalNodeInNamespace_NilNode(t *testing.T) {
	engine := createTestBadgerEngine(t)
	got, err := engine.IsCurrentTemporalNodeInNamespace("test", nil, time.Now())
	require.NoError(t, err)
	require.False(t, got)
}

func TestIsCurrentTemporalNodeInNamespace_TrueForLiveNode(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{
		Name:       "live_fact",
		Type:       ConstraintTemporal,
		Label:      "LiveFact",
		Properties: []string{"k", "valid_from", "valid_to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("live").AddConstraint(constraint))

	v1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&Node{
		ID: "live:fact-1", Labels: []string{"LiveFact"},
		Properties: map[string]interface{}{
			"k":          "k1",
			"valid_from": v1,
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	node, err := engine.GetNode("live:fact-1")
	require.NoError(t, err)
	got, err := engine.IsCurrentTemporalNodeInNamespace("live", node, v1.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, got)
}

func TestIsCurrentTemporalNodeInNamespace_FalseForSupersededVersion(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{
		Name:       "live_fact",
		Type:       ConstraintTemporal,
		Label:      "LiveFact2",
		Properties: []string{"k", "valid_from", "valid_to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("live2").AddConstraint(constraint))

	v1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	_, err := engine.CreateNode(&Node{
		ID: "live2:fact-1", Labels: []string{"LiveFact2"},
		Properties: map[string]interface{}{
			"k":          "k1",
			"valid_from": v1,
			"valid_to":   v2,
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{
		ID: "live2:fact-2", Labels: []string{"LiveFact2"},
		Properties: map[string]interface{}{
			"k":          "k1",
			"valid_from": v2,
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	old, err := engine.GetNode("live2:fact-1")
	require.NoError(t, err)
	got, err := engine.IsCurrentTemporalNodeInNamespace("live2", old, v2.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, got, "asking about an old version after the cutover must be false")
}

func TestIsCurrentTemporalNode_WithoutNamespacePrefix_TrueByDefault(t *testing.T) {
	engine := createTestBadgerEngine(t)
	got, err := engine.IsCurrentTemporalNode(&Node{ID: "no-prefix-here"}, time.Now())
	require.NoError(t, err)
	require.True(t, got, "a node without a namespace prefix can't be temporally evaluated")
}

func TestIsCurrentTemporalNode_NilNodeFalse(t *testing.T) {
	engine := createTestBadgerEngine(t)
	got, err := engine.IsCurrentTemporalNode(nil, time.Now())
	require.NoError(t, err)
	require.False(t, got)
}

package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestV152RCCSmallestLabelPostingCause(t *testing.T) {
	ctx := context.Background()
	exec := ei4R5Exec(t,
		"CREATE INDEX rcc_a FOR (n:RCCA) ON (n.k)",
		"CREATE INDEX rcc_b FOR (n:RCCB) ON (n.k)",
		"CREATE (:RCCA:RCCB {id:'a',k:'hit'})",
	)
	postings := indexSeekPostings{labels: []string{"RCCA", "RCCB"}, property: "k", value: storage.IndexLookupValue("hit")}
	control, err := smallestLabelPosting(ctx, exec.storage, exec.storage.GetSchema(), []string{"RCCA", "RCCB"}, &postings)
	require.NoError(t, err)
	t.Logf("RCC_INDEX_CONTROL owner=pkg/cypher/match_index_seek.go:521-547 selected=%v", control)
	require.Equal(t, []string{"RCCA"}, control, "equal complete postings keep the first label")

	ids, err := exec.storage.GetSchema().PropertyIndexLookup(ctx, "RCCB", "k", "hit")
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.NoError(t, exec.storage.GetSchema().PropertyIndexDelete("RCCB", "k", ids[0], "hit"))
	selected, err := smallestLabelPosting(ctx, exec.storage, exec.storage.GetSchema(), []string{"RCCA", "RCCB"}, &postings)
	require.NoError(t, err)
	rows := ei4R5Rows(t, exec, "MATCH (n:RCCA:RCCB) WHERE n.k='hit' RETURN n.id", nil)
	t.Logf("RCC_INDEX_TRIGGER owner=pkg/cypher/match_index_seek.go:521-547 selected=%v rows=%v healthy_label=RCCA missing_label=RCCB", selected, rows)
	assert.NotEqual(t, []string{"RCCB"}, selected, "a zero-length missing posting must not be treated as the complete candidate population")
	require.Empty(t, rows, "the selected missing posting explains the public empty result")
}

func TestV152RCCMergeIndexedCandidateCause(t *testing.T) {
	ctx := context.Background()
	exec := ei4R5Exec(t,
		"CREATE INDEX rcc_merge_first FOR (n:RCCMergeFirst) ON (n.id)",
		"CREATE INDEX rcc_merge_late FOR (n:RCCMergeLate) ON (n.id)",
		"CREATE (:RCCMergeFirst:RCCMergeLate {id:'target'})",
	)
	props := map[string]any{"id": "target"}
	controlIDs, controlComplete, err := exec.mergeNodeIndexedCandidateIDs(ctx, exec.storage, []string{"RCCMergeFirst", "RCCMergeLate"}, props)
	require.NoError(t, err)
	t.Logf("RCC_MERGE_CONTROL owner=pkg/cypher/merge.go:480-541 ids=%v complete=%v", controlIDs, controlComplete)
	require.True(t, controlComplete)
	require.Len(t, controlIDs, 1)

	schema := exec.storage.GetSchema()
	ids, err := schema.PropertyIndexLookup(ctx, "RCCMergeFirst", "id", "target")
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.NoError(t, schema.PropertyIndexDelete("RCCMergeFirst", "id", ids[0], "target"))
	triggerIDs, triggerComplete, err := exec.mergeNodeIndexedCandidateIDs(ctx, exec.storage, []string{"RCCMergeFirst", "RCCMergeLate"}, props)
	require.NoError(t, err)
	t.Logf("RCC_MERGE_TRIGGER owner=pkg/cypher/merge.go:480-541 ids=%v complete=%v first_label_missing=true late_label_intact=true", triggerIDs, triggerComplete)
	assert.False(t, triggerComplete, "an empty first-label posting is not a complete answer while another label's posting contains the node")
	assert.NotEmpty(t, triggerIDs, "the intact later-label posting still names the candidate")
}

func TestV152RCCNaNListComparabilityCause(t *testing.T) {
	t.Run("finite_control", func(t *testing.T) {
		comparison, comparable := compareCypherOrderedValues(float64(1), float64(2))
		t.Logf("RCC_NAN_CONTROL owner=pkg/cypher/row_expression.go:624-637 comparison=%d comparable=%v", comparison, comparable)
		require.Equal(t, -1, comparison)
		require.True(t, comparable)
	})
	t.Run("nan_trigger", func(t *testing.T) {
		comparison, comparable := compareCypherOrderedValues(math.NaN(), float64(1))
		listResult := compareCypherPredicateValue([]any{math.NaN()}, []any{float64(1)}, "<=")
		t.Logf("RCC_NAN_TRIGGER owner=pkg/cypher/row_expression.go:624-637 comparison=%d comparable=%v list_le=%#v", comparison, comparable, listResult)
		assert.False(t, comparable, "NaN has no predicate order; the numeric fall-through must not report comparable")
		assert.Nil(t, listResult, "the decisive NaN element must make list ordering null")
	})
}

func TestV152RCCTopLevelReturningCallAdmissionCause(t *testing.T) {
	ctx := context.Background()
	trigger := "CALL { RETURN 1 AS a }"
	body := readQueryBody(ctx, trigger, 0, len(trigger))
	require.Len(t, body.branches, 1)
	decisive := unconcludedClause(ctx, trigger, body.branches[0])
	err := validateQueryBodyComposition(ctx, trigger)
	t.Logf("RCC_CALL_TRIGGER owner=pkg/cypher/match_semantic_validation.go:147-169 unconcluded=%q validation_error=%v", decisive, err)
	require.Equal(t, "CALL", decisive, "the existing ending classifier identifies the forbidden top-level clause")
	assert.Error(t, err, "top-level composition must apply the same conclusion rule before execution")

	contained := "CALL { CALL { RETURN 1 AS a } } RETURN 1 AS ok"
	containedErr := validateQueryBodyComposition(ctx, contained)
	t.Logf("RCC_CALL_CONTROL owner=pkg/cypher/match_semantic_validation.go:147-169 contained_error=%v", containedErr)
	require.ErrorContains(t, containedErr, "Query cannot conclude with CALL", "contained body control takes the correct branch")
	require.NoError(t, validateQueryBodyComposition(ctx, "CALL { CREATE () }"), "unit-call control remains valid")
}

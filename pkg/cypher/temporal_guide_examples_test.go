package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/temporalguide"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestTemporalGuideOwnershipExamples(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "guide"))
		temporalguide.Ownership(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideGenericLedgerExamples(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		temporalguide.GenericLedger(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideConstraintBlockExamples(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		temporalguide.Contracts(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideHistoryExample(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newHistoricalQueryStore(t), "guide"))
		temporalguide.History(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideEpochBounds(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		temporalguide.EpochBounds(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideDownloadableScript(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		temporalguide.OwnershipScript(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideCostExamples(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		temporalguide.CostExamples(t, func(query string, params map[string]interface{}) ([][]interface{}, error) {
			result, err := exec.Execute(t.Context(), query, params)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		})
	})
}

func TestTemporalGuideStandaloneDeclarations(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, label  string
			relationship bool
			props        []string
		}{
			{"epoch-single", "FactVersion", false, []string{"fact_key", "valid_from", "valid_to"}},
			{"epoch-composite-node", "FactVersion", false, []string{"tenant", "fact_key", "valid_from", "valid_to"}},
			{"epoch-composite-relationship", "WORKS_AT", true, []string{"from_id", "to_id", "valid_from", "valid_to"}},
			{"epoch-deprecated", "FactVersion", false, []string{"fact_key", "valid_from", "valid_to"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, engine := newConstraintGapExecutor(t)
				_, err := exec.Execute(t.Context(), temporalguide.Example(t, "managing-constraints", tc.name), nil)
				require.NoError(t, err)
				constraints := engine.GetSchema().GetAllConstraints()
				require.Len(t, constraints, 1)
				c := constraints[0]
				require.Equal(t, storage.ConstraintTemporal, c.Type)
				require.Equal(t, tc.label, c.Label)
				require.Equal(t, tc.props, c.Properties)
				want := storage.ConstraintEntityNode
				if tc.relationship {
					want = storage.ConstraintEntityRelationship
				}
				require.Equal(t, want, c.EffectiveEntityType())
			})
		}
	})
}

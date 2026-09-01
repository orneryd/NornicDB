package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

type schemaLocalizationScanErrorEngine struct{ Engine }

func (e *schemaLocalizationScanErrorEngine) GetNodesByLabel(string) ([]*Node, error) {
	return nil, ErrConflict
}

type schemaLocalizationPredicateErrorEngine struct{ Engine }

func (e *schemaLocalizationPredicateErrorEngine) GetNodesByLabel(string) ([]*Node, error) {
	return []*Node{{ID: "test:n1", Labels: []string{"Person"}}}, nil
}

func (e *schemaLocalizationPredicateErrorEngine) GetOutgoingEdges(NodeID) ([]*Edge, error) {
	return nil, ErrConflict
}

func requireStorageSchemaLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()
	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestStorageSchemaLocalizedErrorsHaveTypedIdentity(t *testing.T) {
	t.Run("constraint conflict", func(t *testing.T) {
		schema := NewSchemaManager()
		require.NoError(t, schema.AddConstraint(Constraint{Name: "person_email", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}}))

		err := schema.AddConstraint(Constraint{Name: "person_email", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}})
		localizedErr := requireStorageSchemaLocalizedError(t, err, localization.MessageStorageSchemaConstraintAlreadyExists, `constraint "person_email" already exists`)
		require.Equal(t, "person_email", localizedErr.Message.Data["Name"])
	})

	t.Run("knowledge policy validation", func(t *testing.T) {
		schema := NewSchemaManager()
		bundle := knowledgepolicy.DecayProfileBundle{
			Name:                "invalid_decay",
			HalfLifeSeconds:     604800,
			VisibilityThreshold: 0.1,
			Function:            knowledgepolicy.DecayFunction("unsupported"),
			Scope:               knowledgepolicy.ScopeNode,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		}

		err := schema.CreateDecayProfileBundle(bundle)
		localizedErr := requireStorageSchemaLocalizedError(t, err, localization.MessageStorageSchemaInvalidDecayFunction, `invalid decay function: "unsupported"`)
		require.Equal(t, knowledgepolicy.DecayFunction("unsupported"), localizedErr.Message.Data["Function"])
	})

	t.Run("constraint contract violation", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"team": "ops"}})
		require.NoError(t, err)
		contract := ConstraintContract{
			Name:              "core_team",
			TargetEntityType:  string(ConstraintEntityNode),
			TargetLabelOrType: "Person",
			Entries: []ConstraintContractEntry{{
				Kind:       ConstraintContractKindBooleanNode,
				Expression: "n.team IN ['core']",
			}},
		}

		err = ValidateConstraintContractOnCreationForEngine(engine, contract)
		localizedErr := requireStorageSchemaLocalizedError(t, err, localization.MessageStorageSchemaConstraintContractViolated, `constraint contract core_team violated: predicate "n.team IN ['core']" evaluated to false`)
		require.Equal(t, "core_team", localizedErr.Message.Data["Name"])
		require.Equal(t, "n.team IN ['core']", localizedErr.Message.Data["Predicate"])
	})
}

func TestStorageSchemaLocalizedContractWrapperPreservesSentinel(t *testing.T) {
	t.Run("scan wrapper", func(t *testing.T) {
		contract := ConstraintContract{
			Name:              "scan_contract",
			TargetEntityType:  string(ConstraintEntityNode),
			TargetLabelOrType: "Person",
		}
		err := ValidateConstraintContractOnCreationForEngine(&schemaLocalizationScanErrorEngine{Engine: NewMemoryEngine()}, contract)
		requireStorageSchemaLocalizedError(t, err, localization.MessageStorageSchemaScanNodesFailed, "scanning nodes: conflict")
		require.ErrorIs(t, err, ErrConflict)
	})

	t.Run("predicate wrapper", func(t *testing.T) {
		contract := ConstraintContract{
			Name:              "edge_count",
			TargetEntityType:  string(ConstraintEntityNode),
			TargetLabelOrType: "Person",
			Entries: []ConstraintContractEntry{{
				Kind:       ConstraintContractKindBooleanNode,
				Expression: "COUNT { (n)-[:KNOWS]->() } > 0",
			}},
		}
		err := ValidateConstraintContractOnCreationForEngine(&schemaLocalizationPredicateErrorEngine{Engine: NewMemoryEngine()}, contract)
		requireStorageSchemaLocalizedError(t, err, localization.MessageStorageSchemaConstraintContractInvalid, `constraint contract edge_count invalid: predicate "COUNT { (n)-[:KNOWS]->() } > 0": conflict`)
		require.ErrorIs(t, err, ErrConflict)
	})
}

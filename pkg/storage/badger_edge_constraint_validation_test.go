package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// edgeConstraintFixture spins up a fresh BadgerEngine on a tempdir,
// installs the requested constraint into the "test" namespace, and
// pre-creates two endpoint nodes ("test:a" / "test:b") plus an extra
// pair ("test:c"/"test:d") for cardinality scenarios. Returns the
// engine; t.Cleanup handles disk teardown.
//
// Using a real on-disk engine (not MemoryEngine) is required because
// the in-txn constraint validators iterate Badger key prefixes
// (edgeTypeIndex, outgoingIndex, incomingIndex) that the in-memory
// path does not exercise the same way.
func edgeConstraintFixture(t *testing.T, c Constraint) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.NoError(t,
		engine.GetSchemaForNamespace("test").AddConstraint(c),
	)

	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
		_, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Endpoint"},
			Properties: map[string]any{},
		})
		require.NoError(t, err, "CreateNode(%q)", id)
	}
	return engine
}

func TestCheckEdgeUniquenessInTxn_DuplicateRejected(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	}))

	// Duplicate token under same relationship type → constraint violation.
	err := engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c",
		Type: "OWNS", Properties: map[string]any{"token": "abc"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.Equal(t, "OWNS", cve.Label)
	require.Equal(t, []string{"token"}, cve.Properties)
	require.Contains(t, cve.Message, "test:e1")
	require.Contains(t, cve.Message, "abc")

	// A different token must succeed (proves index iteration didn't misfire).
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:c",
		Type: "OWNS", Properties: map[string]any{"token": "different"},
	}))
}

func TestCheckEdgeUniquenessInTxn_NullValueAllowed(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})

	// Two edges with the property absent (i.e. NULL) — allowed by
	// uniqueness semantics (NULL never equals NULL for this purpose).
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c",
		Type: "OWNS", Properties: map[string]any{},
	}))
}

func TestCheckEdgeUniquenessInTxn_DifferentTypeIgnored(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "unique_token",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Properties: []string{"token"},
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "OWNS", Properties: map[string]any{"token": "shared"},
	}))

	// Same token but DIFFERENT type — must not collide.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:b",
		Type: "WORKS_WITH", Properties: map[string]any{"token": "shared"},
	}))
}

func TestCheckEdgeUniquenessInTxn_CompositeKeyDuplicateRejected(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "unique_pair",
		Type:       ConstraintUnique,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGN",
		Properties: []string{"role", "team"},
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b",
		Type: "ASSIGN", Properties: map[string]any{"role": "lead", "team": "core"},
	}))

	// Same composite (role, team) under same type → reject.
	err := engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c",
		Type: "ASSIGN", Properties: map[string]any{"role": "lead", "team": "core"},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.ElementsMatch(t, []string{"role", "team"}, cve.Properties)

	// Differ on one field — allowed.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:c",
		Type: "ASSIGN", Properties: map[string]any{"role": "lead", "team": "platform"},
	}))

	// One component NULL → composite key not built, no violation.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e4", StartNode: "test:a", EndNode: "test:d",
		Type: "ASSIGN", Properties: map[string]any{"role": "lead"},
	}))
}

func TestCheckEdgeTemporalInTxn_OverlapRejected(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "temporal_records",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "RECORDS",
		Properties: []string{"subject", "valid_from", "valid_to"},
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS",
		Properties: map[string]any{
			"subject":    "s1",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		},
	}))

	// Overlapping interval, same subject → reject.
	err := engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS",
		Properties: map[string]any{
			"subject":    "s1",
			"valid_from": time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
		},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintTemporal, cve.Type)
	require.Contains(t, cve.Message, "test:e1")

	// Adjacent (no overlap), same subject → allowed.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS",
		Properties: map[string]any{
			"subject":    "s1",
			"valid_from": time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		},
	}))

	// Different subject, overlapping interval → allowed (different bucket).
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e4", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS",
		Properties: map[string]any{
			"subject":    "s2",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		},
	}))
}

func TestCheckEdgeTemporalInTxn_NullKeyRejected(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "temporal_records",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityRelationship,
		Label:      "RECORDS",
		Properties: []string{"subject", "valid_from", "valid_to"},
	})

	err := engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "RECORDS",
		Properties: map[string]any{
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintTemporal, cve.Type)
}

func TestCheckEdgeCardinalityInTxn_OutgoingMaxEnforced(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "cardinality_owns_max2",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "OWNS",
		Direction:  "OUTGOING",
		MaxCount:   2,
	})

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "OWNS",
		Properties: map[string]any{},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "OWNS",
		Properties: map[string]any{},
	}))

	// Third outgoing OWNS from test:a violates MaxCount=2.
	err := engine.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:d", Type: "OWNS",
		Properties: map[string]any{},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
	require.Equal(t, "OWNS", cve.Label)
	require.Contains(t, cve.Message, "test:a")
	require.Contains(t, cve.Message, "outgoing")
	require.Contains(t, cve.Message, "2")

	// Different start node — the count for THAT node is 0, allowed.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e4", StartNode: "test:b", EndNode: "test:d", Type: "OWNS",
		Properties: map[string]any{},
	}))
}

func TestCheckEdgeCardinalityInTxn_IncomingMaxEnforced(t *testing.T) {
	engine := edgeConstraintFixture(t, Constraint{
		Name:       "cardinality_owns_in1",
		Type:       ConstraintCardinality,
		EntityType: ConstraintEntityRelationship,
		Label:      "ASSIGNED",
		Direction:  "INCOMING",
		MaxCount:   1,
	})

	// First incoming ASSIGNED edge into test:a is allowed.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:b", EndNode: "test:a", Type: "ASSIGNED",
		Properties: map[string]any{},
	}))

	// Second incoming ASSIGNED into test:a violates MaxCount=1.
	err := engine.CreateEdge(&Edge{
		ID: "test:e2", StartNode: "test:c", EndNode: "test:a", Type: "ASSIGNED",
		Properties: map[string]any{},
	})
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve))
	require.Equal(t, ConstraintCardinality, cve.Type)
	require.Contains(t, cve.Message, "incoming")
	require.Contains(t, cve.Message, "test:a")

	// Different relationship type doesn't count — allowed.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e3", StartNode: "test:c", EndNode: "test:a", Type: "MENTIONS",
		Properties: map[string]any{},
	}))
}

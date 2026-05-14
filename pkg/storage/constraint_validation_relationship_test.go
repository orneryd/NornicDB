package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// asConstraintViolation extracts the typed *ConstraintViolationError
// from an error chain so individual fields can be asserted on. Test
// helper kept local because constraint_validation.go's package-level
// errors are returned via the standard error interface but tests want
// to inspect Type / Label / Properties / Message specifically.
func asConstraintViolation(t *testing.T, err error) *ConstraintViolationError {
	t.Helper()
	require.Error(t, err)
	var cve *ConstraintViolationError
	require.True(t, errors.As(err, &cve), "expected *ConstraintViolationError, got %T: %v", err, err)
	return cve
}

func TestValidateRelUniquenessOnEdges_SingleProperty_Pass(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"label": "a"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"label": "b"}},
		// Different type — must be ignored entirely.
		{ID: "e3", Type: "OTHER", Properties: map[string]any{"label": "a"}},
	}
	c := Constraint{
		Type:       ConstraintUnique,
		Label:      "KNOWS",
		Properties: []string{"label"},
	}
	require.NoError(t, validateRelUniquenessOnEdges(edges, c))
}

func TestValidateRelUniquenessOnEdges_SingleProperty_DuplicateFails(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"label": "dup"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"label": "dup"}},
	}
	c := Constraint{
		Type:       ConstraintUnique,
		Label:      "KNOWS",
		Properties: []string{"label"},
	}
	cve := asConstraintViolation(t, validateRelUniquenessOnEdges(edges, c))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.Equal(t, "KNOWS", cve.Label)
	require.Equal(t, []string{"label"}, cve.Properties)
	require.Contains(t, cve.Message, "e1")
	require.Contains(t, cve.Message, "e2")
	require.Contains(t, cve.Message, "dup")
}

func TestValidateRelUniquenessOnEdges_NilValueSkipped(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"label": nil}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"label": nil}},
		{ID: "e3", Type: "KNOWS", Properties: map[string]any{"label": "real"}},
	}
	c := Constraint{Type: ConstraintUnique, Label: "KNOWS", Properties: []string{"label"}}
	// Two edges with nil don't constitute a duplicate; the
	// uniqueness check skips nil per the function's contract.
	require.NoError(t, validateRelUniquenessOnEdges(edges, c))
}

func TestValidateRelUniquenessOnEdges_MultiPropertyDelegatesToComposite(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "y"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "y"}}, // composite dup
	}
	c := Constraint{
		Type:       ConstraintUnique,
		Label:      "KNOWS",
		Properties: []string{"a", "b"},
	}
	cve := asConstraintViolation(t, validateRelUniquenessOnEdges(edges, c))
	require.Equal(t, ConstraintUnique, cve.Type)
	require.ElementsMatch(t, []string{"a", "b"}, cve.Properties)
	require.Contains(t, cve.Message, "duplicate composite key")
}

func TestValidateRelCompositeUniquenessOnEdges_NilSkipsRow(t *testing.T) {
	// e2 has a nil for one composite component — must be skipped, not
	// reported as a duplicate of e1.
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "y"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": nil}},
	}
	c := Constraint{Type: ConstraintUnique, Label: "KNOWS", Properties: []string{"a", "b"}}
	require.NoError(t, validateRelCompositeUniquenessOnEdges(edges, c))
}

func TestValidateRelExistenceOnEdges_AllPresent(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "y"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "z"}},
		{ID: "e3", Type: "OTHER", Properties: map[string]any{}}, // wrong type, ignored
	}
	c := Constraint{Type: ConstraintExists, Label: "KNOWS", Properties: []string{"a", "b"}}
	require.NoError(t, validateRelExistenceOnEdges(edges, c))
}

func TestValidateRelExistenceOnEdges_MissingFails(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "KNOWS", Properties: map[string]any{"a": "x", "b": "y"}},
		{ID: "e2", Type: "KNOWS", Properties: map[string]any{"a": "x"}}, // missing b
	}
	c := Constraint{Type: ConstraintExists, Label: "KNOWS", Properties: []string{"a", "b"}}
	cve := asConstraintViolation(t, validateRelExistenceOnEdges(edges, c))
	require.Equal(t, ConstraintExists, cve.Type)
	require.Equal(t, []string{"b"}, cve.Properties)
	require.Contains(t, cve.Message, "e2")
	require.Contains(t, cve.Message, "missing required property b")
}

func TestTemporalCompositeKey(t *testing.T) {
	edge := &Edge{ID: "e1", Properties: map[string]any{"x": "a", "y": int64(7)}}
	got, err := temporalCompositeKey(edge, []string{"x", "y"})
	require.NoError(t, err)
	require.Equal(t, "a\x007", got)

	// Nil component → error mentioning the property name.
	edgeBad := &Edge{ID: "e2", Properties: map[string]any{"x": "a"}}
	_, err = temporalCompositeKey(edgeBad, []string{"x", "y"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "e2")
	require.Contains(t, err.Error(), "y")
}

func TestValidateRelTemporalOnCreationForEngine_NoOverlap(t *testing.T) {
	// Three RECORDS edges keyed by (subject, target). Disjoint
	// intervals — must validate clean.
	edges := []*Edge{
		{ID: "e1", Type: "RECORDS", Properties: map[string]any{
			"subject": "s1", "target": "t1",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		}},
		{ID: "e2", Type: "RECORDS", Properties: map[string]any{
			"subject": "s1", "target": "t1",
			"valid_from": time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		}},
		{ID: "e3", Type: "RECORDS", Properties: map[string]any{
			"subject": "s2", "target": "t2",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
	c := Constraint{
		Type:       ConstraintTemporal,
		Label:      "RECORDS",
		Properties: []string{"subject", "target", "valid_from", "valid_to"},
	}
	require.NoError(t, validateRelTemporalOnCreationForEngine(edges, c))
}

func TestValidateRelTemporalOnCreationForEngine_OverlapFails(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "RECORDS", Properties: map[string]any{
			"subject": "s1", "target": "t1",
			"valid_from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			"valid_to":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		}},
		{ID: "e2", Type: "RECORDS", Properties: map[string]any{
			"subject": "s1", "target": "t1",
			"valid_from": time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), // overlaps e1
			"valid_to":   time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
	c := Constraint{
		Type:       ConstraintTemporal,
		Label:      "RECORDS",
		Properties: []string{"subject", "target", "valid_from", "valid_to"},
	}
	cve := asConstraintViolation(t, validateRelTemporalOnCreationForEngine(edges, c))
	require.Equal(t, ConstraintTemporal, cve.Type)
	require.Contains(t, cve.Message, "overlap")
	require.Contains(t, cve.Message, "e1")
	require.Contains(t, cve.Message, "e2")
}

func TestValidateRelTemporalOnCreationForEngine_TooFewProperties(t *testing.T) {
	c := Constraint{Type: ConstraintTemporal, Label: "RECORDS", Properties: []string{"valid_from", "valid_to"}}
	err := validateRelTemporalOnCreationForEngine(nil, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least 3 properties")
}

func TestValidateRelTemporalOnCreationForEngine_BadTimeValueRejected(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "RECORDS", Properties: map[string]any{
			"subject": "s1", "target": "t1",
			"valid_from": "not-a-time",
			"valid_to":   time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
	c := Constraint{
		Type:       ConstraintTemporal,
		Label:      "RECORDS",
		Properties: []string{"subject", "target", "valid_from", "valid_to"},
	}
	cve := asConstraintViolation(t, validateRelTemporalOnCreationForEngine(edges, c))
	require.Equal(t, ConstraintTemporal, cve.Type)
	require.Contains(t, cve.Message, "valid_from")
}

func TestIsValueInAllowedList(t *testing.T) {
	// Exact-match cases use compareValues semantics (numeric coercion).
	require.True(t, isValueInAllowedList("a", []interface{}{"a", "b"}))
	require.True(t, isValueInAllowedList(int64(5), []interface{}{int64(1), int64(5), int64(9)}))
	require.False(t, isValueInAllowedList("c", []interface{}{"a", "b"}))
	// Empty allow-list rejects everything.
	require.False(t, isValueInAllowedList("a", nil))
}

func TestValidateRelDomainOnCreationForEngine_AllAllowed(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "STATUS", Properties: map[string]any{"v": "active"}},
		{ID: "e2", Type: "STATUS", Properties: map[string]any{"v": "inactive"}},
		// Wrong type — ignored.
		{ID: "e3", Type: "OTHER", Properties: map[string]any{"v": "bogus"}},
		// Nil value — explicitly allowed.
		{ID: "e4", Type: "STATUS", Properties: map[string]any{"v": nil}},
	}
	c := Constraint{
		Type:          ConstraintDomain,
		Label:         "STATUS",
		Properties:    []string{"v"},
		AllowedValues: []interface{}{"active", "inactive"},
	}
	require.NoError(t, validateRelDomainOnCreationForEngine(edges, c))
}

func TestValidateRelDomainOnCreationForEngine_DisallowedValueFails(t *testing.T) {
	edges := []*Edge{
		{ID: "e1", Type: "STATUS", Properties: map[string]any{"v": "active"}},
		{ID: "e2", Type: "STATUS", Properties: map[string]any{"v": "deleted"}},
	}
	c := Constraint{
		Type:          ConstraintDomain,
		Label:         "STATUS",
		Properties:    []string{"v"},
		AllowedValues: []interface{}{"active", "inactive"},
	}
	cve := asConstraintViolation(t, validateRelDomainOnCreationForEngine(edges, c))
	require.Equal(t, ConstraintDomain, cve.Type)
	require.Equal(t, []string{"v"}, cve.Properties)
	require.Contains(t, cve.Message, "e2")
	require.Contains(t, cve.Message, "deleted")
}

func TestValidateRelDomainOnCreationForEngine_PropertyCountValidation(t *testing.T) {
	c := Constraint{Type: ConstraintDomain, Label: "X", Properties: []string{"a", "b"}, AllowedValues: []interface{}{"x"}}
	err := validateRelDomainOnCreationForEngine(nil, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly 1 property")
}

func TestValidateRelDomainOnCreationForEngine_EmptyAllowedListRejected(t *testing.T) {
	c := Constraint{Type: ConstraintDomain, Label: "X", Properties: []string{"a"}, AllowedValues: nil}
	err := validateRelDomainOnCreationForEngine(nil, c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one allowed value")
}

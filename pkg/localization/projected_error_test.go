package localization

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type projectionClassification struct{ message string }

func (e *projectionClassification) Error() string      { return e.message }
func (e *projectionClassification) StatusCode() string { return "fixture.classification" }

func TestErrorProjectionPreservesTypedCopiesAndCause(t *testing.T) {
	cause := errors.New("original transport fixture")
	original := NewLocalizedError("original", Message{ID: "fixture", Fallback: "original source fixture"}, cause)
	copy := *original
	copy.Message.Fallback = "safe display"
	classification := &projectionClassification{message: "corrected position"}
	projected := ProjectError(original, "safe display", &copy, classification)
	require.EqualError(t, projected, "safe display")
	require.Same(t, original, errors.Unwrap(projected))
	require.ErrorIs(t, projected, original)
	require.ErrorIs(t, projected, cause)
	var localized *LocalizedError
	require.ErrorAs(t, projected, &localized)
	require.Same(t, &copy, localized)
	var typed *projectionClassification
	require.ErrorAs(t, projected, &typed)
	require.Same(t, classification, typed)
	var status interface{ StatusCode() string }
	require.ErrorAs(t, projected, &status)
	require.Equal(t, "fixture.classification", status.StatusCode())
	require.Equal(t, "original source fixture", original.Message.Fallback)

	plain := ProjectError(original, "position only", nil, nil)
	require.ErrorAs(t, plain, &localized)
	require.Same(t, original, localized, "missing override continues through original chain")
	var typedNil *projectionClassification
	plain = ProjectError(original, "position only", nil, typedNil)
	require.ErrorAs(t, plain, &localized)
	require.Same(t, original, localized)
}

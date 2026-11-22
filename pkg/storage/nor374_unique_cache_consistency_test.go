package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Cache completeness cannot change which supported writes satisfy UNIQUE.
// This comparison does not choose whether NaN inside a list is reflexive.
func TestNOR374UniqueWriteAdmissionIsIndependentOfClaimCacheCompleteness(t *testing.T) {
	accepted := make(map[bool]bool)
	for _, complete := range []bool{false, true} {
		engine := newTestEngine(t)
		schema := engine.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddUniqueConstraint("doc_ref", "Doc", "ref"))
		if complete {
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, schema))
		}
		require.NoError(t, nor374CreateDoc(t, engine, "test:a", []any{math.NaN(), "key"}))
		err := nor374CreateDoc(t, engine, "test:b", []any{math.NaN(), "key"})
		accepted[complete] = err == nil
		count, countErr := engine.NodeCount(t.Context())
		require.NoError(t, countErr)
		if err == nil {
			require.Equal(t, int64(2), count)
		} else {
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, ConstraintUnique, violation.Type)
			require.Equal(t, int64(1), count)
		}
	}
	require.Equal(t, accepted[false], accepted[true], "the same writes must have the same UNIQUE decision before and after a cache rebuild")
}

// Author: Lane #1644 (Codex). Signature migration preserves existing value oracles.
package search

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func mustSearchRead[T any](t *testing.T) func(T, error) T {
	t.Helper()
	return func(value T, err error) T { t.Helper(); require.NoError(t, err); return value }
}

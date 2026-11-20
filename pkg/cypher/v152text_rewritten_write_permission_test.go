package cypher

// v1.5.2 verification, rebuilt from report entry 12: a write statement that the
// engine rewrites before running (a numeric literal with an underscore) must be
// classified as a write by QueryPermissionRequirements.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestV152TextRewrittenWriteIsClassifiedAsWrite(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const q = "CREATE (:Audit {amount: 1.0_0})"
		requirements := QueryPermissionRequirements(context.Background(), q, RawSource)
		b, err := storage.NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		exec := NewStorageExecutor(storage.NewNamespacedEngine(b, "nornic"), QueryCachePolicy{MaxEntries: 0})
		_, execErr := exec.Execute(context.Background(), q, nil)
		t.Logf("V152TEXT E12 requirements=%+v execute err=%v", requirements, execErr)
		require.NoError(t, execErr)
		require.True(t, requirements.Write, "the rewritten write statement was classified without Write")
	})
}

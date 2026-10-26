package storagefault

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeReadBoundaryPreservesFailureAndContinuation(t *testing.T) {
	injected := errors.New("actual snapshot read failure")
	for _, tc := range []struct {
		mode ReadOperation
		kind storage.StoredEntityReadKind
	}{
		{NodeRead, storage.StoredEntityNodePriorRead},
		{EdgeRead, storage.StoredEntityEdgePriorRead},
		{NodeCountRead, storage.StoredEntityNodeSnapshotScan},
		{EdgeCountRead, storage.StoredEntityEdgeSnapshotScan},
		{NodeStreamRead, storage.StoredEntityNodeSnapshotScan},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			fixture := NewReadFailureEngine(nil, nil, injected)
			calls := 0
			read := func() error { calls++; return nil }
			require.NoError(t, fixture.WithStoredEntityRead(tc.kind, "test:actual", read))
			require.Equal(t, 1, calls, "normal decorator must run original read exactly once")
			fixture.SetFailure(tc.mode)
			require.ErrorIs(t, fixture.WithStoredEntityRead(tc.kind, "test:actual", read), injected)
			require.Equal(t, 1, calls, "selected failure must not substitute another read")
			fixture.SetFailure("")
			require.NoError(t, fixture.WithStoredEntityRead(tc.kind, "test:actual", read))
			require.Equal(t, 2, calls)
			other := NewReadFailureEngine(nil, nil, errors.New("other instance"))
			require.NoError(t, other.WithStoredEntityRead(tc.kind, "other:actual", read))
			require.Equal(t, 3, calls, "no global fault policy may leak across instances")
			backendErr := errors.New("native reader failed")
			require.ErrorIs(t, fixture.WithStoredEntityRead(tc.kind, "test:actual", func() error { calls++; return backendErr }), backendErr)
			require.Equal(t, 4, calls)
		})
	}
}

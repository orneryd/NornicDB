package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespacedMaterializedAccessQualifiesColonIDs(t *testing.T) {
	recorder := &namespaceLifecycleRecorder{}
	store := NewNamespacedEngine(recorder, "tenant")
	store.RecordMaterializedAccess("plain")
	store.RecordMaterializedAccess("a:b")
	store.RecordMaterializedAccess("tenant:a:b")
	store.RecordMaterializedAccess("other:id")
	require.Equal(t, []string{"tenant:plain", "tenant:a:b", "tenant:a:b", "tenant:other:id"}, recorder.accesses)
}

package search

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHNSWBuildLeaseUsesStableBorrowedVectors(t *testing.T) {
	store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 4)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Add("first", []float32{1, 2, 3, 4}))
	require.NoError(t, store.Add("second", []float32{4, 3, 2, 1}))

	lease, err := store.beginReadLease()
	if err != nil {
		t.Skipf("read-only vector mapping unavailable: %v", err)
	}
	t.Cleanup(func() { require.NoError(t, lease.Close()) })

	first, ok := lease.Lookup("first")
	require.True(t, ok)
	require.Len(t, first, 4)
	second, ok := lease.Lookup("second")
	require.True(t, ok)
	require.Equal(t, []float32{0.73029673, 0.5477226, 0.36514837, 0.18257418}, second)
	require.NotEqual(t, fmt.Sprintf("%p", &first[0]), fmt.Sprintf("%p", &second[0]))

	allocations := testing.AllocsPerRun(100, func() {
		vector, found := lease.Lookup("first")
		if !found || len(vector) != 4 {
			t.Fatal("borrowed vector unavailable")
		}
	})
	require.Zero(t, allocations, "hot HNSW neighbor lookup must not allocate")

	owned, ok := store.GetVector("first")
	require.True(t, ok)
	require.NotEqual(t, fmt.Sprintf("%p", &first[0]), fmt.Sprintf("%p", &owned[0]),
		"the public accessor must retain owning-copy semantics")
}

func BenchmarkVectorFileReadAccess(b *testing.B) {
	store, err := NewVectorFileStore(filepath.Join(b.TempDir(), "vectors"), 1024)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	vector := benchmarkStorageVector(1024, 42)
	if err := store.Add("neighbor", vector); err != nil {
		b.Fatal(err)
	}
	lease, err := store.beginReadLease()
	if err != nil {
		b.Skipf("read-only vector mapping unavailable: %v", err)
	}
	defer lease.Close()

	b.Run("owning_copy", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, ok := store.GetVector("neighbor"); !ok {
				b.Fatal("missing vector")
			}
		}
	})
	b.Run("borrowed_mapping", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, ok := lease.Lookup("neighbor"); !ok {
				b.Fatal("missing vector")
			}
		}
	})
}

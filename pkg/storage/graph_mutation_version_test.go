package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraphMutationVersions_NamespaceIsolation(t *testing.T) {
	var versions graphMutationVersions

	versions.changed("alpha")
	versions.changed("alpha")
	versions.changed("beta")

	require.Equal(t, uint64(3), versions.read(""))
	require.Equal(t, uint64(2), versions.read("alpha"))
	require.Equal(t, uint64(1), versions.read("beta"))
	require.Zero(t, versions.read("missing"))

	versions.changedPrefix("alpha:")
	require.Equal(t, uint64(4), versions.read(""))
	require.Equal(t, uint64(3), versions.read("alpha"))
	require.Equal(t, uint64(1), versions.read("beta"))
}

func BenchmarkGraphMutationVersions(b *testing.B) {
	b.Run("read", func(b *testing.B) {
		var versions graphMutationVersions
		versions.changed("bench")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = versions.read("bench")
		}
	})

	b.Run("write", func(b *testing.B) {
		var versions graphMutationVersions
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			versions.changed("bench")
		}
	})

	b.Run("parallel-read", func(b *testing.B) {
		var versions graphMutationVersions
		versions.changed("bench")
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = versions.read("bench")
			}
		})
	})
}

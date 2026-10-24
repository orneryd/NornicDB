package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func seedStreamBenchNodes(b *testing.B, count int) *BadgerEngine {
	b.Helper()
	badger, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = badger.Close() })
	for index := 0; index < count; index++ {
		_, err := badger.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("db:n-%05d", index)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"a": int64(index), "b": "value"},
		})
		require.NoError(b, err)
	}
	return badger
}

// BenchmarkStreamNodes_Full measures the full embedding-bearing scan.
func BenchmarkStreamNodes_Full(b *testing.B) {
	badger := seedStreamBenchNodes(b, 1000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := badger.StreamNodes(ctx, func(*Node) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("count = %d, want 1000", count)
		}
	}
}

// BenchmarkStreamNodes_ByPrefix measures the namespace-scoped prefix scan.
func BenchmarkStreamNodes_ByPrefix(b *testing.B) {
	badger := seedStreamBenchNodes(b, 1000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := badger.StreamNodesByPrefix(ctx, "db:", func(*Node) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("count = %d, want 1000", count)
		}
	}
}

// BenchmarkStreamNodes_ByPrefixProjected measures the projected prefix scan.
func BenchmarkStreamNodes_ByPrefixProjected(b *testing.B) {
	badger := seedStreamBenchNodes(b, 1000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := badger.StreamNodesByPrefixProjected(ctx, "db:", []string{"a"}, func(*Node) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("count = %d, want 1000", count)
		}
	}
}

// BenchmarkStreamNodes_Options measures the unified options kernel.
func BenchmarkStreamNodes_Options(b *testing.B) {
	badger := seedStreamBenchNodes(b, 1000)
	ctx := context.Background()
	opts := StreamNodesOptions{Prefix: "db:", Projection: []string{"a"}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := badger.StreamNodesWithOptions(ctx, opts, func(*Node) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("count = %d, want 1000", count)
		}
	}
}

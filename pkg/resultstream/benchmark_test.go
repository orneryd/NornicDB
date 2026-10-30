package resultstream

import (
	"context"
	"testing"
	"time"
)

func BenchmarkTokenRoundTrip(b *testing.B) {
	var secret [32]byte
	var instance [8]byte
	var streamID [16]byte
	secret[0], instance[0], streamID[0] = 1, 2, 3

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		qid := encodeToken(secret, instance, streamID, uint64(index), 1_900_000_000, [32]byte{}, "nornic")
		if _, err := decodeToken(secret, instance, qid); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProgressiveBufferedPull(b *testing.B) {
	rows := make([][]any, 1024)
	for index := range rows {
		rows[index] = []any{index}
	}
	stream, err := NewProgressive(rows, true, len(rows), nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := stream.Pull(ctx, uint64(index%1000), 16); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryBufferedPull(b *testing.B) {
	registry, err := NewRegistry(Config{TTL: time.Hour, MaxStreams: 1, MaxPageSize: 32})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(registry.Close)
	rows := make([][]any, 1024)
	for index := range rows {
		rows[index] = []any{index}
	}
	stream, err := NewProgressive(rows, true, len(rows), nil)
	if err != nil {
		b.Fatal(err)
	}
	scope := Scope{Owner: "sub:benchmark", Database: "nornic"}
	first, err := registry.Start(context.Background(), scope, stream, 1)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Pull(ctx, scope, first.QID, 16); err != nil {
			b.Fatal(err)
		}
	}
}

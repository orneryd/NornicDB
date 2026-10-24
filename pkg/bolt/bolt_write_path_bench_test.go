package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkBoltAutocommitCreateLiteralWriteBehind measures the full Bolt server
// path (handshake, session, RUN autocommit) against a write-behind Badger
// engine, one literal CREATE per iteration — PR #1043 write-path item #12
// (Bolt / server path breakdown). Pair with the direct-executor benchmark
// below for the server overhead delta.
func BenchmarkBoltAutocommitCreateLiteralWriteBehind(b *testing.B) {
	exec, _ := newBoltPathBenchExecutor(b)
	server := New(&Config{Port: 0, AllowAnonymous: true}, &boltQueryExecutorAdapter{executor: exec})
	go func() { _ = server.ListenAndServe() }()
	deadline := time.Now().Add(250 * time.Millisecond)
	for server.listener == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.listener == nil {
		b.Fatal("server failed to start (no listener)")
	}
	b.Cleanup(func() { _ = server.Close() })

	conn, reader := dialBenchConn(b, server.listener.Addr().(*net.TCPAddr).Port)
	scratch := make([]byte, 32*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf("CREATE (n:Node {id: %d})", i)
		if err := SendMessage(conn, BuildRunMessage(query, nil, nil)); err != nil {
			b.Fatal(err)
		}
		if err := benchReadSuccess(reader, scratch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBoltAutocommitCreateParameterized is the same Bolt path with a
// parameterized statement, the write shape a prepared-statement client sends.
func BenchmarkBoltAutocommitCreateParameterized(b *testing.B) {
	exec, _ := newBoltPathBenchExecutor(b)
	server := New(&Config{Port: 0, AllowAnonymous: true}, &boltQueryExecutorAdapter{executor: exec})
	go func() { _ = server.ListenAndServe() }()
	deadline := time.Now().Add(250 * time.Millisecond)
	for server.listener == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.listener == nil {
		b.Fatal("server failed to start (no listener)")
	}
	b.Cleanup(func() { _ = server.Close() })

	conn, reader := dialBenchConn(b, server.listener.Addr().(*net.TCPAddr).Port)
	scratch := make([]byte, 32*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params := map[string]any{"id": int64(i)}
		if err := SendMessage(conn, BuildRunMessage("CREATE (n:Node {id: $id})", params, nil)); err != nil {
			b.Fatal(err)
		}
		if err := benchReadSuccess(reader, scratch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDirectAutocommitCreateLiteralWriteBehind is the in-process baseline for
// the Bolt server benchmark above.
func BenchmarkDirectAutocommitCreateLiteralWriteBehind(b *testing.B) {
	exec, _ := newBoltPathBenchExecutor(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf("CREATE (n:Node {id: %d})", i)
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDirectAutocommitCreateParameterized is the in-process baseline
// for a parameterized statement (the shape #6 auto-parameterization would
// synthesize).
func BenchmarkDirectAutocommitCreateParameterized(b *testing.B) {
	exec, _ := newBoltPathBenchExecutor(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		params := map[string]any{"id": int64(i)}
		if _, err := exec.Execute(ctx, "CREATE (n:Node {id: $id})", params); err != nil {
			b.Fatal(err)
		}
	}
}

// newBoltPathBenchExecutor builds a namespaced cypher executor over a write-behind
// Badger engine in a temp dir.
func newBoltPathBenchExecutor(b *testing.B) (*cypher.StorageExecutor, *storage.BadgerEngine) {
	b.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             b.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "bench")), engine
}

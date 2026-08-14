package bolt

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkTransactionLifecycleNoTimeout(b *testing.B) {
	benchmarks := []struct {
		name     string
		metadata map[string]any
	}{
		{name: "absent"},
		{name: "zero", metadata: map[string]any{"tx_timeout": int64(0)}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				timeout, err := validateTransactionTimeout(benchmark.metadata)
				if err != nil {
					b.Fatal(err)
				}
				lifecycle := transactionLifecycle{}
				if err := lifecycle.begin(context.Background(), timeout, "neo4j", nil, benchmark.metadata, nil); err != nil {
					b.Fatal(err)
				}
				if _, err := lifecycle.claimCommit(); err != nil {
					b.Fatal(err)
				}
				lifecycle.finishCommit(nil)
			}
		})
	}
}

func BenchmarkTransactionLifecycleDisjointTransactions(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lifecycle := transactionLifecycle{}
			if err := lifecycle.begin(context.Background(), 0, "neo4j", nil, nil, nil); err != nil {
				b.Fatal(err)
			}
			if err := lifecycle.rollback(transactionTerminalRollback); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBoltExplicitTransactionNoTimeout(b *testing.B) {
	port := startBoltTransactionBenchmarkServer(b)
	benchmarks := []struct {
		name     string
		metadata map[string]any
	}{
		{name: "absent"},
		{name: "zero", metadata: map[string]any{"tx_timeout": int64(0)}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			conn := openBoltTransactionBenchmarkConn(b, port)
			defer func() { _ = conn.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkBoltTransactionRoundTrip(b, conn, benchmark.metadata)
			}
		})
	}
}

func BenchmarkBoltExplicitTransactionDisjoint(b *testing.B) {
	port := startBoltTransactionBenchmarkServer(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn := openBoltTransactionBenchmarkConn(b, port)
		defer func() { _ = conn.Close() }()
		for pb.Next() {
			benchmarkBoltTransactionRoundTrip(b, conn, nil)
		}
	})
}

func startBoltTransactionBenchmarkServer(b *testing.B) int {
	b.Helper()
	base := storage.NewMemoryEngine()
	b.Cleanup(func() {
		if err := base.Close(); err != nil {
			b.Errorf("close benchmark store: %v", err)
		}
	})
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": storage.NewNamespacedEngine(base, "nornic"),
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  64,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	server.listener = listener
	errCh := make(chan error, 1)
	go func() { errCh <- server.serve() }()
	b.Cleanup(func() {
		if err := server.Close(); err != nil {
			b.Errorf("close benchmark server: %v", err)
		}
		if err := <-errCh; err != nil {
			b.Errorf("serve benchmark server: %v", err)
		}
	})
	return listener.Addr().(*net.TCPAddr).Port
}

func openBoltTransactionBenchmarkConn(b *testing.B, port int) net.Conn {
	b.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		b.Fatal(err)
	}
	if err := PerformHandshake(conn); err != nil {
		_ = conn.Close()
		b.Fatal(err)
	}
	if err := SendMessage(conn, BuildHelloMessage(nil)); err != nil {
		_ = conn.Close()
		b.Fatal(err)
	}
	benchmarkReadSuccess(b, conn)
	return conn
}

func benchmarkBoltTransactionRoundTrip(b *testing.B, conn net.Conn, metadata map[string]any) {
	b.Helper()
	if err := SendMessage(conn, BuildBeginMessage(metadata)); err != nil {
		b.Fatal(err)
	}
	benchmarkReadSuccess(b, conn)
	if err := SendMessage(conn, BuildCommitMessage()); err != nil {
		b.Fatal(err)
	}
	benchmarkReadSuccess(b, conn)
}

func benchmarkReadSuccess(b *testing.B, conn net.Conn) {
	b.Helper()
	messageType, _, err := ReadMessage(conn)
	if err != nil {
		b.Fatal(err)
	}
	if messageType != MsgSuccess {
		b.Fatalf("expected SUCCESS, got 0x%02x", messageType)
	}
}

package bolt

import (
	"fmt"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkBoltLargeRead times an auto-commit read of 100,000 rows over Bolt
// (#939): the first PULL of 1,000 rows followed by a DISCARD of the rest
// (first-batch), and every row pulled in batches of 1,000 (pull-all).
func BenchmarkBoltLargeRead(b *testing.B) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	port, _ := startBenchTransportServer(b, benchTransportTCP, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)})
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	benchMust(b, PerformHandshake(conn))
	benchMust(b, SendMessage(conn, BuildHelloMessage(nil)))
	benchReadUntilSummary(b, conn)
	benchMust(b, SendMessage(conn, BuildRunMessage("UNWIND range(0, 99999) AS i CREATE (:Item {k: i})", nil, nil)))
	benchReadUntilSummary(b, conn)
	benchMust(b, SendMessage(conn, BuildPullMessage(nil)))
	benchReadUntilSummary(b, conn)

	// Each run has its own parameter value, so none is served from the
	// result cache.
	runs := int64(0)
	run := func(int) {
		runs++
		benchMust(b, SendMessage(conn, BuildRunMessage("MATCH (n:Item) RETURN n.k + $i AS k", map[string]any{"i": runs}, nil)))
		if records := benchReadUntilSummary(b, conn); records != 0 {
			b.Fatalf("RUN answered with %d records", records)
		}
	}
	b.Run("first-batch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			run(i)
			benchMust(b, SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(1000)})))
			if records := benchReadUntilSummary(b, conn); records != 1000 {
				b.Fatalf("PULL returned %d records", records)
			}
			benchMust(b, SendMessage(conn, append([]byte{0xB1, MsgDiscard}, encodePackStreamMap(map[string]any{"n": int64(-1)})...)))
			benchReadUntilSummary(b, conn)
		}
	})
	b.Run("pull-all", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			run(i)
			total := 0
			for total < 100000 {
				benchMust(b, SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(1000)})))
				total += benchReadUntilSummary(b, conn)
			}
		}
	})
}

func benchMust(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}

// benchReadUntilSummary reads RECORDs up to the SUCCESS that ends a reply
// and returns how many it read; a FAILURE fails the benchmark.
func benchReadUntilSummary(b *testing.B, conn net.Conn) int {
	b.Helper()
	records := 0
	for {
		msgType, data, err := ReadMessage(conn)
		benchMust(b, err)
		switch msgType {
		case MsgRecord:
			records++
		case MsgSuccess:
			return records
		default:
			b.Fatalf("message 0x%02X: %q", msgType, data)
		}
	}
}

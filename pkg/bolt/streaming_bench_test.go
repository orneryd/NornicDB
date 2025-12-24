package bolt

import (
	"context"
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type benchQueryExecutor struct {
	result *QueryResult
}

func (b *benchQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	return b.result, nil
}

func startBenchServer(b *testing.B, writeBufferSize int, exec QueryExecutor) (*Server, int) {
	b.Helper()

	config := &Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: writeBufferSize,
		AllowAnonymous:  true,
	}

	server := New(config, exec)

	go func() {
		_ = server.ListenAndServe()
	}()

	// Wait for listener binding.
	deadline := time.Now().Add(250 * time.Millisecond)
	for server.listener == nil && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if server.listener == nil {
		b.Fatal("server failed to start (no listener)")
	}

	port := server.listener.Addr().(*net.TCPAddr).Port
	b.Cleanup(func() { _ = server.Close() })
	return server, port
}

func dialBenchConn(b *testing.B, port int) (net.Conn, *bufio.Reader) {
	b.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })

	if err := PerformHandshake(conn); err != nil {
		b.Fatal(err)
	}
	if err := SendMessage(conn, BuildHelloMessage(nil)); err != nil {
		b.Fatal(err)
	}
	reader := bufio.NewReaderSize(conn, 256*1024)
	scratch := make([]byte, 32*1024)
	if err := benchReadSuccess(reader, scratch); err != nil {
		b.Fatal(err)
	}
	return conn, reader
}

func benchReadMessageType(r *bufio.Reader, scratch []byte) (byte, error) {
	var header [2]byte
	var firstTwo [2]byte
	gotFirst := 0

	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return 0, err
		}
		size := int(header[0])<<8 | int(header[1])
		if size == 0 {
			break
		}

		toRead := size

		if gotFirst < 2 {
			need := 2 - gotFirst
			if need > toRead {
				need = toRead
			}
			if _, err := io.ReadFull(r, firstTwo[gotFirst:gotFirst+need]); err != nil {
				return 0, err
			}
			gotFirst += need
			toRead -= need
		}

		for toRead > 0 {
			n := toRead
			if n > len(scratch) {
				n = len(scratch)
			}
			if _, err := io.ReadFull(r, scratch[:n]); err != nil {
				return 0, err
			}
			toRead -= n
		}
	}

	if gotFirst == 0 {
		return 0, fmt.Errorf("message too short")
	}

	if gotFirst >= 2 && firstTwo[0] >= 0xB0 && firstTwo[0] <= 0xBF {
		return firstTwo[1], nil
	}
	return firstTwo[0], nil
}

func benchReadSuccess(r *bufio.Reader, scratch []byte) error {
	msgType, err := benchReadMessageType(r, scratch)
	if err != nil {
		return err
	}
	if msgType != MsgSuccess {
		return fmt.Errorf("expected SUCCESS, got 0x%02x", msgType)
	}
	return nil
}

func BenchmarkBolt_StreamRecords_EndToEnd_SmallRow(b *testing.B) {
	const records = 2000

	row := []any{int64(1), "Alice", int64(30)}
	rows := make([][]any, records)
	for i := range rows {
		rows[i] = row
	}

	result := &QueryResult{
		Columns: []string{"id", "name", "age"},
		Rows:    rows,
	}

	exec := &benchQueryExecutor{result: result}

	for _, writeBuf := range []int{8 * 1024, 64 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("writebuf=%d", writeBuf), func(b *testing.B) {
			_, port := startBenchServer(b, writeBuf, exec)
			conn, reader := dialBenchConn(b, port)
			scratch := make([]byte, 32*1024)

			b.ReportAllocs()
			b.SetBytes(int64(records))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil)); err != nil {
					b.Fatal(err)
				}
				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}

				if err := SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(records)})); err != nil {
					b.Fatal(err)
				}

				for j := 0; j < records; j++ {
					msgType, err := benchReadMessageType(reader, scratch)
					if err != nil {
						b.Fatal(err)
					}
					if msgType != MsgRecord {
						b.Fatalf("expected RECORD, got 0x%02x", msgType)
					}
				}

				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBolt_StreamRecords_EndToEnd_LargeRow(b *testing.B) {
	const records = 2000

	node := &storage.Node{
		ID:     "n1",
		Labels: []string{"Person", "Employee"},
		Properties: map[string]any{
			"name":  "Alice",
			"age":   int64(30),
			"title": "Staff Engineer",
		},
	}
	row := []any{node, "ok", float64(3.14159)}
	rows := make([][]any, records)
	for i := range rows {
		rows[i] = row
	}

	result := &QueryResult{
		Columns: []string{"n", "status", "pi"},
		Rows:    rows,
	}

	exec := &benchQueryExecutor{result: result}

	for _, writeBuf := range []int{8 * 1024, 64 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("writebuf=%d", writeBuf), func(b *testing.B) {
			_, port := startBenchServer(b, writeBuf, exec)
			conn, reader := dialBenchConn(b, port)
			scratch := make([]byte, 32*1024)

			b.ReportAllocs()
			b.SetBytes(int64(records))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil)); err != nil {
					b.Fatal(err)
				}
				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}

				if err := SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(records)})); err != nil {
					b.Fatal(err)
				}

				for j := 0; j < records; j++ {
					msgType, err := benchReadMessageType(reader, scratch)
					if err != nil {
						b.Fatal(err)
					}
					if msgType != MsgRecord {
						b.Fatalf("expected RECORD, got 0x%02x", msgType)
					}
				}

				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

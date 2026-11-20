package bolt

// Hunt ground H10 (decoders and parsers), targets 4-5 of 8:
//
//	FuzzHuntH10PackStream  decodePackStreamValue on arbitrary bytes; a value it
//	                       accepts is encoded again and must decode to the same value
//	FuzzHuntH10BoltStream  arbitrary bytes after a valid Bolt 5.0 handshake into a
//	                       running server (in-memory database)
//
// Oracle: no panic (a panic in the server's connection goroutine ends the
// fuzz process, which records the input); no hang (the server answers or
// closes the connection within 5 s after the client half-closes); round-trip
// equality for PackStream values.
//
// Run: go test ./pkg/bolt -run '^$' -fuzz '^FuzzHuntH10PackStream$' -fuzztime 30m -parallel 1

import (
	"path/filepath"
	"os"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func huntH10Chunk(message []byte) []byte {
	out := []byte{byte(len(message) >> 8), byte(len(message))}
	out = append(out, message...)
	return append(out, 0, 0)
}

func huntH10Struct(signature byte, fields ...any) []byte {
	out := []byte{0xB0 | byte(len(fields)), signature}
	for _, field := range fields {
		out = encodePackStreamValueInto(out, field)
	}
	return out
}

func FuzzHuntH10PackStream(f *testing.F) {
	for _, seed := range []any{
		int64(1), int64(-1 << 40), 1.5, "text", []any{int64(1), "a", nil}, map[string]any{"k": []any{true, false}},
		[]byte{0, 1, 2}, nil, map[string]any{"nested": map[string]any{"x": int64(3)}},
	} {
		f.Add(encodePackStreamValue(seed))
	}
	f.Add(huntH10Struct(0x10, "RETURN 1", map[string]any{}, map[string]any{}))
	f.Fuzz(func(t *testing.T, data []byte) {
		huntH10Remember("FuzzHuntH10PackStream", data)
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, data)
			}
		}()
		done := make(chan struct{})
		var value any
		var err error
		go func() {
			defer close(done)
			value, _, err = decodePackStreamValue(data, 0)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("hang decoding %x", data)
		}
		if err != nil {
			return
		}
		switch value.(type) {
		case nil, bool, int64, float64, string, []byte, []any, map[string]any:
		default:
			return // structures (temporal, spatial, graph) have their own encoders
		}
		again, _, err := decodePackStreamValue(encodePackStreamValue(value), 0)
		if err != nil {
			t.Fatalf("value %#v decoded from %x does not decode after encoding: %v", value, data, err)
		}
		if !reflect.DeepEqual(value, again) && fmt.Sprintf("%v", value) != fmt.Sprintf("%v", again) {
			t.Fatalf("round trip differs: %#v -> %#v (input %x)", value, again, data)
		}
	})
}

var (
	huntH10ServerOnce sync.Once
	huntH10ServerAddr string
)

func huntH10Server(t testing.TB) string {
	huntH10ServerOnce.Do(func() {
		store := storage.NewMemoryEngine()
		executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "nornic"))}
		config := DefaultConfig()
		config.Port = 0
		config.MaxConnections = 64
		config.BoltAuthTimeout = 2 * time.Second      // below the 5 s hang limit
		config.BoltStatementTimeout = 2 * time.Second // a statement may not hold the connection past it
		server := New(config, executor)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		go func() { _ = server.Serve(listener) }()
		huntH10ServerAddr = listener.Addr().String()
	})
	return huntH10ServerAddr
}

func FuzzHuntH10BoltStream(f *testing.F) {
	hello := huntH10Chunk(huntH10Struct(0x01, map[string]any{"user_agent": "hunt-fuzz/1", "scheme": "none"}))
	run := huntH10Chunk(huntH10Struct(0x10, "RETURN 1 AS x", map[string]any{}, map[string]any{}))
	pull := huntH10Chunk(huntH10Struct(0x3F, map[string]any{"n": int64(-1)}))
	begin := huntH10Chunk(huntH10Struct(0x11, map[string]any{}))
	commit := huntH10Chunk(huntH10Struct(0x12))
	goodbye := huntH10Chunk(huntH10Struct(0x02))
	f.Add(append(append(append([]byte{}, hello...), run...), pull...))
	f.Add(append(append(append(append(append([]byte{}, hello...), begin...), run...), pull...), commit...))
	f.Add(append(append([]byte{}, hello...), goodbye...))
	f.Add(hello)
	f.Fuzz(func(t *testing.T, stream []byte) {
		huntH10Remember("FuzzHuntH10BoltStream", stream)
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v (input %q)", r, stream)
			}
		}()
		addr := huntH10Server(t)
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		handshake := []byte{0x60, 0x60, 0xB0, 0x17, 0, 0, 0, 5, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(handshake); err != nil {
			return
		}
		version := make([]byte, 4)
		if _, err := io.ReadFull(conn, version); err != nil {
			return
		}
		_, _ = conn.Write(stream)
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.Copy(io.Discard, conn)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("hang: server neither answered to the end nor closed within 5 s after half-close (stream %x)", stream)
		}
	})
}

// huntH10Remember keeps the input being run in $HUNT_H10_LAST_DIR/<target>.last
// (the lab's shared /work/tnrun dataset), so that an input that crashes the
// process outside the fuzz goroutine survives the job.
func huntH10Remember(target string, data []byte) {
	dir := os.Getenv("HUNT_H10_LAST_DIR")
	if dir == "" {
		return
	}
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, target+".last"), data, 0o644)
}

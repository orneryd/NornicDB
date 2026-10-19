package storage

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

func BenchmarkRecoveryWALEncoder(b *testing.B) {
	n, _ := recoveryFieldFixture()
	payload := WALNodeData{Node: n}
	for _, buffered := range []bool{false, true} {
		name := "owned"
		if buffered {
			name = "buffered"
		}
		b.Run(name, func(b *testing.B) {
			var buffer bytes.Buffer
			var destination *bytes.Buffer
			if buffered {
				destination = &buffer
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := marshalWALRecoveryPayload(destination, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// This stream isolates the actual public writer from storage decoding and disk
// latency. The paired benchmark adds this identical fixture to the old head.
type snapshotEncoderFixture struct {
	*MemoryEngine
	edge *Edge
}

func (s snapshotEncoderFixture) StreamNodes(context.Context, func(*Node) error) error { return nil }
func (s snapshotEncoderFixture) StreamEdges(_ context.Context, fn func(*Edge) error) error {
	for i := 0; i < 128; i++ {
		if err := fn(s.edge); err != nil {
			return err
		}
	}
	return nil
}

func (s snapshotEncoderFixture) StreamStoredEdgesByType(ctx context.Context, _ string, fn func(*Edge) error) error {
	return s.StreamEdges(ctx, fn)
}

func BenchmarkSnapshotEncoder(b *testing.B) {
	for _, pressure := range []bool{false, true} {
		name := "normal"
		if pressure {
			name = "pressure"
		}
		b.Run(name, func(b *testing.B) {
			s := snapshotEncoderFixture{MemoryEngine: NewMemoryEngine(), edge: &Edge{
				ID: "nornic:relationship", StartNode: "nornic:a", EndNode: "nornic:b", Type: "CALLS",
				Properties: map[string]any{"file": strings.Repeat("source/", 32), "line": int64(123), "algo": "vta", "site": "pkg/storage/snapshot_stream.go:80"},
			}}
			defer s.Close()
			// A 64 MiB pointer graph, in one allocation, gives GC meaningful
			// marking work without adding a large lab job or a production dataset.
			var ballast []*byte
			if pressure {
				ballast = make([]*byte, 8<<20)
				anchor := new(byte)
				for i := range ballast {
					ballast[i] = anchor
				}
				runtime.GC()
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				limit := int64(memory.Sys - memory.HeapReleased + memory.HeapAlloc/16)
				previous := debug.SetMemoryLimit(limit)
				defer debug.SetMemoryLimit(previous)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := WriteSnapshot(context.Background(), s, io.Discard, SnapshotOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			runtime.KeepAlive(ballast)
		})
	}
}

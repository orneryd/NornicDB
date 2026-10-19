package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryEncoderFormatEquivalence(t *testing.T) {
	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	props := map[string]any{
		"nil": nil, "bool": true, "string": "<>&\"\\\n", "bytes": []byte{0, 255},
		"i8": int8(-7), "i16": int16(-300), "i32": int32(-70000), "i64": int64(9007199254740993), "int": int(-5),
		"u8": uint8(250), "u16": uint16(65530), "u32": uint32(1<<31), "u64": uint64(1<<63), "uint": uint(7),
		"f32": float32(1.25), "f64": float64(7), "ints": []int64{1, 1<<55}, "strings": []string{"a", "b"},
		"floats": []float32{1, 2}, "bools": []bool{true, false}, "nil-list": []any(nil), "nil-map": map[string]any(nil),
		"int-list": []int{-1, 2}, "i32-list": []int32{-3, 4}, "f64-list": []float64{1.5, 2.5}, "nil-strings": []string(nil),
		"empty-list": []any{}, "empty-map": map[string]any{},
		"nested": []any{map[string]any{"before": "preserved", "stamp": time.Date(2026, 2, 3, 4, 5, 6, 7, zone)}, []any{time.Unix(123, 456).In(time.FixedZone("", 19800)), time.Unix(123, 0).UTC()}},
		"large": strings.Repeat("x", 128<<10),
	}
	n, e := recoveryFieldFixture()
	n.Properties, e.Properties = props, props
	n.EmbedMeta = map[string]any{"stamp": time.Unix(123, 0).UTC()}
	values := []any{n, e, *n, *e, &Node{}, &Edge{}, nil, json.RawMessage(`{"control":1}`), WALTxData{TxID: "tx"}, snapshotCompletion{Nodes: 3, Edges: 2},
		WALNodeData{Node: n, OldNode: n}, WALEdgeData{Edge: e, OldEdge: e}, WALDeleteData{OldNode: n, OldEdge: e},
		WALBulkNodesData{Nodes: []*Node{nil, n, &Node{}}}, WALBulkEdgesData{Edges: []*Edge{nil, e, &Edge{}}},
		WALBulkDeleteNodesData{OldNodes: []*Node{n}}, WALBulkDeleteEdgesData{OldEdges: []*Edge{e}},
		WALBulkNodesData{}, WALBulkEdgesData{Edges: []*Edge{}}, WALDeleteData{},
		(*Node)(nil), (*Edge)(nil), (*WALNodeData)(nil), (*WALEdgeData)(nil), (*WALDeleteData)(nil),
		(*WALBulkNodesData)(nil), (*WALBulkEdgesData)(nil), (*WALBulkDeleteNodesData)(nil), (*WALBulkDeleteEdgesData)(nil),
	}
	// Exercise both the value and pointer forms every append producer accepts.
	for _, v := range append([]any(nil), values...) {
		if v != nil && reflect.TypeOf(v).Kind() == reflect.Struct {
			pointer := reflect.New(reflect.TypeOf(v))
			pointer.Elem().Set(reflect.ValueOf(v))
			values = append(values, pointer.Interface())
		}
	}
	for i, source := range values {
		t.Run(fmt.Sprintf("%02d-%T", i, source), func(t *testing.T) {
			before, err := json.Marshal(source)
			require.NoError(t, err)
			legacy, err := referenceEncodeRecoveryPayload(source)
			require.NoError(t, err)
			current, err := encodeRecoveryPayload(source)
			require.NoError(t, err)
			assertRecoveryEncodedEqual(t, source, legacy, current)
			var buffer bytes.Buffer
			for _, destination := range []*bytes.Buffer{nil, &buffer} {
				legacy, err = referenceMarshalWALRecoveryPayload(nil, source)
				require.NoError(t, err)
				current, err = marshalWALRecoveryPayload(destination, source)
				require.NoError(t, err)
				assertRecoveryEncodedEqual(t, source, legacy, current)
				var oldJSON, newJSON map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(legacy, &oldJSON))
				require.NoError(t, json.Unmarshal(current, &newJSON))
				delete(oldJSON, "_nornicdb_data")
				delete(newJSON, "_nornicdb_data")
				require.Equal(t, oldJSON, newJSON, "legacy JSON projection and encoding marker")
			}
			after, err := json.Marshal(source)
			require.NoError(t, err)
			require.Equal(t, before, after, "encoding must not mutate shared entities or nested containers")
		})
	}
}

func assertRecoveryEncodedEqual(t *testing.T, source any, old, current []byte) {
	t.Helper()
	require.Len(t, current, len(old), "same wire length; map ordering may differ")
	if source == nil || (reflect.ValueOf(source).Kind() == reflect.Ptr && reflect.ValueOf(source).IsNil()) {
		require.Equal(t, old, current)
		return
	}
	typeOf := reflect.TypeOf(source)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}
	want, got := reflect.New(typeOf).Interface(), reflect.New(typeOf).Interface()
	require.NoError(t, decodeRecoveryPayload(old, want))
	require.NoError(t, decodeRecoveryPayload(current, got))
	require.Equal(t, want, got, "production reader must recover identical types and metadata")
}

func TestSnapshotFrameEncoderFormatEquivalence(t *testing.T) {
	n, e := recoveryFieldFixture()
	var output bytes.Buffer
	frames := snapshotFrameWriter{writer: &output}
	for _, item := range []struct {
		kind  byte
		value any
	}{{streamSnapshotNode, n}, {streamSnapshotEdge, e}, {streamSnapshotFooter, snapshotCompletion{Nodes: 1, Edges: 1}}} {
		output.Reset()
		require.NoError(t, frames.write(item.kind, item.value, defaultSnapshotRecordSize))
		kind, payload, err := readSnapshotFrame(&output)
		require.NoError(t, err, "production frame reader checks length and CRC")
		require.Equal(t, item.kind, kind)
		old, err := referenceEncodeRecoveryPayload(item.value)
		require.NoError(t, err)
		assertRecoveryEncodedEqual(t, item.value, old, payload)
	}
	// The public materialized writer must use the same frames for many records.
	var collection bytes.Buffer
	snapshot := &Snapshot{Sequence: 12, Nodes: []*Node{n, n}, Edges: []*Edge{e, e}}
	require.NoError(t, writeSnapshotCollection(&collection, snapshot))
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshot(&collection, visitor)
	require.NoError(t, err)
	require.Equal(t, uint64(12), metadata.Sequence)
	require.Len(t, visitor.nodes, 2)
	require.Len(t, visitor.edges, 2)
	assertRecoveryNodeFields(t, n, visitor.nodes[0])
	assertRecoveryEdgeFields(t, e, visitor.edges[1])
}

func TestRecoveryEncoderOwnedBytesAndConcurrentUse(t *testing.T) {
	n, e := recoveryFieldFixture()
	retained, err := marshalWALRecoveryPayload(nil, WALNodeData{Node: n})
	require.NoError(t, err)
	owned, err := encodeValue(n)
	require.NoError(t, err)
	retainedCopy, ownedCopy := bytes.Clone(retained), bytes.Clone(owned)
	var workers sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < cap(errors); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var buffer bytes.Buffer
			stream := snapshotEncoderFixture{MemoryEngine: NewMemoryEngine(), edge: e}
			defer stream.Close()
			for j := 0; j < 20; j++ {
				raw, err := marshalWALRecoveryPayload(&buffer, WALEdgeData{Edge: e})
				if err == nil {
					var decoded WALEdgeData
					err = decodeRecoveryPayload(raw, &decoded)
				}
				if err == nil {
					err = WriteSnapshot(context.Background(), stream, io.Discard, SnapshotOptions{})
				}
				if err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, retainedCopy, retained)
	require.Equal(t, ownedCopy, owned)
}

func TestValueEncoderResetPreservesMarshalOptions(t *testing.T) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	_, err := encoder.encode(map[string]any{"n": int64(7)})
	require.NoError(t, err)
	encoder.msgpack.UseCompactInts(true)
	current, err := encoder.encode(int64(7))
	require.NoError(t, err)
	old, err := referenceEncodeValue(int64(7))
	require.NoError(t, err)
	require.Equal(t, old, current)
	require.Equal(t, serializationVersion, current[len(serializationMagic)])
	require.Equal(t, serializerIDMsgpack, current[len(serializationMagic)+1])
}

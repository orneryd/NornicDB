package storage_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Cypher registers these extensions; the storage writer must retain all of
// them when replacing msgpack.Marshal with a reset, reusable encoder.
func TestRecoveryEncoderRegisteredPropertyKinds(t *testing.T) {
	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	stamp := time.Date(2026, 2, 3, 4, 5, 6, 789, time.UTC)
	properties := map[string]any{
		"date": cypher.CypherDate{Time: stamp},
		"local-time": cypher.CypherLocalTime{Time: stamp},
		"time": cypher.CypherTime{Time: stamp.In(time.FixedZone("", 19800))},
		"local-datetime": cypher.CypherLocalDateTime{Time: stamp},
		"datetime": cypher.CypherDateTime{Time: stamp.In(zone), ZoneID: zone.String()},
		"duration": &cypher.CypherDuration{Months: 2, Days: 3, Seconds: 4, Nanos: 567},
		"point": cypher.CypherPoint{SRID: 4979, X: 12.5, Y: 34.5, Z: 56.5},
		"uuid": cypher.CypherUUID{},
		"vector-int": cypher.CypherVector{Type: cypher.VectorInteger64, Ints: []int64{1, 1<<55}},
		"vector-float": cypher.CypherVector{Type: cypher.VectorFloat32, Floats: []float64{1.25, 2.5}},
	}
	properties["nested"] = []any{map[string]any{"value": properties["datetime"]}, properties["duration"]}
	node := &storage.Node{ID: "nornic:temporal", Properties: properties}
	// This is the prior writer's wire format: standard storage header followed
	// by msgpack.Marshal, framed with length and CRC32C, plus JSON footer.
	payload, err := msgpack.Marshal(node)
	require.NoError(t, err)
	payload = append([]byte("\xffNDB\x01\x02"), payload...)
	var old bytes.Buffer
	old.WriteString("NDS2\x02")
	require.NoError(t, binary.Write(&old, binary.LittleEndian, uint64(0)))
	require.NoError(t, binary.Write(&old, binary.LittleEndian, int64(0)))
	for _, frame := range []struct {
		kind byte
		data []byte
	}{{1, payload}, {255, []byte(`{"nodes":1,"edges":0}`)}} {
		old.WriteByte(frame.kind)
		require.NoError(t, binary.Write(&old, binary.LittleEndian, uint32(len(frame.data))))
		old.Write(frame.data)
		require.NoError(t, binary.Write(&old, binary.LittleEndian, crc32.Checksum(frame.data, crc32.MakeTable(crc32.Castagnoli))))
	}
	dir := t.TempDir()
	oldPath, newPath := filepath.Join(dir, "old.nds"), filepath.Join(dir, "new.nds")
	require.NoError(t, os.WriteFile(oldPath, old.Bytes(), 0644))
	require.NoError(t, storage.SaveSnapshot(&storage.Snapshot{Nodes: []*storage.Node{node, node}}, newPath))
	want, err := storage.LoadSnapshot(oldPath)
	require.NoError(t, err)
	got, err := storage.LoadSnapshot(newPath)
	require.NoError(t, err)
	require.Len(t, got.Nodes, 2)
	for _, decoded := range got.Nodes {
		require.Equal(t, want.Nodes[0].Properties, decoded.Properties)
		for key, value := range properties {
			require.IsType(t, value, decoded.Properties[key], key)
		}
	}
	require.IsType(t, cypher.CypherDateTime{}, properties["datetime"], "caller retains original values")
}

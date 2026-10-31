package replication

import (
	"bytes"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMessagePackReplicationPreservesCypherTypedProperties(t *testing.T) {
	stamp := time.Date(2024, 3, 5, 10, 20, 30, 123456789, time.UTC)
	node := &storage.Node{ID: "test:typed", Properties: map[string]any{
		"date":     cypher.CypherDate{Time: stamp},
		"datetime": cypher.CypherDateTime{Time: stamp, ZoneID: "UTC"},
		"duration": &cypher.CypherDuration{Months: 3, Nanos: 12},
		"point":    cypher.CypherPoint{SRID: 7203, X: 1.25, Y: -2.5},
	}}
	assertProperties := func(t *testing.T, got map[string]any) {
		t.Helper()
		date, ok := got["date"].(cypher.CypherDate)
		require.True(t, ok, "date became %T", got["date"])
		require.True(t, stamp.Equal(date.Time))
		datetime, ok := got["datetime"].(cypher.CypherDateTime)
		require.True(t, ok, "datetime became %T", got["datetime"])
		require.True(t, stamp.Equal(datetime.Time))
		require.Equal(t, "UTC", datetime.ZoneID)
		require.Equal(t, &cypher.CypherDuration{Months: 3, Nanos: 12}, got["duration"])
		require.Equal(t, cypher.CypherPoint{SRID: 7203, X: 1.25, Y: -2.5}, got["point"])
	}
	t.Run("direct command", func(t *testing.T) {
		data, err := storage.EncodeNodePayload(node)
		require.NoError(t, err)
		got, err := storage.DecodeNodePayload(data)
		require.NoError(t, err)
		assertProperties(t, got.Properties)
	})
	t.Run("snapshot", func(t *testing.T) {
		source := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		target := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		_, err := source.CreateNode(node)
		require.NoError(t, err)
		from, err := NewStorageAdapterWithWAL(source, t.TempDir())
		require.NoError(t, err)
		fixtureowner.Own(t, from)
		to, err := NewStorageAdapterWithWAL(target, t.TempDir())
		require.NoError(t, err)
		fixtureowner.Own(t, to)
		var data bytes.Buffer
		require.NoError(t, from.WriteSnapshot(&data))
		require.NoError(t, to.RestoreSnapshot(&data))
		got, err := target.GetNode(node.ID)
		require.NoError(t, err)
		assertProperties(t, got.Properties)
	})
}

package cypher

import (
	"context"
	"encoding/binary"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	testPrefixNode               = byte(0x01)
	testPrefixMVCCMeta           = byte(0x10)
	testPrefixMVCCSchemaVersion  = byte(0x02)
	testStorageVersionV1         = 1
	testSerializationVersion     = byte(1)
	testSerializerIDMsgpack      = byte(2)
	testNamespacedMigrationDB    = "nornic"
	testSerializationMagicString = "\xffNDB"
)

type testV1NodeBody struct {
	ID                 storage.NodeID         `msgpack:"ID"`
	Labels             []string               `msgpack:"Labels"`
	Properties         map[string]interface{} `msgpack:"Properties"`
	CreatedAt          time.Time              `msgpack:"CreatedAt"`
	UpdatedAt          time.Time              `msgpack:"UpdatedAt"`
	Deleted            bool                   `msgpack:"Deleted"`
	EmbedMeta          map[string]interface{} `msgpack:"EmbedMeta,omitempty"`
	ChunkEmbeddings    [][]float32            `msgpack:"ChunkEmbeddings,omitempty"`
	NamedEmbeddings    map[string][]float32   `msgpack:"NamedEmbeddings,omitempty"`
	Confidence         float64                `msgpack:"Confidence,omitempty"`
	SourceNodes        []storage.NodeID       `msgpack:"SourceNodes,omitempty"`
	InferenceMethod    string                 `msgpack:"InferenceMethod,omitempty"`
	InferenceTimestamp time.Time              `msgpack:"InferenceTimestamp,omitempty"`
	Version            storage.MVCCVersion    `msgpack:"Version,omitempty"`
	FloorVersion       storage.MVCCVersion    `msgpack:"FloorVersion,omitempty"`
	ValidFrom          time.Time              `msgpack:"ValidFrom,omitempty"`
	ValidTo            *time.Time             `msgpack:"ValidTo,omitempty"`
	SystemTime         time.Time              `msgpack:"SystemTime,omitempty"`
	TransactionID      string                 `msgpack:"TransactionID,omitempty"`
}

func testNodeKey(id storage.NodeID) []byte {
	return append([]byte{testPrefixNode}, []byte(id)...)
}

func testSchemaVersionKey() []byte {
	return []byte{testPrefixMVCCMeta, testPrefixMVCCSchemaVersion}
}

func encodeTestMsgpackWithHeader(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := msgpack.Marshal(value)
	require.NoError(t, err)
	out := make([]byte, 0, len(testSerializationMagicString)+2+len(payload))
	out = append(out, []byte(testSerializationMagicString)...)
	out = append(out, testSerializationVersion)
	out = append(out, testSerializerIDMsgpack)
	out = append(out, payload...)
	return out
}

func TestBug_MigratedBadgerNode_MatchSetUpdatesExistingNode(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	if base != nil {
		fixtureowner.Own(t, base)
	}
	require.NoError(t, err)

	namespaced := storage.NewNamespacedEngine(base, testNamespacedMigrationDB)
	_, err = namespaced.CreateNode(&storage.Node{
		ID:     "session-1",
		Labels: []string{"Session"},
		Properties: map[string]interface{}{
			"id":    "my-session-id",
			"title": "Test Session",
		},
		CreatedAt: time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, base.Close())

	rawDB, err := fixtureowner.TrackResult[*badger.DB](t, nil)(badger.Open(badger.DefaultOptions(dir).WithLogger(nil)))
	if rawDB != nil {
		fixtureowner.Own(t, rawDB)
	}
	require.NoError(t, err)

	namespacedID := storage.NodeID(testNamespacedMigrationDB + ":session-1")
	legacyBody := testV1NodeBody{
		ID:     namespacedID,
		Labels: []string{"Session"},
		Properties: map[string]interface{}{
			"id":    "my-session-id",
			"title": "Test Session",
		},
		CreatedAt: time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
	}
	require.NoError(t, rawDB.Update(func(txn *badger.Txn) error {
		if err := txn.Set(testNodeKey(namespacedID), encodeTestMsgpackWithHeader(t, &legacyBody)); err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, testStorageVersionV1)
		return txn.Set(testSchemaVersionKey(), buf)
	}))
	require.NoError(t, rawDB.Close())

	plan, err := multidb.NewCatalogUpgradeV4("system", "")
	require.NoError(t, err)
	migrated, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, AllowStorageUpgrade: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = migrated.Close() })

	store := storage.NewNamespacedEngine(migrated, testNamespacedMigrationDB)
	exec := NewStorageExecutor(store)

	readRes, err := exec.Execute(ctx, `MATCH (s:Session) WHERE s.id = "my-session-id" RETURN s.id, s.title`, nil)
	require.NoError(t, err)
	require.Len(t, readRes.Rows, 1)
	require.Equal(t, "my-session-id", readRes.Rows[0][0])
	require.Equal(t, "Test Session", readRes.Rows[0][1])

	setRes, err := exec.Execute(ctx, `MATCH (s:Session) WHERE s.id = "my-session-id" SET s.finalized_at = "2026-06-02T01:00:00Z" RETURN s.id`, nil)
	require.NoError(t, err)
	require.Len(t, setRes.Rows, 1)
	require.Equal(t, "my-session-id", setRes.Rows[0][0])

	node, err := store.GetNode("session-1")
	require.NoError(t, err)
	require.Equal(t, "2026-06-02T01:00:00Z", node.Properties["finalized_at"])
}

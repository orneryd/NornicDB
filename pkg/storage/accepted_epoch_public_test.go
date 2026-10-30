// Author: NOR-288/NOR-292 implementation lane (Codex), 2026-10-27.
package storage_test

import (
	"context"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcceptedEpochImageMatchesColdReplay(t *testing.T) {
	for _, value := range []time.Time{time.Time{}, time.Unix(0, 0).UTC(), time.Unix(0, 1).UTC(), time.Unix(0, -1).UTC()} {
		name := "zero"
		if !value.IsZero() {
			name = fmt.Sprintf("unix-ns=%d", value.UnixNano())
		}
		t.Run(name, func(t *testing.T) {
			testAcceptedTimeLiveAndCold(t, value)
		})
	}
}

func testAcceptedTimeLiveAndCold(t *testing.T, epoch time.Time) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	owner := storage.NewWALEngine(base, wal)
	manager, err := multidb.NewDatabaseManager(t.Context(), owner, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	name := manager.DefaultDatabaseName()
	_, err = manager.GetStorage(name)
	require.NoError(t, err)
	tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
	require.NoError(t, err)
	seed := epoch
	if epoch.IsZero() {
		seed = time.Unix(1, 0).UTC()
	}
	for _, id := range []storage.NodeID{storage.NodeID(name + ":a"), storage.NodeID(name + ":b")} {
		_, err = tx.CreateNode(&storage.Node{ID: id, Labels: []string{"Epoch"}, CreatedAt: seed, UpdatedAt: seed})
		require.NoError(t, err)
	}
	edgeID := storage.EdgeID(name + ":e")
	require.NoError(t, tx.CreateEdge(&storage.Edge{ID: edgeID, StartNode: storage.NodeID(name + ":a"), EndNode: storage.NodeID(name + ":b"), Type: "LINK", CreatedAt: seed, UpdatedAt: seed}))
	if epoch.IsZero() {
		// Explicit stored updates supply the accepted zero image; ordinary CREATE
		// generates timestamps, whose independent NOR-295 contract is unchanged.
		for _, id := range []storage.NodeID{storage.NodeID(name + ":a"), storage.NodeID(name + ":b")} {
			require.NoError(t, tx.UpdateGraphNode(&storage.Node{ID: id, Labels: []string{"Epoch"}}, storage.StoredNodeImages))
		}
		require.NoError(t, tx.UpdateEdge(&storage.Edge{ID: edgeID, StartNode: storage.NodeID(name + ":a"), EndNode: storage.NodeID(name + ":b"), Type: "LINK"}))
	}
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "public-epoch"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	var acceptedPayload []byte
	if os.Getenv("NOR288_EXPORT_TIME_BACKUPS") == "1" {
		acceptedPayload, err = storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Prepared: p})
		require.NoError(t, err)
	}
	r, err := owner.SubmitEntityCommit(t.Context(), p, nil)
	require.NoError(t, err)
	require.NoError(t, r.Wait(t.Context()))
	live, err := base.GetStoredNode(storage.NodeID(name + ":a"))
	require.NoError(t, err)
	liveEdge, err := base.GetStoredEdge(edgeID)
	require.NoError(t, err)
	wantNode, wantEdge := epoch, epoch
	if epoch.IsZero() {
		// The existing native edge format cannot represent year1. Keep that
		// codec limitation visible; node unset and every real instant must
		// retain their own value. No pre-accept normalization is allowed.
		wantEdge = time.Date(1754, time.August, 30, 22, 43, 41, 128654848, time.UTC)
	}
	assert.True(t, live.CreatedAt.Equal(wantNode), "native accepted node preserves the stored input")
	assert.True(t, live.UpdatedAt.Equal(wantNode))
	assert.True(t, liveEdge.CreatedAt.Equal(wantEdge), "native application uses the accepted compact image")
	assert.True(t, liveEdge.UpdatedAt.Equal(wantEdge))
	if os.Getenv("NOR288_EXPORT_TIME_BACKUPS") == "1" {
		emitAcceptedTimeBackup(t, base, epoch.UnixNano(), wantNode, wantEdge, acceptedPayload)
	}
	liveBytes, liveNodes, liveEdges := manager.GetStorageSize(context.Background(), name)
	require.NoError(t, manager.Close())
	// The semantic codec must retain the same stored timestamps as native
	// application, including unset and the real Unix epoch (NOR-303).
	t.Run("cold-replay", func(t *testing.T) {
		cold, replay, err := storage.RecoverFromWALWithResult(dir, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = cold.Close() })
		require.Empty(t, replay.Errors)
		coldNode, err := cold.GetStoredNode(live.ID)
		require.NoError(t, err)
		coldEdge, err := cold.GetStoredEdge(edgeID)
		require.NoError(t, err)
		// Compare persisted fields, excluding the native runtime embedding-owner pointer.
		assert.Equal(t, live.CreatedAt, coldNode.CreatedAt, "successful live node image must equal cold semantic replay")
		assert.Equal(t, live.UpdatedAt, coldNode.UpdatedAt)
		assert.Equal(t, liveEdge.CreatedAt, coldEdge.CreatedAt, "successful live edge image must equal cold semantic replay")
		assert.Equal(t, liveEdge.UpdatedAt, coldEdge.UpdatedAt)
		for _, pair := range [][2]time.Time{{live.CreatedAt, coldNode.CreatedAt}, {live.UpdatedAt, coldNode.UpdatedAt}, {liveEdge.CreatedAt, coldEdge.CreatedAt}, {liveEdge.UpdatedAt, coldEdge.UpdatedAt}} {
			assert.Equal(t, acceptedTimestampBytes(t, pair[0]), acceptedTimestampBytes(t, pair[1]), "persisted timestamp bytes survive cold replay")
		}
		recount, err := multidb.NewDatabaseManager(t.Context(), cold, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = recount.Close() })
		coldBytes, coldNodes, coldEdges := recount.GetStorageSize(context.Background(), name)
		assert.Equal(t, liveBytes, coldBytes)
		assert.Equal(t, liveNodes, coldNodes)
		assert.Equal(t, liveEdges, coldEdges)
	})
}

// Optional native-backup exchange for the release compatibility proof. The
// records carry only this test's synthetic entities and default catalog.
type acceptedTimeBackup struct {
	InputNanos int64  `json:"input_nanos"`
	NodeTime   string `json:"node_time"`
	EdgeTime   string `json:"edge_time"`
	Backup     string `json:"backup"`
	Payload    string `json:"payload,omitempty"`
}

func emitAcceptedTimeBackup(t *testing.T, base *storage.BadgerEngine, input int64, nodeTime, edgeTime time.Time, payload ...[]byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "time.backup")
	require.NoError(t, base.Backup(path))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err = writer.Write(raw)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	frame := acceptedTimeBackup{InputNanos: input, NodeTime: nodeTime.UTC().Format(time.RFC3339Nano), EdgeTime: edgeTime.UTC().Format(time.RFC3339Nano), Backup: base64.StdEncoding.EncodeToString(compressed.Bytes())}
	if len(payload) > 0 {
		frame.Payload = base64.StdEncoding.EncodeToString(payload[0])
	}
	encoded, err := json.Marshal(frame)
	require.NoError(t, err)
	fmt.Println("NOR288_TIME_BACKUP=" + base64.StdEncoding.EncodeToString(encoded))
}

func TestAcceptedTimeCompatibilityRead(t *testing.T) {
	encoded := os.Getenv("NOR288_TIME_BACKUPS")
	if encoded == "" {
		t.Skip("cross-version native backups supplied by the release proof")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	var frames []acceptedTimeBackup
	require.NoError(t, json.Unmarshal(raw, &frames))
	require.NotEmpty(t, frames)
	for _, frame := range frames {
		t.Run(fmt.Sprintf("unix-ns=%d", frame.InputNanos), func(t *testing.T) {
			t.Run("native-restore", func(t *testing.T) {
				compressed, err := base64.StdEncoding.DecodeString(frame.Backup)
				require.NoError(t, err)
				reader, err := gzip.NewReader(bytes.NewReader(compressed))
				require.NoError(t, err)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				path := filepath.Join(t.TempDir(), "time.backup")
				require.NoError(t, os.WriteFile(path, data, 0600))
				base, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { _ = base.Close() })
				require.NoError(t, base.Restore(path))
				assertAcceptedTimeFrame(t, base, frame)
			})
			if frame.Payload != "" && os.Getenv("NOR288_NATIVE_RESTORE_ONLY") != "1" {
				t.Run("semantic-reapply", func(t *testing.T) {
					payload, err := base64.StdEncoding.DecodeString(frame.Payload)
					require.NoError(t, err)
					base, err := storage.NewBadgerEngine(t.TempDir())
					require.NoError(t, err)
					t.Cleanup(func() { _ = base.Close() })
					require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, payload))
					assertAcceptedTimeFrame(t, base, frame)
				})
			}
		})
	}
}

func assertAcceptedTimeFrame(t *testing.T, base *storage.BadgerEngine, frame acceptedTimeBackup) {
	t.Helper()
	node, err := base.GetStoredNode("nornic:a")
	require.NoError(t, err)
	edge, err := base.GetStoredEdge("nornic:e")
	require.NoError(t, err)
	wantNode, err := time.Parse(time.RFC3339Nano, frame.NodeTime)
	require.NoError(t, err)
	wantEdge, err := time.Parse(time.RFC3339Nano, frame.EdgeTime)
	require.NoError(t, err)
	for _, field := range []struct {
		name             string
		actual, expected time.Time
	}{
		{"node.CreatedAt", node.CreatedAt, wantNode}, {"node.UpdatedAt", node.UpdatedAt, wantNode},
		{"edge.CreatedAt", edge.CreatedAt, wantEdge}, {"edge.UpdatedAt", edge.UpdatedAt, wantEdge},
	} {
		t.Logf("%s expected=%s actual=%s", field.name, field.expected.UTC().Format(time.RFC3339Nano), field.actual.UTC().Format(time.RFC3339Nano))
		if !field.actual.Equal(field.expected) {
			t.Errorf("%s differs", field.name)
		}
		assert.Equal(t, acceptedTimestampBytes(t, field.expected), acceptedTimestampBytes(t, field.actual), "%s canonical timestamp bytes", field.name)
	}
}

func acceptedTimestampBytes(t *testing.T, value time.Time) []byte {
	t.Helper()
	encoded, err := value.UTC().MarshalBinary()
	require.NoError(t, err)
	return encoded
}

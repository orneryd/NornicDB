package nornicdb

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL9HeaderlessReplicationBoundary(t *testing.T) {
	vectors := make(map[string][]float32, 16)
	for i := range 16 {
		vectors[fmt.Sprintf("v%d", i)] = []float32{float32(i), 1}
	}
	node, err := json.Marshal(struct {
		ID string
		NamedEmbeddings map[string][]float32
	}{"nornic:n", vectors})
	require.NoError(t, err)
	makePayload := func(pad int) []byte {
		data, err := json.Marshal(map[string]any{
			"cmd": map[string]any{"Type": replication.CmdCreateNode, "Data": node},
			"padding": strings.Repeat("x", pad),
		})
		require.NoError(t, err)
		return data
	}
	base, err := replication.TranscodeV4WALPayload(0, "replication_command", makePayload(0))
	require.NoError(t, err)
	entry := storage.WALEntry{Sequence: 1, Timestamp: time.Unix(1794000000, 0).UTC(), Operation: "replication_command", Checksum: 999999999}
	header, err := json.Marshal(entry)
	require.NoError(t, err)
	pad := (16 << 20) - 8 - len(header) - len(base)
	require.Positive(t, pad)
	for _, variant := range []struct {
		name string
		padding int
		repeats int
	}{{"within_limit", pad - 64, 1}, {"at_limit", pad, 8}} {
		t.Run(variant.name, func(t *testing.T) {
			payload := makePayload(variant.padding)
			entry.Data = payload
			entry.Checksum = crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
			raw, err := json.Marshal(entry)
			require.NoError(t, err)
			for attempt := range variant.repeats {
				dir := filepath.Join(t.TempDir(), "wal")
				require.NoError(t, os.Mkdir(dir, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), append(raw, '\n'), 0o600))
				before := replicationDirDigest(t, dir)
				err := prepareReplicationWAL(dir, true, string(replication.ModeHAStandby))
				_, backupErr := os.Stat(dir + ".before-v4-v6")
				t.Logf("case=%s attempt=%d upgrade=%v backup=%v", variant.name, attempt, err, backupErr)
				if err != nil {
					require.Equal(t, "at_limit", variant.name)
					require.ErrorContains(t, err, "16777216")
					require.ErrorIs(t, backupErr, os.ErrNotExist)
					require.Equal(t, before, replicationDirDigest(t, dir))
					continue
				}
				require.NoError(t, prepareReplicationWAL(dir, true, string(replication.ModeHAStandby)))
			}
		})
	}
}

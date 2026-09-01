package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestConfigLogEventPreservesCanonicalEnglishAndFields(t *testing.T) {
	var output bytes.Buffer
	config := &Config{Logger: slog.New(slog.NewJSONHandler(&output, nil))}

	config.logEvent(context.Background(), slog.LevelInfo, localization.ReplicationRaftBecameLeaderEvent("node-1", 7))

	var record map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "[Raft node-1] Became leader for term 7", record["msg"])
	require.Equal(t, "replication.raft.became_leader", record["event_id"])
	require.Equal(t, "node-1", record["node_id"])
	require.Equal(t, float64(7), record["term"])
}

func TestConfigLogPrintfPreservesCanonicalEnglishAndOpaqueArguments(t *testing.T) {
	var output bytes.Buffer
	config := &Config{Logger: slog.New(slog.NewJSONHandler(&output, nil))}

	config.logPrintf(context.Background(), slog.LevelWarn, "[HA Primary] Failed to connect to standby: %v", "offline")

	var record map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "[HA Primary] Failed to connect to standby: offline", record["msg"])
	require.Equal(t, "replication.operator", record["event_id"])
	require.Equal(t, "offline", record["arg_0"])
}

package replication

import (
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireReplicationLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestReplicationLocalizedConfigAndFactoryErrors(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.HAStandby.Role = "observer"
	config.HAStandby.PeerAddr = "peer-a:7000"

	err := config.Validate()
	localizedErr := requireReplicationLocalizedError(t, err, localization.MessageReplicationConfigInvalidHARole, "invalid HA role: observer (must be 'primary' or 'standby')")
	require.Equal(t, "observer", localizedErr.Message.Data["Role"])

	config.Mode = ReplicationMode("future_mode")
	_, err = NewReplicator(config, nil)
	localizedErr = requireReplicationLocalizedError(t, err, localization.MessageReplicationConfigUnknownMode, "unknown replication mode: future_mode")
	require.Equal(t, ReplicationMode("future_mode"), localizedErr.Message.Data["Mode"])
}

func TestReplicationLocalizedSentinelPreservesErrorsIs(t *testing.T) {
	err := errors.Join(errors.New("write rejected"), ErrNotLeader)

	require.ErrorIs(t, err, ErrNotLeader)
	requireReplicationLocalizedError(t, ErrNotLeader, localization.MessageReplicationNotLeader, "not leader")
}

func TestReplicationLocalizedTransportCause(t *testing.T) {
	missingCert := filepath.Join(t.TempDir(), "missing.pem")
	config := DefaultConfig()
	config.TLS.Enabled = true
	config.TLS.CertFile = missingCert
	config.TLS.KeyFile = missingCert

	_, err := NewDefaultTransportFromConfig(config)
	require.ErrorIs(t, err, fs.ErrNotExist)
	localizedErr := requireReplicationLocalizedError(t, err, localization.MessageReplicationTransportLoadTLSCertKeyFailed, "load TLS cert/key: open "+missingCert+": no such file or directory")
	require.Contains(t, localizedErr.Message.Data["Cause"], missingCert)
}

func TestReplicationLocalizedRaftValidation(t *testing.T) {
	replicator := &RaftReplicator{}

	_, err := replicator.HandleRaftVote(nil)
	requireReplicationLocalizedError(t, err, localization.MessageReplicationRaftVoteRequestRequired, "nil vote request")
}

func TestReplicationLocalizedStorageAdapterCause(t *testing.T) {
	engine := storage.NewMemoryEngine()
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, adapter.Close())
		require.NoError(t, engine.Close())
	})

	err = adapter.ApplyCommand(&Command{Type: CmdCreateNode, Data: []byte("{"), Timestamp: time.Now()})
	var syntaxErr *json.SyntaxError
	require.ErrorAs(t, err, &syntaxErr)
	require.ErrorIs(t, err, syntaxErr)
	requireReplicationLocalizedError(t, err, localization.MessageReplicationStorageDecodeNodeFailed, "decode node: unexpected end of JSON input")
}

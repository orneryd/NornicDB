package replication

import (
	"bytes"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func replayModeConfig(mode string, logs *bytes.Buffer) *Config {
	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "node-1"
	cfg.HAStandby.Role = "standby"
	cfg.HAStandby.PeerAddr = "primary:7000"
	cfg.ReplicationSecret = "replication-secret-of-forty-characters!!"
	cfg.ReplayProtection = mode
	cfg.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return cfg
}

// The transport says at startup which replay guarantee it gives: compat, a
// transition mode, warns that messages can be replayed; require says only
// nonce-signed messages are accepted.
func TestNewDefaultTransportFromConfig_LogsReplayProtectionMode(t *testing.T) {
	var compatLogs bytes.Buffer
	tr, err := NewDefaultTransportFromConfig(replayModeConfig(ReplayProtectionCompat, &compatLogs))
	require.NoError(t, err)
	_ = tr.Close()
	require.Contains(t, compatLogs.String(), "level=WARN")
	require.Contains(t, compatLogs.String(), "compat")
	require.Contains(t, compatLogs.String(), "replayed")

	var requireLogs bytes.Buffer
	tr, err = NewDefaultTransportFromConfig(replayModeConfig(ReplayProtectionRequire, &requireLogs))
	require.NoError(t, err)
	_ = tr.Close()
	require.Contains(t, requireLogs.String(), "require")
	require.NotContains(t, requireLogs.String(), "level=WARN")
}

// Compat gives no replay guarantee: a fresh receiver accepts an upgraded
// sender's message with its nonce stripped, again and again. Require mode
// refuses it.
func TestVerifyMessage_StrippedFrameCompatAcceptsRequireRefuses(t *testing.T) {
	var logs bytes.Buffer
	sender, err := NewDefaultTransportFromConfig(replayModeConfig(ReplayProtectionCompat, &logs))
	require.NoError(t, err)
	defer sender.Close()
	msg := &ClusterMessage{Type: ClusterMsgForwardApply, Payload: []byte("cmd")}
	sender.(*ClusterTransport).createConnection("a", nil).signMessage(msg)
	stripped := *msg
	stripped.Nonce = nil
	stripped.SignatureV2 = ""

	compat, err := NewDefaultTransportFromConfig(replayModeConfig(ReplayProtectionCompat, &logs))
	require.NoError(t, err)
	defer compat.Close()
	for i := 0; i < 2; i++ {
		frame := stripped
		require.NoError(t, compat.(*ClusterTransport).createConnection(fmt.Sprint(i), nil).verifyMessage(&frame))
	}

	strict, err := NewDefaultTransportFromConfig(replayModeConfig(ReplayProtectionRequire, &logs))
	require.NoError(t, err)
	defer strict.Close()
	frame := stripped
	require.Error(t, strict.(*ClusterTransport).createConnection("a", nil).verifyMessage(&frame))
}

// replayStateEntries counts every entry in every map the replay cache
// holds, so a test sees all the state it keeps, whatever its layout.
func replayStateEntries(c *replayCache) int {
	v := reflect.ValueOf(c).Elem()
	n := 0
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Map {
			n += v.Field(i).Len()
		}
	}
	return n
}

// All replay state stays within the cache limit, whatever sender IDs arrive
// and including admissions the full cache refuses; and an entry's size
// doesn't grow with the sender ID.
func TestReplayCache_AllStateBoundedAcrossSenders(t *testing.T) {
	cache := newReplayCache()
	cache.limit = 2
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		_ = cache.admit(fmt.Sprintf("sender-%d-%s", i, strings.Repeat("x", 1024)), newMessageNonce(), now+int64(time.Minute), now)
	}
	require.LessOrEqual(t, replayStateEntries(cache), cache.limit)

	v := reflect.ValueOf(cache).Elem()
	for i := 0; i < v.NumField(); i++ {
		if f := v.Field(i); f.Kind() == reflect.Map {
			require.False(t, containsString(f.Type().Key()), "field %s keys hold a sender ID", v.Type().Field(i).Name)
		}
	}
}

func containsString(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String:
		return true
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if containsString(t.Field(i).Type) {
				return true
			}
		}
	case reflect.Array:
		return containsString(t.Elem())
	}
	return false
}

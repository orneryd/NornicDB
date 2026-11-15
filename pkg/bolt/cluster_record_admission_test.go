package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B36/report10: one Bolt statement on a clustered (HA primary) server whose
// standby is unreachable carries a single property value too large for one
// 16 MiB journal frame. The schedule is: small write, a 8,388,608-character
// property, a 22,369,624-character property as auto-commit and as an explicit
// transaction, then a small write again.
//
// The intended contract the test asserts is a definite refusal in front of the
// client with no mutation: the statement fails with the journal's frame
// refusal (not an accepted "committed-pending or unknown" outcome), it leaves
// no node behind, it adds no entity-commit record to the engine journal, and it
// adds no entry to the replication journal. The trailing small write must still
// commit, so a refused record must not wedge either journal.
//
// Fixtures are local and disposable: one nornicdb.Open directory per test, the
// journal settings of journalConfig (WAL on, strict durability), the
// environment of admissionClusterEnv (HA primary, peer 127.0.0.1:1, async
// acknowledgement) and a Bolt server over the database manager like
// journalLab. Text fixtures are deterministic (admissionText) and only one of
// the two large values is alive at a time.

const (
	// admissionHalfFrameText is 8 MiB of characters: half of one frame's
	// 16 MiB payload budget, so its statement commits.
	admissionHalfFrameText = 8_388_608
	// admissionOversizedText is the property size of B36/report10.
	admissionOversizedText = 22_369_624

	// admissionFrameText is the journal's own phrase for a record that does not
	// fit one frame (storage.encodeWALEntryPayload).
	admissionFrameText = "exceeds maximum WAL frame size of 16777216 bytes"
	// admissionUncertainText is how an accepted-but-unknown outcome reads on the
	// wire (storage.AcceptedCommitError). It must not be the answer to a record
	// the journal refuses before anything is journaled.
	admissionUncertainText = "committed-pending or unknown"
)

// admissionText returns n printable ASCII characters from a fixed-seed
// xorshift64 generator: reproducible, version independent (not math/rand) and
// free of repeats or runs that a compressor could shrink.
func admissionText(n int) string {
	buf := make([]byte, n)
	state := uint64(0x243F6A8885A308D3)
	for i := range buf {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		buf[i] = byte('a' + state%26)
	}
	return string(buf)
}

// admissionClusterEnv is the environment one HA primary starts with: role
// primary, an unreachable peer, asynchronous acknowledgement, its own
// replication directory, and no replication secret or TLS.
func admissionClusterEnv(dir string) map[string]string {
	return map[string]string{
		"NORNICDB_CLUSTER_MODE":                     "ha_standby",
		"NORNICDB_CLUSTER_NODE_ID":                  "admission-primary",
		"NORNICDB_CLUSTER_HA_ROLE":                  "primary",
		"NORNICDB_CLUSTER_HA_SYNC_MODE":             "async",
		"NORNICDB_CLUSTER_HA_PEER_ADDR":             "127.0.0.1:1",
		"NORNICDB_CLUSTER_HA_AUTO_FAILOVER":         "false",
		"NORNICDB_CLUSTER_HA_HEARTBEAT_MS":          "10",
		"NORNICDB_CLUSTER_HA_RECONNECT_INTERVAL":    "20ms",
		"NORNICDB_CLUSTER_HA_MAX_RECONNECT_BACKOFF": "20ms",
		"NORNICDB_CLUSTER_BIND_ADDR":                "127.0.0.1:0",
		"NORNICDB_CLUSTER_ADVERTISE_ADDR":           "127.0.0.1:0",
		"NORNICDB_CLUSTER_DATA_DIR":                 filepath.Join(dir, "replication"),
		"NORNICDB_CLUSTER_REPLICATION_SECRET":       "",
		"NORNICDB_CLUSTER_TLS_ENABLED":              "false",
	}
}

// admissionJournals is what the two journals of one server hold: the engine
// journal's entity commits and highest sequence with its record operations, and
// the number of replication-journal entries.
type admissionJournals struct {
	engineCommits int
	engineLast    uint64
	engineOps     string
	replicated    int
}

func admissionJournalState(dir string) (admissionJournals, error) {
	ctx := context.Background()
	entries, err := storage.ReadWALEntriesFromDir(ctx, filepath.Join(dir, "wal"))
	if err != nil {
		return admissionJournals{}, fmt.Errorf("read the engine journal: %w", err)
	}
	state := admissionJournals{}
	var ops []string
	for _, entry := range entries {
		state.engineLast = max(state.engineLast, entry.Sequence)
		if entry.Operation == storage.OpEntityCommit {
			state.engineCommits++
		}
		ops = append(ops, fmt.Sprintf("%d:%s", entry.Sequence, entry.Operation))
	}
	state.engineOps = strings.Join(ops, " ")
	replicated, err := storage.ReadWALEntriesFromDir(ctx, filepath.Join(dir, "replication", "wal"))
	if err != nil {
		return admissionJournals{}, fmt.Errorf("read the replication journal: %w", err)
	}
	state.replicated = len(replicated)
	return state, nil
}

// admissionWaitReplicatedNode waits for a specific acknowledged command, not
// an arbitrary interval with no observed changes. The FIFO guarantees all
// earlier entries have also been published when this identity is readable.
func admissionWaitReplicatedNode(t *testing.T, dir string, id storage.NodeID) {
	t.Helper()
	require.Eventually(t, func() bool {
		entries, err := storage.ReadWALEntriesFromDir(t.Context(), filepath.Join(dir, "replication", "wal"))
		if err != nil {
			return false
		}
		for _, entry := range entries {
			var record struct {
				Command *replication.Command `json:"cmd"`
			}
			if json.Unmarshal(entry.Data, &record) != nil || record.Command == nil || record.Command.Type != replication.CmdCommitTransaction {
				continue
			}
			_, ops, err := storage.DecodeEntityTransaction(record.Command.Data)
			if err != nil {
				continue
			}
			for _, op := range ops {
				if op.Node != nil && op.Node.ID == storage.NodeID(storage.EnsureDatabasePrefix("nornic", string(id))) {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 10*time.Millisecond, "acknowledged node %s reaches the replication journal", id)
}

// admissionLabelCount is how many nodes of label the server holds.
func admissionLabelCount(t *testing.T, lab *journalLab, label string) int {
	t.Helper()
	return len(lab.nodesByLabel(label))
}

// admissionOutcome is one statement of the schedule with the journals before
// and after it and what the server holds under its label.
type admissionOutcome struct {
	name       string
	payloadLen int
	err        error
	labelCount int
	readLen    int
	before     admissionJournals
	after      admissionJournals
}

// assertDefiniteRefusal asserts that a statement whose record cannot fit one
// frame is refused in front of the client — it fails with the journal's frame
// refusal and not with an accepted-uncertain outcome — and that it mutates
// nothing. Its checks do not stop the test, so one run reports every refused
// statement of the schedule.
func assertDefiniteRefusal(t *testing.T, outcome admissionOutcome) {
	t.Helper()
	if !assert.Error(t, outcome.err, "%s must be refused", outcome.name) {
		return
	}
	assert.NotContains(t, outcome.err.Error(), admissionUncertainText,
		"%s is a definite journal refusal, not an accepted-unknown outcome", outcome.name)
	assert.Contains(t, outcome.err.Error(), admissionFrameText,
		"%s must report the frame refusal to the client", outcome.name)
	assert.Zero(t, outcome.labelCount, "%s leaves no node behind", outcome.name)
	assert.Equal(t, outcome.before.engineCommits, outcome.after.engineCommits,
		"%s writes no entity commit", outcome.name)
	assert.Equal(t, outcome.before.replicated, outcome.after.replicated,
		"%s writes no replication entry", outcome.name)
}

func TestClusterOversizedRecordAdmission(t *testing.T) {
	dir := t.TempDir()
	for key, value := range admissionClusterEnv(dir) {
		t.Setenv(key, value)
	}
	lab := journalOpen(t, dir, journalConfig())
	opened, err := admissionJournalState(dir)
	require.NoError(t, err)
	t.Logf("ADMISSION open engine{commits=%d last=%d} replication{entries=%d}",
		opened.engineCommits, opened.engineLast, opened.replicated)

	// run executes one statement of the schedule and records what the two
	// journals and the labelled nodes say about it.
	run := func(name, query, label string, payload string, explicit bool) admissionOutcome {
		outcome := admissionOutcome{name: name, payloadLen: len(payload)}
		outcome.before, err = admissionJournalState(dir)
		require.NoError(t, err)
		_, outcome.err = lab.run(query, map[string]any{"payload": payload}, explicit)
		if outcome.err == nil {
			for _, node := range lab.nodesByLabel(label) {
				admissionWaitReplicatedNode(t, dir, node.ID)
			}
		}
		outcome.after, err = admissionJournalState(dir)
		require.NoError(t, err)
		outcome.labelCount = admissionLabelCount(t, lab, label)
		if outcome.labelCount == 1 {
			for _, node := range lab.nodesByLabel(label) {
				value, _ := node.Properties["payload"].(string)
				outcome.readLen = len(value)
			}
		}
		t.Logf("ADMISSION %s err=%v nodes=%d payload=%d read=%d engines{commits %d->%d last %d->%d} replication{entries %d->%d}",
			name, outcome.err, outcome.labelCount, outcome.payloadLen, outcome.readLen,
			outcome.before.engineCommits, outcome.after.engineCommits,
			outcome.before.engineLast, outcome.after.engineLast,
			outcome.before.replicated, outcome.after.replicated)
		if outcome.after.engineOps != outcome.before.engineOps {
			t.Logf("ADMISSION %s engine ops: %s", name, outcome.after.engineOps)
		}
		return outcome
	}

	// 1. The small write first: an unreachable standby does not block a record
	// that fits one frame, so any later failure is that record's size.
	smallBefore := run("small-before", "CREATE (:AdmissionSmall {name: 'before'})", "AdmissionSmall", "", false)

	// 2. 8 MiB of characters: half a frame.
	half := run("half-frame", "CREATE (:AdmissionHalfFrame {payload: $payload})", "AdmissionHalfFrame", admissionText(admissionHalfFrameText), false)

	// 3. The oversized property, auto-commit and explicit.
	oversized := admissionText(admissionOversizedText)
	oversizedAuto := run("oversized-autocommit", "CREATE (:AdmissionOversized {payload: $payload})", "AdmissionOversized", oversized, false)
	oversizedExplicit := run("oversized-explicit", "CREATE (:AdmissionOversizedExplicit {payload: $payload})", "AdmissionOversizedExplicit", oversized, true)
	oversized = ""
	// This body fits the native journal, but its replication JSON base64 does not.
	outerOnly := run("replication-only-oversized", "CREATE (:AdmissionOuterOversized {payload: $payload})", "AdmissionOuterOversized", admissionText(12*1024*1024), false)

	// 4. The small write again: the refusals left both journals usable.
	smallAfter := run("small-after", "CREATE (:AdmissionSmall {name: 'after'})", "AdmissionSmall", "", false)

	// A record that fits one frame commits and stores its value whole.
	for _, committed := range []admissionOutcome{smallBefore, half, smallAfter} {
		require.NoError(t, committed.err, "%s commits with an unreachable standby", committed.name)
		require.NotZero(t, committed.labelCount, "%s stores its node", committed.name)
		require.Greater(t, committed.after.engineCommits, committed.before.engineCommits, "%s journals an entity commit", committed.name)
		require.Greater(t, committed.after.replicated, committed.before.replicated, "%s journals a replication entry", committed.name)
	}
	require.Equal(t, 1, smallBefore.labelCount)
	require.Equal(t, 1, half.labelCount, "the 8 MiB statement stores one node")
	require.Equal(t, admissionHalfFrameText, half.readLen, "the 8 MiB value reads back whole")
	require.Equal(t, 2, smallAfter.labelCount, "the trailing small write commits")

	// A record that does not fit one frame is refused definitely, leaves no
	// node, and never reaches either journal.
	require.Equal(t, admissionOversizedText, oversizedAuto.payloadLen)
	require.Equal(t, admissionOversizedText, oversizedExplicit.payloadLen)
	assertDefiniteRefusal(t, oversizedAuto)
	assertDefiniteRefusal(t, oversizedExplicit)
	assertDefiniteRefusal(t, outerOnly)

	// Close drains the FIFO writer. This is the final absence proof, independent
	// of the scheduling of the asynchronous writer during the probes above.
	lab.close()
	closed, err := admissionJournalState(dir)
	require.NoError(t, err)
	require.Equal(t, smallAfter.after.engineCommits, closed.engineCommits)
	require.Equal(t, smallAfter.after.replicated, closed.replicated)
	lab = journalOpen(t, dir, journalConfig())
	require.Equal(t, 2, admissionLabelCount(t, lab, "AdmissionSmall"))
	require.Equal(t, 1, admissionLabelCount(t, lab, "AdmissionHalfFrame"))
	for _, label := range []string{"AdmissionOversized", "AdmissionOversizedExplicit", "AdmissionOuterOversized"} {
		require.Zero(t, admissionLabelCount(t, lab, label))
	}
}

package storage

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412: a commit's change-capture mode holds from its store-limit check to its
// application; a commit recovered from the journal decides it when it is applied.
//
// Both tests use a low-memory store (a 32 MiB value limit) whose journal writes
// records larger than one frame, and a stored node with 20 MiB of text. A
// transaction adds a small property to that node: its journal record spans
// frames (it carries the node's whole image), its DIFF change event is small,
// and a FULL event of the same change (the text before and after) is over the
// store's value limit.

// captureSwitchWriter sets the database's capture mode when the journal writes
// the first frame of a record that spans frames: after the commit's
// store-limit check and before its application.
type captureSwitchWriter struct {
	io.Writer
	once   sync.Once
	change func()
}

func (w *captureSwitchWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte{'W', 'A', 'L', 'E', walFormatVersionSpanned}) {
		w.once.Do(w.change)
	}
	return w.Writer.Write(p)
}

func captureDecisionEngine(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, lowMemory: true, records: WALRecordsLarge})
}

// captureDecisionSeed stores the node test:admission-0 with 20 MiB of text and
// then sets the database's capture mode to DIFF.
func captureDecisionSeed(t *testing.T, b *BadgerEngine) {
	t.Helper()
	require.NoError(t, admissionCommit(t, b, CommitModeExplicit, "capture-seed", admissionNodes(1, admissionText(20<<20))))
	b.SetChangeCapture("test", ChangeCaptureDiff)
}

// captureFlag adds the property flag = true to test:admission-0.
func captureFlag(ctx context.Context, tx *BadgerTransaction) error {
	node, err := tx.GetNode(ctx, "test:admission-0")
	if err != nil {
		return err
	}
	node = CopyNode(node)
	node.Properties["flag"] = true
	return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node})
}

// requireCaptureFlag checks that the stored node has the property and its text.
func requireCaptureFlag(t *testing.T, b *BadgerEngine) {
	t.Helper()
	node, err := b.GetNode(context.Background(), "test:admission-0")
	require.NoError(t, err)
	require.Equal(t, true, node.Properties["flag"])
	text, _ := node.Properties["text"].(string)
	require.Len(t, text, 20<<20)
}

// requireCaptureEvents checks the database's change feed: no event when diff
// is false, else one DIFF update event of test:admission-0 that holds the
// added property and not the text.
func requireCaptureEvents(t *testing.T, b *BadgerEngine, diff bool) {
	t.Helper()
	events, err := b.ChangeEvents("test", 0, 0, 0)
	require.NoError(t, err)
	if !diff {
		require.Empty(t, events)
		return
	}
	require.Len(t, events, 1)
	event := events[0]
	require.Equal(t, ChangeCaptureDiff, event.Mode)
	require.Equal(t, byte('u'), event.Operation)
	require.NotNil(t, event.After)
	require.Equal(t, true, event.After.Properties["flag"])
	require.NotContains(t, event.After.Properties, "text")
}

// The commit is submitted through the journal engine (admissionCommit), and the
// database's mode is set to FULL while its record is being journaled. The commit
// is applied in the mode it was decided in: the property is stored, and the
// change feed holds one DIFF event for it. The store then reopens with OFF, DIFF
// and FULL set before the recovery at open, which has nothing left to apply, and
// holds the same node and the same event.
func TestChangeCaptureDecisionHoldsFromAdmissionToApplication(t *testing.T) {
	for _, reopen := range []ChangeCaptureMode{ChangeCaptureOff, ChangeCaptureDiff, ChangeCaptureFull} {
		t.Run("reopen_"+reopen.String(), func(t *testing.T) {
			dir := t.TempDir()
			b := captureDecisionEngine(t, dir)
			captureDecisionSeed(t, b)
			w := b.bufferedCommitWAL
			w.mu.Lock()
			require.NoError(t, w.writer.Flush())
			w.writer.Reset(&captureSwitchWriter{Writer: w.file, change: func() { b.SetChangeCapture("test", ChangeCaptureFull) }})
			w.mu.Unlock()

			require.NoError(t, admissionCommit(t, b, CommitModeExplicit, "capture-switch", captureFlag), "the commit is applied in the mode it was decided in")
			require.Equal(t, ChangeCaptureFull, b.ChangeCapture("test"), "the mode was set to FULL while the commit was journaled")
			require.Equal(t, 1, admissionRecords(t, dir, "capture-switch"))
			requireCaptureFlag(t, b)
			requireCaptureEvents(t, b, true)

			crashClose(t, b, w)
			b = captureDecisionEngine(t, dir)
			b.SetChangeCapture("test", reopen)
			_, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
			require.NoError(t, err)
			requireCaptureFlag(t, b)
			requireCaptureEvents(t, b, true)
		})
	}
}

// The commit's record is written to the journal directly (shapeJournal: the
// transaction is prepared and encoded, its application aborted and its record
// appended without the journal engine's submission or store-limit check), and
// the process stops. The recovery at open applies it under the capture mode set
// before it: with OFF it writes no change event, with DIFF the DIFF event, and
// with FULL, whose event would be over the store's value limit, the DIFF event.
func TestChangeCaptureOfACommitRecoveredFromTheJournal(t *testing.T) {
	for _, reopen := range []ChangeCaptureMode{ChangeCaptureOff, ChangeCaptureDiff, ChangeCaptureFull} {
		t.Run("reopen_"+reopen.String(), func(t *testing.T) {
			dir := t.TempDir()
			b := captureDecisionEngine(t, dir)
			captureDecisionSeed(t, b)
			w := b.bufferedCommitWAL
			shapeJournal(t, b, w, "capture-recovered", func(tx *BadgerTransaction) {
				require.NoError(t, captureFlag(context.Background(), tx))
			})

			crashClose(t, b, w)
			b = captureDecisionEngine(t, dir)
			b.SetChangeCapture("test", reopen)
			_, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
			require.NoError(t, err, "the journaled commit is applied")
			requireCaptureFlag(t, b)
			requireCaptureEvents(t, b, reopen != ChangeCaptureOff)
		})
	}
}

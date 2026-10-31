package storage

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Force the real lock order using existing physical/publication hooks. A failed
// schedule releases the schema persister with an error, then joins both writers.
func TestBufferedReplayDoesNotHoldCommitGateWhileReadingSchema(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true, lowMemory: true, wal: true})
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.RegisterTokens([]string{"Ingest"}, []string{"BEFORE"}))
	const nodes = 6000
	rng := rand.New(rand.NewPCG(1, 2))
	pad := make([]byte, 128)
	var admitted *BadgerTransaction
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		admitted = tx
		for i := 0; i < nodes; i++ {
			for j := range pad {
				pad[j] = byte('a' + rng.IntN(26))
			}
			if _, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:ingest-%d", i)), Labels: []string{"Ingest"}, Properties: map[string]any{"pad": string(pad)}}); err != nil {
				return err
			}
		}
		return nil
	})
	require.True(t, admitted.AcknowledgedBuffered(), "fixture must exercise buffered ACK and physical replay")
	schema.mu.RLock()
	original := schema.persist
	schema.mu.RUnlock()
	require.NotNil(t, original)
	entered, held, allow, abort := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enterOnce, allowOnce, abortOnce sync.Once
	sentinel := errors.New("release blocked schema persister after failed lock-order assertion")
	schema.SetPersister(func(def *SchemaDefinition) error {
		close(held)
		select {
		case <-allow:
			return original(def)
		case <-abort:
			return sentinel
		}
	})
	restoreBatch := setLargeCommitBatchHook(func(int) error {
		enterOnce.Do(func() {
			close(entered)
			select {
			case <-held:
			case <-abort:
			}
		})
		return nil
	})
	restorePublication := setPublicationStageHook(func(uint64) { allowOnce.Do(func() { close(allow) }) })
	flushed, registered := make(chan struct{}), make(chan struct{})
	var flushErr, registerErr error
	registrationStarted := false
	go func() { defer close(flushed); flushErr = b.FlushWriteBehind(nil) }()
	defer func() {
		abortOnce.Do(func() { close(abort) })
		<-flushed
		if registrationStarted {
			<-registered
		}
		restorePublication()
		restoreBatch()
		schema.SetPersister(original)
	}()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-entered:
	case <-flushed:
		t.Fatalf("fixture did not promote to a large commit: %v", flushErr)
	case <-timer.C:
		t.Fatal("large commit hook was not reached")
	}
	registrationStarted = true
	go func() { defer close(registered); registerErr = schema.RegisterTokens(nil, []string{"AFTER"}) }()
	select {
	case <-allow:
	case <-timer.C:
		t.Fatal("buffered replay held commit gate while schema persistence held schema mutex")
	}
	<-registered
	<-flushed
	require.NoError(t, flushErr)
	require.NoError(t, registerErr)
	require.Equal(t, []string{"BEFORE", "AFTER"}, schema.ExportDefinition().RelationshipTypeTokens)
	requireNoLargeCommitIntent(t, b)
	require.Zero(t, b.db.oracle.stage.pendingCount())
	got, err := b.GetNodesByLabel("Ingest")
	require.NoError(t, err)
	require.Len(t, got, nodes)
}

package replication

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// publicationNodeCommand builds a real encoded create-node command for the
// fixture's "test" namespace.
func publicationNodeCommand(t *testing.T, id string) *Command {
	t.Helper()
	data, err := encodeEntityCommandPayload(CmdCreateNode, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Publication"}})
	require.NoError(t, err)
	return &Command{Type: CmdCreateNode, Data: data, Timestamp: time.Now()}
}

// publishedNodeID reads the entity identity back out of a published record.
func publishedNodeID(t *testing.T, entry *WALEntry) string {
	t.Helper()
	operations, handled, err := decodeEntityCommandOperations(entry.Command.Type, entry.Command.Data)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, operations, 1)
	require.NotNil(t, operations[0].Node)
	return string(operations[0].Node.ID)
}

// The published receipt is this command's own physical sequence, read back from
// the record the writer appended.
func TestStorageAdapterWALPublishedReturnsOwnExactPosition(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("test:own-position-%d", i)
		publication, err := adapter.ApplyCommand(context.Background(), publicationNodeCommand(t, id), WALPublished)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), publication.Position, "the receipt is this record's own physical sequence")

		entries, err := adapter.GetWALEntries(publication.Position-1, 1)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, publication.Position, entries[0].Position)
		require.Equal(t, id, publishedNodeID(t, entries[0]))
	}
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(3), position)
}

// Concurrent publishers each receive their own record, never an earlier or later
// prefix of another command.
func TestStorageAdapterConcurrentPublicationsKeepTheirOwnIdentity(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	const writers = 8
	commands := make([]*Command, writers)
	ids := make([]string, writers)
	for i := range commands {
		ids[i] = fmt.Sprintf("test:concurrent-%d", i)
		commands[i] = publicationNodeCommand(t, ids[i])
	}

	type observed struct {
		id       string
		position uint64
	}
	results := make(chan observed, writers)
	failures := make(chan error, writers)
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < writers; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			publication, err := adapter.ApplyCommand(context.Background(), commands[i], WALPublished)
			if err != nil {
				failures <- err
				return
			}
			results <- observed{id: ids[i], position: publication.Position}
		}(i)
	}
	close(start)
	group.Wait()
	close(results)
	close(failures)

	for err := range failures {
		require.NoError(t, err)
	}
	positions := make(map[uint64]string, writers)
	for result := range results {
		require.NotZero(t, result.position)
		_, duplicate := positions[result.position]
		require.False(t, duplicate, "two commands reported the same physical record")
		positions[result.position] = result.id
	}
	require.Len(t, positions, writers)
	for position, id := range positions {
		entries, err := adapter.GetWALEntries(position-1, 1)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, id, publishedNodeID(t, entries[0]),
			"each published result matches its own command identity")
	}
}

// A stalled writer cannot yield a published receipt, and the deadline never
// retracts the accepted record or the native application.
func TestStorageAdapterPublicationWaitRetainsAcceptedWork(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	command := publicationNodeCommand(t, "test:deadline-applied")
	adapter.walMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(adapter.walMu.Unlock) }
	t.Cleanup(unlock)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	publication, err := adapter.ApplyCommand(ctx, command, WALPublished)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, publication.Position, "a timed-out wait reports no publication position")

	node, err := adapter.engine.GetNode(t.Context(), "test:deadline-applied")
	require.NoError(t, err, "the accepted native write is not retracted by the wait deadline")
	require.NotNil(t, node)
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Zero(t, position, "nothing was published while the writer was stalled")

	unlock()
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the expired wait never removed the accepted record")
	require.Equal(t, uint64(1), entries[0].Position)
	require.Equal(t, "test:deadline-applied", publishedNodeID(t, entries[0]))
}

// Invalid input fails before any queue or native side effect.
func TestStorageAdapterPublicationInputRejectionsHaveNoEffects(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	command := publicationNodeCommand(t, "test:rejected")
	rejections := []struct {
		name string
		ctx  context.Context
		cmd  *Command
		mode WALPublicationMode
	}{
		{name: "zero value mode", ctx: context.Background(), cmd: command, mode: WALPublicationMode(0)},
		{name: "unknown mode", ctx: context.Background(), cmd: command, mode: WALPublicationMode(3)},
		{name: "nil command", ctx: context.Background(), cmd: nil, mode: WALPublished},
		{name: "decode rejection", ctx: context.Background(), cmd: &Command{Type: CmdCreateNode, Data: []byte("bad"), Timestamp: time.Now()}, mode: WALPublished},
	}
	for _, rejection := range rejections {
		publication, err := adapter.ApplyCommand(rejection.ctx, rejection.cmd, rejection.mode)
		require.Error(t, err, rejection.name)
		require.Zero(t, publication.Position, rejection.name)
	}
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Empty(t, entries, "rejected input must not reach the replication WAL")
	_, err = adapter.engine.GetNode(t.Context(), "test:rejected")
	require.Error(t, err, "rejected input must not reach native storage")

	require.NoError(t, adapter.Close())
	publication, err := adapter.ApplyCommand(context.Background(), publicationNodeCommand(t, "test:closed"), WALQueued)
	require.ErrorIs(t, err, ErrClosed)
	require.Zero(t, publication.Position)
}

// nativeFailureEngine fails the accepted native create without affecting the
// adapter's WAL ownership.
type nativeFailureEngine struct {
	storage.Engine
	err error
}

func (e nativeFailureEngine) CreateNode(ctx context.Context, _ *storage.Node) (storage.NodeID, error) {
	return "", e.err
}

// The native application error stays the primary result and never filters the
// accepted record.
func TestStorageAdapterNativeApplicationFailureKeepsAcceptedRecord(t *testing.T) {
	nativeErr := errors.New("native state machine unavailable")
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	engine := nativeFailureEngine{Engine: storage.NewNamespacedEngine(base, "test"), err: nativeErr}
	adapter, err := NewStorageAdapterWithWAL(engine, filepath.Join(t.TempDir(), "wal"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })

	publication, err := adapter.ApplyCommand(context.Background(), publicationNodeCommand(t, "test:native-failure"), WALPublished)
	require.ErrorIs(t, err, nativeErr, "the native application error is returned unchanged")
	require.Zero(t, publication.Position)

	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "an application error never removes the accepted record")
	require.Equal(t, "test:native-failure", publishedNodeID(t, entries[0]))
}

// A required publication failure is reported without implying rejected native
// work, and the writer's retained append error keeps its native cause.
func TestStorageAdapterPublicationAppendFailureIsRetained(t *testing.T) {
	adapter := setupTestAdapter(t).adapter
	require.NoError(t, adapter.wal.Close()) // real closed native WAL, no injected error

	publication, err := adapter.ApplyCommand(context.Background(), publicationNodeCommand(t, "test:append-failure"), WALPublished)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	require.Zero(t, publication.Position)

	node, err := adapter.engine.GetNode(t.Context(), "test:append-failure")
	require.NoError(t, err, "a publication failure does not roll back the native write")
	require.NotNil(t, node)
}

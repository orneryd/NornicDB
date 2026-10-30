package storage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// pr1655CommitReadFaultMessage is the existing commit-path log record the
// fixture arms on. Native PublishCommit emits it between the optimistic
// validation pass and the authoritative pass, once per transaction that
// carries metadata. Nothing in production is added or changed for the
// fixture: it only observes an existing record.
const pr1655CommitReadFaultMessage = "transaction committing with metadata"

// pr1655CommitReadFault is the concurrency-safe outcome of the one-shot fault
// trigger. The slog handler runs on the committing goroutine, so it never
// touches *testing.T; the test inspects this record afterwards.
type pr1655CommitReadFault struct {
	handlerOnce sync.Once
	armOnce     sync.Once
	armed       bool
	trigger     func()
	fired       atomic.Bool
	triggered   atomic.Bool
	setupErr    atomic.Value // error
}

// setSetupError records why the fixture could not install itself. The trigger
// runs on the commit goroutine and must not fail the test from there.
func (f *pr1655CommitReadFault) setSetupError(err error) {
	if err != nil {
		f.setupErr.Store(err)
	}
}

// setupError reports the fault-installation failure, if the trigger fired
// before it could install the fixture.
func (f *pr1655CommitReadFault) setupError() error {
	if v, ok := f.setupErr.Load().(error); ok {
		return v
	}
	return nil
}

// firedOnce reports that the armed record was seen and its trigger ran once.
func (f *pr1655CommitReadFault) firedOnce() bool {
	return f.fired.Load() && f.triggered.Load()
}

// arm makes the fault one-shot for trigger. It is called only after the graph
// is seeded and the write is staged, so the trigger can only fire at commit.
func (f *pr1655CommitReadFault) arm(trigger func()) {
	f.armOnce.Do(func() {
		f.trigger = trigger
		f.armed = true
	})
}

// pr1655FaultHandler is the custom slog.Handler supplied in BadgerOptions at
// engine construction. It forwards every record to a discard handler and acts
// only on the armed commit record, exactly once.
type pr1655FaultHandler struct {
	fault *pr1655CommitReadFault
	next  slog.Handler
}

func (h *pr1655FaultHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *pr1655FaultHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == pr1655CommitReadFaultMessage && h.hasMetadataAttr(record) {
		h.fault.handlerOnce.Do(func() {
			if !h.fault.armed {
				return
			}
			h.fault.fired.Store(true)
			h.fault.triggered.Store(true)
			h.fault.trigger()
		})
	}
	return h.next.Handle(ctx, record)
}

// hasMetadataAttr requires the metadata attribute the commit-path record
// carries, so an unrelated record with the same text cannot fire the fault.
func (h *pr1655FaultHandler) hasMetadataAttr(record slog.Record) bool {
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "metadata" && attr.Value.Kind() == slog.KindAny && attr.Value.Any() != nil {
			found = true
			return false
		}
		return true
	})
	return found
}

func (h *pr1655FaultHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pr1655FaultHandler) WithGroup(string) slog.Handler      { return h }

// pr1655NewCommitReadFaultEngine opens the real BadgerEngine the commit-read
// regression runs against: the custom slog.Handler is supplied in
// BadgerOptions at construction so the fixture can arm the one-shot fault on
// the existing commit metadata record, with no production hook. The engine is
// tracked by the canonical fixture owner, which audits its Close at cleanup.
func pr1655NewCommitReadFaultEngine(t *testing.T, writeBehind bool) (*BadgerEngine, *pr1655CommitReadFault) {
	t.Helper()
	fault := &pr1655CommitReadFault{}
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		LowMemory:           true,
		WriteBehind:         writeBehind,
		WriteBehindInterval: time.Hour,
		Logger:              slog.New(&pr1655FaultHandler{fault: fault, next: slog.DiscardHandler}),
	}))
	require.NoError(t, err)
	require.NotNil(t, engine)
	t.Cleanup(func() { _ = engine.Close() })
	return engine, fault
}

// pr1655HeadBytes reads the raw MVCC head record of id through a fresh Badger
// read, without applying MVCC visibility or publication semantics.
func pr1655HeadBytes(b *BadgerEngine, id NodeID) ([]byte, error) {
	key := b.mvccNodeHeadKeyStringLookup(id)
	if key == nil {
		return nil, fmt.Errorf("no MVCC head key for node %q", id)
	}
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = append([]byte(nil), val...)
			return nil
		})
	})
	return value, err
}

// pr1655WriteHeadBytes writes raw MVCC head bytes through b.db.Update, not
// withUpdate, so the fixture never publishes a semantic node mutation.
func pr1655WriteHeadBytes(b *BadgerEngine, id NodeID, value []byte) error {
	key := b.mvccNodeHeadKeyStringLookup(id)
	if key == nil {
		return fmt.Errorf("no MVCC head key for node %q", id)
	}
	return b.db.Update(func(txn *badger.Txn) error { return txn.Set(key, value) })
}

// PR1655CorruptMVCCHead is the fixture seam the external storage_test
// regression drives: it replaces id's committed MVCC head record with
// malformed bytes and returns the restore that puts the saved bytes back and
// the actual decodeMVCCHead error the commit path will observe. It is a test
// file method, so production code carries no hook for it.
func (b *BadgerEngine) PR1655CorruptMVCCHead(id NodeID) (restore func() error, decodeErr error) {
	original, err := pr1655HeadBytes(b, id)
	if err != nil {
		return nil, err
	}
	malformed := []byte{0xff}
	if err := pr1655WriteHeadBytes(b, id, malformed); err != nil {
		return nil, err
	}
	restored := false
	restore = func() error {
		if restored {
			return nil
		}
		restored = true
		return pr1655WriteHeadBytes(b, id, original)
	}
	_, decodeErr = decodeMVCCHead(malformed)
	if decodeErr == nil {
		return nil, fmt.Errorf("the malformed MVCC head %v decoded without error", malformed)
	}
	return restore, decodeErr
}

// pr1655CorruptMVCCHead is the internal test's wrapper: it installs the same
// corruption through the fixture seam and restores it at cleanup.
func pr1655CorruptMVCCHead(t *testing.T, b *BadgerEngine, id NodeID) (decodeErr error) {
	t.Helper()
	restore, decodeErr := b.PR1655CorruptMVCCHead(id)
	if restore == nil {
		return decodeErr
	}
	t.Cleanup(func() {
		if err := restore(); err != nil {
			t.Errorf("restoring the MVCC head of %q: %v", id, err)
		}
	})
	return decodeErr
}

// pr1655PhysicalBody reads the node's stored body straight from its record,
// without consulting the MVCC head. It is how a test inspects what a failed
// commit published while the fixture holds the faulty head.
func pr1655PhysicalBody(t *testing.T, b *BadgerEngine, id NodeID) *Node {
	t.Helper()
	var node *Node
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = b.decodeNode(namespaceForNodeID(id), append([]byte(nil), val...))
			return decodeErr
		})
	})
	require.NoError(t, err, "reading the physical body of %q", id)
	require.NotNil(t, node)
	return node
}

// pr1655AssertFaultFired is the post-condition every fixture-driven test
// checks: the armed record fired exactly once and never reported a setup
// failure.
func pr1655AssertFaultFired(t *testing.T, fault *pr1655CommitReadFault) {
	t.Helper()
	require.NoError(t, fault.setupError(), "the commit-read fault fixture failed to install")
	require.True(t, fault.firedOnce(), "the commit-read fault must fire exactly once")
}

// pr1655DrainWriteBehind commits buffered acknowledgements so the seeded graph
// is physically stored before a transaction pins its snapshot.
func pr1655DrainWriteBehind(t *testing.T, b *BadgerEngine) {
	t.Helper()
	require.NoError(t, b.FlushWriteBehind())
}

// pr1655AssertNoLeaks checks the observable post-commit state: the commit
// write barrier is free, no write-behind acknowledgement is still buffered,
// and the namespace's committed sequence did not advance past the failed
// transaction. A commit that leaked its barrier or its buffered work hangs or
// trips here.
func pr1655AssertNoLeaks(t *testing.T, b *BadgerEngine, sequenced uint64, message string) {
	t.Helper()
	acquired := make(chan struct{})
	go func() {
		b.writeBarrier.Lock()
		b.writeBarrier.Unlock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the write barrier is still held after a failed commit", message)
	}
	if b.writeBehind != nil {
		require.Zero(t, b.writeBehind.PendingOps(), "%s: write-behind work is still buffered", message)
	}
	state, err := b.namespaceMVCC("test")
	require.NoError(t, err, "%s: reading the namespace MVCC state", message)
	require.Equal(t, sequenced, state.seq.Load(), "%s: the failed commit published an MVCC sequence", message)
}

// pr1655StageBody stages an update of id's body inside tx.
func pr1655StageBody(tx *BadgerTransaction, id NodeID, body string) error {
	return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"body": body}}, Policy: ClientNodeEdits})
}

// pr1655SeedDoc creates a Doc node with a body in the test namespace, drains
// the write-behind buffer so it is physically stored, and returns the
// namespace's committed sequence afterwards.
func pr1655SeedDoc(t *testing.T, b *BadgerEngine, id NodeID, body string) uint64 {
	t.Helper()
	seed, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, seed.SetNamespace("test"))
	_, err = seed.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"body": body}})
	require.NoError(t, err)
	require.NoError(t, seed.Commit())
	pr1655DrainWriteBehind(t, b)
	state, err := b.namespaceMVCC("test")
	require.NoError(t, err)
	return state.seq.Load()
}

// TestPR1655CommitReadFixturesStayInert proves the fixture itself does not
// change a healthy commit: an unarmed handler lets the metadata commit take
// its ordinary path and never fires.
func TestPR1655CommitReadFixturesStayInert(t *testing.T) {
	engine, fault := pr1655NewCommitReadFaultEngine(t, false)
	pr1655SeedDoc(t, engine, "test:fixture-inert", "before")

	rename, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, rename.SetNamespace("test"))
	require.NoError(t, rename.SetMetadata(map[string]any{"fixture": true}))
	require.NoError(t, pr1655StageBody(rename, "test:fixture-inert", "changed"))
	require.NoError(t, rename.Commit(), "an unarmed fixture must not disturb a healthy commit")
	require.False(t, fault.fired.Load(), "the fault fired without being armed")
	require.False(t, rename.IsActive(), "the committed transaction closed")

	node, err := engine.GetNode("test:fixture-inert")
	require.NoError(t, err)
	require.Equal(t, "changed", node.Properties["body"])
}

// TestPR1655CommitReadCorruptionIsRestorable anchors the fixture on the real
// decoder and proves the saved bytes put a healthy head back: the commit path
// re-reads exactly the malformed record and sees decodeMVCCHead's own error.
func TestPR1655CommitReadCorruptionIsRestorable(t *testing.T) {
	engine, fault := pr1655NewCommitReadFaultEngine(t, false)
	pr1655SeedDoc(t, engine, "test:fixture-head", "before")

	healthy, err := pr1655HeadBytes(engine, "test:fixture-head")
	require.NoError(t, err)
	healthyHead, err := decodeMVCCHead(healthy)
	require.NoError(t, err)
	decodeErr := pr1655CorruptMVCCHead(t, engine, "test:fixture-head")
	require.Error(t, decodeErr)

	corrupted, err := pr1655HeadBytes(engine, "test:fixture-head")
	require.NoError(t, err)
	_, corruptionErr := decodeMVCCHead(corrupted)
	require.Error(t, corruptionErr)
	require.Equal(t, decodeErr.Error(), corruptionErr.Error(), "the stored head is exactly the corrupted record")
	_, headErr := engine.GetNodeCurrentHead("test:fixture-head")
	require.Error(t, headErr, "the commit-path head reload fails on the corrupted record")
	require.ErrorContains(t, headErr, corruptionErr.Error())

	// The saved bytes restore the healthy head, as the cleanup does, so the
	// ordinary MVCC path reads the untouched body again.
	require.NoError(t, pr1655WriteHeadBytes(engine, "test:fixture-head", healthy))
	restored, err := engine.GetNodeCurrentHead("test:fixture-head")
	require.NoError(t, err)
	require.Equal(t, healthyHead.Version, restored.Version)
	node, err := engine.GetNode("test:fixture-head")
	require.NoError(t, err)
	require.Equal(t, "before", node.Properties["body"])
	require.False(t, fault.firedOnce(), "the fixture never arms the fault in this test")
}

// TestPR1655CommitReadCorruptionKeepsPublishedBody proves the corrupted head
// only hides the head record: the body a previous commit stored is unchanged
// and readable straight from its record, which is how the regression checks a
// failed commit published nothing.
func TestPR1655CommitReadCorruptionKeepsPublishedBody(t *testing.T) {
	engine, fault := pr1655NewCommitReadFaultEngine(t, false)
	sequenced := pr1655SeedDoc(t, engine, "test:direct", "before")

	require.Error(t, pr1655CorruptMVCCHead(t, engine, "test:direct"))
	require.Equal(t, "before", pr1655PhysicalBody(t, engine, "test:direct").Properties["body"])
	pr1655AssertNoLeaks(t, engine, sequenced, "injected corruption")
	require.False(t, fault.firedOnce())
}

// TestPR1655CommitReadSeedIsDurableForWriteBehind proves the seed helper the
// write-behind scenarios rely on: the body and its MVCC head are physically
// stored, so a later statement's snapshot validates against them.
func TestPR1655CommitReadSeedIsDurableForWriteBehind(t *testing.T) {
	engine, _ := pr1655NewCommitReadFaultEngine(t, true)
	pr1655SeedDoc(t, engine, "test:seed-durable", "before")

	head, err := pr1655HeadBytes(engine, "test:seed-durable")
	require.NoError(t, err)
	decoded, err := decodeMVCCHead(head)
	require.NoError(t, err)
	require.False(t, decoded.Tombstoned)
	require.Equal(t, "before", pr1655PhysicalBody(t, engine, "test:seed-durable").Properties["body"])
	current, err := engine.GetNodeCurrentHead("test:seed-durable")
	require.NoError(t, err)
	require.Equal(t, decoded.Version, current.Version)
}

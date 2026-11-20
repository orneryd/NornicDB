package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// An observable context lets the test examine discovery at a known record,
// without changing production source or depending on the machine's scan rate.
type reviewR7Context struct {
	context.Context
	observe func()
}

func (c reviewR7Context) Err() error { c.observe(); return c.Context.Err() }

func TestReviewR7PendingShareInsideLongKey(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	base := time.Now().Add(-2 * time.Hour)
	body, err := encodeMVCCNodeRecord(&Node{ID: "test:r7"}, false)
	require.NoError(t, err)
	for id, records := range []int{1, 3, 2000} {
		require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
			for v := 0; v < records; v++ {
				at := base.Add(time.Duration(v) * time.Millisecond)
				if id == 2 { at = time.Now().Add(time.Duration(v) * time.Millisecond) }
				if err := txn.Set(mvccNodeVersionKey(uint64(id+1), MVCCVersion{CommitTimestamp: at, CommitSequence: uint64(v+1)}), body); err != nil { return err }
			}
			return nil
		}))
	}
	logical := append([]byte{prefixMVCCNode}, encodeNumID(2)...)
	const budget = 800 * time.Millisecond
	calls := 0
	remainingDuringDiscovery := 0
	probe := reviewR7Context{Context: t.Context(), observe: func() {
		calls++
		if calls == 12 {
			// Key 2 is pending; key 3's discovery has not finished. Model a
			// slow discovery iteration, then let the loop react to it.
			time.Sleep(3 * budget / mvccPrunePendingShare)
		}
		if calls == 13 {
			// Check on the following iteration, after the walker has had an
			// opportunity to flush once discovery used its share.
			remainingDuringDiscovery = len(mustVersionsUnder(t, b, logical))
		}
	}}
	pass := mvccPrunePass{b: b, ctx: probe, opts: MVCCPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: time.Hour, MaxRuntime: budget}}
	require.NoError(t, pass.walk(prefixMVCCNode, nil))
	t.Logf("after 3 pending shares of further discovery, candidate still had %d records; final deleted=%d", remainingDuringDiscovery, pass.deleted)
	require.Equal(t, 2, remainingDuringDiscovery, "the pending candidate must have committed after one share, even inside the next key")
}

type reviewR7Registry struct {
	mu sync.Mutex
	readers map[string]SnapshotReaderInfo
}

func (r *reviewR7Registry) Register(info SnapshotReaderInfo) (string, func()) {
	r.mu.Lock()
	id := info.ReaderID
	if id == "" { id = fmt.Sprintf("r%d", len(r.readers)+1) }
	if r.readers == nil { r.readers = make(map[string]SnapshotReaderInfo) }
	r.readers[id] = info
	r.mu.Unlock()
	return id, func() { r.mu.Lock(); delete(r.readers,id); r.mu.Unlock() }
}
func (r *reviewR7Registry) Snapshot() []SnapshotReaderInfo {
	r.mu.Lock(); defer r.mu.Unlock()
	var out []SnapshotReaderInfo
	for _, info := range r.readers { out = append(out,info) }
	return out
}
func (r *reviewR7Registry) ActiveCount() int64 { return int64(len(r.Snapshot())) }
func (r *reviewR7Registry) OldestReaderAge() time.Duration { return 0 }
type reviewR7Controller struct {
	*stubLifecycleController
	registry reviewR7Registry
}
func (c *reviewR7Controller) ReaderRegistry() SnapshotReaderRegistry { return &c.registry }
func (c *reviewR7Controller) RegisterSnapshotReader(info SnapshotReaderInfo) func() { _, release := c.registry.Register(info); return release }
func (c *reviewR7Controller) AcquireSnapshotReader(info SnapshotReaderInfo) (func(), error) { return c.RegisterSnapshotReader(info),nil }

func TestReviewR7RegisteringTransactionLosesFreshAsOfCut(t *testing.T) {
	b := createMVCCBadgerEngine(t)
	b.SetLifecycleController(&reviewR7Controller{stubLifecycleController: newStubLifecycleController()})
	id := NodeID("test:r7-register")
	_, err := b.CreateNode(t.Context(), &Node{ID:id, Properties:map[string]any{"v":0}})
	require.NoError(t,err)
	cut, err := b.GetNodeCurrentHead(t.Context(),id)
	require.NoError(t,err)
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t,err)
	defer func(){ _ = tx.Rollback() }()
	for v:=1;v<=5;v++ { require.NoError(t,b.UpdateNode(t.Context(),NodeUpdateRequest{After:&Node{ID:id,Properties:map[string]any{"v":v}}})) }
	release, done := parkPruneInsideDelete(t,b,MVCCPruneOptions{MaxVersionsPerKey:1})
	require.NoError(t,tx.SetNamespace("test"))
	t.Logf("reader registered before commit: %+v",b.mvccPruneReaderFloors().byNamespace)
	release()
	require.NoError(t,<-done)
	pinned, err := tx.GetNode(t.Context(),id)
	require.NoError(t,err)
	require.EqualValues(t,0,pinned.Properties["v"])
	_, err = b.GetNodeVisibleAt(t.Context(),id,cut.Version)
	t.Logf("pinned transaction reads v=0; fresh as-of at its registered cut: %v",err)
	require.NoError(t,err,"a registered transaction must protect its cut for fresh as-of reads")
}

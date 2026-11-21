package storage

// NOR279 callback-reentry coverage.
//
// The ownership correction for NOR-279 detaches record and configuration
// metadata through the package copy owner, CopyValue. That owner runs the
// documented caller-copy protocol: a caller-declared value is asked for its own
// copy, so the caller's code runs inside the store call. The protocol's consumer
// contract allows that code to read the store the value is stored in (CopyValue's
// documentation, and the retention consumer pinned by
// pkg/cypher/profile_transaction_metadata_test.go), and sync.RWMutex is not
// reentrant: its read side also blocks a recursive read as soon as a writer is
// queued. No store may therefore hold its own lock while the protocol runs.
//
// Two bounds cover that placement on every ingress and egress path of the two
// stores, with one supported value implementation and one case table:
//
//   - TestNor279CopyProtocolRunsOutsideStoreLock observes the store's own lock
//     from inside the caller's copy. It is the direct causal check and needs no
//     hang in order to fail.
//   - TestNor279StoreCopyCallbackReentry runs the same cases in an isolated child
//     process, because the user-visible failure is a hang: the caller's public
//     read never returns. A blocked goroutine cannot be joined, so the parent
//     bounds the child and reports the block instead of leaving it behind.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

const (
	nor279ReentryCaseEnv = "NORNIC_NOR279_REENTRY_CASE"
	// nor279ReentryChildBound bounds one isolated child. A correct store finishes
	// these cases in milliseconds; the bound only decides how long a blocked child
	// is allowed to live before the parent reports it.
	nor279ReentryChildBound = 5 * time.Second
	// nor279ReentryWriterBound is how long a delivery case waits for a concurrent
	// writer that needs no state from the copy. Holding the store lock delays it
	// until the copy ends, which is the defect these cases bound.
	nor279ReentryWriterBound = 2 * time.Second
)

// nor279ReentryValue is a supported caller-declared metadata value: it owns the
// caller-copy protocol, returns its own type, and performs the caller's own work
// (a public store read) while its copy is made.
type nor279ReentryValue struct {
	Probe *nor279ReentryProbe
}

func (v nor279ReentryValue) CallerCopy() any {
	v.Probe.observe()
	return v
}

// nor279ReentryProbe carries what the caller's copy observed. read is the
// caller's public store read, writer is a concurrent store write used by the
// delivery cases, and tryLock reports whether the store holds no lock at the
// moment the copy runs.
type nor279ReentryProbe struct {
	read       func() int64
	writer     func()
	tryLock    func() bool
	writerWait time.Duration
	// report enables the child-process markers; the in-process bound reads the
	// recorded flags instead.
	report bool
	// hangWhenLocked keeps the caller's read even when the store still holds its
	// lock, which is how the isolated child demonstrates the user-visible block.
	// The in-process bound leaves it false, so a locked store fails the case at
	// once instead of stalling the package.
	hangWhenLocked bool

	armed         atomic.Bool
	copies        atomic.Int64
	lockHeld      atomic.Bool
	writerBlocked atomic.Bool
	observed      atomic.Int64
}

// arm enables the caller's work for the copy under test. The seed write runs
// before arm, so retention does not perform the delivery work.
func (p *nor279ReentryProbe) arm() { p.armed.Store(true) }

func (p *nor279ReentryProbe) observe() {
	p.copies.Add(1)
	if !p.armed.Load() {
		return
	}
	if p.tryLock != nil && !p.tryLock() {
		p.lockHeld.Store(true)
		p.mark("NOR279_REENTRY_LOCK_HELD")
		if !p.hangWhenLocked {
			return
		}
	}
	if p.writerWait > 0 && p.writer != nil && !p.concurrentWrite() {
		p.writerBlocked.Store(true)
		p.mark("NOR279_REENTRY_WRITER_BLOCKED")
	}
	p.mark("NOR279_REENTRY_READ")
	p.observed.Store(p.read())
}

func (p *nor279ReentryProbe) mark(line string) {
	if p.report {
		fmt.Println(line)
	}
}

// concurrentWrite runs the delivery case's concurrent write once and waits a
// bounded time for it, reporting whether it finished while the copy was running.
// The write needs nothing from the copy, so only a held store lock can delay it.
func (p *nor279ReentryProbe) concurrentWrite() bool {
	done := make(chan struct{})
	go func() {
		p.writer()
		close(done)
	}()

	timer := time.NewTimer(p.writerWait)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// nor279ReentryRecord is the seeded or appended evidence: one record whose
// metadata holds the caller's value and whose other fields are deterministic.
func nor279ReentryRecord(probe *nor279ReentryProbe, materialized bool) EdgeMeta {
	return EdgeMeta{
		EdgeID:       "edge-seed",
		Src:          "node-a",
		Dst:          "node-b",
		Label:        "relates_to",
		SignalType:   string(SignalSimilarity),
		Origin:       "origin-1",
		SessionID:    "session-1",
		Materialized: materialized,
		Timestamp:    nor279EdgeBaseTime,
		Metadata:     map[string]interface{}{"caller": nor279ReentryValue{Probe: probe}},
	}
}

func nor279ReentryNodeConfig(probe *nor279ReentryProbe) NodeConfig {
	return NodeConfig{
		NodeID:      "node",
		PinList:     []string{"pin"},
		MaxOutEdges: 5,
		Metadata:    map[string]interface{}{"caller": nor279ReentryValue{Probe: probe}},
	}
}

// nor279ReentryEdgeStore wires an edge store to the probe: the caller's read is
// the store's public size, the concurrent write appends an independent record,
// and tryLock observes the store's own lock. TryLock never blocks and leaves the
// reader state unchanged, which is why this observation lives in the package.
func nor279ReentryEdgeStore(probe *nor279ReentryProbe) *EdgeMetaStore {
	store := NewEdgeMetaStore()
	probe.read = func() int64 { return int64(store.Size()) }
	probe.writer = func() {
		_ = store.Append(context.Background(), EdgeMeta{
			EdgeID: "writer",
			Src:    "writer-src",
			Dst:    "writer-dst",
			Label:  "relates_to",
		})
	}
	probe.tryLock = func() bool {
		if store.mu.TryLock() {
			store.mu.Unlock()
			return true
		}
		return false
	}
	return store
}

func nor279ReentryNodeConfigStore(probe *nor279ReentryProbe) *NodeConfigStore {
	store := NewNodeConfigStore()
	probe.read = func() int64 { return int64(store.Size()) }
	probe.writer = func() { store.Set(NodeConfig{NodeID: "writer"}) }
	probe.tryLock = func() bool {
		if store.mu.TryLock() {
			store.mu.Unlock()
			return true
		}
		return false
	}
	return store
}

// nor279ReentryCase is one public ingress or egress path. run seeds the store,
// arms the probe when the copy under test is a delivery, and performs the
// operation. want is the store size the caller's read reports afterwards: every
// delivery case lets exactly one concurrent write complete before that read.
type nor279ReentryCase struct {
	name     string
	delivery bool
	want     int64
	run      func(t *testing.T, probe *nor279ReentryProbe)
}

func nor279ReentryCases() []nor279ReentryCase {
	ctx := context.Background()
	return []nor279ReentryCase{
		{
			name: "edge-append-retention",
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				probe.arm()
				require.NoError(t, store.Append(ctx, nor279ReentryRecord(probe, false)))
			},
		},
		{
			name: "edge-import-retention",
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				probe.arm()
				record := nor279ReentryRecord(probe, false)
				require.Equal(t, 1, store.Import([]*EdgeMeta{&record}))
			},
		},
		{
			name:     "edge-gethistory-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				seed := nor279ReentryRecord(probe, false)
				require.NoError(t, store.Append(ctx, seed))
				probe.arm()
				history, err := store.GetHistory(ctx, seed.Src, seed.Dst, seed.Label)
				require.NoError(t, err)
				require.Len(t, history, 1)
			},
		},
		{
			name:     "edge-getlatest-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				seed := nor279ReentryRecord(probe, false)
				require.NoError(t, store.Append(ctx, seed))
				probe.arm()
				latest, err := store.GetLatest(ctx, seed.Src, seed.Dst, seed.Label)
				require.NoError(t, err)
				require.NotNil(t, latest)
			},
		},
		{
			name:     "edge-bysignaltype-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				seed := nor279ReentryRecord(probe, false)
				require.NoError(t, store.Append(ctx, seed))
				probe.arm()
				records, err := store.GetBySignalType(ctx, seed.SignalType, 0)
				require.NoError(t, err)
				require.Len(t, records, 1)
			},
		},
		{
			name:     "edge-bymaterialized-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				seed := nor279ReentryRecord(probe, true)
				require.NoError(t, store.Append(ctx, seed))
				probe.arm()
				records, err := store.GetMaterialized(ctx, 0)
				require.NoError(t, err)
				require.Len(t, records, 1)
			},
		},
		{
			name:     "edge-byorigin-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				seed := nor279ReentryRecord(probe, false)
				require.NoError(t, store.Append(ctx, seed))
				probe.arm()
				records, err := store.GetByOrigin(ctx, seed.Origin, 0)
				require.NoError(t, err)
				require.Len(t, records, 1)
			},
		},
		{
			name:     "edge-export-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryEdgeStore(probe)
				require.NoError(t, store.Append(ctx, nor279ReentryRecord(probe, false)))
				probe.arm()
				require.Len(t, store.Export(), 1)
			},
		},
		{
			name: "node-set-retention",
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryNodeConfigStore(probe)
				probe.arm()
				store.Set(nor279ReentryNodeConfig(probe))
			},
		},
		{
			name: "node-import-retention",
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryNodeConfigStore(probe)
				probe.arm()
				cfg := nor279ReentryNodeConfig(probe)
				require.Equal(t, 1, store.Import([]*NodeConfig{&cfg}))
			},
		},
		{
			name:     "node-get-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryNodeConfigStore(probe)
				store.Set(nor279ReentryNodeConfig(probe))
				probe.arm()
				require.NotNil(t, store.Get("node"))
			},
		},
		{
			name:     "node-getorcreate-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryNodeConfigStore(probe)
				store.Set(nor279ReentryNodeConfig(probe))
				probe.arm()
				require.NotNil(t, store.GetOrCreate("node"))
			},
		},
		{
			name:     "node-export-delivery",
			delivery: true,
			want:     2,
			run: func(t *testing.T, probe *nor279ReentryProbe) {
				store := nor279ReentryNodeConfigStore(probe)
				store.Set(nor279ReentryNodeConfig(probe))
				probe.arm()
				require.Len(t, store.Export(), 1)
			},
		},
	}
}

// TestNor279CopyProtocolRunsOutsideStoreLock is the direct causal bound: while
// the caller's value is copied, the store must own no lock, so the caller may
// read it through the public API. Every case below also performs that read.
func TestNor279CopyProtocolRunsOutsideStoreLock(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	for _, tc := range nor279ReentryCases() {
		t.Run(tc.name, func(t *testing.T) {
			probe := &nor279ReentryProbe{}
			tc.run(t, probe)

			require.Positive(t, probe.copies.Load(), "the store must reach the caller's copy protocol")
			require.False(t, probe.lockHeld.Load(),
				"the store held its lock while it ran the caller's copy protocol; caller code may read the store")
			require.False(t, probe.writerBlocked.Load(),
				"no case may leave a concurrent write behind")
		})
	}
}

// TestNor279StoreCopyCallbackReentry bounds the user-visible consequence in an
// isolated child process: a supported caller value that reads the store during
// its own copy must return instead of hanging inside the store call.
func TestNor279StoreCopyCallbackReentry(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	for _, tc := range nor279ReentryCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), nor279ReentryChildBound)
			defer cancel()

			command := exec.CommandContext(ctx, os.Args[0],
				"-test.run=^TestNor279StoreCopyCallbackReentryChild$", "-test.count=1")
			command.Env = append(os.Environ(), nor279ReentryCaseEnv+"="+tc.name)
			output, err := command.CombinedOutput()
			report := string(output)

			if ctx.Err() != nil {
				t.Fatalf("%s: a supported CallerCopy value that reads the store through its public API never "+
					"returned; the isolated child was stopped after %s:\n%s", tc.name, nor279ReentryChildBound, report)
			}
			require.NotContains(t, report, "NOR279_REENTRY_LOCK_HELD",
				"%s: the store ran the caller's copy protocol while it held its lock", tc.name)
			require.NotContains(t, report, "NOR279_REENTRY_WRITER_BLOCKED",
				"%s: the store held its lock while it ran the caller's copy protocol; a concurrent write could not proceed",
				tc.name)
			require.NoError(t, err, report)
			require.Contains(t, report, fmt.Sprintf("NOR279_REENTRY_RETURNED %d", tc.want),
				"%s: the caller's public store read reported an unexpected state", tc.name)
		})
	}
}

func TestNor279StoreCopyCallbackReentryChild(t *testing.T) {
	name := os.Getenv(nor279ReentryCaseEnv)
	if name == "" {
		return
	}
	defer config.WithEdgeProvenanceEnabled()()

	for _, tc := range nor279ReentryCases() {
		if tc.name != name {
			continue
		}

		probe := &nor279ReentryProbe{report: true, hangWhenLocked: true}
		if tc.delivery {
			probe.writerWait = nor279ReentryWriterBound
		}
		tc.run(t, probe)

		fmt.Printf("NOR279_REENTRY_RETURNED %d\n", probe.observed.Load())
		require.Positive(t, probe.copies.Load(), "the store must reach the caller's copy protocol")
		require.False(t, probe.lockHeld.Load(),
			"the store held its lock while it ran the caller's copy protocol")
		require.False(t, probe.writerBlocked.Load(),
			"the store held its lock while it ran the caller's copy protocol")
		require.Equal(t, tc.want, probe.observed.Load(),
			"the caller's public store read reported an unexpected state")
		return
	}

	t.Fatalf("unknown reentry case %q", name)
}

// TestNor279DisabledProvenanceSkipsCopyProtocol pins the feature-flag no-op: a
// disabled provenance store records nothing and never runs the caller's copy
// protocol, as before this change.
func TestNor279DisabledProvenanceSkipsCopyProtocol(t *testing.T) {
	defer config.WithEdgeProvenanceDisabled()()

	probe := &nor279ReentryProbe{}
	store := nor279ReentryEdgeStore(probe)
	probe.arm()

	require.NoError(t, store.Append(context.Background(), nor279ReentryRecord(probe, false)))
	require.Zero(t, probe.copies.Load(),
		"a disabled provenance store must not run the caller's copy protocol")
	require.Zero(t, store.Size(), "a disabled provenance store must record nothing")
	require.Zero(t, store.Stats().TotalRecords, "a disabled provenance store must report no records")
}

// TestNor279ValueSnapshotsAreTakenUnderTheStoreLock is a bounded check that the
// value fields still come from the lock-held snapshot now that the metadata copy
// runs outside it. A delivered record or config must always be one whole image:
// every exported provenance record keeps its own consistent lifecycle state while
// MarkMaterialized appends, and every delivered config keeps
// CurrentTotalEdges == CurrentOutEdges + CurrentInEdges while the store mutates
// the counters. This is not the race detector (not run in this lane); it fails if
// a future change copies those fields outside the lock and a reader can observe a
// mixed image.
func TestNor279ValueSnapshotsAreTakenUnderTheStoreLock(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	const iterations = 128
	ctx := context.Background()

	edgeMeta := NewEdgeMetaStore()
	require.NoError(t, edgeMeta.Append(ctx, EdgeMeta{
		EdgeID:     "seed",
		Src:        "a",
		Dst:        "b",
		Label:      "relates_to",
		SignalType: string(SignalSimilarity),
		Timestamp:  nor279EdgeBaseTime,
	}))

	materializedDone := make(chan struct{})
	go func() {
		defer close(materializedDone)
		for i := 0; i < iterations; i++ {
			_ = edgeMeta.MarkMaterialized(ctx, "a", "b", "relates_to", "writer")
		}
	}()

	for i := 0; i < iterations; i++ {
		exported := edgeMeta.Export()
		require.NotEmpty(t, exported, "a concurrent append must not hide retained evidence")
		for _, record := range exported {
			require.NotEmpty(t, record.EdgeID, "a delivered record keeps its identity")
			require.False(t, record.Timestamp.IsZero(), "a delivered record keeps its timestamp")
			require.Equal(t, record.Materialized, record.SignalType == "materialization",
				"a delivered record keeps one consistent lifecycle image")
		}
	}
	<-materializedDone

	nodeConfig := NewNodeConfigStore()
	nodeConfig.Set(NodeConfig{NodeID: "node"})

	countersDone := make(chan struct{})
	go func() {
		defer close(countersDone)
		for i := 0; i < iterations; i++ {
			nodeConfig.RecordEdgeCreation("node", "other")
		}
	}()

	for i := 0; i < iterations; i++ {
		cfg := nodeConfig.Get("node")
		require.NotNil(t, cfg)
		require.Zero(t, cfg.CurrentInEdges, "the target node has no incoming increments")
		require.Equal(t, cfg.CurrentTotalEdges, cfg.CurrentOutEdges+cfg.CurrentInEdges,
			"a delivered config keeps one consistent counter image")
	}
	<-countersDone

	require.EqualValues(t, iterations, nodeConfig.Get("node").CurrentOutEdges,
		"every recorded edge creation must be retained")
}

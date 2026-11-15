package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// B10 public regression. A transaction begun before a peer commits a
// relationship creation or deletion answers its count queries from the same
// pinned snapshot its MATCH rows come from. The count-only fast paths answered
// them from the committed derived counters, which hold committed state as of
// the read rather than as of the transaction's begin boundary, so a count
// disagreed with the rows the same transaction returned: it counted the peer's
// relationship the transaction could not read, or left out the one the peer
// deleted and the transaction still read.

// transactionSnapshotCountCase pairs one count statement with the listing that
// returns the rows it must count.
type transactionSnapshotCountCase struct {
	count   string
	listing string
}

// transactionSnapshotCountCases are the count shapes the fast paths serve plus
// one row-served shape, each with its listing.
func transactionSnapshotCountCases() []transactionSnapshotCountCase {
	return []transactionSnapshotCountCase{
		{count: `MATCH ()-[r]->() RETURN count(r)`, listing: `MATCH ()-[r]->() RETURN r`},
		{count: `MATCH ()-[r:T]->() RETURN count(r)`, listing: `MATCH ()-[r:T]->() RETURN r`},
		{count: `MATCH (a:A)-[r:T]->() RETURN count(r)`, listing: `MATCH (a:A)-[r:T]->() RETURN r`},
		{count: `MATCH (n:A) RETURN count(n)`, listing: `MATCH (n:A) RETURN n`},
		{count: `MATCH (n) RETURN count(n)`, listing: `MATCH (n) RETURN n`},
	}
}

const transactionSnapshotCountTyped = `MATCH ()-[r:T]->() RETURN count(r)`
const transactionSnapshotCountTypedRows = `MATCH ()-[r:T]->() RETURN r`

func transactionSnapshotCountScalar(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) int64 {
	t.Helper()
	result, err := exec.Execute(ctx, statement, nil)
	require.NoError(t, err, statement)
	require.Len(t, result.Rows, 1, statement)
	value, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "%s returned %T", statement, result.Rows[0][0])
	return value
}

// newTransactionSnapshotCountFixture seeds the committed state every case
// starts from: one (:A)-[:T]->(:B) relationship and one unconnected :A node.
func newTransactionSnapshotCountFixture(t *testing.T) (*storage.NamespacedEngine, *StorageExecutor, *StorageExecutor) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "snapshot_count")
	writer, peer := NewStorageExecutor(store), NewStorageExecutor(store)
	ctx := context.Background()
	for _, statement := range []string{
		`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
		`CREATE (:A {k: 'a2'})`,
	} {
		_, err := writer.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	return store, writer, peer
}

// TestTransactionSnapshotCountsAgreeWithPeerRelationshipCommits is the reported
// defect: after a peer's committed create or delete, every in-transaction count
// must equal the row count of the listing it counts, and both must equal what
// the transaction read when it began.
func TestTransactionSnapshotCountsAgreeWithPeerRelationshipCommits(t *testing.T) {
	for _, scenario := range []struct {
		name string
		peer func(t *testing.T, ctx context.Context, peer *StorageExecutor)
		// committedRelationships is the :T relationship count a reader outside
		// the transaction answers after the peer's commit.
		committedRelationships int64
	}{
		{
			name: "peer creates a relationship after begin",
			peer: func(t *testing.T, ctx context.Context, peer *StorageExecutor) {
				t.Helper()
				_, err := peer.Execute(ctx, `CREATE (:A {k: 'a3'})-[:T]->(:B {k: 'b3'})`, nil)
				require.NoError(t, err)
			},
			committedRelationships: 2,
		},
		{
			name: "peer deletes the relationship after begin",
			peer: func(t *testing.T, ctx context.Context, peer *StorageExecutor) {
				t.Helper()
				_, err := peer.Execute(ctx, `MATCH ()-[r:T]->() DELETE r`, nil)
				require.NoError(t, err)
			},
			committedRelationships: 0,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, writer, peer := newTransactionSnapshotCountFixture(t)
			ctx := context.Background()
			cases := transactionSnapshotCountCases()

			// The view the transaction must keep, measured before it begins.
			atBegin := make([]int64, len(cases))
			for i, c := range cases {
				atBegin[i] = transactionSnapshotCountScalar(t, writer, ctx, c.count)
			}
			typedAtBegin := transactionSnapshotCountScalar(t, writer, ctx, transactionSnapshotCountTyped)

			_, err := writer.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			rolledBack := false
			defer func() {
				if !rolledBack {
					_, _ = writer.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			scenario.peer(t, ctx, peer)

			// Sensitivity control: the peer's change is committed, so a reader
			// outside the transaction sees it in the counters and in the rows.
			outside := NewStorageExecutor(store)
			require.Equal(t, scenario.committedRelationships,
				transactionSnapshotCountScalar(t, outside, ctx, transactionSnapshotCountTyped),
				"the peer's committed change must be visible outside the transaction")
			require.Equal(t, scenario.committedRelationships,
				rowCount(t, outside, ctx, transactionSnapshotCountTypedRows),
				"the peer's committed change must be visible outside the transaction")

			for i, c := range cases {
				t.Run(c.count, func(t *testing.T) {
					rows := rowCount(t, writer, ctx, c.listing)
					require.Equal(t, atBegin[i], rows, "the listing reads the pinned snapshot")
					require.Equal(t, rows, transactionSnapshotCountScalar(t, writer, ctx, c.count),
						"the count must count the rows this transaction reads")
				})
			}

			// The transaction's own pending relationship still merges on top of
			// the snapshot it reads, and the peer's relationship stays absent.
			_, err = writer.Execute(ctx, `CREATE (:A {k: 'own'})-[:T]->(:B {k: 'ownb'})`, nil)
			require.NoError(t, err)
			ownCount := transactionSnapshotCountScalar(t, writer, ctx, transactionSnapshotCountTyped)
			require.Equal(t, rowCount(t, writer, ctx, transactionSnapshotCountTypedRows), ownCount)
			require.Equal(t, typedAtBegin+1, ownCount, "snapshot rows plus the transaction's own write")

			_, err = writer.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err, "reading a pinned snapshot must not turn the peer's commit into a conflict")
			rolledBack = true
			require.Equal(t, scenario.committedRelationships+1,
				transactionSnapshotCountScalar(t, outside, ctx, transactionSnapshotCountTyped),
				"the transaction's own relationship commits beside the peer's")
		})
	}
}

// TestTransactionSnapshotCountsPreserveGenuineRelationshipConflicts is the
// control for the same fixture: answering counts from the transaction's rows
// must not hide the write-write conflict a peer deletion still causes.
func TestTransactionSnapshotCountsPreserveGenuineRelationshipConflicts(t *testing.T) {
	_, writer, peer := newTransactionSnapshotCountFixture(t)
	ctx := context.Background()

	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = writer.Execute(context.Background(), "ROLLBACK", nil)
		}
	}()

	_, err = peer.Execute(ctx, `MATCH ()-[r:T]->() DELETE r`, nil)
	require.NoError(t, err)

	// The transaction still reads the relationship (pinned snapshot) and its
	// count agrees with that read.
	require.EqualValues(t, 1, transactionSnapshotCountScalar(t, writer, ctx, transactionSnapshotCountTyped))
	require.EqualValues(t, 1, rowCount(t, writer, ctx, transactionSnapshotCountTypedRows))

	// Writing that relationship is the genuine conflict the peer's commit
	// causes, and it is still reported as one.
	_, err = writer.Execute(ctx, `MATCH ()-[r:T]->() DELETE r`, nil)
	require.ErrorIs(t, err, storage.ErrConflict, "a peer-deleted relationship is a conflicting write target")
	require.Contains(t, err.Error(), "changed after transaction start")
	rolledBack = true
	_, _ = writer.Execute(ctx, "ROLLBACK", nil)
}

// peerPublishDuringCountRead is a transparent decorator around the store a
// statement reads through. Once armed for one count family, its first read of
// that family publishes the peer's committed change and then delegates to the
// counter, so the commit lands exactly after the transaction's eligibility
// check and before the counter it would have answered from. That is the window
// a live committed counter cannot close on its own: checking eligibility before
// the read is not enough, because the counter is read after it.
type peerPublishDuringCountRead struct {
	storage.Engine
	namespace string
	// family names the count read that publishes the peer's change.
	family string
	// publish applies the peer's committed change.
	publish func() error
	armed   bool
	// read is the count read that published, and err is what publishing
	// returned.
	read string
	err  error
}

// Namespace keeps the store's database scope visible to the transaction, which
// only answers from the committed counters while they describe that scope.
func (p *peerPublishDuringCountRead) Namespace() string { return p.namespace }

// GetInnerEngine is the canonical EngineUnwrapper accessor.
func (p *peerPublishDuringCountRead) GetInnerEngine() storage.Engine { return p.Engine }

// publishBeforeCount publishes the peer's change once, on the armed family's
// first count read.
func (p *peerPublishDuringCountRead) publishBeforeCount(family string) {
	if !p.armed || p.read != "" || family != p.family {
		return
	}
	p.read = family
	p.err = p.publish()
}

func (p *peerPublishDuringCountRead) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	p.publishBeforeCount(countReadCountStore)
	if reader, ok := p.Engine.(storage.CountStoreReadEngine); ok {
		return reader.ReadCountStore(ctx, request)
	}
	return storage.CountStoreRead{}, nil
}

func (p *peerPublishDuringCountRead) NodeCountByLabel(ctx context.Context, label string) (int64, error) {
	p.publishBeforeCount(countReadNodeLabel)
	counter, ok := p.Engine.(storage.LabelStatsEngine)
	if !ok {
		return 0, errSnapshotCountNoLabelCounter
	}
	return counter.NodeCountByLabel(ctx, label)
}

func (p *peerPublishDuringCountRead) EdgeCountByType(ctx context.Context, edgeType string) (int64, error) {
	p.publishBeforeCount(countReadEdgeType)
	return p.Engine.EdgeCountByType(ctx, edgeType)
}

func (p *peerPublishDuringCountRead) EdgeCountByStartLabel(ctx context.Context, label, edgeType string) (int64, error) {
	p.publishBeforeCount(countReadEdgeStartLabel)
	return p.Engine.EdgeCountByStartLabel(ctx, label, edgeType)
}

func (p *peerPublishDuringCountRead) NodeCount(ctx context.Context) (int64, error) {
	p.publishBeforeCount(countReadNodeTotal)
	return p.Engine.NodeCount(ctx)
}

func (p *peerPublishDuringCountRead) EdgeCount(ctx context.Context) (int64, error) {
	p.publishBeforeCount(countReadEdgeTotal)
	return p.Engine.EdgeCount(ctx)
}

// errSnapshotCountNoLabelCounter is the fixture's answer when the decorated
// store keeps no label counter.
var errSnapshotCountNoLabelCounter = errors.New("the fixture store keeps no label counter")

// The count families a statement can be answered from, as the decorator names
// them.
const (
	countReadCountStore     = "count store"
	countReadNodeLabel      = "node label count"
	countReadEdgeType       = "relationship type count"
	countReadEdgeStartLabel = "relationship start label count"
	countReadNodeTotal      = "total node count"
	countReadEdgeTotal      = "total relationship count"
)

// TestTransactionCountsRejectPeerCommitPublishedDuringCounterRead is the
// fail-first regression for the reported race: the peer commits while the
// transaction's count is being read. The committed counter then holds the
// peer's rows, which the transaction's pinned snapshot does not, so an answer
// taken from it disagrees with the listing the same statement counts. The count
// must stay pinned to the transaction's rows.
func TestTransactionCountsRejectPeerCommitPublishedDuringCounterRead(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		count   string
		listing string
		// peer is the committed change published during the count read.
		peer string
	}{
		{
			name:    "label count",
			count:   `MATCH (n:A) RETURN count(n)`,
			listing: `MATCH (n:A) RETURN n`,
			peer:    `CREATE (:A {k: 'peer'})`,
		},
		{
			name:    "relationship type count",
			count:   `MATCH ()-[r:T]->() RETURN count(r)`,
			listing: `MATCH ()-[r:T]->() RETURN r`,
			peer:    `MATCH (a:A {k: 'a1'}), (b:B {k: 'b1'}) CREATE (a)-[:T]->(b)`,
		},
		{
			name:    "endpoint label count",
			count:   `MATCH (a:A)-[r:T]->() RETURN count(r)`,
			listing: `MATCH (a:A)-[r:T]->() RETURN r`,
			peer:    `CREATE (:A {k: 'peer'})-[:T]->(:B {k: 'peerb'})`,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "snapshot_count_race")
			seed := NewStorageExecutor(store)
			ctx := context.Background()
			for _, statement := range []string{
				`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
				`CREATE (:A {k: 'a2'})`,
			} {
				_, err := seed.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			probe := &peerPublishDuringCountRead{
				Engine:    store,
				namespace: "snapshot_count_race",
				family:    countReadCountStore,
				publish: func() error {
					_, err := NewStorageExecutor(store).Execute(context.Background(), scenario.peer, nil)
					return err
				},
			}
			writer := NewStorageExecutor(probe)

			_, err = writer.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			rolledBack := false
			defer func() {
				if !rolledBack {
					_, _ = writer.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			// The rows the transaction reads, before the peer publishes: the same
			// rows a reader outside the transaction lists before it begins.
			atBegin := rowCount(t, seed, ctx, scenario.listing)
			rows := rowCount(t, writer, ctx, scenario.listing)
			require.Equal(t, atBegin, rows, "the listing reads the pinned snapshot")

			probe.armed = true
			count := transactionSnapshotCountScalar(t, writer, ctx, scenario.count)
			require.NoError(t, probe.err, "the peer's committed change must apply")
			require.Equal(t, countReadCountStore, probe.read,
				"the peer must commit inside the transaction's own count read")

			// Sensitivity control: the peer's change is committed, so a reader
			// outside the transaction counts it in the counters and in the rows.
			outside := NewStorageExecutor(store)
			require.Equal(t, rows+1, transactionSnapshotCountScalar(t, outside, ctx, scenario.count),
				"the peer's committed row is counted outside the transaction")
			require.Equal(t, rows+1, rowCount(t, outside, ctx, scenario.listing),
				"the peer's committed row is listed outside the transaction")

			require.Equal(t, rows, count,
				"the count must count the rows this transaction reads, not the peer's committed counter")
			require.Equal(t, rows, rowCount(t, writer, ctx, scenario.listing),
				"the pinned listing is unchanged")
		})
	}
}

// snapshotCountRaceWrapper is a real native transaction over the fixture, read
// through a decorator that publishes the peer's change during one counter read.
type snapshotCountRaceWrapper struct {
	wrapper *transactionStorageWrapper
	probe   *peerPublishDuringCountRead
	store   storage.Engine
}

// newSnapshotCountRaceWrapper seeds the fixture, opens a transaction pinned to
// the fixture's namespace, and arms the decorator for one count family.
func newSnapshotCountRaceWrapper(t *testing.T, family, peer string) snapshotCountRaceWrapper {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "snapshot_count_families")
	ctx := context.Background()
	seed := NewStorageExecutor(store)
	for _, statement := range []string{
		`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
		`CREATE (:A {k: 'a2'})`,
	} {
		_, err := seed.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	probe := &peerPublishDuringCountRead{
		Engine:    store,
		namespace: "snapshot_count_families",
		family:    family,
		publish: func() error {
			_, err := NewStorageExecutor(store).Execute(context.Background(), peer, nil)
			return err
		},
	}
	tx, err := base.BeginTransaction(ctx, storage.OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("snapshot_count_families"))
	wrapper := &transactionStorageWrapper{
		tx: tx, underlying: probe, namespace: "snapshot_count_families", separator: ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	wrapper.initNamespacePrefix()
	probe.armed = true
	return snapshotCountRaceWrapper{wrapper: wrapper, probe: probe, store: store}
}

// TestTransactionCountFamiliesRejectPeerCommitPublishedDuringCounterRead covers
// the count families no Cypher count statement reaches (the label, type,
// endpoint-label and total counters the executor exposes as storage.Engine
// counts): each is read between two eligibility answers, so a peer commit
// published during the read leaves the answer unproven and the transaction's
// own rows decide it.
func TestTransactionCountFamiliesRejectPeerCommitPublishedDuringCounterRead(t *testing.T) {
	createEdge := `MATCH (a:A {k: 'a1'}), (b:B {k: 'b1'}) CREATE (a)-[:T]->(b)`
	for _, scenario := range []struct {
		name string
		// family is the count read the peer's commit lands inside.
		family string
		// peer is the committed change published during that read.
		peer string
		// counted answers the family's count inside the transaction, and rows
		// counts the same rows from the transaction's own reads.
		counted func(ctx context.Context, w *transactionStorageWrapper) (int64, error)
		rows    func(ctx context.Context, w *transactionStorageWrapper) (int64, error)
		// committed counts the same rows outside the transaction, after the
		// peer's commit.
		committed func(ctx context.Context, store storage.Engine) (int64, error)
	}{
		{
			name:   "node count by label",
			family: countReadNodeLabel,
			peer:   `CREATE (:A {k: 'peer'})`,
			counted: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.NodeCountByLabel(ctx, "A")
			},
			rows: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.nodeCountByLabelScan(ctx, "A")
			},
			committed: func(ctx context.Context, store storage.Engine) (int64, error) {
				counter, ok := store.(storage.LabelStatsEngine)
				if !ok {
					return 0, errSnapshotCountNoLabelCounter
				}
				return counter.NodeCountByLabel(ctx, "A")
			},
		},
		{
			name:   "relationship count by type",
			family: countReadEdgeType,
			peer:   createEdge,
			counted: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.EdgeCountByType(ctx, "T")
			},
			rows: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.edgeCountByTypeScan(ctx, "T")
			},
			committed: func(ctx context.Context, store storage.Engine) (int64, error) {
				return store.EdgeCountByType(ctx, "T")
			},
		},
		{
			name:   "relationship count by start label",
			family: countReadEdgeStartLabel,
			peer:   `CREATE (:A {k: 'peer'})-[:T]->(:B {k: 'peerb'})`,
			counted: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.EdgeCountByStartLabel(ctx, "A", "T")
			},
			rows: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.edgeCountByEndpointLabelScan(ctx, "A", "T", true)
			},
			committed: func(ctx context.Context, store storage.Engine) (int64, error) {
				return store.EdgeCountByStartLabel(ctx, "A", "T")
			},
		},
		{
			name:   "total node count",
			family: countReadNodeTotal,
			peer:   `CREATE (:A {k: 'peer'})`,
			counted: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.NodeCount(ctx)
			},
			rows: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return countOfRead(w.AllNodes(ctx))
			},
			committed: func(ctx context.Context, store storage.Engine) (int64, error) {
				return store.NodeCount(ctx)
			},
		},
		{
			name:   "total relationship count",
			family: countReadEdgeTotal,
			peer:   createEdge,
			counted: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return w.EdgeCount(ctx)
			},
			rows: func(ctx context.Context, w *transactionStorageWrapper) (int64, error) {
				return countOfRead(w.AllEdges(ctx))
			},
			committed: func(ctx context.Context, store storage.Engine) (int64, error) {
				return store.EdgeCount(ctx)
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newSnapshotCountRaceWrapper(t, scenario.family, scenario.peer)
			ctx := context.Background()

			count, err := scenario.counted(ctx, fixture.wrapper)
			require.NoError(t, err)
			require.NoError(t, fixture.probe.err, "the peer's committed change must apply")
			require.Equal(t, scenario.family, fixture.probe.read,
				"the peer must commit inside this family's own counter read")

			rows, err := scenario.rows(ctx, fixture.wrapper)
			require.NoError(t, err)
			require.Equal(t, rows, count,
				"the count must count the rows this transaction reads, not the peer's committed counter")

			// Sensitivity control: the peer's change is committed, so a reader
			// outside the transaction counts it.
			committed, err := scenario.committed(ctx, fixture.store)
			require.NoError(t, err)
			require.Equal(t, rows+1, committed,
				"the peer's committed row is counted outside the transaction")
		})
	}
}

// TestTransactionCountFastPathStaysCounterServed is the non-concurrent control
// for the before/read/after certification: while no peer commit moves the
// namespace past the transaction's read version, the committed counters still
// describe its rows, so the count is served by the counter and answers exactly
// as before. The certification only decides the concurrent case.
func TestTransactionCountFastPathStaysCounterServed(t *testing.T) {
	store, _, _ := newTransactionSnapshotCountFixture(t)
	ctx := context.Background()
	probe := &transactionScopedCountProbe{NamespacedEngine: store}
	writer := NewStorageExecutor(probe)

	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = writer.Execute(context.Background(), "ROLLBACK", nil)
		}
	}()

	collector := newProfileRuntimeCollector()
	result, err := writer.Execute(withProfileRuntimeCollector(ctx, collector), `MATCH (n:A) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	require.Equal(t, 1, probe.countReads, "the count reads the committed label counter once")
	require.Len(t, profileAccessesOfKind(collector, profileKindNodeCountFromCountStore), 1,
		"the committed counter answers the count, so no row is materialized")

	// The same holds for the relationship counters, typed and positional.
	for query, want := range map[string]int64{
		`MATCH ()-[r:T]->() RETURN count(r)`:    1,
		`MATCH (a:A)-[r:T]->() RETURN count(r)`: 1,
	} {
		probe.countReads = 0
		collector = newProfileRuntimeCollector()
		result, err = writer.Execute(withProfileRuntimeCollector(ctx, collector), query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		require.Equal(t, 1, probe.countReads, query)
		require.Len(t, profileAccessesOfKind(collector, profileKindRelationshipCountFromCountStore), 1, query)
	}

	_, err = writer.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	rolledBack = true
}

// TestWriteBehindTransactionCountFastPathStaysCounterServed is the same control
// for the write-behind reader: its reads are read-committed, so the committed
// counters always describe them and the count stays counter-served.
func TestWriteBehindTransactionCountFastPathStaysCounterServed(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour, // the buffer never drains mid-test
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "snapshot_count_write_behind")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, statement := range []string{
		`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
		`CREATE (:A {k: 'a2'})`,
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	probe := &transactionScopedCountProbe{NamespacedEngine: store}
	writer := NewStorageExecutor(probe)

	_, err = writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = writer.Execute(context.Background(), "ROLLBACK", nil)
		}
	}()

	collector := newProfileRuntimeCollector()
	result, err := writer.Execute(withProfileRuntimeCollector(ctx, collector), `MATCH (n:A) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	require.Equal(t, 1, probe.countReads, "the count reads the committed label counter once")
	require.Len(t, profileAccessesOfKind(collector, profileKindNodeCountFromCountStore), 1,
		"the write-behind reader is read-committed, so the counter answers it")

	_, err = writer.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	rolledBack = true
}

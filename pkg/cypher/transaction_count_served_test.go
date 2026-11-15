package cypher

import (
	"context"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// B10 count-served regression. An explicit transaction's counts must equal the
// rows the same transaction reads after a peer commits into its namespace, and
// they must not materialize that population: the counters are persisted per
// snapshot, so the transaction reads its own. The earlier correction answered
// these counts from the rows instead - correct, but it scanned the counted
// population every time a peer committed.
//
// Each case therefore establishes three things: the count equals the listing's
// row count, the native counter kernel consumed the transaction's snapshot
// capability, and the statement recorded no population read at all.

// transactionScopedCountProbe is the store a statement reads through. It
// records the count-store reads the transaction made, how many of them the
// native engine answered at the transaction's snapshot, and it can answer a
// count-store read the way a reader that does not understand the capability
// would.
type transactionScopedCountProbe struct {
	*storage.NamespacedEngine
	countReads int
	consumed   int
	// dropScope answers count-store reads without the transaction's snapshot
	// capability, as a reader that ignores it does.
	dropScope bool
	// dropped records that a read reached this probe carrying a capability.
	dropped bool
	// cancelDuringCount cancels the statement's context inside the first
	// counter read, after the transaction has planned it.
	cancelDuringCount func()
	cancelCalls       int
	// countErr fails every count-store read this probe receives, as a custom
	// reader whose own read failed does.
	countErr error
	// decline answers a count-store read with no described answer, as a reader
	// that refuses the request does.
	decline bool
}

// GetInnerEngine is this decorator's inner engine: the namespaced engine it
// wraps, not the engine beneath it. BEGIN discovers the transaction's namespace
// from the *storage.NamespacedEngine it meets while unwrapping, so the promoted
// accessor would skip the layer and leave the transaction unpinned.
func (p *transactionScopedCountProbe) GetInnerEngine() storage.Engine { return p.NamespacedEngine }

func (p *transactionScopedCountProbe) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	p.countReads++
	if p.cancelDuringCount != nil && p.cancelCalls == 0 {
		p.cancelCalls++
		p.cancelDuringCount()
	}
	if p.countErr != nil {
		return storage.CountStoreRead{}, p.countErr
	}
	if p.decline {
		return storage.CountStoreRead{}, nil
	}
	if p.dropScope && request.Scope != nil {
		p.dropped = true
		request.Scope = nil
	}
	answer, err := p.NamespacedEngine.ReadCountStore(ctx, request)
	if request.Scope.DescribesSnapshot() {
		p.consumed++
	}
	return answer, err
}

// transactionScopedCountCase pairs one count statement with the listing that
// returns the rows it must count and the profile access a counter-served answer
// records.
type transactionScopedCountCase struct {
	count   string
	listing string
	kind    string
}

// transactionScopedCountCases are the count shapes a persisted snapshot counter
// answers: a label count, a typed and an untyped relationship count (the
// per-type counters sum), and a positional endpoint-label count. The total node
// and relationship counts are not among them: they have no snapshot-addressable
// persisted counter and remain outside this contract.
func transactionScopedCountCases() []transactionScopedCountCase {
	return []transactionScopedCountCase{
		{count: `MATCH (n:A) RETURN count(n)`, listing: `MATCH (n:A) RETURN n`, kind: profileKindNodeCountFromCountStore},
		{count: `MATCH ()-[r:T]->() RETURN count(r)`, listing: `MATCH ()-[r:T]->() RETURN r`, kind: profileKindRelationshipCountFromCountStore},
		{count: `MATCH ()-[r]->() RETURN count(r)`, listing: `MATCH ()-[r]->() RETURN r`, kind: profileKindRelationshipCountFromCountStore},
		{count: `MATCH (a:A)-[r:T]->() RETURN count(r)`, listing: `MATCH (a:A)-[r:T]->() RETURN r`, kind: profileKindRelationshipCountFromCountStore},
	}
}

// transactionScopedCountFixture seeds one (:A)-[:T]->(:B) relationship and one
// unconnected :A node through the probe, so every count the transaction runs
// below is observed.
type transactionScopedCountFixture struct {
	store  *storage.NamespacedEngine
	probe  *transactionScopedCountProbe
	seed   *StorageExecutor
	writer *StorageExecutor
}

func newTransactionScopedCountFixture(t *testing.T) transactionScopedCountFixture {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "scoped_count_served")
	probe := &transactionScopedCountProbe{NamespacedEngine: store}
	ctx := context.Background()
	seed := NewStorageExecutor(store)
	for _, statement := range []string{
		`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
		`CREATE (:A {k: 'a2'})`,
	} {
		_, err := seed.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	return transactionScopedCountFixture{store: store, probe: probe, seed: seed, writer: NewStorageExecutor(probe)}
}

// beginInTransaction opens the fixture's transaction and rolls it back unless
// the case commits it.
func (f transactionScopedCountFixture) beginInTransaction(t *testing.T, ctx context.Context) (commit func()) {
	t.Helper()
	_, err := f.writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	finished := false
	t.Cleanup(func() {
		if !finished {
			_, _ = f.writer.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	return func() {
		_, err := f.writer.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		finished = true
	}
}

// transactionScopedCountReads counts one statement's counter reads and profile
// accesses, so a case can tell a counter-served count from one that materialized
// the rows instead.
func transactionScopedCountReads(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) (int64, []profileAccessObservation) {
	t.Helper()
	collector := newProfileRuntimeCollector()
	result, err := exec.Execute(withProfileRuntimeCollector(ctx, collector), statement, nil)
	require.NoError(t, err, statement)
	require.Len(t, result.Rows, 1, statement)
	value, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "%s returned %T", statement, result.Rows[0][0])
	return value, collector.accessObservation()
}

// TestTransactionCountsAfterPeerCommitStayCounterServed is the reported defect's
// corrected contract: after a peer creates or deletes committed rows, every
// count shape whose counter is persisted per snapshot counts exactly the rows
// the transaction reads, from that counter, without reading the population.
func TestTransactionCountsAfterPeerCommitStayCounterServed(t *testing.T) {
	for _, peer := range []struct {
		name      string
		statement string
	}{
		{name: "peer creates", statement: `CREATE (:A {k: 'peer'})-[:T]->(:B {k: 'peerb'})`},
		{name: "peer deletes", statement: `MATCH ()-[r:T]->() DELETE r`},
	} {
		for _, c := range transactionScopedCountCases() {
			t.Run(peer.name+"/"+c.count, func(t *testing.T) {
				fixture := newTransactionScopedCountFixture(t)
				ctx := context.Background()
				atBegin := rowCount(t, fixture.writer, ctx, c.listing)
				commit := fixture.beginInTransaction(t, ctx)
				_, err := fixture.seed.Execute(ctx, peer.statement, nil)
				require.NoError(t, err)

				fixture.probe.countReads, fixture.probe.consumed = 0, 0
				count, accesses := transactionScopedCountReads(t, fixture.writer, ctx, c.count)
				rows := rowCount(t, fixture.writer, ctx, c.listing)

				require.Equal(t, atBegin, rows, "the listing reads the pinned snapshot")
				require.Equal(t, rows, count, "the count must count the rows this transaction reads")
				require.Equal(t, 1, fixture.probe.countReads, "one counter read answers the count")
				require.Equal(t, 1, fixture.probe.consumed,
					"the native kernel must answer the counter at the transaction's own snapshot")
				require.NotEmpty(t, accesses)
				for _, access := range accesses {
					require.Equal(t, c.kind, access.Kind,
						"a counter-served count records no population read")
				}
				commit()
			})
		}
	}
}

// TestTransactionOwnWriteCountsStayCounterServed covers the transaction's own
// staged writes: the snapshot counter plus the staged contribution keeps a label
// or type count in the counter keyspace, and a positional count keeps its
// existing row contract because its staged deltas are derived at commit.
func TestTransactionOwnWriteCountsStayCounterServed(t *testing.T) {
	fixture := newTransactionScopedCountFixture(t)
	ctx := context.Background()
	atBegin := map[string]int64{}
	for _, listing := range []string{
		`MATCH ()-[r:T]->() RETURN r`,
		`MATCH (n:A) RETURN n`,
		`MATCH (a:A)-[r:T]->() RETURN r`,
	} {
		atBegin[listing] = rowCount(t, fixture.seed, ctx, listing)
	}
	commit := fixture.beginInTransaction(t, ctx)
	_, err := fixture.writer.Execute(ctx, `CREATE (:A {k: 'own'})-[:T]->(:B {k: 'ownb'})`, nil)
	require.NoError(t, err)

	for _, c := range []transactionScopedCountCase{
		{count: `MATCH ()-[r:T]->() RETURN count(r)`, listing: `MATCH ()-[r:T]->() RETURN r`, kind: profileKindRelationshipCountFromCountStore},
		{count: `MATCH (n:A) RETURN count(n)`, listing: `MATCH (n:A) RETURN n`, kind: profileKindNodeCountFromCountStore},
	} {
		t.Run(c.count, func(t *testing.T) {
			fixture.probe.countReads, fixture.probe.consumed = 0, 0
			count, accesses := transactionScopedCountReads(t, fixture.writer, ctx, c.count)
			rows := rowCount(t, fixture.writer, ctx, c.listing)
			require.Equal(t, atBegin[c.listing]+1, rows, "the transaction reads its snapshot plus its own write")
			require.Equal(t, rows, count)
			require.Equal(t, 1, fixture.probe.countReads)
			require.Equal(t, 1, fixture.probe.consumed,
				"the snapshot counter answers the count beside the staged delta")
			require.NotEmpty(t, accesses)
			for _, access := range accesses {
				require.Equal(t, c.kind, access.Kind)
			}
		})
	}

	// The positional (label, type) counters are derived at commit, so a staged
	// change keeps that count on the rows it reads - the existing contract, not
	// a regression of this correction.
	t.Run("positional count after an own write counts the rows", func(t *testing.T) {
		count := transactionSnapshotCountScalar(t, fixture.writer, ctx, `MATCH (a:A)-[r:T]->() RETURN count(r)`)
		rows := rowCount(t, fixture.writer, ctx, `MATCH (a:A)-[r:T]->() RETURN r`)
		require.Equal(t, atBegin[`MATCH (a:A)-[r:T]->() RETURN r`]+1, rows)
		require.Equal(t, rows, count)
	})
	commit()
}

// TestTransactionHiddenLabelCountsStayRowBacked keeps the visibility half: a
// hidden node the counters include makes the counter unusable for the read, so
// the count is answered by the rows the transaction reads, peer commit or not.
func TestTransactionHiddenLabelCountsStayRowBacked(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "scoped_count_hidden")
	probe := &transactionScopedCountProbe{NamespacedEngine: store}
	ctx := context.Background()
	seed := NewStorageExecutor(store)
	for _, statement := range []string{
		`CREATE (:A {k: 'visible'})`,
		`CREATE (:A:_internal {k: 'hidden'})`,
	} {
		_, err := seed.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
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

	// The raw label counter counts both nodes: the hidden one is not a row of
	// the visible read.
	committed, err := base.NodeCountByLabelInNamespace(ctx, "scoped_count_hidden", "A")
	require.NoError(t, err)
	require.EqualValues(t, 2, committed)

	count := transactionSnapshotCountScalar(t, writer, ctx, `MATCH (n:A) RETURN count(n)`)
	rows := rowCount(t, writer, ctx, `MATCH (n:A) RETURN n`)
	require.EqualValues(t, 1, rows)
	require.Equal(t, rows, count, "the hidden node is not a row of this read")

	_, err = writer.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	rolledBack = true
}

// TestTransactionCountIsNotCertifiedByAReaderThatIgnoresTheScope is the
// consumption proof: a reader that answers the count-store request itself, or
// strips the transaction's capability, cannot certify live counters as the
// transaction's own, so the count stays with the rows it reads.
func TestTransactionCountIsNotCertifiedByAReaderThatIgnoresTheScope(t *testing.T) {
	fixture := newTransactionScopedCountFixture(t)
	fixture.probe.dropScope = true
	ctx := context.Background()
	commit := fixture.beginInTransaction(t, ctx)
	atBegin := rowCount(t, fixture.writer, ctx, `MATCH ()-[r:T]->() RETURN r`)
	_, err := fixture.seed.Execute(ctx, `CREATE (:A {k: 'peer'})-[:T]->(:B {k: 'peerb'})`, nil)
	require.NoError(t, err)

	count := transactionSnapshotCountScalar(t, fixture.writer, ctx, `MATCH ()-[r:T]->() RETURN count(r)`)
	rows := rowCount(t, fixture.writer, ctx, `MATCH ()-[r:T]->() RETURN r`)
	require.True(t, fixture.probe.dropped, "the reader must have seen the capability it ignored")
	require.Equal(t, 0, fixture.probe.consumed, "an ignored capability certifies nothing")
	require.Equal(t, atBegin, rows)
	require.Equal(t, rows, count, "the live counter the reader returned counts the peer's row")
	commit()
}

// TestTransactionCountCancellationDuringCounterRead keeps cancellation: a count
// whose context is canceled while its counter is read fails as canceled instead
// of returning a value.
func TestTransactionCountCancellationDuringCounterRead(t *testing.T) {
	fixture := newTransactionScopedCountFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commit := fixture.beginInTransaction(t, ctx)
	_ = commit
	// Commit cleanup runs through the deferred rollback with a fresh context, so
	// a canceled statement cannot leave the transaction open.
	finished := false
	defer func() {
		if !finished {
			_, _ = fixture.writer.Execute(context.Background(), "ROLLBACK", nil)
		}
	}()
	fixture.probe.cancelDuringCount = cancel

	_, err := fixture.writer.Execute(ctx, `MATCH (n:A) RETURN count(n)`, nil)
	require.ErrorIs(t, err, context.Canceled, "a canceled count read reports its cancellation")
	require.Equal(t, 1, fixture.probe.cancelCalls, "the cancellation lands inside the counter read")
	finished = true
	_, _ = fixture.writer.Execute(context.Background(), "ROLLBACK", nil)
}

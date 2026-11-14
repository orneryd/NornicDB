package multidb_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Bounded observation windows. The test never sequences work by sleeping: the
// only timers are (a) how long a write that normally returns in milliseconds
// may stay in flight before it is treated as parked, (b) how long the search
// for the parked goroutine may take, and (c) the per-call client deadline.
const (
	mergeAdmissionWriteWindow  = 2 * time.Second
	mergeAdmissionParkWindow   = 10 * time.Second
	mergeAdmissionCallTimeout  = 15 * time.Second
	mergeAdmissionPollInterval = 2 * time.Millisecond
)

// mergeAdmissionFixture is the report entry 9 / B09 chain: a Badger store behind the
// production WAL journal, the production database manager, and a public Bolt
// server serving that manager to the Neo4j Go driver.
type mergeAdmissionFixture struct {
	ctx      context.Context
	manager  *multidb.DatabaseManager
	native   *storage.BadgerEngine
	driver   neo4jdriver.DriverWithContext
	database string
}

func newMergeAdmissionFixture(t *testing.T) *mergeAdmissionFixture {
	t.Helper()
	ctx := context.Background()

	badger, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)

	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	manager, err := multidb.NewDatabaseManager(ctx, storage.NewWALEngine(badger, wal), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "expected a TCP listener with an assigned port, got %T", listener.Addr())
	require.NotZero(t, tcpAddr.Port)

	server := bolt.NewWithDatabaseManager(&bolt.Config{
		Port:            0,
		MaxConnections:  64,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, nil, manager)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		if closeErr := server.Close(); closeErr != nil {
			t.Errorf("close Bolt server: %v", closeErr)
		}
		if serveErr := <-served; serveErr != nil {
			t.Errorf("serve Bolt: %v", serveErr)
		}
	})

	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://%s", listener.Addr().String()), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	require.NoError(t, driver.VerifyConnectivity(ctx))

	return &mergeAdmissionFixture{
		ctx:      ctx,
		manager:  manager,
		native:   badger,
		driver:   driver,
		database: manager.DefaultDatabaseName(),
	}
}

func (f *mergeAdmissionFixture) sessionConfig() neo4jdriver.SessionConfig {
	return neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: f.database,
	}
}

// exec runs one auto-commit statement that must succeed (fixture seed).
func (f *mergeAdmissionFixture) exec(t *testing.T, query string, params map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, mergeAdmissionCallTimeout)
	defer cancel()
	session := f.driver.NewSession(ctx, f.sessionConfig())
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, query, params)
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
}

func (f *mergeAdmissionFixture) seed(t *testing.T) {
	t.Helper()
	f.exec(t, "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE", nil)
	f.exec(t, "UNWIND range(0, 19) AS i CREATE (:Review {id: 'review-' + toString(i), v: -1, keep: 'seeded'})", nil)
}

// properties reads the whole property map of the node with the given id, or nil
// when no such node exists. Reads carry no pending write, so they never wait on
// the write-admission gate.
func (f *mergeAdmissionFixture) properties(t *testing.T, id string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, mergeAdmissionCallTimeout)
	defer cancel()
	session := f.driver.NewSession(ctx, f.sessionConfig())
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, "MATCH (n:Review {id: $id}) RETURN properties(n) AS props", map[string]any{"id": id})
	require.NoError(t, err)
	records, err := result.Collect(ctx)
	require.NoError(t, err)
	if len(records) == 0 {
		return nil
	}
	value, ok := records[0].Get("props")
	if !ok {
		return nil
	}
	props, _ := value.(map[string]any)
	return props
}

func (f *mergeAdmissionFixture) takeLease(t *testing.T, exclusive bool) *mergeAdmissionLease {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, mergeAdmissionCallTimeout)
	defer cancel()
	release, err := f.manager.AcquireWriteAdmissionLeaseForTest(ctx, f.database, exclusive)
	require.NoError(t, err, "the test could not take the production write-admission gate")
	return &mergeAdmissionLease{release: release}
}

type mergeAdmissionLease struct {
	release  func()
	released bool
}

func (l *mergeAdmissionLease) Release() {
	if l == nil || l.released {
		return
	}
	l.released = true
	l.release()
}

// mergeAdmissionWrite is one public Bolt write.
type mergeAdmissionWrite struct {
	name  string
	id    string
	value int64
	tag   string
	run   func(ctx context.Context) error
}

const mergeAdmissionMergeStatement = "MERGE (r:Review {id: $id}) SET r.v = $v, r.w = $w"

// mergeExplicit runs the report statement as an explicit transaction: BEGIN,
// RUN, CONSUME, COMMIT.
func (f *mergeAdmissionFixture) mergeExplicit(ctx context.Context, id string, value int64, tag string) error {
	session := f.driver.NewSession(ctx, f.sessionConfig())
	defer func() { _ = session.Close(context.Background()) }()
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	result, err := tx.Run(ctx, mergeAdmissionMergeStatement, map[string]any{"id": id, "v": value, "w": tag})
	if err != nil {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("run: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("consume: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// mergeAuto runs the report statement as one auto-commit statement.
func (f *mergeAdmissionFixture) mergeAuto(ctx context.Context, id string, value int64, tag string) error {
	session := f.driver.NewSession(ctx, f.sessionConfig())
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, mergeAdmissionMergeStatement, map[string]any{"id": id, "v": value, "w": tag})
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	return nil
}

// matchSetExplicit is the control: the same write without MERGE.
func (f *mergeAdmissionFixture) matchSetExplicit(ctx context.Context, id string, value int64, tag string) error {
	session := f.driver.NewSession(ctx, f.sessionConfig())
	defer func() { _ = session.Close(context.Background()) }()
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	result, err := tx.Run(ctx, "MATCH (r:Review {id: $id}) SET r.v = $v, r.w = $w", map[string]any{"id": id, "v": value, "w": tag})
	if err != nil {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("run: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		_ = tx.Rollback(context.Background())
		return fmt.Errorf("consume: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (f *mergeAdmissionFixture) requireStored(t *testing.T, w mergeAdmissionWrite) {
	t.Helper()
	stored := f.properties(t, w.id)
	require.NotNilf(t, stored, "%s reported success but no node with id %q exists", w.name, w.id)
	require.EqualValuesf(t, w.value, stored["v"], "%s: stored properties of %q are %v", w.name, w.id, stored)
	require.Equalf(t, w.tag, stored["w"], "%s: stored properties of %q are %v", w.name, w.id, stored)
}

// expectWriteStored runs one write with no admission lease held: the baseline
// proving the fixture's statements and store work on this build.
func (f *mergeAdmissionFixture) expectWriteStored(t *testing.T, w mergeAdmissionWrite) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), mergeAdmissionCallTimeout)
	defer cancel()
	require.NoErrorf(t, w.run(ctx), "%s failed with no admission lease held", w.name)
	f.requireStored(t, w)
}

// findGoroutine polls the full goroutine dump until one goroutine matches, so
// the decision to continue is driven by observed state rather than by a delay.
func findGoroutine(timeout time.Duration, match func(string) bool) (string, bool) {
	deadline := time.Now().Add(timeout)
	for {
		buffer := make([]byte, 1<<20)
		n := runtime.Stack(buffer, true)
		for _, goroutine := range strings.Split(string(buffer[:n]), "\n\n") {
			if match(goroutine) {
				return goroutine, true
			}
		}
		if time.Now().After(deadline) {
			return "", false
		}
		time.Sleep(mergeAdmissionPollInterval)
	}
}

func parkedInWriteAdmission(goroutine string) bool {
	return strings.Contains(goroutine, "multidb.(*writeAdmission).acquire") &&
		(strings.Contains(goroutine, "multidb.(*sizeTrackingEngine).admitPrepared") ||
			strings.Contains(goroutine, "multidb.(*sizeTrackingEngine).CommitTransaction"))
}

// expectWriteCompletesWhileLeaseHeld requires that one unrelated admission
// lease on the same database never refuses the write. Waiting for that lease is
// acceptable, but only when the write is observably parked in the production
// admission gate; the test then releases its lease and requires the write to
// complete and persist.
func (f *mergeAdmissionFixture) expectWriteCompletesWhileLeaseHeld(t *testing.T, lease *mergeAdmissionLease, leaseKind string, w mergeAdmissionWrite) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), mergeAdmissionCallTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.run(ctx) }()

	var writeErr error
	select {
	case writeErr = <-done:
	case <-time.After(mergeAdmissionWriteWindow):
		stack, parked := findGoroutine(mergeAdmissionParkWindow, parkedInWriteAdmission)
		require.Truef(t, parked,
			"%s: the write neither returned nor parked in the write-admission gate within %s",
			w.name, mergeAdmissionParkWindow)
		t.Logf("%s: the write is parked in the production write-admission gate before the lease is released:\n%s", w.name, stack)
		lease.Release()
		select {
		case writeErr = <-done:
		case <-time.After(mergeAdmissionParkWindow):
			t.Fatalf("%s: the write did not complete within %s after the test released its admission lease", w.name, mergeAdmissionParkWindow)
		}
	}

	if writeErr != nil {
		stored := f.properties(t, w.id)
		require.NoErrorf(t, writeErr,
			"%s was refused while an unrelated %s admission lease was held on %q; stored properties of %q: %v",
			w.name, leaseKind, f.database, w.id, stored)
		return
	}
	f.requireStored(t, w)
}

// TestBoltMergeCommitWaitsForUnrelatedAdmissionLease is the
// deterministic reproduction of report entry 9 / B09: a public Bolt MERGE that
// holds a MERGE key is refused with Neo.TransientError.Transaction.Outdated
// while another, unrelated holder has a lease on the database's own
// write-admission gate. The same store and the same lease are used for the
// MATCH ... SET control, which is never refused.
func TestBoltMergeCommitWaitsForUnrelatedAdmissionLease(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a Bolt server and a journal-backed store")
	}
	f := newMergeAdmissionFixture(t)
	f.seed(t)

	// Baseline: every statement of this test is accepted when no lease is held.
	t.Run("no lease held/MERGE seeded node, explicit transaction", func(t *testing.T) {
		f.expectWriteStored(t, mergeAdmissionWrite{
			name:  "MERGE seeded node, explicit transaction",
			id:    "review-10",
			value: 1010,
			tag:   "baseline-explicit",
			run: func(ctx context.Context) error {
				return f.mergeExplicit(ctx, "review-10", 1010, "baseline-explicit")
			},
		})
	})
	t.Run("no lease held/MERGE new node, auto-commit", func(t *testing.T) {
		f.expectWriteStored(t, mergeAdmissionWrite{
			name:  "MERGE new node, auto-commit",
			id:    "new-baseline",
			value: 1011,
			tag:   "baseline-new",
			run: func(ctx context.Context) error {
				return f.mergeAuto(ctx, "new-baseline", 1011, "baseline-new")
			},
		})
	})

	for _, exclusive := range []bool{false, true} {
		leaseKind := "shared"
		if exclusive {
			leaseKind = "exclusive"
		}
		writes := []mergeAdmissionWrite{
			{
				name:  "MERGE seeded node, explicit transaction",
				id:    "review-0",
				value: 1001,
				tag:   "explicit-seeded-" + leaseKind,
				run: func(ctx context.Context) error {
					return f.mergeExplicit(ctx, "review-0", 1001, "explicit-seeded-"+leaseKind)
				},
			},
			{
				name:  "MERGE seeded node, auto-commit",
				id:    "review-1",
				value: 1002,
				tag:   "auto-seeded-" + leaseKind,
				run: func(ctx context.Context) error {
					return f.mergeAuto(ctx, "review-1", 1002, "auto-seeded-"+leaseKind)
				},
			},
			{
				name:  "MERGE new node, auto-commit",
				id:    "new-" + leaseKind,
				value: 1003,
				tag:   "new-" + leaseKind,
				run: func(ctx context.Context) error {
					return f.mergeAuto(ctx, "new-"+leaseKind, 1003, "new-"+leaseKind)
				},
			},
			{
				name:  "MATCH ... SET control, explicit transaction",
				id:    "review-2",
				value: 1004,
				tag:   "control-" + leaseKind,
				run: func(ctx context.Context) error {
					return f.matchSetExplicit(ctx, "review-2", 1004, "control-"+leaseKind)
				},
			},
		}
		for _, w := range writes {
			t.Run(leaseKind+"/"+w.name, func(t *testing.T) {
				lease := f.takeLease(t, exclusive)
				defer lease.Release()
				f.expectWriteCompletesWhileLeaseHeld(t, lease, leaseKind, w)
			})
		}
	}
}

// A journal-backed MERGE may wait for manager admission, but cancellation must
// still roll it back before acceptance and release its native key authority.
func TestJournaledMergeAdmissionCancellationReleasesKey(t *testing.T) {
	f := newMergeAdmissionFixture(t)
	f.seed(t)
	engine, err := f.manager.GetStorage(f.database)
	require.NoError(t, err)
	committer, ok := engine.(interface {
		CommitTransaction(context.Context, *storage.BadgerTransaction) error
	})
	require.True(t, ok)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	begin := func(ctx context.Context) *storage.BadgerTransaction {
		t.Helper()
		tx, err := f.native.BeginTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace(f.database))
		require.NoError(t, tx.PrepareMergeNodeForWrite(ctx, []string{"Review"}, map[string]any{"id": "canceled-admission"}))
		require.True(t, tx.PendingChanges(false).HoldsMergeKey)
		_, err = tx.CreateNode(ctx, &storage.Node{ID: storage.NodeID(f.database + ":canceled-admission"), Labels: []string{"Review"}, Properties: map[string]any{"id": "canceled-admission", "v": int64(7)}})
		require.NoError(t, err)
		return tx
	}
	tx := begin(ctx)
	defer tx.Rollback()
	lease := f.takeLease(t, true)
	defer lease.Release()
	done := make(chan error, 1)
	go func() { done <- committer.CommitTransaction(ctx, tx) }()
	_, parked := findGoroutine(mergeAdmissionParkWindow, parkedInWriteAdmission)
	require.True(t, parked, "journal-backed admission must wait rather than report a false conflict")
	cancel()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(mergeAdmissionParkWindow):
		t.Fatal("canceled admission did not return")
	}
	lease.Release()
	require.False(t, tx.IsActive())
	require.Nil(t, f.properties(t, "canceled-admission"), "the canceled write was never accepted")
	retryContext, stopRetry := context.WithTimeout(t.Context(), mergeAdmissionCallTimeout)
	defer stopRetry()
	retry := begin(retryContext)
	defer retry.Rollback()
	require.NoError(t, committer.CommitTransaction(retryContext, retry), "cancellation released the unique key for the next writer")
	require.EqualValues(t, int64(7), f.properties(t, "canceled-admission")["v"])
}

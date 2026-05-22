package nornicdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// search-warmup gate tests cover the runServe/server.New ordering fix.
//
// Before the fix, nornicdb.Open kicked the search-index warmup goroutine
// in the background and that goroutine could read db.dbSearchFlagsResolver
// as nil — racing against pkg/server.New, which installs the resolver
// later. Result: the default database always warmed using global
// fallbacks, even when YAML or admin-API per-DB overrides said otherwise.
//
// After the fix, Open holds the warmup goroutine at
// `<-db.searchWarmupReady` until MarkSearchWarmupReady is called.
// pkg/server.New calls it AFTER SetDbSearchFlagsResolver, so when warmup
// finally runs it sees the operator-configured resolver — including for
// the default DB.

// flagState is a tristate covering everything the resolver can emit for
// a single index. It collapses (enabled, warming) into one ergonomic
// value so the parameter sweep stays readable.
type flagState int

const (
	flagOff     flagState = iota // (false, _) — index disabled
	flagStartup                  // (true, "startup") — build at boot
	flagLazy                     // (true, "lazy") — defer to first query
)

func (s flagState) String() string {
	switch s {
	case flagOff:
		return "off"
	case flagStartup:
		return "startup"
	case flagLazy:
		return "lazy"
	}
	return "?"
}

// resolverValues converts a flagState into the (enabled, warming) tuple
// the per-DB flags resolver returns. Centralized so the matrix table
// below stays one column instead of two.
func (s flagState) resolverValues() (enabled bool, warming string) {
	switch s {
	case flagOff:
		return false, "startup" // warming is irrelevant when disabled
	case flagStartup:
		return true, "startup"
	case flagLazy:
		return true, "lazy"
	}
	return true, "startup"
}

// openWarmupGateDB spins up nornicdb.Open with the test-friendly knobs every
// gate test needs (no decay, no auto-links, sync writes for predictable
// flush timing). Each call gets its own temp dir.
func openWarmupGateDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Database.DataDir = dir
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	// Globals say enabled (the DefaultConfig default). The resolver
	// installed in each test case overrides per-DB, so we're proving
	// the resolver wins over a permissive global.
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true

	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// waitForSearchStatus polls db.GetDatabaseSearchStatus(dbName) until
// pred returns true or the deadline elapses. Returns the last observed
// status either way so the caller can pretty-print it on failure.
//
// Polling beats fixed Sleep waits because actual warmup time on CI is
// noisy: a fresh DB with zero nodes takes <10ms, but slow runners or
// an already-loaded test process can stretch to hundreds of ms.
func waitForSearchStatus(
	db *DB,
	dbName string,
	timeout time.Duration,
	pred func(DatabaseSearchStatus) bool,
) DatabaseSearchStatus {
	deadline := time.Now().Add(timeout)
	var last DatabaseSearchStatus
	for time.Now().Before(deadline) {
		last = db.GetDatabaseSearchStatus(dbName)
		if pred(last) {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	return last
}

// TestSearchWarmupGate_DefaultDBHonorsResolverOverride_Matrix sweeps every
// 3×3 combination of (BM25 ∈ {off, startup, lazy}) × (Vector ∈ same).
//
// The default DB is the most important one to cover: it's the one that
// suffered most from the pre-fix race because it was the first thing
// warmed. If a future refactor reintroduces the race, this matrix will
// fail in a way that points at exactly which combination broke.
func TestSearchWarmupGate_DefaultDBHonorsResolverOverride_Matrix(t *testing.T) {
	cases := []struct {
		bm25   flagState
		vector flagState
	}{
		{flagOff, flagOff},
		{flagOff, flagStartup},
		{flagOff, flagLazy},
		{flagStartup, flagOff},
		{flagStartup, flagStartup},
		{flagStartup, flagLazy},
		{flagLazy, flagOff},
		{flagLazy, flagStartup},
		{flagLazy, flagLazy},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("bm25=%s/vector=%s", tc.bm25, tc.vector)
		t.Run(name, func(t *testing.T) {
			db := openWarmupGateDB(t)
			defaultDB := db.defaultDatabaseName()

			bm25En, bm25W := tc.bm25.resolverValues()
			vecEn, vecW := tc.vector.resolverValues()

			// Install the resolver BEFORE releasing the warmup gate so
			// the warmup goroutine is guaranteed to see it. This is
			// the load-bearing ordering pkg/server gets right via
			// MarkSearchWarmupReady AFTER SetDbSearchFlagsResolver.
			db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
				if dbName == defaultDB {
					return bm25En, vecEn, bm25W, vecW
				}
				return true, true, "startup", "startup"
			})
			db.MarkSearchWarmupReady()

			// Reachable end-state by combination:
			//   off×off:           Ready stays false, both flags off, no lazy trigger
			//   any startup:       Ready=true once that index finishes building
			//   any lazy w/o startup pair: Ready=false, LazyTriggerNeeded=true
			expectBuilds := tc.bm25 == flagStartup || tc.vector == flagStartup
			expectLazyTrigger := !expectBuilds &&
				(tc.bm25 == flagLazy || tc.vector == flagLazy)

			// Wait for a stable end-state. Any-startup → Ready=true; any
			// lazy-only → LazyTriggerNeeded=true; off×off → just confirm
			// the resolver values are mirrored.
			st := waitForSearchStatus(db, defaultDB, 5*time.Second, func(s DatabaseSearchStatus) bool {
				if s.BM25Enabled != bm25En || s.VectorEnabled != vecEn {
					return false
				}
				if expectBuilds {
					return s.Ready
				}
				if expectLazyTrigger {
					return s.LazyTriggerNeeded
				}
				// off×off — accept any state where the flags settled.
				return true
			})

			// Resolver values must be reflected regardless of build outcome.
			assert.Equal(t, bm25En, st.BM25Enabled,
				"BM25Enabled status must mirror resolver value")
			assert.Equal(t, vecEn, st.VectorEnabled,
				"VectorEnabled status must mirror resolver value")

			switch {
			case tc.bm25 == flagOff && tc.vector == flagOff:
				assert.False(t, st.Ready, "no indexes enabled — Ready must remain false")
				assert.False(t, st.Building, "no build should be running")
				assert.False(t, st.LazyTriggerNeeded,
					"both off — no lazy trigger expected")

			case expectBuilds:
				// At least one index built at startup. Ready flips to
				// true once that build completes.
				assert.True(t, st.Ready,
					"expected Ready=true when at least one index has warming=startup, status=%+v", st)
				assert.True(t, st.Initialized,
					"expected Initialized once a build ran, status=%+v", st)

			default:
				// Only lazy-enabled indexes; nothing built at startup.
				// First query would synchronously trigger the build.
				assert.True(t, st.LazyTriggerNeeded,
					"expected LazyTriggerNeeded=true when only lazy indexes are enabled, status=%+v", st)
				assert.False(t, st.Ready,
					"expected Ready=false until first query triggers the lazy build, status=%+v", st)
			}
		})
	}
}

// TestSearchWarmupGate_DefaultOpensGateImmediately covers embedded
// callers that don't opt into DeferSearchWarmup. The default contract
// is: Open releases the gate before returning, so warmup proceeds
// using whatever resolver state exists (none → fall through to
// db.config.Memory.Search*). No timer, no race window, no need for
// the caller to call MarkSearchWarmupReady themselves.
func TestSearchWarmupGate_DefaultOpensGateImmediately(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Database.DataDir = dir
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	// Globals say disabled. With NO resolver installed and
	// DeferSearchWarmup left at its default false, warmup should
	// honour the global fallback via db.config.Memory.Search* inside
	// resolveSearchFlags — and reach a settled state without anyone
	// calling MarkSearchWarmupReady.
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = false
	// Sanity: default is false, but spell it out so a future change
	// to the default is caught here rather than at the assertion.
	require.False(t, cfg.DeferSearchWarmup,
		"DeferSearchWarmup must default to false so embedded callers don't deadlock")

	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	defaultDB := db.defaultDatabaseName()
	st := waitForSearchStatus(db, defaultDB, 2*time.Second, func(s DatabaseSearchStatus) bool {
		return !s.BM25Enabled && !s.VectorEnabled
	})
	assert.False(t, st.BM25Enabled,
		"global cfg said BM25=false; embedded path should honour it without a resolver")
	assert.False(t, st.VectorEnabled,
		"global cfg said vector=false; embedded path should honour it without a resolver")
}

// TestSearchWarmupGate_DeferredHoldsUntilMarkReady covers the pkg/server
// contract: when a caller sets Config.DeferSearchWarmup, the warmup
// goroutine stays parked at the gate until MarkSearchWarmupReady is
// called. Note that an empty search service entry IS created
// synchronously inside Open (the cypher executor wires a service handle
// for routing vector procedures), so Initialized=true on the status
// struct doesn't mean warmup ran — what actually warms is the index
// build behind GetBuildProgress(). We assert on Building/Ready, which
// flip false→true only after the gated warmup loop runs.
func TestSearchWarmupGate_DeferredHoldsUntilMarkReady(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Database.DataDir = dir
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true
	cfg.DeferSearchWarmup = true

	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	defaultDB := db.defaultDatabaseName()

	// Without MarkSearchWarmupReady, the warmup goroutine is parked at
	// the gate — Ready must remain false. Observe long enough that on
	// a non-deferred run warmup would have completed already (the empty
	// default DB warms in <50ms), so the contrast is the proof that
	// the gate held.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := db.GetDatabaseSearchStatus(defaultDB)
		require.False(t, st.Ready,
			"warmup must not complete before MarkSearchWarmupReady; got status=%+v", st)
		require.False(t, st.Building,
			"warmup must not even start building before MarkSearchWarmupReady; got status=%+v", st)
		time.Sleep(10 * time.Millisecond)
	}

	// Releasing the gate must drive warmup through to Ready.
	db.MarkSearchWarmupReady()
	st := waitForSearchStatus(db, defaultDB, 5*time.Second, func(s DatabaseSearchStatus) bool {
		return s.Ready
	})
	assert.True(t, st.Ready, "expected Ready=true after MarkSearchWarmupReady, status=%+v", st)
}

// TestSearchWarmupGate_MarkReadyIdempotent asserts MarkSearchWarmupReady
// is safe to call repeatedly. pkg/server may end up calling it multiple
// times across error paths, and a future test harness might call it
// directly to control timing. Closing a closed channel would panic, so
// the once gate is load-bearing.
func TestSearchWarmupGate_MarkReadyIdempotent(t *testing.T) {
	db := openWarmupGateDB(t)
	db.MarkSearchWarmupReady()
	db.MarkSearchWarmupReady()
	db.MarkSearchWarmupReady()
}

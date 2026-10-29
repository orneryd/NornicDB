package capturecensus

// Storage nil-context census author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func storageNilContextConfig(t *testing.T, patterns ...string) workcensus.Config {
	t.Helper()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	return workcensus.Config{Dir: root, Patterns: patterns, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")}
}

func nilContextFlows(facts Inventory) []string {
	var flows []string
	for _, v := range facts.Violations {
		if strings.Contains(v, "a nil context can reach it") {
			flows = append(flows, v)
		}
	}
	return flows
}

// TestStorageNilContextCensus is the compiled census of every call that can
// pass a nil or absent context into a function reaching the nil boundary (the
// capture owners plus every context reader of the native engine), over the
// whole module (callers in other packages included). It logs each flow with its
// witness, the nil contexts passed elsewhere and the boundary, and requires no
// flow into the boundary.
func TestStorageNilContextCensus(t *testing.T) {
	facts, err := Extract(storageNilContextConfig(t, "./..."), StoragePolicy())
	require.NoError(t, err)
	flows := nilContextFlows(facts)
	for _, flow := range flows {
		t.Logf("NIL-CONTEXT %s", flow)
	}
	for _, finding := range facts.NilContextsElsewhere {
		t.Logf("NIL-CONTEXT-ELSEWHERE %s", finding)
	}
	// Every other census violation (a substitution, a misplaced source, a stale
	// cross-unit item) is listed here; TestSnapshotOverlayPublicReadsReachCapture
	// in pkg/storage requires them all to be empty.
	for _, v := range facts.Violations {
		if !strings.Contains(v, "a nil context can reach it") {
			t.Logf("CONTEXT-VIOLATION %s", v)
		}
	}
	for _, acceptance := range facts.Acceptances {
		if acceptance.Kind == KindNilDetached {
			t.Logf("NIL-DETACHED %s", acceptance.Key)
		}
	}
	for _, finding := range facts.NilContextsDisposed {
		t.Logf("NIL-CONTEXT-DISPOSED %s", finding)
	}
	for _, member := range facts.NilContextBoundary {
		t.Logf("NIL-CONTEXT-BOUNDARY %s", member)
	}
	for _, owner := range StoragePolicy().ContextOwners {
		require.Contains(t, facts.NilContextBoundary, owner, "every listed owner is inside the computed boundary")
	}
	for _, acceptance := range facts.Acceptances {
		t.Logf("ACCEPTANCE %s", acceptance)
	}
	counts := AcceptanceCounts(facts.Acceptances)
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		t.Logf("ACCEPTANCE-COUNT %s %d", kind, counts[kind])
	}
	for _, kind := range ForbiddenAcceptanceKinds {
		require.Zero(t, counts[kind], "Contract A forbids acceptance kind %s", kind)
	}
	// Contract A is unfulfilled wherever a request holder reaches a
	// substitution only across the public API: each such site is listed
	// against the tracked deferral, never counted as a pass. Every edge the
	// in-package walks cut is listed, and counted per interface it dispatches
	// through, so the exclusion population beyond Engine is visible.
	for _, acceptance := range facts.Acceptances {
		if acceptance.Kind == KindUnfulfilled {
			t.Logf("UNFULFILLED-CONTRACT-A %s", acceptance.Key)
		}
	}
	for _, crossing := range facts.BoundaryCrossings {
		t.Logf("PUBLIC-BOUNDARY-CROSSING %s", crossing)
	}
	// The boundary call each classified site's fresh context reaches.
	for _, site := range facts.ContextSites {
		if site.Reaches != "" {
			t.Logf("CONTEXT-SITE-REACHES %s:%d %s: %s", site.File, site.Line, site.Function, site.Reaches)
		}
	}
	interfaces := CrossingInterfaces(facts.BoundaryCrossings)
	names := make([]string, 0, len(interfaces))
	for name := range interfaces {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Logf("PUBLIC-BOUNDARY-INTERFACE %s %d", name, interfaces[name])
	}
	unfulfilledCrossings := 0
	for _, crossing := range facts.BoundaryCrossings {
		if crossing.Holder != "" {
			unfulfilledCrossings++
		}
	}
	t.Logf("PUBLIC-BOUNDARY-COUNT crossings %d with-request-beyond %d unfulfilled-sites %d", len(facts.BoundaryCrossings), unfulfilledCrossings, counts[KindUnfulfilled])
	require.NotEmpty(t, StoragePolicy().DeferredPublicAPI.Reason, "the public API deferral is tracked")
	// Cross-unit acceptances come only from the ruled entries; the census
	// fails an entry that matches nothing (checked with the violations below),
	// so these are exactly the ruled items, each listed with every use.
	require.Len(t, StoragePolicy().CrossUnitOwners, 48, "the ruled cross-unit items (U-1636): the WAL accepted-command submission, the WAL entity wait lifetime (NewWALEngine), the database lifetime (nornicdb.Open), the index backfill worker lifetime (startIndexBackfill), the accepted-entity preparation and submission holders (PrepareEntityChanges, SubmitAcceptedEntityTransaction), the write-behind drain staging (rebindBufferedNumericIDs), the four maintenance lifetime sources, the Bolt cleanup base, accepted-entity commit preparation and the two replication entity-command paths (the multidb size-tracking commit normalizes its own request parameter: no substitution); the 25 allocated from d17 (owner lifetimes, Bolt session lifetime, process/database/server lifetimes, accepted-entity commands); and from d18: the transaction-session expiry check (S4), the two commit-notification drain sites (S2) and Bolt handleRun (S3; commitContext is counted through its holder dispatchInner, recordExplicitTransactionWrite through handleRun); and from d21: the search service build started without a request (search.Service.BuildIndexes, search lifetime)")
	require.NotZero(t, counts[KindCrossUnitOwner])
	// The reviewed held-value records: each pinned to the exact held values it
	// covers and the source it relies on, listed and counted as its own kind.
	for _, acceptance := range facts.Acceptances {
		if acceptance.Kind == KindReviewedHeld {
			t.Logf("REVIEWED-HELD-VALUE %s", acceptance.Key)
		}
	}
	require.Len(t, StoragePolicy().ReviewedHeldValues, 19, "the reviewed held-value sites: extractTraceparent, withParams, BindContext and the three IVF-HNSW serving-view sites (d21); the Bolt transaction lifecycle's commitContext and begin, the two Cypher value-scope evaluators, the database's TriggerSearchClustering and retentionContext, and the MVCC lifecycle schedule (d23)")
	require.Equal(t, 23, counts[KindReviewedHeld], "one per fresh context at a recorded site")
	for site, record := range StoragePolicy().ReviewedHeldValues {
		require.NotEmpty(t, record.Closure, "%s: a storage record pins everything its sources reach", site)
	}
	for _, v := range facts.Violations {
		require.NotContains(t, v, "matches no substitution", "a ruled cross-unit item matched nothing")
		require.NotContains(t, v, "a ruled contract-scope entry matches no context site")
		require.NotContains(t, v, "reviewed held-value record")
	}
	for _, v := range facts.Violations {
		if strings.Contains(v, "nil-context disposition") {
			t.Errorf("%s", v)
		}
	}
	require.Empty(t, flows, "a nil or absent context can reach a capture owner")
}

// The census must catch planted nil flows in the real package: a nil passed
// straight to an exported reader, and a zero context field handed to the
// temporal history lookup (the path a nil commit context took).
func TestStorageNilContextCensusPlants(t *testing.T) {
	c := storageNilContextConfig(t, "./pkg/storage")
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "pkg", "storage", "zz_planted_nil_context.go"): []byte(`package storage
import "context"
type plantedNilContextTx struct {
	engine *BadgerEngine
	ctx    context.Context
}
func plantedNilSnapshot(b *BadgerEngine) { _, _ = b.OpenReadSnapshot(nil) }
func (p *plantedNilContextTx) check(c Constraint, n *Node) {
	_, _, _ = p.engine.temporalKeyHistoryNodesLatest(p.ctx, "", c, n)
}
func newPlantedNilContextTx(b *BadgerEngine) *plantedNilContextTx { return &plantedNilContextTx{engine: b} }
var _ = []any{plantedNilSnapshot, (*plantedNilContextTx).check, newPlantedNilContextTx}
`)}
	facts, err := Extract(c, StoragePolicy())
	require.NoError(t, err)
	all := strings.Join(nilContextFlows(facts), "\n")
	require.Contains(t, all, "plantedNilSnapshot -> (*github.com/orneryd/nornicdb/pkg/storage.BadgerEngine).OpenReadSnapshot: a nil context can reach it: nil")
	require.Contains(t, all, "(*github.com/orneryd/nornicdb/pkg/storage.plantedNilContextTx).check -> (*github.com/orneryd/nornicdb/pkg/storage.BadgerEngine).temporalKeyHistoryNodesLatest: a nil context can reach it: zero field ctx")
}

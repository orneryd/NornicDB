package cypher

// Public fulltext read controls for PROFILE.
//
// Every control drives StorageExecutor.Execute, the entry point Bolt and HTTP
// use, and states only what the supported contract owns: a PROFILE plan must
// report the graph entities a fulltext CALL actually read, and the statement
// must return the same public rows and score bits with or without PROFILE.
//
// The ground truth is the existing fulltextWorkProbe (declared work probe): it
// decorates the real physical readers and counts the entities each reader
// actually delivered, before the acquisition's membership and dedup admission.
// A plan record is therefore compared with what the reader handed over, never
// with the hits the procedure emitted, len(rows) or a planner estimate.
//

import (
	"errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fulltextWorkReading is the work one public statement actually caused, read
// from the existing probe: entities delivered to the acquisition visit, early
// stops returned by the relationship reader, and property values formatted
// into indexed fields.
type fulltextWorkReading struct {
	nodes, edges, stops, formatted int64
}

// fulltextWorkRead snapshots the probe's counters without resetting them.
func fulltextWorkRead(probe *fulltextWorkProbe) fulltextWorkReading {
	return fulltextWorkReading{
		nodes:     probe.nodes.Load(),
		edges:     probe.edges.Load(),
		stops:     probe.stops.Load(),
		formatted: probe.formatted.Load(),
	}
}

// delivered returns the entities of the probed kind the reader handed to the
// acquisition and requires the other entity kind was not read at all.
func (r fulltextWorkReading) delivered(t *testing.T, relationships bool) int64 {
	t.Helper()
	if relationships {
		require.Zero(t, r.nodes, "a relationship read must not stream nodes")
		return r.edges
	}
	require.Zero(t, r.edges, "a node read must not stream relationships")
	return r.nodes
}

// fulltextReadExecutor returns an executor over engine whose query-result
// caching is disabled, so a repeated public statement always reaches the
// procedure, and closes the unmanaged search service it creates.
func fulltextReadExecutor(t *testing.T, engine storage.Engine) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0})
	maxEntries, _ := exec.QueryCachePolicy()
	require.Zero(t, maxEntries, "query-result caching would hide repeated public fulltext reads")
	t.Cleanup(func() {
		if svc := exec.search(); svc != nil {
			require.NoError(t, svc.Close())
		}
	})
	return exec
}

// requireFulltextProfileRead selects a single source-owned access record.
func requireFulltextProfileRead(t *testing.T, plan *ExecutionPlan, kind, detail string) *PlanOperator {
	t.Helper()
	require.Equal(t, 1, countProfileOperatorType(plan.Root, kind))
	record := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, record)
	require.True(t, record.RowsObserved)
	require.True(t, record.DBHitsObserved)
	if detail != "" {
		require.Equal(t, detail, record.Description)
	}
	return record
}

// fulltextProfileIDs returns the identity column of the public rows, preserving
// the order the procedure returned.
func fulltextProfileIDs(t *testing.T, rows [][]interface{}) []interface{} {
	t.Helper()
	ids := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		require.Len(t, row, 2, "the control projects an entity identity and a score")
		ids = append(ids, row[0])
	}
	return ids
}

// TestProfileFulltextReadsReportAcquiredEntities pins the native, non-retained
// first read of both fulltext procedures: a ranked read must report the corpus
// it acquired, a bounded unranked read must report only the prefix it consumed,
// and an early return must acquire no entity at all. The public rows and score
// bits match the equivalent ordinary statement exactly.
func TestProfileFulltextReadsReportAcquiredEntities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const count = 32
		_, store := fulltextWorkFixture(t, count, false)
		probe := &fulltextWorkProbe{NamespacedEngine: store}
		for _, entity := range []struct {
			name          string
			relationships bool
			scanKind      string
			scanDetail    string
			identity      string
		}{
			{name: "nodes", scanKind: profileKindNodeByLabelScan, scanDetail: "Source", identity: "n"},
			{name: "relationships", relationships: true, scanKind: profileKindAllRelationshipsScan, identity: "e"},
		} {
			query := fulltextWorkQuery(entity.relationships)
			// Two matching entities carry the probe term, so the two
			// highest-scoring documents are the first two eligible identities.
			topTwo := []interface{}{entity.identity + "00002", entity.identity + "00003"}

			t.Run(entity.name+"/ranked_limit2", func(t *testing.T) {
				params := map[string]any{"q": "hotel", "opts": map[string]any{"limit": int64(2)}}

				plainExec := fulltextReadExecutor(t, probe)
				probe.reset()
				plain, err := plainExec.Execute(t.Context(), query, params)
				require.NoError(t, err)
				plainWork := fulltextWorkRead(probe)

				// A separate executor, so this is the same native first read,
				// not the retained corpus of the statement above.
				profileExec := fulltextReadExecutor(t, probe)
				probe.reset()
				profiled, err := profileExec.Execute(t.Context(), "PROFILE "+query, params)
				require.NoError(t, err)
				profiledWork := fulltextWorkRead(probe)
				require.Equal(t, plainWork, profiledWork,
					"PROFILE must not add, drop or reformat any reader work on the same statement")
				require.Zero(t, profiledWork.formatted, "an index on one field must not format unrelated properties")

				require.Equal(t, plain.Columns, profiled.Columns)
				require.ElementsMatch(t, plain.Rows, profiled.Rows,
					"PROFILE preserves the public rows and their exact score bits")
				require.Len(t, profiled.Rows, 2)
				require.ElementsMatch(t, topTwo, fulltextProfileIDs(t, profiled.Rows),
					"the two tied highest-scoring documents")

				delivered := profiledWork.delivered(t, entity.relationships)
				plan := executionPlanFromResult(t, profiled)
				require.True(t, plan.TotalDBHitsObserved,
					"a reviewed fulltext registration reports its own reads instead of an opaque callback")
				require.Empty(t, plan.Unobserved)
				scan := requireFulltextProfileRead(t, plan, entity.scanKind, entity.scanDetail)
				require.Equal(t, delivered, scan.ActualRows,
					"the record counts the entities the reader delivered (%d), not the %d emitted rows", delivered, len(profiled.Rows))
				require.Equal(t, delivered, scan.DBHits, "reading an entity is one access unit")
				require.GreaterOrEqual(t, plan.TotalDBHits, delivered,
					"the acquisition's entities are part of the statement's observed work")
				require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByIdSeek),
					"the population's captured images add no point read")
				require.NotEqual(t, int64(len(profiled.Rows)), scan.ActualRows,
					"the emitted hit count is not the input population")
				// The ranked corpus is complete: count minus the out-of-scope
				// first entity and the second entity with an empty field, so
				// every indexed document was read for this ranking.
				require.GreaterOrEqual(t, scan.ActualRows, int64(count-2),
					"a ranked read must read every indexed document its scores describe")
				require.LessOrEqual(t, scan.ActualRows, int64(count))
				require.Equal(t, int64(len(profiled.Rows)), plan.TotalRows)
			})

			for _, bounded := range []struct{ name, query string }{
				{name: "wildcard_limit1", query: "*"},
				{name: "presence_limit1", query: "text:*"},
			} {
				t.Run(entity.name+"/"+bounded.name, func(t *testing.T) {
					params := map[string]any{"q": bounded.query, "opts": map[string]any{"limit": int64(1)}}
					// The unranked path reports its fixed score for an accepted row.
					want := [][]interface{}{{entity.identity + "00002", float64(1)}}

					plainExec := fulltextReadExecutor(t, probe)
					probe.reset()
					plain, err := plainExec.Execute(t.Context(), query, params)
					require.NoError(t, err)
					plainWork := fulltextWorkRead(probe)

					profileExec := fulltextReadExecutor(t, probe)
					probe.reset()
					profiled, err := profileExec.Execute(t.Context(), "PROFILE "+query, params)
					require.NoError(t, err)
					profiledWork := fulltextWorkRead(probe)
					require.Equal(t, plainWork, profiledWork,
						"PROFILE must not add, drop or reformat any reader work on the same statement")
					require.Zero(t, profiledWork.formatted)

					require.Equal(t, plain.Columns, profiled.Columns)
					require.Equal(t, want, plain.Rows)
					require.Equal(t, want, profiled.Rows, "PROFILE returns the same exact row and score")

					delivered := profiledWork.delivered(t, entity.relationships)
					plan := executionPlanFromResult(t, profiled)
					require.True(t, plan.TotalDBHitsObserved)
					require.Empty(t, plan.Unobserved)
					// Ordered pages deliberately preserve whole-population release order;
					// their node scope carries no label hint. Ranked reads above
					// use the declared label stream instead.
					kind, detail := entity.scanKind, entity.scanDetail
					if !entity.relationships {
						kind, detail = profileKindAllNodesScan, ""
					}
					scan := requireFulltextProfileRead(t, plan, kind, detail)
					require.Equal(t, delivered, scan.ActualRows,
						"the record counts the delivered entities (%d), not the one emitted row", delivered)
					require.Equal(t, delivered, scan.DBHits)
					require.GreaterOrEqual(t, plan.TotalDBHits, delivered)
					require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByIdSeek),
						"a prefix read serves its one row from its own image")
					require.NotEqual(t, int64(len(profiled.Rows)), scan.ActualRows)
					require.Less(t, scan.ActualRows, int64(count),
						"a bounded unranked read stops before the full population of %d", count)
				})
			}

			t.Run(entity.name+"/early_returns", func(t *testing.T) {
				for _, early := range []struct {
					name   string
					params map[string]any
				}{
					{name: "empty_query", params: map[string]any{"q": "", "opts": map[string]any{}}},
					{name: "unknown_field", params: map[string]any{"q": "unknown:*", "opts": map[string]any{}}},
					{name: "zero_limit", params: map[string]any{"q": "*", "opts": map[string]any{"limit": int64(0)}}},
				} {
					t.Run(early.name, func(t *testing.T) {
						plainExec := fulltextReadExecutor(t, probe)
						probe.reset()
						plain, err := plainExec.Execute(t.Context(), query, early.params)
						require.NoError(t, err)
						require.Empty(t, plain.Rows)
						require.Equal(t, fulltextWorkReading{}, fulltextWorkRead(probe),
							"an early return delivers no entity to the reader")

						profileExec := fulltextReadExecutor(t, probe)
						probe.reset()
						profiled, err := profileExec.Execute(t.Context(), "PROFILE "+query, early.params)
						require.NoError(t, err)
						require.Empty(t, profiled.Rows)
						require.Equal(t, fulltextWorkReading{}, fulltextWorkRead(probe),
							"the PROFILE statement delivers no entity to the reader either")

						// Only acquired entity records are asserted: the statement
						// still resolves the index declaration, and that schema
						// read is not the entity acquisition under test.
						plan := executionPlanFromResult(t, profiled)
						require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByLabelScan),
							"an early return acquires no node population")
						require.Nil(t, planOperatorOfType(plan.Root, profileKindAllRelationshipsScan),
							"an early return acquires no relationship population")
					})
				}
			})
		}
	})
}

// TestProfileFulltextRetainedReadReusesAcquiredCorpus pins the retained node
// projection: the first PROFILE ranked read acquires the declared Source
// population from storage and serves its rows from the images it captured; the
// next identical call reuses the retained corpus, reads no entity from the
// population again, and its request-local committed snapshot hydration of the
// emitted rows is reported instead of hidden.
func TestProfileFulltextRetainedReadReusesAcquiredCorpus(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		// Ten Source nodes (i%4 == 0) carry the text field; only the first
		// one (i%40 == 0) also carries the probe term.
		const count = 40
		fixtureExec, counted := fulltextMaintainedFixture(t, count)
		maxEntries, _ := fixtureExec.QueryCachePolicy()
		require.Zero(t, maxEntries,
			"the fixture disables query-result caching so every statement reaches the procedure")

		// The existing hydration probe observes the public adapter's actual
		// captured point reads; it changes no reader's decision.
		probe := &fulltextHydrationProbe{fulltextCountingStore: counted}
		service := search.NewService(probe, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
		exec.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return service })
		maxEntries, _ = exec.QueryCachePolicy()
		require.Zero(t, maxEntries)

		const query = "CALL db.index.fulltext.queryNodes('source_text','hotel',{limit:2}) YIELD node,score RETURN node.id,score"
		const profiledQuery = "PROFILE " + query

		_, _ = counted.take() // fixture setup is not part of a measured read
		probe.captures.Store(0)
		probe.nodeReads.Store(0)
		probe.edgeReads.Store(0)
		first, err := exec.Execute(t.Context(), profiledQuery, nil)
		require.NoError(t, err)
		firstScans, firstDocs := counted.take()

		require.Equal(t, 1, firstScans,
			"the first ranked read acquires the indexed population from storage")
		require.GreaterOrEqual(t, firstDocs, count/4,
			"the acquisition covers the declared Source population")
		require.Len(t, first.Rows, 1)
		require.Equal(t, "n000000", first.Rows[0][0],
			"only the first fixture node carries the probe term")
		require.Zero(t, probe.captures.Load(),
			"a population read opens no point-read snapshot")
		require.Zero(t, probe.nodeReads.Load()+probe.edgeReads.Load(),
			"the population's captured images serve the emitted rows, so no point read happens")

		plan := executionPlanFromResult(t, first)
		require.True(t, plan.TotalDBHitsObserved,
			"a reviewed fulltext registration reports its own reads instead of an opaque callback")
		require.Empty(t, plan.Unobserved)
		scan := requireFulltextProfileRead(t, plan, profileKindNodeByLabelScan, "Source")
		require.Equal(t, int64(firstDocs), scan.ActualRows,
			"the record counts the Source entries the reader delivered, not the emitted row")
		require.Equal(t, int64(firstDocs), scan.DBHits)
		require.GreaterOrEqual(t, plan.TotalDBHits, int64(firstDocs))
		require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByIdSeek),
			"the acquired population's images add no point read")
		require.Equal(t, int64(len(first.Rows)), plan.TotalRows)

		// The same statement without PROFILE, on an executor that populates
		// from storage itself: the retained corpus is not a different answer.
		plainExec := fulltextReadExecutor(t, probe)
		_, _ = counted.take()
		plain, err := plainExec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		twinScans, twinDocs := counted.take()
		require.Equal(t, 1, twinScans,
			"the ordinary control acquires the population from storage")
		require.GreaterOrEqual(t, twinDocs, count/4)
		require.Equal(t, first.Rows, plain.Rows,
			"the retained corpus keeps the acquisition's exact rows and score bits")

		probe.captures.Store(0)
		probe.nodeReads.Store(0)
		probe.edgeReads.Store(0)
		_, _ = counted.take()
		second, err := exec.Execute(t.Context(), profiledQuery, nil)
		require.NoError(t, err)
		scans, docs := counted.take()
		require.Zero(t, scans, "a hot ranked read must reuse the retained corpus instead of scanning")
		require.Zero(t, docs, "no entity is re-read from storage to serve the retained corpus")
		require.Equal(t, first.Rows, second.Rows,
			"the retained read returns the acquisition's rows and score bits")
		require.Zero(t, probe.edgeReads.Load())
		require.Equal(t, int64(1), probe.captures.Load(),
			"the retained route hydrates through its request-local committed snapshot (search.ReadFulltextDocuments opens an EntityReadSnapshot when the view is unchanged)")

		hydrated := int64(len(second.Rows))
		require.Equal(t, hydrated, probe.nodeReads.Load(),
			"every emitted retained row is hydrated exactly once (search.FulltextDocuments.Entity reads one entity per admitted hit)")

		hot := executionPlanFromResult(t, second)
		require.True(t, hot.TotalDBHitsObserved)
		require.Empty(t, hot.Unobserved)
		require.Nil(t, planOperatorOfType(hot.Root, profileKindNodeByLabelScan),
			"the retained read performs no population scan")
		require.GreaterOrEqual(t, hot.TotalDBHits, hydrated,
			"the actual point reads are part of the retained statement's observed work")
		hydration := requireFulltextProfileRead(t, hot, profileKindNodeByIdSeek, "")
		require.Equal(t, hydrated, hydration.ActualRows)
		require.Equal(t, hydrated, hydration.DBHits)

		require.Equal(t, hydrated, hot.TotalRows)
	})
}

// TestProfileFulltextReadFailuresPreserveTheReaderError pins the failure path:
// a controlled source-reader failure fails the public CALL and PROFILE CALL
// alike, keeps the reader's own cause and the procedure-call status, and never
// returns a partial successful result.
func TestProfileFulltextReadFailuresPreserveTheReaderError(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		_, store := fulltextWorkFixture(t, 8, false)
		probe := &fulltextWorkProbe{NamespacedEngine: store}
		cause := errors.New("fulltext source reader unavailable")
		params := map[string]any{"q": "hotel", "opts": map[string]any{}}
		for _, entity := range []struct {
			name          string
			relationships bool
		}{
			{name: "nodes"},
			{name: "relationships", relationships: true},
		} {
			query := fulltextWorkQuery(entity.relationships)
			for _, mode := range []struct{ name, prefix string }{
				{name: "call"},
				{name: "profile_call", prefix: "PROFILE "},
			} {
				t.Run(fmt.Sprintf("%s/%s", entity.name, mode.name), func(t *testing.T) {
					exec := fulltextReadExecutor(t, probe)
					probe.readErr = cause
					t.Cleanup(func() { probe.readErr = nil })

					result, err := exec.Execute(t.Context(), mode.prefix+query, params)
					require.ErrorContains(t, err, cause.Error(),
						"the reader's failure reaches the public statement")
					require.ErrorIs(t, err, cause,
						"the reader's own failure stays the preserved cause")
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code,
						"a failed fulltext read keeps the procedure-call status")
					if result != nil {
						require.Empty(t, result.Rows,
							"a failed acquisition returns no partial successful rows")
					}
				})
			}
		}
	})
}

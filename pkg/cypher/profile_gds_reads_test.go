package cypher

import (
	"context"
	"math"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These are the reader-side controls for GDS's and the composite catalog's
// functions: what a PROFILE statement reports for the logical operations those
// functions actually performed. A record is one request the function made and
// the population that request returned - adjacency entries a direction read
// returned, the projection a catalog lookup returned, the property entry a
// projected read returned, the graphs a composite catalog listed - and never
// the statement's own row count, a planner estimate or GDS's numbers. They are
// abstract logical operations, not disk reads or page-cache hits; see
// https://neo4j.com/docs/cypher-manual/current/planning-and-tuning/execution-plans/
// for Neo4j's own operator counters, which these are not copies of.
//
// Each case runs the statement through the public entry point with the
// collector PROFILE's owner installs in the context, on both parsers. The
// fixtures are the existing GDS and composite ones, so the values asserted are
// the ones those suites already check.

// compositeGraphCatalogKind is the record name the fn package's one composite
// catalog acquisition reports (pkg/cypher/fn, profileKindCompositeGraphCatalog):
// the graphs the catalog listed, before the function applies the statement's
// visibility. The constant is unexported there, so the public path asserts the
// name the plan reports.
const compositeGraphCatalogKind = "CompositeGraphCatalog"

// gdsAccess is one record as these controls compare it: the logical operation,
// the scope the request selected, the population it returned and the access
// units it spent.
type gdsAccess struct {
	kind, detail string
	rows, hits   int64
}

// profileGDSAccessesOfKinds returns the collector's records of the named kinds,
// in the order the statement first reported them.
func profileGDSAccessesOfKinds(collector *profileRuntimeCollector, kinds ...string) []gdsAccess {
	wanted := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		wanted[kind] = struct{}{}
	}
	var found []gdsAccess
	for _, observation := range collector.accessObservation() {
		if _, ok := wanted[observation.Kind]; ok {
			found = append(found, gdsAccess{
				kind: observation.Kind, detail: observation.Detail,
				rows: observation.Rows, hits: observation.Hits,
			})
		}
	}
	return found
}

// profileGDSRowsAndAccesses executes query once through the public entry point
// with the collector PROFILE's owner installs in ctx, and returns the statement
// rows and the collector. It is the observed run alone, so a statement whose
// rows hold NaN - which no two runs compare equal - can still be observed.
func profileGDSRowsAndAccesses(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) ([][]interface{}, *profileRuntimeCollector) {
	t.Helper()
	collector := newProfileRuntimeCollector()
	result, err := exec.Execute(withProfileRuntimeCollector(ctx, collector), query, nil)
	require.NoError(t, err, query)
	require.NotNil(t, result, query)
	return result.Rows, collector
}

// requireExpandAccesses asserts exactly the Expand records the statement's
// adjacency requests left, keyed by the direction each request named: a missing
// request, a fabricated one and a different direction all fail.
func requireExpandAccesses(t *testing.T, collector *profileRuntimeCollector, want map[string]gdsAccess) {
	t.Helper()
	expands := profileGDSAccessesOfKinds(collector, profileKindExpand)
	require.Len(t, expands, len(want), "one record per direction the statement's requests selected: %v", expands)
	for _, expand := range expands {
		expected, recorded := want[expand.detail]
		require.True(t, recorded, "unexpected Expand request scope %q", expand.detail)
		require.Equal(t, profileKindExpand, expand.kind)
		require.EqualValues(t, expected.rows, expand.rows,
			"%q returned %d adjacency entries", expand.detail, expected.rows)
		require.EqualValues(t, expected.hits, expand.hits,
			"%q read %d adjacency entries", expand.detail, expected.hits)
	}
}

// TestProfileGDSLinkPredictionCountsAdjacencyRequests: a link-prediction call
// reports each adjacency request its neighbourhoods made, with the entries that
// request returned, and no access record stands in for the statement's one row.
func TestProfileGDSLinkPredictionCountsAdjacencyRequests(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		// One more :K edge gives a and c a common neighbour, so the call also
		// reads the common neighbour's own degree.
		_, err := run("MATCH (a:P {name:'a'}), (c:P {name:'c'}) CREATE (a)-[:K]->(c)")
		require.NoError(t, err)

		query := "MATCH (x:P {name:'a'}), (y:P {name:'c'}) RETURN gds.alpha.linkprediction.commonNeighbors(x, y) AS score"
		rows, collector := profileReaderAccesses(t, exec, query)
		require.Equal(t, [][]interface{}{{1.0}}, rows, "a and c share the common neighbour b")

		// a reads 3 outgoing (a→b, a→c, a→a) and 1 incoming (the self-loop),
		// c reads 0 outgoing and 2 incoming (b→c, a→c), and the common
		// neighbour b reads 1 outgoing (b→c) and 1 incoming (a→b).
		requireExpandAccesses(t, collector, map[string]gdsAccess{
			"outgoing": {rows: 4, hits: 4},
			"incoming": {rows: 4, hits: 4},
		})

		// The registration's whole access is those adjacency reads, so a
		// PROFILE statement certifies its total instead of reporting an opaque
		// callback.
		result, err := exec.Execute(t.Context(), "PROFILE "+query, nil)
		require.NoError(t, err)
		plan := executionPlanFromResult(t, result)
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		require.NotNil(t, planOperatorOfType(plan.Root, profileKindExpand), "the plan keeps the adjacency requests")
	})
}

// TestProfileGDSLinkPredictionKeepsRequestDirection: a config's direction
// selects which requests the call makes, and the record names the direction the
// read selected - never the function's configured direction on a request it
// did not make.
func TestProfileGDSLinkPredictionKeepsRequestDirection(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("MATCH (a:P {name:'a'}), (c:P {name:'c'}) CREATE (a)-[:K]->(c)")
		require.NoError(t, err)

		t.Run("INCOMING reads only incoming adjacency", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec,
				"MATCH (x:P {name:'b'}), (y:P {name:'c'}) RETURN gds.alpha.linkprediction.commonNeighbors(x, y, {direction: 'INCOMING'}) AS score")
			require.Equal(t, [][]interface{}{{1.0}}, rows, "under INCOMING b and c share the common neighbour a")
			// b reads 1 incoming (a→b), c reads 2 (b→c, a→c), and the common
			// neighbour a reads 1 (its self-loop). No outgoing read is made.
			requireExpandAccesses(t, collector, map[string]gdsAccess{
				"incoming": {rows: 4, hits: 4},
			})
		})

		t.Run("OUTGOING reads only outgoing adjacency", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec,
				"MATCH (x:P {name:'b'}), (y:P {name:'c'}) RETURN gds.alpha.linkprediction.commonNeighbors(x, y, {direction: 'OUTGOING'}) AS score")
			require.Equal(t, [][]interface{}{{0.0}}, rows, "under OUTGOING b and c share no neighbour")
			// b reads 1 outgoing (b→c); c reads none, so there is no common
			// neighbour and no degree read.
			requireExpandAccesses(t, collector, map[string]gdsAccess{
				"outgoing": {rows: 1, hits: 1},
			})
		})
	})
}

// TestProfileGDSLinkPredictionCountsReadsBeforeItsTypeFilter: a
// relationshipQuery narrows the neighbourhood after the read, so the record
// keeps every entry the read returned, including the entries the filter
// rejected. The score still follows the filtered neighbourhood.
func TestProfileGDSLinkPredictionCountsReadsBeforeItsTypeFilter(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("MATCH (a:P {name:'a'}), (c:P {name:'c'}) CREATE (a)-[:K]->(c)")
		require.NoError(t, err)
		// A second type on a is read by the same unordered request and then
		// filtered out of the neighbourhood.
		_, err = run("MATCH (a:P {name:'a'}), (i:P {name:'iso'}) CREATE (a)-[:L]->(i)")
		require.NoError(t, err)

		rows, collector := profileReaderAccesses(t, exec,
			"MATCH (x:P {name:'a'}), (y:P {name:'iso'}) RETURN gds.alpha.linkprediction.totalNeighbors(x, y, {relationshipQuery: 'K'}) AS total")
		require.Equal(t, [][]interface{}{{2.0}}, rows, "only :K neighbours count: b and c")

		// a's outgoing request returned 4 entries - a→b, a→c, a→a and the :L
		// a→iso the filter rejects - and iso's incoming request returned 1.
		requireExpandAccesses(t, collector, map[string]gdsAccess{
			"outgoing": {rows: 4, hits: 4},
			"incoming": {rows: 2, hits: 2},
		})
	})
}

// TestProfileGDSLinkPredictionCountsAnEmptyAdjacencyRead: a request that
// returned no entry is still the request that ran: the record is a measured
// zero, not a missing record and not a fabricated row.
func TestProfileGDSLinkPredictionCountsAnEmptyAdjacencyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newGDSGraph(t)

		rows, collector := profileReaderAccesses(t, exec,
			"MATCH (i:P {name:'iso'}) RETURN gds.alpha.linkprediction.commonNeighbors(i, i) AS score")
		require.Equal(t, [][]interface{}{{0.0}}, rows, "a node and itself share no neighbours")

		requireExpandAccesses(t, collector, map[string]gdsAccess{
			"outgoing": {rows: 0, hits: 0},
			"incoming": {rows: 0, hits: 0},
		})
	})
}

// TestProfileGDSLinkPredictionKeepsSelfLoopAndParallelWork: the neighbourhood
// counts a self-loop once and a neighbour once, but the reads that produced it
// are reported as they ran - two parallel relationships are two entries, and a
// self-loop is returned by both directional requests.
func TestProfileGDSLinkPredictionKeepsSelfLoopAndParallelWork(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("MATCH (a:P {name:'a'}), (b:P {name:'b'}) CREATE (a)-[:K]->(b)")
		require.NoError(t, err)

		// a's outgoing request returned the two parallel a→b entries and the
		// self-loop; the self-loop is also what a's incoming request returned.
		// b reads its one outgoing edge and both incoming parallel entries.
		// Each call makes its own set of requests, so one function per
		// statement keeps the records readable.
		rows, collector := profileReaderAccesses(t, exec,
			"MATCH (x:P {name:'a'}), (y:P {name:'b'}) RETURN gds.alpha.linkprediction.totalNeighbors(x, y) AS total")
		require.Equal(t, [][]interface{}{{3.0}}, rows, "the union contains a, b and c despite duplicate relationships")
		requireExpandAccesses(t, collector, map[string]gdsAccess{
			"outgoing": {rows: 4, hits: 4},
			"incoming": {rows: 3, hits: 3},
		})

		// The degree counts the parallel relationship again and the self-loop
		// once: 3 each.
		rows, collector = profileReaderAccesses(t, exec,
			"MATCH (x:P {name:'a'}), (y:P {name:'b'}) RETURN gds.alpha.linkprediction.preferentialAttachment(x, y) AS preferential")
		require.Equal(t, [][]interface{}{{9.0}}, rows)
		requireExpandAccesses(t, collector, map[string]gdsAccess{
			"outgoing": {rows: 4, hits: 4},
			"incoming": {rows: 3, hits: 3},
		})
	})
}

// requireNodePropertyRows asserts the name/age rows the fixture's projection
// answers: a's projected age, and GDS's NaN for the node that projected none.
func requireNodePropertyRows(t *testing.T, rows [][]interface{}) {
	t.Helper()
	require.Len(t, rows, 2)
	require.Equal(t, "a", rows[0][0])
	require.Equal(t, 1.0, rows[0][1])
	require.Equal(t, "c", rows[1][0])
	value, ok := rows[1][1].(float64)
	require.True(t, ok, "an absent projected value is GDS's missing-value Double")
	require.True(t, math.IsNaN(value))
}

// TestProfileGDSGraphCatalogReads: gds.graph.exists and gds.util.nodeProperty
// report the projection catalog lookup they made and the property entry they
// read - not the statement's rows, and not one record per matched node.
func TestProfileGDSGraphCatalogReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("CALL gds.graph.project('g', {P: {properties: 'age'}}, {K: {properties: 'weight'}}, {})")
		require.NoError(t, err)
		lookupKinds := []string{profileKindGraphCatalogSeek, profileKindProjectedPropertySeek}

		t.Run("exists reports the projection its lookup returned", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec, "RETURN gds.graph.exists('g') AS present")
			require.Equal(t, [][]interface{}{{true}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "g", rows: 1, hits: 1},
			}, profileGDSAccessesOfKinds(collector, lookupKinds...))
		})

		t.Run("a missing projection is one lookup that returned none", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec, "RETURN gds.graph.exists('nope') AS present")
			require.Equal(t, [][]interface{}{{false}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "nope", rows: 0, hits: 1},
			}, profileGDSAccessesOfKinds(collector, lookupKinds...))
		})

		t.Run("nodeProperty reports one lookup per row and one property entry", func(t *testing.T) {
			// Two matched rows, so the catalog is asked twice and two property
			// requests are answered; only a holds a projected age.
			query := "MATCH (n:P) WHERE n.name IN ['a','c'] RETURN n.name AS name, gds.util.nodeProperty('g', n, 'age') AS value ORDER BY name"
			rows, collector := profileGDSRowsAndAccesses(t, exec, context.Background(), query)
			requireNodePropertyRows(t, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "g", rows: 2, hits: 2},
				{kind: profileKindProjectedPropertySeek, detail: "age", rows: 1, hits: 2},
			}, profileGDSAccessesOfKinds(collector, lookupKinds...))
		})

		t.Run("a node the graph does not hold reads no property", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec, "RETURN gds.util.nodeProperty('g', 'no-such-node', 'age') AS value")
			require.Equal(t, [][]interface{}{{nil}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "g", rows: 1, hits: 1},
			}, profileGDSAccessesOfKinds(collector, lookupKinds...))
		})

		t.Run("a missing graph fails instead of reading a property", func(t *testing.T) {
			_, err := exec.Execute(context.Background(), "RETURN gds.util.nodeProperty('nope', 'a', 'age') AS value", nil)
			require.ErrorContains(t, err, "Graph with name `nope` does not exist on database `gds`")
		})

		t.Run("a PROFILE statement certifies the catalog lookup's hits", func(t *testing.T) {
			for _, tc := range []struct {
				query       string
				rows        [][]interface{}
				rowReturned int64
			}{
				{"PROFILE RETURN gds.graph.exists('g') AS present", [][]interface{}{{true}}, 1},
				{"PROFILE RETURN gds.graph.exists('nope') AS present", [][]interface{}{{false}}, 0},
			} {
				result, err := exec.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
				plan := executionPlanFromResult(t, result)
				require.True(t, plan.TotalDBHitsObserved, tc.query)
				require.Empty(t, plan.Unobserved, tc.query)
				require.EqualValues(t, 1, plan.TotalDBHits, "one catalog lookup is one hit: %s", tc.query)
				lookup := planOperatorOfType(plan.Root, profileKindGraphCatalogSeek)
				require.NotNil(t, lookup, tc.query)
				require.True(t, lookup.RowsObserved, tc.query)
				require.EqualValues(t, tc.rowReturned, lookup.ActualRows, "the lookup's population: %s", tc.query)
				require.EqualValues(t, 1, lookup.DBHits, tc.query)
			}
		})
	})
}

// TestProfileCompositeGraphCatalogReads: graph.names() and
// graph.propertiesByName() report the one composite catalog acquisition they
// share, with the graphs the catalog listed, before either function filters
// them by the statement's visibility.
func TestProfileCompositeGraphCatalogReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newCompositeFixture(t)
		catalogKinds := []string{compositeGraphCatalogKind}

		t.Run("graph.names lists what the catalog returned", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec, "RETURN graph.names() AS graphs")
			require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: compositeGraphCatalogKind, rows: 2, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...))
		})

		t.Run("graph.propertiesByName acquires the same catalog once", func(t *testing.T) {
			rows, collector := profileReaderAccesses(t, exec, "RETURN graph.propertiesByName('pcomp.other') AS properties")
			require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: compositeGraphCatalogKind, rows: 2, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...))
		})

		t.Run("a visible-graph filter is applied after the acquisition", func(t *testing.T) {
			// Only the composite's own database is visible, so both graphs are
			// filtered out of the row; the acquisition still listed them.
			ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
				return database == "pcomp"
			})
			rows, collector := profileGDSRowsAndAccesses(t, exec, ctx, "RETURN graph.names() AS graphs")
			require.Equal(t, [][]interface{}{{[]interface{}{}}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: compositeGraphCatalogKind, rows: 2, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...))
		})

		t.Run("a PROFILE statement certifies the catalog acquisition", func(t *testing.T) {
			result, err := exec.Execute(t.Context(), "PROFILE RETURN graph.names() AS graphs", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, result.Rows)
			plan := executionPlanFromResult(t, result)
			require.True(t, plan.TotalDBHitsObserved)
			require.Empty(t, plan.Unobserved)
			require.EqualValues(t, 1, plan.TotalDBHits)
			acquisition := planOperatorOfType(plan.Root, compositeGraphCatalogKind)
			require.NotNil(t, acquisition)
			require.EqualValues(t, 2, acquisition.ActualRows, "the graphs the catalog listed")
			require.EqualValues(t, 1, acquisition.DBHits)
		})
	})
}

// TestProfileGDSRegistrationReportsCustomCallback: the policy belongs to the
// registration, not to the name. A custom callback registered under a canonical
// GDS name reads storage outside every observation owner, so the statement's
// total is unobserved - while the built-in under that name is certified.
func TestProfileGDSRegistrationReportsCustomCallback(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("CALL gds.graph.project('g', {P: {properties: 'age'}}, {K: {properties: 'weight'}}, {})")
		require.NoError(t, err)

		const query = "PROFILE RETURN gds.graph.exists('g') AS present"
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		builtin := executionPlanFromResult(t, result)
		require.True(t, builtin.TotalDBHitsObserved, "the built-in catalog read is certified")
		require.Empty(t, builtin.Unobserved)

		t.Cleanup(func() { cypherfn.Register("gds.graph.exists", fnGDSGraphExists, cypherfn.CanonicalReads) })
		cypherfn.Register("gds.graph.exists", func(workCtx context.Context, ctx cypherfn.Context, args []string) (interface{}, error) {
			store, err := ctx.GraphStorage()
			if err != nil {
				return nil, err
			}
			// A read no observation owner sees: the registry cannot certify it.
			_, _ = store.GetNode(workCtx, storage.NodeID("absent"))
			return true, nil
		}, cypherfn.OpaqueReads)

		result, err = exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved, "a custom callback under a built-in name stays opaque")
		require.Zero(t, plan.TotalDBHits, "the callback's read is never certified")
		require.Equal(t, []string{"database reads: registered function callback: gds.graph.exists"}, plan.Unobserved)
	})
}

// TestProfileGDSGraphCatalogProceduresReportTheCatalogTheyRead: gds.graph.exists
// and gds.graph.list read this database's projection catalog alone, so a
// PROFILE statement reports the one lookup or acquisition each made with the
// population the catalog returned: the projection a lookup found (1, or 0 when
// the catalog holds none), the projections a list acquisition listed. A list's
// own name filter narrows those rows afterwards, so the record keeps the
// catalog's population and never the rows the statement emitted.
func TestProfileGDSGraphCatalogProceduresReportTheCatalogTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		for _, name := range []string{"first", "second"} {
			_, err := run("CALL gds.graph.project('" + name + "', 'P', 'K')")
			require.NoError(t, err)
		}
		catalogKinds := []string{profileKindGraphCatalogSeek, profileKindGraphProjectionCatalog}

		t.Run("exists reports the projection its lookup returned", func(t *testing.T) {
			statement := "CALL gds.graph.exists('first') YIELD graphName, exists AS present RETURN graphName, present"
			rows, collector := profileReaderAccesses(t, exec, statement)
			require.Equal(t, [][]interface{}{{"first", true}}, rows, "one boolean for the graph the catalog holds")
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "first", rows: 1, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...),
				"one lookup, whatever the statement does with the answer")
			requireGDSGraphCatalogProcedurePlan(t, exec, statement, rows, profileKindGraphCatalogSeek, 1)
		})

		t.Run("a missing projection is one lookup that returned none", func(t *testing.T) {
			statement := "CALL gds.graph.exists('absent') YIELD graphName, exists AS present RETURN graphName, present"
			rows, collector := profileReaderAccesses(t, exec, statement)
			require.Equal(t, [][]interface{}{{"absent", false}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "absent", rows: 0, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...),
				"the lookup ran even though the catalog held nothing")
			requireGDSGraphCatalogProcedurePlan(t, exec, statement, rows, profileKindGraphCatalogSeek, 0)
		})

		t.Run("list reports the catalog acquisition, not its rows", func(t *testing.T) {
			statement := "CALL gds.graph.list() YIELD graphName RETURN graphName"
			rows, collector := profileReaderAccesses(t, exec, statement)
			require.Equal(t, [][]interface{}{{"first"}, {"second"}}, rows)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphProjectionCatalog, detail: "gds", rows: 2, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...),
				"one acquisition of this database's catalog")
			requireGDSGraphCatalogProcedurePlan(t, exec, statement, rows, profileKindGraphProjectionCatalog, 2)
		})

		t.Run("a name filter narrows the listed rows after the acquisition", func(t *testing.T) {
			statement := "CALL gds.graph.list('second') YIELD graphName RETURN graphName"
			rows, collector := profileReaderAccesses(t, exec, statement)
			require.Equal(t, [][]interface{}{{"second"}}, rows, "the named projection is the one row emitted")
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphProjectionCatalog, detail: "gds", rows: 2, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...),
				"the acquisition listed both projections; the filter then emitted one row")
			requireGDSGraphCatalogProcedurePlan(t, exec, statement, rows, profileKindGraphProjectionCatalog, 2)
		})

		t.Run("a scoped catalog holding no projections is an observed zero", func(t *testing.T) {
			empty := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "emptycatalog"))
			statement := "CALL gds.graph.list() YIELD graphName RETURN graphName"
			rows, collector := profileReaderAccesses(t, empty, statement)
			require.Empty(t, rows, "no projection is scoped to this database")
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphProjectionCatalog, detail: "emptycatalog", rows: 0, hits: 1},
			}, profileGDSAccessesOfKinds(collector, catalogKinds...))
			requireGDSGraphCatalogProcedurePlan(t, empty, statement, rows, profileKindGraphProjectionCatalog, 0)
		})
	})
}

// requireGDSGraphCatalogProcedurePlan profiles statement and holds it to the
// one catalog record it reports: kind, with wantRows as the population the
// catalog returned and the one request as its hits. The statement's whole
// database access is that request, so its total is certified and equals it, and
// no record of the other catalog kind may stand in for it.
func requireGDSGraphCatalogProcedurePlan(t *testing.T, exec *StorageExecutor, statement string, rows [][]interface{}, kind string, wantRows int64) {
	t.Helper()
	result, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
	require.NoError(t, err, statement)
	require.Equal(t, rows, result.Rows, "%s answers the same rows under PROFILE", statement)
	plan := executionPlanFromResult(t, result)
	require.True(t, plan.TotalDBHitsObserved, statement)
	require.Empty(t, plan.Unobserved, statement)
	require.EqualValues(t, 1, plan.TotalDBHits, "%s made exactly the one catalog request it reports", statement)
	for _, catalogKind := range []string{profileKindGraphCatalogSeek, profileKindGraphProjectionCatalog} {
		operator := planOperatorOfType(plan.Root, catalogKind)
		if catalogKind != kind {
			require.Nil(t, operator, "%s reports no %s", statement, catalogKind)
			continue
		}
		require.NotNil(t, operator, statement)
		require.True(t, operator.RowsObserved, statement)
		require.EqualValues(t, wantRows, operator.ActualRows,
			"%s reports the population the catalog returned", statement)
		require.EqualValues(t, 1, operator.DBHits, statement)
	}
}

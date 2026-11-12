package cypher

// PROFILE controls for the GDS algorithm procedures.
//
// gds.version answers from constants, so a PROFILE statement records no
// database access at all and its total is a certified zero. gds.fastRP.stream
// and gds.fastRP.stats read only the projection their graphName names - one
// catalog lookup, the projection the catalog returned - and then compute the
// embeddings on the CPU over that retained projection, so their PROFILE total
// is that one lookup however many nodes and relationships the call walks.
//
// Each case runs the statement through the public entry point with the collector
// PROFILE's owner installs in the context, on both parsers, over the existing
// newGDSGraph fixture. Records are the logical operations the handlers actually
// performed - never the statement's row count, a planner estimate, or a second
// parse of the statement - and only a registration's own declared policy can
// certify them: a replacement under either name stays opaque.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// gdsFixtureNodeNames returns the fixture's node names by the native node id a
// FastRP row reports as nodeId.
func gdsFixtureNodeNames(t *testing.T, exec *StorageExecutor) map[string]string {
	t.Helper()
	result, err := exec.Execute(context.Background(), "MATCH (n:P) RETURN id(n) AS id, n.name AS name", nil)
	require.NoError(t, err)
	names := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		names[row[0].(string)] = row[1].(string)
	}
	return names
}

// gdsRecordsOf lists every access record the statement kept, in the order it
// first reported them, including the explicit unobserved boundary records.
// Known catalog readers must have exactly their named acquisition records.
func gdsRecordsOf(collector *profileRuntimeCollector) []gdsAccess {
	observations := collector.accessObservation()
	records := make([]gdsAccess, 0, len(observations))
	for _, observation := range observations {
		records = append(records, gdsAccess{
			kind: observation.Kind, detail: observation.Detail,
			rows: observation.Rows, hits: observation.Hits,
		})
	}
	return records
}

// requireGDSFastRPCatalogSeekPlan holds a FastRP call's PROFILE plan to the one
// catalog lookup its handler made: the graphName it asked for, the one
// projection that lookup found, and no other database access beside it.
func requireGDSFastRPCatalogSeekPlan(t *testing.T, plan *ExecutionPlan, statement, graphName string) {
	t.Helper()
	require.True(t, plan.TotalDBHitsObserved, statement)
	require.Empty(t, plan.Unobserved, statement)
	require.EqualValues(t, 1, plan.TotalDBHits,
		"%s made exactly the one catalog lookup it reports", statement)
	seek := planOperatorOfType(plan.Root, profileKindGraphCatalogSeek)
	require.NotNil(t, seek, "%s reports its catalog lookup", statement)
	require.Equal(t, graphName, seek.Description,
		"%s reports the graphName it looked up", statement)
	require.True(t, seek.RowsObserved, statement)
	require.EqualValues(t, 1, seek.ActualRows, "%s found the projection", statement)
	require.True(t, seek.DBHitsObserved, statement)
	require.EqualValues(t, 1, seek.DBHits, statement)
	require.Nil(t, planOperatorOfType(plan.Root, profileKindGraphProjectionCatalog),
		"%s lists no catalog: the one lookup is its whole catalog read", statement)
}

// profileGDSFastRPCall runs statement under PROFILE, holds its plan to the one
// catalog lookup a FastRP call makes of graphName, and returns the result.
func profileGDSFastRPCall(t *testing.T, exec *StorageExecutor, statement, graphName string) *ExecuteResult {
	t.Helper()
	result, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
	require.NoError(t, err, statement)
	requireGDSFastRPCatalogSeekPlan(t, executionPlanFromResult(t, result), statement, graphName)
	return result
}

// TestProfileGDSVersionReportsAConstant: gds.version answers from constants, so
// a PROFILE statement records no access at all - a measured, certified zero -
// and the whole result is the version constant.
func TestProfileGDSVersionReportsAConstant(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newGDSGraph(t)
		const statement = "CALL gds.version() YIELD gdsVersion RETURN gdsVersion"

		rows, collector := profileReaderAccesses(t, exec, statement)
		require.Equal(t, [][]interface{}{{gdsVersion}}, rows, "the version constant is the whole result")
		require.Empty(t, gdsRecordsOf(collector), "gds.version reads no graph state")

		result, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
		require.NoError(t, err)
		require.Equal(t, rows, result.Rows, "PROFILE changes nothing the statement returns")
		plan := executionPlanFromResult(t, result)
		require.True(t, plan.TotalDBHitsObserved, "the constant is fully observed")
		require.Zero(t, plan.TotalDBHits, "a constant reads nothing")
		require.Empty(t, plan.Unobserved)
		require.Nil(t, planOperatorOfType(plan.Root, profileKindGraphCatalogSeek))
		require.Nil(t, planOperatorOfType(plan.Root, profileKindGraphProjectionCatalog))
	})
}

// TestProfileGDSFastRPStreamReportsOneCatalogSeek: gds.fastRP.stream reports the
// one lookup of the projection it was given and nothing else, and its rows are
// the projection's nodes in the projection's node order with the embeddings
// FastRP gives them from the retained projection.
func TestProfileGDSFastRPStreamReportsOneCatalogSeek(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("CALL gds.graph.project('g', {P: {properties: 'age'}}, 'K')")
		require.NoError(t, err)
		projection, err := lookupGraphProjection(t.Context(), storageDatabaseName(exec.storage), "g")
		require.NoError(t, err)

		const statement = "CALL gds.fastRP.stream('g', {embeddingDimension: 4, randomSeed: 7}) YIELD nodeId, embedding RETURN nodeId, embedding"
		rows, collector := profileReaderAccesses(t, exec, statement)

		require.Len(t, rows, len(projection.NodeIDs), "one row per projected node")
		names := gdsFixtureNodeNames(t, exec)
		embeddings := make(map[string][]float64, len(rows))
		for index, row := range rows {
			require.Equal(t, projection.NodeIDs[index], row[0].(string),
				"row %d is the projection's node %d, in the projection's node order", index, index)
			embedding, ok := row[1].([]float64)
			require.True(t, ok, "row %d carries a dense embedding", index)
			require.Len(t, embedding, 4, "the requested embeddingDimension")
			embeddings[names[row[0].(string)]] = embedding
		}
		// Over a→b→c and a→a nothing propagates to b, c or the isolated node, so
		// their embeddings are exactly zero; a's is not.
		require.Len(t, embeddings, 4)
		zero := []float64{0, 0, 0, 0}
		require.Equal(t, zero, embeddings["b"])
		require.Equal(t, zero, embeddings["c"])
		require.Equal(t, zero, embeddings["iso"])
		require.NotEqual(t, zero, embeddings["a"])

		require.Equal(t, []gdsAccess{
			{kind: profileKindGraphCatalogSeek, detail: "g", rows: 1, hits: 1},
		}, gdsRecordsOf(collector),
			"the embeddings are CPU work over the projection the one lookup returned")

		require.Equal(t, rows, profileGDSFastRPCall(t, exec, statement, "g").Rows,
			"PROFILE answers the same rows")
	})
}

// TestProfileGDSFastRPStatsReportsOneCatalogSeek: gds.fastRP.stats runs the same
// FastRP over the same one lookup and reports GDS's node count and
// configuration. Only its two timing columns are run-specific, and they are the
// only fields these two runs may differ in.
func TestProfileGDSFastRPStatsReportsOneCatalogSeek(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)
		_, err := run("CALL gds.graph.project('g', {P: {properties: 'age'}}, 'K')")
		require.NoError(t, err)

		const statement = "CALL gds.fastRP.stats('g', {embeddingDimension: 4, randomSeed: 7, jobId: 'profile'})"
		plain, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"nodeCount", "preProcessingMillis", "computeMillis", "configuration"}, plain.Columns)
		require.Len(t, plain.Rows, 1)

		collector := newProfileRuntimeCollector()
		observed, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, nil)
		require.NoError(t, err)
		require.Equal(t, plain.Columns, observed.Columns)
		require.Len(t, observed.Rows, 1)
		requireGDSFastRPStatsRowEqualApartFromTiming(t, plain.Rows[0], observed.Rows[0])

		require.Equal(t, int64(4), observed.Rows[0][0], "the four projected nodes")
		require.Equal(t, map[string]interface{}{
			"embeddingDimension":    int64(4),
			"iterationWeights":      []interface{}{0.0, 1.0, 1.0},
			"normalizationStrength": 0.0,
			"nodeSelfInfluence":     0.0,
			"propertyRatio":         0.0,
			"featureProperties":     []interface{}{},
			"nodeLabels":            []interface{}{"*"},
			"relationshipTypes":     []interface{}{"*"},
			"randomSeed":            int64(7),
			"concurrency":           int64(4),
			"logProgress":           true,
			"sudo":                  false,
			"jobId":                 "profile",
		}, observed.Rows[0][3], "stats reports the configuration it ran")

		require.Equal(t, []gdsAccess{
			{kind: profileKindGraphCatalogSeek, detail: "g", rows: 1, hits: 1},
		}, gdsRecordsOf(collector),
			"the node count and configuration come from the projection the one lookup returned")

		result := profileGDSFastRPCall(t, exec, statement, "g")
		require.Len(t, result.Rows, 1)
		requireGDSFastRPStatsRowEqualApartFromTiming(t, plain.Rows[0], result.Rows[0])
	})
}

// requireGDSFastRPStatsRowEqualApartFromTiming compares two gds.fastRP.stats
// rows field by field: the node count and the configuration are equal, and the
// two timing columns are each a measured, non-negative integer that these
// assertions do not compare between the runs.
func requireGDSFastRPStatsRowEqualApartFromTiming(t *testing.T, plain, observed []interface{}) {
	t.Helper()
	require.Len(t, plain, 4)
	require.Len(t, observed, 4)
	require.Equal(t, plain[0], observed[0], "the node count is the same")
	require.Equal(t, plain[3], observed[3], "the configuration is the same")
	for _, timing := range []struct {
		column int
		name   string
	}{{1, "preProcessingMillis"}, {2, "computeMillis"}} {
		for run, row := range map[string][]interface{}{"plain": plain, "profile": observed} {
			millis, ok := row[timing.column].(int64)
			require.True(t, ok, "%s %s is a measured integer", run, timing.name)
			require.GreaterOrEqual(t, millis, int64(0), "%s %s", run, timing.name)
		}
	}
}

// TestProfileGDSFastRPAppliesOptionsAfterTheOneCatalogSeek: an option narrows
// what FastRP computes from the projection the one lookup returned - a feature
// property, a label filter, a type filter - and never adds a second catalog
// request or a per-node one.
func TestProfileGDSFastRPAppliesOptionsAfterTheOneCatalogSeek(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, run := newGDSGraph(t)

		t.Run("feature property", func(t *testing.T) {
			_, err := run("CREATE (m:X {f: 0.5}), (n:X {f: 1.5}), (m)-[:F]->(n)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('features', {X: {properties: 'f'}}, 'F')")
			require.NoError(t, err)

			const statement = "CALL gds.fastRP.stream('features', {embeddingDimension: 4, propertyRatio: 0.5, featureProperties: ['f'], randomSeed: 7, nodeLabels: ['X'], relationshipTypes: ['F']}) YIELD nodeId, embedding RETURN nodeId, embedding"
			rows, collector := profileReaderAccesses(t, exec, statement)
			require.Len(t, rows, 2, "the projection's two :X nodes")
			for index, row := range rows {
				embedding, ok := row[1].([]float64)
				require.True(t, ok, "row %d carries a dense embedding", index)
				require.Len(t, embedding, 4, "the requested embeddingDimension")
			}
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "features", rows: 1, hits: 1},
			}, gdsRecordsOf(collector), "the feature subspace is read from the retained projection")
			require.Equal(t, rows, profileGDSFastRPCall(t, exec, statement, "features").Rows)
		})

		t.Run("label and type filters", func(t *testing.T) {
			_, err := run("CREATE (q:Q {name:'q'})")
			require.NoError(t, err)
			_, err = run("MATCH (c:P {name:'c'}), (q:Q {name:'q'}) CREATE (c)-[:L]->(q)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('filters', {P: {}, Q: {}}, {K: {}, L: {}})")
			require.NoError(t, err)

			// Unfiltered, the call walks every projected node: the filters change
			// what it computes, never how often it reads the catalog.
			const unfiltered = "CALL gds.fastRP.stream('filters', {embeddingDimension: 3, randomSeed: 7}) YIELD nodeId, embedding RETURN nodeId, embedding"
			rows, collector := profileReaderAccesses(t, exec, unfiltered)
			require.Len(t, rows, 5, "the projection's four :P nodes and its :Q node")
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "filters", rows: 1, hits: 1},
			}, gdsRecordsOf(collector))
			require.Equal(t, rows, profileGDSFastRPCall(t, exec, unfiltered, "filters").Rows)

			const filtered = "CALL gds.fastRP.stream('filters', {embeddingDimension: 3, randomSeed: 7, nodeLabels: ['Q'], relationshipTypes: ['K']}) YIELD nodeId, embedding RETURN nodeId, embedding"
			rows, collector = profileReaderAccesses(t, exec, filtered)
			require.Len(t, rows, 1, "only the :Q node is selected")
			embedding, ok := rows[0][1].([]float64)
			require.True(t, ok)
			// Nothing of type :K leaves q, so its whole embedding stays zero.
			require.Equal(t, []float64{0, 0, 0}, embedding)
			require.Equal(t, []gdsAccess{
				{kind: profileKindGraphCatalogSeek, detail: "filters", rows: 1, hits: 1},
			}, gdsRecordsOf(collector), "the filters are applied to the projection, not looked up")
			require.Equal(t, rows, profileGDSFastRPCall(t, exec, filtered, "filters").Rows)
		})
	})
}

// TestProfileGDSFastRPMissingProjectionFailsTheSameWay: a graphName the catalog
// does not hold is one lookup that found nothing, and the call fails with GDS's
// error in both modes - PROFILE never turns a failed lookup into a row or an
// unobserved success.
func TestProfileGDSFastRPMissingProjectionFailsTheSameWay(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newGDSGraph(t)
		const message = "Graph with name `absent` does not exist on database `gds`. It might exist on another database."
		for _, procedure := range []string{"gds.fastRP.stream", "gds.fastRP.stats"} {
			t.Run(procedure, func(t *testing.T) {
				statement := "CALL " + procedure + "('absent', {embeddingDimension: 4})"
				_, plainErr := exec.Execute(context.Background(), statement, nil)
				require.ErrorContains(t, plainErr, message)

				collector := newProfileRuntimeCollector()
				_, observedErr := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, nil)
				require.EqualError(t, observedErr, plainErr.Error())
				_, profiledErr := exec.Execute(t.Context(), "PROFILE "+statement, nil)
				require.ErrorContains(t, profiledErr, message)
				require.Equal(t, plainErr.Error(), profiledErr.Error(), "PROFILE reports the same failure")

				require.Equal(t, []gdsAccess{
					{kind: profileKindGraphCatalogSeek, detail: "absent", rows: 0, hits: 1},
				}, gdsRecordsOf(collector), "the lookup ran even though the catalog held no such projection")
			})
		}
	})
}

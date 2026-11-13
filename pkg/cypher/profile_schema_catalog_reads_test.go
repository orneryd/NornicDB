package cypher

// db.indexes, db.index.stats and db.constraints read this database's own
// catalogs and report each acquisition through the canonical access owner, so a
// PROFILE statement states the population each one actually read instead of
// calling it unknown. Every other procedure stays opaque, and so does a
// replacement of one of these three registered through the exported API: a name
// carries no observation trust.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// schemaCatalogExecutor returns an executor over one property index, one
// uniqueness constraint and one property type constraint: the index, constraint
// and property-type-constraint catalogs each hold an entry. db.constraints
// answers with both constraint catalogs as one merged row list, so no single
// statement row count can stand in for the populations the procedure read.
func schemaCatalogExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, _ := newProfileRuntimeTestExecutor(t)
	for _, statement := range []string{
		"CREATE INDEX idx_schema_catalog_name IF NOT EXISTS FOR (n:Person) ON (n.name)",
		"CREATE CONSTRAINT c_schema_catalog_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT c_schema_catalog_born IF NOT EXISTS FOR (n:Person) REQUIRE n.born IS :: DATE",
	} {
		_, err := exec.Execute(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}
	return exec
}

func TestProfileSchemaCatalogProceduresReportTheCatalogsTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := schemaCatalogExecutor(t)
		schema := exec.storage.GetSchema()

		indexes := int64(len(schema.GetIndexes()))
		require.Positive(t, indexes)
		result, plan := profileEquivalentObservedStatement(t, exec,
			"CALL db.indexes() YIELD name, type, state RETURN name, type, state")
		require.Len(t, result.Rows, int(indexes), "one row per index entry")
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		catalog := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
		require.NotNil(t, catalog, "the index catalog read is reported as a plan operator")
		require.True(t, catalog.RowsObserved)
		require.Equal(t, indexes, catalog.ActualRows, "the record is the index catalog population read")
		require.EqualValues(t, 1, catalog.DBHits, "one acquisition, whatever it returned")
		require.EqualValues(t, 1, plan.TotalDBHits,
			"the one catalog acquisition is this statement's only database read")

		stats := int64(len(schema.GetIndexStats()))
		require.Positive(t, stats)
		result, plan = profileEquivalentObservedStatement(t, exec,
			"CALL db.index.stats() YIELD name, type, totalEntries RETURN name, type, totalEntries")
		require.Len(t, result.Rows, int(stats), "one row per statistics entry")
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		catalog = planOperatorOfType(plan.Root, profileKindSchemaIndexStatsCatalog)
		require.NotNil(t, catalog, "the statistics catalog read is reported as a plan operator")
		require.True(t, catalog.RowsObserved)
		require.Equal(t, stats, catalog.ActualRows, "the record is the statistics catalog population read")
		require.EqualValues(t, 1, catalog.DBHits)
		require.EqualValues(t, 1, plan.TotalDBHits)

		constraints := int64(len(schema.GetAllConstraints()))
		propertyTypes := int64(len(schema.GetAllPropertyTypeConstraints()))
		require.Positive(t, constraints)
		require.Positive(t, propertyTypes)
		result, plan = profileEquivalentObservedStatement(t, exec,
			"CALL db.constraints() YIELD name, type RETURN name, type")
		require.Len(t, result.Rows, int(constraints+propertyTypes),
			"the procedure answers with both catalogs' entries as one row list")
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		constraintAccess := planOperatorOfType(plan.Root, profileKindSchemaConstraintCatalog)
		require.NotNil(t, constraintAccess)
		require.True(t, constraintAccess.RowsObserved)
		require.Equal(t, constraints, constraintAccess.ActualRows,
			"the constraint catalog population, not the statement's merged rows")
		propertyTypeAccess := planOperatorOfType(plan.Root, profileKindSchemaPropertyTypeConstraintCatalog)
		require.NotNil(t, propertyTypeAccess)
		require.True(t, propertyTypeAccess.RowsObserved)
		require.Equal(t, propertyTypes, propertyTypeAccess.ActualRows,
			"the property type constraint catalog population")
		require.EqualValues(t, 1, constraintAccess.DBHits)
		require.EqualValues(t, 1, propertyTypeAccess.DBHits)
		require.EqualValues(t, 2, plan.TotalDBHits,
			"both constraint acquisitions are reported")
	})
}

// A catalog holding no entries is an observed zero, not a missing measurement:
// the procedure reports the population it read, so its hit total stays
// certified.
func TestProfileSchemaCatalogProceduresOnEmptyCatalogsReportObservedZeros(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		schema := exec.storage.GetSchema()
		require.Empty(t, schema.GetIndexStats(), "the fixture starts with no index statistics")
		require.Empty(t, schema.GetAllConstraints())
		require.Empty(t, schema.GetAllPropertyTypeConstraints())

		result, plan := profileEquivalentObservedStatement(t, exec, "CALL db.index.stats() YIELD name RETURN name")
		require.Empty(t, result.Rows)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		catalog := planOperatorOfType(plan.Root, profileKindSchemaIndexStatsCatalog)
		require.NotNil(t, catalog)
		require.True(t, catalog.RowsObserved, "an empty catalog is an observed zero")
		require.Zero(t, catalog.ActualRows)
		require.EqualValues(t, 1, catalog.DBHits, "the acquisition ran even though it listed nothing")
		require.EqualValues(t, 1, plan.TotalDBHits)

		result, plan = profileEquivalentObservedStatement(t, exec, "CALL db.constraints() YIELD name RETURN name")
		require.Empty(t, result.Rows)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		for _, kind := range []string{
			profileKindSchemaConstraintCatalog,
			profileKindSchemaPropertyTypeConstraintCatalog,
		} {
			access := planOperatorOfType(plan.Root, kind)
			require.NotNil(t, access, kind)
			require.True(t, access.RowsObserved, kind)
			require.Zero(t, access.ActualRows, kind)
			require.EqualValues(t, 1, access.DBHits, kind)
		}
		require.EqualValues(t, 2, plan.TotalDBHits, "both empty constraint acquisitions are reported")

		// This database's schema always holds its token lookup indexes, so its
		// index catalog is not empty here: the record is the population the
		// reader returned, which is what an empty catalog would report as well.
		indexes := int64(len(schema.GetIndexes()))
		require.Positive(t, indexes, "every schema this database creates holds its token lookup indexes")
		result, plan = profileEquivalentObservedStatement(t, exec, "CALL db.indexes() YIELD name RETURN name")
		require.Len(t, result.Rows, int(indexes))
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		catalog = planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
		require.NotNil(t, catalog)
		require.True(t, catalog.RowsObserved)
		require.Equal(t, indexes, catalog.ActualRows)
		require.EqualValues(t, 1, plan.TotalDBHits)
	})
}

// A VECTOR index entry is in the index catalog like any other, and its state
// comes from the search service's population records. That lookup is its own
// record when this executor has a search service; an executor without one
// answers ONLINE without reading, so no record claims a lookup that did not
// happen.
func TestProfileDbIndexesAccountsForTheVectorPopulationStateLookup(t *testing.T) {
	const vectorIndex = "idx_schema_catalog_vec"
	vectorCatalogExecutor := func(t *testing.T) *StorageExecutor {
		t.Helper()
		exec, _ := newProfileRuntimeTestExecutor(t)
		_, err := exec.Execute(t.Context(),
			"CALL db.index.vector.createNodeIndex('"+vectorIndex+"', 'Doc', 'emb', 3, 'cosine')", nil)
		require.NoError(t, err)
		return exec
	}

	forEachParser(t, func(t *testing.T) {
		t.Run("without a search service", func(t *testing.T) {
			exec := vectorCatalogExecutor(t)
			indexes := int64(len(exec.storage.GetSchema().GetIndexes()))
			require.Positive(t, indexes)
			result, plan := profileEquivalentObservedStatement(t, exec, "CALL db.indexes() YIELD name, state RETURN name, state")
			require.Len(t, result.Rows, int(indexes))
			require.True(t, plan.TotalDBHitsObserved)
			require.Empty(t, plan.Unobserved)
			require.Nil(t, planOperatorOfType(plan.Root, profileKindVectorIndexPopulationStateSeek),
				"an executor without a search service reads no population record")
			catalog := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
			require.NotNil(t, catalog)
			require.Equal(t, indexes, catalog.ActualRows)
			require.EqualValues(t, 1, catalog.DBHits)
			require.EqualValues(t, 1, plan.TotalDBHits)
		})

		t.Run("with a search service", func(t *testing.T) {
			exec := vectorCatalogExecutor(t)
			service := search.NewService(exec.storage, 3, nil)
			t.Cleanup(func() { _ = service.Close() })
			exec.SetSearchService(service)
			indexes := int64(len(exec.storage.GetSchema().GetIndexes()))
			require.Positive(t, indexes)
			result, plan := profileEquivalentObservedStatement(t, exec, "CALL db.indexes() YIELD name, state RETURN name, state")
			require.Len(t, result.Rows, int(indexes))
			require.True(t, plan.TotalDBHitsObserved)
			require.Empty(t, plan.Unobserved)
			seek := planOperatorOfType(plan.Root, profileKindVectorIndexPopulationStateSeek)
			require.NotNil(t, seek, "the population-record lookup is reported")
			require.True(t, seek.RowsObserved)
			require.Equal(t, int64(1), seek.ActualRows)
			require.EqualValues(t, 1, seek.DBHits)
			require.Equal(t, vectorIndex, seek.Description)
			catalog := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
			require.NotNil(t, catalog)
			require.Equal(t, indexes, catalog.ActualRows)
			require.EqualValues(t, 1, catalog.DBHits)
			require.EqualValues(t, 2, plan.TotalDBHits, "the state lookup is reported beside the catalog acquisition")
		})
	})
}

// A replacement of a reviewed built-in registered through the exported API is
// opaque: the same name carries no trust to another handler, and no catalog is
// claimed for it.
func TestProfileReviewedProcedureReplacementsStayOpaque(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	for _, name := range []string{
		"db.labels", "db.relationshipTypes", "db.propertyKeys",
		"db.stats.collect", "db.stats.retrieve", "db.stats.retrieveAllAnTheStats", "db.stats.clear", "db.stats.status", "db.stats.stop",
		"apoc.custom.installProcedure", "apoc.custom.declareProcedure", "apoc.custom.dropProcedure", "apoc.custom.list", "apoc.custom.show",
		"db.indexes", "db.index.stats", "db.constraints",
		"dbms.components", "nornicdb.version", "nornicdb.stats", "dbms.functions", "dbms.procedures",
		"db.schema.nodeProperties", "db.schema.relProperties",
		"db.index.fulltext.queryNodes", "db.index.fulltext.queryRelationships",
		"db.index.fulltext.createNodeIndex", "db.index.fulltext.createRelationshipIndex", "db.index.fulltext.drop",
		"db.index.vector.createNodeIndex", "db.index.vector.createRelationshipIndex", "db.index.vector.drop",
		"db.create.setNodeVectorProperty", "db.create.setRelationshipVectorProperty",
		"apoc.load.json", "apoc.load.jsonArray", "apoc.load.csv", "apoc.import.json",
		"apoc.cypher.run", "apoc.cypher.doIt", "apoc.cypher.doitall", "apoc.cypher.runMany",
		"apoc.periodic.iterate", "apoc.periodic.commit", "apoc.periodic.rock_n_roll",
		"apoc.export.json.all", "apoc.export.csv.all", "apoc.export.json.query", "apoc.export.csv.query",
		"db.index.vector.queryNodes", "db.index.vector.queryRelationships",
		"gds.graph.exists", "gds.graph.list", "gds.graph.project", "gds.graph.drop",
		"gds.version", "gds.fastRP.stream", "gds.fastRP.stats",
		"apoc.algo.pageRank", "apoc.algo.betweenness", "apoc.algo.closeness",
		"apoc.algo.louvain", "apoc.algo.labelPropagation", "apoc.algo.wcc",
		"db.ping", "dbms.clientConfig", "db.index.fulltext.listAvailableAnalyzers",
		"db.schema.nodeTypeProperties", "db.schema.relTypeProperties", "db.schema.visualization",
		"db.info", "dbms.info", "dbms.listConfig", "dbms.listConnections", "db.awaitIndex", "db.awaitIndexes", "db.resampleIndex", "db.clearQueryCaches",
		"kp.suppress", "kp.unsuppress", "kp.decayInfo", "kp.info", "kp.profiles", "kp.policies", "kp.resolve", "kp.deindexStatus",
		"apoc.path.expand", "apoc.path.expandConfig", "apoc.path.subgraphNodes", "apoc.path.subgraphAll", "apoc.path.spanningTree",
		"apoc.algo.dijkstra", "apoc.algo.aStar", "apoc.algo.allSimplePaths", "apoc.neighbors.tohop", "apoc.neighbors.byhop",
		"gds.linkPrediction.adamicAdar.stream", "gds.linkPrediction.commonNeighbors.stream", "gds.linkPrediction.resourceAllocation.stream",
		"gds.linkPrediction.preferentialAttachment.stream", "gds.linkPrediction.jaccard.stream", "gds.linkPrediction.predict.stream",
		"db.retrieve", "db.rretrieve", "db.retrieveRelationships", "db.retrieveGraph", "db.rretrieveGraph", "db.rerank", "db.infer", "db.index.vector.embed",
		"db.cdc.current", "db.cdc.earliest", "db.cdc.query", "db.txlog.entries", "db.txlog.byTxId", "db.temporal.assertNoOverlap", "db.temporal.asOf",
	} {
		column := "name"
		switch name {
		case "db.labels":
			column = "label"
		case "kp.decayInfo", "kp.info":
			column = "enabled"
		case "kp.profiles", "kp.policies":
			column = "kind"
		case "kp.resolve":
			column = "TargetID"
		case "kp.deindexStatus":
			column = "pending_count"
		}
		for _, registration := range []struct {
			name     string
			register func(ProcedureSpec, ProcedureHandler) error
		}{
			{"builtin API", globalProcedureRegistry.RegisterBuiltIn},
			{"user API", RegisterUserProcedure},
		} {
			t.Run(name+"/"+registration.name, func(t *testing.T) {
				registry := globalProcedureRegistry
				registry.mu.Lock()
				previousBuiltin, hadBuiltin := registry.builtins[name]
				previousUser, hadUser := registry.user[name]
				_, hadFold := registry.userFold[lowerASCII(name)]
				delete(registry.user, name)
				delete(registry.userFold, lowerASCII(name))
				registry.mu.Unlock()
				t.Cleanup(func() {
					registry.mu.Lock()
					defer registry.mu.Unlock()
					if hadBuiltin {
						registry.builtins[name] = previousBuiltin
					} else {
						delete(registry.builtins, name)
					}
					if hadUser {
						registry.user[name] = previousUser
					} else {
						delete(registry.user, name)
					}
					if hadFold {
						registry.userFold[lowerASCII(name)] = struct{}{}
					} else {
						delete(registry.userFold, lowerASCII(name))
					}
					registry.generation.Add(1)
				})
				require.NoError(t, registration.register(ProcedureSpec{
					Name: name, Signature: name + "() :: (" + column + " :: STRING)", Mode: ProcedureModeRead,
					Returns: []ProcedureColumn{{Name: column, Type: "STRING"}},
				}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
					return &ExecuteResult{Columns: []string{column}, Rows: [][]interface{}{{"custom"}}}, nil
				}))
				forEachParser(t, func(t *testing.T) {
					exec := tokenListingExecutor(t)
					result, err := exec.Execute(t.Context(), "PROFILE CALL "+name+"() YIELD "+column+" RETURN "+column, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"custom"}}, result.Rows)
					plan := executionPlanFromResult(t, result)
					require.False(t, plan.TotalDBHitsObserved, "a replacement's reads are not observed")
					require.Contains(t, plan.Unobserved, "database reads: procedure callback: "+name)
					for _, kind := range []string{
						profileKindAllNodesScan,
						profileKindAllRelationshipsScan,
						profileKindNodeCountRequest,
						profileKindRelationshipCountRequest,
						profileKindSchemaIndexCatalog,
						profileKindSchemaIndexStatsCatalog,
						profileKindSchemaConstraintCatalog,
						profileKindSchemaPropertyTypeConstraintCatalog,
						"QueryStatisticsState", "QueryStatisticsCatalog", "SchemaTokenOrder",
						profileKindGraphCatalogSeek, profileKindGraphProjectionCatalog,
						profileKindVectorIndexPopulationStateSeek,
					} {
						require.Nil(t, planOperatorOfType(plan.Root, kind),
							"no catalog is claimed for %s: %s", name, kind)
					}
					require.Contains(t, formatPlan(plan), "Total DB Hits: unobserved")
				})
			})
		}
	}
}

func TestProfileVectorIndexStateObservesTheServiceItResolved(t *testing.T) {
	exec, _ := newProfileRuntimeTestExecutor(t)
	service := search.NewService(exec.storage, 3, nil)
	t.Cleanup(func() { _ = service.Close() })
	require.NoError(t, service.BuildIndexes(t.Context()))
	resolutions := 0
	exec.SetSearchServiceResolver("profile-state", func(string) *search.Service {
		resolutions++
		if resolutions == 1 {
			return service
		}
		return nil
	})
	collector := newProfileRuntimeCollector()
	state, percent, message := exec.vectorIndexShowState(withProfileRuntimeCollector(t.Context(), collector), "index")
	require.Equal(t, search.VectorIndexOnline, state)
	require.Equal(t, 100.0, percent)
	require.Empty(t, message)
	require.Equal(t, 1, resolutions, "observe the actual reader without a second service resolution")
	accesses := collector.accessObservation()
	require.Len(t, accesses, 1)
	require.Equal(t, profileKindVectorIndexPopulationStateSeek, accesses[0].Kind)
	require.Equal(t, "index", accesses[0].Detail)
	require.EqualValues(t, 1, accesses[0].Rows)
	require.EqualValues(t, 1, accesses[0].Hits)
}

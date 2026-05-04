package cypher

import (
	"context"
	"sync"
)

var builtinProcedureRegistryOnce sync.Once

func ensureBuiltInProceduresRegistered() {
	builtinProcedureRegistryOnce.Do(func() {
		registerBuiltInProcedure("db.labels", "db.labels() :: (label :: STRING)", "Lists all labels in the database", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbLabels()
			})
		registerBuiltInProcedure("db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING)", "Lists all relationship types in the database", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRelationshipTypes()
			})
		registerBuiltInProcedure("db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING)", "Lists all property keys in the database", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPropertyKeys()
			})
		registerBuiltInProcedure("db.indexes", "db.indexes() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, state :: STRING)", "Lists all indexes in the database", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexes()
			})
		registerBuiltInProcedure("db.index.stats", "db.index.stats() :: (name :: STRING, type :: STRING, label :: STRING, property :: STRING, totalEntries :: INTEGER, uniqueValues :: INTEGER, selectivity :: FLOAT)", "Returns index statistics", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexStats()
			})
		registerBuiltInProcedure("db.constraints", "db.constraints() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, propertyType :: STRING)", "Lists all constraints in the database", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbConstraints()
			})
		registerBuiltInProcedure("db.info", "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", "Returns database information", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfo()
			})
		registerBuiltInProcedure("db.ping", "db.ping() :: (success :: BOOLEAN)", "Checks database connectivity", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPing()
			})
		registerBuiltInProcedure("db.schema.visualization", "db.schema.visualization() :: (nodes :: LIST<MAP>, relationships :: LIST<MAP>)", "Visualizes schema", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaVisualization()
			})
		registerBuiltInProcedure("db.schema.nodeProperties", "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns node properties by label", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaNodeProperties()
			})
		registerBuiltInProcedure("db.schema.relProperties", "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)", "Returns relationship properties by type", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaRelProperties()
			})

		registerBuiltInProcedure("db.index.fulltext.queryNodes", "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING) :: (node :: NODE, score :: FLOAT)", "Fulltext search on nodes", ProcedureModeRead, 2, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryNodes(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.queryRelationships", "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Fulltext search on relationships", ProcedureModeRead, 2, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryRelationships(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.createNodeIndex", "db.index.fulltext.createNodeIndex(indexName :: STRING, labels :: LIST<STRING>, properties :: LIST<STRING>)", "Creates fulltext index on nodes", ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateNodeIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.createRelationshipIndex", "db.index.fulltext.createRelationshipIndex(indexName :: STRING, relationshipTypes :: LIST<STRING>, properties :: LIST<STRING>)", "Creates fulltext index on relationships", ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateRelationshipIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.drop", "db.index.fulltext.drop(indexName :: STRING)", "Drops fulltext index", ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextDrop(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.listAvailableAnalyzers", "db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING, description :: STRING)", "Lists available analyzers", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextListAvailableAnalyzers()
			})

		registerBuiltInProcedure("db.index.vector.queryNodes", "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (node :: NODE, score :: FLOAT)", "Vector search on nodes", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorQueryNodes(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.queryRelationships", "db.index.vector.queryRelationships(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Vector search on relationships", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorQueryRelationships(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.embed", "db.index.vector.embed(text :: STRING) :: (embedding :: LIST<FLOAT>)", "Embeds text using configured embedder", ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorEmbed(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.createNodeIndex", "db.index.vector.createNodeIndex(indexName :: STRING, label :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", "Creates vector index on nodes", ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateNodeIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.createRelationshipIndex", "db.index.vector.createRelationshipIndex(indexName :: STRING, relationshipType :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", "Creates vector index on relationships", ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateRelationshipIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.drop", "db.index.vector.drop(indexName :: STRING)", "Drops vector index", ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorDrop(cypher)
			})

		registerBuiltInProcedure("db.create.setNodeVectorProperty", "db.create.setNodeVectorProperty(nodeId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a node", ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCreateSetNodeVectorProperty(ctx, cypher)
			})
		registerBuiltInProcedure("db.create.setRelationshipVectorProperty", "db.create.setRelationshipVectorProperty(relationshipId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", "Sets vector property on a relationship", ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCreateSetRelationshipVectorProperty(ctx, cypher)
			})

		registerBuiltInProcedure("dbms.components", "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)", "Lists DBMS components", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsComponents()
			})
		registerBuiltInProcedure("dbms.info", "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", "Returns DBMS information", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsInfo()
			})
		registerBuiltInProcedure("dbms.listConfig", "dbms.listConfig() :: (name :: STRING, description :: STRING, value :: ANY, dynamic :: BOOLEAN)", "Lists DBMS configuration", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConfig()
			})
		registerBuiltInProcedure("dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, value :: ANY)", "Returns client config", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsClientConfig()
			})
		registerBuiltInProcedure("dbms.listConnections", "dbms.listConnections() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, clientAddress :: STRING)", "Lists active DBMS connections", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConnections()
			})
		registerBuiltInProcedure("dbms.procedures", "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING)", "Lists procedures", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsProcedures()
			})
		registerBuiltInProcedure("dbms.functions", "dbms.functions() :: (name :: STRING, description :: STRING, category :: STRING)", "Lists functions", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsFunctions()
			})

		registerBuiltInProcedure("db.awaitIndex", "db.awaitIndex(indexName :: STRING, timeoutSeconds :: INTEGER = 300)", "Waits for one index to be online", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndex(cypher)
			})
		registerBuiltInProcedure("db.awaitIndexes", "db.awaitIndexes(timeoutSeconds :: INTEGER = 300)", "Waits for all indexes to be online", ProcedureModeRead, 0, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndexes(cypher)
			})
		registerBuiltInProcedure("db.resampleIndex", "db.resampleIndex(indexName :: STRING)", "Resamples index statistics", ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbResampleIndex(cypher)
			})
		registerBuiltInProcedure("db.clearQueryCaches", "db.clearQueryCaches() :: (status :: STRING)", "Clears query caches", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbClearQueryCaches()
			})

		registerBuiltInProcedure("db.stats.collect", "db.stats.collect(section :: STRING = 'ALL')", "Starts statistics collection", ProcedureModeDBMS, 0, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsCollect(cypher)
			})
		registerBuiltInProcedure("db.stats.retrieve", "db.stats.retrieve(section :: STRING = 'ALL')", "Retrieves collected statistics", ProcedureModeDBMS, 0, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsRetrieve(cypher)
			})
		registerBuiltInProcedure("db.stats.retrieveAllAnTheStats", "db.stats.retrieveAllAnTheStats()", "Retrieves all statistics", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsRetrieveAllAnTheStats()
			})
		registerBuiltInProcedure("db.stats.clear", "db.stats.clear()", "Clears collected statistics", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsClear()
			})
		registerBuiltInProcedure("db.stats.status", "db.stats.status()", "Returns statistics collection status", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsStatus()
			})
		registerBuiltInProcedure("db.stats.stop", "db.stats.stop()", "Stops statistics collection", ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsStop()
			})

		registerBuiltInProcedure("tx.setMetaData", "tx.setMetaData(metadata :: MAP)", "Sets transaction metadata", ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callTxSetMetadata(cypher)
			})

		registerBuiltInProcedure("nornicdb.version", "nornicdb.version() :: (version :: STRING, build :: STRING, edition :: STRING)", "Returns NornicDB version", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbVersion()
			})
		registerBuiltInProcedure("nornicdb.stats", "nornicdb.stats() :: (nodes :: INTEGER, relationships :: INTEGER, labels :: INTEGER, relationshipTypes :: INTEGER)", "Returns NornicDB stats", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbStats()
			})
		registerBuiltInProcedure("nornicdb.decay.info", "nornicdb.decay.info() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)", "Returns knowledge-layer scoring configuration", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbDecayInfo()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.info", "nornicdb.knowledgepolicy.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)", "Returns knowledge-layer profile and policy catalog counts", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyInfo()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.profiles", "nornicdb.knowledgepolicy.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER)", "Returns knowledge-layer decay bundles and bindings", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyProfiles()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.policies", "nornicdb.knowledgepolicy.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN)", "Returns knowledge-layer promotion profiles and policies", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyPolicies()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.resolve", "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (TargetID :: STRING, TargetScope :: STRING, ResolvedDecayProfileID :: STRING, ResolvedScoreFrom :: STRING, ResolutionSourceChain :: LIST<STRING>, AppliedDecayProfileNames :: LIST<STRING>, AppliedPromotionPolicyName :: STRING, AppliedPromotionProfileName :: STRING, EffectiveRate :: FLOAT, EffectiveThreshold :: FLOAT, EffectiveMultiplier :: FLOAT, BaseScore :: FLOAT, FinalScore :: FLOAT, NoDecay :: BOOLEAN, SuppressionEligible :: BOOLEAN, Explanation :: STRING)", "Resolves the effective knowledge-layer scoring policy for an entity, label set, or edge type", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyResolve(args)
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.deindexStatus", "nornicdb.knowledgepolicy.deindexStatus() :: (pending_count :: INTEGER, supported :: BOOLEAN, message :: STRING, workItemId :: STRING, targetId :: STRING, targetScope :: STRING, enqueuedAt :: INTEGER, status :: STRING)", "Returns knowledge-layer deindex queue status", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyDeindexStatus()
			})

		registerBuiltInProcedure("db.retrieve", "db.retrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN)", "Hybrid retrieval procedure", ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieve(ctx, cypher)
			})
		registerBuiltInProcedure("db.rretrieve", "db.rretrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN)", "Hybrid retrieval + rerank procedure", ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieve(ctx, cypher)
			})
		registerBuiltInProcedure("db.rerank", "db.rerank(request :: MAP) :: (id :: STRING, content :: STRING, original_rank :: INTEGER, new_rank :: INTEGER, bi_score :: FLOAT, cross_score :: FLOAT, final_score :: FLOAT)", "Standalone rerank procedure", ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRerank(ctx, cypher)
			})
		registerBuiltInProcedure("db.infer", "db.infer(request :: MAP) :: (text :: STRING, structured :: ANY, model :: STRING, usage :: MAP, latencyMs :: INTEGER, finishReason :: STRING)", "LLM inference procedure", ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfer(ctx, cypher)
			})

		registerBuiltInProcedure("db.txlog.entries", "db.txlog.entries() :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", "Returns transaction log entries", ProcedureModeDBMS, 0, 4, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogEntries(ctx, cypher)
			})
		registerBuiltInProcedure("db.txlog.byTxId", "db.txlog.byTxId(txId :: STRING) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", "Returns WAL entries for a transaction id", ProcedureModeDBMS, 1, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogByTxID(ctx, cypher)
			})
		registerBuiltInProcedure("db.temporal.assertNoOverlap", "db.temporal.assertNoOverlap(args :: MAP) :: (ok :: BOOLEAN)", "Checks temporal overlap constraints", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAssertNoOverlap(ctx, cypher)
			})
		registerBuiltInProcedure("db.temporal.asOf", "db.temporal.asOf(args :: MAP) :: (node :: NODE)", "Reads temporal graph at point in time", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAsOf(ctx, cypher)
			})

		registerBuiltInProcedure("apoc.path.subgraphNodes", "apoc.path.subgraphNodes(startNode :: NODE, config :: MAP) :: (node :: NODE)", "Returns all nodes in a subgraph", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSubgraphNodes(cypher)
			})
		registerBuiltInProcedure("apoc.path.expand", "apoc.path.expand(startNode :: NODE, relationshipFilter :: STRING, labelFilter :: STRING, minLevel :: INTEGER, maxLevel :: INTEGER) :: (path :: PATH)", "Expands paths from a start node", ProcedureModeRead, 1, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathExpand(cypher)
			})
		registerBuiltInProcedure("apoc.path.spanningTree", "apoc.path.spanningTree(startNode :: NODE, config :: MAP) :: (path :: PATH)", "Returns spanning tree paths", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSpanningTree(cypher)
			})
		registerBuiltInProcedure("apoc.cypher.run", "apoc.cypher.run(statement :: STRING, params :: MAP) :: (value :: MAP)", "Runs dynamic Cypher", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.cypher.doitall", "apoc.cypher.doitall(statement :: STRING, params :: MAP) :: (value :: MAP)", "Alias of apoc.cypher.run", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.cypher.runMany", "apoc.cypher.runMany(statements :: STRING, params :: MAP) :: (row :: INTEGER, result :: MAP)", "Runs many Cypher statements", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRunMany(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.periodic.iterate", "apoc.periodic.iterate(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, errorMessages :: LIST<STRING>)", "Runs batch iterate/action jobs", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.periodic.commit", "apoc.periodic.commit(statement :: STRING, params :: MAP) :: (updates :: INTEGER, executions :: INTEGER, runtime :: INTEGER)", "Runs periodic commits", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicCommit(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.periodic.rock_n_roll", "apoc.periodic.rock_n_roll(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, errorMessages :: LIST<STRING>)", "Alias of apoc.periodic.iterate", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, cypher)
			})

		registerBuiltInProcedure("gds.version", "gds.version() :: (version :: STRING)", "Returns GDS version", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsVersion()
			})
		registerBuiltInProcedure("gds.graph.list", "gds.graph.list() :: (graphName :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", "Lists projected GDS graphs", ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphList()
			})
		registerBuiltInProcedure("gds.graph.drop", "gds.graph.drop(graphName :: STRING)", "Drops projected GDS graph", ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphDrop(cypher)
			})
		registerBuiltInProcedure("gds.graph.project", "gds.graph.project(graphName :: STRING, nodeProjection :: ANY, relationshipProjection :: ANY) :: (graphName :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", "Projects graph into GDS catalog", ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphProject(cypher)
			})
		registerBuiltInProcedure("gds.fastRP.stream", "gds.fastRP.stream(graphName :: STRING, config :: MAP) :: (nodeId :: INTEGER, embedding :: LIST<FLOAT>)", "Streams FastRP embeddings", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStream(cypher)
			})
		registerBuiltInProcedure("gds.fastRP.stats", "gds.fastRP.stats(graphName :: STRING, config :: MAP) :: (nodeCount :: INTEGER, embeddingDimension :: INTEGER, computeMillis :: INTEGER)", "Runs FastRP and returns statistics", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStats(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.adamicAdar.stream", "gds.linkPrediction.adamicAdar.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", "Runs Adamic-Adar link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionAdamicAdar(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.commonNeighbors.stream", "gds.linkPrediction.commonNeighbors.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", "Runs common neighbors link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionCommonNeighbors(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.resourceAllocation.stream", "gds.linkPrediction.resourceAllocation.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", "Runs resource-allocation link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionResourceAllocation(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.preferentialAttachment.stream", "gds.linkPrediction.preferentialAttachment.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", "Runs preferential attachment link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPreferentialAttachment(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.jaccard.stream", "gds.linkPrediction.jaccard.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", "Runs Jaccard link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionJaccard(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.predict.stream", "gds.linkPrediction.predict.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, probability :: FLOAT)", "Runs model-based link prediction", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPredict(cypher)
			})

		registerBuiltInProcedure("apoc.algo.dijkstra", "apoc.algo.dijkstra(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING) :: (path :: PATH, weight :: FLOAT)", "Runs weighted shortest path", ProcedureModeRead, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoDijkstra(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.aStar", "apoc.algo.aStar(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, latPropertyName :: STRING, lonPropertyName :: STRING) :: (path :: PATH, weight :: FLOAT)", "Runs A* shortest path", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAStar(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.allSimplePaths", "apoc.algo.allSimplePaths(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, maxNodes :: INTEGER) :: (path :: PATH)", "Enumerates all simple paths", ProcedureModeRead, 4, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAllSimplePaths(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.pageRank", "apoc.algo.pageRank(nodes :: LIST<NODE>, relTypes :: STRING, iterations :: INTEGER, dampingFactor :: FLOAT) :: (node :: NODE, score :: FLOAT)", "Runs PageRank", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoPageRank(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.betweenness", "apoc.algo.betweenness(nodes :: LIST<NODE>, relTypes :: STRING, direction :: STRING) :: (node :: NODE, score :: FLOAT)", "Runs betweenness centrality", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoBetweenness(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.closeness", "apoc.algo.closeness(nodes :: LIST<NODE>, relTypes :: STRING, direction :: STRING) :: (node :: NODE, score :: FLOAT)", "Runs closeness centrality", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoCloseness(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.louvain", "apoc.algo.louvain(label :: STRING, relType :: STRING) :: (node :: NODE, community :: INTEGER, score :: FLOAT)", "Runs Louvain community detection", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLouvain(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.labelPropagation", "apoc.algo.labelPropagation(label :: STRING, relType :: STRING, iterations :: INTEGER = 10) :: (node :: NODE, community :: INTEGER)", "Runs label propagation", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLabelPropagation(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.algo.wcc", "apoc.algo.wcc(label :: STRING, relType :: STRING) :: (node :: NODE, component :: INTEGER)", "Runs weakly connected components", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoWCC(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.neighbors.tohop", "apoc.neighbors.tohop(node :: NODE, relTypes :: STRING, distance :: INTEGER) :: (nodes :: LIST<NODE>)", "Collects neighbors to N hops", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsTohop(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.neighbors.byhop", "apoc.neighbors.byhop(node :: NODE, relTypes :: STRING, distance :: INTEGER) :: (nodes :: LIST<NODE>)", "Collects neighbors grouped by hop distance", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsByhop(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.load.json", "apoc.load.json(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)", "Loads JSON", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadJson(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.load.jsonArray", "apoc.load.jsonArray(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)", "Loads JSON array", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadJsonArray(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.load.csv", "apoc.load.csv(urlOrBinary :: STRING, config :: MAP = {}, nullValues :: LIST<STRING> = []) :: (lineNo :: INTEGER, list :: LIST<STRING>, map :: MAP)", "Loads CSV", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadCsv(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.export.json.all", "apoc.export.json.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to JSON", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonAll(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.export.json.query", "apoc.export.json.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to JSON", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonQuery(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.export.csv.all", "apoc.export.csv.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to CSV", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvAll(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.export.csv.query", "apoc.export.csv.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to CSV", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvQuery(ctx, cypher)
			})
		registerBuiltInProcedure("apoc.import.json", "apoc.import.json(url :: STRING, config :: MAP = {}) :: (file :: STRING, source :: STRING, format :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Imports JSON", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocImportJson(ctx, cypher)
			})
	})
}

func registerBuiltInProcedure(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	_ = globalProcedureRegistry.RegisterBuiltIn(ProcedureSpec{
		Name:          name,
		Signature:     signature,
		Description:   description,
		Mode:          mode,
		WorksOnSystem: worksOnSystem,
		MinArgs:       minArgs,
		MaxArgs:       maxArgs,
	}, handler)
}

package cypher

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
)

var builtinProcedureRegistryOnce sync.Once

func ensureBuiltInProceduresRegistered() {
	builtinProcedureRegistryOnce.Do(func() {
		registerBuiltInProcedure("db.labels", "db.labels() :: (label :: STRING)", localization.CypherProcedureMetadata("db.labels"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbLabels()
			})
		registerBuiltInProcedure("db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING)", localization.CypherProcedureMetadata("db.relationshipTypes"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRelationshipTypes()
			})
		registerBuiltInProcedure("db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING)", localization.CypherProcedureMetadata("db.propertyKeys"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPropertyKeys()
			})
		registerBuiltInProcedure("db.indexes", "db.indexes() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, state :: STRING)", localization.CypherProcedureMetadata("db.indexes"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexes()
			})
		registerBuiltInProcedure("db.index.stats", "db.index.stats() :: (name :: STRING, type :: STRING, label :: STRING, property :: STRING, totalEntries :: INTEGER, uniqueValues :: INTEGER, selectivity :: FLOAT)", localization.CypherProcedureMetadata("db.index.stats"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexStats()
			})
		registerBuiltInProcedure("db.constraints", "db.constraints() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, propertyType :: STRING)", localization.CypherProcedureMetadata("db.constraints"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbConstraints()
			})
		registerBuiltInProcedure("db.info", "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", localization.CypherProcedureMetadata("db.info"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfo()
			})
		registerBuiltInProcedure("db.ping", "db.ping() :: (success :: BOOLEAN)", localization.CypherProcedureMetadata("db.ping"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPing()
			})
		registerBuiltInProcedure("db.schema.visualization", "db.schema.visualization() :: (nodes :: LIST<MAP>, relationships :: LIST<MAP>)", localization.CypherProcedureMetadata("db.schema.visualization"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaVisualization()
			})
		registerBuiltInProcedure("db.schema.nodeProperties", "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)", localization.CypherProcedureMetadata("db.schema.nodeProperties"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaNodeProperties()
			})
		registerBuiltInProcedure("db.schema.relProperties", "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)", localization.CypherProcedureMetadata("db.schema.relProperties"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaRelProperties()
			})

		registerBuiltInProcedure("db.index.fulltext.queryNodes", "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (node :: NODE, score :: FLOAT)", localization.CypherProcedureMetadata("db.index.fulltext.queryNodes"), ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryNodes(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.queryRelationships", "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, score :: FLOAT)", localization.CypherProcedureMetadata("db.index.fulltext.queryRelationships"), ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryRelationships(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.createNodeIndex", "db.index.fulltext.createNodeIndex(indexName :: STRING, labels :: LIST<STRING>, properties :: LIST<STRING>)", localization.CypherProcedureMetadata("db.index.fulltext.createNodeIndex"), ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateNodeIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.createRelationshipIndex", "db.index.fulltext.createRelationshipIndex(indexName :: STRING, relationshipTypes :: LIST<STRING>, properties :: LIST<STRING>)", localization.CypherProcedureMetadata("db.index.fulltext.createRelationshipIndex"), ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateRelationshipIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.drop", "db.index.fulltext.drop(indexName :: STRING)", localization.CypherProcedureMetadata("db.index.fulltext.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextDrop(cypher)
			})
		registerBuiltInProcedure("db.index.fulltext.listAvailableAnalyzers", "db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING, description :: STRING)", localization.CypherProcedureMetadata("db.index.fulltext.listAvailableAnalyzers"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextListAvailableAnalyzers()
			})

		registerBuiltInProcedure("db.index.vector.queryNodes", "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (node :: NODE, score :: FLOAT)", localization.CypherProcedureMetadata("db.index.vector.queryNodes"), ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorQueryNodes(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.queryRelationships", "db.index.vector.queryRelationships(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (relationship :: RELATIONSHIP, score :: FLOAT)", localization.CypherProcedureMetadata("db.index.vector.queryRelationships"), ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorQueryRelationships(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.embed", "db.index.vector.embed(text :: STRING) :: (embedding :: LIST<FLOAT>)", localization.CypherProcedureMetadata("db.index.vector.embed"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorEmbed(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.createNodeIndex", "db.index.vector.createNodeIndex(indexName :: STRING, label :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", localization.CypherProcedureMetadata("db.index.vector.createNodeIndex"), ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateNodeIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.createRelationshipIndex", "db.index.vector.createRelationshipIndex(indexName :: STRING, relationshipType :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", localization.CypherProcedureMetadata("db.index.vector.createRelationshipIndex"), ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateRelationshipIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.drop", "db.index.vector.drop(indexName :: STRING)", localization.CypherProcedureMetadata("db.index.vector.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorDrop(cypher)
			})

		registerBuiltInProcedure("db.create.setNodeVectorProperty", "db.create.setNodeVectorProperty(nodeId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", localization.CypherProcedureMetadata("db.create.setNodeVectorProperty"), ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCreateSetNodeVectorProperty(ctx, cypher)
			})
		registerBuiltInProcedure("db.create.setRelationshipVectorProperty", "db.create.setRelationshipVectorProperty(relationshipId :: STRING, propertyKey :: STRING, vector :: LIST<FLOAT>)", localization.CypherProcedureMetadata("db.create.setRelationshipVectorProperty"), ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCreateSetRelationshipVectorProperty(ctx, cypher)
			})

		registerBuiltInProcedure("dbms.components", "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)", localization.CypherProcedureMetadata("dbms.components"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsComponents()
			})
		registerBuiltInProcedure("dbms.info", "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", localization.CypherProcedureMetadata("dbms.info"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsInfo()
			})
		registerBuiltInProcedure("dbms.listConfig", "dbms.listConfig() :: (name :: STRING, description :: STRING, value :: ANY, dynamic :: BOOLEAN)", localization.CypherProcedureMetadata("dbms.listConfig"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConfig()
			})
		registerBuiltInProcedure("dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, value :: ANY)", localization.CypherProcedureMetadata("dbms.clientConfig"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsClientConfig()
			})
		registerBuiltInProcedure("dbms.listConnections", "dbms.listConnections() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, clientAddress :: STRING)", localization.CypherProcedureMetadata("dbms.listConnections"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConnections()
			})
		registerBuiltInProcedure("dbms.procedures", "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING)", localization.CypherProcedureMetadata("dbms.procedures"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsProcedures()
			})
		registerBuiltInProcedure("dbms.functions", "dbms.functions() :: (name :: STRING, description :: STRING, category :: STRING)", localization.CypherProcedureMetadata("dbms.functions"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsFunctions()
			})

		registerBuiltInProcedure("db.awaitIndex", "db.awaitIndex(indexName :: STRING, timeoutSeconds :: INTEGER = 300)", localization.CypherProcedureMetadata("db.awaitIndex"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndex(cypher)
			})
		registerBuiltInProcedure("db.awaitIndexes", "db.awaitIndexes(timeoutSeconds :: INTEGER = 300)", localization.CypherProcedureMetadata("db.awaitIndexes"), ProcedureModeRead, 0, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndexes(cypher)
			})
		registerBuiltInProcedure("db.resampleIndex", "db.resampleIndex(indexName :: STRING)", localization.CypherProcedureMetadata("db.resampleIndex"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbResampleIndex(cypher)
			})
		registerBuiltInProcedure("db.clearQueryCaches", "db.clearQueryCaches() :: (status :: STRING)", localization.CypherProcedureMetadata("db.clearQueryCaches"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbClearQueryCaches()
			})

		registerBuiltInProcedure("db.stats.collect", "db.stats.collect(section :: STRING = 'ALL')", localization.CypherProcedureMetadata("db.stats.collect"), ProcedureModeDBMS, 0, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsCollect(cypher)
			})
		registerBuiltInProcedure("db.stats.retrieve", "db.stats.retrieve(section :: STRING = 'ALL')", localization.CypherProcedureMetadata("db.stats.retrieve"), ProcedureModeDBMS, 0, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsRetrieve(cypher)
			})
		registerBuiltInProcedure("db.stats.retrieveAllAnTheStats", "db.stats.retrieveAllAnTheStats()", localization.CypherProcedureMetadata("db.stats.retrieveAllAnTheStats"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsRetrieveAllAnTheStats()
			})
		registerBuiltInProcedure("db.stats.clear", "db.stats.clear()", localization.CypherProcedureMetadata("db.stats.clear"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsClear()
			})
		registerBuiltInProcedure("db.stats.status", "db.stats.status()", localization.CypherProcedureMetadata("db.stats.status"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsStatus()
			})
		registerBuiltInProcedure("db.stats.stop", "db.stats.stop()", localization.CypherProcedureMetadata("db.stats.stop"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbStatsStop()
			})

		registerBuiltInProcedure("tx.setMetaData", "tx.setMetaData(metadata :: MAP)", localization.CypherProcedureMetadata("tx.setMetaData"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callTxSetMetadata(ctx, cypher)
			})

		registerBuiltInProcedure("nornicdb.version", "nornicdb.version() :: (version :: STRING, build :: STRING, edition :: STRING)", localization.CypherProcedureMetadata("nornicdb.version"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbVersion()
			})
		registerBuiltInProcedure("nornicdb.stats", "nornicdb.stats() :: (nodes :: INTEGER, relationships :: INTEGER, labels :: INTEGER, relationshipTypes :: INTEGER)", localization.CypherProcedureMetadata("nornicdb.stats"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbStats()
			})
		registerBuiltInProcedure("nornicdb.decay.info", "nornicdb.decay.info() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)", localization.CypherProcedureMetadata("nornicdb.decay.info"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbDecayInfo()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.info", "nornicdb.knowledgepolicy.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)", localization.CypherProcedureMetadata("nornicdb.knowledgepolicy.info"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyInfo()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.profiles", "nornicdb.knowledgepolicy.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER)", localization.CypherProcedureMetadata("nornicdb.knowledgepolicy.profiles"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyProfiles()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.policies", "nornicdb.knowledgepolicy.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN)", localization.CypherProcedureMetadata("nornicdb.knowledgepolicy.policies"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyPolicies()
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.resolve", "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (TargetID :: STRING, TargetScope :: STRING, ResolvedDecayProfileID :: STRING, ResolvedScoreFrom :: STRING, ResolutionSourceChain :: LIST<STRING>, AppliedDecayProfileNames :: LIST<STRING>, AppliedPromotionPolicyName :: STRING, AppliedPromotionProfileName :: STRING, EffectiveRate :: FLOAT, EffectiveThreshold :: FLOAT, EffectiveMultiplier :: FLOAT, BaseScore :: FLOAT, FinalScore :: FLOAT, NoDecay :: BOOLEAN, SuppressionEligible :: BOOLEAN, Explanation :: STRING)", localization.CypherProcedureMetadata("nornicdb.knowledgepolicy.resolve"), ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyResolve(args)
			})
		registerBuiltInProcedure("nornicdb.knowledgepolicy.deindexStatus", "nornicdb.knowledgepolicy.deindexStatus() :: (pending_count :: INTEGER, supported :: BOOLEAN, message :: STRING, workItemId :: STRING, targetId :: STRING, targetScope :: STRING, enqueuedAt :: INTEGER, status :: STRING)", localization.CypherProcedureMetadata("nornicdb.knowledgepolicy.deindexStatus"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyDeindexStatus()
			})

		registerBuiltInProcedure("db.retrieve", "db.retrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN)", localization.CypherProcedureMetadata("db.retrieve"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieve(ctx, cypher)
			})
		registerBuiltInProcedure("db.rretrieve", "db.rretrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN)", localization.CypherProcedureMetadata("db.rretrieve"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieve(ctx, cypher)
			})
		registerBuiltInProcedure("db.rerank", "db.rerank(request :: MAP) :: (id :: STRING, content :: STRING, original_rank :: INTEGER, new_rank :: INTEGER, bi_score :: FLOAT, cross_score :: FLOAT, final_score :: FLOAT)", localization.CypherProcedureMetadata("db.rerank"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRerank(ctx, cypher)
			})
		registerBuiltInProcedure("db.infer", "db.infer(request :: MAP) :: (text :: STRING, structured :: ANY, model :: STRING, usage :: MAP, latencyMs :: INTEGER, finishReason :: STRING)", localization.CypherProcedureMetadata("db.infer"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfer(ctx, cypher)
			})

		registerBuiltInProcedure("db.txlog.entries", "db.txlog.entries() :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.entries"), ProcedureModeDBMS, 0, 4, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogEntries(ctx, cypher)
			})
		registerBuiltInProcedure("db.txlog.byTxId", "db.txlog.byTxId(txId :: STRING) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.byTxId"), ProcedureModeDBMS, 1, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogByTxID(ctx, cypher)
			})
		registerBuiltInProcedure("db.temporal.assertNoOverlap", "db.temporal.assertNoOverlap(args :: MAP) :: (ok :: BOOLEAN)", localization.CypherProcedureMetadata("db.temporal.assertNoOverlap"), ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAssertNoOverlap(ctx, cypher)
			})
		registerBuiltInProcedure("db.temporal.asOf", "db.temporal.asOf(args :: MAP) :: (node :: NODE)", localization.CypherProcedureMetadata("db.temporal.asOf"), ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAsOf(ctx, cypher)
			})

		registerBuiltInProcedureLiteral("apoc.path.subgraphNodes", "apoc.path.subgraphNodes(startNode :: NODE, config :: MAP) :: (node :: NODE)", "Returns all nodes in a subgraph", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSubgraphNodes(cypher)
			})
		registerBuiltInProcedureLiteral("apoc.path.expand", "apoc.path.expand(startNode :: NODE, relationshipFilter :: STRING, labelFilter :: STRING, minLevel :: INTEGER, maxLevel :: INTEGER) :: (path :: PATH)", "Expands paths from a start node", ProcedureModeRead, 1, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathExpand(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.path.spanningTree", "apoc.path.spanningTree(startNode :: NODE, config :: MAP) :: (path :: PATH)", "Returns spanning tree paths", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSpanningTree(cypher)
			})
		registerBuiltInProcedureLiteral("apoc.cypher.run", "apoc.cypher.run(statement :: STRING, params :: MAP) :: (value :: MAP)", "Runs dynamic Cypher", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.cypher.doitall", "apoc.cypher.doitall(statement :: STRING, params :: MAP) :: (value :: MAP)", "Alias of apoc.cypher.run", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.cypher.runMany", "apoc.cypher.runMany(statements :: STRING, params :: MAP) :: (row :: INTEGER, result :: MAP)", "Runs many Cypher statements", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRunMany(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.periodic.iterate", "apoc.periodic.iterate(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, errorMessages :: LIST<STRING>)", "Runs batch iterate/action jobs", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.periodic.commit", "apoc.periodic.commit(statement :: STRING, params :: MAP) :: (updates :: INTEGER, executions :: INTEGER, runtime :: INTEGER)", "Runs periodic commits", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicCommit(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.periodic.rock_n_roll", "apoc.periodic.rock_n_roll(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, errorMessages :: LIST<STRING>)", "Alias of apoc.periodic.iterate", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, cypher)
			})

		registerBuiltInProcedure("gds.version", "gds.version() :: (version :: STRING)", localization.CypherProcedureMetadata("gds.version"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsVersion()
			})
		registerBuiltInProcedure("gds.graph.list", "gds.graph.list() :: (graphName :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", localization.CypherProcedureMetadata("gds.graph.list"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphList()
			})
		registerBuiltInProcedure("gds.graph.drop", "gds.graph.drop(graphName :: STRING)", localization.CypherProcedureMetadata("gds.graph.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphDrop(cypher)
			})
		registerBuiltInProcedure("gds.graph.project", "gds.graph.project(graphName :: STRING, nodeProjection :: ANY, relationshipProjection :: ANY) :: (graphName :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)", localization.CypherProcedureMetadata("gds.graph.project"), ProcedureModeWrite, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphProject(cypher)
			})
		registerBuiltInProcedure("gds.fastRP.stream", "gds.fastRP.stream(graphName :: STRING, config :: MAP) :: (nodeId :: INTEGER, embedding :: LIST<FLOAT>)", localization.CypherProcedureMetadata("gds.fastRP.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStream(cypher)
			})
		registerBuiltInProcedure("gds.fastRP.stats", "gds.fastRP.stats(graphName :: STRING, config :: MAP) :: (nodeCount :: INTEGER, embeddingDimension :: INTEGER, computeMillis :: INTEGER)", localization.CypherProcedureMetadata("gds.fastRP.stats"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStats(cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.adamicAdar.stream", "gds.linkPrediction.adamicAdar.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.adamicAdar.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionAdamicAdar(ctx, cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.commonNeighbors.stream", "gds.linkPrediction.commonNeighbors.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.commonNeighbors.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionCommonNeighbors(ctx, cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.resourceAllocation.stream", "gds.linkPrediction.resourceAllocation.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.resourceAllocation.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionResourceAllocation(ctx, cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.preferentialAttachment.stream", "gds.linkPrediction.preferentialAttachment.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.preferentialAttachment.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPreferentialAttachment(ctx, cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.jaccard.stream", "gds.linkPrediction.jaccard.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.jaccard.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionJaccard(ctx, cypher)
			})
		registerBuiltInProcedure("gds.linkPrediction.predict.stream", "gds.linkPrediction.predict.stream(graphName :: STRING, config :: MAP) :: (node1 :: INTEGER, node2 :: INTEGER, probability :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.predict.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPredict(ctx, cypher)
			})

		registerBuiltInProcedureLiteral("apoc.algo.dijkstra", "apoc.algo.dijkstra(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING) :: (path :: PATH, weight :: FLOAT)", "Runs weighted shortest path", ProcedureModeRead, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoDijkstra(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.aStar", "apoc.algo.aStar(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, latPropertyName :: STRING, lonPropertyName :: STRING) :: (path :: PATH, weight :: FLOAT)", "Runs A* shortest path", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAStar(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.allSimplePaths", "apoc.algo.allSimplePaths(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, maxNodes :: INTEGER) :: (path :: PATH)", "Enumerates all simple paths", ProcedureModeRead, 4, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAllSimplePaths(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.pageRank", "apoc.algo.pageRank(nodes :: LIST<NODE>, relTypes :: STRING, iterations :: INTEGER, dampingFactor :: FLOAT) :: (node :: NODE, score :: FLOAT)", "Runs PageRank", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoPageRank(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.betweenness", "apoc.algo.betweenness(nodes :: LIST<NODE>, relTypes :: STRING, direction :: STRING) :: (node :: NODE, score :: FLOAT)", "Runs betweenness centrality", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoBetweenness(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.closeness", "apoc.algo.closeness(nodes :: LIST<NODE>, relTypes :: STRING, direction :: STRING) :: (node :: NODE, score :: FLOAT)", "Runs closeness centrality", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoCloseness(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.louvain", "apoc.algo.louvain(label :: STRING, relType :: STRING) :: (node :: NODE, community :: INTEGER, score :: FLOAT)", "Runs Louvain community detection", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLouvain(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.labelPropagation", "apoc.algo.labelPropagation(label :: STRING, relType :: STRING, iterations :: INTEGER = 10) :: (node :: NODE, community :: INTEGER)", "Runs label propagation", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLabelPropagation(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.algo.wcc", "apoc.algo.wcc(label :: STRING, relType :: STRING) :: (node :: NODE, component :: INTEGER)", "Runs weakly connected components", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoWCC(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.neighbors.tohop", "apoc.neighbors.tohop(node :: NODE, relTypes :: STRING, distance :: INTEGER) :: (nodes :: LIST<NODE>)", "Collects neighbors to N hops", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsTohop(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.neighbors.byhop", "apoc.neighbors.byhop(node :: NODE, relTypes :: STRING, distance :: INTEGER) :: (nodes :: LIST<NODE>)", "Collects neighbors grouped by hop distance", ProcedureModeRead, 3, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsByhop(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.load.json", "apoc.load.json(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)", "Loads JSON", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadJson(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.load.jsonArray", "apoc.load.jsonArray(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)", "Loads JSON array", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadJsonArray(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.load.csv", "apoc.load.csv(urlOrBinary :: STRING, config :: MAP = {}, nullValues :: LIST<STRING> = []) :: (lineNo :: INTEGER, list :: LIST<STRING>, map :: MAP)", "Loads CSV", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadCsv(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.export.json.all", "apoc.export.json.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to JSON", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonAll(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.export.json.query", "apoc.export.json.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to JSON", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonQuery(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.export.csv.all", "apoc.export.csv.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to CSV", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvAll(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.export.csv.query", "apoc.export.csv.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to CSV", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvQuery(ctx, cypher)
			})
		registerBuiltInProcedureLiteral("apoc.import.json", "apoc.import.json(url :: STRING, config :: MAP = {}) :: (file :: STRING, source :: STRING, format :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Imports JSON", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocImportJson(ctx, cypher)
			})
	})
}

func registerBuiltInProcedure(name, signature string, description localization.Message, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name:               name,
		Signature:          signature,
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               mode,
		WorksOnSystem:      worksOnSystem,
		MinArgs:            minArgs,
		MaxArgs:            maxArgs,
	}, handler)
}

func registerBuiltInProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name:          name,
		Signature:     signature,
		Description:   description,
		Mode:          mode,
		WorksOnSystem: worksOnSystem,
		MinArgs:       minArgs,
		MaxArgs:       maxArgs,
	}, handler)
}

func registerProcedure(spec ProcedureSpec, handler ProcedureHandler) {
	_ = globalProcedureRegistry.RegisterBuiltIn(spec, handler)
}

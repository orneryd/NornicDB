package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
)

var builtinProcedureRegistryOnce sync.Once

func ensureBuiltInProceduresRegistered() {
	builtinProcedureRegistryOnce.Do(func() {
		registerProcedure(tokenListingProcedureSpec("db.labels", "label", "A label within the database."),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbLabels()
			})
		registerProcedure(tokenListingProcedureSpec("db.relationshipTypes", "relationshipType", "A relationship type in the database."),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRelationshipTypes()
			})
		registerProcedure(tokenListingProcedureSpec("db.propertyKeys", "propertyKey", "A property key in the database."),
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
		registerProcedure(ProcedureSpec{
			Name:               "db.info",
			Signature:          "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)",
			Description:        localization.CypherProcedureMetadata("db.info").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.info"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      true,
			Returns: []ProcedureColumn{
				{Name: "id", Type: "STRING", Description: "The id of the database."},
				{Name: "name", Type: "STRING", Description: "The name of the database."},
				{Name: "creationDate", Type: "STRING", Description: "The creation date of the database, formatted according to the ISO-8601 Standard."},
				{Name: "nodeCount", Type: "INTEGER", Description: "The number of nodes in the database."},
				{Name: "relationshipCount", Type: "INTEGER", Description: "The number of relationships in the database."},
			},
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfo(ctx)
			})
		registerProcedure(ProcedureSpec{
			Name:               "db.ping",
			Signature:          "db.ping() :: (success :: BOOLEAN)",
			Description:        localization.CypherProcedureMetadata("db.ping").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.ping"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      true,
			Returns:            []ProcedureColumn{{Name: "success", Type: "BOOLEAN", Description: "Whether or not the connection call to the database has been successful."}},
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPing()
			})
		registerProcedure(schemaTypePropertiesProcedureSpec("db.schema.nodeTypeProperties", true),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaTypeProperties(ctx, true)
			})
		registerProcedure(schemaTypePropertiesProcedureSpec("db.schema.relTypeProperties", false),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaTypeProperties(ctx, false)
			})
		registerProcedure(schemaVisualizationProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaVisualizationWithContext(ctx)
			})
		registerBuiltInProcedure("db.schema.nodeProperties", "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)", localization.CypherProcedureMetadata("db.schema.nodeProperties"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaNodeProperties()
			})
		registerBuiltInProcedure("db.schema.relProperties", "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)", localization.CypherProcedureMetadata("db.schema.relProperties"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaRelProperties()
			})

		registerProcedure(fulltextQueryProcedureSpec("db.index.fulltext.queryNodes", "node", "NODE"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryNodes(cypher)
			})
		registerProcedure(fulltextQueryProcedureSpec("db.index.fulltext.queryRelationships", "relationship", "RELATIONSHIP"),
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
		registerProcedure(fulltextAnalyzerProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextListAvailableAnalyzers()
			})

		registerProcedure(vectorQueryProcedureSpec("db.index.vector.queryNodes", "node", "NODE"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callVectorQueryArguments(ctx, args, false)
			})
		registerProcedure(vectorQueryProcedureSpec("db.index.vector.queryRelationships", "relationship", "RELATIONSHIP"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callVectorQueryArguments(ctx, args, true)
			})
		registerBuiltInProcedure("db.index.vector.embed", "db.index.vector.embed(text :: STRING) :: (embedding :: LIST<FLOAT>)", localization.CypherProcedureMetadata("db.index.vector.embed"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorEmbed(ctx, cypher)
			})
		registerProcedure(vectorCreateNodeProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateNodeIndexArguments(ctx, args)
			})
		registerBuiltInProcedure("db.index.vector.createRelationshipIndex", "db.index.vector.createRelationshipIndex(indexName :: STRING, relationshipType :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", localization.CypherProcedureMetadata("db.index.vector.createRelationshipIndex"), ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateRelationshipIndex(ctx, cypher)
			})
		registerBuiltInProcedure("db.index.vector.drop", "db.index.vector.drop(indexName :: STRING)", localization.CypherProcedureMetadata("db.index.vector.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorDrop(cypher)
			})

		registerProcedure(vectorSetterProcedureSpec("db.create.setNodeVectorProperty", "node", "NODE"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callSetVectorProperty(ctx, args, false)
			})
		registerProcedure(vectorSetterProcedureSpec("db.create.setRelationshipVectorProperty", "relationship", "RELATIONSHIP"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callSetVectorProperty(ctx, args, true)
			})

		registerProcedure(ProcedureSpec{
			Name:               "dbms.components",
			Signature:          "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)",
			Description:        localization.CypherProcedureMetadata("dbms.components").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.components"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Returns: []ProcedureColumn{
				{Name: "name", Type: "STRING", Description: "The name of the component."},
				{Name: "versions", Type: "LIST<STRING>", Description: "The installed versions of the component."},
				{Name: "edition", Type: "STRING", Description: "The NornicDB edition of the DBMS."},
			},
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsComponents()
			})
		registerProcedure(ProcedureSpec{
			Name:               "dbms.info",
			Signature:          "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)",
			Description:        localization.CypherProcedureMetadata("dbms.info").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.info"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Returns: []ProcedureColumn{
				{Name: "id", Type: "STRING", Description: "The id of the DBMS."},
				{Name: "name", Type: "STRING", Description: "The name of the DBMS."},
				{Name: "creationDate", Type: "STRING", Description: "The creation date of the DBMS."},
			},
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsInfo()
			})
		registerProcedure(dbmsListConfigProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConfigArguments(ctx, args)
			})
		registerBuiltInProcedure("dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, description :: STRING, value :: STRING, dynamic :: BOOLEAN, defaultValue :: STRING, startupValue :: STRING, explicitlySet :: BOOLEAN, validValues :: STRING)", localization.CypherProcedureMetadata("dbms.clientConfig"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsClientConfig()
			})
		registerProcedure(dbmsListConnectionsProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConnectionsWithContext(ctx)
			})
		registerBuiltInProcedure("dbms.procedures", "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING)", localization.CypherProcedureMetadata("dbms.procedures"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsProcedures()
			})
		registerBuiltInProcedure("dbms.functions", "dbms.functions() :: (name :: STRING, description :: STRING, category :: STRING)", localization.CypherProcedureMetadata("dbms.functions"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsFunctions()
			})

		// Query statistics procedures: canonical Neo4j 5.26 signatures. The
		// section argument is required; retrieve/collect accept an optional
		// config map.
		registerBuiltInProcedure("db.stats.retrieve", "db.stats.retrieve(section :: STRING, config = {} :: MAP) :: (section :: STRING, data :: MAP)", localization.CypherProcedureMetadata("db.stats.retrieve"), ProcedureModeDBMS, 1, 2, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "retrieve", args)
			})
		registerBuiltInProcedure("db.stats.collect", "db.stats.collect(section :: STRING, config = {} :: MAP) :: (section :: STRING, success :: BOOLEAN, message :: STRING)", localization.CypherProcedureMetadata("db.stats.collect"), ProcedureModeDBMS, 1, 2, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "collect", args)
			})
		registerBuiltInProcedure("db.stats.clear", "db.stats.clear(section :: STRING) :: (section :: STRING, success :: BOOLEAN, message :: STRING)", localization.CypherProcedureMetadata("db.stats.clear"), ProcedureModeDBMS, 1, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "clear", args)
			})
		registerBuiltInProcedure("db.stats.status", "db.stats.status() :: (section :: STRING, status :: STRING, data :: MAP)", localization.CypherProcedureMetadata("db.stats.status"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "status", nil)
			})
		registerBuiltInProcedure("db.stats.stop", "db.stats.stop(section :: STRING) :: (section :: STRING, success :: BOOLEAN, message :: STRING)", localization.CypherProcedureMetadata("db.stats.stop"), ProcedureModeDBMS, 1, 1, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "stop", args)
			})

		registerProcedure(awaitIndexProcedureSpec("db.awaitIndex", true),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNamedIndexManagement(args)
			})
		registerProcedure(awaitIndexProcedureSpec("db.awaitIndexes", false),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndexes(cypher)
			})
		registerProcedure(ProcedureSpec{
			Name:               "db.resampleIndex",
			Signature:          "db.resampleIndex(indexName :: STRING)",
			Description:        localization.CypherProcedureMetadata("db.resampleIndex").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.resampleIndex"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      true,
			Params:             []ProcedureParam{{Name: "indexName", Type: "STRING", Description: "The name of the index."}},
			MinArgs:            1,
			MaxArgs:            1,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNamedIndexManagement(args)
			})
		registerProcedure(ProcedureSpec{
			Name:               "db.clearQueryCaches",
			Signature:          "db.clearQueryCaches() :: (value :: STRING)",
			Description:        localization.CypherProcedureMetadata("db.clearQueryCaches").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.clearQueryCaches"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Admin:              true,
			Returns:            []ProcedureColumn{{Name: "value", Type: "STRING", Description: "Information about the number of cleared query caches."}},
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbClearQueryCaches()
			})

		registerProcedure(queryStatisticsProcedureSpec("collect"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "collect", args)
			})
		registerProcedure(queryStatisticsProcedureSpec("retrieve"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "retrieve", args)
			})
		registerBuiltInProcedure("db.stats.retrieveAllAnTheStats", "db.stats.retrieveAllAnTheStats()", localization.CypherProcedureMetadata("db.stats.retrieveAllAnTheStats"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.retrieveGraphStatistics(ctx, "ALL")
			})
		registerProcedure(queryStatisticsProcedureSpec("clear"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "clear", args)
			})
		registerProcedure(queryStatisticsProcedureSpec("status"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "status", args)
			})
		registerProcedure(queryStatisticsProcedureSpec("stop"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "stop", args)
			})

		registerProcedure(ProcedureSpec{
			Name:               "tx.setMetaData",
			Signature:          "tx.setMetaData(data :: MAP)",
			Description:        localization.CypherProcedureMetadata("tx.setMetaData").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("tx.setMetaData"),
			Mode:               ProcedureModeDBMS,
			Params:             []ProcedureParam{{Name: "data", Type: "MAP", Description: "Metadata to attach to the transaction."}},
			MinArgs:            1,
			MaxArgs:            1,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callTxSetMetadataArguments(ctx, args)
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

		// db.retrieve and db.rretrieve return one row per result, or, for a
		// paged request (mode, n / limit or a continuation qid), one row
		// with the page map in page (executeSearchContinuationPage); the signature
		// declares both so YIELD page holds after MATCH / WITH too (#946).
		registerBuiltInProcedure("db.retrieve", "db.retrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, page :: MAP)", localization.CypherProcedureMetadata("db.retrieve"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieve(ctx, cypher)
			})
		registerBuiltInProcedure("db.rretrieve", "db.rretrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, page :: MAP)", localization.CypherProcedureMetadata("db.rretrieve"), ProcedureModeRead, 1, 1, false,
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

		registerBuiltInProcedure("db.txlog.entries", "db.txlog.entries(fromSeq = null :: INTEGER, toSeq = null :: INTEGER) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.entries"), ProcedureModeDBMS, 0, 2, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogEntries(ctx, args)
			})
		registerBuiltInProcedure("db.txlog.byTxId", "db.txlog.byTxId(txId :: STRING, limit = null :: INTEGER) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.byTxId"), ProcedureModeDBMS, 1, 2, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogByTxID(ctx, args)
			})
		registerBuiltInProcedure("db.temporal.assertNoOverlap", "db.temporal.assertNoOverlap(args :: MAP) :: (ok :: BOOLEAN)", localization.CypherProcedureMetadata("db.temporal.assertNoOverlap"), ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAssertNoOverlap(ctx, cypher)
			})
		registerBuiltInProcedure("db.temporal.asOf", "db.temporal.asOf(args :: MAP) :: (node :: NODE)", localization.CypherProcedureMetadata("db.temporal.asOf"), ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAsOf(ctx, cypher)
			})

		registerBuiltInProcedureLiteral("apoc.path.subgraphNodes", "apoc.path.subgraphNodes(startNode :: ANY, config :: MAP) :: (node :: NODE)", "Returns the nodes reachable from the start node(s) under the config's filters", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSubgraphNodes(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.path.subgraphAll", "apoc.path.subgraphAll(startNode :: ANY, config :: MAP) :: (nodes :: LIST<NODE>, relationships :: LIST<RELATIONSHIP>)", "Returns the reachable nodes and the relationships between them", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSubgraphAll(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.path.expand", "apoc.path.expand(startNode :: ANY, relationshipFilter :: STRING, labelFilter :: STRING, minLevel :: INTEGER, maxLevel :: INTEGER) :: (path :: PATH)", "Expands paths from the start node(s)", ProcedureModeRead, 1, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathExpand(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.path.expandConfig", "apoc.path.expandConfig(startNode :: ANY, config :: MAP) :: (path :: PATH)", "Expands paths from the start node(s) under a config", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathExpandConfig(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.path.spanningTree", "apoc.path.spanningTree(startNode :: ANY, config :: MAP) :: (path :: PATH)", "Returns a path from the start node(s) to every node reached once", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPathSpanningTree(ctx, args)
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
		registerBuiltInProcedureLiteral("apoc.neighbors.tohop", "apoc.neighbors.tohop(node :: NODE, relTypes :: STRING = '', distance :: INTEGER = 1) :: (node :: NODE)", "Collects neighbors to N hops", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsTohop(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.neighbors.byhop", "apoc.neighbors.byhop(node :: NODE, relTypes :: STRING = '', distance :: INTEGER = 1) :: (nodes :: LIST<NODE>)", "Collects neighbors grouped by hop distance", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocNeighborsByhop(ctx, args)
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

func queryStatisticsProcedureSpec(action string) ProcedureSpec {
	name := "db.stats." + action
	description := localization.CypherProcedureMetadata(name)
	spec := ProcedureSpec{Name: name, Description: description.Fallback, DescriptionMessage: description, Mode: ProcedureModeRead, WorksOnSystem: true, Admin: true}
	if action == "status" {
		spec.Signature = name + "() :: (section :: STRING, status :: STRING, data :: MAP)"
		spec.Returns = []ProcedureColumn{{Name: "section", Type: "STRING", Description: "String with the message \"QUERIES\"."}, {Name: "status", Type: "STRING", Description: "The status of the QueryCollector: \"idle\" or \"collecting\"."}, {Name: "data", Type: "MAP", Description: "data :: MAP"}}
		return spec
	}
	spec.Params = []ProcedureParam{{Name: "section", Type: "STRING"}}
	spec.MaxArgs = 1
	arguments := "section :: STRING"
	if action == "collect" || action == "retrieve" {
		arguments += ", config = {} :: MAP"
		configDescription := "{durationSeconds = -1 :: INTEGER}"
		if action == "retrieve" {
			configDescription = "{maxInvocations = 100 :: INTEGER}"
		}
		spec.Params = append(spec.Params, ProcedureParam{Name: "config", Type: "MAP", Optional: true, Default: "DefaultParameterValue{value={}, type=MAP}", Description: configDescription})
		spec.MaxArgs = 2
	}
	if action == "retrieve" {
		spec.Signature = name + "(" + arguments + ") :: (section :: STRING, data :: MAP)"
		spec.Params[0].Description = "A section of stats to retrieve: ('GRAPH COUNTS', 'TOKENS', 'QUERIES', 'META')."
		spec.Returns = []ProcedureColumn{{Name: "section", Type: "STRING", Description: "The section retrieved."}, {Name: "data", Type: "MAP", Description: "Data pertaining to the retrieved statistics."}}
		return spec
	}
	spec.Signature = name + "(" + arguments + ") :: (section :: STRING, success :: BOOLEAN, message :: STRING)"
	spec.Params[0].Description = "The section to " + action + ". The only available section is: 'QUERIES'."
	sectionDescription := "The section collected."
	successDescription := "Whether the section was successfully collected."
	if action == "clear" {
		sectionDescription = "The section cleared."
		successDescription = "Whether the section was successfully cleared."
	}
	if action == "stop" {
		sectionDescription = "The stopped section."
		successDescription = "Whether the section was successfully stopped."
	}
	spec.Returns = []ProcedureColumn{{Name: "section", Type: "STRING", Description: sectionDescription}, {Name: "success", Type: "BOOLEAN", Description: successDescription}, {Name: "message", Type: "STRING", Description: "Details about the outcome of the procedure."}}
	return spec
}

func dbmsListConnectionsProcedureSpec() ProcedureSpec {
	name := "dbms.listConnections"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, serverAddress :: STRING, clientAddress :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeDBMS,
		WorksOnSystem:      true,
		Returns: []ProcedureColumn{
			{Name: "connectionId", Type: "STRING", Description: "The id of the connection."},
			{Name: "connectTime", Type: "STRING", Description: "The time the connection was established, formatted according to the ISO-8601 Standard."},
			{Name: "connector", Type: "STRING", Description: "The protocol of the connector."},
			{Name: "username", Type: "STRING", Description: "The username of the connected user."},
			{Name: "userAgent", Type: "STRING", Description: "The active agent."},
			{Name: "serverAddress", Type: "STRING", Description: "The address of the connected server."},
			{Name: "clientAddress", Type: "STRING", Description: "The address of the connected client."},
		},
	}
}

func dbmsListConfigProcedureSpec() ProcedureSpec {
	name := "dbms.listConfig"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(searchString =  :: STRING) :: (name :: STRING, description :: STRING, value :: STRING, dynamic :: BOOLEAN, defaultValue :: STRING, startupValue :: STRING, explicitlySet :: BOOLEAN, validValues :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeDBMS,
		WorksOnSystem:      true,
		Admin:              true,
		Params:             []ProcedureParam{{Name: "searchString", Type: "STRING", Optional: true, Default: "DefaultParameterValue{value=, type=STRING}", Description: "A string that filters on the name of config settings."}},
		Returns: []ProcedureColumn{
			{Name: "name", Type: "STRING", Description: "The name of the setting."},
			{Name: "description", Type: "STRING", Description: "The description of the setting."},
			{Name: "value", Type: "STRING", Description: "The set value of the setting."},
			{Name: "dynamic", Type: "BOOLEAN", Description: "If the setting can be set dynamically or not."},
			{Name: "defaultValue", Type: "STRING", Description: "The default value of the setting."},
			{Name: "startupValue", Type: "STRING", Description: "The value of the setting when the database started."},
			{Name: "explicitlySet", Type: "BOOLEAN", Description: "Whether or not the setting was explicitly set."},
			{Name: "validValues", Type: "STRING", Description: "A description of the valid values."},
		},
		MaxArgs: 1,
	}
}

func schemaVisualizationProcedureSpec() ProcedureSpec {
	name := "db.schema.visualization"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "() :: (nodes :: LIST<NODE>, relationships :: LIST<RELATIONSHIP>)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns: []ProcedureColumn{
			{Name: "nodes", Type: "LIST<NODE>", Description: "A list of virtual nodes representing each label in the database."},
			{Name: "relationships", Type: "LIST<RELATIONSHIP>", Description: "A list of virtual relationships representing all combinations between start and end nodes in the database."},
		},
	}
}

func fulltextAnalyzerProcedureSpec() ProcedureSpec {
	name := "db.index.fulltext.listAvailableAnalyzers"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "() :: (analyzer :: STRING, description :: STRING, stopwords :: LIST<STRING>, kind :: STRING, version :: STRING, digest :: STRING, dynamicLoad :: BOOLEAN, selectedDatabases :: LIST<STRING>)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns: []ProcedureColumn{
			{Name: "analyzer", Type: "STRING", Description: "The name of the analyzer."},
			{Name: "description", Type: "STRING", Description: "The  description of the analyzer."},
			{Name: "stopwords", Type: "LIST<STRING>", Description: "The stopwords used by the analyzer to tokenize strings."},
			{Name: "kind", Type: "STRING", Description: "The native analyzer implementation kind."},
			{Name: "version", Type: "STRING", Description: "The registered stemmer plugin version."},
			{Name: "digest", Type: "STRING", Description: "The abbreviated registered plugin artifact digest."},
			{Name: "dynamicLoad", Type: "BOOLEAN", Description: "Whether this platform supports dynamically loaded stemmer plugins."},
			{Name: "selectedDatabases", Type: "LIST<STRING>", Description: "The database selections reported for this analyzer."},
		},
	}
}

func fulltextQueryProcedureSpec(name, entityName, entityType string) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(indexName :: STRING, queryString :: STRING, options = {} :: MAP) :: (" + entityName + " :: " + entityType + ", score :: FLOAT)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		MinArgs:            2,
		MaxArgs:            3,
		Params: []ProcedureParam{
			{Name: "indexName", Type: "STRING", Description: "The name of the full-text index."},
			{Name: "queryString", Type: "STRING", Description: "The string to find approximate matches for."},
			{Name: "options", Type: "MAP", Optional: true, Description: "{skip :: INTEGER, limit :: INTEGER, analyzer :: STRING}", Default: "DefaultParameterValue{value={}, type=MAP}"},
		},
		Returns: []ProcedureColumn{
			{Name: entityName, Type: entityType, Description: "A " + entityName + " which contains a property similar to the query string."},
			{Name: "score", Type: "FLOAT", Description: "The score measuring how similar the " + entityName + " property is to the query string."},
		},
	}
}

func tokenListingProcedureSpec(name, columnName, columnDescription string) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "() :: (" + columnName + " :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns:            []ProcedureColumn{{Name: columnName, Type: "STRING", Description: columnDescription}},
	}
}

func awaitIndexProcedureSpec(name string, single bool) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	params := []ProcedureParam{{Name: "timeOutSeconds", Type: "INTEGER", Optional: true, Default: "DefaultParameterValue{value=300, type=INTEGER}", Description: "The maximum time to wait in seconds."}}
	arguments := "timeOutSeconds = 300 :: INTEGER"
	minimum := 0
	if single {
		params = append([]ProcedureParam{{Name: "indexName", Type: "STRING", Description: "The name of the awaited index."}}, params...)
		arguments = "indexName :: STRING, " + arguments
		minimum = 1
	}
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(" + arguments + ")",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Params:             params,
		MinArgs:            minimum,
		MaxArgs:            len(params),
	}
}

func vectorQueryProcedureSpec(name, entityName, entityType string) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(indexName :: STRING, numberOfNearestNeighbours :: INTEGER, query :: ANY) :: (" + entityName + " :: " + entityType + ", score :: FLOAT)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		Params: []ProcedureParam{
			{Name: "indexName", Type: "STRING", Description: "The name of the vector index."},
			{Name: "numberOfNearestNeighbours", Type: "INTEGER", Description: "The size of the vector neighbourhood."},
			{Name: "query", Type: "ANY", Description: "The object to find approximate matches for."},
		},
		Returns: []ProcedureColumn{
			{Name: entityName, Type: entityType, Description: "A " + entityName + " which contains a vector property similar to the query object."},
			{Name: "score", Type: "FLOAT", Description: "The score measuring how similar the " + entityName + " property is to the query object."},
		},
		MinArgs: 3,
		MaxArgs: 3,
	}
}

func vectorCreateNodeProcedureSpec() ProcedureSpec {
	name := "db.index.vector.createNodeIndex"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(indexName :: STRING, label :: STRING, propertyKey :: STRING, vectorDimension :: INTEGER, vectorSimilarityFunction :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeSchema,
		IsDeprecated:       true,
		DeprecatedBy:       "CREATE VECTOR INDEX",
		Params: []ProcedureParam{
			{Name: "indexName", Type: "STRING", Description: "indexName :: STRING"},
			{Name: "label", Type: "STRING", Description: "label :: STRING"},
			{Name: "propertyKey", Type: "STRING", Description: "propertyKey :: STRING"},
			{Name: "vectorDimension", Type: "INTEGER", Description: "vectorDimension :: INTEGER"},
			{Name: "vectorSimilarityFunction", Type: "STRING", Description: "vectorSimilarityFunction :: STRING"},
		},
		MinArgs: 4,
		MaxArgs: 5,
	}
}

func vectorSetterProcedureSpec(name, entityName, entityType string) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(" + entityName + " :: " + entityType + ", key :: STRING, vector :: ANY)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeWrite,
		Params: []ProcedureParam{
			{Name: entityName, Type: entityType, Description: "The " + entityName + " on which the new property will be stored."},
			{Name: "key", Type: "STRING", Description: "The name of the new property."},
			{Name: "vector", Type: "ANY", Description: "The object containing the embedding."},
		},
		MinArgs: 3,
		MaxArgs: 3,
	}
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
	arguments, returnSignature := functionSignatureDescriptions(spec.Signature)
	if len(spec.Params) == 0 {
		for index, argument := range arguments {
			description := argument.(map[string]interface{})
			defaultValue, _ := description["default"].(string)
			spec.Params = append(spec.Params, ProcedureParam{
				Name:     description["name"].(string),
				Type:     description["type"].(string),
				Optional: index >= spec.MinArgs,
				Default:  defaultValue,
			})
		}
	}
	if len(spec.Returns) == 0 && strings.HasPrefix(returnSignature, "(") {
		columns, _ := functionSignatureDescriptions("returns" + returnSignature)
		for _, column := range columns {
			description := column.(map[string]interface{})
			spec.Returns = append(spec.Returns, ProcedureColumn{
				Name: description["name"].(string),
				Type: description["type"].(string),
			})
		}
	}
	_ = globalProcedureRegistry.RegisterBuiltIn(spec, handler)
}

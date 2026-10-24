package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
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
				return e.callDbIndexFulltextQueryNodes(args)
			})
		registerProcedure(fulltextQueryProcedureSpec("db.index.fulltext.queryRelationships", "relationship", "RELATIONSHIP"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryRelationships(args)
			})
		registerBuiltInProcedure("db.index.fulltext.createNodeIndex", "db.index.fulltext.createNodeIndex(indexName :: STRING, labels :: LIST<STRING>, properties :: LIST<STRING>)", localization.CypherProcedureMetadata("db.index.fulltext.createNodeIndex"), ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateNodeIndex(ctx, args)
			})
		registerBuiltInProcedure("db.index.fulltext.createRelationshipIndex", "db.index.fulltext.createRelationshipIndex(indexName :: STRING, relationshipTypes :: LIST<STRING>, properties :: LIST<STRING>)", localization.CypherProcedureMetadata("db.index.fulltext.createRelationshipIndex"), ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateRelationshipIndex(ctx, args)
			})
		registerBuiltInProcedure("db.index.fulltext.drop", "db.index.fulltext.drop(indexName :: STRING)", localization.CypherProcedureMetadata("db.index.fulltext.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextDrop(args)
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
				return e.callDbIndexVectorEmbed(ctx, args)
			})
		registerProcedure(vectorCreateNodeProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateIndex(ctx, args, storage.ConstraintEntityNode)
			})
		registerBuiltInProcedure("db.index.vector.createRelationshipIndex", "db.index.vector.createRelationshipIndex(indexName :: STRING, relationshipType :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", localization.CypherProcedureMetadata("db.index.vector.createRelationshipIndex"), ProcedureModeWrite, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateIndex(ctx, args, storage.ConstraintEntityRelationship)
			})
		registerBuiltInProcedure("db.index.vector.drop", "db.index.vector.drop(indexName :: STRING)", localization.CypherProcedureMetadata("db.index.vector.drop"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorDrop(args)
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
		registerBuiltInProcedure("dbms.procedures", "dbms.procedures() :: (name :: STRING, description :: STRING, mode :: STRING, signature :: STRING)", localization.CypherProcedureMetadata("dbms.procedures"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsProcedures()
			})
		registerBuiltInProcedure("dbms.functions", "dbms.functions() :: (name :: STRING, description :: STRING, category :: STRING)", localization.CypherProcedureMetadata("dbms.functions"), ProcedureModeDBMS, 0, 0, true,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsFunctions()
			})

		registerProcedure(awaitIndexProcedureSpec("db.awaitIndex", true),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNamedIndexManagement("db.awaitIndex", args)
			})
		registerProcedure(awaitIndexProcedureSpec("db.awaitIndexes", false),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndexes()
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
				return e.callNamedIndexManagement("db.resampleIndex", args)
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
		registerAdminBuiltInProcedure("db.stats.retrieveAllAnTheStats", "db.stats.retrieveAllAnTheStats()", localization.CypherProcedureMetadata("db.stats.retrieveAllAnTheStats"), ProcedureModeDBMS, 0, 0, true,
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
		registerBuiltInProcedure("kp.decayInfo", "kp.decayInfo() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)", localization.CypherProcedureMetadata("kp.decayInfo"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbDecayInfo()
			})
		registerBuiltInProcedure("kp.info", "kp.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)", localization.CypherProcedureMetadata("kp.info"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyInfo()
			})
		registerBuiltInProcedure("kp.profiles", "kp.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER, Apply :: STRING)", localization.CypherProcedureMetadata("kp.profiles"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyProfiles()
			})
		registerBuiltInProcedure("kp.policies", "kp.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, Apply :: STRING)", localization.CypherProcedureMetadata("kp.policies"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyPolicies()
			})
		registerBuiltInProcedure("kp.resolve", "kp.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (TargetID :: STRING, TargetScope :: STRING, ResolvedDecayProfileID :: STRING, ResolvedScoreFrom :: STRING, ResolutionSourceChain :: LIST<STRING>, AppliedDecayProfileNames :: LIST<STRING>, AppliedPromotionPolicyName :: STRING, AppliedPromotionProfileName :: STRING, EffectiveRate :: FLOAT, EffectiveThreshold :: FLOAT, EffectiveMultiplier :: FLOAT, BaseScore :: FLOAT, FinalScore :: FLOAT, NoDecay :: BOOLEAN, SuppressionEligible :: BOOLEAN, Explanation :: STRING)", localization.CypherProcedureMetadata("kp.resolve"), ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyResolve(args)
			})
		registerBuiltInProcedure("kp.deindexStatus", "kp.deindexStatus() :: (pending_count :: INTEGER, supported :: BOOLEAN, message :: STRING, workItemId :: STRING, targetId :: STRING, targetScope :: STRING, enqueuedAt :: INTEGER, status :: STRING)", localization.CypherProcedureMetadata("kp.deindexStatus"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyDeindexStatus()
			})
		// The knowledge-policy procedures are kp.*, a vendor namespace as apoc.*
		// and gds.* are (#1078); their earlier nornicdb.* names stay as
		// deprecated aliases of the same procedures.
		for _, alias := range [][2]string{
			{"nornicdb.decay.info", "kp.decayInfo"},
			{"nornicdb.knowledgepolicy.info", "kp.info"},
			{"nornicdb.knowledgepolicy.profiles", "kp.profiles"},
			{"nornicdb.knowledgepolicy.policies", "kp.policies"},
			{"nornicdb.knowledgepolicy.resolve", "kp.resolve"},
			{"nornicdb.knowledgepolicy.deindexStatus", "kp.deindexStatus"},
		} {
			registerDeprecatedProcedureAlias(alias[0], alias[1])
		}

		// db.retrieve and db.rretrieve return one row per result, or, for a
		// paged request (mode, n / limit or a continuation qid), one row
		// with the page map in page (executeSearchContinuationPage); the signature
		// declares both so YIELD page holds after MATCH / WITH too (#946).
		registerBuiltInProcedure("db.retrieve", "db.retrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, query_chunks_searched :: INTEGER, query_chunks_total :: INTEGER, matches :: LIST<MAP>, page :: MAP)", localization.CypherProcedureMetadata("db.retrieve"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieve(ctx, args)
			})
		registerBuiltInProcedure("db.rretrieve", "db.rretrieve(request :: MAP) :: (node :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, query_chunks_searched :: INTEGER, query_chunks_total :: INTEGER, matches :: LIST<MAP>, page :: MAP)", localization.CypherProcedureMetadata("db.rretrieve"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieve(ctx, args)
			})
		registerBuiltInProcedure("kp.suppress", "kp.suppress(entity :: ANY)", localization.CypherProcedureMetadata("kp.suppress"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callKpSuppress(ctx, "kp.suppress", args, true)
			})
		registerBuiltInProcedure("kp.unsuppress", "kp.unsuppress(entity :: ANY)", localization.CypherProcedureMetadata("kp.unsuppress"), ProcedureModeWrite, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callKpSuppress(ctx, "kp.unsuppress", args, false)
			})
		registerBuiltInProcedure("db.rerank", "db.rerank(request :: MAP) :: (id :: STRING, content :: STRING, original_rank :: INTEGER, new_rank :: INTEGER, bi_score :: FLOAT, cross_score :: FLOAT, final_score :: FLOAT)", localization.CypherProcedureMetadata("db.rerank"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRerank(ctx, args)
			})
		// db.retrieveRelationships searches relationship properties, when the
		// database indexes them (#1065).
		registerBuiltInProcedure("db.retrieveRelationships", "db.retrieveRelationships(request :: MAP) :: (relationship :: RELATIONSHIP, start :: NODE, end :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING)", localization.CypherProcedureMetadata("db.retrieveRelationships"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieveRelationships(ctx, args)
			})
		// db.retrieveGraph ranks nodes and relationships together: rows, or
		// with n / maxResults one terminal page; the signature declares both,
		// as db.rretrieve's does, so YIELD page holds after MATCH / WITH too.
		registerBuiltInProcedure("db.retrieveGraph", "db.retrieveGraph(request :: MAP) :: (kind :: STRING, node :: NODE, relationship :: RELATIONSHIP, start :: NODE, end :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, query_chunks_searched :: INTEGER, query_chunks_total :: INTEGER, matches :: LIST<MAP>, page :: MAP)", localization.CypherProcedureMetadata("db.retrieveGraph"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieveGraph(ctx, args)
			})
		// db.rretrieveGraph is db.retrieveGraph with the configured reranker
		// ordering the fused pool of both kinds, as db.rretrieve is db.retrieve's.
		registerBuiltInProcedure("db.rretrieveGraph", "db.rretrieveGraph(request :: MAP) :: (kind :: STRING, node :: NODE, relationship :: RELATIONSHIP, start :: NODE, end :: NODE, score :: FLOAT, rrf_score :: FLOAT, vector_rank :: INTEGER, bm25_rank :: INTEGER, search_method :: STRING, fallback_triggered :: BOOLEAN, fallback_reason :: STRING, query_chunks_searched :: INTEGER, query_chunks_total :: INTEGER, matches :: LIST<MAP>, page :: MAP)", localization.CypherProcedureMetadata("db.rretrieveGraph"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieveGraph(ctx, args)
			})
		registerBuiltInProcedure("db.infer", "db.infer(request :: MAP) :: (text :: STRING, structured :: ANY, model :: STRING, usage :: MAP, latencyMs :: INTEGER, finishReason :: STRING)", localization.CypherProcedureMetadata("db.infer"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfer(ctx, args)
			})

		registerBuiltInProcedure("db.cdc.current", "db.cdc.current() :: (id :: STRING)", localization.CypherProcedureMetadata("db.cdc.current"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcCurrent()
			})
		registerBuiltInProcedure("db.cdc.earliest", "db.cdc.earliest() :: (id :: STRING)", localization.CypherProcedureMetadata("db.cdc.earliest"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcEarliest()
			})
		registerAdminBuiltInProcedure("db.cdc.query", "db.cdc.query(from = \"\" :: STRING, selectors = [] :: LIST<MAP>, limit = null :: INTEGER) :: (id :: STRING, txId :: INTEGER, seq :: INTEGER, metadata :: MAP, event :: MAP)", localization.CypherProcedureMetadata("db.cdc.query"), ProcedureModeDBMS, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcQuery(args)
			})
		registerAdminBuiltInProcedure("db.txlog.entries", "db.txlog.entries(fromSeq = null :: INTEGER, toSeq = null :: INTEGER) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.entries"), ProcedureModeDBMS, 0, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogEntries(ctx, args)
			})
		registerAdminBuiltInProcedure("db.txlog.byTxId", "db.txlog.byTxId(txId :: STRING, limit = null :: INTEGER) :: (txId :: STRING, db :: STRING, kind :: STRING, seq :: INTEGER, timestamp :: STRING, payload :: STRING)", localization.CypherProcedureMetadata("db.txlog.byTxId"), ProcedureModeDBMS, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTxlogByTxID(ctx, args)
			})
		registerBuiltInProcedure("db.temporal.assertNoOverlap", "db.temporal.assertNoOverlap(label :: STRING, keyProp :: STRING, validFromProp :: STRING, validToProp :: STRING, keyValue :: ANY, newValidFrom :: ANY, newValidTo :: ANY, systemTime = null :: ANY, systemSequence = null :: INTEGER) :: (ok :: BOOLEAN)", localization.CypherProcedureMetadata("db.temporal.assertNoOverlap"), ProcedureModeRead, 7, 9, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAssertNoOverlap(ctx, args)
			})
		registerBuiltInProcedure("db.temporal.asOf", "db.temporal.asOf(label :: STRING, keyProp :: STRING, keyValue :: ANY, validFromProp :: STRING, validToProp :: STRING, asOf :: ANY, systemTime = null :: ANY, systemSequence = null :: INTEGER) :: (node :: NODE)", localization.CypherProcedureMetadata("db.temporal.asOf"), ProcedureModeRead, 6, 8, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAsOf(ctx, args)
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
		// A custom procedure is one for the whole server (the procedure
		// registry is process-wide), and whoever calls it runs its body, on
		// any database: installing, replacing and dropping one needs the
		// admin entitlement, as CREATE / DROP PROCEDURE does and as APOC 5
		// marks apoc.custom.installProcedure and dropProcedure (@Admin).
		registerAdminBuiltInProcedureLiteral("apoc.custom.installProcedure", "apoc.custom.installProcedure(signature :: STRING, statement :: STRING, databaseName :: STRING = 'neo4j', mode :: STRING = 'read', description :: STRING = '')", "Installs a statement as the procedure custom.<name>", ProcedureModeWrite, 2, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomInstall(ctx, "apoc.custom.installProcedure", args, true)
			})
		registerAdminBuiltInProcedureLiteral("apoc.custom.declareProcedure", "apoc.custom.declareProcedure(signature :: STRING, statement :: STRING, mode :: STRING = 'read', description :: STRING = '')", "Installs a statement as the procedure custom.<name> (APOC 4 form)", ProcedureModeWrite, 2, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomInstall(ctx, "apoc.custom.declareProcedure", args, false)
			})
		registerAdminBuiltInProcedureLiteral("apoc.custom.dropProcedure", "apoc.custom.dropProcedure(name :: STRING, databaseName :: STRING = 'neo4j')", "Drops the custom procedure custom.<name>", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomDrop(ctx, args)
			})
		for _, name := range []string{"apoc.custom.list", "apoc.custom.show"} {
			registerBuiltInProcedureLiteral(name, name+"() :: (type :: STRING, name :: STRING, description :: STRING, mode :: STRING, statement :: STRING, inputs :: LIST<ANY>, outputs :: LIST<ANY>, forceSingle :: BOOLEAN)", "Lists the custom procedures", ProcedureModeRead, 0, 1, false,
				func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
					return e.callApocCustomList(ctx)
				})
		}
		registerStatementProcedureLiteral("apoc.cypher.run", "apoc.cypher.run(statement :: STRING, params :: MAP) :: (value :: MAP)", "Runs dynamic Cypher", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.run", args)
			})
		registerBuiltInProcedureLiteral("apoc.cypher.doIt", "apoc.cypher.doIt(statement :: STRING, params :: MAP) :: (value :: MAP)", "Runs dynamic Cypher", ProcedureModeWrite, 2, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.doIt", args)
			})
		registerStatementProcedureLiteral("apoc.cypher.doitall", "apoc.cypher.doitall(statement :: STRING, params :: MAP) :: (value :: MAP)", "Alias of apoc.cypher.run", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.doitall", args)
			})
		registerBuiltInProcedureLiteral("apoc.cypher.runMany", "apoc.cypher.runMany(statements :: STRING, params :: MAP, config :: MAP = {}) :: (row :: INTEGER, result :: MAP)", "Runs many Cypher statements", ProcedureModeWrite, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRunMany(ctx, args)
			})
		registerBatchProcedureLiteral("apoc.periodic.iterate", "apoc.periodic.iterate(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, timeTaken :: INTEGER, committedOperations :: INTEGER, failedOperations :: INTEGER, failedBatches :: INTEGER, retries :: INTEGER, errorMessages :: MAP, batch :: MAP, operations :: MAP, wasTerminated :: BOOLEAN, failedParams :: MAP, updateStatistics :: MAP)", "Runs batch iterate/action jobs", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, "apoc.periodic.iterate", args)
			})
		registerBatchProcedureLiteral("apoc.periodic.commit", "apoc.periodic.commit(statement :: STRING, params :: MAP) :: (updates :: INTEGER, executions :: INTEGER, runtime :: INTEGER, batches :: INTEGER)", "Runs periodic commits", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicCommit(ctx, args)
			})
		registerBatchProcedureLiteral("apoc.periodic.rock_n_roll", "apoc.periodic.rock_n_roll(iterate :: STRING, action :: STRING, config :: MAP) :: (batches :: INTEGER, total :: INTEGER, timeTaken :: INTEGER, committedOperations :: INTEGER, failedOperations :: INTEGER, failedBatches :: INTEGER, retries :: INTEGER, errorMessages :: MAP, batch :: MAP, operations :: MAP, wasTerminated :: BOOLEAN, failedParams :: MAP, updateStatistics :: MAP)", "Alias of apoc.periodic.iterate", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, "apoc.periodic.rock_n_roll", args)
			})

		registerBuiltInProcedure("gds.version", "gds.version() :: (gdsVersion :: STRING)", localization.CypherProcedureMetadata("gds.version"), ProcedureModeRead, 0, 0, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsVersion()
			})
		registerBuiltInProcedure("gds.graph.exists", "gds.graph.exists(graphName :: STRING) :: (graphName :: STRING, exists :: BOOLEAN)", localization.CypherProcedureMetadata("gds.graph.exists"), ProcedureModeRead, 1, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphExists(args)
			})
		registerBuiltInProcedure("gds.graph.list", "gds.graph.list(graphName = __ALL__ :: STRING) :: (degreeDistribution :: MAP, graphName :: STRING, database :: STRING, databaseLocation :: STRING, memoryUsage :: STRING, sizeInBytes :: INTEGER, nodeCount :: INTEGER, relationshipCount :: INTEGER, configuration :: MAP, density :: FLOAT, creationTime :: ZONED DATETIME, modificationTime :: ZONED DATETIME, schema :: MAP, schemaWithOrientation :: MAP)", localization.CypherProcedureMetadata("gds.graph.list"), ProcedureModeRead, 0, 1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphList(args)
			})
		registerBuiltInProcedure("gds.graph.drop", "gds.graph.drop(graphName :: ANY, failIfMissing = true :: BOOLEAN, dbName =  :: STRING, username =  :: STRING) :: (graphName :: STRING, database :: STRING, databaseLocation :: STRING, memoryUsage :: STRING, sizeInBytes :: INTEGER, nodeCount :: INTEGER, relationshipCount :: INTEGER, configuration :: MAP, density :: FLOAT, creationTime :: ZONED DATETIME, modificationTime :: ZONED DATETIME, schema :: MAP, schemaWithOrientation :: MAP)", localization.CypherProcedureMetadata("gds.graph.drop"), ProcedureModeWrite, 1, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphDrop(ctx, args)
			})
		registerBuiltInProcedure("gds.graph.project", "gds.graph.project(graphName :: STRING, nodeProjection :: ANY, relationshipProjection :: ANY, configuration = {} :: MAP) :: (nodeProjection :: MAP, relationshipProjection :: MAP, graphName :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER, projectMillis :: INTEGER)", localization.CypherProcedureMetadata("gds.graph.project"), ProcedureModeWrite, 3, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphProject(args)
			})
		registerBuiltInProcedure("gds.fastRP.stream", "gds.fastRP.stream(graphName :: STRING, configuration = {} :: MAP) :: (nodeId :: STRING, embedding :: LIST<FLOAT>)", localization.CypherProcedureMetadata("gds.fastRP.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStream(args)
			})
		registerBuiltInProcedure("gds.fastRP.stats", "gds.fastRP.stats(graphName :: STRING, configuration = {} :: MAP) :: (nodeCount :: INTEGER, preProcessingMillis :: INTEGER, computeMillis :: INTEGER, configuration :: MAP)", localization.CypherProcedureMetadata("gds.fastRP.stats"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStats(args)
			})
		registerBuiltInProcedure("gds.linkPrediction.adamicAdar.stream", "gds.linkPrediction.adamicAdar.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.adamicAdar.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionAdamicAdar(ctx, args)
			})
		registerBuiltInProcedure("gds.linkPrediction.commonNeighbors.stream", "gds.linkPrediction.commonNeighbors.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.commonNeighbors.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionCommonNeighbors(ctx, args)
			})
		registerBuiltInProcedure("gds.linkPrediction.resourceAllocation.stream", "gds.linkPrediction.resourceAllocation.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.resourceAllocation.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionResourceAllocation(ctx, args)
			})
		registerBuiltInProcedure("gds.linkPrediction.preferentialAttachment.stream", "gds.linkPrediction.preferentialAttachment.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.preferentialAttachment.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPreferentialAttachment(ctx, args)
			})
		registerBuiltInProcedure("gds.linkPrediction.jaccard.stream", "gds.linkPrediction.jaccard.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT)", localization.CypherProcedureMetadata("gds.linkPrediction.jaccard.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionJaccard(ctx, args)
			})
		registerBuiltInProcedure("gds.linkPrediction.predict.stream", "gds.linkPrediction.predict.stream(graphNameOrConfig :: ANY, config :: MAP) :: (node1 :: STRING, node2 :: STRING, score :: FLOAT, topology_score :: FLOAT, semantic_score :: FLOAT, reason :: STRING)", localization.CypherProcedureMetadata("gds.linkPrediction.predict.stream"), ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsLinkPredictionPredict(ctx, args)
			})

		registerBuiltInProcedureLiteral("apoc.algo.dijkstra", "apoc.algo.dijkstra(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, defaultWeight :: FLOAT) :: (path :: PATH, weight :: FLOAT)", "Runs weighted shortest path", ProcedureModeRead, 4, 5, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoDijkstra(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.aStar", "apoc.algo.aStar(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, latPropertyName :: STRING, lonPropertyName :: STRING) :: (path :: PATH, weight :: FLOAT)", "Runs A* shortest path", ProcedureModeRead, 0, -1, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAStar(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.allSimplePaths", "apoc.algo.allSimplePaths(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, maxNodes :: INTEGER) :: (path :: PATH)", "Enumerates all simple paths", ProcedureModeRead, 4, 4, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoAllSimplePaths(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.pageRank", "apoc.algo.pageRank(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, score :: FLOAT)", "Runs PageRank", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoPageRank(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.betweenness", "apoc.algo.betweenness(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, score :: FLOAT)", "Runs betweenness centrality", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoBetweenness(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.closeness", "apoc.algo.closeness(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, score :: FLOAT)", "Runs closeness centrality", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoCloseness(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.louvain", "apoc.algo.louvain(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, community :: INTEGER)", "Runs Louvain community detection", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLouvain(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.labelPropagation", "apoc.algo.labelPropagation(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, community :: INTEGER)", "Runs label propagation", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoLabelPropagation(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.algo.wcc", "apoc.algo.wcc(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP) :: (node :: NODE, componentId :: INTEGER)", "Runs weakly connected components", ProcedureModeRead, 0, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocAlgoWCC(ctx, args)
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
				return e.callApocLoadJson(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.load.jsonArray", "apoc.load.jsonArray(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {}) :: (value :: MAP)", "Loads JSON array", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadJsonArray(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.load.csv", "apoc.load.csv(urlOrBinary :: STRING, config :: MAP = {}, nullValues :: LIST<STRING> = []) :: (lineNo :: INTEGER, list :: LIST<STRING>, map :: MAP)", "Loads CSV", ProcedureModeRead, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocLoadCsv(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.export.json.all", "apoc.export.json.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to JSON", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonAll(ctx, args)
			})
		registerStatementProcedureLiteral("apoc.export.json.query", "apoc.export.json.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to JSON", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportJsonQuery(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.export.csv.all", "apoc.export.csv.all(file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports graph to CSV", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvAll(ctx, args)
			})
		registerStatementProcedureLiteral("apoc.export.csv.query", "apoc.export.csv.query(query :: STRING, file :: STRING, config :: MAP = {}) :: (file :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Exports query result to CSV", ProcedureModeRead, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocExportCsvQuery(ctx, args)
			})
		registerBuiltInProcedureLiteral("apoc.import.json", "apoc.import.json(url :: STRING, config :: MAP = {}) :: (file :: STRING, source :: STRING, format :: STRING, nodes :: INTEGER, relationships :: INTEGER, properties :: INTEGER, time :: INTEGER, rows :: INTEGER, batchSize :: INTEGER, batches :: INTEGER, done :: BOOLEAN, data :: STRING)", "Imports JSON", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocImportJson(ctx, args)
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

// registerAdminBuiltInProcedure is registerBuiltInProcedure for a procedure
// only an administrator runs (ProcedureSpec.Admin).
func registerAdminBuiltInProcedure(name, signature string, description localization.Message, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name:               name,
		Signature:          signature,
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               mode,
		WorksOnSystem:      worksOnSystem,
		Admin:              true,
		MinArgs:            minArgs,
		MaxArgs:            maxArgs,
	}, handler)
}

// registerDeprecatedProcedureAlias registers alias as a deprecated name of the
// built-in procedure name: the same handler, arguments and results, with
// SHOW PROCEDURES reporting it deprecated by name.
func registerDeprecatedProcedureAlias(alias, name string) {
	procedure, ok := globalProcedureRegistry.Get(name)
	if !ok {
		panic("deprecated procedure alias " + alias + " of unregistered " + name)
	}
	description := localization.CypherProcedureMetadata(alias)
	spec := procedure.Spec
	spec.Name = alias
	spec.Signature = alias + strings.TrimPrefix(spec.Signature, name)
	spec.Description = description.Fallback
	spec.DescriptionMessage = description
	spec.IsDeprecated = true
	spec.DeprecatedBy = name
	_ = globalProcedureRegistry.RegisterBuiltIn(spec, procedure.Handler)
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

// registerStatementProcedureLiteral records a READ procedure's executable
// argument in the same registry that declares ordinary WRITE procedures.
func registerStatementProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name: name, Signature: signature, Description: description, Mode: mode,
		WorksOnSystem: worksOnSystem, MinArgs: minArgs, MaxArgs: maxArgs,
		ExecutesStatements: true,
	}, handler)
}

// registerAdminBuiltInProcedureLiteral is registerBuiltInProcedureLiteral
// for a procedure only an administrator runs (ProcedureSpec.Admin).
func registerAdminBuiltInProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name:          name,
		Signature:     signature,
		Description:   description,
		Mode:          mode,
		WorksOnSystem: worksOnSystem,
		Admin:         true,
		MinArgs:       minArgs,
		MaxArgs:       maxArgs,
	}, handler)
}

func registerProcedure(spec ProcedureSpec, handler ProcedureHandler) {
	if legacyProcedureColumns(spec.Name) != nil {
		name := spec.Name
		spec.ResultSchemas = func() [][]string { return [][]string{legacyProcedureColumns(name)} }
	}
	switch spec.Name {
	case "db.retrieve", "db.rretrieve":
		spec.ResultSchemas = func() [][]string { return retrievalResultSchemas(false) }
	case "db.retrieveGraph", "db.rretrieveGraph":
		spec.ResultSchemas = func() [][]string { return retrievalResultSchemas(true) }
	}
	if dependencies, declared := builtinProcedureResultCache[spec.Name]; declared {
		spec.ResultCache = dependencies
	}
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

func registerBatchProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name: name, Signature: signature, Description: description, Mode: mode,
		WorksOnSystem: worksOnSystem, MinArgs: minArgs, MaxArgs: maxArgs, OwnsTransactions: true,
	}, handler)
}

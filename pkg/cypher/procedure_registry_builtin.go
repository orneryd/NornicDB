package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

import (
	"context"
	"strings"
	"sync"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var builtinProcedureRegistryOnce sync.Once

func ensureBuiltInProceduresRegistered() {
	builtinProcedureRegistryOnce.Do(func() {
		registerProcedure(tokenListingProcedureSpec("db.labels", dbLabelsColumns, "A label within the database."),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbLabels(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(tokenListingProcedureSpec("db.relationshipTypes", dbRelationshipTypesColumns, "A relationship type in the database."),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRelationshipTypes(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(tokenListingProcedureSpec("db.propertyKeys", dbPropertyKeysColumns, "A property key in the database."),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPropertyKeys(ctx)
			}, cypherfn.CanonicalReads)
		// These handlers report their actual schema catalog acquisitions through
		// the statement context; custom replacements retain opaque policy.
		registerProcedure(ProcedureSpec{
			Name:               "db.indexes",
			Signature:          procedureReturnSignature("db.indexes()", dbIndexesColumns),
			Description:        localization.CypherProcedureMetadata("db.indexes").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.indexes"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexes(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.index.stats",
			Signature:          procedureReturnSignature("db.index.stats()", dbIndexStatsColumns),
			Description:        localization.CypherProcedureMetadata("db.index.stats").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.index.stats"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexStats(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.constraints",
			Signature:          procedureReturnSignature("db.constraints()", dbConstraintsColumns),
			Description:        localization.CypherProcedureMetadata("db.constraints").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.constraints"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbConstraints(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.info",
			Signature:          "db.info()",
			Description:        localization.CypherProcedureMetadata("db.info").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.info"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      true,
			Returns:            dbInfoColumns,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callInfo(ctx, infoScopeDatabase)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.ping",
			Signature:          "db.ping()",
			Description:        localization.CypherProcedureMetadata("db.ping").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.ping"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      true,
			Returns:            dbPingColumns,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbPing()
			}, cypherfn.CanonicalReads)
		registerProcedure(schemaTypePropertiesProcedureSpec("db.schema.nodeTypeProperties", true),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaTypeProperties(ctx, true)
			}, cypherfn.CanonicalReads)
		registerProcedure(schemaTypePropertiesProcedureSpec("db.schema.relTypeProperties", false),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaTypeProperties(ctx, false)
			}, cypherfn.CanonicalReads)
		registerProcedure(schemaVisualizationProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaVisualizationWithContext(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.schema.nodeProperties",
			Signature:          procedureReturnSignature("db.schema.nodeProperties()", dbSchemaNodePropertiesColumns),
			Description:        localization.CypherProcedureMetadata("db.schema.nodeProperties").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.schema.nodeProperties"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaNodeProperties(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.schema.relProperties",
			Signature:          procedureReturnSignature("db.schema.relProperties()", dbSchemaRelPropertiesColumns),
			Description:        localization.CypherProcedureMetadata("db.schema.relProperties").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.schema.relProperties"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbSchemaRelProperties(ctx)
			}, cypherfn.CanonicalReads)

		registerProcedure(fulltextQueryProcedureSpec("db.index.fulltext.queryNodes", nodeScoreColumns),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryNodes(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(fulltextQueryProcedureSpec("db.index.fulltext.queryRelationships", relationshipScoreColumns),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextQueryRelationships(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.index.fulltext.createNodeIndex", Signature: "db.index.fulltext.createNodeIndex(indexName :: STRING, labels :: LIST<STRING>, properties :: LIST<STRING>)", Description: localization.CypherProcedureMetadata("db.index.fulltext.createNodeIndex").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.index.fulltext.createNodeIndex"), Mode: ProcedureModeWrite, MinArgs: 3, MaxArgs: 4},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateIndex(ctx, args, storage.ConstraintEntityNode)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.index.fulltext.createRelationshipIndex", Signature: "db.index.fulltext.createRelationshipIndex(indexName :: STRING, relationshipTypes :: LIST<STRING>, properties :: LIST<STRING>)", Description: localization.CypherProcedureMetadata("db.index.fulltext.createRelationshipIndex").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.index.fulltext.createRelationshipIndex"), Mode: ProcedureModeWrite, MinArgs: 3, MaxArgs: 4},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextCreateIndex(ctx, args, storage.ConstraintEntityRelationship)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.index.fulltext.drop", Signature: "db.index.fulltext.drop(indexName :: STRING)", Description: localization.CypherProcedureMetadata("db.index.fulltext.drop").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.index.fulltext.drop"), Mode: ProcedureModeWrite, MinArgs: 1, MaxArgs: 1},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextDrop(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(fulltextAnalyzerProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexFulltextListAvailableAnalyzers()
			}, cypherfn.CanonicalReads)

		registerProcedure(vectorQueryProcedureSpec("db.index.vector.queryNodes", nodeScoreColumns),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callVectorQueryArguments(ctx, args, false)
			}, cypherfn.CanonicalReads)
		registerProcedure(vectorQueryProcedureSpec("db.index.vector.queryRelationships", relationshipScoreColumns),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callVectorQueryArguments(ctx, args, true)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.index.vector.embed",
			Signature:          procedureReturnSignature("db.index.vector.embed(text :: STRING)", dbIndexVectorEmbedColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.index.vector.embed"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.index.vector.embed").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorEmbed(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(vectorCreateNodeProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateIndex(ctx, args, storage.ConstraintEntityNode)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.index.vector.createRelationshipIndex", Signature: "db.index.vector.createRelationshipIndex(indexName :: STRING, relationshipType :: STRING, property :: STRING, dimension :: INTEGER, similarityFunction :: STRING)", Description: localization.CypherProcedureMetadata("db.index.vector.createRelationshipIndex").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.index.vector.createRelationshipIndex"), Mode: ProcedureModeWrite, MinArgs: 4, MaxArgs: 5},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorCreateIndex(ctx, args, storage.ConstraintEntityRelationship)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.index.vector.drop", Signature: "db.index.vector.drop(indexName :: STRING)", Description: localization.CypherProcedureMetadata("db.index.vector.drop").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.index.vector.drop"), Mode: ProcedureModeWrite, MinArgs: 1, MaxArgs: 1},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbIndexVectorDrop(ctx, args)
			}, cypherfn.CanonicalReads)

		registerProcedure(vectorSetterProcedureSpec("db.create.setNodeVectorProperty", "node", "NODE"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callSetVectorProperty(ctx, args, false)
			}, cypherfn.CanonicalReads)
		registerProcedure(vectorSetterProcedureSpec("db.create.setRelationshipVectorProperty", "relationship", "RELATIONSHIP"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callSetVectorProperty(ctx, args, true)
			}, cypherfn.CanonicalReads)

		registerProcedure(ProcedureSpec{
			Name:               "dbms.components",
			Signature:          "dbms.components()",
			Description:        localization.CypherProcedureMetadata("dbms.components").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.components"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Returns:            dbmsComponentsColumns,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsComponents()
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "dbms.info",
			Signature:          "dbms.info()",
			Description:        localization.CypherProcedureMetadata("dbms.info").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.info"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Returns:            dbmsInfoColumns,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callInfo(ctx, infoScopeDBMS)
			}, cypherfn.CanonicalReads)
		registerProcedure(dbmsListConfigProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConfigArguments(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "dbms.clientConfig",
			Signature:          procedureReturnSignature("dbms.clientConfig()", configSettingColumns),
			Description:        localization.CypherProcedureMetadata("dbms.clientConfig").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.clientConfig"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsClientConfig()
			}, cypherfn.CanonicalReads)
		registerProcedure(dbmsListConnectionsProcedureSpec(),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsListConnectionsWithContext(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "dbms.procedures",
			Signature:          procedureReturnSignature("dbms.procedures()", dbmsProceduresColumns),
			Description:        localization.CypherProcedureMetadata("dbms.procedures").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.procedures"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsProcedures()
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "dbms.functions",
			Signature:          procedureReturnSignature("dbms.functions()", dbmsFunctionsColumns),
			Description:        localization.CypherProcedureMetadata("dbms.functions").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("dbms.functions"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbmsFunctions()
			}, cypherfn.CanonicalReads)

		registerProcedure(awaitIndexProcedureSpec("db.awaitIndex", true),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNamedIndexManagement(ctx, "db.awaitIndex", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(awaitIndexProcedureSpec("db.awaitIndexes", false),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbAwaitIndexes()
			}, cypherfn.CanonicalReads)
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
				return e.callNamedIndexManagement(ctx, "db.resampleIndex", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.clearQueryCaches",
			Signature:          "db.clearQueryCaches()",
			Description:        localization.CypherProcedureMetadata("db.clearQueryCaches").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.clearQueryCaches"),
			Mode:               ProcedureModeDBMS,
			WorksOnSystem:      true,
			Admin:              true,
			Returns:            dbClearQueryCachesColumns,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbClearQueryCaches()
			}, cypherfn.CanonicalReads)

		registerProcedure(queryStatisticsProcedureSpec("collect"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "collect", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(queryStatisticsProcedureSpec("retrieve"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "retrieve", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "db.stats.retrieveAllAnTheStats", Signature: "db.stats.retrieveAllAnTheStats()", Description: localization.CypherProcedureMetadata("db.stats.retrieveAllAnTheStats").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("db.stats.retrieveAllAnTheStats"), Mode: ProcedureModeDBMS, Admin: true, WorksOnSystem: true},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.retrieveGraphStatistics(ctx, "ALL")
			}, cypherfn.CanonicalReads)
		registerProcedure(queryStatisticsProcedureSpec("clear"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "clear", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(queryStatisticsProcedureSpec("status"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "status", args)
			}, cypherfn.CanonicalReads)
		registerProcedure(queryStatisticsProcedureSpec("stop"),
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callQueryStatistics(ctx, "stop", args)
			}, cypherfn.CanonicalReads)

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
				return e.callTxSetMetadata(ctx, args)
			}, cypherfn.OpaqueReads)

		registerProcedure(ProcedureSpec{
			Name:               "nornicdb.version",
			Signature:          procedureReturnSignature("nornicdb.version()", nornicdbVersionColumns),
			Description:        localization.CypherProcedureMetadata("nornicdb.version").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("nornicdb.version"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbVersion()
			}, cypherfn.CanonicalReads)
		// nornicdb.stats reports all four of the operations it performs: the node
		// count request, the relationship count request, and the two token
		// populations it lists. A replacement of this name registered through the
		// public API stays opaque: a name carries no observation trust.
		registerProcedure(ProcedureSpec{
			Name:               "nornicdb.stats",
			Signature:          procedureReturnSignature("nornicdb.stats()", nornicdbStatsColumns),
			Description:        localization.CypherProcedureMetadata("nornicdb.stats").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("nornicdb.stats"),
			Mode:               ProcedureModeRead,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbStats(ctx)
			}, cypherfn.CanonicalReads)
		// The knowledge-policy metadata procedures read this database's own
		// policy catalogs, point entities and access metadata, and the pending
		// deindex queue; kp.decayInfo reads no database state at all. Each
		// reports its actual requests through the canonical access owner. A
		// replacement of one of these names registered through the exported API
		// stays opaque: a name carries no observation trust.
		registerProcedure(ProcedureSpec{
			Name:               "kp.decayInfo",
			Signature:          procedureReturnSignature("kp.decayInfo()", kpDecayInfoColumns),
			Description:        localization.CypherProcedureMetadata("kp.decayInfo").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.decayInfo"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbDecayInfo()
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.info",
			Signature:          procedureReturnSignature("kp.info()", kpInfoColumns),
			Description:        localization.CypherProcedureMetadata("kp.info").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.info"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyInfo(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.profiles",
			Signature:          procedureReturnSignature("kp.profiles()", kpProfilesColumns),
			Description:        localization.CypherProcedureMetadata("kp.profiles").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.profiles"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyProfiles(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.policies",
			Signature:          procedureReturnSignature("kp.policies()", kpPoliciesColumns),
			Description:        localization.CypherProcedureMetadata("kp.policies").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.policies"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyPolicies(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.resolve",
			Signature:          procedureReturnSignature("kp.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '')", kpResolveColumns),
			Description:        localization.CypherProcedureMetadata("kp.resolve").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.resolve"),
			Mode:               ProcedureModeRead,
			MaxArgs:            3,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyResolve(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.deindexStatus",
			Signature:          procedureReturnSignature("kp.deindexStatus()", kpDeindexStatusColumns),
			Description:        localization.CypherProcedureMetadata("kp.deindexStatus").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.deindexStatus"),
			Mode:               ProcedureModeRead,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callNornicDbKnowledgePolicyDeindexStatus(ctx)
			}, cypherfn.CanonicalReads)
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
		registerProcedure(ProcedureSpec{
			Name:               "db.retrieve",
			Signature:          procedureReturnSignature("db.retrieve(request :: MAP)", retrievalColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.retrieve"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.retrieve").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieve(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.rretrieve",
			Signature:          procedureReturnSignature("db.rretrieve(request :: MAP)", retrievalColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.rretrieve"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.rretrieve").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieve(ctx, args)
			}, cypherfn.CanonicalReads)
		// The suppression procedures report what they actually do: the entity the
		// statement resolved, the one manual-suppression write request they issue
		// (profileKindManualSuppressionWriteRequest) and the search refresh that
		// publishes it. Their registration declares canonical reads; a custom
		// replacement of either name keeps opaque policy.
		registerProcedure(ProcedureSpec{
			Name:               "kp.suppress",
			Signature:          "kp.suppress(entity :: ANY)",
			Description:        localization.CypherProcedureMetadata("kp.suppress").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.suppress"),
			Mode:               ProcedureModeWrite,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callKpSuppress(ctx, "kp.suppress", args, true)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "kp.unsuppress",
			Signature:          "kp.unsuppress(entity :: ANY)",
			Description:        localization.CypherProcedureMetadata("kp.unsuppress").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("kp.unsuppress"),
			Mode:               ProcedureModeWrite,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callKpSuppress(ctx, "kp.unsuppress", args, false)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.rerank",
			Signature:          procedureReturnSignature("db.rerank(request :: MAP)", dbRerankColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.rerank"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.rerank").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRerank(ctx, args)
			}, cypherfn.CanonicalReads)
		// db.retrieveRelationships searches relationship properties, when the
		// database indexes them (#1065).
		registerProcedure(ProcedureSpec{
			Name:               "db.retrieveRelationships",
			Signature:          procedureReturnSignature("db.retrieveRelationships(request :: MAP)", dbRetrieveRelationshipsColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.retrieveRelationships"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.retrieveRelationships").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieveRelationships(ctx, args)
			}, cypherfn.CanonicalReads)
		// db.retrieveGraph ranks nodes and relationships together: rows, or
		// with n / maxResults one terminal page; the signature declares both,
		// as db.rretrieve's does, so YIELD page holds after MATCH / WITH too.
		registerProcedure(ProcedureSpec{
			Name:               "db.retrieveGraph",
			Signature:          procedureReturnSignature("db.retrieveGraph(request :: MAP)", retrievalGraphColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.retrieveGraph"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.retrieveGraph").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRetrieveGraph(ctx, args)
			}, cypherfn.CanonicalReads)
		// db.rretrieveGraph is db.retrieveGraph with the configured reranker
		// ordering the fused pool of both kinds, as db.rretrieve is db.retrieve's.
		registerProcedure(ProcedureSpec{
			Name:               "db.rretrieveGraph",
			Signature:          procedureReturnSignature("db.rretrieveGraph(request :: MAP)", retrievalGraphColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.rretrieveGraph"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.rretrieveGraph").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbRRetrieveGraph(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.infer",
			Signature:          procedureReturnSignature("db.infer(request :: MAP)", dbInferColumns),
			DescriptionMessage: localization.CypherProcedureMetadata("db.infer"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
			WorksOnSystem:      false,
			Description:        localization.CypherProcedureMetadata("db.infer").Fallback,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbInfer(ctx, args)
			}, cypherfn.CanonicalReads)

		// The CDC, txlog and temporal procedures report the feed, WAL entry and
		// label population reads their handlers perform through the statement
		// context, so a PROFILE statement states what each one actually read. A
		// custom replacement registered through the exported API keeps opaque
		// policy: a name carries no observation trust.
		registerProcedure(ProcedureSpec{
			Name:               "db.cdc.current",
			Signature:          procedureReturnSignature("db.cdc.current()", cdcPositionColumns),
			Description:        localization.CypherProcedureMetadata("db.cdc.current").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.cdc.current"),
			Mode:               ProcedureModeRead,
			MinArgs:            0,
			MaxArgs:            0,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcCurrent(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.cdc.earliest",
			Signature:          procedureReturnSignature("db.cdc.earliest()", cdcPositionColumns),
			Description:        localization.CypherProcedureMetadata("db.cdc.earliest").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.cdc.earliest"),
			Mode:               ProcedureModeRead,
			MinArgs:            0,
			MaxArgs:            0,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcEarliest(ctx)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.cdc.query",
			Signature:          procedureReturnSignature("db.cdc.query(from = \"\" :: STRING, selectors = [] :: LIST<MAP>, limit = null :: INTEGER)", dbCdcQueryColumns),
			Description:        localization.CypherProcedureMetadata("db.cdc.query").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.cdc.query"),
			Mode:               ProcedureModeDBMS,
			Admin:              true,
			MinArgs:            0,
			MaxArgs:            3,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbCdcQuery(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.txlog.entries",
			Signature:          procedureReturnSignature("db.txlog.entries(fromSeq = null :: INTEGER, toSeq = null :: INTEGER)", txlogColumns),
			Description:        localization.CypherProcedureMetadata("db.txlog.entries").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.txlog.entries"),
			Mode:               ProcedureModeDBMS,
			Admin:              true,
			MinArgs:            0,
			MaxArgs:            2,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDBTxlog(ctx, args, txlogBySequence)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.txlog.byTxId",
			Signature:          procedureReturnSignature("db.txlog.byTxId(txId :: STRING, limit = null :: INTEGER)", txlogColumns),
			Description:        localization.CypherProcedureMetadata("db.txlog.byTxId").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.txlog.byTxId"),
			Mode:               ProcedureModeDBMS,
			Admin:              true,
			MinArgs:            1,
			MaxArgs:            2,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDBTxlog(ctx, args, txlogByID)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.temporal.assertNoOverlap",
			Signature:          procedureReturnSignature("db.temporal.assertNoOverlap(label :: STRING, keyProp :: STRING, validFromProp :: STRING, validToProp :: STRING, keyValue :: ANY, newValidFrom :: ANY, newValidTo :: ANY, systemTime = null :: ANY, systemSequence = null :: INTEGER)", dbTemporalAssertNoOverlapColumns),
			Description:        localization.CypherProcedureMetadata("db.temporal.assertNoOverlap").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.temporal.assertNoOverlap"),
			Mode:               ProcedureModeRead,
			MinArgs:            7,
			MaxArgs:            9,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAssertNoOverlap(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "db.temporal.asOf",
			Signature:          procedureReturnSignature("db.temporal.asOf(label :: STRING, keyProp :: STRING, keyValue :: ANY, validFromProp :: STRING, validToProp :: STRING, asOf :: ANY, systemTime = null :: ANY, systemSequence = null :: INTEGER)", nodeColumns),
			Description:        localization.CypherProcedureMetadata("db.temporal.asOf").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("db.temporal.asOf"),
			Mode:               ProcedureModeRead,
			MinArgs:            6,
			MaxArgs:            8,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callDbTemporalAsOf(ctx, args)
			}, cypherfn.CanonicalReads)

		// The apoc.path expanders report every read they make - each node
		// argument they resolve and each adjacency request, where the read hands
		// the entries over - so their registrations declare canonical reads
		// instead of an opaque callback. A custom replacement keeps opaque
		// policy: a name carries no observation trust.
		registerProcedure(ProcedureSpec{Name: "apoc.path.subgraphNodes", Signature: procedureReturnSignature("apoc.path.subgraphNodes(startNode :: ANY, config :: MAP)", nodeColumns), Description: "Returns the nodes reachable from the start node(s) under the config's filters", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocPathSubgraphNodes(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.path.subgraphAll", Signature: procedureReturnSignature("apoc.path.subgraphAll(startNode :: ANY, config :: MAP)", graphColumns), Description: "Returns the reachable nodes and the relationships between them", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocPathSubgraphAll(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.path.expand", Signature: procedureReturnSignature("apoc.path.expand(startNode :: ANY, relationshipFilter :: STRING, labelFilter :: STRING, minLevel :: INTEGER, maxLevel :: INTEGER)", pathColumns), Description: "Expands paths from the start node(s)", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 5}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocPathExpand(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.path.expandConfig", Signature: procedureReturnSignature("apoc.path.expandConfig(startNode :: ANY, config :: MAP)", pathColumns), Description: "Expands paths from the start node(s) under a config", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocPathExpandConfig(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.path.spanningTree", Signature: procedureReturnSignature("apoc.path.spanningTree(startNode :: ANY, config :: MAP)", pathColumns), Description: "Returns a path from the start node(s) to every node reached once", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocPathSpanningTree(ctx, args)
		}, cypherfn.CanonicalReads)
		// A custom procedure is one for the whole server (the procedure
		// registry is process-wide), and whoever calls it runs its body, on
		// any database: installing, replacing and dropping one needs the
		// admin entitlement, as CREATE / DROP PROCEDURE does and as APOC 5
		// marks apoc.custom.installProcedure and dropProcedure (@Admin).
		registerProcedure(ProcedureSpec{Name: "apoc.custom.installProcedure", Signature: "apoc.custom.installProcedure(signature :: STRING, statement :: STRING, databaseName :: STRING = 'neo4j', mode :: STRING = 'read', description :: STRING = '')", Description: "Installs a statement as the procedure custom.<name>", Mode: ProcedureModeWrite, MinArgs: 2, MaxArgs: 5, WorksOnSystem: false, Admin: true},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomInstall(ctx, "apoc.custom.installProcedure", args, true)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.custom.declareProcedure", Signature: "apoc.custom.declareProcedure(signature :: STRING, statement :: STRING, mode :: STRING = 'read', description :: STRING = '')", Description: "Installs a statement as the procedure custom.<name> (APOC 4 form)", Mode: ProcedureModeWrite, MinArgs: 2, MaxArgs: 4, WorksOnSystem: false, Admin: true},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomInstall(ctx, "apoc.custom.declareProcedure", args, false)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.custom.dropProcedure", Signature: "apoc.custom.dropProcedure(name :: STRING, databaseName :: STRING = 'neo4j')", Description: "Drops the custom procedure custom.<name>", Mode: ProcedureModeWrite, MinArgs: 1, MaxArgs: 2, WorksOnSystem: false, Admin: true},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCustomDrop(ctx, args)
			}, cypherfn.CanonicalReads)
		for _, name := range []string{"apoc.custom.list", "apoc.custom.show"} {
			registerProcedure(ProcedureSpec{Name: name, Signature: procedureReturnSignature(name+"()", apocCustomListColumns), Description: "Lists the custom procedures", Mode: ProcedureModeRead, MaxArgs: 1},
				func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
					return e.callApocCustomList(ctx)
				}, cypherfn.CanonicalReads)
		}
		registerStatementProcedureLiteral("apoc.cypher.run", procedureReturnSignature("apoc.cypher.run(statement :: STRING, params :: MAP)", valueMapColumns), "Runs dynamic Cypher", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.run", args)
			})
		registerStatementProcedureLiteral("apoc.cypher.doIt", procedureReturnSignature("apoc.cypher.doIt(statement :: STRING, params :: MAP)", valueMapColumns), "Runs dynamic Cypher", ProcedureModeWrite, 2, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.doIt", args)
			})
		registerStatementProcedureLiteral("apoc.cypher.doitall", procedureReturnSignature("apoc.cypher.doitall(statement :: STRING, params :: MAP)", valueMapColumns), "Alias of apoc.cypher.run", ProcedureModeRead, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRun(ctx, "apoc.cypher.doitall", args)
			})
		registerStatementProcedureLiteral("apoc.cypher.runMany", procedureReturnSignature("apoc.cypher.runMany(statements :: STRING, params :: MAP, config :: MAP = {})", apocCypherRunManyColumns), "Runs many Cypher statements", ProcedureModeWrite, 1, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocCypherRunMany(ctx, args)
			})
		registerBatchProcedureLiteral("apoc.periodic.iterate", procedureReturnSignature("apoc.periodic.iterate(iterate :: STRING, action :: STRING, config :: MAP)", periodicIterateColumns), "Runs batch iterate/action jobs", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, "apoc.periodic.iterate", args)
			})
		registerBatchProcedureLiteral("apoc.periodic.commit", procedureReturnSignature("apoc.periodic.commit(statement :: STRING, params :: MAP)", apocPeriodicCommitColumns), "Runs periodic commits", ProcedureModeWrite, 1, 2, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicCommit(ctx, args)
			})
		registerBatchProcedureLiteral("apoc.periodic.rock_n_roll", procedureReturnSignature("apoc.periodic.rock_n_roll(iterate :: STRING, action :: STRING, config :: MAP)", periodicIterateColumns), "Alias of apoc.periodic.iterate", ProcedureModeWrite, 2, 3, false,
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callApocPeriodicIterate(ctx, "apoc.periodic.rock_n_roll", args)
			})

		// gds.version reports fixed constants and reads no graph state, so its
		// registration certifies a complete access total of zero; a replacement of
		// it keeps opaque policy.
		registerProcedure(ProcedureSpec{
			Name:               "gds.version",
			Signature:          procedureReturnSignature("gds.version()", gdsVersionColumns),
			Description:        localization.CypherProcedureMetadata("gds.version").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.version"),
			Mode:               ProcedureModeRead,
			MinArgs:            0,
			MaxArgs:            0,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsVersion()
			}, cypherfn.CanonicalReads)
		// gds.graph.exists reads only this database's projection catalog, as a
		// lookup, and gds.graph.list only that catalog, as a list: each reports
		// the requests it actually made, and a replacement of either retains
		// opaque policy.
		registerProcedure(ProcedureSpec{
			Name:               "gds.graph.exists",
			Signature:          procedureReturnSignature("gds.graph.exists(graphName :: STRING)", gdsGraphExistsColumns),
			Description:        localization.CypherProcedureMetadata("gds.graph.exists").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.graph.exists"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            1,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphExists(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "gds.graph.list",
			Signature:          procedureReturnSignature("gds.graph.list(graphName = __ALL__ :: STRING)", gdsGraphListColumns),
			Description:        localization.CypherProcedureMetadata("gds.graph.list").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.graph.list"),
			Mode:               ProcedureModeRead,
			MinArgs:            0,
			MaxArgs:            1,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsGraphList(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name: "gds.graph.drop", Signature: procedureReturnSignature("gds.graph.drop(graphName :: ANY, failIfMissing = true :: BOOLEAN, dbName =  :: STRING, username =  :: STRING)", gdsGraphDropColumns),
			Description:        localization.CypherProcedureMetadata("gds.graph.drop").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.graph.drop"),
			Mode:               ProcedureModeWrite, MinArgs: 1, MaxArgs: 4,
		}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsGraphDrop(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name: "gds.graph.project", Signature: procedureReturnSignature("gds.graph.project(graphName :: STRING, nodeProjection :: ANY, relationshipProjection :: ANY, configuration = {} :: MAP)", gdsGraphProjectColumns),
			Description:        localization.CypherProcedureMetadata("gds.graph.project").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.graph.project"),
			Mode:               ProcedureModeWrite, MinArgs: 3, MaxArgs: 4,
		}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsGraphProject(ctx, args)
		}, cypherfn.CanonicalReads)
		// gds.fastRP.stream and gds.fastRP.stats read only the projection their
		// graphName names, as one catalog lookup, and then compute the embeddings on
		// the CPU over that retained projection: each reports the one request it
		// actually made, and a replacement of either keeps opaque policy.
		registerProcedure(ProcedureSpec{
			Name:               "gds.fastRP.stream",
			Signature:          procedureReturnSignature("gds.fastRP.stream(graphName :: STRING, configuration = {} :: MAP)", gdsFastRPStreamColumns),
			Description:        localization.CypherProcedureMetadata("gds.fastRP.stream").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.fastRP.stream"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            2,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStream(ctx, args)
			}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{
			Name:               "gds.fastRP.stats",
			Signature:          procedureReturnSignature("gds.fastRP.stats(graphName :: STRING, configuration = {} :: MAP)", gdsFastRPStatsColumns),
			Description:        localization.CypherProcedureMetadata("gds.fastRP.stats").Fallback,
			DescriptionMessage: localization.CypherProcedureMetadata("gds.fastRP.stats"),
			Mode:               ProcedureModeRead,
			MinArgs:            1,
			MaxArgs:            2,
		},
			func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
				return e.callGdsFastRPStats(ctx, args)
			}, cypherfn.CanonicalReads)
		// The link-prediction streams read the graph they score - the named
		// projection's entry or every stored node and its adjacency - and the
		// hybrid one also reads each scored node's full image. Every one of those
		// reads is reported by the reader that performs it, so their registrations
		// declare canonical reads instead of an opaque callback.
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.adamicAdar.stream", Signature: procedureReturnSignature("gds.linkPrediction.adamicAdar.stream(graphNameOrConfig :: ANY, config :: MAP)", linkPredictionColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.adamicAdar.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.adamicAdar.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionAdamicAdar(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.commonNeighbors.stream", Signature: procedureReturnSignature("gds.linkPrediction.commonNeighbors.stream(graphNameOrConfig :: ANY, config :: MAP)", linkPredictionColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.commonNeighbors.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.commonNeighbors.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionCommonNeighbors(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.resourceAllocation.stream", Signature: procedureReturnSignature("gds.linkPrediction.resourceAllocation.stream(graphNameOrConfig :: ANY, config :: MAP)", linkPredictionColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.resourceAllocation.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.resourceAllocation.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionResourceAllocation(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.preferentialAttachment.stream", Signature: procedureReturnSignature("gds.linkPrediction.preferentialAttachment.stream(graphNameOrConfig :: ANY, config :: MAP)", linkPredictionColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.preferentialAttachment.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.preferentialAttachment.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionPreferentialAttachment(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.jaccard.stream", Signature: procedureReturnSignature("gds.linkPrediction.jaccard.stream(graphNameOrConfig :: ANY, config :: MAP)", linkPredictionColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.jaccard.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.jaccard.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionJaccard(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "gds.linkPrediction.predict.stream", Signature: procedureReturnSignature("gds.linkPrediction.predict.stream(graphNameOrConfig :: ANY, config :: MAP)", gdsLinkPredictionPredictStreamColumns), Description: localization.CypherProcedureMetadata("gds.linkPrediction.predict.stream").Fallback, DescriptionMessage: localization.CypherProcedureMetadata("gds.linkPrediction.predict.stream"), Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callGdsLinkPredictionPredict(ctx, args)
		}, cypherfn.CanonicalReads)

		// The path algorithms report every read they make through one owner: each
		// adjacency request where the read hands it over, and each node they read
		// (a start/end argument's point read, an A* heuristic's node). A read that
		// fails is the statement's failure, so no partial graph is presented as the
		// graph. Their registrations therefore declare canonical reads instead of
		// an opaque callback.
		registerProcedure(ProcedureSpec{Name: "apoc.algo.dijkstra", Signature: procedureReturnSignature("apoc.algo.dijkstra(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, defaultWeight :: FLOAT)", pathWeightColumns), Description: "Runs weighted shortest path", Mode: ProcedureModeRead, MinArgs: 4, MaxArgs: 5}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoDijkstra(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.aStar", Signature: procedureReturnSignature("apoc.algo.aStar(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, weightPropertyName :: STRING, latPropertyName :: STRING, lonPropertyName :: STRING)", pathWeightColumns), Description: "Runs A* shortest path", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: -1}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoAStar(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.allSimplePaths", Signature: procedureReturnSignature("apoc.algo.allSimplePaths(startNode :: NODE, endNode :: NODE, relTypesAndDirections :: STRING, maxNodes :: INTEGER)", pathColumns), Description: "Enumerates all simple paths", Mode: ProcedureModeRead, MinArgs: 4, MaxArgs: 4}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoAllSimplePaths(ctx, args)
		}, cypherfn.CanonicalReads)
		// These six whole-graph algorithms report every read they make - the node
		// population they run over and each adjacency request, before the call's
		// type selection narrows it - so their registrations declare canonical
		// reads instead of an opaque callback.
		registerProcedure(ProcedureSpec{Name: "apoc.algo.pageRank", Signature: procedureReturnSignature("apoc.algo.pageRank(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", nodeScoreColumns), Description: "Runs PageRank", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoPageRank(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.betweenness", Signature: procedureReturnSignature("apoc.algo.betweenness(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", nodeScoreColumns), Description: "Runs betweenness centrality", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoBetweenness(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.closeness", Signature: procedureReturnSignature("apoc.algo.closeness(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", nodeScoreColumns), Description: "Runs closeness centrality", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoCloseness(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.louvain", Signature: procedureReturnSignature("apoc.algo.louvain(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", nodeCommunityColumns), Description: "Runs Louvain community detection", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoLouvain(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.labelPropagation", Signature: procedureReturnSignature("apoc.algo.labelPropagation(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", nodeCommunityColumns), Description: "Runs label propagation", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoLabelPropagation(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.algo.wcc", Signature: procedureReturnSignature("apoc.algo.wcc(nodes :: ANY, relationshipTypesOrConfig :: ANY, config :: MAP)", apocAlgoWccColumns), Description: "Runs weakly connected components", Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocAlgoWCC(ctx, args)
		}, cypherfn.CanonicalReads)
		// The neighbours listings report each adjacency request they make, where the
		// read hands the entries over, and each node they read back through the
		// point-read owner, so their registrations declare canonical reads.
		registerProcedure(ProcedureSpec{Name: "apoc.neighbors.tohop", Signature: procedureReturnSignature("apoc.neighbors.tohop(node :: NODE, relTypes :: STRING = '', distance :: INTEGER = 1)", nodeColumns), Description: "Collects neighbors to N hops", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocNeighborsTohop(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.neighbors.byhop", Signature: procedureReturnSignature("apoc.neighbors.byhop(node :: NODE, relTypes :: STRING = '', distance :: INTEGER = 1)", apocNeighborsByhopColumns), Description: "Collects neighbors grouped by hop distance", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocNeighborsByhop(ctx, args)
		}, cypherfn.CanonicalReads)
		// I/O providers report actual custom invocations at the capability
		// boundary; graph exports report the populations they acquire. Query
		// exports retain statement execution and its canonical inner readers.
		registerProcedure(ProcedureSpec{Name: "apoc.load.json", Signature: procedureReturnSignature("apoc.load.json(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {})", valueMapColumns), Description: "Loads JSON", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 3}, jsonLoadProcedure("apoc.load.json"), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.load.jsonArray", Signature: procedureReturnSignature("apoc.load.jsonArray(urlOrKeyOrBinary :: STRING, path :: STRING = '', config :: MAP = {})", valueMapColumns), Description: "Loads JSON array", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 3}, jsonLoadProcedure("apoc.load.jsonArray"), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.load.csv", Signature: procedureReturnSignature("apoc.load.csv(urlOrBinary :: STRING, config :: MAP = {}, nullValues :: LIST<STRING> = [])", apocLoadCsvColumns), Description: "Loads CSV", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 3}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocLoadCsv(ctx, args)
		}, cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.export.json.all", Signature: procedureReturnSignature("apoc.export.json.all(file :: STRING, config :: MAP = {})", apocExportColumns), Description: "Exports graph to JSON", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, exportProcedure(exportJSONGraphDeclaration), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.export.json.query", Signature: procedureReturnSignature("apoc.export.json.query(query :: STRING, file :: STRING, config :: MAP = {})", apocExportColumns), Description: "Exports query result to JSON", Mode: ProcedureModeRead, MinArgs: 2, MaxArgs: 3, ExecutesStatements: true}, exportProcedure(exportJSONQueryDeclaration), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.export.csv.all", Signature: procedureReturnSignature("apoc.export.csv.all(file :: STRING, config :: MAP = {})", apocExportColumns), Description: "Exports graph to CSV", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, exportProcedure(exportCSVGraphDeclaration), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.export.csv.query", Signature: procedureReturnSignature("apoc.export.csv.query(query :: STRING, file :: STRING, config :: MAP = {})", apocExportColumns), Description: "Exports query result to CSV", Mode: ProcedureModeRead, MinArgs: 2, MaxArgs: 3, ExecutesStatements: true}, exportProcedure(exportCSVQueryDeclaration), cypherfn.CanonicalReads)
		registerProcedure(ProcedureSpec{Name: "apoc.import.json", Signature: procedureReturnSignature("apoc.import.json(url :: STRING, config :: MAP = {})", apocImportJsonColumns), Description: "Imports JSON", Mode: ProcedureModeWrite, MinArgs: 1, MaxArgs: 2}, func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			return e.callApocImportJson(ctx, args)
		}, cypherfn.CanonicalReads)
	})
}

func queryStatisticsProcedureSpec(action string) ProcedureSpec {
	name := "db.stats." + action
	description := localization.CypherProcedureMetadata(name)
	spec := ProcedureSpec{Name: name, Description: description.Fallback, DescriptionMessage: description, Mode: ProcedureModeRead, WorksOnSystem: true, Admin: true}
	if action == "status" {
		spec.Signature = name + "()"
		spec.Returns = queryStatisticsColumns(action)
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
		spec.Signature = name + "(" + arguments + ")"
		spec.Params[0].Description = "A section of stats to retrieve: ('GRAPH COUNTS', 'TOKENS', 'QUERIES', 'META')."
		spec.Returns = queryStatisticsColumns(action)
		return spec
	}
	spec.Signature = name + "(" + arguments + ")"
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
	spec.Returns = describeColumns(queryStatisticsColumns(action), sectionDescription, successDescription, "Details about the outcome of the procedure.")
	return spec
}

func dbmsListConnectionsProcedureSpec() ProcedureSpec {
	name := "dbms.listConnections"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "()",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeDBMS,
		WorksOnSystem:      true,
		Returns:            dbmsListConnectionsColumns,
	}
}

func dbmsListConfigProcedureSpec() ProcedureSpec {
	name := "dbms.listConfig"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(searchString =  :: STRING)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeDBMS,
		WorksOnSystem:      true,
		Admin:              true,
		Params:             []ProcedureParam{{Name: "searchString", Type: "STRING", Optional: true, Default: "DefaultParameterValue{value=, type=STRING}", Description: "A string that filters on the name of config settings."}},
		Returns: describeColumns(configSettingColumns,
			"The name of the setting.",
			"The description of the setting.",
			"The set value of the setting.",
			"If the setting can be set dynamically or not.",
			"The default value of the setting.",
			"The value of the setting when the database started.",
			"Whether or not the setting was explicitly set.",
			"A description of the valid values."),
		MaxArgs: 1,
	}
}

func schemaVisualizationProcedureSpec() ProcedureSpec {
	name := "db.schema.visualization"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "()",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns: describeColumns(graphColumns,
			"A list of virtual nodes representing each label in the database.",
			"A list of virtual relationships representing all combinations between start and end nodes in the database."),
	}
}

func fulltextAnalyzerProcedureSpec() ProcedureSpec {
	name := "db.index.fulltext.listAvailableAnalyzers"
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "()",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns:            dbIndexFulltextListAvailableAnalyzersColumns,
	}
}

func fulltextQueryProcedureSpec(name string, columns []ProcedureColumn) ProcedureSpec {
	entityName := columns[0].Name
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(indexName :: STRING, queryString :: STRING, options = {} :: MAP)",
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
		Returns: describeColumns(columns,
			"A "+entityName+" which contains a property similar to the query string.",
			"The score measuring how similar the "+entityName+" property is to the query string."),
	}
}

func tokenListingProcedureSpec(name string, columns []ProcedureColumn, columnDescription string) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "()",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
		Returns:            describeColumns(columns, columnDescription),
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

func vectorQueryProcedureSpec(name string, columns []ProcedureColumn) ProcedureSpec {
	entityName := columns[0].Name
	description := localization.CypherProcedureMetadata(name)
	return ProcedureSpec{
		Name:               name,
		Signature:          name + "(indexName :: STRING, numberOfNearestNeighbours :: INTEGER, query :: ANY)",
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		Params: []ProcedureParam{
			{Name: "indexName", Type: "STRING", Description: "The name of the vector index."},
			{Name: "numberOfNearestNeighbours", Type: "INTEGER", Description: "The size of the vector neighbourhood."},
			{Name: "query", Type: "ANY", Description: "The object to find approximate matches for."},
		},
		Returns: describeColumns(columns,
			"A "+entityName+" which contains a vector property similar to the query object.",
			"The score measuring how similar the "+entityName+" property is to the query object."),
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

// registerDeprecatedProcedureAlias registers alias as a deprecated name of the
// built-in procedure name: the same handler, arguments and results, with
// SHOW PROCEDURES reporting it deprecated by name. The alias keeps the aliased
// registration's read-observation policy, because it is the same handler
// reading the same database access.
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
	_ = globalProcedureRegistry.registerBuiltIn(spec, procedure.Handler, procedure.reads)
}

// registerStatementProcedureLiteral declares a procedure whose evaluated
// argument contains executable statement text, including WRITE procedures.
func registerStatementProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name: name, Signature: signature, Description: description, Mode: mode,
		WorksOnSystem: worksOnSystem, MinArgs: minArgs, MaxArgs: maxArgs,
		ExecutesStatements: true,
	}, handler, cypherfn.CanonicalReads)
}

// registerProcedure is the one owner of a built-in registration: it completes
// the spec's shared declarations and registers the handler with the
// read-observation policy this registration declares for that handler's
// database access. A registration opts in only where its handler was reviewed;
// every other registration states opaque reads.
func registerProcedure(spec ProcedureSpec, handler ProcedureHandler, reads cypherfn.ReadObservation) {
	if legacyProcedureColumns(spec.Name) != nil {
		name := spec.Name
		spec.ResultSchemas = func() [][]string { return [][]string{legacyProcedureColumns(name)} }
	}
	if dependencies, declared := builtinProcedureResultCache[spec.Name]; declared {
		spec.ResultCache = dependencies
	}
	if len(spec.Returns) > 0 && !strings.Contains(spec.Signature, " :: (") {
		// A registration that describes its returns declares them once: the
		// signature's YIELD list is rendered from them.
		spec.Signature = procedureReturnSignature(spec.Signature, spec.Returns)
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
	switch spec.Name {
	case "db.retrieve", "db.rretrieve":
		spec.ResultSchemas = func() [][]string { return retrievalResultSchemas(retrievalColumns) }
	case "db.retrieveGraph", "db.rretrieveGraph":
		spec.ResultSchemas = func() [][]string { return retrievalResultSchemas(retrievalGraphColumns) }
	}
	// Every handler this bundle registers was reviewed: each reads a node
	// argument's identity, labels and Cypher properties, and none inspects or
	// serializes the embedding vector payloads a node object holds outside
	// those properties (storage.Node.ChunkEmbeddings and
	// storage.Node.NamedEmbeddings). The bundle therefore declares the
	// metadata image for its node arguments. A procedure that delegates statement
	// text or owns transaction batches keeps the native full image: that text can hand the node to a
	// native consumer this review cannot see.
	if !spec.ExecutesStatements && !spec.OwnsTransactions {
		spec.NodeImages = MetadataNodeImages
	}
	_ = globalProcedureRegistry.registerBuiltIn(spec, handler, reads)
}

func registerBatchProcedureLiteral(name, signature, description string, mode ProcedureMode, minArgs, maxArgs int, worksOnSystem bool, handler ProcedureHandler) {
	registerProcedure(ProcedureSpec{
		Name: name, Signature: signature, Description: description, Mode: mode,
		WorksOnSystem: worksOnSystem, MinArgs: minArgs, MaxArgs: maxArgs, OwnsTransactions: true,
	}, handler, cypherfn.CanonicalReads)
}

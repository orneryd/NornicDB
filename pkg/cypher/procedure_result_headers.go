package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30.

// The result header of every built-in procedure is declared once, here, as
// the ordered columns it yields. Each registration that yields a header
// renders its signature from (or describes) that value, and each handler
// that emits it reads the same value, so a header change reaches every
// registration and producer together. Descriptions are per-registration
// metadata (describeColumns), not part of the header.
var (
	dbIndexesColumns                             = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "type", Type: "STRING"}, {Name: "labelsOrTypes", Type: "LIST<STRING>"}, {Name: "properties", Type: "LIST<STRING>"}, {Name: "state", Type: "STRING"}}
	dbIndexStatsColumns                          = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "type", Type: "STRING"}, {Name: "label", Type: "STRING"}, {Name: "property", Type: "STRING"}, {Name: "totalEntries", Type: "INTEGER"}, {Name: "uniqueValues", Type: "INTEGER"}, {Name: "selectivity", Type: "FLOAT"}}
	dbConstraintsColumns                         = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "type", Type: "STRING"}, {Name: "labelsOrTypes", Type: "LIST<STRING>"}, {Name: "properties", Type: "LIST<STRING>"}, {Name: "propertyType", Type: "STRING"}}
	dbSchemaNodePropertiesColumns                = []ProcedureColumn{{Name: "nodeLabel", Type: "STRING"}, {Name: "propertyName", Type: "STRING"}, {Name: "propertyType", Type: "STRING"}}
	dbSchemaRelPropertiesColumns                 = []ProcedureColumn{{Name: "relType", Type: "STRING"}, {Name: "propertyName", Type: "STRING"}, {Name: "propertyType", Type: "STRING"}}
	dbIndexVectorEmbedColumns                    = []ProcedureColumn{{Name: "embedding", Type: "LIST<FLOAT>"}}
	configSettingColumns                         = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "description", Type: "STRING"}, {Name: "value", Type: "STRING"}, {Name: "dynamic", Type: "BOOLEAN"}, {Name: "defaultValue", Type: "STRING"}, {Name: "startupValue", Type: "STRING"}, {Name: "explicitlySet", Type: "BOOLEAN"}, {Name: "validValues", Type: "STRING"}}
	dbmsProceduresColumns                        = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "description", Type: "STRING"}, {Name: "mode", Type: "STRING"}, {Name: "signature", Type: "STRING"}}
	dbmsFunctionsColumns                         = []ProcedureColumn{{Name: "name", Type: "STRING"}, {Name: "description", Type: "STRING"}, {Name: "category", Type: "STRING"}}
	nornicdbVersionColumns                       = []ProcedureColumn{{Name: "version", Type: "STRING"}, {Name: "build", Type: "STRING"}, {Name: "edition", Type: "STRING"}}
	nornicdbStatsColumns                         = []ProcedureColumn{{Name: "nodes", Type: "INTEGER"}, {Name: "relationships", Type: "INTEGER"}, {Name: "labels", Type: "INTEGER"}, {Name: "relationshipTypes", Type: "INTEGER"}}
	kpDecayInfoColumns                           = []ProcedureColumn{{Name: "enabled", Type: "BOOLEAN"}, {Name: "system", Type: "STRING"}, {Name: "configuredVia", Type: "STRING"}}
	kpInfoColumns                                = []ProcedureColumn{{Name: "enabled", Type: "BOOLEAN"}, {Name: "system", Type: "STRING"}, {Name: "decayProfiles", Type: "INTEGER"}, {Name: "decayBindings", Type: "INTEGER"}, {Name: "promotionProfiles", Type: "INTEGER"}, {Name: "promotionPolicies", Type: "INTEGER"}, {Name: "configuredVia", Type: "STRING"}}
	kpProfilesColumns                            = []ProcedureColumn{{Name: "kind", Type: "STRING"}, {Name: "Name", Type: "STRING"}, {Name: "HalfLifeSeconds", Type: "INTEGER"}, {Name: "VisibilityThreshold", Type: "FLOAT"}, {Name: "ScoreFloor", Type: "FLOAT"}, {Name: "Function", Type: "STRING"}, {Name: "Scope", Type: "STRING"}, {Name: "DecayEnabled", Type: "BOOLEAN"}, {Name: "ScoreFrom", Type: "STRING"}, {Name: "ScoreFromProperty", Type: "STRING"}, {Name: "Enabled", Type: "BOOLEAN"}, {Name: "TargetLabels", Type: "LIST<STRING>"}, {Name: "TargetEdgeType", Type: "STRING"}, {Name: "IsWildcard", Type: "BOOLEAN"}, {Name: "IsEdge", Type: "BOOLEAN"}, {Name: "ProfileRef", Type: "STRING"}, {Name: "NoDecay", Type: "BOOLEAN"}, {Name: "Order", Type: "INTEGER"}, {Name: "Apply", Type: "STRING"}}
	kpPoliciesColumns                            = []ProcedureColumn{{Name: "kind", Type: "STRING"}, {Name: "Name", Type: "STRING"}, {Name: "Scope", Type: "STRING"}, {Name: "Multiplier", Type: "FLOAT"}, {Name: "ScoreFloor", Type: "FLOAT"}, {Name: "ScoreCap", Type: "FLOAT"}, {Name: "Enabled", Type: "BOOLEAN"}, {Name: "TargetLabels", Type: "LIST<STRING>"}, {Name: "TargetEdgeType", Type: "STRING"}, {Name: "IsWildcard", Type: "BOOLEAN"}, {Name: "IsEdge", Type: "BOOLEAN"}, {Name: "Apply", Type: "STRING"}}
	kpResolveColumns                             = []ProcedureColumn{{Name: "TargetID", Type: "STRING"}, {Name: "TargetScope", Type: "STRING"}, {Name: "ResolvedDecayProfileID", Type: "STRING"}, {Name: "ResolvedScoreFrom", Type: "STRING"}, {Name: "ResolutionSourceChain", Type: "LIST<STRING>"}, {Name: "AppliedDecayProfileNames", Type: "LIST<STRING>"}, {Name: "AppliedPromotionPolicyName", Type: "STRING"}, {Name: "AppliedPromotionProfileName", Type: "STRING"}, {Name: "EffectiveRate", Type: "FLOAT"}, {Name: "EffectiveThreshold", Type: "FLOAT"}, {Name: "EffectiveMultiplier", Type: "FLOAT"}, {Name: "BaseScore", Type: "FLOAT"}, {Name: "FinalScore", Type: "FLOAT"}, {Name: "NoDecay", Type: "BOOLEAN"}, {Name: "SuppressionEligible", Type: "BOOLEAN"}, {Name: "Explanation", Type: "STRING"}}
	kpDeindexStatusColumns                       = []ProcedureColumn{{Name: "pending_count", Type: "INTEGER"}, {Name: "supported", Type: "BOOLEAN"}, {Name: "message", Type: "STRING"}, {Name: "workItemId", Type: "STRING"}, {Name: "targetId", Type: "STRING"}, {Name: "targetScope", Type: "STRING"}, {Name: "enqueuedAt", Type: "INTEGER"}, {Name: "status", Type: "STRING"}}
	retrievalColumns                             = []ProcedureColumn{{Name: "node", Type: "NODE"}, {Name: "score", Type: "FLOAT"}, {Name: "rrf_score", Type: "FLOAT"}, {Name: "vector_rank", Type: "INTEGER"}, {Name: "bm25_rank", Type: "INTEGER"}, {Name: "search_method", Type: "STRING"}, {Name: "fallback_triggered", Type: "BOOLEAN"}, {Name: "fallback_reason", Type: "STRING"}, {Name: "query_chunks_searched", Type: "INTEGER"}, {Name: "query_chunks_total", Type: "INTEGER"}, {Name: "matches", Type: "LIST<MAP>"}, {Name: "page", Type: "MAP"}}
	dbRerankColumns                              = []ProcedureColumn{{Name: "id", Type: "STRING"}, {Name: "content", Type: "STRING"}, {Name: "original_rank", Type: "INTEGER"}, {Name: "new_rank", Type: "INTEGER"}, {Name: "bi_score", Type: "FLOAT"}, {Name: "cross_score", Type: "FLOAT"}, {Name: "final_score", Type: "FLOAT"}}
	dbRetrieveRelationshipsColumns               = []ProcedureColumn{{Name: "relationship", Type: "RELATIONSHIP"}, {Name: "start", Type: "NODE"}, {Name: "end", Type: "NODE"}, {Name: "score", Type: "FLOAT"}, {Name: "rrf_score", Type: "FLOAT"}, {Name: "vector_rank", Type: "INTEGER"}, {Name: "bm25_rank", Type: "INTEGER"}, {Name: "search_method", Type: "STRING"}, {Name: "fallback_triggered", Type: "BOOLEAN"}, {Name: "fallback_reason", Type: "STRING"}}
	retrievalGraphColumns                        = []ProcedureColumn{{Name: "kind", Type: "STRING"}, {Name: "node", Type: "NODE"}, {Name: "relationship", Type: "RELATIONSHIP"}, {Name: "start", Type: "NODE"}, {Name: "end", Type: "NODE"}, {Name: "score", Type: "FLOAT"}, {Name: "rrf_score", Type: "FLOAT"}, {Name: "vector_rank", Type: "INTEGER"}, {Name: "bm25_rank", Type: "INTEGER"}, {Name: "search_method", Type: "STRING"}, {Name: "fallback_triggered", Type: "BOOLEAN"}, {Name: "fallback_reason", Type: "STRING"}, {Name: "query_chunks_searched", Type: "INTEGER"}, {Name: "query_chunks_total", Type: "INTEGER"}, {Name: "matches", Type: "LIST<MAP>"}, {Name: "page", Type: "MAP"}}
	dbInferColumns                               = []ProcedureColumn{{Name: "text", Type: "STRING"}, {Name: "structured", Type: "ANY"}, {Name: "model", Type: "STRING"}, {Name: "usage", Type: "MAP"}, {Name: "latencyMs", Type: "INTEGER"}, {Name: "finishReason", Type: "STRING"}}
	cdcPositionColumns                           = []ProcedureColumn{{Name: "id", Type: "STRING"}}
	txlogColumns                                 = []ProcedureColumn{{Name: "txId", Type: "STRING"}, {Name: "db", Type: "STRING"}, {Name: "kind", Type: "STRING"}, {Name: "seq", Type: "INTEGER"}, {Name: "timestamp", Type: "STRING"}, {Name: "payload", Type: "STRING"}}
	dbTemporalAssertNoOverlapColumns             = []ProcedureColumn{{Name: "ok", Type: "BOOLEAN"}}
	nodeColumns                                  = []ProcedureColumn{{Name: "node", Type: "NODE"}}
	graphColumns                                 = []ProcedureColumn{{Name: "nodes", Type: "LIST<NODE>"}, {Name: "relationships", Type: "LIST<RELATIONSHIP>"}}
	pathColumns                                  = []ProcedureColumn{{Name: "path", Type: "PATH"}}
	valueMapColumns                              = []ProcedureColumn{{Name: "value", Type: "MAP"}}
	apocCypherRunManyColumns                     = []ProcedureColumn{{Name: "row", Type: "INTEGER"}, {Name: "result", Type: "MAP"}}
	periodicIterateColumns                       = []ProcedureColumn{{Name: "batches", Type: "INTEGER"}, {Name: "total", Type: "INTEGER"}, {Name: "timeTaken", Type: "INTEGER"}, {Name: "committedOperations", Type: "INTEGER"}, {Name: "failedOperations", Type: "INTEGER"}, {Name: "failedBatches", Type: "INTEGER"}, {Name: "retries", Type: "INTEGER"}, {Name: "errorMessages", Type: "MAP"}, {Name: "batch", Type: "MAP"}, {Name: "operations", Type: "MAP"}, {Name: "wasTerminated", Type: "BOOLEAN"}, {Name: "failedParams", Type: "MAP"}, {Name: "updateStatistics", Type: "MAP"}}
	apocPeriodicCommitColumns                    = []ProcedureColumn{{Name: "updates", Type: "INTEGER"}, {Name: "executions", Type: "INTEGER"}, {Name: "runtime", Type: "INTEGER"}, {Name: "batches", Type: "INTEGER"}}
	gdsVersionColumns                            = []ProcedureColumn{{Name: "gdsVersion", Type: "STRING"}}
	gdsGraphExistsColumns                        = []ProcedureColumn{{Name: "graphName", Type: "STRING"}, {Name: "exists", Type: "BOOLEAN"}}
	gdsGraphListColumns                          = append([]ProcedureColumn{{Name: "degreeDistribution", Type: "MAP"}}, gdsGraphDropColumns...)
	gdsGraphDropColumns                          = []ProcedureColumn{{Name: "graphName", Type: "STRING"}, {Name: "database", Type: "STRING"}, {Name: "databaseLocation", Type: "STRING"}, {Name: "memoryUsage", Type: "STRING"}, {Name: "sizeInBytes", Type: "INTEGER"}, {Name: "nodeCount", Type: "INTEGER"}, {Name: "relationshipCount", Type: "INTEGER"}, {Name: "configuration", Type: "MAP"}, {Name: "density", Type: "FLOAT"}, {Name: "creationTime", Type: "ZONED DATETIME"}, {Name: "modificationTime", Type: "ZONED DATETIME"}, {Name: "schema", Type: "MAP"}, {Name: "schemaWithOrientation", Type: "MAP"}}
	gdsGraphProjectColumns                       = []ProcedureColumn{{Name: "nodeProjection", Type: "MAP"}, {Name: "relationshipProjection", Type: "MAP"}, {Name: "graphName", Type: "STRING"}, {Name: "nodeCount", Type: "INTEGER"}, {Name: "relationshipCount", Type: "INTEGER"}, {Name: "projectMillis", Type: "INTEGER"}}
	gdsFastRPStreamColumns                       = []ProcedureColumn{{Name: "nodeId", Type: "STRING"}, {Name: "embedding", Type: "LIST<FLOAT>"}}
	gdsFastRPStatsColumns                        = []ProcedureColumn{{Name: "nodeCount", Type: "INTEGER"}, {Name: "preProcessingMillis", Type: "INTEGER"}, {Name: "computeMillis", Type: "INTEGER"}, {Name: "configuration", Type: "MAP"}}
	linkPredictionColumns                        = []ProcedureColumn{{Name: "node1", Type: "STRING"}, {Name: "node2", Type: "STRING"}, {Name: "score", Type: "FLOAT"}}
	gdsLinkPredictionPredictStreamColumns        = []ProcedureColumn{{Name: "node1", Type: "STRING"}, {Name: "node2", Type: "STRING"}, {Name: "score", Type: "FLOAT"}, {Name: "topology_score", Type: "FLOAT"}, {Name: "semantic_score", Type: "FLOAT"}, {Name: "reason", Type: "STRING"}}
	pathWeightColumns                            = []ProcedureColumn{{Name: "path", Type: "PATH"}, {Name: "weight", Type: "FLOAT"}}
	nodeScoreColumns                             = []ProcedureColumn{{Name: "node", Type: "NODE"}, {Name: "score", Type: "FLOAT"}}
	nodeCommunityColumns                         = []ProcedureColumn{{Name: "node", Type: "NODE"}, {Name: "community", Type: "INTEGER"}}
	apocAlgoWccColumns                           = []ProcedureColumn{{Name: "node", Type: "NODE"}, {Name: "componentId", Type: "INTEGER"}}
	apocNeighborsByhopColumns                    = []ProcedureColumn{{Name: "nodes", Type: "LIST<NODE>"}}
	apocLoadCsvColumns                           = []ProcedureColumn{{Name: "lineNo", Type: "INTEGER"}, {Name: "list", Type: "LIST<STRING>"}, {Name: "map", Type: "MAP"}}
	apocExportColumns                            = []ProcedureColumn{{Name: "file", Type: "STRING"}, {Name: "nodes", Type: "INTEGER"}, {Name: "relationships", Type: "INTEGER"}, {Name: "properties", Type: "INTEGER"}, {Name: "time", Type: "INTEGER"}, {Name: "rows", Type: "INTEGER"}, {Name: "batchSize", Type: "INTEGER"}, {Name: "batches", Type: "INTEGER"}, {Name: "done", Type: "BOOLEAN"}, {Name: "data", Type: "STRING"}}
	apocImportJsonColumns                        = []ProcedureColumn{{Name: "file", Type: "STRING"}, {Name: "source", Type: "STRING"}, {Name: "format", Type: "STRING"}, {Name: "nodes", Type: "INTEGER"}, {Name: "relationships", Type: "INTEGER"}, {Name: "properties", Type: "INTEGER"}, {Name: "time", Type: "INTEGER"}, {Name: "rows", Type: "INTEGER"}, {Name: "batchSize", Type: "INTEGER"}, {Name: "batches", Type: "INTEGER"}, {Name: "done", Type: "BOOLEAN"}, {Name: "data", Type: "STRING"}}
	dbCdcQueryColumns                            = []ProcedureColumn{{Name: "id", Type: "STRING"}, {Name: "txId", Type: "INTEGER"}, {Name: "seq", Type: "INTEGER"}, {Name: "metadata", Type: "MAP"}, {Name: "event", Type: "MAP"}}
	apocCustomListColumns                        = []ProcedureColumn{{Name: "type", Type: "STRING"}, {Name: "name", Type: "STRING"}, {Name: "description", Type: "STRING"}, {Name: "mode", Type: "STRING"}, {Name: "statement", Type: "STRING"}, {Name: "inputs", Type: "LIST<ANY>"}, {Name: "outputs", Type: "LIST<ANY>"}, {Name: "forceSingle", Type: "BOOLEAN"}}
	dbIndexFulltextListAvailableAnalyzersColumns = []ProcedureColumn{
		{Name: "analyzer", Type: "STRING", Description: "The name of the analyzer."},
		{Name: "description", Type: "STRING", Description: "The  description of the analyzer."},
		{Name: "stopwords", Type: "LIST<STRING>", Description: "The stopwords used by the analyzer to tokenize strings."},
		{Name: "kind", Type: "STRING", Description: "The native analyzer implementation kind."},
		{Name: "version", Type: "STRING", Description: "The registered stemmer plugin version."},
		{Name: "digest", Type: "STRING", Description: "The abbreviated registered plugin artifact digest."},
		{Name: "dynamicLoad", Type: "BOOLEAN", Description: "Whether this platform supports dynamically loaded stemmer plugins."},
		{Name: "selectedDatabases", Type: "LIST<STRING>", Description: "The database selections reported for this analyzer."},
	}
	dbmsListConnectionsColumns = []ProcedureColumn{
		{Name: "connectionId", Type: "STRING", Description: "The id of the connection."},
		{Name: "connectTime", Type: "STRING", Description: "The time the connection was established, formatted according to the ISO-8601 Standard."},
		{Name: "connector", Type: "STRING", Description: "The protocol of the connector."},
		{Name: "username", Type: "STRING", Description: "The username of the connected user."},
		{Name: "userAgent", Type: "STRING", Description: "The active agent."},
		{Name: "serverAddress", Type: "STRING", Description: "The address of the connected server."},
		{Name: "clientAddress", Type: "STRING", Description: "The address of the connected client."},
	}
	queryStatisticsRetrieveColumns = []ProcedureColumn{{Name: "section", Type: "STRING", Description: "The section retrieved."}, {Name: "data", Type: "MAP", Description: "Data pertaining to the retrieved statistics."}}
	queryStatisticsStatusColumns   = []ProcedureColumn{{Name: "section", Type: "STRING", Description: "String with the message \"QUERIES\"."}, {Name: "status", Type: "STRING", Description: "The status of the QueryCollector: \"idle\" or \"collecting\"."}, {Name: "data", Type: "MAP", Description: "data :: MAP"}}
	dbClearQueryCachesColumns      = []ProcedureColumn{{Name: "value", Type: "STRING", Description: "Information about the number of cleared query caches."}}
	dbmsInfoColumns                = []ProcedureColumn{
		{Name: "id", Type: "STRING", Description: "The id of the DBMS."},
		{Name: "name", Type: "STRING", Description: "The name of the DBMS."},
		{Name: "creationDate", Type: "STRING", Description: "The creation date of the DBMS."},
	}
	dbmsComponentsColumns = []ProcedureColumn{
		{Name: "name", Type: "STRING", Description: "The name of the component."},
		{Name: "versions", Type: "LIST<STRING>", Description: "The installed versions of the component."},
		{Name: "edition", Type: "STRING", Description: "The NornicDB edition of the DBMS."},
	}
	dbPingColumns = []ProcedureColumn{{Name: "success", Type: "BOOLEAN", Description: "Whether or not the connection call to the database has been successful."}}
	dbInfoColumns = []ProcedureColumn{
		{Name: "id", Type: "STRING", Description: "The id of the database."},
		{Name: "name", Type: "STRING", Description: "The name of the database."},
		{Name: "creationDate", Type: "STRING", Description: "The creation date of the database, formatted according to the ISO-8601 Standard."},
		{Name: "nodeCount", Type: "INTEGER", Description: "The number of nodes in the database."},
		{Name: "relationshipCount", Type: "INTEGER", Description: "The number of relationships in the database."},
	}
	dbSchemaRelTypePropertiesColumns = []ProcedureColumn{
		{Name: "relType", Type: "STRING", Description: "A name generated from the type on the relationship."},
		{Name: "propertyName", Type: "STRING", Description: "A property key on a category of relationship."},
		{Name: "propertyTypes", Type: "LIST<STRING>", Description: "All types of a property belonging to a relationship category."},
		{Name: "mandatory", Type: "BOOLEAN", Description: "Whether or not the property is present on all relationships belonging to a relationship category."},
	}
	dbSchemaNodeTypePropertiesColumns = []ProcedureColumn{
		{Name: "nodeType", Type: "STRING", Description: "A name generated from the labels on the node."},
		{Name: "nodeLabels", Type: "LIST<STRING>", Description: "A list containing the labels on a category of node."},
		{Name: "propertyName", Type: "STRING", Description: "A property key on a category of node."},
		{Name: "propertyTypes", Type: "LIST<STRING>", Description: "All types of a property belonging to a node category."},
		{Name: "mandatory", Type: "BOOLEAN", Description: "Whether or not the property is present on all nodes belonging to a node category."},
	}
	queryStatisticsOutcomeColumns = []ProcedureColumn{{Name: "section", Type: "STRING"}, {Name: "success", Type: "BOOLEAN"}, {Name: "message", Type: "STRING"}}
	relationshipScoreColumns      = []ProcedureColumn{{Name: "relationship", Type: "RELATIONSHIP"}, {Name: "score", Type: "FLOAT"}}
	dbLabelsColumns               = []ProcedureColumn{{Name: "label", Type: "STRING"}}
	dbRelationshipTypesColumns    = []ProcedureColumn{{Name: "relationshipType", Type: "STRING"}}
	dbPropertyKeysColumns         = []ProcedureColumn{{Name: "propertyKey", Type: "STRING"}}
)

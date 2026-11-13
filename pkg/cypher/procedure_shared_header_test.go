// Author: declared-list-owners lane (Claude), 2026-10-30.

package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSharedHeaderSiblingsReadOneDeclaration checks, on both parsers, that
// every registration sharing a result header yields exactly the declared
// value: its registered YIELD columns, the static header of a CALL that
// yields them all, and its signature are all derived from that one value,
// so a change to the value reaches every sibling together.
func TestSharedHeaderSiblingsReadOneDeclaration(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	shared := []struct {
		columns    []ProcedureColumn
		procedures []string
	}{
		{txlogColumns, []string{"db.txlog.entries", "db.txlog.byTxId"}},
		{linkPredictionColumns, []string{"gds.linkPrediction.adamicAdar.stream", "gds.linkPrediction.commonNeighbors.stream", "gds.linkPrediction.resourceAllocation.stream", "gds.linkPrediction.preferentialAttachment.stream", "gds.linkPrediction.jaccard.stream"}},
		{retrievalColumns, []string{"db.retrieve", "db.rretrieve"}},
		{retrievalGraphColumns, []string{"db.retrieveGraph", "db.rretrieveGraph"}},
		{periodicIterateColumns, []string{"apoc.periodic.iterate", "apoc.periodic.rock_n_roll"}},
		{pathWeightColumns, []string{"apoc.algo.dijkstra", "apoc.algo.aStar"}},
		{nodeScoreColumns, []string{"apoc.algo.pageRank", "apoc.algo.betweenness", "apoc.algo.closeness", "db.index.fulltext.queryNodes", "db.index.vector.queryNodes"}},
		{relationshipScoreColumns, []string{"db.index.fulltext.queryRelationships", "db.index.vector.queryRelationships"}},
		{nodeCommunityColumns, []string{"apoc.algo.louvain", "apoc.algo.labelPropagation"}},
		{graphColumns, []string{"apoc.path.subgraphAll", "db.schema.visualization"}},
		{configSettingColumns, []string{"dbms.clientConfig", "dbms.listConfig"}},
		{apocExportColumns, []string{"apoc.export.json.all", "apoc.export.json.query", "apoc.export.csv.all", "apoc.export.csv.query"}},
		{queryStatisticsOutcomeColumns, []string{"db.stats.collect", "db.stats.clear", "db.stats.stop"}},
		{cdcPositionColumns, []string{"db.cdc.current", "db.cdc.earliest"}},
		// db.temporal.asOf is not listed here: it serves two approved shapes, the
		// historical query row and the deprecated interval signature's node,
		// declared as a closed set of result schemas. See
		// TestTemporalAsOfDeclaresBothApprovedHeaders.
		{nodeColumns, []string{"apoc.path.subgraphNodes", "apoc.neighbors.tohop"}},
		{pathColumns, []string{"apoc.path.expand", "apoc.path.expandConfig", "apoc.path.spanningTree", "apoc.algo.allSimplePaths"}},
		{valueMapColumns, []string{"apoc.cypher.run", "apoc.cypher.doIt", "apoc.cypher.doitall", "apoc.load.json", "apoc.load.jsonArray"}},
	}
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(previous)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			for _, group := range shared {
				names := columnNames(group.columns)
				yields := procedureReturnSignature("", group.columns)
				for _, name := range group.procedures {
					procedure, found := globalProcedureRegistry.Get(name)
					require.True(t, found, name)
					require.Equal(t, names, columnNames(procedureDeclaredReturns(procedure.Spec)), name)
					for i, column := range procedureDeclaredReturns(procedure.Spec) {
						require.Equal(t, group.columns[i].Type, column.Type, name)
					}
					require.True(t, strings.HasSuffix(procedure.Spec.Signature, yields), name)
					static, err := exec.StatementResultColumns("CALL "+name+" YIELD "+strings.Join(names, ", "), RawSource)
					require.NoError(t, err, name)
					require.Equal(t, names, static, name)
				}
			}
		})
	}
}

// TestTemporalAsOfDeclaresBothApprovedHeaders: db.temporal.asOf returns the
// historical query's row and, for the deprecated interval signature, its
// node. Its registration declares both approved shapes once - the returns
// describe every legal YIELD field and the result schemas name the closed set
// of headers - and each shape's static header is its declaration.
func TestTemporalAsOfDeclaresBothApprovedHeaders(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	procedure, found := globalProcedureRegistry.Get("db.temporal.asOf")
	require.True(t, found)
	require.NotNil(t, procedure.Spec.ResultSchemas)
	require.Equal(t, "db.temporal.asOf(at :: DATETIME, query :: STRING, parameters = {} :: MAP) :: (row :: MAP)", procedure.Spec.Signature)

	declared := procedureDeclaredReturns(procedure.Spec)
	require.Equal(t, []string{"row", "node"}, columnNames(declared))
	require.Equal(t, "MAP", declared[0].Type)
	require.False(t, declared[0].IsDeprecated, "the historical query row is the approved output")
	require.Equal(t, "NODE", declared[1].Type)
	require.True(t, declared[1].IsDeprecated, "the interval signature's node is deprecated")
	require.Equal(t, [][]string{{"row"}, {"node"}}, procedure.Spec.ResultSchemas())

	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(previous)
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			for _, column := range declared {
				static, err := exec.StatementResultColumns("CALL db.temporal.asOf YIELD "+column.Name, RawSource)
				require.NoError(t, err, column.Name)
				require.Equal(t, []string{column.Name}, static, column.Name)
			}
		})
	}
}

// TestDeprecatedAliasKeepsItsHeaderWhenCanonicalIsShadowed: a user procedure
// may replace a canonical knowledge-policy name; the deprecated alias still
// runs the built-in handler and returns the built-in header, on both parsers.
func TestDeprecatedAliasKeepsItsHeaderWhenCanonicalIsShadowed(t *testing.T) {
	aliases := []struct {
		alias, canonical, arguments string
		columns                     []ProcedureColumn
	}{
		{"nornicdb.decay.info", "kp.decayInfo", "", kpDecayInfoColumns},
		{"nornicdb.knowledgepolicy.info", "kp.info", "", kpInfoColumns},
		{"nornicdb.knowledgepolicy.profiles", "kp.profiles", "", kpProfilesColumns},
		{"nornicdb.knowledgepolicy.policies", "kp.policies", "", kpPoliciesColumns},
		{"nornicdb.knowledgepolicy.resolve", "kp.resolve", "'', 'Person', ''", kpResolveColumns},
		{"nornicdb.knowledgepolicy.deindexStatus", "kp.deindexStatus", "", kpDeindexStatusColumns},
	}
	ctx := context.Background()
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(previous)
			t.Cleanup(ClearUserProcedures)
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			// kp.resolve needs a compiled binding table; an empty one resolves.
			store.GetSchema().SetBindingTable(knowledgepolicy.NewBindingTable())
			exec := NewStorageExecutor(store)
			for _, pair := range aliases {
				_, err := exec.Execute(ctx, "CREATE PROCEDURE "+pair.canonical+"() MODE READ AS RETURN 1 AS value", nil)
				require.NoError(t, err, pair.canonical)

				shadowed, err := exec.Execute(ctx, "CALL "+pair.canonical+"()", nil)
				require.NoError(t, err, pair.canonical)
				require.Equal(t, []string{"value"}, shadowed.Columns, pair.canonical)

				aliased, err := exec.Execute(ctx, "CALL "+pair.alias+"("+pair.arguments+")", nil)
				require.NoError(t, err, pair.alias)
				require.Equal(t, columnNames(pair.columns), aliased.Columns, pair.alias)
			}
		})
	}
}

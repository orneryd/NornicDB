package cypher

import (
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProfileDefinitionProceduresCertifyNoGraphReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := tokenListingExecutor(t)
		for _, query := range []string{
			"CALL dbms.components() YIELD name RETURN name",
			"CALL nornicdb.version() YIELD version RETURN version",
			"CALL dbms.functions() YIELD name RETURN name",
			"CALL dbms.procedures() YIELD name RETURN name",
		} {
			result, plan := profileEquivalentObservedStatement(t, exec, query)
			require.NotEmpty(t, result.Rows, query)
			require.True(t, plan.TotalDBHitsObserved, query)
			require.Zero(t, plan.TotalDBHits, "definitions are not stored graph reads: %s", query)
			require.Empty(t, plan.Unobserved, query)
		}
	})
}

func TestProfileSchemaPropertiesObserveTheirInputPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, populated := range []bool{false, true} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			if populated {
				for _, seed := range []string{
					"CREATE (a:Person {name: 'ada', code: 1})-[:KNOWS {since: 1, code: 2}]->(b:Person {name: 'bob'})",
					"MATCH (a:Person {name: 'ada'}), (b:Person {name: 'bob'}) CREATE (b)-[:KNOWS {since: 2}]->(a), (a)-[:OTHER]->(b)",
					"CREATE ()",
				} {
					_, err := exec.Execute(t.Context(), seed, nil)
					require.NoError(t, err)
				}
			}
			for _, test := range []struct{ query, kind string }{
				{"CALL db.schema.nodeProperties()", profileKindAllNodesScan},
				{"CALL db.schema.relProperties()", profileKindAllRelationshipsScan},
			} {
				result, plan := profileEquivalentObservedStatement(t, exec, test.query)
				population, rows := int64(0), 0
				if populated {
					population, rows = 3, 2
				}
				require.Len(t, result.Rows, rows, test.query)
				require.True(t, plan.TotalDBHitsObserved, test.query)
				require.Empty(t, plan.Unobserved, test.query)
				read := planOperatorOfType(plan.Root, test.kind)
				require.NotNil(t, read, test.query)
				require.True(t, read.RowsObserved)
				require.Equal(t, population, read.ActualRows, "input entities, not derived property rows")
				require.Equal(t, population, read.DBHits)
				require.Equal(t, population, plan.TotalDBHits)
			}
		}
	})
}

func TestSchemaPropertyProceduresPropagatePopulationErrors(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cause := errors.New("metadata population unavailable")
		for _, test := range []struct {
			name, query string
			store       storage.Engine
		}{
			{"node schema", "CALL db.schema.nodeProperties()", &allNodesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: cause}},
			{"relationship schema", "CALL db.schema.relProperties()", &allEdgesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: cause}},
			{"node statistics", "CALL nornicdb.stats()", &allNodesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: cause}},
			{"relationship statistics", "CALL nornicdb.stats()", &allEdgesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: cause}},
		} {
			t.Run(test.name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(test.store, "schema_property_failure"))
				for _, prefix := range []string{"", "PROFILE "} {
					t.Run(prefix+test.query, func(t *testing.T) {
						result, err := exec.Execute(t.Context(), prefix+test.query, nil)
						require.ErrorContains(t, err, cause.Error(), prefix+test.query)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code)
						if result != nil {
							require.Empty(t, result.Rows, "no successful partial metadata on a failed population")
						}
					})
				}
			})
		}
	})
}

func TestProfileDatabaseStatisticsObserveRequestsAndPopulations(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, populated := range []bool{false, true} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			nodes, edges, labels, types := int64(0), int64(0), int64(0), int64(0)
			if populated {
				exec = tokenListingExecutor(t)
				nodes, edges, labels, types = 2, 1, 1, 1
			}
			result, plan := profileEquivalentObservedStatement(t, exec, "CALL nornicdb.stats() YIELD nodes, relationships, labels, relationshipTypes RETURN toInteger(nodes), toInteger(relationships), toInteger(labels), toInteger(relationshipTypes)")
			require.Equal(t, [][]interface{}{{nodes, edges, labels, types}}, result.Rows)
			require.True(t, plan.TotalDBHitsObserved)
			require.Empty(t, plan.Unobserved)
			for _, kind := range []string{"NodeCountRequest", "RelationshipCountRequest"} {
				read := planOperatorOfType(plan.Root, kind)
				require.NotNil(t, read, kind)
				require.True(t, read.RowsObserved)
				require.EqualValues(t, 1, read.ActualRows, "one count response, not its numerical count")
				require.EqualValues(t, 1, read.DBHits, "one actual API request; no physical count-store claim")
			}
			for _, read := range []struct {
				kind       string
				population int64
			}{
				{profileKindAllNodesScan, nodes}, {profileKindAllRelationshipsScan, edges},
			} {
				operator := planOperatorOfType(plan.Root, read.kind)
				require.NotNil(t, operator)
				require.True(t, operator.RowsObserved)
				require.Equal(t, read.population, operator.ActualRows)
				require.Equal(t, read.population, operator.DBHits)
			}
			require.Equal(t, int64(2)+nodes+edges, plan.TotalDBHits)
		}
	})
}

package cypher

// PROFILE covers the three db.schema shape procedures. db.schema.nodeTypeProperties
// and db.schema.relTypeProperties report the entity population their scan read,
// and db.schema.visualization reports its node and relationship populations plus
// each constraint/index catalog request it makes per label. A reviewed
// registration must therefore state the population it actually read, not the
// category or virtual rows it built from it, and a replacement of the same name
// must stay opaque.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// schemaShapesSeed creates four :Person nodes and one :Company node, each with
// two properties, joined by two relationship types: five nodes and three
// relationships. Every category holds two properties, so the shape procedures'
// output row count (one row per category and property) can never stand in for
// the population they read.
const schemaShapesSeed = "CREATE (ada:Person {name: 'ada', born: 1815}), " +
	"(grace:Person {name: 'grace', born: 1906}), " +
	"(alan:Person {name: 'alan', born: 1912}), " +
	"(edsger:Person {name: 'edsger', born: 1930}), " +
	"(acme:Company {name: 'acme', founded: 1999}), " +
	"(ada)-[:KNOWS {since: 1950}]->(grace), " +
	"(grace)-[:KNOWS {since: 1960}]->(alan), " +
	"(ada)-[:WORKS_AT {role: 'engineer'}]->(acme)"

// schemaShapeExecutor returns an executor over the shape fixture with one index
// on each label and one node constraint on :Person: the index catalog holds
// entries of both labels, and the constraint owns an index of its own.
func schemaShapeExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, _ := newProfileRuntimeTestExecutor(t, schemaShapesSeed,
		"CREATE INDEX shape_person_nickname FOR (n:Person) ON (n.nickname)",
		"CREATE INDEX shape_company_ticker FOR (n:Company) ON (n.ticker)",
		"CREATE CONSTRAINT shape_person_name FOR (n:Person) REQUIRE n.name IS UNIQUE")
	return exec
}

// schemaShapeProfile runs statement under PROFILE through the public entry
// point and holds its complete output to the same statement without PROFILE:
// reporting what the procedure read changes no column and no row.
func schemaShapeProfile(t *testing.T, exec *StorageExecutor, statement string) (*ExecuteResult, *ExecutionPlan) {
	t.Helper()
	plain, err := exec.Execute(t.Context(), statement, nil)
	require.NoError(t, err, statement)
	profiled, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
	require.NoError(t, err, statement)
	require.Equal(t, plain.Columns, profiled.Columns, statement)
	require.Equal(t, plain.Rows, profiled.Rows, statement)
	return profiled, executionPlanFromResult(t, profiled)
}

// schemaShapeVirtualNode is a virtual node's reported metadata, compared
// without the entity's object identity.
type schemaShapeVirtualNode struct {
	Name        string
	Indexes     []string
	Constraints []string
}

// schemaShapeVisualization reads db.schema.visualization's one row: the virtual
// node labels in the order it reported them, each virtual node's metadata, and
// each virtual relationship as "<type>: <start label>-><end label>".
func schemaShapeVisualization(t *testing.T, rows [][]interface{}) ([]string, map[string]schemaShapeVirtualNode, []string) {
	t.Helper()
	require.Len(t, rows, 1, "db.schema.visualization answers one row")
	require.Len(t, rows[0], 2, "the row is the node list and the relationship list")
	virtualNodes, ok := rows[0][0].([]*storage.Node)
	require.True(t, ok, "the first column is a list of virtual nodes")
	virtualEdges, ok := rows[0][1].([]*storage.Edge)
	require.True(t, ok, "the second column is a list of virtual relationships")
	labels := make([]string, 0, len(virtualNodes))
	nodes := make(map[string]schemaShapeVirtualNode, len(virtualNodes))
	labelByID := make(map[storage.NodeID]string, len(virtualNodes))
	for _, virtual := range virtualNodes {
		require.Len(t, virtual.Labels, 1, "a virtual node represents one label")
		label := virtual.Labels[0]
		labels = append(labels, label)
		labelByID[virtual.ID] = label
		name, _ := virtual.Properties["name"].(string)
		indexes, _ := virtual.Properties["indexes"].([]string)
		constraints, _ := virtual.Properties["constraints"].([]string)
		nodes[label] = schemaShapeVirtualNode{Name: name, Indexes: indexes, Constraints: constraints}
	}
	edges := make([]string, 0, len(virtualEdges))
	for _, virtual := range virtualEdges {
		start, ok := labelByID[virtual.StartNode]
		require.True(t, ok, "a virtual relationship starts at a virtual node")
		end, ok := labelByID[virtual.EndNode]
		require.True(t, ok, "a virtual relationship ends at a virtual node")
		edges = append(edges, virtual.Type+": "+start+"->"+end)
	}
	return labels, nodes, edges
}

// The node shape procedure reads every node it groups into categories: five
// nodes read, four category/property rows built. The PROFILE record is the
// population read.
func TestProfileNodeTypePropertiesReportsThePopulationItRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := schemaShapeExecutor(t)
		result, plan := schemaShapeProfile(t, exec, "CALL db.schema.nodeTypeProperties()")
		require.Equal(t, [][]interface{}{
			{":`Company`", []string{"Company"}, "founded", []string{"Long"}, true},
			{":`Company`", []string{"Company"}, "name", []string{"String"}, true},
			{":`Person`", []string{"Person"}, "born", []string{"Long"}, true},
			{":`Person`", []string{"Person"}, "name", []string{"String"}, true},
		}, result.Rows)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		scan := planOperatorOfType(plan.Root, profileKindAllNodesScan)
		require.NotNil(t, scan, "the node population read is reported as a plan operator")
		require.True(t, scan.RowsObserved)
		require.Equal(t, int64(5), scan.ActualRows, "every node the scan returned, not the four grouped rows")
		require.EqualValues(t, 5, scan.DBHits)
		require.NotEqual(t, int64(len(result.Rows)), scan.ActualRows,
			"the record is the population read, not the shape output")
		require.EqualValues(t, 5, plan.TotalDBHits)
	})
}

// The relationship shape procedure reads every relationship it groups by type:
// three relationships read, two type/property rows built.
func TestProfileRelTypePropertiesReportsThePopulationItRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := schemaShapeExecutor(t)
		result, plan := schemaShapeProfile(t, exec, "CALL db.schema.relTypeProperties()")
		require.Equal(t, [][]interface{}{
			{":`KNOWS`", "since", []string{"Long"}, true},
			{":`WORKS_AT`", "role", []string{"String"}, true},
		}, result.Rows)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		scan := planOperatorOfType(plan.Root, profileKindAllRelationshipsScan)
		require.NotNil(t, scan, "the relationship population read is reported as a plan operator")
		require.True(t, scan.RowsObserved)
		require.Equal(t, int64(3), scan.ActualRows, "every relationship the scan returned, not the grouped rows")
		require.EqualValues(t, 3, scan.DBHits)
		require.NotEqual(t, int64(len(result.Rows)), scan.ActualRows,
			"the record is the population read, not the shape output")
		require.EqualValues(t, 3, plan.TotalDBHits)
	})
}

// The visualization reads both populations and, for each of the two labels,
// the schema owner's constraint and index catalogs. Every request is its own
// acquisition, including the Company request that found no constraint, and the
// index record is the whole catalog each request returned, before the label
// filter narrows it to the label's own entries.
func TestProfileSchemaVisualizationReportsItsAcquisitionsPerLabel(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := schemaShapeExecutor(t)
		catalogEntries := int64(len(exec.storage.GetSchema().GetIndexes()))
		require.Positive(t, catalogEntries, "every schema holds its index catalog entries")

		plain, err := exec.Execute(t.Context(), "CALL db.schema.visualization()", nil)
		require.NoError(t, err)
		profiled, err := exec.Execute(t.Context(), "PROFILE CALL db.schema.visualization()", nil)
		require.NoError(t, err)
		require.Equal(t, plain.Columns, profiled.Columns)
		plan := executionPlanFromResult(t, profiled)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)

		labels, nodes, edges := schemaShapeVisualization(t, plain.Rows)
		require.Equal(t, []string{"Company", "Person"}, labels)
		require.Equal(t, []string{"KNOWS: Person->Person", "WORKS_AT: Person->Company"}, edges)
		profiledLabels, profiledNodes, profiledEdges := schemaShapeVisualization(t, profiled.Rows)
		require.Equal(t, labels, profiledLabels, "PROFILE reports the same virtual node order")
		require.Equal(t, nodes, profiledNodes, "PROFILE reports the same virtual node metadata")
		require.Equal(t, edges, profiledEdges, "PROFILE reports the same virtual relationships")

		company := nodes["Company"]
		require.Equal(t, "Company", company.Name)
		require.Equal(t, []string{"ticker"}, company.Indexes, "the label's own index after the catalog's label filter")
		require.Empty(t, company.Constraints, "the Company request found no constraint")
		person := nodes["Person"]
		require.Equal(t, "Person", person.Name)
		require.Equal(t, []string{"nickname"}, person.Indexes, "the constraint-owned index is not the label's own index")
		require.Len(t, person.Constraints, 1)
		require.Contains(t, person.Constraints[0], "shape_person_name")
		require.Contains(t, person.Constraints[0], "IS UNIQUE")

		nodeScan := planOperatorOfType(plan.Root, profileKindAllNodesScan)
		require.NotNil(t, nodeScan, "the node population read is reported")
		require.True(t, nodeScan.RowsObserved)
		require.Equal(t, int64(5), nodeScan.ActualRows)
		require.EqualValues(t, 5, nodeScan.DBHits)
		edgeScan := planOperatorOfType(plan.Root, profileKindAllRelationshipsScan)
		require.NotNil(t, edgeScan, "the relationship population read is reported")
		require.True(t, edgeScan.RowsObserved)
		require.Equal(t, int64(3), edgeScan.ActualRows)
		require.EqualValues(t, 3, edgeScan.DBHits)

		constraintCatalog := planOperatorOfType(plan.Root, profileKindSchemaConstraintCatalog)
		require.NotNil(t, constraintCatalog, "the constraint catalog read is reported per label")
		require.True(t, constraintCatalog.RowsObserved)
		require.Equal(t, int64(1), constraintCatalog.ActualRows,
			"the entries the two label requests returned, one of them empty")
		require.EqualValues(t, 2, constraintCatalog.DBHits, "one constraint request per label, the empty one included")

		indexCatalog := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
		require.NotNil(t, indexCatalog, "the index catalog read is reported per label")
		require.True(t, indexCatalog.RowsObserved)
		require.Equal(t, 2*catalogEntries, indexCatalog.ActualRows,
			"each label request acquired the whole catalog, before its label filter")
		require.EqualValues(t, 2, indexCatalog.DBHits, "one index request per label")

		require.EqualValues(t, 5+3+2+2, plan.TotalDBHits,
			"both population reads and both labels' catalog requests are the statement's hits")
	})
}

// An empty graph is an observed zero, not a missing measurement: the shape
// procedures report the empty populations they read, and the visualization
// asks no catalog because it has no label to ask about.
func TestProfileSchemaShapesOnAnEmptyGraphReportObservedZeros(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		for _, shape := range []struct {
			query string
			kind  string
		}{
			{"CALL db.schema.nodeTypeProperties()", profileKindAllNodesScan},
			{"CALL db.schema.relTypeProperties()", profileKindAllRelationshipsScan},
		} {
			result, plan := schemaShapeProfile(t, exec, shape.query)
			require.Empty(t, result.Rows, shape.query)
			require.True(t, plan.TotalDBHitsObserved, shape.query)
			require.Empty(t, plan.Unobserved, shape.query)
			scan := planOperatorOfType(plan.Root, shape.kind)
			require.NotNil(t, scan, shape.query)
			require.True(t, scan.RowsObserved, "an empty population is an observed zero: %s", shape.query)
			require.Zero(t, scan.ActualRows, shape.query)
			require.Zero(t, scan.DBHits, shape.query)
			require.Zero(t, plan.TotalDBHits, shape.query)
		}

		result, plan := schemaShapeProfile(t, exec, "CALL db.schema.visualization()")
		labels, nodes, edges := schemaShapeVisualization(t, result.Rows)
		require.Empty(t, labels)
		require.Empty(t, nodes)
		require.Empty(t, edges)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
		for _, kind := range []string{profileKindAllNodesScan, profileKindAllRelationshipsScan} {
			scan := planOperatorOfType(plan.Root, kind)
			require.NotNil(t, scan, kind)
			require.True(t, scan.RowsObserved, kind)
			require.Zero(t, scan.ActualRows, kind)
			require.Zero(t, scan.DBHits, kind)
		}
		require.Nil(t, planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog),
			"no label asked for the index catalog")
		require.Nil(t, planOperatorOfType(plan.Root, profileKindSchemaConstraintCatalog),
			"no label asked for the constraint catalog")
		require.Zero(t, plan.TotalDBHits)
	})
}

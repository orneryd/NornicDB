package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestCreatePipelineTemplateMatchesGeneralRoute runs the template route and
// the general planCreatePatterns route on the same rows and compares the
// planned nodes and edges (labels, types, endpoints, properties). Row order
// is preserved and IDs are regenerated per route, so IDs are compared as
// equal only in position.
func TestCreatePipelineTemplateMatchesGeneralRoute(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := context.Background()

	rows := []pipelineRow{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
		{"id": int64(3), "name": "c"},
	}
	clause := "CREATE (n:N {id: id, name: name})"
	t.Run(clause, func(t *testing.T) {
		planned := compareCreateRoutes(t, exec, ctx, rows, clause)
		require.True(t, planned, "the template must plan every row of %q", clause)
	})

	endpoints := func(name string) *storage.Node {
		return &storage.Node{ID: storage.NodeID(name)}
	}
	edgeRows := []pipelineRow{
		{"id": int64(1), "n": endpoints("n1"), "m": endpoints("m1")},
		{"id": int64(2), "n": endpoints("n2"), "m": endpoints("m2")},
	}
	clauseEdge := "CREATE (n)-[:R {w: id + 1}]->(m)"
	t.Run(clauseEdge, func(t *testing.T) {
		planned := compareCreateRoutes(t, exec, ctx, edgeRows, clauseEdge)
		require.True(t, planned, "the template must plan every row of %q", clauseEdge)
	})

	// Unsupported shapes must fall back: the template reports usable=false.
	for _, shape := range []string{
		"CREATE p = (n:N {id: id})",
		"CREATE (n {id: id})-[:R]->(:M)",
		"CREATE (n:N $props)",
	} {
		tpl := exec.pipelineCreateTemplateFor(originalBraceView(shape))
		require.False(t, tpl.usable, "shape %q must not use the template", shape)
	}
}

// An applicable template owns evaluation failure and discards its whole plan.
// A successful NULL from an earlier staged binding remains a valid value.
func TestCreatePipelineTemplatePlanIsAtomic(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := context.Background()
	tpl := exec.pipelineCreateTemplateFor(originalBraceView("CREATE (a:T {id: 1}), (b:U {name: properties(1)})"))
	require.NotNil(t, tpl)
	require.True(t, tpl.usable)

	plan := acquireCreatePlan()
	nodes, edges := plan.bindings()
	paths, planned, err := tpl.plan(ctx, exec, pipelineRow{}, nodes, edges, plan)
	require.True(t, planned, "an evaluation failure never retries another route")
	require.Error(t, err)
	require.Nil(t, paths)
	require.Empty(t, plan.nodes, "no partial node plan may survive a deferred row")
	require.Empty(t, plan.edges, "no partial edge plan may survive a deferred row")
	require.Empty(t, nodes)
	require.Empty(t, edges)

	nullTemplate := exec.pipelineCreateTemplateFor(originalBraceView("CREATE (a:T {id: 1}), (b:U {name: a.missing + 1})"))
	paths, planned, err = nullTemplate.plan(ctx, exec, pipelineRow{}, nodes, edges, plan)
	require.NoError(t, err)
	require.True(t, planned)
	require.Len(t, plan.nodes, 2)
	require.NotContains(t, nodes["b"].Properties, "name")

	// A row that resolves completely plans all patterns.
	okTemplate := exec.pipelineCreateTemplateFor(originalBraceView("CREATE (a:T {id: 1}), (b:U {name: name})"))
	require.NotNil(t, okTemplate)
	require.True(t, okTemplate.usable)
	plan = acquireCreatePlan()
	nodes, edges = plan.bindings()
	paths, planned, err = okTemplate.plan(ctx, exec, pipelineRow{"name": "x"}, nodes, edges, plan)
	require.NoError(t, err)
	require.True(t, planned)
	require.Len(t, plan.nodes, 2)
	require.NotNil(t, nodes["a"])
	require.NotNil(t, nodes["b"])
	require.Equal(t, "x", nodes["b"].Properties["name"])
	require.Empty(t, edges)
}

func compareCreateRoutes(t *testing.T, exec *StorageExecutor, ctx context.Context, rows []pipelineRow, clause string) bool {
	t.Helper()
	generalNodes, generalEdges, err := planRowsGeneral(t, exec, ctx, rows, clause)
	require.NoError(t, err)
	templateNodes, templateEdges, planned := planRowsTemplate(t, exec, ctx, rows, clause)
	if !planned {
		// Template declined (acceptable); nothing to compare.
		return false
	}
	require.Len(t, templateNodes, len(generalNodes), "node counts differ")
	require.Len(t, templateEdges, len(generalEdges), "edge counts differ")
	for i, node := range generalNodes {
		require.Equal(t, node.Labels, templateNodes[i].Labels, "node %d labels", i)
		require.Equal(t, node.Properties, templateNodes[i].Properties, "node %d properties", i)
	}
	for i, edge := range generalEdges {
		require.Equal(t, edge.Type, templateEdges[i].Type, "edge %d type", i)
		require.Equal(t, edge.Properties, templateEdges[i].Properties, "edge %d properties", i)
	}
	return true
}

// planRowsGeneral plans every row through planCreatePatterns, returning the
// nodes and edges appended in row order.
func planRowsGeneral(t *testing.T, exec *StorageExecutor, ctx context.Context, rows []pipelineRow, clause string) ([]*storageNodeLite, []*storageEdgeLite, error) {
	t.Helper()
	var outNodes []*storageNodeLite
	var outEdges []*storageEdgeLite
	for _, row := range rows {
		plan := acquireCreatePlan()
		nodes, edges := plan.bindings()
		newRow := make(pipelineRow, len(row))
		for name, value := range row {
			newRow[name] = value
			if typed, ok := value.(*storage.Node); ok && typed != nil {
				nodes[name] = typed
			}
		}
		pattern := pipelineClauseBody(clause, "CREATE")
		_, err := exec.planCreatePatterns(withValueBindings(ctx, newRow), originalBraceView(pattern), nodes, edges, plan)
		if err != nil {
			plan.release()
			return nil, nil, err
		}
		for _, node := range plan.nodes {
			outNodes = append(outNodes, &storageNodeLite{Labels: node.Labels, Properties: node.Properties})
		}
		for _, edge := range plan.edges {
			outEdges = append(outEdges, &storageEdgeLite{Type: edge.Type, Properties: edge.Properties})
		}
		plan.release()
	}
	return outNodes, outEdges, nil
}

// planRowsTemplate plans every row through the template, reporting whether
// every row used it.
func planRowsTemplate(t *testing.T, exec *StorageExecutor, ctx context.Context, rows []pipelineRow, clause string) ([]*storageNodeLite, []*storageEdgeLite, bool) {
	t.Helper()
	template := exec.pipelineCreateTemplateFor(originalBraceView(clause))
	if template == nil || !template.usable {
		return nil, nil, false
	}
	plan := acquireCreatePlan()
	defer plan.release()
	nodes, edges := plan.bindings()
	allPlanned := true
	for _, row := range rows {
		// Row-bound nodes and relationships become the template's
		// endpoints, as pipelinePlanCreateRow binds them before planning.
		for name, value := range row {
			if typed, ok := value.(*storage.Node); ok && typed != nil {
				nodes[name] = typed
			}
			if typed, ok := value.(*storage.Edge); ok && typed != nil {
				edges[name] = typed
			}
		}
		_, planned, err := template.plan(ctx, exec, row, nodes, edges, plan)
		require.NoError(t, err)
		if !planned {
			allPlanned = false
			break
		}
	}
	var outNodes []*storageNodeLite
	var outEdges []*storageEdgeLite
	for _, node := range plan.nodes {
		outNodes = append(outNodes, &storageNodeLite{Labels: node.Labels, Properties: node.Properties})
	}
	for _, edge := range plan.edges {
		outEdges = append(outEdges, &storageEdgeLite{Type: edge.Type, Properties: edge.Properties})
	}
	return outNodes, outEdges, allPlanned
}

type storageNodeLite struct {
	Labels     []string
	Properties map[string]interface{}
}

type storageEdgeLite struct {
	Type       string
	Properties map[string]interface{}
}

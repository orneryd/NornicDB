package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
)

func repeatedPropertyReviewQuery() string {
	return "MATCH (d:Doc {title:'a'}) RETURN [" + strings.TrimSuffix(strings.Repeat("d.body,", 100), ",") + "] AS hidden"
}
func TestReviewRepeatedPropertyProjectionAllocationBudget(t *testing.T) {
	e, _ := reviewPropertyExecutor(t)
	q := repeatedPropertyReviewQuery()
	ctx := context.Background()
	_, err := e.Execute(ctx, q, nil)
	require.NoError(t, err)
	allocs := testing.AllocsPerRun(3, func() {
		_, err = e.Execute(ctx, q, nil)
		if err != nil {
			panic(err)
		}
	})
	t.Logf("100 repeated suppressed property reads: %.0f allocations", allocs)
	require.Less(t, allocs, float64(1800), "one read-only statement must reuse an entity's detached property view")
}
func BenchmarkReviewRepeatedPropertyProjection(b *testing.B) {
	e, _ := reviewPropertyExecutor(b)
	q := repeatedPropertyReviewQuery()
	ctx := context.Background()
	_, err := e.Execute(ctx, q, nil)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = e.Execute(ctx, q, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// This decorator deliberately produces fresh native entities for every recheck.
// An entity-ID-only cache is also insufficient: the source body is checked.
type reviewCloningPropertyEngine struct{ storage.Engine }

func (e *reviewCloningPropertyEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *reviewCloningPropertyEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	n, err := e.Engine.GetNode(id)
	return storage.CopyNode(n), err
}
func TestReviewStatementPropertyViewsReuseClonedBodies(t *testing.T) {
	base, _ := reviewPropertyExecutor(t)
	wrapper := &reviewCloningPropertyEngine{Engine: base.storage}
	e := base.cloneWithStorage(wrapper).withStatementPropertyViews("MATCH (n:Doc {title:'a'}) RETURN n.body")
	require.NotNil(t, e)
	first, err := wrapper.GetNode("a")
	require.NoError(t, err)
	again, err := wrapper.GetNode("a")
	require.NoError(t, err)
	require.NotSame(t, first, again)
	a := e.queryVisibleValue(first).(*storage.Node)
	b := e.queryVisibleValue(again).(*storage.Node)
	require.NotContains(t, a.Properties, "body")
	require.Equal(t, reflect.ValueOf(a.Properties).UnsafePointer(), reflect.ValueOf(b.Properties).UnsafePointer(), "cloned index rechecks must reuse a verified identical body projection")
	require.Same(t, a, e.queryVisibleValue(first))
	require.Same(t, a, e.queryVisibleValue(a))
	changed := storage.CopyNode(again)
	changed.Properties["title"] = "changed"
	require.Equal(t, "changed", e.queryVisibleValue(changed).(*storage.Node).Properties["title"], "same ID and timestamps do not prove body identity")
	unscored := storage.CopyNode(again)
	unscored.Labels = []string{"Other"}
	require.Contains(t, e.queryVisibleValue(unscored).(*storage.Node).Properties, "body")
	edge, err := wrapper.GetEdge("ab")
	require.NoError(t, err)
	r := e.queryVisibleValue(edge).(*storage.Edge)
	r2 := e.queryVisibleValue(storage.CopyEdge(edge)).(*storage.Edge)
	require.NotContains(t, r.Properties, "summary")
	require.Equal(t, reflect.ValueOf(r.Properties).UnsafePointer(), reflect.ValueOf(r2.Properties).UnsafePointer())
	require.Same(t, r, e.queryVisibleValue(edge))
	require.Same(t, r, e.queryVisibleValue(r))
	changedEdge := storage.CopyEdge(edge)
	changedEdge.Type = "OTHER"
	require.Contains(t, e.queryVisibleValue(changedEdge).(*storage.Edge).Properties, "summary")
	require.Nil(t, e.statementPropertyViews.node(nil))
	require.Nil(t, e.statementPropertyViews.edge(nil))
}
func TestReviewStatementPropertyViewsExcludeWritesAndRefresh(t *testing.T) {
	e, be := reviewPropertyExecutor(t)
	ctx := context.Background()
	require.Nil(t, e.withStatementPropertyViews("MATCH (d:Doc) SET d.title='updated' RETURN d.title"))
	for _, title := range []string{"a", "a"} {
		r, err := e.Execute(ctx, "MATCH (d:Doc {title:'a'}) RETURN d.title", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{title}}, r.Rows)
	}
	r, err := e.Execute(ctx, "MATCH (d:Doc {title:'a'}) SET d.title='updated' RETURN d.title,d.body", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"updated", nil}}, r.Rows)
	raw, err := be.GetNode("review:a")
	require.NoError(t, err)
	require.Equal(t, "hidden", raw.Properties["body"])
	r, err = e.Execute(ctx, "MATCH (d:Doc {title:'updated'}) RETURN d.title", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"updated"}}, r.Rows)
}

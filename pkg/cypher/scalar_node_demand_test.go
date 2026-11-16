package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A single labelled node whose only large content is one property no query in
// this file reads. The public statement is the subject of the test; the store
// records the property demand the statement makes, because the demand — not a
// duration — is what decides whether a 128 KiB body is decoded for a row that
// never reads it.
const (
	scalarNodeLabel          = "EIBig"
	scalarNodeUserID         = "n1"
	scalarNodeSmallProperty  = "small"
	scalarNodeSmallValue     = "v"
	scalarNodeUnusedProperty = "unused_big"
	scalarNodeUnusedBytes    = 128 << 10
)

// scalarNodeDemandGraph is one database holding the fixture node, read through
// the public executor.
type scalarNodeDemandGraph struct {
	store  *recordingLabelScanEngine
	scoped *storage.NamespacedEngine
	exec   *StorageExecutor
}

// newScalarNodeDemandGraph builds the fixture with, or without, the unread
// 128 KiB property, so one statement can be compared across the two bodies. The
// store is opened with the high-performance profile: the default in-memory
// profile refuses a value over its 64 KiB threshold, so the unused body could
// not exist there at all.
func newScalarNodeDemandGraph(t *testing.T, withUnused bool) *scalarNodeDemandGraph {
	t.Helper()
	engine, err := storage.NewMemoryEngineWithOptions(storage.BadgerOptions{HighPerformance: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := &recordingLabelScanEngine{MemoryEngine: engine}
	scoped := storage.NewNamespacedEngine(store, "test")
	properties := map[string]interface{}{scalarNodeSmallProperty: scalarNodeSmallValue}
	if withUnused {
		properties[scalarNodeUnusedProperty] = strings.Repeat("x", scalarNodeUnusedBytes)
	}
	_, err = scoped.CreateNode(t.Context(), &storage.Node{
		ID:              scalarNodeUserID,
		Labels:          []string{scalarNodeLabel},
		Properties:      properties,
		ChunkEmbeddings: [][]float32{{0, 0, 0, 1}},
	})
	require.NoError(t, err)
	return &scalarNodeDemandGraph{
		store:  store,
		scoped: scoped,
		exec:   NewStorageExecutor(scoped, QueryCachePolicy{MaxEntries: 0}),
	}
}

func (g *scalarNodeDemandGraph) run(t *testing.T, query string) *ExecuteResult {
	t.Helper()
	g.store.scans = nil
	result, err := g.exec.Execute(t.Context(), query, nil)
	require.NoError(t, err, query)
	return result
}

// scalarNodeWholeBodySizes is the decoded size of the fixture node's complete
// property body.
func scalarNodeWholeBodySizes() map[string]int {
	return map[string]int{scalarNodeSmallProperty: len(scalarNodeSmallValue), scalarNodeUnusedProperty: scalarNodeUnusedBytes}
}

// scalarNodeWholeProperties is the fixture node's complete property map.
func scalarNodeWholeProperties() map[string]interface{} {
	return map[string]interface{}{
		scalarNodeSmallProperty:  scalarNodeSmallValue,
		scalarNodeUnusedProperty: strings.Repeat("x", scalarNodeUnusedBytes),
	}
}

// installScalarNodeCallback registers one native plugin function under name
// that receives the node image and returns what it observed, so a test can
// assert the image a native consumer keeps and whether its edits reach storage.
func installScalarNodeCallback(t *testing.T, name string, observe func(*storage.Node) interface{}) {
	t.Helper()
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = checkedTestLookup(func(candidate string) (interface{}, bool) {
		if candidate == name {
			return func(value interface{}) interface{} {
				node, ok := value.(*storage.Node)
				if !ok {
					return fmt.Sprintf("%T", value)
				}
				return observe(node)
			}, true
		}
		if previous != nil {
			if handler, found := previous(candidate); found {
				return handler, true
			}
		}
		return nil, false
	})
}

// A statement that reads neither the node's property map nor a property body
// through the variable must not acquire the unused 128 KiB property: identity,
// labels, null-ness and a list's size read the image's header only, and
// head([n]).small reads one property. The answers are identical with and
// without the unused body; the property demand is what this test settles.
func TestScalarNodeUsesDoNotDemandUnusedPropertyBodies(t *testing.T) {
	for _, tc := range []struct {
		name           string
		query          string
		want           [][]interface{}
		wantProperties []string
	}{
		{
			name:           "id",
			query:          "MATCH (n:EIBig) RETURN id(n)",
			want:           [][]interface{}{{scalarNodeUserID}},
			wantProperties: []string{},
		},
		{
			name:           "element_id",
			query:          "MATCH (n:EIBig) RETURN elementId(n)",
			want:           [][]interface{}{{"4:test:" + scalarNodeUserID}},
			wantProperties: []string{},
		},
		{
			name:           "labels",
			query:          "MATCH (n:EIBig) RETURN labels(n)",
			want:           [][]interface{}{{[]interface{}{scalarNodeLabel}}},
			wantProperties: []string{},
		},
		{
			name:           "is_null",
			query:          "MATCH (n:EIBig) RETURN n IS NULL",
			want:           [][]interface{}{{false}},
			wantProperties: []string{},
		},
		{
			name:           "is_not_null",
			query:          "MATCH (n:EIBig) RETURN n IS NOT NULL",
			want:           [][]interface{}{{true}},
			wantProperties: []string{},
		},
		{
			name:           "where_is_not_null",
			query:          "MATCH (n:EIBig) WHERE n IS NOT NULL RETURN 1",
			want:           [][]interface{}{{int64(1)}},
			wantProperties: []string{},
		},
		{
			name:           "single_element_list_size",
			query:          "MATCH (n:EIBig) RETURN size([n])",
			want:           [][]interface{}{{int64(1)}},
			wantProperties: []string{},
		},
		{
			name:           "collected_list_size",
			query:          "MATCH (n:EIBig) RETURN size(collect(n))",
			want:           [][]interface{}{{int64(1)}},
			wantProperties: []string{},
		},
		{
			name:           "passed_through_with",
			query:          "MATCH (n:EIBig) WITH n RETURN 1",
			want:           [][]interface{}{{int64(1)}},
			wantProperties: []string{},
		},
		{
			name:           "property",
			query:          "MATCH (n:EIBig) RETURN n.small",
			want:           [][]interface{}{{scalarNodeSmallValue}},
			wantProperties: []string{scalarNodeSmallProperty},
		},
		{
			name:           "bracket_property",
			query:          "MATCH (n:EIBig) RETURN n['small']",
			want:           [][]interface{}{{scalarNodeSmallValue}},
			wantProperties: []string{scalarNodeSmallProperty},
		},
		{
			name:           "parenthesized_property",
			query:          "MATCH (n:EIBig) RETURN (n).small",
			want:           [][]interface{}{{scalarNodeSmallValue}},
			wantProperties: []string{scalarNodeSmallProperty},
		},
		{
			name:           "head_property",
			query:          "MATCH (n:EIBig) RETURN head([n]).small",
			want:           [][]interface{}{{scalarNodeSmallValue}},
			wantProperties: []string{scalarNodeSmallProperty},
		},
		{
			name: "identity_and_scalar_projection",
			query: "MATCH (n:EIBig) RETURN id(n), elementId(n), labels(n), n IS NULL, n.small, n['small'], " +
				"(n).small, head([n]).small, size([n]), size(collect(n))",
			want: [][]interface{}{{
				scalarNodeUserID, "4:test:" + scalarNodeUserID, []interface{}{scalarNodeLabel}, false,
				scalarNodeSmallValue, scalarNodeSmallValue, scalarNodeSmallValue, scalarNodeSmallValue,
				int64(1), int64(1),
			}},
			wantProperties: []string{scalarNodeSmallProperty},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small := newScalarNodeDemandGraph(t, false)
			big := newScalarNodeDemandGraph(t, true)
			smallResult := small.run(t, tc.query)
			bigResult := big.run(t, tc.query)
			require.Equal(t, smallResult.Columns, bigResult.Columns, "the unused property does not change the columns")
			require.Equal(t, smallResult.Rows, bigResult.Rows, "the unused property does not change the answer")
			require.Equal(t, tc.want, bigResult.Rows, "the statement's answer is the same with and without the unused body")

			require.Len(t, big.store.scans, 1, "one label read answers the statement")
			scan := big.store.scans[0]
			require.NotEmpty(t, scan.images, "the read delivered the matched node")
			require.NotNil(t, scan.projection, "the read demanded every stored property body, including the unused 128 KiB one")
			require.Equal(t, tc.wantProperties, scan.projection, "the read decodes only the properties the statement uses")
			for _, image := range scan.images {
				require.Equal(t, []string{scalarNodeLabel}, image.labels, "every image carries the node's header")
				require.NotContains(t, image.propertySizes, scalarNodeUnusedProperty,
					"the read decoded the unused 128 KiB property into the row image")
			}
		})
	}
}

// The other direction: a node value that leaves the statement — returned to the
// client, handed to properties(), or passed to native code — keeps the whole
// property body, including the unused 128 KiB property. Those reads are the
// escapes the demand decision must keep conservative.
func TestNodeImageEscapesKeepWholePropertyBodies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		check func(t *testing.T, result *ExecuteResult, scan recordedNodeScan)
	}{
		{
			name:  "returned_node",
			query: "MATCH (n:EIBig) RETURN n",
			check: func(t *testing.T, result *ExecuteResult, scan recordedNodeScan) {
				t.Helper()
				require.Equal(t, []string{"n"}, result.Columns)
				require.Len(t, result.Rows, 1)
				node := result.Rows[0][0].(*storage.Node)
				require.Equal(t, storage.NodeID(scalarNodeUserID), node.ID)
				require.Equal(t, []string{scalarNodeLabel}, node.Labels)
				require.Equal(t, scalarNodeWholeProperties(), node.Properties)
				require.Empty(t, node.ChunkEmbeddings, "an ordinary result keeps the statement's metadata image")
				require.EqualValues(t, 1, node.EmbedMeta["chunk_count"], "the metadata image still reports the stored vector")
				require.True(t, scan.queryRow, "the query-row read asked storage for embedding metadata")
				require.Zero(t, scan.images[0].chunkEmbeddings, "the query-row read hydrated metadata, not vectors")
			},
		},
		{
			name:  "properties",
			query: "MATCH (n:EIBig) RETURN properties(n)",
			check: func(t *testing.T, result *ExecuteResult, _ recordedNodeScan) {
				t.Helper()
				require.Equal(t, [][]interface{}{{scalarNodeWholeProperties()}}, result.Rows)
			},
		},
		{
			name:  "properties_member",
			query: "MATCH (n:EIBig) RETURN properties(n).small",
			check: func(t *testing.T, result *ExecuteResult, _ recordedNodeScan) {
				t.Helper()
				require.Equal(t, [][]interface{}{{scalarNodeSmallValue}}, result.Rows)
			},
		},
		{
			name:  "single_element_list",
			query: "MATCH (n:EIBig) RETURN head([n])",
			check: func(t *testing.T, result *ExecuteResult, _ recordedNodeScan) {
				t.Helper()
				require.Equal(t, scalarNodeWholeProperties(), result.Rows[0][0].(*storage.Node).Properties)
			},
		},
		{
			name:  "collected_nodes",
			query: "MATCH (n:EIBig) RETURN collect(n)",
			check: func(t *testing.T, result *ExecuteResult, _ recordedNodeScan) {
				t.Helper()
				collected := result.Rows[0][0].([]interface{})
				require.Len(t, collected, 1)
				require.Equal(t, scalarNodeWholeProperties(), collected[0].(*storage.Node).Properties)
			},
		},
		{
			name: "identity_projection_with_properties_member",
			query: "MATCH (n:EIBig) RETURN id(n), elementId(n), labels(n), n IS NULL, n.small, n['small'], " +
				"(n).small, properties(n).small, head([n]).small, size([n]), size(collect(n))",
			check: func(t *testing.T, result *ExecuteResult, _ recordedNodeScan) {
				t.Helper()
				require.Equal(t, [][]interface{}{{
					scalarNodeUserID, "4:test:" + scalarNodeUserID, []interface{}{scalarNodeLabel}, false,
					scalarNodeSmallValue, scalarNodeSmallValue, scalarNodeSmallValue, scalarNodeSmallValue,
					scalarNodeSmallValue, int64(1), int64(1),
				}}, result.Rows)
			},
		},
		{
			name:  "native_callback",
			query: "MATCH (n:EIBig) RETURN image.scalarNodeProbe(n)",
			check: func(t *testing.T, result *ExecuteResult, scan recordedNodeScan) {
				t.Helper()
				require.Equal(t, [][]interface{}{{"properties=2 big=131072 vectors=1 small=v"}}, result.Rows)
				require.Equal(t, 1, scan.images[0].chunkEmbeddings, "a native consumer's image keeps the stored vectors")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installScalarNodeCallback(t, "image.scalarNodeProbe", func(node *storage.Node) interface{} {
				return fmt.Sprintf("properties=%d big=%d vectors=%d small=%v",
					len(node.Properties), len(node.Properties[scalarNodeUnusedProperty].(string)), len(node.ChunkEmbeddings), node.Properties[scalarNodeSmallProperty])
			})
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, tc.query)
			require.Len(t, graph.store.scans, 1, "one label read answers the statement")
			scan := graph.store.scans[0]
			require.Nil(t, scan.projection, "the read must demand every property body for a node value that leaves the statement")
			require.Len(t, scan.images, 1)
			require.Equal(t, scalarNodeWholeBodySizes(), scan.images[0].propertySizes, "the read acquired the complete property body")
			tc.check(t, result, scan)
		})
	}
}

// A delivered image belongs to the row, not to storage: a projected read must
// not leave a header-only image behind for a later whole-node read, and edits a
// caller or a native callback make to an image must not reach the stored node.
func TestDeliveredNodeImagesStayDetachedFromStorage(t *testing.T) {
	installScalarNodeCallback(t, "image.scalarNodeMutate", func(node *storage.Node) interface{} {
		node.Properties[scalarNodeSmallProperty] = "callback edit"
		delete(node.Properties, scalarNodeUnusedProperty)
		node.Labels[0] = "CallbackLabel"
		node.ChunkEmbeddings = nil
		return "mutated"
	})
	graph := newScalarNodeDemandGraph(t, true)
	readStored := func(t *testing.T) [][]interface{} {
		t.Helper()
		return graph.run(t, "MATCH (n:EIBig) RETURN labels(n), properties(n).small, size(properties(n).unused_big)").Rows
	}
	wantStored := [][]interface{}{{[]interface{}{scalarNodeLabel}, scalarNodeSmallValue, int64(scalarNodeUnusedBytes)}}

	require.Equal(t, [][]interface{}{{scalarNodeSmallValue}}, graph.run(t, "MATCH (n:EIBig) RETURN n.small").Rows)
	node := graph.run(t, "MATCH (n:EIBig) RETURN n").Rows[0][0].(*storage.Node)
	require.Equal(t, scalarNodeWholeProperties(), node.Properties, "a read after a projected read still carries the whole image")

	require.Equal(t, [][]interface{}{{"mutated"}}, graph.run(t, "MATCH (n:EIBig) RETURN image.scalarNodeMutate(n)").Rows)
	require.Equal(t, wantStored, readStored(t), "a native consumer's edits never reach the stored node")

	node.Properties[scalarNodeSmallProperty] = "caller edit"
	node.Properties[scalarNodeUnusedProperty] = "caller edit"
	node.Labels[0] = "CallerLabel"
	node.ChunkEmbeddings = [][]float32{{1}}
	require.Equal(t, wantStored, readStored(t), "a caller's edits never reach the stored node")

	stored, err := graph.scoped.GetNode(t.Context(), scalarNodeUserID)
	require.NoError(t, err)
	require.Equal(t, []string{scalarNodeLabel}, stored.Labels)
	require.Equal(t, scalarNodeWholeProperties(), stored.Properties)
	require.Len(t, stored.ChunkEmbeddings, 1)
}

// The demand this file asks for already has a storage answer: a label scan told
// to decode no user property still delivers the image's identity and labels, so
// a header-only demand needs no new scanner or leaf pattern. Vectors are that
// reader's caller's separate decision, not part of the property demand.
func TestProjectedLabelScanDeliversHeaderWithoutProperties(t *testing.T) {
	graph := newScalarNodeDemandGraph(t, true)
	require.NoError(t, graph.scoped.StreamNodesByLabelProjected(t.Context(), scalarNodeLabel, []string{}, func(*storage.Node) error {
		return nil
	}))
	require.Len(t, graph.store.scans, 1)
	scan := graph.store.scans[0]
	require.NotNil(t, scan.projection, "an empty projection is a demand, not the whole-body nil")
	require.Empty(t, scan.projection)
	require.Len(t, scan.images, 1)
	image := scan.images[0]
	require.Equal(t, storage.NodeID("test:"+scalarNodeUserID), image.id)
	require.Equal(t, []string{scalarNodeLabel}, image.labels)
	require.Empty(t, image.propertySizes, "a header-only projection decodes no property body")
}

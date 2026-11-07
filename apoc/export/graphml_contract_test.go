package apocexport_test

import (
	"context"
	"encoding/xml"
	"fmt"
	apoclog "github.com/orneryd/nornicdb/apoc/log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

type graphMLValue struct {
	Key   string `xml:"key,attr"`
	Value string `xml:",chardata"`
}
type graphMLDocument struct {
	Graph struct {
		Nodes []struct {
			ID   string         `xml:"id,attr"`
			Data []graphMLValue `xml:"data"`
		} `xml:"node"`
		Edges []struct {
			Source string         `xml:"source,attr"`
			Target string         `xml:"target,attr"`
			Data   []graphMLValue `xml:"data"`
		} `xml:"edge"`
	} `xml:"graph"`
}

// callRegisteredExport invokes one registered export handler the way the query
// adapter does: the caller's context is bound as the handler's first argument.
func callRegisteredExport(t *testing.T, name string, ctx context.Context, args ...interface{}) (interface{}, error) {
	t.Helper()
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	info, found := apoc.GetInfo(name)
	require.True(t, found, "%s must be registered", name)
	require.NotNil(t, info.Handler, name)
	return info.Handler.CallWithPolicy(args, []reflect.Value{reflect.ValueOf(ctx)}, registry.APOCArguments)
}

func TestGraphMLPreservesXMLTextAttributesAndUnicode(t *testing.T) {
	const key = "key\"&<>\n漢"
	const value = "value&<>\"'\nélan漢😀"
	const relationType = "TYPE&<>\"'漢"
	output, err := apocexport.GraphML([]*apocexport.Node{{ID: 1, Properties: map[string]interface{}{key: value}}, {ID: 2}}, []*apocexport.Relationship{{ID: 3, StartNode: 1, EndNode: 2, Type: relationType, Properties: map[string]interface{}{key: value}}})
	require.NoError(t, err)
	var document graphMLDocument
	require.NoError(t, xml.Unmarshal([]byte(output), &document))
	require.Len(t, document.Graph.Nodes, 2)
	require.Equal(t, "n1", document.Graph.Nodes[0].ID)
	require.Equal(t, []graphMLValue{{Key: key, Value: value}}, document.Graph.Nodes[0].Data)
	require.Len(t, document.Graph.Edges, 1)
	edge := document.Graph.Edges[0]
	require.Equal(t, "n1", edge.Source)
	require.Equal(t, "n2", edge.Target)
	require.ElementsMatch(t, []graphMLValue{{Key: "type", Value: relationType}, {Key: key, Value: value}}, edge.Data)
}

func TestGraphMLRegisteredFileExportWritesParsableDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.graphml")
	_, err := callRegisteredExport(t, "apoc.export.graphMLAll", exportCaller(t), []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "a&b"}}}, []*apocexport.Relationship(nil), path)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var document graphMLDocument
	require.NoError(t, xml.Unmarshal(data, &document))
	require.Len(t, document.Graph.Nodes, 1)
	require.Equal(t, []graphMLValue{{Key: "name", Value: "a&b"}}, document.Graph.Nodes[0].Data)
}

func TestGraphMLFileExportPropagatesDestinationError(t *testing.T) {
	require.Error(t, apocexport.GraphMLAll(exportCaller(t), nil, nil, t.TempDir()), "a directory is not a writable export file")
}

func TestGraphMLRejectsUnrepresentableXMLBeforeWriting(t *testing.T) {
	for _, bad := range []string{"\x00", "\x01", "\x0b", "\ufffe", "\uffff", "\xff", "\xed\xa0\x80"} {
		for _, field := range []string{"key", "node value", "relationship value", "type"} {
			t.Run(field+"/"+fmt.Sprintf("%x", bad), func(t *testing.T) {
				nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}
				rels := []*apocexport.Relationship{{ID: 2, StartNode: 1, EndNode: 1, Type: "TYPE", Properties: map[string]interface{}{"name": "value"}}}
				switch field {
				case "key":
					nodes[0].Properties = map[string]interface{}{bad: "value"}
				case "node value":
					nodes[0].Properties["name"] = bad
				case "relationship value":
					rels[0].Properties["name"] = bad
				case "type":
					rels[0].Type = bad
				}
				output, err := apocexport.ToString(nodes, rels, "graphml")
				require.Error(t, err)
				require.Empty(t, output)
				path := filepath.Join(t.TempDir(), "graph.graphml")
				require.Error(t, apocexport.GraphMLAll(exportCaller(t), nodes, rels, path))
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			})
		}
	}
}

func TestGraphMLRegisteredMemoryExportAndEmptyGraph(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	for _, nodes := range [][]*apocexport.Node{nil, {}, {{ID: 1, Properties: map[string]interface{}{"name": "a&b\t\n\r"}}}} {
		result, err := apoc.Call("apoc.export.graphML", []interface{}{nodes, []*apocexport.Relationship(nil)})
		require.NoError(t, err)
		output, ok := result.(string)
		require.True(t, ok)
		var document graphMLDocument
		require.NoError(t, xml.Unmarshal([]byte(output), &document))
		require.Len(t, document.Graph.Nodes, len(nodes))
		if len(nodes) > 0 {
			require.Equal(t, []graphMLValue{{Key: "name", Value: "a&b\t\n\r"}}, document.Graph.Nodes[0].Data)
		}
	}
	_, err := apoc.Call("apoc.export.graphML", []interface{}{[]*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"bad": "\x00"}}}, []*apocexport.Relationship(nil)})
	require.Error(t, err)
}

func TestGraphMLRejectedInputPreservesExistingDestination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.graphml")
	require.NoError(t, os.WriteFile(path, []byte("existing export"), 0600))
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"bad": "\x00"}}}
	for _, name := range []string{"apoc.export.graphMLAll", "apoc.export.graphMLData"} {
		_, err := callRegisteredExport(t, name, exportCaller(t), nodes, []*apocexport.Relationship(nil), path)
		require.Error(t, err)
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "existing export", string(content))
	}
}

func TestExportSiblingFileEntryPointsWriteAndReportErrors(t *testing.T) {
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}
	for _, name := range []string{"cypher", "raw"} {
		t.Run(name, func(t *testing.T) {
			ctx := exportCaller(t)
			path := filepath.Join(t.TempDir(), "nested", "export.txt")
			var err error
			if name == "cypher" {
				err = apocexport.CypherAll(ctx, nodes, nil, path)
			} else {
				err = apocexport.ToFile(ctx, "raw content", path)
			}
			require.NoError(t, err)
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			if name == "cypher" {
				require.Equal(t, apocexport.Cypher(nodes, nil), string(content))
				require.Error(t, apocexport.CypherAll(ctx, nodes, nil, t.TempDir()))
			} else {
				require.Equal(t, "raw content", string(content))
				require.Error(t, apocexport.ToFile(ctx, "raw", t.TempDir()))
			}
		})
	}
}

func TestRegisteredRawExportUsesCanonicalFileWriter(t *testing.T) {
	ctx := exportCaller(t)
	path := filepath.Join(t.TempDir(), "raw.txt")
	_, err := callRegisteredExport(t, "apoc.export.toFile", ctx, "content", path)
	require.NoError(t, err)
	output, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "content", string(output))
	_, err = callRegisteredExport(t, "apoc.export.toFile", ctx, "content", t.TempDir())
	require.Error(t, err)
}

func TestExportReplacementAndLogAppendHaveDifferentFileContracts(t *testing.T) {
	ctx := exportCaller(t)
	path := filepath.Join(t.TempDir(), "data.txt")
	require.NoError(t, apocexport.ToFile(ctx, "first", path))
	require.NoError(t, apocexport.ToFile(ctx, "replacement", path))
	output, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "replacement", string(output))
	require.NoError(t, apoclog.ToFile(path, "log one"))
	require.NoError(t, apoclog.ToFile(path, "log two"))
	output, err = os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(output), "replacement"))
	require.Contains(t, string(output), "log one\n")
	require.Contains(t, string(output), "log two\n")
	require.Greater(t, len(output), len("replacementlog one\nlog two\n"), "logger adds timestamp formatting; export preserves exact bytes")
}

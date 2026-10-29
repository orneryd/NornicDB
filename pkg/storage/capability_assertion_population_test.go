package storage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestCapabilityAssertionDomainPopulation(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
type NodeID string
type Node struct{}
type Engine interface{GetNode(NodeID)(*Node,error)}
type Base struct{}
func(*Base)GetNode(NodeID)(*Node,error){return nil,nil}
func(*Base)StreamNodes(func(*Node)error)error{return nil}
func(*Base)Close()error{return nil}
type Display interface{String()string}
type Duration struct{}
func(Duration)String()string{return "duration"}
type DiagnosticError struct{}
func(*DiagnosticError)Error()string{return "diagnostic"}
type StoredMetadata struct{Present bool}
`,
		"plugins/unfamiliar/assert.go": `package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
type ParseContext struct{}
type Listener interface{Next(*ParseContext)}
func parser(v any){_,_=v.(Listener)}
func diagnostic(v any){_,_=v.(*storage.DiagnosticError)}
func facts(v any){_,_=v.(storage.StoredMetadata)}
func read(v any){_,_=v.(interface{GetNode(storage.NodeID)(*storage.Node,error)})}
func stream(v any){_,_=v.(interface{StreamNodes(func(*storage.Node)error)error})}
func sameName(v any){_,_=v.(interface{GetNode(string)string})}
func display(v any){_,_=v.(storage.Duration)}
func closeService(v any){_,_=v.(interface{Close()error})}
`,
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	expected := map[string]string{"read": "capability-assertion", "stream": "capability-assertion"}
	seen := map[string]bool{}
	for _, site := range result.Sites {
		if site.Population != "assertions" || site.Kind != "type-assert" {
			continue
		}
		for name, contract := range expected {
			if strings.HasSuffix(site.Function, "."+name) {
				require.Equal(t, contract, site.Contract, "typed assertion %s: %s", site.Key, site.Detail)
				seen[name] = true
			}
		}
	}
	require.Len(t, seen, len(expected), "both actual storage capability contracts must be queried")
	for _, site := range result.Sites {
		if site.Population == "assertions" {
			require.NotEqual(t, "value-or-service-assertion", site.Contract, "unrelated value assertions are source cross-checks")
		}
	}
}

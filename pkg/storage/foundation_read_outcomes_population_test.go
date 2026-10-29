// Author: Lane #1644 (Codex). Errorless read adapters are discovered by their
// compiled values and signatures; a spelling never supplies a disposition.
package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestFoundationReadOutcomeAdapterControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/types.go": `package storage
 type NodeID string
 type Node struct{}
 type Engine interface{GetNode(NodeID)(*Node,error);OpaqueCounter()(int64,error)}
 `,
		"plugins/unfamiliar/outcomes.go": `package unfamiliar
 import "github.com/orneryd/nornicdb/pkg/storage"
 type Reader func(storage.NodeID)(*storage.Node,error)
 func opaque(e storage.Engine)*storage.Node{n,_:=e.GetNode("n");return n}
 func renamed(f Reader)*storage.Node{n,_:=f("n");return n}
 func scalar(e storage.Engine)int64{n,_:=e.OpaqueCounter();return n}
 func skipped(e storage.Engine)(*storage.Node,error){for{n,err:=e.GetNode("n");if err!=nil{continue};return n,nil}}
 func skippedInit(e storage.Engine)(*storage.Node,error){for{if _,err:=e.GetNode("n");err!=nil{continue};return nil,nil}}
 func propagated(e storage.Engine)(*storage.Node,error){return e.GetNode("n")}
 func sameName()string{return "ordinary service"}
 `,
	}
	for name, body := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(body), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	var adapters []capabilitycensus.Site
	for _, site := range result.Mechanisms {
		if site.Population == "read-outcome-boundaries" {
			adapters = append(adapters, site)
		}
	}
	require.Len(t, adapters, 5)
	require.NotEmpty(t, capabilitycensus.CoverageProblems(adapters, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(adapters, adapters), "recovery meaning remains OPEN until individually read")
}

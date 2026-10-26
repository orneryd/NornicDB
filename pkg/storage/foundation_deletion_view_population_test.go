// Author: Lane #1644 (Codex). Type-resolved method-set control includes
// interface promotions and pointer aliases, with no spelling-based labels.
package storage_test

import (
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFoundationDeletionViewControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26\n",
		"pkg/storage/engine.go": `package storage
type NodeID string
type EdgeID string
type Engine interface{Erase(NodeID)error;Many([]NodeID)error;Remove(EdgeID)error;Several([]EdgeID)error}
type Native struct{}
func(Native)Erase(NodeID)error{return nil}
func(Native)Many([]NodeID)error{return nil}
func(Native)Remove(EdgeID)error{return nil}
func(Native)Several([]EdgeID)error{return nil}
`,
		"plugins/unfamiliar/view.go": `package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
type Renamed struct{storage.Engine}
func(*Renamed)Erase(storage.NodeID)error{return nil}
type Alias=*Renamed
type Pretender struct{}
func(Pretender)Erase(string)error{return nil}
func(Pretender)Many([]string)error{return nil}
`,
	}
	for file, source := range files {
		path := filepath.Join(root, file)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	members := capabilitycensus.EngineDeletionViews(result)
	require.Len(t, members, 3, "native, unfamiliar interface-promoting view and pointer alias; unrelated spelling is excluded")
	found := false
	for _, member := range members {
		require.Len(t, member.Evidence.Callees, 4)
		require.Empty(t, member.Disposition)
		require.Empty(t, member.Reason)
		if strings.HasSuffix(member.Function, ".Renamed") {
			found = true
			require.Contains(t, member.Contract, ".Renamed).Erase")
			require.Contains(t, member.Contract, ".Engine).Many")
		}
	}
	require.True(t, found)
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members), "method-set facts cannot approve themselves")
}

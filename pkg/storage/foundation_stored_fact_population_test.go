// Author: Lane #1644 (Codex). A new compiled direct consumer fails the same
// canonical body-registration gate as existing producers and view adapters.
package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestFoundationStoredFactOwnerControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26\n",
		"pkg/storage/facts.go": `package storage
type Engine interface{Core()}
type StoredNodeFact struct{Exists bool}
type StoredMetadata struct{Nodes []StoredNodeFact}
func ReadStoredMetadata()(StoredMetadata,error){return StoredMetadata{},nil}
`,
		"plugins/unfamiliar/consumer.go": `package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
type Alias=storage.StoredMetadata
func Renamed(value Alias)bool{return value.Nodes[0].Exists}
func Read()error{_,err:=storage.ReadStoredMetadata();return err}
func Local(value struct{Nodes []struct{Exists bool}})bool{return value.Nodes[0].Exists}
`,
	}
	for file, source := range files {
		path := filepath.Join(root, file)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	members := capabilitycensus.StoredFactOwners(inventory)
	require.Len(t, members, 3, "the producer plus two real consumers; same-shaped local fields are not stored facts")
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members), "discovery never supplies semantic approval")
	for _, member := range members {
		require.Empty(t, member.Disposition)
		require.Empty(t, member.Reason)
		require.NotEmpty(t, member.Evidence.Callees)
	}
}

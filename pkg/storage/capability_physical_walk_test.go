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

func TestCapabilityPhysicalWalkOutsidePackageControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
type Engine interface{Core()}
type EngineUnwrapper interface{GetInnerEngine()Engine}
`,
		"plugins/unfamiliar/physical.go": `package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
func direct(e storage.Engine){for e!=nil{source,ok:=e.(storage.EngineUnwrapper);if !ok{return};e=source.GetInnerEngine()}}
func aliased(e storage.Engine){for e!=nil{source,ok:=e.(storage.EngineUnwrapper);if !ok{return};next:=source.GetInnerEngine;e=next()}}
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
	problems := capabilitycensus.Problems(result.Sites)
	for _, owner := range []string{"unfamiliar.direct/", "unfamiliar.aliased/"} {
		require.Contains(t, strings.Join(problems, "\n"), owner, "direct calls and a method value in an unfamiliar new file must use the same structural owner")
	}
	require.NotEmpty(t, capabilitycensus.CoverageProblems(result.Sites, result.Sites), "a new discovered source never approves its own semantic disposition")
}

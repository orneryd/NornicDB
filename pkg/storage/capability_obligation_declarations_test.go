package storage_test

// Author: Lane #1644 (Codex). This control exercises compiled obligation
// membership, independently of the behavioral adapters it indexes.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/stretchr/testify/require"
)

func TestCapabilityCompiledObligationDeclarations(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module obligationcontrol\ngo 1.26.0\n",
		"probe.go": `package obligationcontrol
import "testing"
func TestProduction(t *testing.T) {}
`,
		"probe_test.go": `package obligationcontrol
import "testing"
func TestVisible(t *testing.T) {}
func Testament(t *testing.T) {}
type helper struct{}
func (helper) TestMethod(t *testing.T) {}
`,
		"external_test.go": `package obligationcontrol_test
import "testing"
func TestExternal(t *testing.T) {}
`,
		"inactive_windows_test.go": `//go:build windows

package obligationcontrol
import "testing"
func TestInactive(t *testing.T) {}
`,
		"engine_population_contract_test.go": `package obligationcontrol
var controlRegistry = map[string]struct{Factory func(); Oracle func()}{
 "new-engine": {Factory: func(){}, Oracle: func(){}},
}
`,
		"testdata/invalid_test.go": "This is uncompiled fixture data, not Go source.",
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	first, err := capabilitycensus.TestObligations(root)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"TestVisible": true, "TestExternal": true}, first.Names)
	require.NotEmpty(t, first.Registry.Members)
	for _, member := range first.Registry.Members {
		require.Equal(t, "engine_population_contract_test.go", member.File)
	}
	// A new compiled file outside the first package must be found on the next
	// query. A previous declaration inventory must not hide the new member.
	other := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(other, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(other, "new_test.go"), []byte(`package outside
import "testing"
func TestNewMember(t *testing.T) {}
`), 0644))
	second, err := capabilitycensus.TestObligations(root)
	require.NoError(t, err)
	require.True(t, second.Names["TestNewMember"])
	require.Len(t, second.Names, 3)
}

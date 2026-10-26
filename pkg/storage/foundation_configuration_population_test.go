// Author: Lane #1644 (Codex). An unfamiliar writer outside storage must be
// selected by actual field identity, while a same-named other router is excluded.
package storage_test

import (
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestFoundationConfigurationWriterIdentity(t *testing.T) {
	root := t.TempDir()
	write := func(path, body string) {
		name := filepath.Join(root, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(name), 0755))
		require.NoError(t, os.WriteFile(name, []byte(body), 0644))
	}
	write("go.mod", "module github.com/orneryd/nornicdb\ngo 1.26.0\n")
	write("pkg/storage/engine.go", `package storage
 type Engine interface{Core()}
 `)
	write("pkg/multidb/router.go", `package multidb
 type LabelRouting struct{labelMap map[string][]string}
 type Other struct{labelMap map[string][]string}
 `)
	discover := func() []capabilitycensus.Site {
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		result, err := capabilitycensus.Interpret(inventory)
		require.NoError(t, err)
		var members []capabilitycensus.Site
		for _, site := range result.Mechanisms {
			if site.Population == "configuration-handoffs" {
				members = append(members, site)
			}
		}
		return members
	}
	require.Empty(t, discover())
	write("pkg/multidb/unfamiliar_writer.go", `package multidb
 func opaque(r *LabelRouting,v []string){r.labelMap["doc"]=v}
 func SetLabelRouting(r *Other,v []string){r.labelMap["doc"]=v}
 `)
	members := discover()
	require.Len(t, members, 1)
	require.Equal(t, "pkg/multidb/unfamiliar_writer.go", members[0].Path)
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members), "an unread new writer cannot approve itself")
}

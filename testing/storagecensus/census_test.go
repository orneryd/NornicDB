package storagecensus

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// root is the module root, two directories above this package.
var root = filepath.Join("..", "..")

var (
	censusOnce   sync.Once
	censusReport *Report
	censusErr    error
)

func census(t *testing.T) *Report {
	t.Helper()
	censusOnce.Do(func() { censusReport, censusErr = Census(root, TheSpec) })
	require.NoError(t, censusErr)
	return censusReport
}

const artifact = "docs/generated/stored-entity-census"

// TestStoredEntityCensusIsCurrent regenerates the census and compares it with
// the committed artifact. NORNICDB_CENSUS_UPDATE=1 writes it instead;
// NORNICDB_CENSUS_PRINT=1 also prints it.
func TestStoredEntityCensusIsCurrent(t *testing.T) {
	r := census(t)
	js, err := r.JSON()
	require.NoError(t, err)
	md := r.Markdown()
	if os.Getenv("NORNICDB_CENSUS_PRINT") != "" {
		fmt.Printf("CENSUS-JSON-BEGIN\n%sCENSUS-JSON-END\nCENSUS-MD-BEGIN\n%sCENSUS-MD-END\n", js, md)
	}
	for ext, want := range map[string][]byte{".json": js, ".md": md} {
		path := filepath.Join(root, filepath.FromSlash(artifact+ext))
		if os.Getenv("NORNICDB_CENSUS_UPDATE") != "" {
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, want, 0o644))
			continue
		}
		got, err := os.ReadFile(path)
		require.NoError(t, err, "the census artifact is missing: regenerate it")
		got = bytes.ReplaceAll(got, []byte("\r\n"), []byte("\n"))
		require.True(t, bytes.Equal(got, want), "%s is stale: regenerate it (NORNICDB_CENSUS_UPDATE=1)", artifact+ext)
	}
}

// TestStoredEntityEffectsPassTheirOwners fails on an effect that bypasses its
// owner, and on an owner or exemption row that no longer matches the code.
func TestStoredEntityEffectsPassTheirOwners(t *testing.T) {
	r := census(t)
	require.Empty(t, r.Violations, "effects that bypass their owner:\n  %s", strings.Join(r.Violations, "\n  "))
	require.Empty(t, r.Stale, "owner and exemption rows that no longer match the code:\n  %s", strings.Join(r.Stale, "\n  "))
}

// copyTree copies the analysed packages' non-test sources under dst.
func copyTree(t *testing.T, dst string) {
	t.Helper()
	for _, rel := range Packages {
		paths, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(rel), "*.go"))
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Join(dst, filepath.FromSlash(rel)), 0o755))
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			src, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dst, filepath.FromSlash(rel), filepath.Base(path)), src, 0o644))
		}
	}
}

// TestOwnerGuardCatchesMutations adds, to a copy of the sources, each kind of
// bypass the census forbids, and requires the census to report it.
func TestOwnerGuardCatchesMutations(t *testing.T) {
	mutations := []struct {
		name, pkg, src, want string
	}{
		{
			name: "direct counter update", pkg: "pkg/storage",
			src:  "package storage\n\nfunc (b *BadgerEngine) CensusMutantCounter() { b.nodeCount.Add(1) }\n",
			want: "counter: storage.BadgerEngine.CensusMutantCounter",
		},
		{
			name: "hidden-node replace that skips the owner", pkg: "pkg/storage",
			src:  "package storage\n\nfunc (tx *BadgerTransaction) CensusMutantHiddenReplace(n *Node) {\n\tif tx.engine.hidesNode(visibleRead, n, 0) {\n\t\ttx.bufferSet(nodeKey(n.ID), nil)\n\t}\n}\n",
			want: "hidden: storage.BadgerTransaction.CensusMutantHiddenReplace",
		},
		{
			name: "unscoped quota read", pkg: "pkg/multidb",
			src:  "package multidb\n\nimport \"github.com/orneryd/nornicdb/pkg/storage\"\n\nfunc (m *DatabaseManager) CensusMutantQuota(e storage.Engine) int64 {\n\tn, _ := e.NodeCount()\n\treturn n\n}\n",
			want: "total: multidb.DatabaseManager.CensusMutantQuota",
		},
		{
			name: "write that skips the write order", pkg: "pkg/storage",
			src:  "package storage\n\nfunc (b *BadgerEngine) CensusMutantWrite() error {\n\treturn b.db.Update(nil)\n}\n",
			want: "write: storage.BadgerEngine.CensusMutantWrite reaches a write without an owner",
		},
		{
			name: "go statement that writes", pkg: "pkg/storage",
			src:  "package storage\n\nfunc (b *BadgerEngine) CensusMutantGo() {\n\tgo func() { _ = b.db.Update(nil) }()\n}\n",
			want: "write: storage.BadgerEngine.CensusMutantGo$go1 reaches a write without an owner",
		},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			copyTree(t, dir)
			require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(m.pkg), "zz_census_mutant.go"), []byte(m.src), 0o644))
			r, err := Census(dir, TheSpec)
			require.NoError(t, err)
			found := false
			for _, v := range r.Violations {
				if strings.HasPrefix(v, m.want) {
					found = true
				}
			}
			require.True(t, found, "the census did not report the %s; violations:\n  %s", m.name, strings.Join(r.Violations, "\n  "))
		})
	}
}

// Author: Lane #1644 (Codex). The registry is an individual semantic review,
// not a name-based classifier. New typed declarations cannot classify themselves.
package storage_test

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type legacyReadReview struct {
	Method        string `json:"method"`
	Class         string `json:"class"`
	Peer          string `json:"peer"`
	Reason        string `json:"reason"`
	Author        string `json:"author"`
	DeclarationID string `json:"declaration_id"`
	Digest        string `json:"digest"`
	OwnerDigest   string `json:"owner_digest"`
}

func legacyReadReviews(t *testing.T) map[string]legacyReadReview {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "foundation-legacy-read-apis.json"))
	require.NoError(t, err)
	var rows []legacyReadReview
	require.NoError(t, json.Unmarshal(raw, &rows))
	reviews := map[string]legacyReadReview{}
	for _, row := range rows {
		require.NotEmpty(t, row.Method)
		require.NotEmpty(t, row.Author)
		require.NotEmpty(t, row.Reason)
		_, duplicate := reviews[row.Method]
		require.False(t, duplicate, row.Method)
		reviews[row.Method] = row
	}
	return reviews
}
func TestFoundationLegacyReadPopulation(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	reviews := legacyReadReviews(t)
	actual := map[string]bool{}
	for _, api := range result.ErrorlessReadAPIs {
		actual[api.Method] = true
		row, ok := reviews[api.Method]
		require.True(t, ok, "unread errorless API: %s", api.Method)
		require.Contains(t, []string{"legacy compatibility", "distinct responsibility"}, row.Class, "OPEN member: %s", api.Method)
		require.NotEmpty(t, api.Declaration.ID, "definition must come from canonical engine: %s", api.Method)
		require.Equal(t, api.Declaration.ID, row.DeclarationID, "stale declaration review: %s", api.Method)
		require.Equal(t, api.Declaration.Digest, row.Digest, "stale declaration source: %s", api.Method)
		require.Equal(t, api.Declaration.OwnerDigest, row.OwnerDigest, "changed API body needs another individual reading: %s", api.Method)
		if row.Class == "legacy compatibility" {
			require.NotEmpty(t, row.Peer, "the error channel already exists: %s", api.Method)
		}
	}
	for method := range reviews {
		require.True(t, actual[method], "stale API review: %s", method)
	}
	for _, use := range capabilitycensus.ErrorlessReadReferences(result) {
		if reviews[use.API].Class != "legacy compatibility" {
			continue
		}
		require.Equal(t, "legacy compatibility", reviews[use.Owner].Class, "internal read consumer uses errorless API %s at %s; only public compatibility implementations may forward it", use.API, use.Member.ID)
	}
	t.Logf("%d typed errorless declarations, each individually reviewed", len(actual))
}

// Make every reviewed legacy declaration unavailable at compile time. Only its
// compatibility implementation is renamed along with it. Ordinary consumers
// retain their original call and therefore become precise compiler failures.
func TestFoundationLegacyReadCompilerPopulation(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	reviews := legacyReadReviews(t)
	targets := map[*types.Func]bool{}
	for _, api := range result.ErrorlessReadAPIs {
		if reviews[api.Method].Class == "legacy compatibility" {
			targets[api.Object.Origin()] = true
		}
	}
	require.NotEmpty(t, targets)
	type replacement struct {
		start, end int
		text       string
	}
	edits := map[string]map[int]replacement{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		id, ok := member.Syntax.Node.(*ast.Ident)
		if !ok {
			continue
		}
		fn, ok := member.Syntax.Info.ObjectOf(id).(*types.Func)
		if !ok || !targets[fn.Origin()] {
			continue
		}
		declaration := member.Syntax.Info.Defs[id] != nil
		if !declaration {
			allowed := false
			for _, ancestor := range member.Syntax.Ancestors {
				if owner, ok := ancestor.(*ast.FuncDecl); ok {
					if fn, ok := member.Syntax.Info.Defs[owner.Name].(*types.Func); ok && reviews[fn.Origin().FullName()].Class == "legacy compatibility" {
						allowed = true
					}
				}
			}
			if !allowed {
				continue
			}
		}
		position := member.Syntax.FileSet.PositionFor(id.Pos(), false)
		if edits[position.Filename] == nil {
			edits[position.Filename] = map[int]replacement{}
		}
		edits[position.Filename][position.Offset] = replacement{position.Offset, position.Offset + len(id.Name), "FoundationUnavailable" + id.Name}
	}
	directory := t.TempDir()
	overlay := map[string]string{}
	for file, offsets := range edits {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		var positions []int
		for offset := range offsets {
			positions = append(positions, offset)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(positions)))
		for _, offset := range positions {
			edit := offsets[offset]
			body = append(append(append([]byte{}, body[:edit.start]...), []byte(edit.text)...), body[edit.end:]...)
		}
		target := filepath.Join(directory, strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(file))
		require.NoError(t, os.WriteFile(target, body, 0644))
		overlay[file] = target
	}
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	run := func() ([]byte, error) {
		raw, err := json.Marshal(map[string]any{"Replace": overlay})
		require.NoError(t, err)
		path := filepath.Join(directory, "overlay.json")
		require.NoError(t, os.WriteFile(path, raw, 0644))
		command := exec.Command("go", "build", "-overlay", path, "./...")
		command.Dir = root
		return command.CombinedOutput()
	}
	output, err := run()
	require.NoError(t, err, "all internal production consumers must build without the legacy APIs:\n%s", output)
	// A same-named unrelated API is harmless; the actual legacy method and
	// method-value uses in another package must be rejected by the compiler.
	control := filepath.Join(directory, "control.go")
	require.NoError(t, os.WriteFile(control, []byte(`package enginecontract
 import("github.com/orneryd/nornicdb/pkg/storage";"github.com/orneryd/nornicdb/pkg/cypher")
 type unrelated struct{}
 func(unrelated)GetAllNodes()string{return "ordinary method"}
 func legacyUnavailable(e storage.Engine,c *storage.CompositeEngine,x *cypher.StorageExecutor){
 _=unrelated{}.GetAllNodes()
 _=e.GetAllNodes();_=e.GetEdgeBetween("a","b","");_=e.GetInDegree("a");_=e.GetOutDegree("a")
 _=c.ConstituentDatabaseForNode("a");_=c.ConstituentDatabaseForEdge("r")
 f:=x.BatchGetNodes;_=f(nil)
 }
 `), 0644))
	overlay[filepath.Join(root, "internal", "enginecontract", "legacy_unavailable_control.go")] = control
	output, err = run()
	require.Error(t, err, "planted real consumers must fail the production build")
	for _, name := range []string{"GetAllNodes", "GetEdgeBetween", "GetInDegree", "GetOutDegree", "ConstituentDatabaseForNode", "ConstituentDatabaseForEdge", "BatchGetNodes"} {
		require.Contains(t, string(output), name, "compiler must enumerate the entire planted class: %s", output)
	}
	t.Logf("legacy class unavailable: production build passed; all seven planted API uses rejected")
}

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

// Author: Lane #1644 (Codex). The planted direct use must fail registration;
// a transitive caller and unrelated loops are context, never members.
func TestFoundationDirectMechanismPopulation(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
 import "context"
 type NodeID string
 type Node struct{}
 type Engine interface{GetNode(NodeID)(*Node,error)}
 type EngineUnwrapper interface{GetInnerEngine()Engine}
 type StoredEntityReader interface{GetStoredNode(NodeID)(*Node,error)}
 type ScalarFact interface{Scope()string}
 type CommitFact interface{LastDecision(context.Context)error}
 func UnwrapEngine(e Engine)Engine{return e}
 `,
		"plugins/unfamiliar/uses.go": `package unfamiliar
 import("context";"github.com/orneryd/nornicdb/pkg/storage")
 type renamedView struct{storage.Engine}
 func(*renamedView)Scope()string{return "scope"}
 func(*renamedView)LastDecision(context.Context)error{return nil}
 func(*renamedView)Close()error{return nil}
 func scalar(value any){_,_=value.(storage.ScalarFact)}
 func renamedCommit(value any){_,_=value.(interface{LastDecision(context.Context)error})}
 func sameSpelledNonContract(value any){_,_=value.(interface{LastDecision(string)string})}
 func nonStorageCloser(value interface{Embed(string)(string,error)}){_,_=value.(interface{Close()error})}
 func direct(e storage.Engine){_,_=e.(storage.StoredEntityReader)}
 func transitive(e storage.Engine){direct(e);for i:=0;i<4;i++{}}
 func raw(e storage.Engine)storage.Engine{return storage.UnwrapEngine(e)}
 func alias(e storage.Engine)storage.Engine{f:=storage.UnwrapEngine;return f(e)}
 func full(r storage.StoredEntityReader){_,_=r.GetStoredNode("n")}
 func UnwrapEngine(s string)string{return s}
 func sameName(){_=UnwrapEngine("ordinary service")}
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
	counts := map[string]int{}
	for _, site := range result.Mechanisms {
		require.Equal(t, "plugins/unfamiliar/uses.go", site.Path)
		require.False(t, strings.HasSuffix(site.Function, ".transitive"))
		require.False(t, strings.HasSuffix(site.Function, ".sameName"))
		require.False(t, strings.HasSuffix(site.Function, ".sameSpelledNonContract"))
		require.False(t, strings.HasSuffix(site.Function, ".nonStorageCloser"), "coincidental Close signature on a disjoint service is context")
		counts[site.Population]++
	}
	require.Equal(t, map[string]int{"direct-capability-assertions": 3, "raw-discovery-or-unwrap": 2, "direct-legacy-stored-reads": 1, "read-outcome-boundaries": 1}, counts)
	require.NotEmpty(t, capabilitycensus.CoverageProblems(result.Mechanisms, nil), "new direct member fails the ordinary shared manifest validator")
	require.NotEmpty(t, capabilitycensus.CoverageProblems(result.Mechanisms, result.Mechanisms), "discovery supplies no semantic approval")
}

// Author: Lane #1644 (Codex). The direct caller query is anchored to the actual
// Service method object; a same-named method on another service is not a member.
func TestFoundationSearchConsumerIdentity(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(body), 0644))
	}
	write("go.mod", "module github.com/orneryd/nornicdb\ngo 1.26.0\n")
	write("pkg/storage/engine.go", `package storage
 type NodeID string
 type Node struct{}
 type Engine interface{GetNode(NodeID)(*Node,error)}
 `)
	write("pkg/search/owner.go", `package search
 import "github.com/orneryd/nornicdb/pkg/storage"
 type Service struct{engine storage.Engine}
 func(*Service)filterByType()error{return nil}
 type Other struct{}
 func(*Other)filterByType()error{return nil}
 `)
	discover := func() []capabilitycensus.Site {
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		result, err := capabilitycensus.Interpret(inventory)
		require.NoError(t, err)
		var members []capabilitycensus.Site
		for _, site := range result.Mechanisms {
			if site.Population == "direct-search-read-consumers" {
				members = append(members, site)
			}
		}
		return members
	}
	require.Empty(t, discover())
	write("pkg/search/new_consumer.go", `package search
 import "github.com/orneryd/nornicdb/pkg/storage"
 func opaque(s *Service)error{return s.filterByType()}
 func sameName(s *Other)error{return s.filterByType()}
 func renamedWriter(s *Service){s.engine=nil}
 func(s *Service)SearchRelationships(id storage.NodeID)(*storage.Node,error){return s.engine.GetNode(id)}
 `)
	members := discover()
	require.Len(t, members, 3)
	require.Equal(t, "pkg/search/new_consumer.go", members[0].Path)
	var opaque, hydration bool
	for _, member := range members {
		opaque = opaque || strings.HasSuffix(member.Function, ".opaque")
		hydration = hydration || strings.HasSuffix(member.Function, ".SearchRelationships")
	}
	require.True(t, opaque)
	require.True(t, hydration)
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members))
}

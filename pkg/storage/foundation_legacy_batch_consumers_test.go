// Author: Lane #1644 (Codex). The public legacy signature is an explicit
// compatibility exception; production references inside this module are banned.
package storage_test

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func legacyExecutorBatchConsumers(inventory workcensus.Inventory) []string {
	var legacy *types.Func
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Package == nil || member.Syntax.Package.Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
			continue
		}
		if executor := member.Syntax.Package.Scope().Lookup("StorageExecutor"); executor != nil {
			if method := types.NewMethodSet(types.NewPointer(executor.Type())).Lookup(member.Syntax.Package, "BatchGetNodes"); method != nil {
				legacy, _ = method.Obj().(*types.Func)
				break
			}
		}
	}
	if legacy == nil {
		return []string{"legacy declaration was not resolved"}
	}
	var consumers []string
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if identifier, ok := member.Syntax.Node.(*ast.Ident); ok {
			if used, ok := member.Syntax.Info.Uses[identifier].(*types.Func); ok && used.Origin() == legacy.Origin() {
				consumers = append(consumers, member.ID)
			}
		}
	}
	sort.Strings(consumers)
	return consumers
}

func TestFoundationLegacyBatchHasNoInternalConsumers(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	require.Empty(t, legacyExecutorBatchConsumers(result.Inventory), "internal callers must preserve errors through the error-returning owner")
}

func TestFoundationLegacyBatchConsumerControl(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(body), 0644))
	}
	write("go.mod", "module github.com/orneryd/nornicdb\ngo 1.26.0\n")
	write("pkg/cypher/api.go", `package cypher
 type StorageExecutor struct{}
 func(*StorageExecutor)BatchGetNodes([]string)map[string]string{return nil}
 `)
	discover := func() []string {
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		return legacyExecutorBatchConsumers(inventory)
	}
	require.Empty(t, discover())
	write("plugins/unfamiliar/uses.go", `package unfamiliar
 import "github.com/orneryd/nornicdb/pkg/cypher"
 type Other struct{}
 func(*Other)BatchGetNodes([]string)map[string]string{return nil}
 func direct(e *cypher.StorageExecutor){_=e.BatchGetNodes(nil)}
 func alias(e *cypher.StorageExecutor){f:=e.BatchGetNodes;_=f(nil)}
 func sameName(e *Other){_=e.BatchGetNodes(nil)}
 `)
	consumers := discover()
	require.Len(t, consumers, 2, "direct and bound-method references are real consumers; the other receiver is not")
	for _, member := range consumers {
		require.Contains(t, member, "plugins/unfamiliar/uses.go")
	}
}

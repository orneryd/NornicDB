package storagecensus

import (
	"encoding/json"
	"fmt"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). This bounded domain query consumes
// the release census loader and VTA graph. It adds no flow analysis or approval
// policy. Files select the reconciled owners; every compiled body in them and
// every incoming compiled call is emitted, including closures and wrappers.
func TestResidualReleaseOwnerConsumers(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	plantPath := filepath.Join(abs, "internal", "testfixture", "residualconsumer", "consumer.go")
	const plant = `package residualconsumer
import "github.com/orneryd/nornicdb/pkg/storage"
func Read(b *storage.BadgerEngine, id storage.NodeID) (*storage.Node, error) { return b.GetNode(id) }
func Scalar(n int) int { return n }
`
	p, err := load(abs, map[string][]byte{plantPath: []byte(plant)})
	require.NoError(t, err)
	files := map[string]bool{}
	for _, name := range []string{
		"badger_cache.go", "badger_queries.go", "badger_nodes.go", "badger_edges.go",
		"property_codec.go", "property_encoding_writer.go", "property_ownership.go",
		"badger_publication_stage.go", "publication_constraint_view.go",
		"badger_transaction_receipt.go", "entity_authority_prototype.go",
		"prepared_entity_commit_prototype.go", "badger_write_behind.go",
		"stored_entity_publication.go", "schema_ownership.go", "schema_persistence.go",
	} {
		files["pkg/storage/"+name] = true
	}
	type row struct{ Kind, Symbol, Target, Position, Signature string }
	var rows []row
	packages := map[string]*types.Package{}
	controlFound := false
	for _, fn := range p.funcs {
		if fn.Pkg != nil {
			packages[fn.Pkg.Pkg.Path()] = fn.Pkg.Pkg
		}
		pos := p.fset.Position(fn.Pos())
		rel, err := filepath.Rel(abs, pos.Filename)
		if err != nil || !files[filepath.ToSlash(rel)] {
			continue
		}
		rows = append(rows, row{"owner", p.funcName(fn), "", p.pos(fn.Pos()), fn.Signature.String()})
		if node := p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.In {
				if edge.Site == nil {
					continue
				}
				caller := p.funcName(edge.Caller.Func)
				rows = append(rows, row{"caller", caller, p.funcName(fn), p.pos(edge.Site.Pos()), fn.Signature.String()})
				if strings.Contains(caller, "internal/testfixture/residualconsumer.Read") && fn.Name() == "GetNode" {
					controlFound = true
				}
			}
		}
	}
	require.True(t, controlFound, "compiled caller outside storage must be discovered")
	engine := packages[Module+"pkg/storage"].Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
	for path, pkg := range packages {
		if !strings.HasPrefix(path+"/", Module) {
			continue
		}
		for _, name := range pkg.Scope().Names() {
			obj, ok := pkg.Scope().Lookup(name).(*types.TypeName)
			if !ok || obj.IsAlias() {
				continue
			}
			typ, ok := obj.Type().(*types.Named)
			if !ok {
				continue
			}
			if _, abstract := typ.Underlying().(*types.Interface); abstract {
				continue
			}
			if types.Implements(typ, engine) || types.Implements(types.NewPointer(typ), engine) {
				rows = append(rows, row{"engine-implementer", strings.TrimPrefix(path, Module) + "." + name, "pkg/storage.Engine", p.pos(obj.Pos()), typ.String()})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Symbol != b.Symbol {
			return a.Symbol < b.Symbol
		}
		if a.Target != b.Target {
			return a.Target < b.Target
		}
		return a.Position < b.Position
	})
	result := struct {
		Author, Qualification string
		Rows                  []row
	}{"#1548 implementation lane (Codex)", "Compiled owner/caller/implementer inventory only; no callback, acquisition, context-binding or behavioral pass", rows}
	data, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	fmt.Printf("RESIDUAL-RELEASE-CONSUMERS-BEGIN\n%s\nRESIDUAL-RELEASE-CONSUMERS-END\n", data)
}

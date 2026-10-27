package storage

// Author: #1667 implementation lane (Codex). The foundation's first census
// client supplies selectors and a framing policy; workcensus owns extraction.

import (
	"encoding/json"
	"go/ast"
	"go/constant"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestNodeEnvelopeCompiledPopulation(t *testing.T) {
	dir, err := filepath.Abs("../..")
	require.NoError(t, err)
	c := workcensus.Config{
		Dir: dir, Patterns: []string{"./..."}, SourceOnly: true, IncludeSyntax: true,
		BuildFlags: []string{"-tags=noui,nolocalllm"},
		Env:        append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=readonly"),
		Selectors: []workcensus.Selector{
			{Kind: "varint", Node: "call", Packages: []string{"encoding/binary"}, Names: []string{"Uvarint", "Varint"}, Reachability: "all"},
			{Kind: "varint", Node: "call", Packages: []string{"github.com/orneryd/nornicdb/pkg/storage"}, Names: []string{"readUvarint"}, Reachability: "all"},
			{Kind: "format", Node: "binary", Operators: []string{"==", "!="}, Reachability: "all"},
		},
	}
	extract := func(c workcensus.Config) workcensus.Inventory {
		t.Helper()
		inv, err := workcensus.Extract(c)
		require.NoError(t, err)
		return inv
	}
	// Interpret only the engine-selected typed expressions. There is no client
	// parser, source traversal, call resolver, identity generator or loader.
	frames := func(inv workcensus.Inventory) workcensus.Inventory {
		headers := map[string]bool{}
		integer := func(expr ast.Expr, syntax *workcensus.GoSyntax, want int64) bool {
			value := syntax.Info.Types[expr].Value
			if value == nil || value.Kind() != constant.Int {
				return false
			}
			n, ok := constant.Int64Val(value)
			return ok && n == want
		}
		for _, m := range inv.Members {
			if m.Kind != "format" || m.Syntax == nil || m.Syntax.Info == nil {
				continue
			}
			n := m.Syntax.Node.(*ast.BinaryExpr)
			for _, pair := range [][2]ast.Expr{{n.X, n.Y}, {n.Y, n.X}} {
				index, ok := pair[0].(*ast.IndexExpr)
				if !ok || !integer(index.Index, m.Syntax, 0) || !integer(pair[1], m.Syntax, 16) {
					continue
				}
				typ := m.Syntax.Info.TypeOf(index)
				if typ != nil {
					basic, ok := typ.Underlying().(*types.Basic)
					if ok && basic.Kind() == types.Uint8 {
						headers[m.Owner] = true
					}
				}
			}
		}
		out := workcensus.Inventory{Roots: inv.Roots}
		for _, m := range inv.Members {
			if m.Kind == "varint" && headers[m.Owner] {
				out.Members = append(out.Members, m)
			}
		}
		return out
	}
	actual := extract(c)
	var varints []workcensus.Member
	for _, m := range actual.Members {
		if m.Kind == "varint" {
			varints = append(varints, m)
		}
	}
	raw, err := json.Marshal(varints)
	require.NoError(t, err)
	t.Log("ENVELOPE-VARINT-POPULATION-JSON " + string(raw))
	positive := frames(actual)
	require.Len(t, positive.Members, 1, "new framing owners require semantic review")
	require.True(t, strings.HasSuffix(positive.Members[0].Owner, ".splitNodeEnvelope"))
	approved := positive
	approved.Members = append([]workcensus.Member(nil), positive.Members...)
	approved.Members[0].Class = "shared framing owner"
	approved.Members[0].Reason = "The sole V2 header/varint owner validates bounds and returns borrowed property and metadata views; protocol consumers map its failure facts."
	policy := workcensus.Policy{Classes: []string{"shared framing owner"}, ClassKinds: map[string]string{"shared framing owner": "review"}, RequireOwnerDigest: true}
	require.Empty(t, workcensus.Validate(positive, approved, policy))
	// An arbitrary new parser in another compiled package must enter exactly
	// the same selectors, typed-fact interpretation and Validate as the owner.
	probe := filepath.Join(dir, "pkg", "util", "envelope_population_control.go")
	c.Overlay = map[string][]byte{probe: []byte(`package util
import unusual "encoding/binary"
func UnfamiliarEnvelope(data []byte) []byte {
 if len(data)==0 || data[0]!=16 { return nil }
 size,n:=unusual.Uvarint(data[1:]); if n<=0 || size>uint64(len(data)-1-n) { return nil }
 return data[1+n:1+n+int(size)]
}
`)}
	mutated := frames(extract(c))
	failures := strings.Join(workcensus.Validate(mutated, approved, policy), "\n")
	require.Contains(t, failures, "UNLISTED pkg/util/envelope_population_control.go:")
}

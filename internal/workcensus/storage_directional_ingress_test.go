package workcensus

// Author: #1667 implementation lane (Codex). This storage client queries the
// shared typed source facts, including arbitrary names and function values.

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func storageDirectionalIngressConfig(dir string) Config {
	return Config{Dir: dir, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, SourceOnly: true, IncludeSyntax: true, ContextFields: []string{"walk", "ctx", "context", "checkpoint"}, Selectors: []Selector{{Kind: "directional-ingress", Node: "call", Group: "directional-ingress", Reachability: "all", ResultTypes: []string{module + "pkg/storage.Edge"}, UnwrapResults: true}}}
}

// The direct mechanism is an adjacency-shaped operation: one or two anchored
// NodeID inputs and one or two Edge slices plus the returned error. Names do
// not decide membership; unresolved aliases remain in the typed population.
func storageDirectionalIngressCall(m Member) bool {
	if m.Syntax == nil || m.Syntax.Info == nil {
		return false
	}
	call, ok := m.Syntax.Node.(*ast.CallExpr)
	if !ok {
		return false
	}
	sig, ok := m.Syntax.Info.TypeOf(call.Fun).Underlying().(*types.Signature)
	if !ok {
		return false
	}
	if sig.Results().Len() < 2 || sig.Results().Len() > 3 || !types.Identical(sig.Results().At(sig.Results().Len()-1).Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	for i := 0; i < sig.Results().Len()-1; i++ {
		slice, ok := types.Unalias(sig.Results().At(i).Type()).Underlying().(*types.Slice)
		if !ok {
			return false
		}
		pointer, ok := types.Unalias(slice.Elem()).Underlying().(*types.Pointer)
		if !ok || typeString(types.Unalias(pointer.Elem())) != module+"pkg/storage.Edge" {
			return false
		}
	}
	anchors := 0
	for i := 0; i < sig.Params().Len(); i++ {
		if typeString(types.Unalias(sig.Params().At(i).Type())) == module+"pkg/storage.NodeID" {
			anchors++
		}
	}
	return anchors == 1 || anchors == 2
}

// This is a query of engine-owned declaration facts. SourceOnly deliberately
// omits context-access diagnostics, so receiver fields and execution provenance
// must not become a successful empty availability proof.
func storageDirectionalContextInputs(m Member, definitions map[string]Definition) []string {
	var out []string
	for owner := m.OwnerID; owner != ""; {
		d, ok := definitions[owner]
		if !ok {
			break
		}
		for _, v := range d.Values {
			if v.Role == "parameter" && v.Type == "context.Context" {
				out = append(out, v.Reference)
			}
		}
		owner = d.ParentID
	}
	return out
}

func TestStorageDirectionalIngressDiscovery(t *testing.T) {
	if !strings.Contains(flag.Lookup("test.run").Value.String(), "TestStorageDirectionalIngressDiscovery") {
		t.Skip("explicit all-source mechanism query required")
	}
	inv, err := Extract(storageDirectionalIngressConfig("../.."))
	if err != nil {
		t.Fatal(err)
	}
	definitions := storageDefinitions(inv)
	count, scoped := 0, 0
	for _, m := range inv.Members {
		if storageDirectionalIngressCall(m) {
			inputs := storageDirectionalContextInputs(m, definitions)
			if m.Facts == nil {
				m.Facts = map[string][]string{}
			}
			m.Facts["context input candidates"] = inputs
			m.Facts["context access mode"] = []string{"SourceOnly: lexical parameter definitions only; execution/receiver-access diagnostics omitted"}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("DIRECTIONAL_INGRESS " + string(raw))
			count++
			if len(inputs) > 0 {
				scoped++
			}
		}
	}
	raw, err := json.Marshal(inv.Limitations)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("DIRECTIONAL_LIMITATIONS " + string(raw))
	t.Logf("typed adjacency-shaped call sites %d; standard-context input candidates %d; all-source facts are not reachability or behavioral approval", count, scoped)
}

func TestStorageDirectionalIngressQueryControls(t *testing.T) {
	dir := t.TempDir()
	pkg := module + "pkg/storage"
	files := map[string]string{
		"go.mod": "module " + pkg + "\n\ngo 1.26\n",
		"entry.go": `package storage
import "context"
type NodeID string
type Edge struct{}
type idAlias=NodeID
type edgesAlias=[]*Edge
type reader struct{}
func(reader)Arbitrary(id NodeID)([]*Edge,error){return nil,nil}
func(reader)Contextual(ctx context.Context,id NodeID)([]*Edge,error){return nil,ctx.Err()}
func list(kind string)([]*Edge,error){return nil,nil}
func point(id NodeID)(*Edge,error){return nil,nil}
func batch(ids []NodeID)([]*Edge,error){return nil,nil}
func unrelated(a,b,c NodeID)([]*Edge,error){return nil,nil}
func Execute(ctx context.Context,id NodeID,provided func(idAlias)(edgesAlias,error)){
 alias:=reader{}.Arbitrary
 _,_=reader{}.Arbitrary(id)
 _,_=alias(id)
 _,_=reader{}.Contextual(ctx,id)
 _,_=provided(id)
 _,_=list("x")
 _,_=point(id)
 _,_=batch([]NodeID{id})
 _,_=unrelated(id,id,id)
}`,
		"plugins/foreign/reader.go": `package foreign
import("context";storage "` + pkg + `")
func Execute(ctx context.Context,id storage.NodeID,read func(storage.NodeID)([]*storage.Edge,error)){_,_=read(id)}`,
	}
	for name, raw := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inv := mustExtract(t, storageDirectionalIngressConfig(dir))
	definitions := storageDefinitions(inv)
	count, foreign, unknown := 0, false, false
	for _, m := range inv.Members {
		if !storageDirectionalIngressCall(m) {
			continue
		}
		if len(storageDirectionalContextInputs(m, definitions)) != 1 {
			t.Fatal("engine context-input facts missing", m.OwnerID)
		}
		count++
		foreign = foreign || strings.Contains(m.File, "plugins/foreign/")
		unknown = unknown || strings.Contains(m.Code, "provided(id)")
		if strings.Contains(m.Code, "list(") || strings.Contains(m.Code, "point(") || strings.Contains(m.Code, "batch(") || strings.Contains(m.Code, "unrelated(") {
			t.Fatal("unrelated signature admitted", m.Code)
		}
	}
	if count != 5 || !foreign || !unknown {
		t.Fatalf("same query lost direct/method-value/unknown/foreign mechanism: count=%d foreign=%v unknown=%v", count, foreign, unknown)
	}
}

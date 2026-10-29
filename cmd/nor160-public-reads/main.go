// Author: Codex, NOR-160 implementation lane, 29 October 2026.
// Contract-first domain query over the maintained workcensus typed/SSA loader.
// No receiver whitelist, package-private parser, loader, or product execution.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb"

type surface struct {
	Name      string   `json:"name"`
	Receiver  string   `json:"receiver,omitempty"`
	Signature string   `json:"signature"`
	File      string   `json:"file"`
	Line      int      `json:"line"`
	Reason    []string `json:"surface_reasons"`
	Callees   []string `json:"callees"`
	Callers   []string `json:"callers"`
	Source    string   `json:"source"`
	Hash      string   `json:"body_sha256"`
	ReadPath  []string `json:"opaque_read_path,omitempty"`
}

// Entity-bearing output types include arbitrarily nested result containers and
// visitor inputs. Interfaces are not expanded into their method sets: returning
// an engine is construction, not returning its future node/edge images.
func entities(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if a, ok := t.(*types.Alias); ok {
		return entities(types.Unalias(a), seen)
	}
	if n, ok := t.(*types.Named); ok {
		if p := n.Obj().Pkg(); p != nil && p.Path() == module+"/pkg/knowledgepolicy" && n.Obj().Name() == "EntityMeta" {
			return true
		}
		if p := n.Obj().Pkg(); p != nil && p.Path() == module+"/pkg/storage" {
			switch n.Obj().Name() {
			case "Node", "Edge", "EdgeHeader":
				return true
			}
		}
		return entities(n.Underlying(), seen)
	}
	switch x := t.(type) {
	case *types.Pointer:
		return entities(x.Elem(), seen)
	case *types.Slice:
		return entities(x.Elem(), seen)
	case *types.Array:
		return entities(x.Elem(), seen)
	case *types.Map:
		return entities(x.Key(), seen) || entities(x.Elem(), seen)
	case *types.Chan:
		return entities(x.Elem(), seen)
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if entities(x.At(i).Type(), seen) {
				return true
			}
		}
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if x.Field(i).Exported() && entities(x.Field(i).Type(), seen) {
				return true
			}
		}
	case *types.Signature:
		return entities(x.Params(), seen) || entities(x.Results(), seen)
	}
	return false
}

func referenceResult(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if a, ok := t.(*types.Alias); ok {
		return referenceResult(types.Unalias(a), seen)
	}
	if n, ok := t.(*types.Named); ok {
		return referenceResult(n.Underlying(), seen)
	}
	switch x := t.(type) {
	case *types.Map, *types.Slice, *types.Pointer, *types.Chan, *types.Signature:
		return true
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if referenceResult(x.At(i).Type(), seen) {
				return true
			}
		}
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if x.Field(i).Exported() && referenceResult(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func opaqueResult(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if a, ok := t.(*types.Alias); ok {
		return opaqueResult(types.Unalias(a), seen)
	}
	if n, ok := t.(*types.Named); ok {
		return opaqueResult(n.Underlying(), seen)
	}
	switch x := t.(type) {
	case *types.Interface:
		return x.NumMethods() == 0
	case *types.Pointer:
		return opaqueResult(x.Elem(), seen)
	case *types.Slice:
		return opaqueResult(x.Elem(), seen)
	case *types.Array:
		return opaqueResult(x.Elem(), seen)
	case *types.Map:
		return opaqueResult(x.Key(), seen) || opaqueResult(x.Elem(), seen)
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if opaqueResult(x.At(i).Type(), seen) {
				return true
			}
		}
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if x.Field(i).Exported() && opaqueResult(x.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func carriers(t types.Type, seen map[types.Type]bool, out map[string]bool) {
	if t == nil || seen[t] {
		return
	}
	seen[t] = true
	if a, ok := t.(*types.Alias); ok {
		carriers(types.Unalias(a), seen, out)
		return
	}
	if n, ok := t.(*types.Named); ok {
		if n.Obj().Pkg() != nil && strings.HasPrefix(n.Obj().Pkg().Path(), module) && opaqueResult(n, map[types.Type]bool{}) {
			out[n.Obj().Pkg().Path()+"."+n.Obj().Name()] = true
		}
		return
	}
	switch x := t.(type) {
	case *types.Pointer:
		carriers(x.Elem(), seen, out)
	case *types.Slice:
		carriers(x.Elem(), seen, out)
	case *types.Array:
		carriers(x.Elem(), seen, out)
	case *types.Map:
		carriers(x.Elem(), seen, out)
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			carriers(x.At(i).Type(), seen, out)
		}
	}
}

func reasons(name string, sig *types.Signature) []string {
	var out []string
	if entities(sig.Results(), map[types.Type]bool{}) {
		out = append(out, "entity-bearing returned value/container/iterator")
	}
	if entities(sig.Params(), map[types.Type]bool{}) && referenceResult(sig.Results(), map[types.Type]bool{}) {
		out = append(out, "entity input with reference-bearing output; classify transform versus retained read")
	}
	for i := 0; i < sig.Params().Len(); i++ {
		t := sig.Params().At(i).Type()
		if n, ok := t.(*types.Named); ok {
			t = n.Underlying()
		}
		if cb, ok := t.(*types.Signature); ok && entities(cb.Params(), map[types.Type]bool{}) {
			out = append(out, "entity-bearing visitor input")
		}
	}
	// Metadata has no static Node inside its map. This companion is an explicit
	// part of the public node read contracts, never excluded by result shape.
	if strings.Contains(name, "NodeEmbeddingMeta") || strings.Contains(name, "NodeMetadata") {
		out = append(out, "node metadata companion read")
	}
	return out
}

func unique(xs []string) []string {
	sort.Strings(xs)
	var out []string
	for _, x := range xs {
		if len(out) == 0 || out[len(out)-1] != x {
			out = append(out, x)
		}
	}
	return out
}

// targetTree stages an already-pushed revision from the lab's shared Git
// object store, without altering the tool checkout or copying its loader.
func targetTree(spec string) (string, func(), error) {
	if !strings.HasPrefix(spec,"git:") {return spec,func(){},nil}
	sha:=strings.TrimPrefix(spec,"git:")
	if len(sha)!=40 {return "",nil,fmt.Errorf("target must use a full commit SHA")}
	for _,c:=range sha {if !(c>='0'&&c<='9'||c>='a'&&c<='f'){return "",nil,fmt.Errorf("invalid target SHA")}}
	tree,err:=os.MkdirTemp("","nor160-source-"+sha[:8]+"-");if err!=nil{return "",nil,err}
	cleanup:=func(){_ = os.RemoveAll(tree)} // exactly the directory MkdirTemp created
	archive:=filepath.Join(tree,"source.tar")
	command:=exec.Command("git","archive","--format=tar","--output",archive,sha)
	command.Stderr=os.Stderr
	if err=command.Run();err!=nil{cleanup();return "",nil,err}
	command=exec.Command("tar","-xf",archive,"-C",tree);command.Stderr=os.Stderr
	if err=command.Run();err!=nil{cleanup();return "",nil,err}
	if err=os.Remove(archive);err!=nil{cleanup();return "",nil,err}
	return tree,cleanup,nil
}

func main() {
	if len(os.Args)!=3 {panic("usage: nor160-public-reads <tree|git:fullSHA> <output|->")}
	spec,output:=os.Args[1],os.Args[2]
	tree,cleanup,err:=targetTree(spec);if err!=nil{panic(err)};defer cleanup()
	config := workcensus.Config{Dir: tree, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Env: append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")}
	control := os.Getenv("NOR160_CENSUS_CONTROL") == "1"
	if control {
		// New virtual file in an already loaded package outside pkg/storage.
		// The maintained loader's supported overlay owns package discovery.
		path := filepath.Join(tree, "internal/testfixture/readownership/nor160_public_surface_control.go")
		config.Overlay = map[string][]byte{path: []byte("package readownership\nimport \"github.com/orneryd/nornicdb/pkg/storage\"\ntype NOR160PublicSurfaceControl struct{}\ntype NOR160EntityAlias = []*storage.Edge\nfunc (NOR160PublicSurfaceControl) ReadImages(visit func(*storage.Node) error) NOR160EntityAlias { return nil }\nfunc (NOR160PublicSurfaceControl) ReadErasedImages(e storage.Engine) any { n,_:=e.GetNode(\"control:a\");return n }\n")}
	}
	p, err := workcensus.LoadProgram(config)
	if err != nil {
		panic(err)
	}
	// Typed images can be erased into any/row containers at a public query or
	// adapter boundary. Retain those upstream consumers too; the graph is an
	// over-approximation and each witness still requires semantic classification.
	readPath := map[*ssa.Function][]string{}
	var queue []*ssa.Function
	for _, fn := range p.Functions {
		if fn.Parent() == nil && ast.IsExported(fn.Name()) && len(reasons(fn.Name(), fn.Signature)) > 0 {
			readPath[fn] = []string{fn.String()}
			queue = append(queue, fn)
		}
		// A public interface/function parameter can have no concrete VTA target.
		// Its compiled call signature still proves the typed image boundary.
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				if entities(common.Signature().Results(), map[types.Type]bool{}) {
					if _, exists := readPath[fn]; !exists {
						readPath[fn] = []string{fn.String(), "typed call: " + common.String()}
						queue = append(queue, fn)
					}
				}
			}
		}
	}
	expanded := 0
	expand := func() {
		for expanded < len(queue) {
			fn := queue[expanded]
			expanded++
			node := p.CallGraph.Nodes[fn]
			if node == nil {
				continue
			}
			for _, edge := range node.In {
				caller := edge.Caller.Func
				if _, ok := readPath[caller]; ok {
					continue
				}
				readPath[caller] = append([]string{caller.String()}, readPath[fn]...)
				queue = append(queue, caller)
			}
		}
	}
	expand()
	readCarriers := map[string]bool{}
	carrierRead := func(sig *types.Signature) bool {
		found := map[string]bool{}
		carriers(sig.Results(), map[types.Type]bool{}, found)
		for name := range found {
			if readCarriers[name] {
				return true
			}
		}
		return false
	}
	// Close over erased carrier types as well as callers: a cache replay can
	// return ExecuteResult without executing another engine read, and a stream
	// can expose another carrier after consuming that result through an interface.
	for {
		for fn := range readPath {
			if fn.Parent() == nil && ast.IsExported(fn.Name()) && opaqueResult(fn.Signature.Results(), map[types.Type]bool{}) {
				carriers(fn.Signature.Results(), map[types.Type]bool{}, readCarriers)
			}
		}
		before := len(queue)
		for _, fn := range p.Functions {
			if _, exists := readPath[fn]; exists {
				continue
			}
			for _, block := range fn.Blocks {
				for _, instruction := range block.Instrs {
					call, ok := instruction.(ssa.CallInstruction)
					if !ok {
						continue
					}
					if carrierRead(call.Common().Signature()) {
						readPath[fn] = []string{fn.String(), "erased carrier call: " + call.Common().String()}
						queue = append(queue, fn)
						break
					}
				}
				if _, exists := readPath[fn]; exists {
					break
				}
			}
		}
		expand()
		if len(queue) == before {
			break
		}
	}
	objectPaths := map[*types.Func][]string{}
	for fn, path := range readPath {
		if obj, ok := fn.Object().(*types.Func); ok {
			objectPaths[obj] = path
		}
	}
	rows := map[string]*surface{}
	functions := map[string]*ssa.Function{}
	for _, fn := range p.Functions {
		if fn.Parent() != nil || !ast.IsExported(fn.Name()) {
			continue
		}
		pos := p.FileSet.Position(fn.Pos())
		file, err := filepath.Rel(p.Root, pos.Filename)
		if err != nil || pos.Filename == "" || file == ".." || strings.HasPrefix(file, ".."+string(filepath.Separator)) {
			continue
		}
		why := reasons(fn.Name(), fn.Signature)
		if len(why) == 0 && len(readPath[fn]) > 0 && opaqueResult(fn.Signature.Results(), map[types.Type]bool{}) {
			why = append(why, "opaque public result with compiled path to typed image surface; provenance classification required")
		}
		if len(why) == 0 && carrierRead(fn.Signature) {
			why = append(why, "public accessor for a type-erased image carrier, including replay without a fresh typed read")
		}
		if len(why) == 0 {
			continue
		}
		key := fn.String()
		r := rows[key]
		if r == nil {
			r = &surface{Name: key, Signature: fn.Signature.String(), File: filepath.ToSlash(file), Line: pos.Line, Reason: why}
			if !entities(fn.Signature.Results(), map[types.Type]bool{}) && opaqueResult(fn.Signature.Results(), map[types.Type]bool{}) {
				r.ReadPath = readPath[fn]
			}
			if fn.Signature.Recv() != nil {
				r.Receiver = types.TypeString(fn.Signature.Recv().Type(), nil)
			}
			if syntax := fn.Syntax(); syntax != nil {
				a, b := p.FileSet.Position(syntax.Pos()), p.FileSet.Position(syntax.End())
				bytes, overlaid := config.Overlay[a.Filename]
				if !overlaid {
					var e error
					bytes, e = os.ReadFile(a.Filename)
					if e != nil {
						panic(e)
					}
				}
				r.Source = strings.ReplaceAll(string(bytes[a.Offset:b.Offset]), "\r\n", "\n")
				r.Hash = fmt.Sprintf("%x", sha256.Sum256([]byte(r.Source)))
			}
			rows[key] = r
			functions[key] = fn
		}
		if node := p.CallGraph.Nodes[fn]; node != nil {
			for _, e := range node.In {
				r.Callers = append(r.Callers, e.Caller.Func.String())
			}
			for _, e := range node.Out {
				r.Callees = append(r.Callees, e.Callee.Func.String())
			}
		}
	}
	type contract struct {
		Name            string   `json:"name"`
		Methods         []string `json:"methods"`
		Implementations []string `json:"implementations"`
	}
	var contracts []contract
	type declared struct {
		Name string
		Type types.Type
	}
	var concrete []declared
	for _, pkg := range p.Packages {
		if !strings.HasPrefix(pkg.PkgPath, module) {
			continue
		}
		for _, name := range pkg.Types.Scope().Names() {
			obj, ok := pkg.Types.Scope().Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			if _, ok := obj.Type().Underlying().(*types.Interface); !ok {
				concrete = append(concrete, declared{pkg.PkgPath + "." + name, obj.Type()})
			}
		}
	}
	type publicEntry struct {
		Receiver      string   `json:"receiver"`
		Method        string   `json:"method"`
		Owner         string   `json:"declared_owner"`
		PromotionPath []int    `json:"promotion_path"`
		Signature     string   `json:"signature"`
		File          string   `json:"file"`
		Line          int      `json:"line"`
		Reasons       []string `json:"surface_reasons"`
	}
	var entries []publicEntry
	for _, d := range concrete {
		// Method sets, rather than emitted SSA bodies alone, retain promoted
		// read entry points such as MemoryEngine and embedded adapters.
		for _, typ := range []types.Type{d.Type, types.NewPointer(d.Type)} {
			set := types.NewMethodSet(typ)
			for i := 0; i < set.Len(); i++ {
				sel := set.At(i)
				fn, ok := sel.Obj().(*types.Func)
				if !ok || !fn.Exported() {
					continue
				}
				sig := fn.Type().(*types.Signature)
				why := reasons(fn.Name(), sig)
				if len(why) == 0 && len(objectPaths[fn]) > 0 && opaqueResult(sig.Results(), map[types.Type]bool{}) {
					why = append(why, "opaque public result with compiled read provenance")
				}
				if len(why) == 0 && carrierRead(sig) {
					why = append(why, "public accessor for type-erased image carrier")
				}
				if len(why) == 0 {
					continue
				}
				pos := p.FileSet.Position(fn.Pos())
				file, _ := filepath.Rel(p.Root, pos.Filename)
				entries = append(entries, publicEntry{types.TypeString(typ, nil), fn.Name(), fn.FullName(), sel.Index(), sig.String(), filepath.ToSlash(file), pos.Line, why})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Receiver != entries[j].Receiver {
			return entries[i].Receiver < entries[j].Receiver
		}
		return entries[i].Method < entries[j].Method
	})
	for _, pkg := range p.Packages {
		if !strings.HasPrefix(pkg.PkgPath, module) {
			continue
		}
		for _, name := range pkg.Types.Scope().Names() {
			obj, ok := pkg.Types.Scope().Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			iface, ok := obj.Type().Underlying().(*types.Interface)
			if !ok {
				continue
			}
			iface.Complete()
			c := contract{Name: pkg.PkgPath + "." + name}
			for i := 0; i < iface.NumMethods(); i++ {
				m := iface.Method(i)
				if len(reasons(m.Name(), m.Type().(*types.Signature))) > 0 || carrierRead(m.Type().(*types.Signature)) {
					c.Methods = append(c.Methods, m.Name()+m.Type().String())
				}
			}
			if len(c.Methods) == 0 {
				continue
			}
			for _, d := range concrete {
				if types.Implements(d.Type, iface) || types.Implements(types.NewPointer(d.Type), iface) {
					c.Implementations = append(c.Implementations, d.Name)
				}
			}
			c.Methods = unique(c.Methods)
			c.Implementations = unique(c.Implementations)
			contracts = append(contracts, c)
		}
	}
	var ordered []*surface
	for _, r := range rows {
		r.Callees = unique(r.Callees)
		r.Callers = unique(r.Callers)
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	sort.Slice(contracts, func(i, j int) bool { return contracts[i].Name < contracts[j].Name })
	data := struct {
		Author    string        `json:"author"`
		Tree      string        `json:"tree"`
		TargetSpec string `json:"target_spec"`
		Scope string `json:"scope"`
		Loader    string        `json:"loader"`
		Loaded    int           `json:"loaded_functions"`
		Method    string        `json:"method"`
		Control   bool          `json:"virtual_outside_package_control"`
		Contracts []contract    `json:"contracts"`
		Entries   []publicEntry `json:"public_method_entries"`
		Surfaces  []*surface    `json:"surfaces"`
	}{"Codex, NOR-160 implementation lane, 29 October 2026", tree, spec, "Nornic2: storage public read surface, every engine/adapter/wrapper/transaction/buffer getter; abstract calls and erased carriers inside storage. Above-storage caches, replay pages, search materialization/continuations and APOC-owned images are OUT OF SCOPE. Broad discovery candidates require per-member classification.", "maintained internal/workcensus.LoadProgram at b013bdfd4b8064725d63b29f3d2e9467408b3ae7", len(p.Functions), "Typed native storage-image results/containers/iterators/visitor inputs, metadata companions, public method sets/promotions, abstract typed call sites and erased carrier/replay discovery. No receiver whitelist. Non-read producers and above-storage consumers remain explicit candidates, not fixes.", control, contracts, entries, ordered}
	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		panic(err)
	}
	if output=="-" {
		fmt.Println("NOR160_CENSUS_JSON_BEGIN")
		if _,err=os.Stdout.Write(append(bytes,'\n'));err!=nil{panic(err)}
		fmt.Println("NOR160_CENSUS_JSON_END")
	} else if err = os.WriteFile(output, append(bytes, '\n'), 0644); err != nil {
		panic(err)
	}
	fmt.Printf("loaded=%d contracts=%d public_surface_candidates=%d\n", len(p.Functions), len(contracts), len(ordered))
}

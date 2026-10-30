// Author: Claude (NOR-180 lane), 30 October 2026.
// Behaviour enumeration (Rule 73 course change after the bbc976761 review):
// every function in the module that composes more than one error or selects a
// Neo4j status code, found by what its compiled SSA does, not by names.
// A domain client of #1667's canonical engine (internal/workcensus.LoadProgram);
// it performs no product or test execution.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb/"

type inputs struct {
	Author, SubjectHead, ArchiveSHA256 string
}

type finding struct {
	Kind, Position, Detail string
}

type member struct {
	Function, Package, Position, Signature string
	Test                                   bool
	Findings                               []finding
}

var statusCode = regexp.MustCompile(`^Neo\.(ClientError|DatabaseError|TransientError)\.[A-Za-z]+\.[A-Za-z]+$`)

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

var errorType = types.Universe.Lookup("error").Type()

func isError(t types.Type) bool { return types.Identical(t, errorType) }

func errorParams(sig *types.Signature) int {
	n := 0
	for i := 0; i < sig.Params().Len(); i++ {
		t := sig.Params().At(i).Type()
		if isError(t) {
			n++
		} else if s, ok := t.(*types.Slice); ok && isError(s.Elem()) {
			n += 2 // a variadic or slice of errors composes any number
		}
	}
	return n
}

// statusProbe reports whether t is an interface asking an error for its own
// Neo4j status (a BoltErrorCode method).
func statusProbe(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	iface, ok := t.Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for i := 0; i < iface.NumMethods(); i++ {
		if iface.Method(i).Name() == "BoltErrorCode" {
			return true
		}
	}
	return false
}

// renderProbe reports whether t asks an error for what a client is shown:
// its status (BoltErrorCode), its own message (StatusMessage), a diagnostic
// (StatusDiagnostic), a terminal outcome view, or a localized primary.
func renderProbe(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if named, ok := t.(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == module+"pkg/localization" && named.Obj().Name() == "LocalizedError" {
		return true
	}
	iface, ok := t.Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for i := 0; i < iface.NumMethods(); i++ {
		switch iface.Method(i).Name() {
		case "BoltErrorCode", "StatusMessage", "StatusDiagnostic", "terminalOutcomeView":
			return true
		}
	}
	return false
}

// methodNamed reports whether t is an interface (or pointer to one) with a
// method of the given name.
func methodNamed(t types.Type, name string) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	iface, ok := t.Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for i := 0; i < iface.NumMethods(); i++ {
		if iface.Method(i).Name() == name {
			return true
		}
	}
	return false
}

func hasErrorParam(sig *types.Signature) bool {
	for i := 0; i < sig.Params().Len(); i++ {
		if isError(sig.Params().At(i).Type()) {
			return true
		}
	}
	return false
}

func returnsString(sig *types.Signature) bool {
	for i := 0; i < sig.Results().Len(); i++ {
		if b, ok := sig.Results().At(i).Type().Underlying().(*types.Basic); ok && b.Kind() == types.String {
			return true
		}
	}
	return false
}

func returnsError(sig *types.Signature) bool {
	for i := 0; i < sig.Results().Len(); i++ {
		if isError(sig.Results().At(i).Type()) {
			return true
		}
	}
	return false
}

// variadicArity is the number of elements SSA packed into a variadic slice
// argument, or -1 when the slice is passed through (spread).
func variadicArity(v ssa.Value) int {
	s, ok := v.(*ssa.Slice)
	if !ok {
		return -1
	}
	alloc, ok := s.X.(*ssa.Alloc)
	if !ok {
		return -1
	}
	if p, ok := alloc.Type().(*types.Pointer); ok {
		if a, ok := p.Elem().(*types.Array); ok {
			return int(a.Len())
		}
	}
	return -1
}

func extract(archive []byte, root string) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		panic(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(e)
		}
		clean := filepath.Clean(h.Name)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			panic("archive path escapes subject")
		}
		path := filepath.Join(root, clean)
		if h.Typeflag == tar.TypeDir {
			if e = os.MkdirAll(path, 0755); e != nil {
				panic(e)
			}
			continue
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
			panic(e)
		}
		f, e := os.Create(path)
		if e != nil {
			panic(e)
		}
		_, ce := io.Copy(f, tr)
		if fe := f.Close(); ce != nil || fe != nil {
			panic(fmt.Sprint(ce, fe))
		}
	}
	if err = gz.Close(); err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) != 4 {
		panic("usage: behaviour <subject.tar.gz> <inputs.json> <default|antlr>")
	}
	archive, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	raw, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	var in inputs
	if err = json.Unmarshal(raw, &in); err != nil {
		panic(err)
	}
	if digest(archive) != in.ArchiveSHA256 {
		panic("subject archive hash mismatch")
	}
	root, err := os.MkdirTemp("", "nor180-r6-subject-")
	if err != nil {
		panic(err)
	}
	extract(archive, root)
	subject := filepath.Join(root, "subject")
	// Planted control outside every owner package: it composes two errors three
	// ways, selects a status code, provides a status by type, defaults a status
	// when an error has none, and renders diagnostics with a dispatch of its own.
	// It also discovers a terminal outcome carrier with a walk of its own.
	// The enumeration must report all nine kinds.
	control := "// Analysis-only planted control, never a product change.\npackage nor180behaviourcontrol\n\nimport (\n\t\"errors\"\n\t\"fmt\"\n)\n\n" +
		"func Join(primary, cleanup error) error { return errors.Join(primary, cleanup) }\n\n" +
		"func Wrap(primary, cleanup error) error { return fmt.Errorf(\"%w: %w\", primary, cleanup) }\n\n" +
		"type pair struct{ a, b error }\n\nfunc (p pair) Error() string { return p.a.Error() }\n\nfunc (p pair) Unwrap() []error { return []error{p.a, p.b} }\n\n" +
		"type statusErr struct{}\n\nfunc (statusErr) Error() string { return \"control\" }\n\nfunc (statusErr) BoltErrorCode() string { return \"Neo.ClientError.Statement.SyntaxError\" }\n\n" +
		"type diagnostic interface{ StatusDiagnostic() string }\n\n" +
		"type carrier interface{ terminalOutcomeView() string }\n\n" +
		"func Find(err error) bool {\n\tfor err != nil {\n\t\tif _, ok := err.(carrier); ok {\n\t\t\treturn true\n\t\t}\n\t\tw, ok := err.(interface{ Unwrap() error })\n\t\tif !ok {\n\t\t\treturn false\n\t\t}\n\t\terr = w.Unwrap()\n\t}\n\treturn false\n}\n\n" +
		"func Render(err error) string {\n\tvar d diagnostic\n\tif errors.As(err, &d) {\n\t\treturn err.Error() + \"\\n\" + d.StatusDiagnostic()\n\t}\n\treturn err.Error()\n}\n\n" +
		"func Status(err error) string {\n\tvar coder interface{ BoltErrorCode() string }\n\tif errors.As(err, &coder) {\n\t\treturn coder.BoltErrorCode()\n\t}\n\treturn \"Neo.DatabaseError.Transaction.TransactionRollbackFailed\"\n}\n"
	controlPath := filepath.Join(subject, "internal", "nor180behaviourcontrol", "control.go")
	if err = os.MkdirAll(filepath.Dir(controlPath), 0755); err != nil {
		panic(err)
	}
	if err = os.WriteFile(controlPath, []byte(control), 0644); err != nil {
		panic(err)
	}
	program, err := workcensus.LoadProgram(workcensus.Config{Dir: subject, Patterns: []string{"./..."}, LoadMode: packages.LoadAllSyntax, BuildFlags: []string{"-mod=readonly", "-tags=noui,nolocalllm"}, Env: append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off", "GOFLAGS=-p=2", "NORNICDB_PARSER="+os.Args[3])})
	if err != nil {
		panic(err)
	}
	position := func(pos token.Pos) string {
		at := program.FileSet.Position(pos)
		if at.Filename == "" {
			return "-"
		}
		rel, e := filepath.Rel(subject, at.Filename)
		if e != nil {
			return at.Filename
		}
		return filepath.ToSlash(rel) + fmt.Sprintf(":%d:%d", at.Line, at.Column)
	}
	members := map[string]*member{}
	add := func(fn *ssa.Function, f finding) {
		key := fn.String()
		m := members[key]
		if m == nil {
			pos := position(fn.Pos())
			pkg := ""
			if fn.Pkg != nil {
				pkg = fn.Pkg.Pkg.Path()
			}
			m = &member{Function: key, Package: strings.TrimPrefix(pkg, module), Position: pos, Signature: fn.Signature.String(), Test: strings.Contains(pos, "_test.go")}
			members[key] = m
		}
		m.Findings = append(m.Findings, f)
	}
	// Scan the loaded functions plus every method of every named type in the
	// module (T and *T), so a type that nothing calls is enumerated too.
	all := append([]*ssa.Function(nil), program.Functions...)
	for _, pkg := range program.SSA.AllPackages() {
		if pkg.Pkg == nil || !strings.HasPrefix(pkg.Pkg.Path()+"/", module) {
			continue
		}
		for _, m := range pkg.Members {
			t, ok := m.(*ssa.Type)
			if !ok {
				continue
			}
			for _, typ := range []types.Type{t.Type(), types.NewPointer(t.Type())} {
				set := program.SSA.MethodSets.MethodSet(typ)
				for i := 0; i < set.Len(); i++ {
					if fn := program.SSA.MethodValue(set.At(i)); fn != nil {
						all = append(all, fn)
					}
				}
			}
		}
	}
	for i := 0; i < len(all); i++ {
		if all[i] != nil {
			all = append(all, all[i].AnonFuncs...)
		}
	}
	seen := map[*ssa.Function]bool{}
	edges := map[*ssa.Function][]*ssa.Function{}
	dispatch := map[*ssa.Function]bool{}
	carrierProbe, unwrapProbe := map[*ssa.Function]bool{}, map[*ssa.Function]bool{}
	for _, fn := range all {
		if fn == nil || seen[fn] || !strings.Contains(fn.String(), module) || fn.Synthetic != "" && fn.Synthetic != "bound" && fn.Synthetic != "package initializer" {
			continue
		}
		seen[fn] = true
		sig := fn.Signature
		if errorParams(sig) >= 2 && returnsError(sig) {
			add(fn, finding{"combiner-signature", position(fn.Pos()), sig.String()})
		}
		if fn.Name() == "Unwrap" && sig.Results().Len() == 1 {
			if s, ok := sig.Results().At(0).Type().(*types.Slice); ok && isError(s.Elem()) {
				add(fn, finding{"multi-error-type", position(fn.Pos()), sig.String()})
			}
		}
		// A status provider produces an outcome's Neo4j status by its type.
		if fn.Name() == "BoltErrorCode" && sig.Recv() != nil {
			add(fn, finding{"status-provider", position(fn.Pos()), types.TypeString(sig.Recv().Type(), nil)})
		}
		// A status default asks an error for its own status and otherwise
		// produces a status constant of its own (a fallback or zero value).
		probe, statusConst := false, false
		edges[fn] = append(edges[fn], fn.AnonFuncs...)
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				for _, op := range ins.Operands(nil) {
					if ref, ok := (*op).(*ssa.Function); ok {
						edges[fn] = append(edges[fn], ref)
					}
				}
				if ta, ok := ins.(*ssa.TypeAssert); ok && renderProbe(ta.AssertedType) {
					dispatch[fn] = true
				}
				// Terminal outcome discovery: testing a value for the carrier
				// method and walking Unwrap in the same function.
				if ta, ok := ins.(*ssa.TypeAssert); ok {
					if methodNamed(ta.AssertedType, "terminalOutcomeView") {
						carrierProbe[fn] = true
					}
					if methodNamed(ta.AssertedType, "Unwrap") {
						unwrapProbe[fn] = true
					}
				}
				for _, op := range ins.Operands(nil) {
					if c, ok := (*op).(*ssa.Const); ok && c.Value != nil && c.Value.Kind() == constant.String {
						if s := constant.StringVal(c.Value); statusCode.MatchString(s) {
							add(fn, finding{"status-code", position(ins.Pos()), s})
							statusConst = true
						}
					}
				}
				if ta, ok := ins.(*ssa.TypeAssert); ok && statusProbe(ta.AssertedType) {
					probe = true
				}
				call, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				callee := common.StaticCallee()
				if callee == nil {
					continue
				}
				edges[fn] = append(edges[fn], callee)
				name := callee.String()
				if name == "errors.As" && len(common.Args) == 2 {
					target := common.Args[1]
					if mi, ok := target.(*ssa.MakeInterface); ok {
						target = mi.X
					}
					if statusProbe(target.Type()) {
						probe = true
					}
					if renderProbe(target.Type()) {
						dispatch[fn] = true
					}
				}
				switch {
				case name == "errors.Join":
					n := -1
					if len(common.Args) == 1 {
						n = variadicArity(common.Args[0])
					}
					add(fn, finding{"errors.Join", position(ins.Pos()), fmt.Sprintf("arity=%d", n)})
				case name == "fmt.Errorf":
					if len(common.Args) > 0 {
						if c, ok := common.Args[0].(*ssa.Const); ok && c.Value != nil && c.Value.Kind() == constant.String {
							if w := strings.Count(constant.StringVal(c.Value), "%w"); w >= 2 {
								add(fn, finding{"fmt.Errorf-multi-%w", position(ins.Pos()), fmt.Sprintf("wraps=%d %q", w, constant.StringVal(c.Value))})
							}
						}
					}
				case strings.HasPrefix(name, module+"pkg/errors.") && (strings.HasSuffix(name, "CombineTerminalOutcomes") || strings.HasSuffix(name, "NormalizeTerminalOutcome") || strings.HasSuffix(name, "TransactionControlFailure") || strings.HasSuffix(name, "WithStatus") || strings.HasSuffix(name, "NewTerminalFailure")):
					add(fn, finding{"owner-call", position(ins.Pos()), strings.TrimPrefix(name, module)})
				}
			}
		}
		if probe && statusConst {
			add(fn, finding{"status-default", position(fn.Pos()), "probes an error's own status and produces a status constant otherwise"})
		}
	}
	// Render owner closure (Rule 73 course change after the 46e4 review): every
	// function that turns an error into status, message or diagnostics must
	// enter the one render pass, renderStatus, or be a leaf it calls itself.
	var owner *ssa.Function
	for fn := range seen {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == module+"pkg/errors" && fn.Name() == "renderStatus" && fn.Parent() == nil {
			owner = fn
		}
	}
	if owner == nil {
		panic("render owner renderStatus not found")
	}
	leaf := map[*ssa.Function]bool{}
	for queue := []*ssa.Function{owner}; len(queue) > 0; queue = queue[1:] {
		for _, next := range edges[queue[0]] {
			if !leaf[next] {
				leaf[next] = true
				queue = append(queue, next)
			}
		}
	}
	// Everything that can reach the owner: a breadth-first walk of the
	// reversed call graph, so call cycles cannot hide a path.
	callers := map[*ssa.Function][]*ssa.Function{}
	for from, tos := range edges {
		for _, to := range tos {
			callers[to] = append(callers[to], from)
		}
	}
	upstream := map[*ssa.Function]bool{owner: true}
	for queue := []*ssa.Function{owner}; len(queue) > 0; queue = queue[1:] {
		for _, prev := range callers[queue[0]] {
			if !upstream[prev] {
				upstream[prev] = true
				queue = append(queue, prev)
			}
		}
	}
	reaches := func(fn *ssa.Function) bool { return upstream[fn] }
	relation := func(fn *ssa.Function) string {
		switch {
		case fn == owner:
			return "owner"
		case reaches(fn):
			return "reaches-owner"
		case leaf[fn]:
			return "leaf-of-owner"
		}
		return "outside-owner"
	}
	entries := map[*ssa.Function]bool{}
	for fn := range seen {
		sig := fn.Signature
		if !hasErrorParam(sig) || !returnsString(sig) {
			continue
		}
		if dispatch[fn] {
			add(fn, finding{"render-dispatch", position(fn.Pos()), relation(fn)})
		}
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == module+"pkg/errors" {
			entries[fn] = true
			add(fn, finding{"render-entry", position(fn.Pos()), relation(fn)})
		}
	}
	for fn := range seen {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == module+"pkg/errors" {
			continue
		}
		// A call is an edge as a static callee and again as a function
		// reference; report each caller/entry pair once.
		reported := map[*ssa.Function]bool{}
		for _, next := range edges[fn] {
			if entries[next] && !reported[next] {
				reported[next] = true
				add(fn, finding{"render-caller", position(fn.Pos()), strings.TrimPrefix(next.String(), module) + " " + relation(next)})
			}
		}
	}
	for fn := range seen {
		if carrierProbe[fn] && unwrapProbe[fn] {
			add(fn, finding{"carrier-discovery", position(fn.Pos()), "tests for a terminal outcome carrier while walking Unwrap"})
		}
	}
	rows := make([]*member, 0, len(members))
	for _, m := range members {
		sort.Slice(m.Findings, func(i, j int) bool {
			return m.Findings[i].Position+m.Findings[i].Kind < m.Findings[j].Position+m.Findings[j].Kind
		})
		rows = append(rows, m)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Function < rows[j].Function })
	controlKinds := map[string]bool{}
	for _, m := range rows {
		if m.Package == "internal/nor180behaviourcontrol" {
			for _, f := range m.Findings {
				controlKinds[f.Kind] = true
			}
		}
	}
	controlOK := controlKinds["status-default"] && controlKinds["status-provider"] && controlKinds["errors.Join"] && controlKinds["fmt.Errorf-multi-%w"] && controlKinds["multi-error-type"] && controlKinds["status-code"] && controlKinds["combiner-signature"] && controlKinds["render-dispatch"] && controlKinds["carrier-discovery"]
	if !controlOK {
		panic(fmt.Sprintf("planted control not fully detected: %v", controlKinds))
	}
	out := map[string]any{"author": "Claude (NOR-180 lane)", "subject_head": in.SubjectHead, "parser": os.Args[3], "runtime": runtime.Version() + "/" + runtime.GOOS,
		"archive_sha256": in.ArchiveSHA256, "functions_scanned": len(seen), "members": rows, "control": controlKinds}
	var payload bytes.Buffer
	zw := gzip.NewWriter(&payload)
	if err = json.NewEncoder(zw).Encode(out); err != nil {
		panic(err)
	}
	if err = zw.Close(); err != nil {
		panic(err)
	}
	counts := map[string]int{}
	for _, m := range rows {
		for _, f := range m.Findings {
			counts[f.Kind]++
		}
	}
	fmt.Printf("NOR180_BEHAVIOUR subject=%s parser=%s functions=%d members=%d control=true counts=%v gzip_sha256=%s\n", in.SubjectHead, os.Args[3], len(seen), len(rows), counts, digest(payload.Bytes()))
	fmt.Println("NOR180_BEHAVIOUR_GZIP_BASE64_BEGIN")
	text := base64.StdEncoding.EncodeToString(payload.Bytes())
	for len(text) > 0 {
		n := 16384
		if n > len(text) {
			n = len(text)
		}
		fmt.Println(text[:n])
		text = text[n:]
	}
	fmt.Println("NOR180_BEHAVIOUR_GZIP_BASE64_END")
}

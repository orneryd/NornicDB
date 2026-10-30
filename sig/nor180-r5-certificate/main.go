// Author: Codex, NOR-180 producer-derived source obligations, 29 October 2026.
// This is a domain client of #1667's canonical compiler engine, not another
// loader/parser/SSA implementation. It performs no product/test execution.
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
	"go/token"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

const module = "github.com/orneryd/nornicdb/"

type inputs struct {
	Author, SubjectHead, ToolParent, ArchiveSHA256 string
	BaselineFunctions                              []string
	ChangedProductionGo                            []string
}
type reason struct{ Kind, From, Position, Instruction string }
type unit struct {
	Function, Position, Signature string
	Reasons                       []reason
	Instructions                  []map[string]any
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func own(fn *ssa.Function) bool { return fn != nil && strings.Contains(fn.String(), module) }
func isError(t types.Type) bool {
	return types.Implements(t, types.Universe.Lookup("error").Type().Underlying().(*types.Interface))
}
func errorInput(t types.Type) bool {
	if isError(t) {
		return true
	}
	switch t := t.Underlying().(type) {
	case *types.Pointer:
		return isError(t.Elem())
	case *types.Slice:
		return errorInput(t.Elem())
	case *types.Array:
		return errorInput(t.Elem())
	}
	return false
}
func errorField(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if isError(t) {
		return true
	}
	switch t := t.Underlying().(type) {
	case *types.Pointer:
		return errorField(t.Elem(), seen)
	case *types.Array:
		return errorField(t.Elem(), seen)
	case *types.Slice:
		return errorField(t.Elem(), seen)
	case *types.Chan:
		return errorField(t.Elem(), seen)
	case *types.Struct:
		for i := 0; i < t.NumFields(); i++ {
			if errorField(t.Field(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}
func fieldKey(value ssa.Value) string {
	var base types.Type
	var index int
	switch v := value.(type) {
	case *ssa.FieldAddr:
		base, index = v.X.Type(), v.Field
	case *ssa.Field:
		base, index = v.X.Type(), v.Field
	default:
		return ""
	}
	if ptr, ok := base.(*types.Pointer); ok {
		base = ptr.Elem()
	}
	s, ok := base.Underlying().(*types.Struct)
	if !ok || !errorField(s.Field(index).Type(), map[types.Type]bool{}) {
		return ""
	}
	return types.TypeString(base, nil) + "." + s.Field(index).Name()
}

func main() {
	if len(os.Args) != 4 {
		panic("usage: certificate <subject.tar.gz> <inputs.json> <default|antlr>")
	}
	archive, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	b, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	var in inputs
	if err = json.Unmarshal(b, &in); err != nil {
		panic(err)
	}
	if digest(archive) != in.ArchiveSHA256 {
		panic("subject archive hash mismatch")
	}
	root, err := os.MkdirTemp("", "nor180-r5-subject-")
	if err != nil {
		panic(err)
	}
	// The subject is an immutable git archive. Keep it for this job's lifetime.
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		panic(err)
	}
	tarReader := tar.NewReader(gz)
	for {
		header, e := tarReader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(e)
		}
		clean := filepath.Clean(header.Name)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			panic("archive path escapes subject")
		}
		path := filepath.Join(root, clean)
		if header.Typeflag == tar.TypeDir {
			if e = os.MkdirAll(path, 0755); e != nil {
				panic(e)
			}
			continue
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
			panic(e)
		}
		file, e := os.Create(path)
		if e != nil {
			panic(e)
		}
		_, copyErr := io.Copy(file, tarReader)
		closeErr := file.Close()
		if copyErr != nil {
			panic(copyErr)
		}
		if closeErr != nil {
			panic(closeErr)
		}
	}
	if err = gz.Close(); err != nil {
		panic(err)
	}
	subject := filepath.Join(root, "subject")
	controlPath := filepath.Join(subject, "internal", "nor180certificatecontrol", "control.go")
	control := []byte("// Author: Codex. Analysis-only planted consumer,never a product change.\npackage nor180certificatecontrol\nimport (\"context\";\"github.com/orneryd/nornicdb/pkg/cypher\")\nfunc Count(ctx context.Context, exec *cypher.StorageExecutor) int { _, _ = exec.Execute(ctx, \"CREATE (:CertificateControl)\", nil); return 1 }\n")
	if err = os.MkdirAll(filepath.Dir(controlPath), 0755); err != nil {
		panic(err)
	}
	if err = os.WriteFile(controlPath, control, 0644); err != nil {
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
			panic(e)
		}
		return filepath.ToSlash(rel) + fmt.Sprintf(":%d:%d", at.Line, at.Column)
	}
	functions := map[string]*ssa.Function{}
	fields := map[string][]ssa.Instruction{}
	targets := map[ssa.CallInstruction][]*ssa.Function{}
	for _, fn := range program.Functions {
		if own(fn) {
			functions[fn.String()] = fn
			for _, block := range fn.Blocks {
				for _, i := range block.Instrs {
					if value, ok := i.(ssa.Value); ok {
						if key := fieldKey(value); key != "" {
							fields[key] = append(fields[key], i)
						}
					}
				}
			}
		}
	}
	for _, node := range program.CallGraph.Nodes {
		for _, edge := range node.Out {
			if edge.Site != nil && edge.Callee.Func != nil {
				targets[edge.Site] = append(targets[edge.Site], edge.Callee.Func)
			}
		}
	}
	producer := functions["("+module+"pkg/cypher.transactionWAL).finish"]
	if producer == nil {
		panic("canonical finish producer missing")
	}
	units := map[*ssa.Function]*unit{}
	external := map[*ssa.Function]bool{}
	queue := []*ssa.Function{}
	seenReasons := map[string]bool{}
	activeFields := map[string][]reason{}
	add := func(fn *ssa.Function, r reason) {
		if !own(fn) {
			if fn != nil {
				external[fn] = true
			}
			return
		}
		key := fn.String() + fmt.Sprint(r)
		if seenReasons[key] {
			return
		}
		seenReasons[key] = true
		if units[fn] == nil {
			units[fn] = &unit{Function: fn.String(), Position: position(fn.Pos()), Signature: fn.Signature.String()}
			queue = append(queue, fn)
		}
		units[fn].Reasons = append(units[fn].Reasons, r)
	}
	// Derive caller population from the producer, independent of return values,
	// entry names or a claim that an aggregate forwards its error.
	upstream := []*ssa.Function{producer}
	seenUpstream := map[*ssa.Function]bool{producer: true}
	for len(upstream) > 0 {
		fn := upstream[0]
		upstream = upstream[1:]
		add(fn, reason{Kind: "producer-derived complete caller body", From: producer.String()})
		if node := program.CallGraph.Nodes[fn]; node != nil {
			for _, edge := range node.In {
				caller := edge.Caller.Func
				if caller != nil && !seenUpstream[caller] {
					seenUpstream[caller] = true
					upstream = append(upstream, caller)
					r := reason{Kind: "resolved producer caller,regardless of result/discard", From: fn.String()}
					if edge.Site != nil {
						r.Position = position(edge.Site.Pos())
						r.Instruction = edge.Site.String()
					}
					add(caller, r)
				}
			}
		}
	}
	var missingBaseline []string
	for _, name := range in.BaselineFunctions {
		if fn := functions[name]; fn != nil {
			add(fn, reason{Kind: "preserved pre-change obligation,requires semantic re-verification", From: in.SubjectHead})
		} else {
			missingBaseline = append(missingBaseline, name)
		}
	}
	changed := map[string]bool{}
	for _, path := range in.ChangedProductionGo {
		changed[path] = true
	}
	for _, fn := range program.Functions {
		if !own(fn) {
			continue
		}
		at := program.FileSet.Position(fn.Pos())
		if rel, e := filepath.Rel(subject, at.Filename); e == nil && changed[filepath.ToSlash(rel)] {
			add(fn, reason{Kind: "own delta complete source-body obligation", From: in.SubjectHead})
		}
	}
	activate := func(key string, r reason) {
		if key == "" {
			return
		}
		first := len(activeFields[key]) == 0
		activeFields[key] = append(activeFields[key], r)
		if first {
			for _, i := range fields[key] {
				add(i.Parent(), reason{Kind: "typed escaping-field reader/writer,not automatic retention", From: key, Position: position(i.Pos()), Instruction: i.String()})
			}
		}
	}
	for index := 0; index < len(queue); index++ {
		fn := queue[index]
		u := units[fn]
		for _, block := range fn.Blocks {
			for _, i := range block.Instrs {
				item := map[string]any{"position": position(i.Pos()), "block": block.Index, "operation": fmt.Sprintf("%T", i), "instruction": i.String()}
				if value, ok := i.(ssa.Value); ok {
					item["value"] = value.Name()
					item["type"] = value.Type().String()
					activate(fieldKey(value), reason{Kind: "error-bearing field access,including opaque atomic/context state;not identity proof", From: fn.String(), Position: position(i.Pos()), Instruction: i.String()})
				}
				if call, ok := i.(ssa.CallInstruction); ok {
					names := []string{}
					for _, target := range targets[call] {
						names = append(names, target.String())
					}
					sort.Strings(names)
					item["targets"] = names
					item["unresolved_call_obligation"] = len(names) == 0
					input := call.Common().IsInvoke() && isError(call.Common().Value.Type())
					for _, arg := range call.Common().Args {
						input = input || errorInput(arg.Type())
					}
					if input {
						for _, target := range targets[call] {
							add(target, reason{Kind: "error-input dispatch/projection/composition body", From: fn.String(), Position: position(i.Pos()), Instruction: i.String()})
						}
					}
				}
				u.Instructions = append(u.Instructions, item)
				if store, ok := i.(*ssa.Store); ok && errorField(store.Val.Type(), map[types.Type]bool{}) {
					activate(fieldKey(store.Addr), reason{Kind: "escaping error-bearing store obligation", From: fn.String(), Position: position(i.Pos()), Instruction: i.String()})
				}
			}
		}
	}
	controlName := module + "internal/nor180certificatecontrol.Count"
	controlFound := false
	var rows []*unit
	pins := map[string]string{}
	for fn, u := range units {
		if fn.String() == controlName {
			controlFound = true
		}
		sort.Slice(u.Reasons, func(i, j int) bool { return fmt.Sprint(u.Reasons[i]) < fmt.Sprint(u.Reasons[j]) })
		rows = append(rows, u)
		at := program.FileSet.Position(fn.Pos())
		if data, e := os.ReadFile(at.Filename); e == nil {
			rel, _ := filepath.Rel(subject, at.Filename)
			pins[filepath.ToSlash(rel)] = digest(data)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Function < rows[j].Function })
	sort.Strings(missingBaseline)
	var externalRows []map[string]any
	for fn := range external {
		var body []map[string]string
		for _, block := range fn.Blocks {
			for _, i := range block.Instrs {
				body = append(body, map[string]string{"instruction": i.String(), "position": position(i.Pos())})
			}
		}
		externalRows = append(externalRows, map[string]any{"function": fn.String(), "signature": fn.Signature.String(), "position": position(fn.Pos()), "instructions": body, "opaque_body_obligation": len(body) == 0, "status": "OPEN external integration/value-flow obligation;no automatic retention approval and no third-party patch authority"})
	}
	sort.Slice(externalRows, func(i, j int) bool {
		return externalRows[i]["function"].(string) < externalRows[j]["function"].(string)
	})
	if !controlFound {
		panic("producer-derived census missed planted out-of-package discard")
	}
	out := map[string]any{"author": "Codex,29Oct2026", "subject_head": in.SubjectHead, "tool_parent": in.ToolParent, "parser": os.Args[3], "archive_sha256": in.ArchiveSHA256, "runtime": runtime.Version() + "/" + runtime.GOOS, "loaded_functions": len(program.Functions), "packages": len(program.Packages), "producer": producer.String(), "producer_caller_closure": len(seenUpstream), "units": rows, "consumer_units": len(rows), "typed_field_slots": len(activeFields), "field_obligations": activeFields, "source_pins": pins, "missing_baseline_function_obligations": missingBaseline, "external_integration_obligations": externalRows, "control": map[string]any{"function": controlName, "sha256": digest(control), "detected": controlFound, "scope": "analysis-only planted discard outside producer package;classify/remove from product counts explicitly"}, "limits": "Conservative source obligations,not semantic approvals. Caller traversal crosses external nodes rather than treating them as a barrier. Opaque/unresolved/external dispatch remains an explicit source-backed integration obligation;VTA is conservative,not a completeness approval. The producer caller closure does not depend on selected entries or returned errors. Complete bodies plus escaped fields,error-input dispatch,baseline and own delta are retained. Every executable member,missing/replaced baseline function and unknown/aggregation/control boundary still requires identity/truth/classification/message/sink disposition. MCP,NOR-174,NOR-296,NOR-302 are classified after discovery,not silently filtered."}
	var payload bytes.Buffer
	zw := gzip.NewWriter(&payload)
	enc := json.NewEncoder(zw)
	if err = enc.Encode(out); err != nil {
		panic(err)
	}
	if err = zw.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("NOR180_CERTIFICATE author=Codex subject=%s parser=%s units=%d caller_closure=%d fields=%d control=true gzip_sha256=%s\n", in.SubjectHead, os.Args[3], len(rows), len(seenUpstream), len(activeFields), digest(payload.Bytes()))
	fmt.Println("NOR180_CERTIFICATE_GZIP_BASE64_BEGIN")
	text := base64.StdEncoding.EncodeToString(payload.Bytes())
	for len(text) > 0 {
		n := 16384
		if len(text) < n {
			n = len(text)
		}
		fmt.Println(text[:n])
		text = text[n:]
	}
	fmt.Println("NOR180_CERTIFICATE_GZIP_BASE64_END")
}

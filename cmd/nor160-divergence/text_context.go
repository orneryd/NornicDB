// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"go/ast"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

func sourceName(name string) string {
	n := strings.ToLower(name)
	for _, word := range []string{"query", "statement", "cypher", "stmt", "source", "name"} {
		if strings.Contains(n, word) {
			return word + ":" + name
		}
	}
	return ""
}

func calleeName(c *ssa.CallCommon) (string, string) {
	if f := c.StaticCallee(); f != nil {
		pkg := ""
		if f.Pkg != nil {
			pkg = f.Pkg.Pkg.Path()
		}
		name := f.Name()
		if f.Object() != nil {
			name = f.Object().Name()
		}
		return pkg, name
	}
	if c.Method != nil && c.Method.Pkg() != nil {
		return c.Method.Pkg().Path(), c.Method.Name()
	}
	return "", ""
}

func rederives(pkg, name string) bool {
	if pkg == "strings" {
		switch name {
		case "Fields", "FieldsFunc", "Split", "SplitN", "SplitAfter", "SplitAfterN", "ToLower", "ToUpper", "EqualFold", "Cut", "CutPrefix", "CutSuffix":
			return true
		}
	}
	if pkg == "regexp" {
		return true
	}
	n := strings.ToLower(name)
	return strings.Contains(n, "normalizesource") || strings.Contains(n, "tokeniz") || strings.Contains(n, "lex") || strings.HasPrefix(n, "parse") || strings.HasPrefix(n, "newscanner") || strings.HasPrefix(n, "newparser") || strings.HasPrefix(n, "newlexer") || (pkg == "text/scanner" && name == "Init")
}

func (a *analysis) text() []Finding {
	taint := map[ssa.Value]string{}
	// Keep origin strings deterministic; taint is monotone, so propagation terminates.
	mark := func(v ssa.Value, origin string) bool {
		if v == nil || origin == "" || taint[v] != "" {
			return false
		}
		taint[v] = origin
		return true
	}
	for _, fn := range a.functions {
		for _, p := range fn.SSA.Params {
			mark(p, sourceName(p.Name()))
		}
		for _, b := range fn.SSA.Blocks {
			for _, ins := range b.Instrs {
				switch x := ins.(type) {
				case *ssa.DebugRef:
					if id, ok := x.Expr.(*ast.Ident); ok {
						mark(x.X, sourceName(id.Name))
					}
				case *ssa.Field:
					mark(x, sourceName(fieldID(x.X.Type(), x.Field)))
				case *ssa.FieldAddr:
					mark(x, sourceName(fieldID(x.X.Type(), x.Field)))
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, fn := range a.functions {
			for _, b := range fn.SSA.Blocks {
				for _, ins := range b.Instrs {
					origin := ""
					for _, operand := range ins.Operands(nil) {
						if operand != nil && *operand != nil && taint[*operand] != "" {
							origin = taint[*operand]
							break
						}
					}
					if c, ok := ins.(ssa.CallInstruction); ok {
						// Argument -> parameter, and return -> call, through refined dynamic edges.
						if node := a.graph.Nodes[fn.SSA]; node != nil {
							for _, edge := range node.Out {
								if edge.Site != c {
									continue
								}
								target := edge.Callee.Func
								if a.canonical(target) == nil {
									continue
								}
								args := c.Common().Args
								if c.Common().IsInvoke() {
									args = append([]ssa.Value{c.Common().Value}, args...)
								}
								for i, arg := range args {
									if i < len(target.Params) && mark(target.Params[i], taint[arg]) {
										changed = true
									}
								}
								for _, block := range target.Blocks {
									for _, inst := range block.Instrs {
										if r, ok := inst.(*ssa.Return); ok {
											for _, v := range r.Results {
												if taint[v] != "" && mark(c.Value(), taint[v]) {
													changed = true
												}
											}
										}
									}
								}
							}
						}
						if mark(c.Value(), origin) {
							changed = true
						}
					} else if v, ok := ins.(ssa.Value); ok {
						if mark(v, origin) {
							changed = true
						}
					}
				}
			}
		}
	}
	out := []Finding{}
	for _, fn := range a.functions {
		for _, b := range fn.SSA.Blocks {
			for _, ins := range b.Instrs {
				c, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				pkg, name := calleeName(c.Common())
				if !rederives(pkg, name) {
					continue
				}
				origins := []string{}
				for _, arg := range c.Common().Args {
					if taint[arg] != "" {
						origins = append(origins, taint[arg])
					}
				}
				if c.Common().IsInvoke() && taint[c.Common().Value] != "" {
					origins = append(origins, taint[c.Common().Value])
				}
				if len(origins) == 0 {
					continue
				}
				if a.owners.owns("text", fn.Site.Package, fn.Site.Function, "") {
					continue
				}
				sort.Strings(origins)
				out = append(out, Finding{Kind: "text_rederivation", Subject: pkg + "." + name, Why: "A compiler-resolved text operation consumes statement/query/name-derived SSA values outside the supplied text owners; inspect whether carried facts could replace re-derivation.", Sites: []Site{a.site(fn.Site.Package, fn.Site.Function, c.Pos(), "rederivation")}, Evidence: map[string]any{"callee": pkg + "." + name, "provenance": origins}})
			}
		}
	}
	return out
}

func isContext(t types.Type) bool { return typeKey(types.Unalias(t)) == "context.Context" }
func isRequest(t types.Type) bool {
	t = types.Unalias(t)
	s := typeKey(t)
	if p, ok := t.(*types.Pointer); ok {
		s = "*" + typeKey(types.Unalias(p.Elem()))
	}
	return s == "*net/http.Request" || s == "*github.com/valyala/fasthttp.RequestCtx" || s == "*google.golang.org/grpc.ServerStream"
}

func (a *analysis) contextLoss() []Finding {
	roots := map[*ssa.Function]string{}
	receives := map[*ssa.Function]bool{}
	for _, fn := range a.functions {
		sig := fn.SSA.Signature
		hasRequest, hasWriter := false, false
		for i := 0; i < sig.Params().Len(); i++ {
			t := sig.Params().At(i).Type()
			if isContext(t) {
				receives[fn.SSA] = true
			}
			if isRequest(t) {
				hasRequest = true
			}
			if typeKey(types.Unalias(t)) == "net/http.ResponseWriter" {
				hasWriter = true
			}
		}
		if hasRequest && (hasWriter || strings.Contains(strings.ToLower(fn.SSA.Name()), "handle")) {
			roots[fn.SSA] = fn.Site.Function
		}
		// gRPC-style request methods receive context plus a pointer request and (response,error).
		if sig.Recv() != nil && sig.Params().Len() == 2 && isContext(sig.Params().At(0).Type()) && sig.Results().Len() == 2 {
			if _, ok := sig.Params().At(1).Type().(*types.Pointer); ok {
				roots[fn.SSA] = fn.Site.Function
			}
		}
	}
	// Recognize registrations and http.HandlerFunc conversions from their typed SSA values.
	for _, fn := range a.functions {
		for _, b := range fn.SSA.Blocks {
			for _, ins := range b.Instrs {
				c, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				pkg, name := calleeName(c.Common())
				if pkg != "net/http" || (name != "HandleFunc" && name != "Handle") {
					continue
				}
				for _, arg := range c.Common().Args {
					switch v := arg.(type) {
					case *ssa.Function:
						roots[v] = v.String()
					case *ssa.MakeClosure:
						if f, ok := v.Fn.(*ssa.Function); ok {
							roots[f] = f.String()
						}
					}
				}
			}
		}
	}
	via := map[*ssa.Function]string{}
	parent := map[*ssa.Function]*ssa.Function{}
	queue := []*ssa.Function{}
	rootList := []*ssa.Function{}
	for f := range roots {
		rootList = append(rootList, f)
	}
	sort.Slice(rootList, func(i, j int) bool { return rootList[i].String() < rootList[j].String() })
	for _, f := range rootList {
		via[f] = roots[f]
		queue = append(queue, f)
	}
	for len(queue) > 0 {
		f := queue[0]
		queue = queue[1:]
		if n := a.graph.Nodes[f]; n != nil {
			for _, e := range n.Out {
				g := e.Callee.Func
				if g != nil && via[g] == "" {
					via[g] = via[f]
					parent[g] = f
					queue = append(queue, g)
				}
			}
		}
	}
	out := []Finding{}
	for _, fn := range a.functions {
		if !receives[fn.SSA] && via[fn.SSA] == "" {
			continue
		}
		for _, b := range fn.SSA.Blocks {
			for _, ins := range b.Instrs {
				c, ok := ins.(ssa.CallInstruction)
				if !ok {
					continue
				}
				pkg, name := calleeName(c.Common())
				if pkg != "context" || (name != "Background" && name != "TODO") {
					continue
				}
				sites := []Site{a.site(fn.Site.Package, fn.Site.Function, c.Pos(), "new_root_context")}
				path := []string{}
				for f := fn.SSA; f != nil; f = parent[f] {
					path = append(path, f.String())
					if root := roots[f]; root != "" {
						if rf := a.canonical(f); rf != nil {
							site := rf.Site
							site.Role = "request_handler"
							sites = append(sites, site)
						}
						break
					}
				}
				out = append(out, Finding{Kind: "context_loss", Subject: "context." + name, Why: "A fresh root context is created where an incoming context or statically reachable request lifetime already exists; inspect cancellation and request metadata loss.", Sites: sites, Evidence: map[string]any{"receives_context": receives[fn.SSA], "request_handler": via[fn.SSA], "reverse_call_path": path}})
			}
		}
	}
	return out
}

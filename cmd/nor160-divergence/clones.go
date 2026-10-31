// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"sort"
	"strings"
)

type cloneUnit struct {
	site   Site
	tokens []uint32
	hash   string
}

// Reflect only AST data, omitting position, scope, comments and object back-pointers.
// go/types resolves calls/fields/globals, so import aliases and local spelling are immaterial.
func normalized(n ast.Node, info *types.Info, params map[types.Object]bool) []string {
	var out []string
	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		if !v.IsValid() {
			return
		}
		if v.Kind() == reflect.Interface {
			if !v.IsNil() {
				walk(v.Elem())
			}
			return
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return
			}
			if id, ok := v.Interface().(*ast.Ident); ok {
				obj := info.ObjectOf(id)
				role := "local"
				switch x := obj.(type) {
				case *types.Func:
					role = "call:" + x.FullName()
				case *types.Builtin:
					role = "call:" + x.Name()
				case *types.PkgName:
					role = "package:" + x.Imported().Path()
				case *types.TypeName:
					role = "type:" + typeKey(x.Type())
				case *types.Var:
					if x.IsField() {
						role = "field:" + x.Id()
					} else if x.Pkg() != nil && x.Parent() == x.Pkg().Scope() {
						role = "global:" + x.Pkg().Path() + "." + x.Name()
					} else if params[x] {
						role = "param"
					}
				case *types.Const:
					role = "constant:" + typeKey(x.Type())
				case *types.Nil:
					role = "nil"
				}
				out = append(out, role)
				return
			}
			if lit, ok := v.Interface().(*ast.BasicLit); ok {
				out = append(out, "literal:"+lit.Kind.String())
				return
			}
			walk(v.Elem())
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			if v.Type() == reflect.TypeOf(token.Pos(0)) {
				return
			}
			out = append(out, v.Type().Name()+"{")
			for i := 0; i < v.NumField(); i++ {
				f := v.Type().Field(i)
				if f.Type == reflect.TypeOf(token.Pos(0)) || f.Name == "Obj" || f.Name == "Scope" || f.Name == "Doc" || f.Name == "Comment" || f.Name == "Comments" {
					continue
				}
				walk(v.Field(i))
			}
			out = append(out, "}")
		case reflect.Slice:
			out = append(out, "[")
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
			out = append(out, "]")
		case reflect.Int:
			if v.Type() == reflect.TypeOf(token.Token(0)) {
				out = append(out, token.Token(v.Int()).String())
			}
		case reflect.Bool:
			out = append(out, fmt.Sprint(v.Bool()))
		case reflect.String:
			out = append(out, v.String())
		}
	}
	walk(reflect.ValueOf(n))
	return out
}

func (a *analysis) clones() []Finding {
	units := []cloneUnit{}
	seen := map[string]bool{}
	tokenIDs := map[string]uint32{}
	add := func(n ast.Node, fn *function, role string) {
		s := fn.Site
		s.Line = a.prog.Fset.Position(n.Pos()).Line
		s.Role = role
		k := siteKey(s)
		if seen[k] {
			return
		}
		seen[k] = true
		params := map[types.Object]bool{}
		sig := fn.SSA.Signature
		for i := 0; i < sig.Params().Len(); i++ {
			params[sig.Params().At(i)] = true
		}
		if sig.Recv() != nil {
			params[sig.Recv()] = true
		}
		t := normalized(n, fn.Pkg.TypesInfo, params)
		ids := make([]uint32, len(t))
		for i, token := range t {
			id, ok := tokenIDs[token]
			if !ok {
				id = uint32(len(tokenIDs))
				tokenIDs[token] = id
			}
			ids[i] = id
		}
		units = append(units, cloneUnit{s, ids, digest(strings.Join(t, "\x00"))})
	}
	for _, fn := range a.functions {
		add(fn.Body, fn, "body")
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			if b, ok := n.(*ast.BlockStmt); ok && len(b.List) >= 8 {
				for i := 0; i+8 <= len(b.List); i++ {
					add(&ast.BlockStmt{Lbrace: b.List[i].Pos(), List: b.List[i : i+8], Rbrace: b.List[i+7].End()}, fn, "eight_statement_block")
				}
			}
			return true
		})
	}
	groups := map[string][]int{}
	for i, u := range units {
		groups[u.hash] = append(groups[u.hash], i)
	}
	out := []Finding{}
	for hash, g := range groups {
		if len(g) < 2 {
			continue
		}
		sites := []Site{}
		for _, i := range g {
			sites = append(sites, units[i].site)
		}
		out = append(out, Finding{Kind: "exact", Subject: hash, Why: "Different source sites have identical typed, role-normalized AST bodies or eight-statement blocks.", Sites: sites, Evidence: map[string]any{"normalized_sha256": hash, "tokens": len(units[g[0]].tokens)}})
	}
	// Near matching works on one representative per exact group. An inverted shingle index
	// avoids all-pairs comparisons. Qualifying pairs retain their actual edit similarity.
	reps := []int{}
	for _, g := range groups {
		reps = append(reps, g[0])
	}
	sort.Ints(reps)
	// Common structural shingles carry little discrimination and cause quadratic
	// indexing. Keep only shingles appearing in <=128 exact-group representatives.
	frequency := map[uint64]int{}
	for _, i := range reps {
		if len(units[i].tokens) >= 40 {
			for s := range cloneShingles(units[i].tokens) {
				frequency[s]++
			}
		}
	}
	index := map[uint64][]int{}
	for _, i := range reps {
		u := units[i]
		if len(u.tokens) < 40 {
			continue
		}
		shingles := cloneShingles(u.tokens)
		candidates := map[int]int{}
		for s := range shingles {
			if frequency[s] > 128 {
				continue
			}
			for _, j := range index[s] {
				candidates[j]++
			}
		}
		for j, hits := range candidates {
			v := units[j]
			if u.site.Function == v.site.Function || hits < 3 || min(len(u.tokens), len(v.tokens))*10 < max(len(u.tokens), len(v.tokens))*9 {
				continue
			}
			limit := max(len(u.tokens), len(v.tokens)) / 10
			distance := tokenDistance(u.tokens, v.tokens, limit)
			if distance > limit {
				continue
			}
			sites := []Site{}
			for _, k := range groups[u.hash] {
				sites = append(sites, units[k].site)
			}
			for _, k := range groups[v.hash] {
				sites = append(sites, units[k].site)
			}
			out = append(out, Finding{Kind: "near", Subject: u.hash + ":" + v.hash, Why: "Different functions share at least 90% of normalized AST tokens; inspect the remaining decisions before unifying them.", Sites: sites, Evidence: map[string]any{"edit_similarity": 1 - float64(distance)/float64(max(len(u.tokens), len(v.tokens))), "hashes": []string{u.hash, v.hash}}})
		}
		for s := range shingles {
			if frequency[s] > 128 {
				continue
			}
			index[s] = append(index[s], i)
		}
	}
	return out
}

// Banded Levenshtein; bounded by the 10% admissible distance.
func cloneShingles(tokens []uint32) map[uint64]bool {
	out := map[uint64]bool{}
	for j := 0; j+12 <= len(tokens); j += 4 {
		h := uint64(14695981039346656037)
		for _, v := range tokens[j : j+12] {
			h ^= uint64(v)
			h *= 1099511628211
		}
		out[h] = true
	}
	return out
}

func tokenDistance(a, b []uint32, limit int) int {
	if len(a) > len(b) {
		a, b = b, a
	}
	if len(b)-len(a) > limit {
		return limit + 1
	}
	prev := make([]int, len(b)+1)
	next := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = min(j, limit+1)
		next[j] = limit + 1
	}
	for i := 1; i <= len(a); i++ {
		low, high := max(1, i-limit), min(len(b), i+limit)
		next[low-1] = limit + 1
		if low == 1 {
			next[0] = i
		}
		if high < len(b) {
			next[high+1] = limit + 1
		}
		best := limit + 1
		for j := low; j <= high; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			next[j] = min(prev[j]+1, next[j-1]+1, prev[j-1]+cost)
			best = min(best, next[j])
		}
		if best > limit {
			return limit + 1
		}
		prev, next = next, prev
	}
	return prev[len(b)]
}

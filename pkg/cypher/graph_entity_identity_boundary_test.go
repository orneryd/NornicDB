package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Inspect every production reference, including function-value aliases and
// package initializers. A newly added caller cannot silently escape the owner.
func TestGraphEntityIdentityReaderBoundary(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		allowed := make(map[token.Pos]bool)
		var deferred *ast.BlockStmt
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if name == "graph_entity_identity.go" && fn.Name.Name == "gdsNodeID" {
				allowed[fn.Name.Pos()] = true
			}
			if name == "gds_node_similarity_rows.go" && fn.Name.Name == "similarityNodeID" {
				deferred = fn.Body
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			id, ok := node.(*ast.Ident)
			if !ok || (id.Name != "gdsNodeID" && id.Name != "linkPredictionNodeID") {
				return true
			}
			if allowed[id.Pos()] {
				return true
			}
			// Only row similarity's separately deferred key-domain adapter may use
			// the legacy reader. Projection filters are expressly outside that gap.
			if id.Name == "gdsNodeID" && deferred != nil && deferred.Pos() <= id.Pos() && id.End() <= deferred.End() {
				return true
			}
			t.Errorf("%s: bare-local %s reference outside identity owner/deferred similarity adapter", name, id.Name)
			return true
		})
	}
}

// Guard the causal boundary as well as known symbol references: GDS value
// readers may not decode an entity by selecting its bare ID. All function
// contexts that carry a graph executor must also carry its database selection.
func TestGraphEntityIdentityInputAndContextOwners(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			fields := make(map[string]ast.Expr)
			for _, element := range literal.Elts {
				if field, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := field.Key.(*ast.Ident); ok {
						fields[key.Name] = field.Value
					}
				}
			}
			if selection, ok := literal.Type.(*ast.SelectorExpr); ok && selection.Sel.Name == "Context" && fields["Graphs"] != nil && fields["Storage"] != nil && fields["Database"] == nil {
				t.Errorf("%s: graph function context omits the selected database", name)
			}
			if typ, ok := literal.Type.(*ast.Ident); ok && typ.Name == "StorageExecutor" {
				if manager, ok := fields["dbManager"].(*ast.SelectorExpr); ok && manager.Sel.Name == "dbManager" && fields["graphIdentityScope"] == nil {
					t.Errorf("%s: executor clone drops its identity scope", name)
				}
			}
			return true
		})
		if !strings.HasPrefix(name, "gds_") {
			continue
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Name.Name == "similarityNodeID" {
				continue
			}
			readsValue := false
			if function.Type.Params != nil {
				for _, param := range function.Type.Params.List {
					if _, ok := param.Type.(*ast.InterfaceType); ok {
						readsValue = true
					}
					if typ, ok := param.Type.(*ast.Ident); ok && typ.Name == "any" {
						readsValue = true
					}
				}
			}
			if !readsValue {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "ID" {
					t.Errorf("%s: value reader %s bypasses the identity owner with .ID", name, function.Name.Name)
				}
				return true
			})
		}
	}
}

// The guard follows one construction and straight-line metadata transfers in
// its containing block. It never treats a setter on another object, a setter
// after return, or a branch-local setter as proof for this construction.
type nodeProjection struct {
	name                                               string
	source, inlineDatabase, metadataFrom, sharedRecord ast.Expr
}

func (p nodeProjection) copiedID() bool { return p.source != nil }
func (p nodeProjection) attested() bool {
	if !p.copiedID() {
		return true
	}
	for _, evidence := range []ast.Expr{p.inlineDatabase, p.metadataFrom, p.sharedRecord} {
		if sameProjectionObject(evidence, p.source) {
			return true
		}
	}
	return false
}

func sameProjectionObject(a, b ast.Expr) bool {
	if a == nil || b == nil {
		return false
	}
	// An in-place replacement writes *node, then sets metadata on node.
	for {
		if star, ok := a.(*ast.StarExpr); ok {
			a = star.X
		} else {
			break
		}
	}
	for {
		if star, ok := b.(*ast.StarExpr); ok {
			b = star.X
		} else {
			break
		}
	}
	switch x := a.(type) {
	case *ast.Ident:
		y, ok := b.(*ast.Ident)
		return ok && x.Name == y.Name
	case *ast.SelectorExpr:
		y, ok := b.(*ast.SelectorExpr)
		return ok && x.Sel.Name == y.Sel.Name && sameProjectionObject(x.X, y.X)
	}
	return false
}

func projectionField(lit *ast.CompositeLit, name string) ast.Expr {
	for _, element := range lit.Elts {
		if field, ok := element.(*ast.KeyValueExpr); ok {
			if key, ok := field.Key.(*ast.Ident); ok && key.Name == name {
				return field.Value
			}
		}
	}
	return nil
}

func runtimeMetadataSource(value ast.Expr, aliases map[string]ast.Expr) ast.Expr {
	switch x := value.(type) {
	case *ast.Ident:
		return aliases[x.Name]
	case *ast.CallExpr:
		if method, ok := x.Fun.(*ast.SelectorExpr); ok && method.Sel.Name == "RuntimeMetadata" && len(x.Args) == 0 {
			return method.X
		}
	case *ast.CompositeLit:
		// Only the typed metadata shape supplies database provenance.
		typ, ok := x.Type.(*ast.SelectorExpr)
		if !ok || typ.Sel.Name != "NodeRuntimeMetadata" {
			return nil
		}
		pkg, ok := typ.X.(*ast.Ident)
		if !ok || pkg.Name != "storage" {
			return nil
		}
		if db, ok := projectionField(x, "Database").(*ast.SelectorExpr); ok && db.Sel.Name == "Database" {
			return runtimeMetadataSource(db.X, aliases)
		}
	}
	return nil
}

func projectionTransfer(block *ast.BlockStmt, lit *ast.CompositeLit) ast.Expr {
	aliases := make(map[string]ast.Expr)
	var target ast.Expr
	constructed := false
	for _, statement := range block.List {
		if assignment, ok := statement.(*ast.AssignStmt); ok {
			for i, left := range assignment.Lhs {
				if i >= len(assignment.Rhs) {
					continue
				}
				right := assignment.Rhs[i]
				if right.Pos() <= lit.Pos() && lit.End() <= right.End() {
					target, constructed = left, true
				} else if constructed && sameProjectionObject(left, target) {
					return nil // This variable no longer denotes the constructed image.
				}
				if name, ok := left.(*ast.Ident); ok {
					aliases[name.Name] = runtimeMetadataSource(right, aliases)
				}
				if field, ok := left.(*ast.SelectorExpr); ok && field.Sel.Name == "Database" {
					if name, ok := field.X.(*ast.Ident); ok {
						delete(aliases, name.Name)
					}
				}
			}
		}
		if !constructed {
			continue
		}
		if _, returned := statement.(*ast.ReturnStmt); returned {
			return nil
		}
		expression, ok := statement.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expression.X.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			continue
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "SetRuntimeMetadata" || !sameProjectionObject(method.X, target) {
			continue
		}
		return runtimeMetadataSource(call.Args[0], aliases)
	}
	return nil
}

func projectedNodeLiterals(name string, source []byte) ([]nodeProjection, error) {
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		return nil, err
	}
	var projections []nodeProjection
	var blocks []*ast.BlockStmt
	var isBlock []bool
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			if isBlock[len(isBlock)-1] {
				blocks = blocks[:len(blocks)-1]
			}
			isBlock = isBlock[:len(isBlock)-1]
			return false
		}
		block, blockNode := node.(*ast.BlockStmt)
		isBlock = append(isBlock, blockNode)
		if blockNode {
			blocks = append(blocks, block)
		}
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		p := nodeProjection{}
		switch typ := lit.Type.(type) {
		case *ast.Ident:
			p.name = typ.Name
		case *ast.SelectorExpr:
			if pkg, ok := typ.X.(*ast.Ident); ok && pkg.Name == "storage" {
				p.name = typ.Sel.Name
			}
		}
		if p.name != "Node" && p.name != "Edge" {
			return true
		}
		if id, ok := projectionField(lit, "ID").(*ast.SelectorExpr); ok && id.Sel.Name == "ID" {
			p.source = id.X
		}
		if p.name == "Edge" {
			if db, ok := projectionField(lit, "Database").(*ast.SelectorExpr); ok && db.Sel.Name == "Database" {
				p.inlineDatabase = db.X
			}
		} else {
			if shared, ok := projectionField(lit, "runtime").(*ast.SelectorExpr); ok && shared.Sel.Name == "runtime" {
				p.sharedRecord = shared.X
			}
			if len(blocks) != 0 {
				p.metadataFrom = projectionTransfer(blocks[len(blocks)-1], lit)
			}
		}
		projections = append(projections, p)
		return true
	})
	return projections, nil
}

// Native entity projections must copy provenance alongside the local ID.
// Public write and policy regression tests cover consumers of those carriers.
//
// Node provenance now travels through the typed runtime API, in one of two
// approved shapes: a storage copy that shares the source's immutable runtime
// record (runtime: node.runtime), or an explicit SetRuntimeMetadata transfer
// of source-derived metadata onto the same constructed object. Acceptance is
// decided per construction and requires the copy to derive from the entity it
// copied its ID from, so no file or function is exempt; a dropped Database, a
// transfer on an unrelated receiver, a substituted source or a zero value
// still fails.
func TestNativeEntityProjectionPreservesSource(t *testing.T) {
	for _, directory := range []string{".", "../storage", "../search", "../bolt", "../server"} {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			projections, err := projectedNodeLiterals(path, source)
			if err != nil {
				return err
			}
			for _, projection := range projections {
				if !projection.attested() {
					t.Errorf("%s: native %s projection drops or substitutes its source provenance", path, projection.name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestNativeEntityProjectionGuardRejectsPlantedRegressions proves the guard
// still fails on the ways provenance can be lost, including the shapes the
// approved runtime API migration introduced. Each source must describe exactly
// one ID-copying construction, and the guard must accept or reject that one:
// a case the guard misjudges means the acceptance has been weakened.
func TestNativeEntityProjectionGuardRejectsPlantedRegressions(t *testing.T) {
	cases := []struct {
		name    string
		source  string
		accepts bool
	}{
		{
			name: "typed_metadata_transfer_recognized",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	deleted.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: node.RuntimeMetadata().Database})
	return deleted
}`,
			accepts: true,
		},
		{
			name: "whole_metadata_transfer_recognized",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	copied := &storage.Node{ID: node.ID}
	copied.SetRuntimeMetadata(node.RuntimeMetadata())
	return copied
}`,
			accepts: true,
		},
		{
			name: "shared_immutable_record_recognized",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	return &storage.Node{runtime: node.runtime, ID: node.ID}
}`,
			accepts: true,
		},
		{
			name: "in_place_replacement_with_transfer_recognized",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	runtimeMetadata := node.RuntimeMetadata()
	runtimeMetadata.EmbeddingsStoredSeparately = false
	*node = storage.Node{ID: node.ID, Labels: node.Labels}
	node.SetRuntimeMetadata(runtimeMetadata)
	return node
}`,
			accepts: true,
		},
		{
			name: "container_field_transfer_recognized",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	result.Relationship = deleted
	deleted.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: node.RuntimeMetadata().Database})
	return deleted
}`,
			accepts: true,
		},
		{
			name: "edge_inline_database_recognized",
			source: `package planted

func project(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Database: edge.Database, Type: edge.Type}
}`,
			accepts: true,
		},
		{
			name: "dropped_database",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	return &storage.Node{ID: node.ID}
}`,
		},
		{
			name: "dropped_database_with_unrelated_transfer",
			source: `package planted

func project(node *storage.Node, other *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	deleted.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: other.RuntimeMetadata().Database})
	return deleted
}`,
		},
		{
			name: "wrong_receiver",
			source: `package planted

func project(node *storage.Node, sink *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	sink.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: node.RuntimeMetadata().Database})
	return deleted
}`,
		},
		{
			name: "zero_value_metadata",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	deleted := &storage.Node{ID: node.ID}
	deleted.SetRuntimeMetadata(storage.NodeRuntimeMetadata{})
	return deleted
}`,
		},
		{
			name: "missing_metadata_transfer",
			source: `package planted

func project(node *storage.Node) *storage.Node {
	metadata := node.RuntimeMetadata()
	metadata.EmbeddingsStoredSeparately = false
	return &storage.Node{ID: node.ID}
}`,
		},
		{
			name: "shared_record_from_other_entity",
			source: `package planted

func project(node *storage.Node, other *storage.Node) *storage.Node {
	return &storage.Node{runtime: other.runtime, ID: node.ID}
}`,
		},
		{
			name: "relationship_dropped_database",
			source: `package planted

func project(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Type: edge.Type}
}`,
		},
		{
			name: "relationship_reset_database_literal",
			source: `package planted

func project(edge *storage.Edge) *storage.Edge {
	return &storage.Edge{ID: edge.ID, Database: "", Type: edge.Type}
}`,
		},
	}
	for _, testCase := range cases {
		projections, err := projectedNodeLiterals(testCase.name+".go", []byte(testCase.source))
		if err != nil {
			t.Fatalf("%s: %v", testCase.name, err)
		}
		copied := 0
		for _, projection := range projections {
			if projection.copiedID() {
				copied++
			}
		}
		if copied != 1 {
			t.Fatalf("%s: expected exactly one ID-copying literal, found %d", testCase.name, copied)
		}
		accepted := 0
		for _, projection := range projections {
			if projection.attested() {
				accepted++
			}
		}
		if testCase.accepts {
			if accepted != copied {
				t.Errorf("%s: recognized provenance was rejected", testCase.name)
			}
			continue
		}
		if accepted != 0 {
			t.Errorf("%s: planted provenance regression escaped the guard", testCase.name)
		}
	}
}

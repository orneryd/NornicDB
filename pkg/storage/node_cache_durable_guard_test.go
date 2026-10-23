package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
)

// durableWriterViolations checks declarations, references and positive owner
// ordering. Owner identity includes the declared receiver, never just a method
// name or a whole-file exemption. Runtime admission remains the primary guard.
func durableWriterViolations(name string, source []byte) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	reject := func(node ast.Node, why string) {
		problems = append(problems, why+" at "+fset.Position(node.Pos()).String())
	}
	entrances := map[string]bool{
		"newCommitWriter": true, "withUpdate": true, "withUpdateUnits": true,
		"withScopedNodeCacheUpdate": true, "withNotifyingUpdate": true,
		"commitEngineWrite": true, "commitEngineWriteWithCache": true,
		"commitEngineWriteHeld": true, "commitEngineWriteWithRead": true,
		"updateWithScope": true, "forEachStoredRecordInChunks": true,
		"beginLargeCommit":  true,
		"admitDurableWrite": true, "commitDurableWrite": true,
	}
	for _, decl := range file.Decls {
		fn, _ := decl.(*ast.FuncDecl)
		method, receiver := "", ""
		if fn != nil {
			method, receiver = fn.Name.Name, receiverType(fn)
		}
		isOwner := func(file, typ, function string) bool {
			return name == file && receiver == typ && method == function
		}
		// Bind selector references to their immediate call. Merely retaining a
		// method value in an approved owner does not establish issuance.
		calls := make(map[ast.Expr]*ast.CallExpr)
		ast.Inspect(decl, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				calls[call.Fun] = call
			}
			return true
		})
		var durableAt, issueAt token.Pos
		issueCalls, cacheCalls, admissionCalls := 0, 0, 0
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.TypeSpec:
				if body, ok := n.Type.(*ast.StructType); ok {
					for _, field := range body.Fields.List {
						if typ, ok := field.Type.(*ast.Ident); ok && typ.Name == "nodeCacheWriteScope" {
							allowed := (name == "badger_commit_writer.go" && n.Name.Name == "commitWriter") ||
								(name == "badger_managed.go" && (n.Name.Name == "largeCommit" || n.Name.Name == "restoreBatches" || n.Name.Name == "scopedCacheBatches"))
							if !allowed {
								reject(field, "unowned scope carrier")
							}
						}
					}
				}
			case *ast.ValueSpec:
				if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "nodeCacheWriteScope" && len(n.Values) == 0 {
					reject(n, "zero scope")
				}
			case *ast.CompositeLit:
				if typ, ok := n.Type.(*ast.Ident); ok && typ.Name == "nodeCacheWriteScope" && name != "badger_cache_scope.go" {
					reject(n, "scope must be issued by its constructor")
				}
			case *ast.IncDecStmt:
				if field, ok := n.X.(*ast.SelectorExpr); ok && field.Sel.Name == "nodeCacheInvalidationTs" {
					reject(n, "copied fence increment")
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if field, ok := lhs.(*ast.SelectorExpr); ok && field.Sel.Name == "nodeCacheInvalidationTs" && !isOwner("badger_cache_scope.go", "BadgerEngine", "fenceUnknownNodeCacheWritesLocked") {
						reject(n, "copied fence assignment")
					}
				}
			case *ast.UnaryExpr:
				if field, ok := n.X.(*ast.SelectorExpr); ok && n.Op == token.AND && (field.Sel.Name == "nodeCacheInvalidationTs" || field.Sel.Name == "nodeCacheCommitTs") {
					reject(n, "escaped cache state address")
				}
			case *ast.SelectorExpr:
				allowed, protected := true, true
				switch n.Sel.Name {
				case "CommitAt":
					allowed = isOwner("badger_cache_commit.go", "", "commitDurableWrite")
				case "Update":
					allowed = isOwner("badger_cache_commit.go", "", "updateBadgerMetadata")
				case "NewWriteBatchAt":
					allowed = false
				case "NewWriteBatch":
					allowed = name == "serializer_migration.go" || (calls[n] != nil && len(calls[n].Args) == 1)
				case "cacheCommitNodeWrites":
					allowed = isOwner("badger_cache_commit.go", "", "issueNodeCacheCommit")
					if calls[n] != nil {
						cacheCalls++
					}
				case "invalidateNodeCacheScopeLocked":
					allowed = isOwner("badger_cache.go", "BadgerEngine", "cacheCommitNodeWrites")
				case "fenceUnknownNodeCacheWritesLocked":
					allowed = isOwner("badger_cache_scope.go", "BadgerEngine", "invalidateNodeCacheScopeLocked") || isOwner("badger_cache_scope.go", "BadgerEngine", "invalidateNodeCacheRawKeyLocked") || isOwner("badger_cache_scope.go", "BadgerEngine", "invalidateNodeCacheOperationsLocked")
				case "invalidateNodeCacheOperationsLocked", "invalidateNodeCacheRawLocked":
					allowed = isOwner("badger_cache_scope.go", "BadgerEngine", "invalidateNodeCacheScopeLocked")
				case "invalidateNodeCacheRawKeyLocked":
					allowed = isOwner("badger_cache_scope.go", "BadgerEngine", "invalidateNodeCacheRawLocked")
				case "Store", "Swap", "CompareAndSwap", "Add":
					if field, ok := n.X.(*ast.SelectorExpr); ok && field.Sel.Name == "nodeCacheCommitTs" {
						allowed = isOwner("badger_cache.go", "BadgerEngine", "cacheCommitNodeWrites")
					} else {
						protected = false
					}
				default:
					protected = false
				}
				if protected && (!allowed || calls[n] == nil) {
					reject(n, "durable owner bypass")
				}
			case *ast.CallExpr:
				called := ""
				switch fun := n.Fun.(type) {
				case *ast.SelectorExpr:
					called = fun.Sel.Name
				case *ast.Ident:
					called = fun.Name
				}
				if called == "new" && len(n.Args) == 1 {
					if typ, ok := n.Args[0].(*ast.Ident); ok && typ.Name == "nodeCacheWriteScope" {
						reject(n, "zero scope allocation")
					}
				}
				if entrances[called] {
					if len(n.Args) == 0 {
						reject(n, "missing writer scope")
					} else if _, missing := n.Args[0].(*ast.FuncLit); missing {
						reject(n, "missing writer scope")
					}
				}
				if called == "issueNodeCacheCommit" {
					issueAt = n.Pos()
					issueCalls++
				}
				if called == "commitDurableWrite" {
					durableAt = n.Pos()
				}
				if called == "admitDurableWrite" {
					admissionCalls++
				}
				if (called == "finish" || called == "waitPublished") && isOwner("badger_managed.go", "managedBadgerDB", "commitShared") {
					// Its deferred finish is the required release. An immediate
					// finish/wait would publish before cache issuance.
					deferred := false
					for _, stmt := range fn.Body.List {
						if d, ok := stmt.(*ast.DeferStmt); ok && d.Call == n {
							deferred = true
						}
					}
					if !deferred {
						reject(n, "publication must follow issuance")
					}
				}
				if called == "withMetadataUpdate" || called == "updateBadgerMetadata" {
					ast.Inspect(n, func(child ast.Node) bool {
						if key, ok := child.(*ast.CallExpr); ok {
							if id, ok := key.Fun.(*ast.Ident); ok && (id.Name == "nodeKey" || id.Name == "edgeKey" || id.Name == "embeddingKey") {
								reject(key, "metadata route writes entity data")
							}
						}
						return true
					})
				}
			}
			return true
		})
		if isOwner("badger_managed.go", "managedBadgerDB", "commitShared") && (issueCalls != 1 || admissionCalls != 1 || durableAt == token.NoPos || issueAt <= durableAt) {
			reject(fn, "ordinary owner requires admission, durability, then issuance")
		}
		if isOwner("badger_cache_commit.go", "", "issueNodeCacheCommit") && cacheCalls != 1 {
			reject(fn, "issuer must call cache owner exactly once")
		}
		if isOwner("badger_cache_commit.go", "", "commitDurableWrite") && admissionCalls != 1 {
			reject(fn, "durable terminal requires admission")
		}
	}
	return problems
}

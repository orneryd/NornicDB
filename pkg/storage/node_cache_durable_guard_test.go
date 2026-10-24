package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
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
	aliases := map[string]bool{"nodeCacheWriteScope": true}
	ast.Inspect(file, func(n ast.Node) bool {
		if spec, ok := n.(*ast.TypeSpec); ok {
			if id, ok := spec.Type.(*ast.Ident); ok && aliases[id.Name] {
				aliases[spec.Name.Name] = true
			}
		}
		return true
	})
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
		terminalCalls := 0
		scopeVars := make(map[string]bool)
		if fn != nil {
			for _, field := range fn.Type.Params.List {
				if id, ok := field.Type.(*ast.Ident); ok && aliases[id.Name] {
					for _, id := range field.Names {
						scopeVars[id.Name] = true
					}
				}
			}
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			if assign, ok := n.(*ast.AssignStmt); ok {
				for i, rhs := range assign.Rhs {
					call, ok := rhs.(*ast.CallExpr)
					if !ok || i >= len(assign.Lhs) {
						continue
					}
					if id, ok := call.Fun.(*ast.Ident); ok && strings.HasSuffix(id.Name, "CacheWrites") {
						if lhs, ok := assign.Lhs[i].(*ast.Ident); ok {
							scopeVars[lhs.Name] = true
						}
					}
				}
			}
			return true
		})
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
				if typ, ok := n.Type.(*ast.Ident); ok && aliases[typ.Name] && name != "badger_cache_scope.go" {
					reject(n, "scope must be issued by its constructor")
				}
			case *ast.IncDecStmt:
				if field, ok := n.X.(*ast.SelectorExpr); ok && field.Sel.Name == "nodeCacheInvalidationTs" {
					reject(n, "copied fence increment")
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if field, ok := lhs.(*ast.SelectorExpr); ok {
						if id, ok := field.X.(*ast.Ident); ok && scopeVars[id.Name] && name != "badger_cache_scope.go" {
							reject(n, "scope fields are immutable outside their owner")
						}
					}
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
				case "DropPrefix", "DropAll", "Load":
					if raw, ok := n.X.(*ast.SelectorExpr); ok && raw.Sel.Name == "DB" {
						allowed = (n.Sel.Name == "DropPrefix" && isOwner("badger_managed.go", "managedBadgerDB", "dropPrefixWithScope")) ||
							(n.Sel.Name == "DropAll" && isOwner("badger_cache_commit.go", "managedBadgerDB", "DropAll")) ||
							(n.Sel.Name == "Load" && isOwner("badger_managed.go", "managedBadgerDB", "Load"))
					} else {
						protected = false
					}
				case "NewManagedWriteBatch", "NewStreamWriter", "SetEntryAt":
					allowed = false
				case "commitWithNodeCache":
					allowed = isOwner("badger_managed.go", "managedBadgerDB", "commit") || isOwner("badger_cache_commit.go", "managedBadgerDB", "commitMetadata")
				case "commitMetadata":
					allowed = isOwner("badger_cache_commit.go", "managedBadgerDB", "updateMetadata") || isOwner("badger_cache_commit.go", "metadataBatches", "commitBatch")
				case "invalidateRelationshipCacheScope":
					allowed = isOwner("badger_cache.go", "BadgerEngine", "cacheCommitNodeWrites")
				case "CommitAt":
					allowed = isOwner("badger_cache_commit.go", "", "commitDurableWrite")
					if calls[n] != nil {
						durableAt = n.Pos()
						terminalCalls++
					}
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
					terminalCalls++
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
					inspect := func(root ast.Node) {
						ast.Inspect(root, func(child ast.Node) bool {
							if key, ok := child.(*ast.CallExpr); ok {
								if id, ok := key.Fun.(*ast.Ident); ok && (id.Name == "nodeKey" || id.Name == "edgeKey" || id.Name == "embeddingKey") {
									reject(key, "metadata route writes entity data")
								}
							}
							return true
						})
					}
					inspect(n)
					for _, arg := range n.Args {
						if id, ok := arg.(*ast.Ident); ok && id.Obj != nil {
							inspect(id.Obj.Decl.(ast.Node))
						}
					}
				}
			}
			return true
		})
		if isOwner("badger_managed.go", "managedBadgerDB", "commitShared") && (terminalCalls != 1 || admissionCalls != 1) {
			reject(fn, "ordinary owner requires the issuing durable terminal")
		}
		if isOwner("badger_cache_commit.go", "", "issueNodeCacheCommit") && cacheCalls != 1 {
			reject(fn, "issuer must call cache owner exactly once")
		}
		if isOwner("badger_cache_commit.go", "", "commitDurableWrite") && (admissionCalls != 1 || terminalCalls != 1 || issueCalls != 1 || issueAt <= durableAt) {
			reject(fn, "durable terminal requires admission, durability, then issuance")
		}
		if isOwner("badger_managed.go", "largeCommit", "commitBatch") || isOwner("badger_managed.go", "restoreBatches", "commitBatch") || isOwner("badger_managed.go", "managedBadgerDB", "beginLargeCommit") {
			if terminalCalls != 1 {
				reject(fn, "large and rollback batches require the issuing durable terminal")
			}
		}
	}
	return problems
}

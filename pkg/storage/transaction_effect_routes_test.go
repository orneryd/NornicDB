// Author: Claude, NOR-180 completeness of the transaction effect signal, 2026-10-31.
package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// transactionEffectFields hold what a transaction's commit publishes. Every other
// field of BadgerTransaction must be named in transactionStateFields, so a new field
// can't join the transaction without someone deciding which it is.
var transactionEffectFields = strings.Fields(`
	operations connectedDeletes pendingWrites pendingDeletes pendingNodes pendingEdges
	deletedNodes deletedEdges deferredEdgeWrites
	cancelledEdgeWrites edgeCreateNoOps droppedEmbeddings pendingLabelCountDeltas
	pendingEdgeTypeCountDeltas pendingEdgeTypeLabelCountDeltas knowledgeSchemaDefinition
	knowledgeSchemaDirty schemaRuntime Metadata preparedEntityCommit`)

// transactionStateFields are read caches, lifecycle, snapshot and configuration
// state of BadgerTransaction, which no commit publishes as the statement's effect.
// pendingAdjacency and pendingIndex are read views a read may build from pendingEdges
// and pendingNodes, which are effects.
var transactionStateFields = strings.Fields(`
	pendingAdjacency pendingIndex
	CommitVersion ID StartTime Status badgerDB badgerReadHeld badgerReadTs badgerTx
	beginPresence beginSnapshot bufferEpoch bufferedAck bufferedStagingTransferred
	changeCapture clientDetached closedErr commitCountsHeld commitCtx commitEdgeTypeCounts
	commitLogicalID commitMode commitReleaseAcceptedAuthority commitReleaseWrite commitW
	deferConstraintValidation effects engine knowledgeSchema lifecycleSeq mergeKeyReleases
	mu namespace nodeHeadCache observed readTS schemaTransaction skipCreateExistenceCheck
	snapshotDeregister snapshotEdgeByID snapshotEdgeBytes snapshotEdgeBytesByID
	snapshotEdgeOrder snapshotEquality snapshotIncomingAdjacency snapshotLabelNodeByID
	snapshotLabelNodes snapshotLabelPrefixBytes snapshotLabelPrefixNodeBytes
	snapshotLabelPrefixNodes snapshotNodeLabelBytes snapshotNodeLabels
	snapshotOutgoingAdjacency snapshotPrefixNodeByID snapshotPrefixNodeBytes
	snapshotPrefixNodeBytesByID snapshotPrefixNodeOrder snapshotProjectedLabelNodes
	snapshotReaderInfo snapshotTx writeAdmission`)

// transactionEffectBoundaries are where a transaction ends: no statement is open, so
// what they stage can't belong to one. The walk stops at them. PrepareEntityCommit
// stages its receipt and publishes in the same call.
var transactionEffectBoundaries = map[string]string{
	"BadgerTransaction.Commit":              "ends the transaction",
	"BadgerTransaction.CommitContext":       "ends the transaction",
	"BadgerTransaction.PublishCommit":       "ends the transaction",
	"BadgerTransaction.Rollback":            "ends the transaction",
	"BadgerTransaction.closeLocked":         "ends the transaction and clears its state",
	"BadgerTransaction.abortCommitLocked":   "ends a failed commit",
	"BadgerTransaction.PrepareEntityCommit": "stages its receipt and publishes in one call",
}

// transactionEffectOwnRoots are reached only from a counted route or an engine's own
// transaction, and have no caller the walk can resolve.
var transactionEffectOwnRoots = map[string]string{
	"BadgerEngine.executeNodeDeletion": "reached from DeleteGraphNode, which counts, or the engine's own transaction",
}

type transactionEffectFunc struct {
	name   string
	body   *ast.BlockStmt
	bases  map[string]bool
	stages []string
	counts bool
	calls  []string
}

// loadTransactionEffectFuncs reads this package's source: each function, the
// variables that name a transaction in it, and what it stages, counts and calls.
func loadTransactionEffectFuncs(t *testing.T) (map[string]*transactionEffectFunc, []string) {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fileSet := token.NewFileSet()
	effect := map[string]bool{}
	for _, field := range transactionEffectFields {
		effect[field] = true
	}
	funcs := map[string]*transactionEffectFunc{}
	var fields []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			if general, ok := declaration.(*ast.GenDecl); ok {
				for _, spec := range general.Specs {
					typed, ok := spec.(*ast.TypeSpec)
					if !ok || typed.Name.Name != "BadgerTransaction" {
						continue
					}
					for _, field := range typed.Type.(*ast.StructType).Fields.List {
						for _, name := range field.Names {
							fields = append(fields, name.Name)
						}
					}
				}
			}
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			entry := &transactionEffectFunc{name: "." + function.Name.Name, body: function.Body, bases: map[string]bool{"tx": true}}
			addBases := func(list *ast.FieldList) string {
				found := ""
				if list == nil {
					return found
				}
				for _, field := range list.List {
					star, ok := field.Type.(*ast.StarExpr)
					if !ok {
						continue
					}
					typeName, ok := star.X.(*ast.Ident)
					if !ok {
						continue
					}
					found = typeName.Name
					if typeName.Name == "BadgerTransaction" || typeName.Name == "stagedKV" {
						for _, name := range field.Names {
							entry.bases[name.Name] = true
						}
					}
				}
				return found
			}
			if receiver := addBases(function.Recv); receiver != "" {
				entry.name = receiver + "." + function.Name.Name
			}
			addBases(function.Type.Params)
			funcs[entry.name] = entry
		}
	}
	for _, entry := range funcs {
		root := func(expression ast.Expr) (string, bool) {
			for {
				switch current := expression.(type) {
				case *ast.IndexExpr:
					expression = current.X
				case *ast.SelectorExpr:
					base, ok := current.X.(*ast.Ident)
					return current.Sel.Name, ok && entry.bases[base.Name]
				default:
					return "", false
				}
			}
		}
		stage := func(expression ast.Expr) {
			if field, ok := root(expression); ok && effect[field] {
				entry.stages = append(entry.stages, field)
			}
		}
		ast.Inspect(entry.body, func(node ast.Node) bool {
			switch current := node.(type) {
			case *ast.AssignStmt:
				for _, target := range current.Lhs {
					stage(target)
				}
			case *ast.IncDecStmt:
				stage(current.X)
			case *ast.CallExpr:
				if name, ok := current.Fun.(*ast.Ident); ok && name.Name == "delete" && len(current.Args) > 0 {
					stage(current.Args[0])
				}
				selector, ok := current.Fun.(*ast.SelectorExpr)
				if !ok {
					break
				}
				if base, ok := selector.X.(*ast.SelectorExpr); ok && base.Sel.Name == "badgerTx" {
					switch selector.Sel.Name {
					case "Set", "SetEntry", "Delete":
						entry.stages = append(entry.stages, "native."+selector.Sel.Name)
					}
				}
				if field, ok := root(selector.X); ok && field == "effects" && selector.Sel.Name == "Add" {
					entry.counts = true
				}
				if base, ok := selector.X.(*ast.Ident); ok && entry.bases[base.Name] {
					switch selector.Sel.Name {
					case "admitWriteLocked", "NoteEffect":
						entry.counts = true
					case "ensureOperationAllowedLocked":
						// A read's admission reaches no count; a write's does.
						if len(current.Args) == 1 {
							if argument, ok := current.Args[0].(*ast.Ident); ok && (argument.Name == "transactionDataWrite" || argument.Name == "transactionSchemaWrite") {
								entry.counts = true
							}
						}
					}
				}
			case *ast.SelectorExpr:
				if base, ok := current.X.(*ast.Ident); ok && entry.bases[base.Name] {
					entry.calls = append(entry.calls, "BadgerTransaction."+current.Sel.Name)
				}
			case *ast.Ident:
				entry.calls = append(entry.calls, "."+current.Name)
			}
			return true
		})
	}
	return funcs, fields
}

// Every field is classified, and from every function that stages committed state,
// every chain of callers reaches a counted route, the end of the transaction, or a
// reviewed root. The table of roots is exact, so a new route fails here.
func TestEveryTransactionWriteRouteMovesTheEffectSignal(t *testing.T) {
	funcs, fields := loadTransactionEffectFuncs(t)

	state := map[string]bool{}
	for _, field := range transactionStateFields {
		state[field] = true
	}
	known := map[string]bool{}
	for _, field := range transactionEffectFields {
		known[field] = true
	}
	for _, field := range fields {
		require.True(t, known[field] != state[field], "BadgerTransaction.%s must be exactly one of effect or state", field)
	}
	require.Len(t, fields, len(state)+len(known), "the tables name a field the transaction no longer has")

	callers := map[string][]string{}
	for name, entry := range funcs {
		for _, callee := range entry.calls {
			if _, ok := funcs[callee]; ok && callee != name {
				callers[callee] = append(callers[callee], name)
			}
		}
	}
	roots := map[string]bool{}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		if funcs[name].counts {
			return
		}
		if _, boundary := transactionEffectBoundaries[name]; boundary {
			return
		}
		method := name[strings.LastIndex(name, ".")+1:]
		if ast.IsExported(method) && strings.HasPrefix(name, "BadgerTransaction.") || len(callers[name]) == 0 {
			roots[name] = true
		}
		for _, caller := range callers[name] {
			walk(caller)
		}
	}
	sites := 0
	for name, entry := range funcs {
		if len(entry.stages) > 0 {
			sites++
			walk(name)
		}
	}
	require.Greater(t, sites, 20, "the source walk found the staging functions")
	var got, want []string
	for name := range roots {
		got = append(got, name)
	}
	for name := range transactionEffectOwnRoots {
		want = append(want, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	require.Equal(t, want, got, "a route stages committed state with no counted route above it")
}

// Each public write moves the signal, also over a pending image; reads don't.
func TestTransactionWritesMoveTheEffectSignal(t *testing.T) {
	engine := createTestBadgerEngine(t)
	begin := func() *BadgerTransaction {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace("fx"))
		return tx
	}
	seed := begin()
	for _, id := range []NodeID{"fx:a", "fx:b", "fx:c"} {
		_, err := seed.CreateNode(&Node{ID: id, Labels: []string{"A"}})
		require.NoError(t, err)
	}
	require.NoError(t, seed.CreateEdge(&Edge{ID: "fx:ab", StartNode: "fx:a", EndNode: "fx:b", Type: "R"}))
	require.NoError(t, seed.Commit())

	tx := begin()
	moves := func(name string, do func() error) {
		t.Helper()
		before := tx.EffectSerial()
		require.NoError(t, do(), name)
		require.Greater(t, tx.EffectSerial(), before, name)
	}
	still := func(name string, do func() error) {
		t.Helper()
		before := tx.EffectSerial()
		require.NoError(t, do(), name)
		require.Equal(t, before, tx.EffectSerial(), name)
	}
	view, err := tx.Schema(KnowledgePolicyChanges)
	require.NoError(t, err)
	still("reading a node", func() error { _, err := tx.GetNode("fx:a"); return err })
	still("reading an edge", func() error { _, err := tx.GetEdge("fx:ab"); return err })
	still("reading the knowledge schema", func() error { _, err := tx.Schema(KnowledgePolicyChanges); return err })
	still("reading the count", func() error { _ = tx.OperationCount(); return nil })

	moves("a knowledge-policy change", func() error {
		return view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{Name: "fx", TargetLabels: []string{"A"}, Enabled: true})
	})
	moves("transaction metadata", func() error { return tx.SetMetadata(map[string]interface{}{"k": "v"}) })
	moves("the write admission", tx.AdmitWrite)
	moves("creating a node", func() error { _, err := tx.CreateNode(&Node{ID: "fx:new", Labels: []string{"A"}}); return err })
	moves("updating a pending node", func() error {
		count := tx.OperationCount()
		err := tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "fx:new", Labels: []string{"A"}, Properties: map[string]interface{}{"p": 1}}})
		require.Equal(t, count, tx.OperationCount(), "the pending image was replaced in place")
		return err
	})
	moves("updating a node", func() error {
		return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "fx:a", Labels: []string{"A"}, Properties: map[string]interface{}{"p": 1}}})
	})
	moves("creating an edge", func() error { return tx.CreateEdge(&Edge{ID: "fx:bc", StartNode: "fx:b", EndNode: "fx:c", Type: "R"}) })
	moves("deleting an edge", func() error { return tx.DeleteEdge("fx:ab") })
	moves("deleting a node", func() error { return tx.DeleteGraphNode("fx:c", CascadeEdges) })

	schema := begin()
	before := schema.EffectSerial()
	_, err = schema.Schema(OrdinarySchemaChanges)
	require.NoError(t, err)
	require.Greater(t, schema.EffectSerial(), before, "an ordinary schema view claims a write")
}

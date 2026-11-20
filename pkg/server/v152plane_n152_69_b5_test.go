package server

// NOR-287 Heimdall count-consumer caller inventory.
//
// The earlier NOR-287 audit recorded that heimdallDBRouter.Stats
// (server_heimdall.go:322-336) discards both native count errors and that no
// in-repository caller was found by a literal ".Stats(ctx" search. A literal
// regex only matches that exact spelling, so this inventory derives the caller
// population from the syntax tree instead:
//
//   - every selector call named Stats with two arguments, whatever the receiver
//     expression and whatever the argument expressions are (this is what the
//     regex missed: ctx.Context, ...),
//   - every two-argument Stats method implementation,
//   - every interface declaration of a two-argument Stats method,
//   - every struct field and function parameter whose declared type is
//     DatabaseRouter, i.e. every place a router value can be held or handed to
//     code that can call Stats,
//   - every non-call reference to Stats through such a held value (a method
//     value), which a call-site search cannot see.
//
// Test files are excluded because they are not production callers. The result
// is a ratchet: the population below is recorded together with the externally
// visible consequence of a fabricated answer. If a caller appears, this guard
// fails and the reviewer must establish whether the discarded count error
// reaches an externally reported answer.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// nor287StatsCall is one two-argument Stats call with its enclosing owner.
type nor287StatsCall struct {
	File     string
	Owner    string
	Receiver string
}

// key identifies the call owner without a shifting line number.
func (c nor287StatsCall) key() string { return c.File + ":" + c.Owner + " " + c.Receiver + ".Stats" }

// nor287StatsReference is a method-value reference to Stats.
type nor287StatsReference struct {
	File     string
	Owner    string
	Receiver string
	Holder   string
}

func (r nor287StatsReference) key() string {
	return r.File + ":" + r.Owner + " " + r.Receiver + ".Stats"
}

// nor287StatsInventory is the syntax-derived population.
type nor287StatsInventory struct {
	calls           []nor287StatsCall
	references      []nor287StatsReference
	implementations []string
	declarations    []string
	holders         []string
	parameters      []string
	holderNames     map[string]bool
}

// nor287Owner is one function/method declaration range used to attribute a
// call to its enclosing owner.
type nor287Owner struct {
	start, end token.Pos
	name       string
}

// nor287IsDatabaseRouterType reports whether expr denotes the Heimdall
// DatabaseRouter interface, qualified or not.
func nor287IsDatabaseRouterType(expr ast.Expr) bool {
	text := types.ExprString(expr)
	return text == "DatabaseRouter" || strings.HasSuffix(text, ".DatabaseRouter")
}

// nor287ReceiverName returns the final identifier of a receiver expression.
func nor287ReceiverName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	}
	return ""
}

// nor287EnclosingOwner names the innermost function/method containing pos.
func nor287EnclosingOwner(owners []nor287Owner, pos token.Pos) string {
	name := "<package scope>"
	best := token.NoPos
	for _, owner := range owners {
		if pos < owner.start || pos > owner.end {
			continue
		}
		if best == token.NoPos || owner.start > best {
			best = owner.start
			name = owner.name
		}
	}
	return name
}

// nor287RepositoryRoot finds the checkout containing this test file.
func nor287RepositoryRoot() (string, error) {
	directory, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fs.ErrNotExist
		}
		directory = parent
	}
}

// nor287Record adds one sortable "file:owner detail" entry.
func nor287Record(entries *[]string, file, detail string) {
	*entries = append(*entries, file+":"+detail)
}

// nor287ScanStatsInventory parses every production Go file below root.
func nor287ScanStatsInventory(t *testing.T, root string) *nor287StatsInventory {
	t.Helper()

	inventory := &nor287StatsInventory{holderNames: make(map[string]bool)}
	err := fixtureowner.WalkRepositoryTree(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules", ".verify", "ui", "docs-site":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		relative = filepath.ToSlash(relative)

		fileSet := token.NewFileSet()
		parsed, parseErr := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		var owners []nor287Owner
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			owners = append(owners, nor287Owner{start: function.Pos(), end: function.End(), name: function.Name.Name})
		}

		// Selector expressions used as a callee are calls, not method values.
		callees := make(map[*ast.SelectorExpr]bool)
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Stats" {
				return true
			}
			callees[selector] = true
			if len(call.Args) != 2 {
				return true
			}
			inventory.calls = append(inventory.calls, nor287StatsCall{
				File:     relative,
				Owner:    nor287EnclosingOwner(owners, call.Pos()),
				Receiver: types.ExprString(selector.X),
			})
			return true
		})

		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				if typed.Sel.Name != "Stats" || callees[typed] {
					return true
				}
				inventory.references = append(inventory.references, nor287StatsReference{
					File:     relative,
					Owner:    nor287EnclosingOwner(owners, typed.Pos()),
					Receiver: types.ExprString(typed.X),
					Holder:   nor287ReceiverName(typed.X),
				})
			case *ast.FuncDecl:
				if typed.Name.Name == "Stats" && typed.Recv != nil && typed.Type.Params != nil && len(typed.Type.Params.List) == 2 {
					receiver := "unknown"
					if len(typed.Recv.List) > 0 {
						receiver = types.ExprString(typed.Recv.List[0].Type)
					}
					nor287Record(&inventory.implementations, relative, receiver+".Stats")
				}
				if typed.Type.Params == nil {
					return true
				}
				for _, parameter := range typed.Type.Params.List {
					if !nor287IsDatabaseRouterType(parameter.Type) {
						continue
					}
					for _, name := range parameter.Names {
						nor287Record(&inventory.parameters, relative, typed.Name.Name+" "+name.Name)
					}
				}
			case *ast.InterfaceType:
				for _, method := range typed.Methods.List {
					if len(method.Names) != 1 || method.Names[0].Name != "Stats" {
						continue
					}
					signature, ok := method.Type.(*ast.FuncType)
					if !ok || signature.Params == nil || len(signature.Params.List) != 2 {
						continue
					}
					nor287Record(&inventory.declarations, relative, "interface Stats(a, b)")
				}
			case *ast.TypeSpec:
				structType, ok := typed.Type.(*ast.StructType)
				if !ok {
					return true
				}
				for _, field := range structType.Fields.List {
					if !nor287IsDatabaseRouterType(field.Type) {
						continue
					}
					if len(field.Names) == 0 {
						nor287Record(&inventory.holders, relative, typed.Name.Name+" embedded DatabaseRouter")
						inventory.holderNames["DatabaseRouter"] = true
						continue
					}
					for _, name := range field.Names {
						nor287Record(&inventory.holders, relative, typed.Name.Name+"."+name.Name)
						inventory.holderNames[name.Name] = true
					}
				}
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)

	sort.Slice(inventory.calls, func(i, j int) bool { return inventory.calls[i].key() < inventory.calls[j].key() })
	sort.Slice(inventory.references, func(i, j int) bool { return inventory.references[i].key() < inventory.references[j].key() })
	sort.Strings(inventory.implementations)
	sort.Strings(inventory.declarations)
	sort.Strings(inventory.holders)
	sort.Strings(inventory.parameters)
	return inventory
}

// nor287HeldStatsReferences keeps method-value references made through a value
// that a DatabaseRouter holder can expose; field accesses on unrelated Result
// types also end in ".Stats" and are not callers.
func nor287HeldStatsReferences(inventory *nor287StatsInventory) []string {
	keys := make([]string, 0, len(inventory.references))
	for _, reference := range inventory.references {
		if !inventory.holderNames[reference.Holder] {
			continue
		}
		keys = append(keys, reference.key())
	}
	sort.Strings(keys)
	return keys
}

// nor287StatsCallAllowlist records every production caller of a two-argument
// Stats method with the externally visible consequence of a fabricated answer.
// Test callers are excluded; the diagnostic audit and the Heimdall access tests
// are not production consumers.
var nor287StatsCallAllowlist = map[string]string{
	"plugins/heimdall/conversation_turn.go:privateConversationPrompt database.Stats": "a fabricated count is written into the assistant prompt context as 'Database statistics: 0 nodes, 0 relationships'",
	"plugins/heimdall/plugin.go:actionRepoMap ctx.Database.Stats":                    "a fabricated count lets the repository-map action answer Success with zero graph statistics",
	"plugins/heimdall/plugin.go:actionStatus ctx.Database.Stats":                     "a fabricated count is published in the plugin status payload as database nodes/relationships",
	"plugins/heimdall/plugin.go:actionDBStats ctx.Database.Stats":                    "a fabricated count is published by the database-stats action as a successful zero-node database",
}

// TestNOR287HeimdallStatsCallerInventory records the production population that
// can reach a two-argument Stats method and fails if it changes.
func TestNOR287HeimdallStatsCallerInventory(t *testing.T) {
	root, err := nor287RepositoryRoot()
	require.NoError(t, err)

	inventory := nor287ScanStatsInventory(t, root)
	observed := make(map[string]bool, len(inventory.calls))
	for _, call := range inventory.calls {
		t.Logf("two-argument Stats caller: %s", call.key())
		observed[call.key()] = true
	}
	for _, entry := range inventory.holders {
		t.Logf("DatabaseRouter holder: %s", entry)
	}
	for _, entry := range inventory.parameters {
		t.Logf("DatabaseRouter parameter: %s", entry)
	}
	for _, entry := range inventory.implementations {
		t.Logf("two-argument Stats implementation: %s", entry)
	}
	for _, entry := range inventory.declarations {
		t.Logf("two-argument Stats contract: %s", entry)
	}
	for _, entry := range nor287HeldStatsReferences(inventory) {
		t.Logf("held Stats method value: %s", entry)
	}

	for caller, reason := range nor287StatsCallAllowlist {
		require.True(t, observed[caller], "recorded caller %q (%s) no longer exists; update this inventory", caller, reason)
	}
	require.Len(t, inventory.calls, len(nor287StatsCallAllowlist),
		"an unrecorded production caller of a two-argument Stats method appeared; establish whether the discarded count error reaches an externally reported answer, then record its consequence")

	require.Equal(t, []string{"pkg/server/server_heimdall.go:*heimdallDBRouter.Stats"}, inventory.implementations,
		"the set of two-argument Stats implementations changed")
	require.Equal(t, []string{"pkg/heimdall/plugin.go:interface Stats(a, b)"}, inventory.declarations,
		"the Heimdall DatabaseRouter.Stats contract declaration changed")
	require.Empty(t, nor287HeldStatsReferences(inventory),
		"a Stats method value held through a DatabaseRouter field appeared; establish whether the discarded count error reaches an externally reported answer")
}

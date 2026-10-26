package storage_test

import (
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func engineMember(typ types.Type, core *types.Interface) bool {
	typ = types.Unalias(typ)
	if _, iface := typ.Underlying().(*types.Interface); iface {
		return false
	}
	return types.Implements(typ, core) || types.Implements(types.NewPointer(typ), core)
}

// Discover Engine first, including value/pointer/promoted/alias method sets.
// There is deliberately no ReadCapabilitySource admission prerequisite.
func TestEnginePopulationCoverage(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	loaded, _, err := capabilitycensus.Packages("../..")
	require.NoError(t, err)
	var core, decl *types.Interface
	for _, pkg := range loaded {
		require.Empty(t, pkg.Errors, pkg.PkgPath)
		if pkg.PkgPath == "github.com/orneryd/nornicdb/pkg/storage" {
			core = pkg.Types.Scope().Lookup("Engine").Type().Underlying().(*types.Interface)
			decl = pkg.Types.Scope().Lookup("EngineBoundaryDeclaration").Type().Underlying().(*types.Interface)
		}
	}
	require.NotNil(t, core)
	require.NotNil(t, decl)
	adapters := map[string]int{}
	publicAdapters := map[string]int{}
	// This finite registry is read from real compiling test adapters. Adding a
	// string alone cannot satisfy it: both executable Factory and Oracle fields
	// are required, and the package's adapter test executes each entry.
	registry, err := capabilitycensus.SourceSyntax("../..", true)
	require.NoError(t, err)
	for _, member := range registry.Members {
		if member.Syntax == nil {
			continue
		}
		path, _, _ := strings.Cut(member.ID, ":")
		base := filepath.Base(path)
		if base != "engine_population_contract_test.go" && base != "foundation_public_obligation_test.go" && base != "foundation_external_obligation_test.go" {
			continue
		}
		func(node ast.Node) {
			declaration, ok := node.(*ast.ValueSpec)
			if !ok {
				return
			}
			for i, name := range declaration.Names {
				if name.Name != "publicEngineObligationFactories" {
					continue
				}
				registry, ok := declaration.Values[i].(*ast.CompositeLit)
				require.True(t, ok)
				for _, entry := range registry.Elts {
					pair := entry.(*ast.KeyValueExpr)
					key := pair.Key.(*ast.BasicLit)
					name, err := strconv.Unquote(key.Value)
					require.NoError(t, err)
					require.NotEqual(t, "nil", expressionName(pair.Value), "public obligation needs executable adapter")
					publicAdapters[name]++
				}
			}
			return
		}(member.Syntax.Node)
		func(node ast.Node) {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return
			}
			m, ok := lit.Type.(*ast.MapType)
			if !ok {
				return
			}
			selector, ok := m.Value.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Adapter" {
				return
			}
			for _, element := range lit.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				require.True(t, ok)
				key, ok := pair.Key.(*ast.BasicLit)
				require.True(t, ok)
				name, err := strconv.Unquote(key.Value)
				require.NoError(t, err)
				adapter, ok := pair.Value.(*ast.CompositeLit)
				require.True(t, ok)
				fields := map[string]bool{}
				for _, element := range adapter.Elts {
					field := element.(*ast.KeyValueExpr)
					id := field.Key.(*ast.Ident)
					fields[id.Name] = true
					require.NotEqual(t, "nil", strings.TrimSpace(expressionName(field.Value)))
				}
				require.True(t, fields["Factory"] && fields["Oracle"] && fields["Stored"], "%s needs an executable factory and independent oracle", name)
				adapters[name]++
			}
			return
		}(member.Syntax.Node)
	}
	require.NoError(t, err)
	var members []string
	for key, object := range result.Types {
		members = append(members, key)
		typ := types.Unalias(object.Type())
		named, ok := typ.(*types.Named)
		require.True(t, ok, "concrete named Engine required")
		ownDeclaration := false
		ownStoredDeclaration := false
		for i := 0; i < named.NumMethods(); i++ {
			if named.Method(i).Name() == "StoredReadPolicy" {
				ownStoredDeclaration = true
			}
			if named.Method(i).Name() == "EngineCapabilityPolicy" {
				ownDeclaration = true
			}
		}
		require.True(t, ownStoredDeclaration, "%s must explicitly declare stored modes; promoted read transparency is not a fact contract", key)
		require.True(t, ownDeclaration, "%s must declare its own classification/logical policy; a promoted declaration cannot classify a new view", key)
		require.True(t, types.Implements(typ, decl) || types.Implements(types.NewPointer(typ), decl), "%s has no structural/logical declaration", key)
		require.Equal(t, 1, adapters[key], "%s needs exactly one registered factory/oracle", key)
		require.Equal(t, 1, publicAdapters[key], "%s needs exactly one executable public logical-obligation adapter", key)
		delete(adapters, key)
		delete(publicAdapters, key)
	}
	require.Empty(t, adapters, "stale/duplicate adapters")
	require.Empty(t, publicAdapters, "stale/duplicate public obligation adapters")
	sort.Strings(members)
	for _, name := range members {
		t.Log(name)
	}
	require.NotEmpty(t, members)
}

func expressionName(expr ast.Expr) string {
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return "executable"
}

func TestEnginePopulationNegativeControls(t *testing.T) {
	// A synthetic wrapper with promoted Engine and no optional opt-in is a
	// structural member. Alias and value method sets must reach the same gate.
	pkg := types.NewPackage("control", "control")
	method := types.NewFunc(token.NoPos, pkg, "Core", types.NewSignatureType(nil, nil, nil, types.NewTuple(), types.NewTuple(), false))
	core := types.NewInterfaceType([]*types.Func{method}, nil).Complete()
	embedded := types.NewField(token.NoPos, pkg, "Engine", core, true)
	wrapper := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "Undeclared", nil), types.NewStruct([]*types.Var{embedded}, nil), nil)
	require.True(t, engineMember(wrapper, core))
	alias := types.NewAlias(types.NewTypeName(token.NoPos, pkg, "Alias", nil), wrapper)
	require.True(t, engineMember(alias, core))
	require.False(t, types.Implements(types.NewPointer(wrapper), types.NewInterfaceType([]*types.Func{types.NewFunc(token.NoPos, pkg, "EngineCapabilityPolicy", method.Type().(*types.Signature))}, nil).Complete()))
	// Same method set, missing logical lease: declaration gate rejects it.
	policy := (&storage.MemoryEngine{}).EngineCapabilityPolicy()
	policy.ReadLease = 0
	require.NotEmpty(t, enginecontract.PolicyProblems(policy))
}

func TestEnginePopulationLogicalBindingNegativeControl(t *testing.T) {
	_, expected, _ := populationMemberSeed(nil)
	actual := expected
	actual.ReadLease = nil
	require.Contains(t, enginecontract.BindingProblems(expected, actual), "lost or replaced ReadLease owner")
	actual = expected
	actual.ReadLease = new(int)
	require.Contains(t, enginecontract.BindingProblems(expected, actual), "lost or replaced ReadLease owner", "equal copied state is not the causal owner")
	require.Empty(t, enginecontract.BindingProblems(expected, expected))
}

func TestEnginePopulationOutsidePackageFileControl(t *testing.T) {
	// A real typed module keeps the control small while exercising the SAME
	// maintained-source, go/types and canonical validation path as production.
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/orneryd/nornicdb\ngo 1.26.0\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "pkg", "storage"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "pkg", "storage", "engine.go"), []byte("package storage\ntype Engine interface{Core()}\ntype Base struct{}\nfunc(*Base)Core(){}\nfunc BindMembership(e Engine)Engine{return e}\ntype NodeID string\ntype StoredReadRequest struct{Nodes []NodeID}\ntype TransactionCommitter interface{CommitTransaction()error}\n"), 0644))
	baseline, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	old, err := capabilitycensus.Interpret(baseline)
	require.NoError(t, err)
	require.Len(t, old.Types, 1)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "plugins", "unfamiliar"), 0755))
	path := filepath.Join(root, "plugins", "unfamiliar", "new_view.go")
	require.NoError(t, os.WriteFile(path, []byte(`package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
import "slices"
type Arbitrary struct{storage.Engine}
type Alias = Arbitrary
func hiddenFailure()error{_ = storage.BindMembership(nil);return nil}
func failureConsumer(){_ = hiddenFailure()}
func hiddenCommitOwner()storage.TransactionCommitter{return nil}
func commitOwnerConsumer(){_ = hiddenCommitOwner()}
func hiddenProvider()storage.Engine{return storage.BindMembership(nil)}
func providerConsumer(){_ = hiddenProvider()}
func hiddenQuadratic(request storage.StoredReadRequest){for _,id:=range request.Nodes{_ = slices.Contains(request.Nodes,id)}}
`), 0644))
	changed, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	actual, err := capabilitycensus.Interpret(changed)
	require.NoError(t, err)
	for _, name := range []string{"Arbitrary", "Alias"} {
		require.NotNil(t, actual.Types["github.com/orneryd/nornicdb/plugins/unfamiliar."+name], "promoted/alias Engine in a new outside-package file escaped")
	}
	lookupOwners := map[string]bool{}
	for _, site := range actual.Sites {
		if site.Population == "lookups" {
			lookupOwners[site.Function] = true
		}
	}
	require.True(t, lookupOwners["github.com/orneryd/nornicdb/plugins/unfamiliar.hiddenFailure"], "the actual lookup inside an error-returning operation remains covered")
	require.False(t, lookupOwners["github.com/orneryd/nornicdb/plugins/unfamiliar.failureConsumer"], "an error return is not a capability binding")
	require.True(t, lookupOwners["github.com/orneryd/nornicdb/plugins/unfamiliar.providerConsumer"], "an opaque real Engine provider remains a covered alias")
	commitOwners := map[string]bool{}
	for _, site := range actual.Sites {
		if site.Population == "commit_owners" {
			commitOwners[site.Function] = true
		}
	}
	for _, name := range []string{"hiddenCommitOwner", "commitOwnerConsumer"} {
		require.True(t, commitOwners["github.com/orneryd/nornicdb/plugins/unfamiliar."+name], "an arbitrary typed commit-provider name and its consumer remain in the complete population")
	}
	require.Contains(t, strings.Join(capabilitycensus.Problems(actual.Sites), "\n"), "stored request membership scan inside an entity loop", "a new opaque outside-package quadratic consumer must fail the causal property")
	var registered, discovered []capabilitycensus.Site
	for _, site := range old.Sites {
		if site.Population == "stored_views" {
			site.Disposition = "distinct responsibility"
			site.Reason = "Synthetic baseline Base declares the fixture Core method; this exact type is the known control input for detecting a new promoted/alias Engine. It is not a production view approval."
			registered = append(registered, site)
		}
	}
	require.Empty(t, capabilitycensus.CoverageProblems(registered, registered), "the controlled baseline must pass before the outside-package member is planted")
	for _, site := range actual.Sites {
		if site.Population == "stored_views" {
			discovered = append(discovered, site)
		}
	}
	failures := strings.Join(capabilitycensus.CoverageProblems(discovered, registered), "\n")
	require.Contains(t, failures, "plugins/unfamiliar/new_view.go:")
	require.NotEmpty(t, capabilitycensus.SourceProblems(changed.Sources, baseline.Sources), "new source and new Engine must both fail until explicitly registered")
}

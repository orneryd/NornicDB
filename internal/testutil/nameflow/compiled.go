//go:build nameflowcensus

package nameflow

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// CompiledQueries records code membership, never semantic approval. Sites come
// from the shared loaded program's executable SSA and compiler field objects.
type CompiledQueries struct {
	Owners            []string            `json:"owners"`
	CodeSites         map[string][]string `json:"code_sites"`
	OwnerCalls        map[string][]string `json:"owner_calls"`
	FieldAccesses     map[string][]string `json:"field_accesses"`
	Implementations   map[string][]string `json:"implementations"`
	InterfaceTargets  map[string][]string `json:"interface_targets"`
	TextOnly          []workcensus.Member `json:"text_only"`
	SourceOnlyFlow    []workcensus.Member `json:"source_only_flow"`
	NoInstructionFlow []workcensus.Member `json:"no_instruction_flow"`
}

func declaredFieldKey(typ types.Type, index int) string {
	if p, ok := typ.(*types.Pointer); ok {
		typ = p.Elem()
	}
	if named, ok := types.Unalias(typ).(*types.Named); ok && named.Obj().Pkg() != nil {
		if structure, ok := named.Underlying().(*types.Struct); ok && index < structure.NumFields() {
			return "field:" + named.Obj().Pkg().Path() + "." + named.Obj().Name() + "." + structure.Field(index).Name()
		}
	}
	return ""
}

// AnalyzeCompiled retains only executable compiled code members. The original
// Analyze remains exploratory input/control evidence; source-only Go hits do
// not enter this population. The name def/use interpretation still requires
// semantic review and may conservatively union fields and call contexts.
func AnalyzeCompiled(input workcensus.Inventory, contracts Contracts) Result {
	return analyzeCompiled(input, contracts, true)
}

// AnalyzeRecordingCalls enumerates exact compiler-resolved callers of the
// supplied construction owners. It does not run exploratory name-flow analysis.
func AnalyzeRecordingCalls(input workcensus.Inventory, contracts Contracts) Result {
	return analyzeCompiled(input, Contracts{RecordingOwners: contracts.RecordingOwners}, false)
}

func analyzeCompiled(input workcensus.Inventory, contracts Contracts, includeFlow bool) Result {
	if input.Program == nil {
		panic("compiled name queries require the shared Go program")
	}
	raw := input.Members
	input.Members = nil
	query := &CompiledQueries{CodeSites: map[string][]string{}, OwnerCalls: map[string][]string{}, FieldAccesses: map[string][]string{}, Implementations: map[string][]string{}, InterfaceTargets: map[string][]string{}}
	byNode := map[ast.Node]workcensus.Member{}
	for _, member := range raw {
		if member.State != "compiled" || !member.Typed || member.Syntax == nil || member.Syntax.Info == nil {
			query.TextOnly = append(query.TextOnly, member)
			continue
		}
		input.Members = append(input.Members, member)
		byNode[member.Syntax.Node] = member
	}
	site := instructionSite(input)
	mark := func(pos token.Pos, kind, witness string) workcensus.Member {
		member := site(pos, kind)
		if member.ID != "" {
			query.CodeSites[member.ID] = append(query.CodeSites[member.ID], witness)
		}
		return member
	}
	owners := map[string]bool{"github.com/orneryd/nornicdb/internal/cyphertext.ScanIdentifier": true}
	for _, owner := range contracts.Calls {
		owners[owner.Symbol] = true
	}
	for _, owner := range contracts.Writers {
		owners[owner] = true
	}
	for _, owner := range contracts.RecordingOwners {
		owners[owner] = true
	}
	for owner := range owners {
		query.Owners = append(query.Owners, owner)
	}
	sort.Strings(query.Owners)
	fields := map[string]bool{}
	for _, field := range contracts.Fields {
		fields["field:"+field.Type+"."+field.Field] = true
	}
	implementations := map[*ssa.Function]bool{}
	active := compiledProductionFunctions(input)
	for _, fn := range input.Program.Functions {
		if !active[fn] {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				witness := fn.String() + ": " + instruction.String()
				switch n := instruction.(type) {
				case ssa.CallInstruction:
					member := mark(n.Pos(), "call", witness)
					var targets []*ssa.Function
					if target := n.Common().StaticCallee(); target != nil {
						targets = append(targets, target)
					} else if node := input.Program.CallGraph.Nodes[fn]; node != nil {
						for _, edge := range node.Out {
							if edge.Site == n {
								targets = append(targets, edge.Callee.Func)
							}
						}
					}
					for _, target := range targets {
						symbol := target.String()
						if object, ok := target.Object().(*types.Func); ok {
							symbol = object.FullName()
						}
						if member.ID != "" && owners[symbol] {
							query.OwnerCalls[member.ID] = append(query.OwnerCalls[member.ID], symbol)
							implementations[target] = true
						}
						if member.ID != "" && n.Common().IsInvoke() {
							query.InterfaceTargets[member.ID] = append(query.InterfaceTargets[member.ID], n.Common().Method.FullName()+" => "+symbol)
						}
					}
				case *ssa.BinOp:
					mark(n.Pos(), "binary", witness)
				case *ssa.Lookup:
					mark(n.Pos(), "index", witness)
				case *ssa.Index:
					mark(n.Pos(), "index", witness)
				case *ssa.IndexAddr:
					mark(n.Pos(), "index", witness)
				case *ssa.Slice:
					mark(n.Pos(), "slice", witness)
				case *ssa.Convert:
					mark(n.Pos(), "call", witness)
				case *ssa.ChangeType:
					mark(n.Pos(), "call", witness)
				case *ssa.TypeAssert:
					mark(n.Pos(), "type-assert", witness)
				case *ssa.MapUpdate:
					mark(n.Pos(), "index", witness)
					mark(n.Pos(), "assign", witness)
					mark(n.Pos(), "composite", witness)
				case *ssa.Store:
					mark(n.Pos(), "assign", witness)
					mark(n.Pos(), "composite", witness)
				case *ssa.MakeMap:
					mark(n.Pos(), "composite", witness)
				case *ssa.Alloc:
					mark(n.Pos(), "composite", witness)
				case *ssa.FieldAddr:
					key := declaredFieldKey(n.X.Type(), n.Field)
					if fields[key] {
						member := mark(n.Pos(), "identifier", witness)
						if member.ID != "" {
							query.FieldAccesses[member.ID] = append(query.FieldAccesses[member.ID], key)
						}
						implementations[fn] = true
						if references := n.Referrers(); references != nil {
							for _, reference := range *references {
								if store, ok := reference.(*ssa.Store); ok && store.Addr == n {
									member := mark(store.Pos(), "assign", fn.String()+": field writer "+key)
									if member.ID != "" {
										query.FieldAccesses[member.ID] = append(query.FieldAccesses[member.ID], "write "+key)
									}
								}
							}
						}
					}
				case *ssa.Field:
					key := declaredFieldKey(n.X.Type(), n.Field)
					if fields[key] {
						member := mark(n.Pos(), "identifier", witness)
						if member.ID != "" {
							query.FieldAccesses[member.ID] = append(query.FieldAccesses[member.ID], key)
						}
						implementations[fn] = true
					}
				}
			}
		}
	}
	for fn := range implementations {
		syntax := fn.Syntax()
		if syntax == nil && fn.Origin() != nil {
			syntax = fn.Origin().Syntax()
		}
		if member, ok := byNode[syntax]; ok {
			query.Implementations[member.ID] = append(query.Implementations[member.ID], fn.String())
		}
	}
	result := Result{Inventory: input}
	result.Inventory.Members = nil
	if includeFlow {
		result = analyze(input, contracts, true)
	}
	result.Compiled = query
	selected := map[string]bool{}
	var retained []workcensus.Member
	for _, member := range result.Inventory.Members {
		if len(query.CodeSites[member.ID]) == 0 {
			query.NoInstructionFlow = append(query.NoInstructionFlow, member)
			continue
		}
		member.Facts["compiled instruction"] = query.CodeSites[member.ID]
		retained = append(retained, member)
		selected[member.ID] = true
	}
	for _, member := range input.Members {
		if selected[member.ID] {
			continue
		}
		var reason []string
		kind := ""
		if values := query.OwnerCalls[member.ID]; len(values) > 0 {
			reason = append(reason, values...)
			kind = "compiled owner caller"
		}
		if values := query.FieldAccesses[member.ID]; len(values) > 0 {
			reason = append(reason, values...)
			kind = "compiled identity field access/write"
		}
		if values := query.Implementations[member.ID]; len(values) > 0 {
			reason = append(reason, values...)
			kind = "compiled owner/field implementation"
		}
		if kind == "" {
			continue
		}
		member.Facts = map[string][]string{"flow sink": {kind}, "decoded inputs": reason, "compiled query": reason}
		member.Class, member.Reason = "", ""
		retained = append(retained, member)
		selected[member.ID] = true
	}
	result.Inventory.Members = retained
	result.Inventory.Limitations = append(result.Inventory.Limitations, "Authoritative Go membership is compiler/SSA instruction, exact owner-call, typed identity-field or implementation queries. Text-only source hits are cross-checks. CHA/VTA and name-flow unions remain conservative; no semantic approval follows.")
	sort.Slice(result.Inventory.Members, func(i, j int) bool {
		return strings.Compare(result.Inventory.Members[i].ID, result.Inventory.Members[j].ID) < 0
	})
	return result
}

// instructionSite joins shared-engine source facts to executable compiler
// instructions. It parses and hashes nothing. Position containment is needed
// because SSA field/map stores can point inside their source assignment.
func instructionSite(input workcensus.Inventory) func(token.Pos, string) workcensus.Member {
	byFile := map[string]map[string][]workcensus.Member{}
	for _, member := range input.Members {
		if member.Syntax == nil {
			continue
		}
		file, _, _ := strings.Cut(member.ID, ":")
		if byFile[file] == nil {
			byFile[file] = map[string][]workcensus.Member{}
		}
		byFile[file][member.Node] = append(byFile[file][member.Node], member)
	}
	return func(pos token.Pos, kind string) workcensus.Member {
		if !pos.IsValid() {
			return workcensus.Member{}
		}
		position := input.Program.FileSet.Position(pos)
		file, err := filepath.Rel(input.Program.Root, position.Filename)
		if err != nil {
			return workcensus.Member{}
		}
		var best workcensus.Member
		for _, member := range byFile[filepath.ToSlash(file)][kind] {
			if position.Offset < member.Offset || position.Offset >= member.EndOffset {
				continue
			}
			if best.ID == "" || member.EndOffset-member.Offset < best.EndOffset-best.Offset {
				best = member
			}
		}
		return best
	}
}

// compiledProductionFunctions is the shared executable-code eligibility query.
// Eligibility never supplies membership or semantic approval.
func compiledProductionFunctions(input workcensus.Inventory) map[*ssa.Function]bool {
	// Reachability establishes code eligibility only. It never makes a
	// downstream operation a member. Public library APIs, package init and
	// executable entrypoints are roots; test-only private helper cycles are not.
	active := map[*ssa.Function]bool{}
	var pending []*ssa.Function
	for _, fn := range input.Program.Functions {
		if fn.Pkg == nil || (!strings.HasPrefix(fn.Pkg.Pkg.Path(), "github.com/orneryd/nornicdb") && fn.Pkg.Pkg.Path() != "command-line-arguments") {
			continue
		}
		object := fn.Object()
		if object != nil && object.Exported() || fn.Name() == "init" || fn.Name() == "main" && fn.Pkg.Pkg.Name() == "main" {
			pending = append(pending, fn)
		}
	}
	for len(pending) != 0 {
		fn := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if active[fn] {
			continue
		}
		active[fn] = true
		if node := input.Program.CallGraph.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				pending = append(pending, edge.Callee.Func)
			}
		}
	}
	return active
}

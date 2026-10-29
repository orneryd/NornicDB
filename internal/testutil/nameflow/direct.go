//go:build nameflowcensus

package nameflow

import (
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// Direct mechanisms are the population. Other name-bearing flows are context.
// Roles describe declared name products and local compiler values, never whole
// containing structs, arbitrary receivers or generic result-row payloads.
type nameRole uint16

const (
	directName nameRole = 1 << iota
	directElements
	directKeys
	directValues
	writtenName
	boxedName
	boxedElements
)
const memoryShift = 8

// AnalyzeDirect asks which instructions directly decode outside the owner,
// mutate a name, compare written and decoded products, or construct name text.
// A proposal still needs a short source/caller disposition. Compiler reach does
// not approve the mechanism or make a distinct protocol into a graph name.
func AnalyzeDirect(input workcensus.Inventory, contracts Contracts) Result {
	if input.Program == nil {
		panic("direct mechanisms require the shared compiler program")
	}
	raw := input.Members
	input.Members = nil
	for _, member := range raw {
		if member.State == "compiled" && member.Typed && member.Syntax != nil {
			input.Members = append(input.Members, member)
		}
	}
	site := instructionSite(input)
	roles := map[ssa.Value]nameRole{}
	tuple := map[ssa.Value]map[int]nameRole{}
	returns := map[*ssa.Function]map[int]nameRole{}
	fields := map[string]nameRole{}
	for _, field := range contracts.Fields {
		role := directName
		if field.Channel == "/element" {
			role = directElements
		} else if field.Channel == "/key" {
			role = directKeys
		} else if field.Channel == "/value" {
			role = directValues
		}
		fields["field:"+field.Type+"."+field.Field] |= role
	}
	writers := map[string]bool{}
	for _, symbol := range contracts.Writers {
		writers[symbol] = true
	}
	for _, symbol := range []string{"github.com/orneryd/nornicdb/internal/cyphertext.StringLiteral", "github.com/orneryd/nornicdb/pkg/cypher.cypherStringLiteral", "github.com/orneryd/nornicdb/pkg/cypher.quoteKnowledgePolicyName"} {
		writers[symbol] = true
	}
	nameType := func(typ types.Type) nameRole {
		if typ == nil {
			return 0
		}
		if basic, ok := typ.Underlying().(*types.Basic); ok && basic.Info()&types.IsString != 0 {
			return directName
		}
		if slice, ok := typ.Underlying().(*types.Slice); ok {
			if basic, ok := slice.Elem().Underlying().(*types.Basic); ok && basic.Info()&types.IsString != 0 {
				return directElements
			}
		}
		return 0
	}
	symbol := func(fn *ssa.Function) string {
		if object, ok := fn.Object().(*types.Func); ok {
			return object.FullName()
		}
		return fn.String()
	}
	type callSite struct {
		instruction ssa.CallInstruction
		targets     []*ssa.Function
		args        []ssa.Value
		member      workcensus.Member
	}
	active := compiledProductionFunctions(input)
	var calls []callSite
	var instructions []ssa.Instruction
	for _, fn := range input.Program.Functions {
		if !active[fn] {
			continue
		}
		if fn.Pkg == nil || (!strings.HasPrefix(fn.Pkg.Pkg.Path(), "github.com/orneryd/nornicdb") && fn.Pkg.Pkg.Path() != "command-line-arguments") {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				instructions = append(instructions, instruction)
				call, ok := instruction.(ssa.CallInstruction)
				if !ok {
					continue
				}
				entry := callSite{instruction: call, member: site(call.Pos(), "call"), args: call.Common().Args}
				if call.Common().IsInvoke() {
					entry.args = append([]ssa.Value{call.Common().Value}, entry.args...)
				}
				if target := call.Common().StaticCallee(); target != nil {
					entry.targets = append(entry.targets, target)
				} else if node := input.Program.CallGraph.Nodes[fn]; node != nil {
					for _, edge := range node.Out {
						if edge.Site == call {
							entry.targets = append(entry.targets, edge.Callee.Func)
						}
					}
				}
				calls = append(calls, entry)
			}
		}
	}
	changed := false
	put := func(value ssa.Value, role nameRole) {
		if value != nil && role != 0 && roles[value]&role != role {
			roles[value] |= role
			changed = true
		}
	}
	slot := func(value ssa.Value, index int, role nameRole) {
		if role == 0 || value == nil {
			return
		}
		if tuple[value] == nil {
			tuple[value] = map[int]nameRole{}
		}
		if tuple[value][index]&role != role {
			tuple[value][index] |= role
			changed = true
		}
	}
	ret := func(fn *ssa.Function, index int, role nameRole) {
		if role == 0 {
			return
		}
		if returns[fn] == nil {
			returns[fn] = map[int]nameRole{}
		}
		if returns[fn][index]&role != role {
			returns[fn][index] |= role
			changed = true
		}
	}
	transfer := func(a, b ssa.Value) { put(b, roles[a]) }
	// Forward def/use from declared products and exact decoded return slots.
	// A consumer's use must not infer that all alternate definitions or other
	// callers of a shared helper were decoded names.
	for pass := 0; pass < 100; pass++ {
		changed = false
		for _, instruction := range instructions {
			switch n := instruction.(type) {
			case *ssa.FieldAddr:
				key := declaredFieldKey(n.X.Type(), n.Field)
				put(n, fields[key]<<memoryShift)
				// Undeclared containing fields are context/proposals, not new identity
				// roots. Their semantic contract must be reviewed before catalog adoption.
			case *ssa.Field:
				key := declaredFieldKey(n.X.Type(), n.Field)
				put(n, fields[key])
			case *ssa.Store:
				expected := roles[n.Addr] >> memoryShift
				put(n.Val, expected & ^writtenName)
				put(n.Addr, roles[n.Val]<<memoryShift)
			case *ssa.UnOp:
				if n.Op == token.MUL {
					put(n, roles[n.X]>>memoryShift)
				}
			case *ssa.Phi:
				for _, edge := range n.Edges {
					transfer(edge, n)
				}
			case *ssa.ChangeType:
				transfer(n.X, n)
			case *ssa.MakeInterface:
				// Boxing into a generic data payload does not make the payload an identity.
				if roles[n]&directName != 0 {
					put(n.X, directName)
				}
				if roles[n.X]&directName != 0 {
					put(n, boxedName)
				}
			case *ssa.TypeAssert:
				if nameType(n.AssertedType) == directName {
					put(n, roles[n.X]&directName)
					slot(n, 0, roles[n.X]&directName)
				}
			case *ssa.Extract:
				put(n, tuple[n.Tuple][n.Index])
			case *ssa.IndexAddr:
				if roles[n.X]&directElements != 0 {
					put(n, directName<<memoryShift)
				}
				if roles[n]&(boxedName<<memoryShift) != 0 {
					put(n.X, boxedElements)
				}
				if roles[n.X]&boxedElements != 0 {
					put(n, boxedName<<memoryShift)
				}
			case *ssa.Index:
				if roles[n.X]&directElements != 0 {
					put(n, directName)
				}
			case *ssa.Slice:
				// A decoded-name slice is a transform. A source extent used to
				// produce a name does not turn the entire source into an identity.
				put(n, roles[n.X])
			case *ssa.Range:
				put(n, roles[n.X]&(directKeys|directValues))
			case *ssa.Next:
				if roles[n.Iter]&directKeys != 0 {
					slot(n, 1, directName)
				}
				if roles[n.Iter]&directValues != 0 {
					slot(n, 2, directName)
				}
			case *ssa.MapUpdate:
				if roles[n.Map]&directKeys != 0 {
					put(n.Key, directName)
				}
				if roles[n.Map]&directValues != 0 {
					put(n.Value, directName)
				}
			case *ssa.Lookup:
				if roles[n.X]&directKeys != 0 {
					put(n.Index, directName)
				}
				if roles[n.X]&directValues != 0 {
					put(n, directName)
					slot(n, 0, directName)
				}
			case *ssa.BinOp:
				// Construction is the direct mechanism. Its resulting query/key/display
				// string does not become another decoded name merely by containing one.
			case *ssa.Return:
				for index, value := range n.Results {
					ret(n.Parent(), index, roles[value])
				}
			}
		}
		for _, entry := range calls {
			call := entry.instruction.Value()
			if call == nil {
				continue
			}
			for _, target := range entry.targets {
				targetSymbol := symbol(target)
				if writers[targetSymbol] {
					put(call, writtenName)
					continue
				}
				if targetSymbol == "github.com/orneryd/nornicdb/pkg/cypher.scanSymbolicName" {
					slot(call, 0, writtenName)
					continue
				}
				if target.Name() == "GetText" && strings.Contains(entry.member.ReceiverType, "/pkg/cypher/antlr.") {
					put(call, writtenName)
					continue
				}
				producer := false
				for _, contract := range contracts.Calls {
					if targetSymbol == contract.Symbol {
						role := nameType(target.Signature.Results().At(contract.Result).Type())
						if _, ok := call.Type().(*types.Tuple); ok {
							slot(call, contract.Result, role)
						} else {
							put(call, role)
						}
						producer = true
					}
				}
				if targetSymbol == "github.com/orneryd/nornicdb/internal/cyphertext.ScanIdentifier" {
					producer = true
				}
				if producer {
					continue
				}
				// These generic case helpers serve both written query text and
				// decoded callable names. A shared formal/return join would make
				// every unrelated source caller a name. Like strings.ToUpper,
				// the direct transform call is the obligation; its output is not
				// a new decoded-name producer.
				if targetSymbol == "github.com/orneryd/nornicdb/pkg/cypher.upperASCII" || targetSymbol == "github.com/orneryd/nornicdb/pkg/cypher.lowerASCII" {
					continue
				}
				// Standard-library functions can have SSA bodies too. Their
				// shared formal/return slots must never unify unrelated call sites.
				internal := target.Pkg != nil && (strings.HasPrefix(target.Pkg.Pkg.Path(), "github.com/orneryd/nornicdb") || target.Pkg.Pkg.Path() == "command-line-arguments")
				if internal && len(target.Blocks) > 0 {
					for index, arg := range entry.args {
						if index < len(target.Params) {
							put(target.Params[index], roles[arg])
						}
					}
					for index, role := range returns[target] {
						if _, ok := call.Type().(*types.Tuple); ok {
							slot(call, index, role)
						} else if index == 0 {
							put(call, role)
						}
					}
				}
				// A transform is itself the obligation; no reachability population is
				// inferred from the transformed protocol/output string.
			}
		}
		if !changed {
			break
		}
		if pass == 99 {
			panic("direct-name representation contracts did not converge")
		}
	}
	selected := map[string]workcensus.Member{}
	add := func(member workcensus.Member, class, witness string) {
		if member.ID == "" {
			return
		}
		if member.File == "internal/cyphertext/name.go" || member.File == "internal/cyphertext/identifier.go" || member.File == "internal/cyphertext/name_emission.go" {
			return
		}
		current, exists := selected[member.ID]
		if !exists {
			current = member
			current.Facts = map[string][]string{}
			current.Class = ""
			current.Reason = ""
		}
		current.Facts["direct mechanism"] = append(current.Facts["direct mechanism"], class)
		current.Facts["compiled witness"] = append(current.Facts["compiled witness"], witness)
		selected[member.ID] = current
	}
	textConstant := func(value ssa.Value) string {
		if literal, ok := value.(*ssa.Const); ok && literal.Value != nil && literal.Value.Kind() == constant.String {
			return constant.StringVal(literal.Value)
		}
		return ""
	}
	products := map[string]bool{}
	for _, contract := range contracts.Calls {
		products[contract.Symbol] = true
	}
	// A decoder or normalizer that writes a declared identity slot is a direct
	// mechanism even when its API input has no earlier decoded-name annotation.
	// This is a compiled field/key writer query, not backward taint through all
	// callers of the function producing the value.
	identityWrite := func(value ssa.Value, destination string) {
		for {
			switch n := value.(type) {
			case *ssa.ChangeType:
				value = n.X
			case *ssa.MakeInterface:
				value = n.X
			case *ssa.Extract:
				value = n.Tuple
			default:
				goto unwrapped
			}
		}
	unwrapped:
		call, ok := value.(*ssa.Call)
		if !ok {
			return
		}
		callee := "dynamic call"
		if target := call.Common().StaticCallee(); target != nil {
			callee = symbol(target)
			if products[callee] {
				return
			}
		} else if builtin, ok := call.Common().Value.(*ssa.Builtin); ok {
			callee = builtin.Name()
		} else if call.Common().Method != nil {
			callee = call.Common().Method.FullName()
		}
		add(site(call.Pos(), "call"), "identity field/key writer outside owner", destination+" <- "+callee)
	}
	for _, entry := range calls {
		named, formattedName := false, false
		for _, arg := range entry.args {
			named = named || roles[arg]&(directName|directElements|directKeys) != 0
			formattedName = formattedName || roles[arg]&(directName|directElements|directKeys|boxedName|boxedElements) != 0
		}
		for _, target := range entry.targets {
			name := symbol(target)
			if products[name] && named {
				add(entry.member, "decoded product passed to name reader", name)
			}
			quotePrimitive := strings.HasPrefix(name, "strings.Trim") || name == "strings.Replace" || name == "strings.ReplaceAll"
			if quotePrimitive {
				for _, arg := range entry.args[1:] {
					if strings.Contains(textConstant(arg), "`") {
						add(entry.member, "name reader/writer outside owner", name)
						break
					}
				}
			}
			stringTransform := strings.HasPrefix(name, "strings.Trim") || strings.HasPrefix(name, "strings.To") || strings.HasPrefix(name, "strings.Split") || strings.HasPrefix(name, "strings.Fields") || strings.HasPrefix(name, "strings.Replace") || strings.HasPrefix(name, "strings.Contains") || strings.HasPrefix(name, "strings.Index") || name == "strings.Join" || name == "strings.EqualFold" || name == "strings.Map" || strings.HasPrefix(name, "unicode.To") || name == "github.com/orneryd/nornicdb/pkg/cypher.upperASCII" || name == "github.com/orneryd/nornicdb/pkg/cypher.lowerASCII"
			if named && stringTransform {
				add(entry.member, "decoded identity transform/compare/key", name)
			}
			if name == "(github.com/orneryd/nornicdb/pkg/vectorspace.VectorSpaceKey).Canonical" {
				add(entry.member, "registry name canonicalization", name)
			}
			if named && (name == "(*strings.Builder).WriteString" || name == "(*bytes.Buffer).WriteString" || name == "(*bytes.Buffer).Write") {
				add(entry.member, "name emission/builder construction", name)
			}
			if name == "fmt.Sprintf" || name == "fmt.Fprintf" {
				formatIndex := 0
				if name == "fmt.Fprintf" {
					formatIndex = 1
				}
				if formatIndex < len(entry.args) {
					format := textConstant(entry.args[formatIndex])
					if strings.Contains(format, "`%") || (formattedName && strings.ContainsAny(format, ":.|_\x00")) {
						add(entry.member, "name emission/tuple construction", name+" "+format)
					}
				}
			}
		}
	}
	for _, instruction := range instructions {
		switch n := instruction.(type) {
		case *ssa.BinOp:
			if n.Op == token.ADD && nameType(n.Type()) == directName {
				left, right := textConstant(n.X), textConstant(n.Y)
				named := (roles[n.X]|roles[n.Y])&directName != 0
				if left == "`" || right == "`" || (named && (strings.ContainsAny(left, ":.|_\x00") || strings.ContainsAny(right, ":.|_\x00"))) {
					add(site(n.Pos(), "binary"), "name emission/tuple construction", n.String())
				}
			}
			if (n.Op == token.EQL || n.Op == token.NEQ) && ((roles[n.X]&writtenName != 0 && roles[n.Y]&directName != 0) || (roles[n.Y]&writtenName != 0 && roles[n.X]&directName != 0)) {
				add(site(n.Pos(), "binary"), "written-versus-decoded identity comparison", n.String())
			}
		case *ssa.Slice:
			if roles[n.X]&directName != 0 {
				add(site(n.Pos(), "slice"), "decoded identity transform/compare/key", n.String())
			}
		case *ssa.MapUpdate:
			if roles[n.Map]&directKeys != 0 {
				identityWrite(n.Key, "decoded identity map key")
			}
			if roles[n.Map]&directValues != 0 {
				identityWrite(n.Value, "decoded identity map value")
			}
		case *ssa.Store:
			if address, ok := n.Addr.(*ssa.FieldAddr); ok {
				key := declaredFieldKey(address.X.Type(), address.Field)
				if fields[key] != 0 {
					identityWrite(n.Val, key)
				}
				if fields[key]&directName != 0 && roles[n.Val]&writtenName != 0 {
					add(site(n.Pos(), "assign"), "written product stored as decoded identity", n.String())
				}
			}
		}
	}
	result := Result{Inventory: input}
	result.Inventory.Members = nil
	for _, member := range selected {
		result.Inventory.Members = append(result.Inventory.Members, member)
	}
	sort.Slice(result.Inventory.Members, func(i, j int) bool { return result.Inventory.Members[i].ID < result.Inventory.Members[j].ID })
	result.Inventory.Limitations = append(result.Inventory.Limitations, "Direct mechanism membership only; alias, parameter and declared-product roles are context. No whole-struct/receiver/error/data-row reachability membership. Each mechanism needs a read-based disposition; metadata products and opaque/reflection boundaries require class controls.")
	return result
}

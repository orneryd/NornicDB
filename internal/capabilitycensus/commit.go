package capabilitycensus

import (
	"go/ast"
	"go/types"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// commitTypes interprets actual protocol type objects from the canonical
// inventory. Function/variable spelling contributes no commit classification.
// Native transaction operations whose type cannot identify their effect stay
// explicitly undecided; discovery never supplies a semantic disposition/reason.
type commitTypes struct {
	contracts       []*types.Interface
	transactions    []*types.Interface
	frames          []types.Type
	routes          []types.Type
	objects         map[string][]*types.Func
	implementations map[types.Type]map[*types.Interface]bool
	framesChecked   map[types.Type]bool
	references      map[types.Type]bool
	producers       map[*types.Signature]bool
	methods         map[*types.Func]string
}

func newCommitTypes(inventory workcensus.Inventory) *commitTypes {
	c := &commitTypes{objects: map[string][]*types.Func{}, implementations: map[types.Type]map[*types.Interface]bool{}, framesChecked: map[types.Type]bool{}, references: map[types.Type]bool{}, producers: map[*types.Signature]bool{}, methods: map[*types.Func]string{}}
	packages := map[*types.Package]bool{}
	functions := map[*types.Func]bool{}
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		if member.Syntax.Package != nil {
			packages[member.Syntax.Package] = true
		}
		if id, ok := member.Syntax.Node.(*ast.Ident); ok {
			if fn, ok := member.Syntax.Info.Defs[id].(*types.Func); ok && !functions[fn] {
				functions[fn] = true
				c.objects[fn.FullName()] = append(c.objects[fn.FullName()], fn)
			}
		}
	}
	add := func(pkg *types.Package, name string, interfaces *[]*types.Interface, frames *[]types.Type) {
		object := pkg.Scope().Lookup(name)
		if object == nil {
			return
		}
		typ := types.Unalias(object.Type())
		if interfaces != nil {
			if contract, ok := typ.Underlying().(*types.Interface); ok {
				*interfaces = append(*interfaces, contract)
			}
			return
		}
		if frames != nil {
			*frames = append(*frames, types.NewPointer(typ))
			return
		}
		c.routes = append(c.routes, typ)
	}
	for pkg := range packages {
		switch pkg.Path() {
		case storagePackage:
			for _, name := range []string{"TransactionCommitter", "EntityCommitSubmitter", "EntityCommitAdmissionGate", "EntityDirectForwarder"} {
				add(pkg, name, &c.contracts, nil)
			}
			add(pkg, "RemoteCypherTx", &c.transactions, nil)
			for _, name := range []string{"BadgerTransaction", "PreparedEntityCommit"} {
				add(pkg, name, nil, &c.frames)
			}
			add(pkg, "EntityCommitRoute", nil, nil)
			for _, imported := range pkg.Imports() {
				if imported.Path() == "github.com/dgraph-io/badger/v4" {
					add(imported, "Txn", nil, &c.frames)
				}
			}
		case "github.com/orneryd/nornicdb/pkg/fabric":
			add(pkg, "FabricTransaction", nil, &c.frames)
		}
	}
	for _, frame := range c.frames {
		methods := types.NewMethodSet(frame)
		for i := 0; i < methods.Len(); i++ {
			if fn, ok := methods.At(i).Obj().(*types.Func); ok && !functions[fn] {
				functions[fn] = true
				c.objects[fn.FullName()] = append(c.objects[fn.FullName()], fn)
			}
		}
	}
	return c
}

func (c *commitTypes) implements(typ types.Type, interfaces []*types.Interface) bool {
	if typ == nil {
		return false
	}
	for _, contract := range interfaces {
		if c.implementations[typ] == nil {
			c.implementations[typ] = map[*types.Interface]bool{}
		}
		matched, known := c.implementations[typ][contract]
		if !known {
			matched = types.Implements(typ, contract)
			c.implementations[typ][contract] = matched
		}
		if matched {
			return true
		}
	}
	return false
}

// Compare the selected implementation object with the actual interface's
// method set, rather than checking the selected identifier's text.
func (c *commitTypes) contractMethod(fn *types.Func, contracts []*types.Interface) bool {
	signature, ok := fn.Type().(*types.Signature)
	if !ok || signature.Recv() == nil {
		return false
	}
	receiver := signature.Recv().Type()
	if !c.implements(receiver, contracts) {
		return false
	}
	methods := types.NewMethodSet(receiver)
	for _, contract := range contracts {
		if !types.Implements(receiver, contract) {
			continue
		}
		for i := 0; i < contract.NumMethods(); i++ {
			method := contract.Method(i)
			selected := methods.Lookup(method.Pkg(), method.Name())
			if selected != nil {
				if actual, ok := selected.Obj().(*types.Func); ok && actual.Origin() == fn.Origin() {
					return true
				}
			}
		}
	}
	return false
}

func (c *commitTypes) frame(typ types.Type) (result bool) {
	if typ == nil {
		return false
	}
	if result, known := c.framesChecked[typ]; known {
		return result
	}
	defer func() { c.framesChecked[typ] = result }()
	for _, frame := range c.frames {
		if types.Identical(types.Unalias(typ), frame) {
			return true
		}
	}
	return c.implements(typ, c.transactions)
}

func (c *commitTypes) strongReference(typ types.Type) bool {
	if typ == nil {
		return false
	}
	if c.implements(typ, c.contracts) {
		return true
	}
	for _, route := range c.routes {
		if types.Identical(types.Unalias(typ), route) {
			return true
		}
	}
	base := types.Unalias(typ)
	if pointer, ok := base.(*types.Pointer); ok {
		base = types.Unalias(pointer.Elem())
	}
	if record, ok := base.Underlying().(*types.Struct); ok {
		for i := 0; i < record.NumFields(); i++ {
			field := record.Field(i).Type()
			if c.implements(field, c.contracts) {
				return true
			}
			for _, route := range c.routes {
				if types.Identical(types.Unalias(field), route) {
					return true
				}
			}
		}
	}
	return false
}

func (c *commitTypes) reference(typ types.Type) (result bool) {
	if typ == nil {
		return false
	}
	if result, known := c.references[typ]; known {
		return result
	}
	defer func() { c.references[typ] = result }()
	if c.strongReference(typ) || c.frame(typ) {
		return true
	}
	base := types.Unalias(typ)
	if pointer, ok := base.(*types.Pointer); ok {
		base = types.Unalias(pointer.Elem())
	}
	if record, ok := base.Underlying().(*types.Struct); ok {
		for i := 0; i < record.NumFields(); i++ {
			if c.frame(record.Field(i).Type()) {
				return true
			}
		}
	}
	if contract, ok := base.Underlying().(*types.Interface); ok {
		for i := 0; i < contract.NumMethods(); i++ {
			signature := contract.Method(i).Type().(*types.Signature)
			for j := 0; j < signature.Results().Len(); j++ {
				if c.frame(signature.Results().At(j).Type()) || c.strongReference(signature.Results().At(j).Type()) {
					return true
				}
			}
		}
	}
	return false
}

func (c *commitTypes) producer(signature *types.Signature) (result bool) {
	if signature == nil {
		return false
	}
	if result, known := c.producers[signature]; known {
		return result
	}
	defer func() { c.producers[signature] = result }()
	for i := 0; i < signature.Results().Len(); i++ {
		if c.reference(signature.Results().At(i).Type()) {
			return true
		}
	}
	return false
}

func (c *commitTypes) method(fn *types.Func) (role string) {
	if role, known := c.methods[fn]; known {
		return role
	}
	defer func() { c.methods[fn] = role }()
	if c.contractMethod(fn, c.contracts) {
		return "commit-capability-call"
	}
	signature := fn.Type().(*types.Signature)
	if signature.Recv() != nil && c.frame(signature.Recv().Type()) {
		return "transaction-operation-undecided"
	}
	return ""
}

func (c *commitTypes) producerRole(signature *types.Signature) string {
	if !c.producer(signature) {
		return ""
	}
	for i := 0; i < signature.Results().Len(); i++ {
		if c.strongReference(signature.Results().At(i).Type()) {
			return "commit-reference-producer"
		}
	}
	// A snapshot and a writer can share a native transaction type. The type
	// proves a reference, not its commit authority or the operation's behavior.
	return "transaction-reference-undecided"
}

func (c *commitTypes) declaration(fn *types.Func) string {
	if fn == nil {
		return ""
	}
	if role := c.producerRole(fn.Type().(*types.Signature)); role != "" {
		return role
	}
	return c.method(fn)
}

func (c *commitTypes) call(member workcensus.Member, call *ast.CallExpr) string {
	if member.Syntax == nil || member.Syntax.Info == nil {
		return ""
	}
	info := member.Syntax.Info
	if signature, ok := info.TypeOf(call.Fun).(*types.Signature); ok {
		if role := c.producerRole(signature); role != "" {
			return role
		}
	}
	if fn, ok := object(info, call.Fun).(*types.Func); ok {
		if role := c.method(fn); role != "" {
			return role
		}
	}
	// Alias targets are the engine's resolved symbol facts. This join only
	// recovers their supplied type objects; no private def/use or loader exists.
	undecided := ""
	for _, symbol := range member.Callees {
		for _, fn := range c.objects[symbol] {
			role := c.declaration(fn)
			if role == "commit-capability-call" || role == "commit-reference-producer" {
				return role
			}
			if role != "" {
				undecided = role
			}
		}
	}
	return undecided
}

func mergeCommitConsumer(a, b string) string {
	if a == "commit-capability-call" || a == "commit-reference-producer" || a == "commit-consumer" || b == "commit-capability-call" || b == "commit-reference-producer" || b == "commit-consumer" {
		return "commit-consumer"
	}
	if a != "" || b != "" {
		return "transaction-consumer-undecided"
	}
	return ""
}

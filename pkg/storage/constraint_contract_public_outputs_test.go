package storage

import (
	"bufio"
	"fmt"
	"go/types"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// contractDataTypes are the types that carry a constraint contract's data,
// and its primitive companions' and the schema's other constraints', to
// callers: descriptions, handles, constraint records and violation reports.
var contractDataTypes = []string{"Constraint", "PropertyTypeConstraint", "ConstraintContract", "ConstraintContractEntry",
	"CompiledConstraintContract", "ConstraintViolationError", "ConstraintFact"}

// contractOutputAPIs derives from the package's declarations every exported
// function and method, interface methods included, that can hand a contract
// data type to its caller: in its results, or as an argument of a callback
// it calls; directly, or through exported fields, pointers, slices, arrays
// and maps.
func contractOutputAPIs(t *testing.T) []string {
	t.Helper()
	checked := checkStoragePackage(t, "")
	scope := checked.pkg.Scope()
	roots := map[*types.TypeName]bool{}
	for _, name := range contractDataTypes {
		object, ok := scope.Lookup(name).(*types.TypeName)
		require.True(t, ok, "%s is not a type", name)
		roots[object] = true
	}
	var carries func(types.Type, map[types.Type]bool) bool
	carries = func(typ types.Type, seen map[types.Type]bool) bool {
		if seen[typ] {
			return false
		}
		seen[typ] = true
		switch value := typ.(type) {
		case *types.Alias:
			return carries(types.Unalias(value), seen)
		case *types.Named:
			if roots[value.Obj()] {
				return true
			}
			return carries(value.Underlying(), seen)
		case *types.Pointer:
			return carries(value.Elem(), seen)
		case *types.Slice:
			return carries(value.Elem(), seen)
		case *types.Array:
			return carries(value.Elem(), seen)
		case *types.Map:
			return carries(value.Key(), seen) || carries(value.Elem(), seen)
		case *types.Struct:
			for index := 0; index < value.NumFields(); index++ {
				if field := value.Field(index); field.Exported() && carries(field.Type(), seen) {
					return true
				}
			}
		}
		return false
	}
	returns := func(signature *types.Signature) bool {
		for index := 0; index < signature.Results().Len(); index++ {
			if carries(signature.Results().At(index).Type(), map[types.Type]bool{}) {
				return true
			}
		}
		for index := 0; index < signature.Params().Len(); index++ {
			callback, ok := signature.Params().At(index).Type().Underlying().(*types.Signature)
			if !ok {
				continue
			}
			for argument := 0; argument < callback.Params().Len(); argument++ {
				if carries(callback.Params().At(argument).Type(), map[types.Type]bool{}) {
					return true
				}
			}
		}
		return false
	}
	found := map[string]bool{}
	for _, name := range scope.Names() {
		object := scope.Lookup(name)
		if !object.Exported() {
			continue
		}
		switch typed := object.(type) {
		case *types.Func:
			if returns(typed.Type().(*types.Signature)) {
				found[name] = true
			}
		case *types.TypeName:
			for _, receiver := range []types.Type{typed.Type(), types.NewPointer(typed.Type())} {
				methods := types.NewMethodSet(receiver)
				for index := 0; index < methods.Len(); index++ {
					method := methods.At(index).Obj().(*types.Func)
					if method.Exported() && returns(method.Type().(*types.Signature)) {
						found[name+"."+method.Name()] = true
					}
				}
			}
		}
	}
	var apis []string
	for api := range found {
		apis = append(apis, api)
	}
	sort.Strings(apis)
	return apis
}

// readContractOutputLedger reads testdata/contract_public_outputs.txt: each
// record is "API|subject", the detachment subject that exercises the API.
func readContractOutputLedger(t *testing.T) map[string]string {
	t.Helper()
	file, err := os.Open("testdata/contract_public_outputs.txt")
	require.NoError(t, err)
	defer file.Close()
	ledger := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		api, subject, ok := strings.Cut(line, "|")
		require.True(t, ok, "malformed ledger record %q", line)
		ledger[api] = subject
	}
	require.NoError(t, scanner.Err())
	return ledger
}

// Every exported API that can return contract data is in the ledger with
// the detachment subject that exercises it, and every ledger record is such
// an API. A new API fails until it is exercised.
func TestContractPublicOutputsAreInventoried(t *testing.T) {
	ledger := readContractOutputLedger(t)
	var recorded []string
	for api, subject := range ledger {
		recorded = append(recorded, api)
		_, ok := contractOutputSubjects[subject]
		require.True(t, ok, "ledger subject %q of %s is not a detachment subject", subject, api)
	}
	sort.Strings(recorded)
	require.Equal(t, recorded, contractOutputAPIs(t), "exported APIs that return contract data (testdata/contract_public_outputs.txt)")
	// The read-only view hands out no field, only values and a copy.
	require.Empty(t, exportedFields(reflect.TypeOf(ConstraintFact{}), "ConstraintFact"))
}

// mutateExported changes everything a caller can reach from value through
// exported fields, slice and array elements, map entries, pointers and
// interface values, and through an error's public accessors: strings,
// numbers and flags at every depth, nested lists and maps included.
func mutateExported(value reflect.Value, seen map[uintptr]bool) {
	switch value.Kind() {
	case reflect.String:
		if value.CanSet() {
			value.SetString("changed")
		}
	case reflect.Bool:
		if value.CanSet() {
			value.SetBool(!value.Bool())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.CanSet() {
			value.SetInt(value.Int() + 7)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.CanSet() {
			value.SetUint(value.Uint() + 7)
		}
	case reflect.Float32, reflect.Float64:
		if value.CanSet() {
			value.SetFloat(value.Float() + 0.5)
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			mutateExported(value.Index(index), seen)
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			item := reflect.New(value.Type().Elem()).Elem()
			item.Set(value.MapIndex(key))
			mutateExported(item, seen)
			value.SetMapIndex(key, item)
		}
	case reflect.Pointer:
		if !value.IsNil() && !seen[value.Pointer()] {
			seen[value.Pointer()] = true
			mutateExported(value.Elem(), seen)
		}
	case reflect.Interface:
		if value.IsNil() {
			return
		}
		inner := value.Elem()
		if err, ok := inner.Interface().(error); ok {
			mutatePublic(err)
		}
		item := reflect.New(inner.Type()).Elem()
		item.Set(inner)
		mutateExported(item, seen)
		if value.CanSet() {
			value.Set(item)
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).IsExported() {
				mutateExported(value.Field(index), seen)
			}
		}
	}
}

func mutateAll(values ...any) {
	seen := map[uintptr]bool{}
	for index := range values {
		mutateExported(reflect.ValueOf(&values[index]).Elem(), seen)
	}
}

// renderExported renders what a caller reads from values: exported fields,
// elements and map entries at every depth, and errors through their public
// accessors. Nothing of where values live in memory is rendered.
func renderExported(values ...any) string {
	var out strings.Builder
	var render func(reflect.Value)
	render = func(value reflect.Value) {
		if !value.IsValid() {
			out.WriteString("nil")
			return
		}
		if value.CanInterface() {
			if err, ok := value.Interface().(error); ok && value.Kind() != reflect.Struct {
				if value.Kind() == reflect.Pointer && value.IsNil() {
					out.WriteString("nil")
					return
				}
				out.WriteString(renderPublic(err))
				return
			}
		}
		switch value.Kind() {
		case reflect.Pointer, reflect.Interface:
			if value.IsNil() {
				out.WriteString("nil")
				return
			}
			render(value.Elem())
		case reflect.Slice, reflect.Array:
			if value.Kind() == reflect.Slice && value.IsNil() {
				out.WriteString("[]nil")
				return
			}
			out.WriteString("[")
			for index := 0; index < value.Len(); index++ {
				render(value.Index(index))
				out.WriteString(",")
			}
			out.WriteString("]")
		case reflect.Map:
			keys := value.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
			out.WriteString("{")
			for _, key := range keys {
				out.WriteString(fmt.Sprint(key) + ":")
				render(value.MapIndex(key))
				out.WriteString(",")
			}
			out.WriteString("}")
		case reflect.Struct:
			out.WriteString(value.Type().String() + "{")
			for index := 0; index < value.NumField(); index++ {
				if field := value.Type().Field(index); field.IsExported() {
					out.WriteString(field.Name + "=")
					render(value.Field(index))
					out.WriteString(",")
				}
			}
			out.WriteString("}")
		default:
			out.WriteString(fmt.Sprintf("%s(%v)", value.Type(), value.Interface()))
		}
	}
	for _, value := range values {
		render(reflect.ValueOf(&value).Elem())
		out.WriteString("\n")
	}
	return out.String()
}

// contractOutputEnv is a schema with every kind of contract data installed:
// primitive node and relationship key contracts with their companions, a
// boolean contract, and plain node key, domain, existence and property type
// constraints.
type contractOutputEnv struct {
	engine *BadgerEngine
	schema *SchemaManager
	handle CompiledConstraintContract
}

func newContractOutputEnv(t *testing.T) *contractOutputEnv {
	t.Helper()
	engine := newDetachedEngine(t)
	env := &contractOutputEnv{engine: engine, schema: engine.GetSchemaForNamespace("test")}
	for _, primitive := range []struct {
		name, target string
		kind         string
		entity       ConstraintEntityType
		constraint   ConstraintType
		expression   string
	}{
		{"primitive_node", "PRIMITIVE", ConstraintContractKindPrimitiveNode, ConstraintEntityNode, ConstraintNodeKey, "(n.required) IS NODE KEY"},
		{"primitive_rel", "PRIMITIVE_REL", ConstraintContractKindPrimitiveRelationship, ConstraintEntityRelationship, ConstraintRelationshipKey, "(r.required) IS RELATIONSHIP KEY"},
	} {
		contract := ConstraintContract{Name: primitive.name, TargetEntityType: string(primitive.entity), TargetLabelOrType: primitive.target,
			Entries: []ConstraintContractEntry{{Kind: primitive.kind, PrimitiveType: string(primitive.constraint), Properties: []string{"required"}, Expression: primitive.expression}}}
		compiled, err := CompileConstraintContract(contract)
		require.NoError(t, err)
		companion := Constraint{Name: ConstraintContractEntryName(contract.Name, 0), Type: primitive.constraint, EntityType: primitive.entity,
			Label: primitive.target, Properties: []string{"required"}}
		require.NoError(t, env.schema.AddCompiledConstraintContractBundle(compiled, []Constraint{companion}, nil, false))
		env.handle = compiled
	}
	adults := ConstraintContract{Name: "adults", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "ADULT",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 10"}}}
	require.NoError(t, env.schema.AddConstraintContractBundle(adults, nil, nil, false))
	require.NoError(t, env.schema.AddConstraint(Constraint{Name: "key_node", Type: ConstraintNodeKey, Label: "KEYED", Properties: []string{"a", "b"}}))
	require.NoError(t, env.schema.AddConstraint(Constraint{Name: "domain_color", Type: ConstraintDomain, Label: "DOMAIN", Properties: []string{"color"},
		AllowedValues: []interface{}{"red", int64(1), []interface{}{"nested"}}}))
	require.NoError(t, env.schema.AddConstraint(Constraint{Name: "exists_name", Type: ConstraintExists, Label: "NAMED", Properties: []string{"name"}}))
	require.NoError(t, env.schema.AddPropertyTypeConstraint("typed_age", "TYPED", "age", PropertyTypeInteger))
	require.NoError(t, env.write("seed", "", "PRIMITIVE_REL", map[string]any{"required": int64(1)}))
	return env
}

// write commits one node with label, or a relationship of relType between
// two new nodes, with props.
func (env *contractOutputEnv) write(id, label, relType string, props map[string]any) error {
	tx, err := env.engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.SetNamespace("test"); err != nil {
		return err
	}
	if relType == "" {
		if _, err := tx.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{label}, Properties: props}); err != nil {
			return err
		}
		return tx.Commit()
	}
	for _, end := range []string{"a", "b"} {
		if _, err := tx.CreateNode(&Node{ID: NodeID("test:" + id + end), Labels: []string{"END"}, Properties: map[string]any{}}); err != nil {
			return err
		}
	}
	if err := tx.CreateEdge(&Edge{ID: EdgeID("test:" + id), StartNode: NodeID("test:" + id + "a"), EndNode: NodeID("test:" + id + "b"), Type: relType, Properties: props}); err != nil {
		return err
	}
	return tx.Commit()
}

// contractProbes are writes every installed rule must reject. Several use
// the name a mutation writes ("changed"), so a rule a caller changed by
// editing a returned value would accept them.
var contractProbes = []struct {
	name, label, relType string
	props                map[string]any
}{
	{"primitive-missing", "PRIMITIVE", "", map[string]any{"changed": int64(2)}},
	{"primitive-rel-duplicate", "", "PRIMITIVE_REL", map[string]any{"required": int64(1)}},
	{"primitive-rel-missing", "", "PRIMITIVE_REL", map[string]any{"changed": int64(1)}},
	{"key-missing", "KEYED", "", map[string]any{"a": int64(1), "changed": int64(1)}},
	{"domain-invalid", "DOMAIN", "", map[string]any{"color": "changed"}},
	{"exists-missing", "NAMED", "", map[string]any{"changed": "x"}},
	{"type-invalid", "TYPED", "", map[string]any{"age": "changed"}},
	{"adult-young", "ADULT", "", map[string]any{"age": int64(5)}},
}

// observe renders the installed rules and what every probe reports.
func (env *contractOutputEnv) observe() string {
	constraints := env.schema.GetAllConstraints()
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	typed := env.schema.GetAllPropertyTypeConstraints()
	sort.Slice(typed, func(i, j int) bool { return typed[i].Name < typed[j].Name })
	var reports []any
	for _, probe := range contractProbes {
		reports = append(reports, env.write("probe-"+probe.name, probe.label, probe.relType, probe.props))
	}
	return renderExported(constraints, typed, env.schema.GetAllConstraintContracts(), reports)
}

// contractOutputSubjects return what a public API hands a caller: the values
// it returns, and for an API that installs, the inputs it was given. Each
// runs on its own environment.
var contractOutputSubjects = map[string]func(t *testing.T, env *contractOutputEnv) []any{
	"constraints": func(t *testing.T, env *contractOutputEnv) []any {
		all := env.schema.GetAllConstraints()
		sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
		return []any{all, env.schema.GetConstraintsForLabels([]string{"PRIMITIVE", "KEYED", "DOMAIN", "NAMED", "PRIMITIVE_REL"})}
	},
	"persister": func(t *testing.T, env *contractOutputEnv) []any {
		var received []any
		env.schema.SetPersister(func(definition *SchemaDefinition) error {
			received = append(received, definition)
			return nil
		})
		require.NoError(t, env.schema.AddConstraint(Constraint{Name: "persisted", Type: ConstraintNodeKey, Label: "PERSISTED", Properties: []string{"p", "q"}}))
		require.NotEmpty(t, received)
		return received
	},
	"property-types": func(t *testing.T, env *contractOutputEnv) []any {
		return []any{env.schema.GetAllPropertyTypeConstraints(), env.schema.GetPropertyTypeConstraintsForLabels([]string{"TYPED"})}
	},
	"contracts": func(t *testing.T, env *contractOutputEnv) []any {
		return []any{env.schema.GetAllConstraintContracts(), env.schema.GetConstraintContractsForTarget(ConstraintEntityNode, "PRIMITIVE"),
			env.schema.CompiledConstraintContracts(), env.handle.Contract()}
	},
	"compile": func(t *testing.T, env *contractOutputEnv) []any {
		contract := ConstraintContract{Name: "compiled", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "COMPILED",
			Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 1", Properties: []string{"age"}}}}
		compiled, err := CompileConstraintContract(contract)
		require.NoError(t, err)
		return []any{compiled, compiled.Contract(), &contract}
	},
	"facts": func(t *testing.T, env *contractOutputEnv) []any {
		var copies []Constraint
		var values []any
		env.schema.VisitConstraints(nil, func(fact ConstraintFact) bool {
			copies = append(copies, fact.Constraint())
			values = append(values, fact.Name(), fact.Type(), fact.EntityType(), fact.Label(), fact.PropertyCount())
			for index := 0; index < fact.PropertyCount(); index++ {
				values = append(values, fact.Property(index))
			}
			return true
		})
		env.schema.VisitConstraints([]string{"PRIMITIVE"}, func(fact ConstraintFact) bool {
			copies = append(copies, fact.Constraint())
			return true
		})
		return []any{copies, values}
	},
	"definition": func(t *testing.T, env *contractOutputEnv) []any {
		return []any{env.schema.ExportDefinition()}
	},
	"install-inputs": func(t *testing.T, env *contractOutputEnv) []any {
		constraint := Constraint{Name: "key_extra", Type: ConstraintNodeKey, Label: "EXTRA", Properties: []string{"x", "y"}}
		require.NoError(t, env.schema.AddConstraint(constraint))
		return []any{&constraint}
	},
	"replace-input": func(t *testing.T, env *contractOutputEnv) []any {
		definition := env.schema.ExportDefinition()
		require.NoError(t, env.schema.ReplaceFromDefinition(definition))
		return []any{definition}
	},
	"compiled-companion-input": func(t *testing.T, env *contractOutputEnv) []any {
		contract := ConstraintContract{Name: "companion-compiled", TargetEntityType: "NODE", TargetLabelOrType: "COMPANION", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintNodeKey), Properties: []string{"required"}}}}
		companion := Constraint{Name: ConstraintContractEntryName(contract.Name, 0), Type: ConstraintNodeKey, Label: "COMPANION", Properties: []string{"required"}}
		handle, err := CompileConstraintContract(contract)
		require.NoError(t, err)
		require.NoError(t, env.schema.AddCompiledConstraintContractBundle(handle, []Constraint{companion}, nil, false))
		return []any{&companion, &contract}
	},
	"companion-input": func(t *testing.T, env *contractOutputEnv) []any {
		contract := ConstraintContract{Name: "companion", TargetEntityType: "NODE", TargetLabelOrType: "COMPANION", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintNodeKey), Properties: []string{"required"}}}}
		companion := Constraint{Name: ConstraintContractEntryName(contract.Name, 0), Type: ConstraintNodeKey, Label: "COMPANION", Properties: []string{"required"}}
		require.NoError(t, env.schema.AddConstraintContractBundle(contract, []Constraint{companion}, nil, false))
		return []any{&companion, &contract}
	},
	"violations": func(t *testing.T, env *contractOutputEnv) []any {
		var reports []any
		for _, probe := range contractProbes {
			reports = append(reports, env.write("subject-"+probe.name, probe.label, probe.relType, probe.props))
		}
		// The same rules through bulk writes, and through the creation check
		// of a constraint over existing data.
		reports = append(reports, env.engine.BulkCreateNodes([]*Node{{ID: "test:bulk", Labels: []string{"PRIMITIVE"}, Properties: map[string]any{"changed": int64(1)}}}))
		require.NoError(t, env.write("existing", "SCANNED", "", map[string]any{"other": int64(1)}))
		for _, constraint := range env.schema.GetAllConstraints() {
			if constraint.Name == "key_node" {
				constraint.Label = "SCANNED"
				reports = append(reports, ValidateConstraintOnCreationForEngine(NewNamespacedEngine(env.engine, "test"), constraint))
			}
		}
		// A primitive contract's existing data is checked through its
		// companion constraint, like any key constraint above.
		for index, report := range reports {
			err, _ := report.(error)
			require.Error(t, err, "report %d", index)
		}
		return reports
	},
}

// Every public output that carries contract data is the caller's own: a
// caller changes everything it can reach from what an API returned (and, for
// an API that installs, from what it was given), and neither the installed
// rules, what they reject, nor what the same API returns on another schema
// change. The violations include the round-11 review's scenario: change a
// key violation's Properties[0], then omit the required property.
func TestContractPublicOutputsAreDetached(t *testing.T) {
	for name, subject := range contractOutputSubjects {
		t.Run(name, func(t *testing.T) {
			env := newContractOutputEnv(t)
			returned := subject(t, env)
			want := renderExported(returned...)
			installed := env.observe()
			mutateAll(returned...)
			require.Equal(t, installed, env.observe(), "changing what the API returned or was given changed the installed rules")
			require.Equal(t, want, renderExported(subject(t, newContractOutputEnv(t))...), "the same API on another schema returned what this caller changed")
		})
	}
}

// The violation route reaches every report the probes produce, so a probe a
// changed rule would accept is visible: each probe is rejected to begin with.
func TestContractProbesAreRejected(t *testing.T) {
	env := newContractOutputEnv(t)
	for _, probe := range contractProbes {
		err := env.write("check-"+probe.name, probe.label, probe.relType, probe.props)
		require.Error(t, err, probe.name)
	}
}

// A reader that only decides something reads constraints without copying
// them: the visitor allocates the same with 2 or 65 constraints installed,
// each with a property list. The copying getter, whose result leaves the
// package, copies each list: the positive control.
func TestConstraintReadsCopyNothing(t *testing.T) {
	measure := func(extra int) (visit, copies float64) {
		schema := newDetachedEngine(t).GetSchemaForNamespace("test")
		for index := 0; index <= extra; index++ {
			require.NoError(t, schema.AddConstraint(Constraint{Name: fmt.Sprintf("key_%d", index), Type: ConstraintNodeKey,
				Label: fmt.Sprintf("Label%d", index), Properties: []string{"a", "b"}}))
		}
		visit = testing.AllocsPerRun(20, func() {
			schema.VisitConstraints(nil, func(fact ConstraintFact) bool {
				_ = fact.Property(fact.PropertyCount() - 1)
				return true
			})
		})
		copies = testing.AllocsPerRun(20, func() { _ = schema.GetAllConstraints() })
		return visit, copies
	}
	fewVisit, fewCopies := measure(1)
	manyVisit, manyCopies := measure(64)
	require.Equal(t, fewVisit, manyVisit, "reading 63 more constraints allocated")
	require.GreaterOrEqual(t, manyCopies-fewCopies, float64(63), "the copying getter copies each property list")
}

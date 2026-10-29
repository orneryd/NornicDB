package storage

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// referenceKinds lists every path in typ that is not an immutable value: a
// map, slice, pointer, interface, function, channel, unsafe pointer or
// uintptr, at any depth of struct fields and array elements. Go types hold
// themselves only through those kinds, so the walk is finite and complete.
func referenceKinds(typ reflect.Type, path string) []string {
	switch typ.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return nil
	case reflect.Array:
		return referenceKinds(typ.Elem(), path+"[n]")
	case reflect.Struct:
		var found []string
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			found = append(found, referenceKinds(field.Type, path+"."+field.Name)...)
		}
		return found
	}
	return []string{path + " is a " + typ.Kind().String()}
}

// exportedFields lists every exported struct field a caller can reach in
// typ: through exported fields and array elements, at any depth. An
// unexported field ends the walk; no caller can select through it.
func exportedFields(typ reflect.Type, path string) []string {
	switch typ.Kind() {
	case reflect.Array:
		return exportedFields(typ.Elem(), path+"[n]")
	case reflect.Struct:
		var found []string
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			if field.IsExported() {
				found = append(found, path+"."+field.Name)
				found = append(found, exportedFields(field.Type, path+"."+field.Name)...)
			}
		}
		return found
	}
	return nil
}

// The error a contract reports for an entry that does not compile holds only
// immutable values, none of them exported: nothing in it can be reached or
// changed by a caller, so no internal path that keeps or shares it matters.
func TestContractErrorHoldsOnlyValues(t *testing.T) {
	boundary := reflect.TypeOf(contractError{})
	require.Empty(t, referenceKinds(boundary, "contractError"))
	require.Empty(t, exportedFields(boundary, "contractError"))
	require.Equal(t, []string{"root.Inner", "root.Inner.Text"}, exportedFields(reflect.TypeOf(struct{ Inner struct{ Text string } }{}), "root"))
	require.Equal(t, []string{"root.Levels", "root.Levels[n].Code"}, exportedFields(reflect.TypeOf(struct{ Levels [2]struct{ Code string } }{}), "root"))
	require.Empty(t, exportedFields(reflect.TypeOf(struct{ hidden struct{ Code string } }{}), "root"))
}

// The walk accepts values and rejects every reference kind, directly and
// nested in fields and array elements.
func TestRetainedErrorValueCheckRejectsReferences(t *testing.T) {
	type values struct {
		text    string
		flag    bool
		number  int64
		ratio   float64
		code    [4]uint8
		nested  struct{ inner [2]struct{ name string } }
		complex complex128
	}
	require.Empty(t, referenceKinds(reflect.TypeOf(values{}), "values"))
	for name, test := range map[string]struct {
		value any
		want  string
	}{
		"map":       {struct{ data map[string]any }{}, "root.data is a map"},
		"slice":     {struct{ list []any }{}, "root.list is a slice"},
		"pointer":   {struct{ cause *localization.LocalizedError }{}, "root.cause is a ptr"},
		"interface": {struct{ cause error }{}, "root.cause is a interface"},
		"func":      {struct{ build func() error }{}, "root.build is a func"},
		"chan":      {struct{ reports chan error }{}, "root.reports is a chan"},
		"unsafe":    {struct{ raw unsafe.Pointer }{}, "root.raw is a unsafe.Pointer"},
		"uintptr":   {struct{ address uintptr }{}, "root.address is a uintptr"},
		"in-array":  {[2]struct{ data map[string]any }{}, "root[n].data is a map"},
		"in-struct": {struct{ level struct{ plural any } }{}, "root.level.plural is a interface"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, []string{test.want}, referenceKinds(reflect.TypeOf(test.value), "root"))
		})
	}
}

// A rebuilt error is the retained one: the same code, message ID, fallback,
// plural count and data, value types included, at every level of its chain.
// Errors that are not localized keep their text.
func TestRetainedErrorRebuildsTheSameError(t *testing.T) {
	leaf := localization.NewLocalizedError("Leaf", localization.Message{ID: "leaf", Fallback: "leaf fallback"}, nil)
	data := map[string]any{"text": "a:b", "empty": "", "flag": true, "count": 3, "wide": int64(-1 << 40), "ratio": 0.25,
		"names": []string{"x", ""}, "list": []any{"x", int64(1), nil, []any{false}}, "map": map[string]any{"k": map[string]any{}},
		"error": leaf}
	input := localization.NewLocalizedError("Outer", localization.Message{ID: "outer", Fallback: "outer fallback", Data: data, PluralCount: int64(2)}, leaf)
	rebuilt := retainError(input).err()
	require.Equal(t, input, rebuilt)
	require.NotSame(t, input, rebuilt)

	empty := localization.NewLocalizedError("Empty", localization.Message{ID: "empty", Data: map[string]any{}}, nil)
	require.Equal(t, empty, retainError(empty).err(), "empty data stays empty, not nil")
	none := localization.NewLocalizedError("None", localization.Message{ID: "none"}, nil)
	require.Equal(t, none, retainError(none).err(), "no data stays nil")
	other := localization.NewLocalizedError("Other", localization.Message{ID: "other", Data: map[string]any{"small": int32(7)}}, nil)
	require.Equal(t, "7", retainError(other).err().(*localization.LocalizedError).Message.Data["small"], "another type is kept as its text")

	_, atoi := strconv.Atoi("99999999999999999999999")
	text := retainError(fmt.Errorf("wrap: %w", atoi)).err()
	var levels []string
	for level := text; level != nil; level = errors.Unwrap(level) {
		levels = append(levels, level.Error())
	}
	require.Equal(t, []string{"wrap: " + atoi.Error(), atoi.Error(), strconv.ErrRange.Error()}, levels)
	require.Nil(t, retainError(nil).err())
	require.Equal(t, retainedError{}, retainError(nil))
}

// publicLocalized is the localized error a caller reads at one level of an
// error chain: the level itself, or the one its As method hands out.
func publicLocalized(level error) *localization.LocalizedError {
	if localized, ok := level.(*localization.LocalizedError); ok {
		return localized
	}
	var localized *localization.LocalizedError
	if as, ok := level.(interface{ As(any) bool }); ok && as.As(&localized) {
		return localized
	}
	return nil
}

// renderPublic renders what a caller can read from an error chain through
// its public accessors: every level's text and, for a localized level, its
// code, message ID, fallback, data and plural count. Nothing of where the
// error lives in memory is rendered.
func renderPublic(value any) string {
	switch typed := value.(type) {
	case nil:
		return "nil"
	case error:
		var levels []string
		for level := typed; level != nil; level = errors.Unwrap(level) {
			if localized := publicLocalized(level); localized != nil {
				levels = append(levels, fmt.Sprintf("localized{code=%q id=%q fallback=%q text=%q data=%s plural=%s}", localized.Code,
					localized.Message.ID, localized.Message.Fallback, level.Error(), renderPublic(localized.Message.Data), renderPublic(localized.Message.PluralCount)))
			} else {
				levels = append(levels, fmt.Sprintf("error{%q}", level.Error()))
			}
		}
		return strings.Join(levels, " <- ")
	case map[string]any:
		if typed == nil {
			return "map(nil)"
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for index, key := range keys {
			parts[index] = key + ":" + renderPublic(typed[key])
		}
		return "map{" + strings.Join(parts, ",") + "}"
	case []any:
		parts := make([]string, len(typed))
		for index, item := range typed {
			parts[index] = renderPublic(item)
		}
		return "list[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprintf("%T(%#v)", value, value)
}

// mutatePublic changes everything a caller can reach from err through public
// accessors: at every level, the localized code, message ID, fallback, data
// (nested maps, lists and errors included) and plural count, then its cause.
func mutatePublic(err error) {
	for level := err; level != nil; {
		next := errors.Unwrap(level)
		if localized := publicLocalized(level); localized != nil {
			localized.Code, localized.Message.ID, localized.Message.Fallback = "changed", "changed", "caller changed the error"
			mutatePublicValue(localized.Message.Data)
			if localized.Message.Data != nil {
				localized.Message.Data["added"] = "caller"
			}
			mutatePublicValue(localized.Message.PluralCount)
			localized.Message.PluralCount = "changed"
			localized.Cause = errors.New("caller replaced the cause")
		}
		level = next
	}
}

func mutatePublicValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			mutatePublicValue(item)
			typed[key] = "changed"
		}
	case []any:
		for index, item := range typed {
			mutatePublicValue(item)
			typed[index] = "changed"
		}
	case []string:
		for index := range typed {
			typed[index] = "changed"
		}
	case error:
		mutatePublic(typed)
	}
}

// Every value shape the encoding supports, in message data and plural
// counts and through causes: a caller changes everything it can reach from
// one public read of a reported error, nested lists and maps included, and
// the report, a second read and another report of the same error all read
// as before. Changing the error the report was made from changes nothing
// either.
func TestContractErrorPayloadIsDetached(t *testing.T) {
	leaf := localization.NewLocalizedError("Leaf", localization.Message{ID: "leaf", Fallback: "leaf", Data: map[string]any{"names": []string{"n"}}}, nil)
	data := map[string]any{"text": "a", "count": 3, "wide": int64(2), "ratio": 0.5, "flag": true,
		"list": []any{"x", int64(1), []any{"nested", []string{"deep"}}}, "names": []string{"p", "q"},
		"map":   map[string]any{"inner": []any{"y", map[string]any{"k": []any{int64(9)}}}},
		"error": leaf, "wrapped": fmt.Errorf("wrapped: %w", leaf)}
	input := localization.NewLocalizedError("Outer", localization.Message{ID: "outer", Fallback: "outer", Data: data, PluralCount: []any{int64(1), "two"}},
		fmt.Errorf("cause wrapper: %w", leaf))
	reported, other := reportedContractError(input), reportedContractError(input)
	want := renderPublic(reported)
	var first *localization.LocalizedError
	require.True(t, errors.As(reported, &first))
	mutatePublic(first)
	mutatePublic(reported)
	mutatePublic(input)
	require.Equal(t, want, renderPublic(reported), "changing one read changed the report")
	var second *localization.LocalizedError
	require.True(t, errors.As(reported, &second))
	require.NotSame(t, first, second)
	mutatePublic(second)
	require.Equal(t, want, renderPublic(other), "changing reads of one report changed another report")
	require.Equal(t, want, renderPublic(reportedContractError(reported)), "the report is encoded again unchanged")
	require.Equal(t, want, renderPublic(reported))
}

// contractErrorScenarios are entries that do not compile, one for each error
// the predicate compiler can keep, for nodes and relationships.
var contractErrorScenarios = []struct {
	name, expression string
	relationship     bool
}{
	{"node-unsupported", "unsupported()", false},
	{"node-literal", "n.age = unsupported()", false},
	{"node-in", "n.age IN [unsupported()]", false},
	{"node-pattern", "COUNT { nonsense } > 0", false},
	{"node-threshold", "COUNT { (n)-[:R]->() } > 999999999999999999999999999999999", false},
	{"relationship-unsupported", "unsupported()", true},
	{"relationship-literal", "r.w = unsupported()", true},
	{"relationship-in", "r.w IN [unsupported()]", true},
}

func scenarioContract(expression string, relationship bool) ConstraintContract {
	kind, entity := ConstraintContractKindBooleanNode, string(ConstraintEntityNode)
	if relationship {
		kind, entity = ConstraintContractKindBooleanRelationship, string(ConstraintEntityRelationship)
	}
	return ConstraintContract{Name: "detached", TargetEntityType: entity, TargetLabelOrType: "DETACHED",
		Entries: []ConstraintContractEntry{{Kind: kind, Expression: expression}}}
}

// commitScenarioEntity commits one entity the scenario's contract targets.
func commitScenarioEntity(engine *BadgerEngine, id string, relationship bool) error {
	if relationship {
		return commitContractEntity(engine, id, "", "DETACHED")
	}
	return commitContractEntity(engine, id, "DETACHED", "")
}

// contractErrorSubjects are the public calls that report an error from a
// contract: expression validation, compilation, the creation scan of existing
// data, and real commits after the bundle and reload routes install it. Each
// returns a call that can be repeated.
var contractErrorSubjects = map[string]func(t *testing.T, contract ConstraintContract, relationship bool) func() error{
	"validate": func(t *testing.T, contract ConstraintContract, _ bool) func() error {
		return func() error { return ValidateConstraintContractExpressions(contract) }
	},
	"compile": func(t *testing.T, contract ConstraintContract, _ bool) func() error {
		return func() error { _, err := CompileConstraintContract(contract); return err }
	},
	"creation-scan": func(t *testing.T, contract ConstraintContract, relationship bool) func() error {
		engine := newDetachedEngine(t)
		require.NoError(t, commitScenarioEntity(engine, "existing", relationship))
		return func() error {
			return ValidateConstraintContractOnCreationForEngine(NewNamespacedEngine(engine, "test"), contract)
		}
	},
	"bundle-commit": func(t *testing.T, contract ConstraintContract, relationship bool) func() error {
		engine := newDetachedEngine(t)
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false))
		return func() error { return commitScenarioEntity(engine, "commit", relationship) }
	},
	"reload-commit": func(t *testing.T, contract ConstraintContract, relationship bool) func() error {
		engine := newDetachedEngine(t)
		require.NoError(t, engine.GetSchemaForNamespace("test").ReplaceFromDefinition(&SchemaDefinition{ConstraintContracts: []ConstraintContract{contract}}))
		return func() error { return commitScenarioEntity(engine, "commit", relationship) }
	},
}

func newDetachedEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })
	return engine
}

// Every public call that reports a contract's error, for every error the
// predicate compiler keeps: a caller changes everything it can reach from
// the error through Error, Unwrap and As, at every level. The error itself
// still reads the same, the next report (a fresh validation, compilation,
// scan or commit) reads the same, and two reads hand out two copies. The
// reviewers' scenarios are among these: unwrap a rejected commit, change its
// localized cause and commit again, through the bundle and through a reload.
// Nothing here depends on which internal path built the error.
func TestConstraintContractErrorsAreDetached(t *testing.T) {
	for _, scenario := range contractErrorScenarios {
		for subject, prepare := range contractErrorSubjects {
			t.Run(scenario.name+"/"+subject, func(t *testing.T) {
				call := prepare(t, scenarioContract(scenario.expression, scenario.relationship), scenario.relationship)
				first := call()
				require.Error(t, first)
				var reported contractError
				require.True(t, errors.As(first, &reported), "the contract reports %T", first)
				want, wantReported := renderPublic(first), renderPublic(reported)
				// A commit wraps the report in its own error, built for that
				// commit; the contract's report hands out a copy on each read.
				var one, two *localization.LocalizedError
				require.True(t, errors.As(reported, &one))
				require.True(t, errors.As(reported, &two))
				require.NotSame(t, one, two, "two reads must hand out two copies")
				require.NotSame(t, errors.Unwrap(reported), errors.Unwrap(reported), "two unwraps must hand out two causes")
				mutatePublic(first)
				mutatePublic(one)
				require.Equal(t, wantReported, renderPublic(reported), "changing what the error handed out changed the error")
				require.Equal(t, want, renderPublic(call()), "changing what one caller received changed the next report")
			})
		}
	}
	// A violation is built for each report; changing one changes no later one.
	engine := newDetachedEngine(t)
	contract := ConstraintContract{Name: "adults", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "DETACHED",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 10"}}}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false))
	first := commitScenarioEntity(engine, "young", false)
	require.Error(t, first)
	want := renderPublic(first)
	mutatePublic(first)
	require.Equal(t, want, renderPublic(commitScenarioEntity(engine, "young", false)))
}

// commitContractEntity commits one node with label, or a relationship of
// relType between two new nodes, and returns the commit's error.
func commitContractEntity(engine *BadgerEngine, id, label, relType string) error {
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.SetNamespace("test"); err != nil {
		return err
	}
	if relType == "" {
		if _, err := tx.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{label}, Properties: map[string]any{"age": int64(5)}}); err != nil {
			return err
		}
		return tx.Commit()
	}
	for _, end := range []string{"a", "b"} {
		if _, err := tx.CreateNode(&Node{ID: NodeID("test:" + id + end), Labels: []string{"End"}, Properties: map[string]any{}}); err != nil {
			return err
		}
	}
	if err := tx.CreateEdge(&Edge{ID: EdgeID("test:" + id), StartNode: NodeID("test:" + id + "a"), EndNode: NodeID("test:" + id + "b"), Type: relType, Properties: map[string]any{"w": int64(1)}}); err != nil {
		return err
	}
	return tx.Commit()
}

// BenchmarkRetainedErrorRebuild measures what a report of a retained compile
// error costs: one decode of the record into a new error. Only commits that
// reach an entry which does not compile pay it.
func BenchmarkRetainedErrorRebuild(b *testing.B) {
	_, err := compileNodeConstraintContractExpression("n.age = unsupported()")
	record := retainError(err)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if record.err() == nil {
			b.Fatal("no error rebuilt")
		}
	}
}

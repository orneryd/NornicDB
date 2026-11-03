package convert

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// JSONEncoder is constructed only by a domain entry point. FLOAT has a fractional
// or exponent part, including in typed envelopes. Custom JSON/text marshalers,
// raw JSON, binary data, field tags and encoding/json's errors retain their
// contracts. Writers traverse borrowed values directly without proxy envelopes
// or copied property containers. Field plans contain immutable metadata only.
type JSONEncoder struct {
	writer         io.Writer
	escapeHTML     bool
	prefix, indent string
	err            error
}

func newJSONEncoder(writer io.Writer) *JSONEncoder {
	return &JSONEncoder{writer: writer, escapeHTML: true}
}
func (encoder *JSONEncoder) SetEscapeHTML(escape bool) { encoder.escapeHTML = escape }
func (encoder *JSONEncoder) SetIndent(prefix, indent string) {
	encoder.prefix, encoder.indent = prefix, indent
}

func (encoder *JSONEncoder) Encode(value interface{}) error {
	if encoder.err != nil {
		return encoder.err
	}
	state := jsonWriterStates.Get().(*jsonWriterState)
	defer state.release()
	state.reset(encoder.escapeHTML)
	state.formatting = encoder.prefix != "" || encoder.indent != ""
	state.prefix, state.indent = encoder.prefix, encoder.indent
	if err := state.any(value); err != nil {
		return err
	}
	data := append(state.data, '\n')
	_, encoder.err = encoder.writer.Write(data)
	return encoder.err
}

func marshalJSON(value interface{}, typedState bool) ([]byte, error) {
	state := jsonWriterStates.Get().(*jsonWriterState)
	defer state.release()
	state.reset(true)
	state.typedState = typedState
	var err error
	if typedState {
		err = state.value(reflect.ValueOf(value))
	} else {
		err = state.any(value)
	}
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), state.data...), nil
}

// marshalJSONIndent formats while the domain writer traverses the borrowed
// value, so indentation needs no compact document, second indenter or extra
// pooled buffer. The returned document is a caller-owned copy of the single
// formatted working buffer.
func marshalJSONIndent(value interface{}, prefix, indent string) ([]byte, error) {
	state := jsonWriterStates.Get().(*jsonWriterState)
	defer state.release()
	state.reset(true)
	state.formatting, state.prefix, state.indent = true, prefix, indent
	if err := state.any(value); err != nil {
		return nil, err
	}
	return append([]byte(nil), state.data...), nil
}

// jsonFloat is Neo4j HTTP's FLOAT representation. Nonfinite values are text
// because JSON has no such numbers; ordinary JSON encoding still rejects them.
func jsonFloat(value float64, bits int) interface{} {
	text := FormatFloat(value, bits)
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return text
	}
	return json.Number(text)
}

// prepareJSON adapts a custom serializer's envelope to the same domain writer.
// The adapter borrows the value; it does not prepare another object graph.
func prepareJSON(value interface{}) interface{} {
	return preparedJSON{value}
}

type preparedJSON struct{ value interface{} }

func (value preparedJSON) MarshalJSON() ([]byte, error) { return marshalJSON(value.value, false) }

type jsonEncodingVisit struct {
	kind   reflect.Kind
	ptr    uintptr
	length int
}

// Domain writers traverse borrowed values once. Cached field descriptions are
// metadata only: no proxy envelope, converted slice or copied property map is
// allocated. Opaque marshalers and ,string retain the standard library owner.
type jsonWriterState struct {
	data           []byte
	raw, fragment  bytes.Buffer
	opaque         *jsontext.Encoder
	escapeHTML     bool
	typedState     bool
	formatting     bool
	prefix, indent string
	depth          int // walk depth, cycle detection only
	containerDepth int // open ordinary containers; pointers and interfaces are not containers
	active         map[jsonEncodingVisit]bool
}

var jsonWriterStates = sync.Pool{New: func() interface{} { return new(jsonWriterState) }}

// release drops borrowed formatting strings before returning the working
// buffer. A one-shot prefix/indent must not extend its caller's lifetime.
func (state *jsonWriterState) release() {
	state.prefix, state.indent = "", ""
	jsonWriterStates.Put(state)
}

func (state *jsonWriterState) reset(escape bool) {
	state.data = state.data[:0]
	state.escapeHTML, state.depth, state.typedState = escape, 0, false
	state.formatting, state.prefix, state.indent, state.containerDepth = false, "", "", 0
}

// The container, separator and colon operations own both compact and formatted
// tokens. Compact mode writes the release bytes; formatted mode writes the
// standard encoder's line breaks, prefix and indentation.
func (state *jsonWriterState) openContainer(token byte) {
	state.data = append(state.data, token)
	state.containerDepth++
}

func (state *jsonWriterState) elementSeparator(first bool) {
	// Reserve scalar headroom in geometric size classes. Starting from a
	// fixed class prevents a small append-selected capacity from setting an
	// oversized doubling sequence retained by a later large export.
	if cap(state.data)-len(state.data) < 64 {
		if len(state.data) < 4096 {
			buffer := bytes.NewBuffer(state.data)
			buffer.Grow(64)
			state.data = buffer.Bytes()
		} else {
			capacity := 128
			for capacity < len(state.data)+64 {
				capacity *= 2
			}
			data := make([]byte, len(state.data), capacity)
			copy(data, state.data)
			state.data = data
		}
	}
	if !first {
		state.data = append(state.data, ',')
	}
	if state.formatting {
		state.appendNewline(state.containerDepth)
	}
}

func (state *jsonWriterState) closeContainer(token byte, empty bool) {
	if state.formatting && !empty {
		state.appendNewline(state.containerDepth - 1)
	}
	state.data = append(state.data, token)
	state.containerDepth--
}

func (state *jsonWriterState) colon() {
	state.data = append(state.data, ':')
	if state.formatting {
		state.data = append(state.data, ' ')
	}
}

func (state *jsonWriterState) appendNewline(depth int) {
	state.data = append(state.data, '\n')
	state.data = append(state.data, state.prefix...)
	for index := 0; index < depth; index++ {
		state.data = append(state.data, state.indent...)
	}
}

func (state *jsonWriterState) opaqueValue(value interface{}) error {
	state.raw.Reset()
	if state.opaque == nil {
		state.opaque = jsontext.NewEncoder(&state.raw)
	}
	state.opaque.SetEscapeHTML(state.escapeHTML)
	if err := state.opaque.Encode(value); err != nil {
		// An unusable json.Number lexeme keeps the standard numeric owner's
		// rejection, exactly as appendJSONFloat does for a nonfinite FLOAT: the
		// opaque encoder's message is not the numeric error contract. Only this
		// failure path consults it, so a usable lexeme costs nothing.
		if _, numeric := value.(json.Number); numeric {
			if _, canonical := json.Marshal(value); canonical != nil {
				return canonical
			}
		}
		return err
	}
	data := state.raw.Bytes()
	return state.writeFragment(data[:len(data)-1])
}

// writeFragment appends one compact standard JSON fragment produced by an
// opaque marshaler. Only opaque fragments use the standard indenter, at the
// parent container's prefix and depth; ordinary documents are formatted by the
// domain writer itself.
func (state *jsonWriterState) writeFragment(fragment []byte) error {
	if !state.formatting || len(fragment) == 0 || fragment[0] != '{' && fragment[0] != '[' {
		state.data = append(state.data, fragment...)
		return nil
	}
	state.fragment.Reset()
	prefix := state.prefix
	if state.indent != "" {
		prefix += strings.Repeat(state.indent, state.containerDepth)
	}
	if err := json.Indent(&state.fragment, fragment, prefix, state.indent); err != nil {
		return err
	}
	state.data = append(state.data, state.fragment.Bytes()...)
	return nil
}

func (state *jsonWriterState) float(value float64, bits int, original reflect.Value) error {
	var err error
	state.data, err = appendJSONFloat(state.data, value, bits, original, state.typedState)
	return err
}

func (state *jsonWriterState) any(value interface{}) error {
	// Crossing an interface loses the declared numeric type. Even a typed
	// container nested here needs explicit FLOAT tokens for an exact reader.
	typedState := state.typedState
	state.typedState = false
	defer func() { state.typedState = typedState }()
	switch value := value.(type) {
	case nil:
		state.data = append(state.data, "null"...)
	case string:
		state.data = appendJSONString(state.data, value, state.escapeHTML)
	case bool:
		state.data = strconv.AppendBool(state.data, value)
	case int:
		state.data = strconv.AppendInt(state.data, int64(value), 10)
	case int64:
		state.data = strconv.AppendInt(state.data, value, 10)
	case uint64:
		state.data = strconv.AppendUint(state.data, value, 10)
	case float64:
		return state.float(value, 64, reflect.Value{})
	case float32:
		return state.float(float64(value), 32, reflect.Value{})
	case json.Number:
		if number, ok := ReadNumber(value); ok {
			if floating, ok := number.Float(); ok {
				return state.float(floating, 64, reflect.Value{})
			}
		}
		return state.opaqueValue(value)
	default:
		return state.value(reflect.ValueOf(value))
	}
	return nil
}

var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
var jsonTextType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var jsonNumberType = reflect.TypeOf(json.Number(""))
var jsonMethods sync.Map // reflect.Type -> jsonEncodingMethods
type jsonEncodingMethods struct{ value, address bool }

func jsonValueMethods(typ reflect.Type) jsonEncodingMethods {
	if cached, ok := jsonMethods.Load(typ); ok {
		return cached.(jsonEncodingMethods)
	}
	methods := jsonEncodingMethods{
		value:   typ.Implements(jsonMarshalerType) || typ.Implements(jsonTextType),
		address: reflect.PointerTo(typ).Implements(jsonMarshalerType) || reflect.PointerTo(typ).Implements(jsonTextType),
	}
	actual, _ := jsonMethods.LoadOrStore(typ, methods)
	return actual.(jsonEncodingMethods)
}

func (state *jsonWriterState) value(value reflect.Value) error {
	if !value.IsValid() {
		return state.any(nil)
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return state.any(nil)
		}
		return state.any(value.Interface())
	}
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return state.any(nil)
	}
	typ := value.Type()
	if typ == jsonNumberType {
		return state.any(value.Interface())
	}
	if typ.NumMethod() != 0 || value.CanAddr() && reflect.PointerTo(typ).NumMethod() != 0 {
		methods := jsonValueMethods(typ)
		if value.CanAddr() && methods.address {
			return state.opaqueValue(value.Addr().Interface())
		}
		if methods.value {
			return state.opaqueValue(value.Interface())
		}
	}
	switch value.Kind() {
	case reflect.Bool:
		state.data = strconv.AppendBool(state.data, value.Bool())
		return nil
	case reflect.String:
		state.data = appendJSONString(state.data, value.String(), state.escapeHTML)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		state.data = strconv.AppendInt(state.data, value.Int(), 10)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		state.data = strconv.AppendUint(state.data, value.Uint(), 10)
		return nil
	case reflect.Float32:
		return state.float(value.Float(), 32, value)
	case reflect.Float64:
		return state.float(value.Float(), 64, value)
	}
	state.depth++
	defer func() { state.depth-- }()
	// The ordinary acyclic path allocates no visit map. Deep graphs retain
	// cycle errors without copying or modifying the source containers.
	if state.depth > 1000 && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice) && !value.IsNil() {
		visit := jsonEncodingVisit{kind: value.Kind()}
		if value.Kind() == reflect.Map {
			visit.ptr = uintptr(value.UnsafePointer())
		} else {
			visit.ptr = value.Pointer()
		}
		if value.Kind() == reflect.Slice {
			visit.length = value.Len()
		}
		if state.active[visit] {
			return &json.UnsupportedValueError{Value: value, Str: "encountered a cycle via " + typ.String()}
		}
		if state.active == nil {
			state.active = make(map[jsonEncodingVisit]bool)
		}
		state.active[visit] = true
		defer delete(state.active, visit)
	}
	switch value.Kind() {
	case reflect.Pointer:
		return state.value(value.Elem())
	case reflect.Map:
		if value.IsNil() {
			// Key validation precedes nil handling in encoding/json.
			return state.opaqueValue(value.Interface())
		}
		if values, ok := value.Interface().(map[string]interface{}); ok {
			keys := make([]string, 0, len(values))
			for key := range values {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			state.openContainer('{')
			for index, key := range keys {
				state.elementSeparator(index == 0)
				state.data = appendJSONString(state.data, key, state.escapeHTML)
				state.colon()
				if err := state.any(values[key]); err != nil {
					return err
				}
			}
			state.closeContainer('}', len(keys) == 0)
			return nil
		}
		return state.mapValue(value)
	case reflect.Slice:
		if value.IsNil() {
			return state.any(nil)
		}
		if typ.Elem().Kind() == reflect.Uint8 {
			if typ.Elem().NumMethod() != 0 || reflect.PointerTo(typ.Elem()).NumMethod() != 0 {
				return state.opaqueValue(value.Interface())
			}
			state.data = append(state.data, '"')
			state.data = base64.StdEncoding.AppendEncode(state.data, value.Bytes())
			state.data = append(state.data, '"')
			return nil
		}
		fallthrough
	case reflect.Array:
		state.openContainer('[')
		writeElement := jsonValueWriterFor(typ.Elem())
		for index := 0; index < value.Len(); index++ {
			state.elementSeparator(index == 0)
			if err := writeElement(state, value.Index(index)); err != nil {
				return err
			}
		}
		state.closeContainer(']', value.Len() == 0)
		return nil
	case reflect.Struct:
		plan := jsonEncodingStructPlan(typ)
		if plan.unsupportedFormat != "" {
			// The v1 options record a non-empty `format` value while compiling
			// the struct and Marshal reports it before writing or serializing
			// any member, so no field callback on this value may run.
			return &json.UnsupportedValueError{Str: "Go struct field " + plan.unsupportedFormat + " has unsupported `format` tag option"}
		}
		state.openContainer('{')
		first := true
		for _, field := range plan.fields {
			item := value
			for _, index := range field.index {
				if item.Kind() == reflect.Pointer {
					if item.IsNil() {
						item = reflect.Value{}
						break
					}
					item = item.Elem()
				}
				item = item.Field(index)
			}
			if !item.IsValid() || field.omitEmpty && jsonEncodingEmpty(item) || field.omitZero && jsonEncodingZero(item) {
				continue
			}
			state.elementSeparator(first)
			first = false
			if state.escapeHTML {
				state.data = append(state.data, field.htmlName...)
			} else {
				state.data = append(state.data, field.jsonName...)
			}
			state.colon()
			if field.quoted {
				quoted := reflect.New(field.quoteProxy).Elem()
				quoted.Field(0).Set(item)
				state.raw.Reset()
				if state.opaque == nil {
					state.opaque = jsontext.NewEncoder(&state.raw)
				}
				state.opaque.SetEscapeHTML(state.escapeHTML)
				if err := state.opaque.Encode(quoted.Interface()); err != nil {
					return err
				}
				raw := state.raw.Bytes()
				if err := state.writeFragment(raw[len(`{"v":`) : len(raw)-2]); err != nil {
					return err
				}
			} else if err := field.write(state, item); err != nil {
				return err
			}
		}
		state.closeContainer('}', first)
		return nil
	default:
		return state.opaqueValue(value.Interface())
	}
}

type jsonMapEntry struct {
	key   string
	value reflect.Value
}

func (state *jsonWriterState) mapValue(value reflect.Value) error {
	typ := value.Type().Key()
	kind := typ.Kind()
	textKey := typ.Implements(jsonTextType)
	if kind != reflect.String && !(kind >= reflect.Int && kind <= reflect.Uintptr) && !textKey {
		return state.opaqueValue(value.Interface())
	}
	entries := make([]jsonMapEntry, 0, value.Len())
	for iter := value.MapRange(); iter.Next(); {
		key := iter.Key()
		var name string
		switch {
		case kind == reflect.String:
			name = key.String()
		case textKey:
			if key.Kind() != reflect.Pointer || !key.IsNil() {
				text, err := key.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return fmt.Errorf("json: encoding error for type %q: %q", value.Type().String(), err.Error())
				}
				name = string(text)
			}
		case kind >= reflect.Int && kind <= reflect.Int64:
			name = strconv.FormatInt(key.Int(), 10)
		default:
			name = strconv.FormatUint(key.Uint(), 10)
		}
		entries = append(entries, jsonMapEntry{name, iter.Value()})
	}
	if len(entries) > 1 {
		sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	}
	state.openContainer('{')
	for index, entry := range entries {
		state.elementSeparator(index == 0)
		state.data = appendJSONString(state.data, entry.key, state.escapeHTML)
		state.colon()
		if err := state.value(entry.value); err != nil {
			return err
		}
	}
	state.closeContainer('}', len(entries) == 0)
	return nil
}

// appendJSONString preserves the release source-string owner. HTML selection
// belongs to encoding/json; logging has its independent release policy.
func appendJSONString(dst []byte, value string, escapeHTML bool) []byte {
	start := len(dst)
	dst = jsontext.AppendString(dst, value)
	if !escapeHTML || !strings.ContainsAny(value, "<>&") {
		return dst
	}
	var escaped bytes.Buffer
	json.HTMLEscape(&escaped, dst[start:])
	return append(dst[:start], escaped.Bytes()...)
}

var jsonEncodingPlans sync.Map // reflect.Type -> *jsonEncodingPlan
var jsonValueWriters sync.Map  // reflect.Type -> jsonValueWriter

type jsonValueWriter func(*jsonWriterState, reflect.Value) error

// Compile the scalar operation once per declared field/element type. Dynamic
// properties still dispatch on their actual value; typed envelopes need not
// re-discover method sets and numeric kinds at every leaf.
func jsonValueWriterFor(typ reflect.Type) jsonValueWriter {
	if cached, ok := jsonValueWriters.Load(typ); ok {
		return cached.(jsonValueWriter)
	}
	write := jsonValueWriter((*jsonWriterState).value)
	methods := jsonValueMethods(typ)
	if !methods.value && !methods.address {
		switch typ.Kind() {
		case reflect.Bool:
			write = jsonWriteBool
		case reflect.String:
			if typ != jsonNumberType {
				write = jsonWriteString
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			write = jsonWriteInt
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			write = jsonWriteUint
		case reflect.Float32:
			write = jsonWriteFloat32
		case reflect.Float64:
			write = jsonWriteFloat64
		}
	}
	actual, _ := jsonValueWriters.LoadOrStore(typ, write)
	return actual.(jsonValueWriter)
}

func jsonWriteBool(state *jsonWriterState, value reflect.Value) error {
	state.data = strconv.AppendBool(state.data, value.Bool())
	return nil
}
func jsonWriteString(state *jsonWriterState, value reflect.Value) error {
	state.data = appendJSONString(state.data, value.String(), state.escapeHTML)
	return nil
}
func jsonWriteInt(state *jsonWriterState, value reflect.Value) error {
	state.data = strconv.AppendInt(state.data, value.Int(), 10)
	return nil
}
func jsonWriteUint(state *jsonWriterState, value reflect.Value) error {
	state.data = strconv.AppendUint(state.data, value.Uint(), 10)
	return nil
}
func jsonWriteFloat32(state *jsonWriterState, value reflect.Value) error {
	return state.float(value.Float(), 32, value)
}
func jsonWriteFloat64(state *jsonWriterState, value reflect.Value) error {
	return state.float(value.Float(), 64, value)
}

type jsonEncodingField struct {
	name                                string
	index                               []int
	tagged, omitEmpty, omitZero, quoted bool
	quoteProxy                          reflect.Type
	jsonName, htmlName                  []byte
	write                               jsonValueWriter
}
type jsonEncodingPlan struct {
	fields            []jsonEncodingField
	unsupportedFormat string
}

// Resolve promoted fields once per envelope type. A shallow field dominates;
// at equal depth one explicitly named field wins, otherwise the name is omitted.
func jsonEncodingStructPlan(typ reflect.Type) *jsonEncodingPlan {
	if cached, ok := jsonEncodingPlans.Load(typ); ok {
		return cached.(*jsonEncodingPlan)
	}
	candidates := map[string][]jsonEncodingField{}
	var unsupportedFormat string
	var unsupportedDepth int
	var collect func(reflect.Type, []int, map[reflect.Type]bool)
	collect = func(current reflect.Type, path []int, parents map[reflect.Type]bool) {
		if parents[current] {
			return
		}
		parents[current] = true
		defer delete(parents, current)
		for i := 0; i < current.NumField(); i++ {
			field := current.Field(i)
			base := field.Type
			if base.Kind() == reflect.Pointer {
				base = base.Elem()
			}
			if field.PkgPath != "" && (!field.Anonymous || base.Kind() != reflect.Struct) {
				continue
			}
			metadata, ignored := jsonFieldTagMetadata(field)
			if ignored {
				continue
			}
			index := append(append([]int(nil), path...), i)
			if field.Anonymous && !metadata.hasName && base.Kind() == reflect.Struct {
				collect(base, index, parents)
				continue
			}
			if metadata.format && (unsupportedFormat == "" || len(index) < unsupportedDepth) {
				// Discovery recurses depth-first, but standard metadata errors
				// select the first field in breadth-first declaration order.
				unsupportedFormat, unsupportedDepth = field.Name, len(index)
			}
			entry := jsonEncodingField{name: metadata.name, index: index, tagged: metadata.hasName,
				omitEmpty: metadata.omitEmpty, omitZero: metadata.omitZero, quoted: metadata.quoted}
			entry.write = jsonValueWriterFor(field.Type)
			if entry.quoted {
				quotedType := field.Type
				if quotedType.Name() == "" && quotedType.Kind() == reflect.Pointer {
					quotedType = quotedType.Elem()
				}
				switch quotedType.Kind() {
				case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
					reflect.Float32, reflect.Float64, reflect.String:
					entry.quoteProxy = reflect.StructOf([]reflect.StructField{{Name: "V", Type: field.Type, Tag: `json:"v,string"`}})
				default:
					entry.quoted = false
				}
			}
			candidates[entry.name] = append(candidates[entry.name], entry)
		}
	}
	collect(typ, nil, map[reflect.Type]bool{})
	plan := &jsonEncodingPlan{unsupportedFormat: unsupportedFormat}
	for _, entries := range candidates {
		sort.SliceStable(entries, func(i, j int) bool { return len(entries[i].index) < len(entries[j].index) })
		shortest := len(entries[0].index)
		var selected []jsonEncodingField
		for _, field := range entries {
			if len(field.index) == shortest {
				selected = append(selected, field)
			}
		}
		if len(selected) > 1 {
			tagged := selected[:0:0]
			for _, field := range selected {
				if field.tagged {
					tagged = append(tagged, field)
				}
			}
			selected = tagged
		}
		if len(selected) == 1 {
			plan.fields = append(plan.fields, selected[0])
		}
	}
	sort.Slice(plan.fields, func(i, j int) bool {
		a, b := plan.fields[i].index, plan.fields[j].index
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return len(a) < len(b)
	})
	for i := range plan.fields {
		field := &plan.fields[i]
		field.jsonName = appendJSONString(nil, field.name, false)
		field.htmlName = appendJSONString(nil, field.name, true)
	}
	actual, _ := jsonEncodingPlans.LoadOrStore(typ, plan)
	return actual.(*jsonEncodingPlan)
}

// jsonFieldTag is the `json` tag metadata of one planned field: the JSON object
// name this writer emits, the options it implements, and whether the field
// carries a non-empty `format` value, which the standard encoder accepts in the
// tag and then rejects while marshaling the struct.
type jsonFieldTag struct {
	name                         string
	hasName, omitEmpty, omitZero bool
	quoted                       bool
	format                       bool
}

// jsonTagReserved holds the characters encoding/json reserves inside a tag
// instead of accepting them in the member name.
const jsonTagReserved = ",\"'`\\"

// jsonFieldTagMetadata reads field's `json` tag with encoding/json's own rule. The
// standard field planner (parseFieldOptions and consumeTagOption, reached through
// the package's default v1 options) owns the JSON object name and the omitempty,
// omitzero, string, case and format options, so the compiled plan must accept,
// recover from and ignore exactly the tag text the standard encoder does: a name
// ends at a reserved character and, when the tag does not continue with a comma,
// the whole tag is re-read as a tag option, which names the member when it starts
// with a Go identifier. Tag errors are not reported: the v1 options discard them
// and keep the recovered metadata. A `case` value and a `format` value are their
// own spans, so the characters inside them never become field options, and only a
// non-empty `format` value reaches the plan as a rejection.
func jsonFieldTagMetadata(field reflect.StructField) (jsonFieldTag, bool) {
	metadata := jsonFieldTag{name: field.Name}
	tag := field.Tag.Get("json")
	if tag == "-" {
		return metadata, true
	}
	if len(tag) > 0 && tag[0] != ',' {
		end := 0
		for end < len(tag) {
			r, size := utf8.DecodeRuneInString(tag[end:])
			if strings.ContainsRune(jsonTagReserved, r) {
				break
			}
			end += size
		}
		name, valid := tag[:end], true
		if end != len(tag) && tag[end] != ',' {
			name, end, valid = jsonTagOption(tag, false)
		}
		if !utf8.ValidString(name) {
			name = string([]rune(name)) // the standard planner's RuneError repair
		}
		if valid {
			metadata.name, metadata.hasName = name, true
		}
		tag = tag[end:]
	}
	for len(tag) > 0 {
		if tag[0] == ',' {
			tag = tag[1:]
			if len(tag) == 0 {
				break // a trailing comma ends the options
			}
		}
		option, end, _ := jsonTagOption(tag, false)
		tag = tag[end:]
		switch option {
		case "omitempty":
			metadata.omitEmpty = true
		case "omitzero":
			metadata.omitZero = true
		case "string":
			metadata.quoted = true
		case "case":
			if strings.HasPrefix(tag, ":") {
				tag = tag[1:]
				_, width, _ := jsonTagOption(tag, false)
				tag = tag[width:]
			}
		case "format":
			if !strings.HasPrefix(tag, ":") {
				break
			}
			tag = tag[1:]
			value, width, ok := jsonTagOption(tag, true)
			tag = tag[width:]
			if ok && value != "" {
				metadata.format = true
			}
		}
	}
	return metadata, false
}

// jsonTagOption consumes the next tag option and its width using encoding/json's
// grammar: a Go identifier, or, for a `format` value, a single-quoted Go string
// literal. The literal is converted to the equivalent double-quoted form and
// strconv.Unquote validates its escapes and reports the decoded value, so there
// is no second literal decoder. Anything else is skipped to the next comma with
// the invalid flag the v1 options discard.
func jsonTagOption(tag string, allowQuoted bool) (option string, width int, valid bool) {
	if first, _ := utf8.DecodeRuneInString(tag); first == '_' || unicode.IsLetter(first) {
		end := 0
		for end < len(tag) {
			r, size := utf8.DecodeRuneInString(tag[end:])
			if r != '_' && !unicode.IsLetter(r) && !unicode.IsNumber(r) {
				break
			}
			end += size
		}
		return tag[:end], end, true
	}
	if first, _ := utf8.DecodeRuneInString(tag); allowQuoted && first == '\'' {
		end := strings.IndexByte(tag, ',')
		if end < 0 {
			end = len(tag)
		}
		literal := []byte{'"'}
		cursor, escaped := len(`'`), false
		for cursor < len(tag) {
			r, size := utf8.DecodeRuneInString(tag[cursor:])
			switch {
			case escaped:
				if r == '\'' {
					literal = literal[:len(literal)-1]
				}
				escaped = false
			case r == '\\':
				escaped = true
			case r == '"':
				literal = append(literal, '\\')
			case r == '\'':
				literal = append(literal, '"')
				value, err := strconv.Unquote(string(literal))
				if err != nil {
					return tag[:end], end, false
				}
				return value, cursor + size, true
			}
			literal = append(literal, tag[cursor:cursor+size]...)
			cursor += size
		}
		return tag[:end], end, false
	}
	if end := strings.IndexByte(tag, ','); end >= 0 {
		return tag[:end], end, false
	}
	return tag, len(tag), false
}

func jsonEncodingEmpty(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

var jsonZeroType = reflect.TypeOf((*interface{ IsZero() bool })(nil)).Elem()

func jsonEncodingZero(value reflect.Value) bool {
	if value.Type().Implements(jsonZeroType) {
		if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil() {
			return true
		}
		if value.Kind() == reflect.Interface && value.Elem().Kind() == reflect.Pointer && value.Elem().IsNil() {
			return true
		}
		return value.Interface().(interface{ IsZero() bool }).IsZero()
	}
	if value.Kind() != reflect.Pointer && reflect.PointerTo(value.Type()).Implements(jsonZeroType) {
		if !value.CanAddr() {
			copy := reflect.New(value.Type()).Elem()
			copy.Set(value)
			value = copy
		}
		return value.Addr().Interface().(interface{ IsZero() bool }).IsZero()
	}
	return value.IsZero()
}

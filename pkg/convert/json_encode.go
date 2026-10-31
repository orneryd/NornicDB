package convert

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
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
	defer jsonWriterStates.Put(state)
	state.reset(encoder.escapeHTML)
	if err := state.any(value); err != nil {
		return err
	}
	data := state.data
	if encoder.prefix != "" || encoder.indent != "" {
		state.indented.Reset()
		if err := json.Indent(&state.indented, data, encoder.prefix, encoder.indent); err != nil {
			return err
		}
		data = state.indented.Bytes()
	}
	data = append(data, '\n')
	_, encoder.err = encoder.writer.Write(data)
	return encoder.err
}

func marshalJSON(value interface{}) ([]byte, error) {
	state := jsonWriterStates.Get().(*jsonWriterState)
	defer jsonWriterStates.Put(state)
	state.reset(true)
	if err := state.any(value); err != nil {
		return nil, err
	}
	return append([]byte(nil), state.data...), nil
}

func marshalJSONIndent(value interface{}, prefix, indent string) ([]byte, error) {
	data, err := marshalJSON(value)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := json.Indent(&out, data, prefix, indent); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
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

func (value preparedJSON) MarshalJSON() ([]byte, error) { return marshalJSON(value.value) }

type jsonEncodingVisit struct {
	kind   reflect.Kind
	ptr    uintptr
	length int
}

// Domain writers traverse borrowed values once. Cached field descriptions are
// metadata only: no proxy envelope, converted slice or copied property map is
// allocated. Opaque marshalers and ,string retain the standard library owner.
type jsonWriterState struct {
	data          []byte
	raw, indented bytes.Buffer
	opaque        *jsontext.Encoder
	escapeHTML    bool
	depth         int
	active        map[jsonEncodingVisit]bool
}

var jsonWriterStates = sync.Pool{New: func() interface{} { return new(jsonWriterState) }}

func (state *jsonWriterState) reset(escape bool) {
	state.data = state.data[:0]
	state.escapeHTML, state.depth = escape, 0
}

func (state *jsonWriterState) opaqueValue(value interface{}) error {
	state.raw.Reset()
	if state.opaque == nil {
		state.opaque = jsontext.NewEncoder(&state.raw)
	}
	state.opaque.SetEscapeHTML(state.escapeHTML)
	if err := state.opaque.Encode(value); err != nil {
		return err
	}
	data := state.raw.Bytes()
	state.data = append(state.data, data[:len(data)-1]...)
	return nil
}

func (state *jsonWriterState) float(value float64, bits int, original reflect.Value) error {
	var err error
	state.data, err = appendJSONFloat(state.data, value, bits, original)
	return err
}

func (state *jsonWriterState) any(value interface{}) error {
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
			state.data = append(state.data, '{')
			for i, key := range keys {
				if i != 0 {
					state.data = append(state.data, ',')
				}
				state.data = appendJSONString(state.data, key, state.escapeHTML)
				state.data = append(state.data, ':')
				if err := state.any(values[key]); err != nil {
					return err
				}
			}
			state.data = append(state.data, '}')
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
		state.data = append(state.data, '[')
		writeElement := jsonValueWriterFor(typ.Elem())
		for i := 0; i < value.Len(); i++ {
			if i != 0 {
				state.data = append(state.data, ',')
			}
			if err := writeElement(state, value.Index(i)); err != nil {
				return err
			}
		}
		state.data = append(state.data, ']')
		return nil
	case reflect.Struct:
		state.data = append(state.data, '{')
		first := true
		for _, field := range jsonEncodingStructPlan(typ).fields {
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
			if !first {
				state.data = append(state.data, ',')
			}
			first = false
			if state.escapeHTML {
				state.data = append(state.data, field.htmlName...)
			} else {
				state.data = append(state.data, field.jsonName...)
			}
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
				state.data = append(state.data, raw[len(`{"v":`):len(raw)-2]...)
			} else if err := field.write(state, item); err != nil {
				return err
			}
		}
		state.data = append(state.data, '}')
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
	state.data = append(state.data, '{')
	for i, entry := range entries {
		if i != 0 {
			state.data = append(state.data, ',')
		}
		state.data = appendJSONString(state.data, entry.key, state.escapeHTML)
		state.data = append(state.data, ':')
		if err := state.value(entry.value); err != nil {
			return err
		}
	}
	state.data = append(state.data, '}')
	return nil
}

func appendJSONString(dst []byte, value string, escapeHTML bool) []byte {
	if escapeHTML {
		return jsontext.AppendHTMLEscapedString(dst, value)
	}
	return jsontext.AppendString(dst, value)
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
	fields []jsonEncodingField
}

// Resolve promoted fields once per envelope type. A shallow field dominates;
// at equal depth one explicitly named field wins, otherwise the name is omitted.
func jsonEncodingStructPlan(typ reflect.Type) *jsonEncodingPlan {
	if cached, ok := jsonEncodingPlans.Load(typ); ok {
		return cached.(*jsonEncodingPlan)
	}
	candidates := map[string][]jsonEncodingField{}
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
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			name := parts[0]
			for _, char := range name {
				if !unicode.IsLetter(char) && !unicode.IsDigit(char) && !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", char) {
					name = ""
					break
				}
			}
			index := append(append([]int(nil), path...), i)
			if field.Anonymous && name == "" && base.Kind() == reflect.Struct {
				collect(base, index, parents)
				continue
			}
			entry := jsonEncodingField{name: name, index: index, tagged: name != ""}
			entry.write = jsonValueWriterFor(field.Type)
			if name == "" {
				entry.name = field.Name
			}
			for _, option := range parts[1:] {
				switch option {
				case "omitempty":
					entry.omitEmpty = true
				case "omitzero":
					entry.omitZero = true
				case "string":
					entry.quoted = true
				}
			}
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
	plan := &jsonEncodingPlan{}
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
		field.jsonName = append(appendJSONString(nil, field.name, false), ':')
		field.htmlName = append(appendJSONString(nil, field.name, true), ':')
	}
	actual, _ := jsonEncodingPlans.LoadOrStore(typ, plan)
	return actual.(*jsonEncodingPlan)
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

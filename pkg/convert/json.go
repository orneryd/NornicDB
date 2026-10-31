package convert

import (
	"encoding/json"
	"errors"

	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// jsonDecoder is private: only domain entry points can construct it. The
// underlying stdlib decoder and number policy cannot escape to consumers.
type jsonDecoder[domain jsonDecodeDomain] struct{ decoder jsontext.NumberDecoder }

type jsonNumberPolicy struct{ exact bool }

// Policy belongs to the concrete domain type, not per-reader storage. The
// private named field prevents access to the standard decoder while avoiding
// another allocated decoder object or a runtime policy field on every stream.
type jsonDecodeDomain interface{ numberPolicy() jsonNumberPolicy }
type jsonPropertyPolicy struct{}
type jsonInputPolicy struct{}

func (jsonPropertyPolicy) numberPolicy() jsonNumberPolicy { return jsonNumberPolicy{exact: true} }
func (jsonInputPolicy) numberPolicy() jsonNumberPolicy    { return jsonNumberPolicy{} }

func newJSONDecoder[domain jsonDecodeDomain](reader io.Reader) *jsonDecoder[domain] {
	return &jsonDecoder[domain]{decoder: *jsontext.NewNumberDecoder(reader)}
}

func (decoder *jsonDecoder[domain]) Decode(target interface{}) error {
	if err := decoder.decoder.Decode(target); err != nil {
		return err
	}
	var owner domain
	return normalizeDecodedJSON(reflect.ValueOf(target).Elem(), owner.numberPolicy())
}

func unmarshalJSON[domain jsonDecodeDomain](data []byte, target interface{}) error {
	value := reflect.ValueOf(target)
	// Typed scalars/containers already receive exact standard-library values.
	// Only interface-bearing targets need UseNumber and domain normalization.
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || !jsonNormalizationPlanFor(value.Type().Elem()).needed {
		return jsontext.Unmarshal(data, target)
	}
	if !json.Valid(data) {
		return jsontext.Unmarshal(data, target)
	}
	// A validated complete document can reuse the syntax decoder and its read
	// buffer. Streams keep their caller-owned lifetime. The reusable state has
	// no number policy: normalization remains bound to this domain's type.
	document := jsonDocumentDecoders.Get().(*jsonDocumentDecoder)
	document.data = data
	document.terminated = false
	if err := document.decoder.Decode(target); err != nil {
		return err // A failed decoder is not reusable.
	}
	document.data = nil
	jsonDocumentDecoders.Put(document)
	var owner domain
	return normalizeDecodedJSON(value.Elem(), owner.numberPolicy())
}

// Read supplies a whitespace terminator without allocating another input
// buffer. Numeric scalars then finish before the streaming decoder sees EOF;
// the next complete document can use the same decoder. json.Valid above owns
// the complete-document/trailing-data check.
type jsonDocumentDecoder struct {
	data       []byte
	decoder    *jsontext.NumberDecoder
	terminated bool
}

func (document *jsonDocumentDecoder) Read(target []byte) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	if len(document.data) == 0 {
		if document.terminated {
			return 0, io.EOF
		}
		target[0] = '\n'
		document.terminated = true
		return 1, nil
	}
	count := copy(target, document.data)
	document.data = document.data[count:]
	if len(document.data) == 0 && count < len(target) {
		target[count] = '\n'
		document.terminated = true
		count++
	}
	return count, nil
}

var jsonDocumentDecoders = sync.Pool{New: func() interface{} {
	document := &jsonDocumentDecoder{}
	document.decoder = jsontext.NewNumberDecoder(document)
	return document
}}

// UnsupportedNumberError is Neo4j's rejection of an external INTEGER outside
// the 64-bit range ("Unsupported type of Number").
type UnsupportedNumberError struct{ Lexeme string }

func (e *UnsupportedNumberError) Error() string { return "Unsupported type of Number " + e.Lexeme }

func jsonNumbers(value interface{}, policy jsonNumberPolicy) (interface{}, error) {
	switch v := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			integer, err := v.Int64()
			if err == nil {
				return integer, nil
			}
			if policy.exact {
				unsigned, unsignedErr := strconv.ParseUint(string(v), 10, 64)
				if unsignedErr != nil {
					return nil, fmt.Errorf("stored integer %q is outside the 64-bit integer range: %w", v, err)
				}
				return unsigned, nil
			}
			if errors.Is(err, strconv.ErrRange) {
				return nil, &UnsupportedNumberError{Lexeme: string(v)}
			}
			return nil, err
		}
		floating, err := v.Float64()
		if err != nil && !policy.exact && errors.Is(err, strconv.ErrRange) {
			// Neo4j reads an out-of-range FLOAT as +/-Infinity.
			return floating, nil
		}
		return floating, err
	case []interface{}:
		for index, item := range v {
			normalized, err := jsonNumbers(item, policy)
			if err != nil {
				return nil, err
			}
			v[index] = normalized
		}
	case map[string]interface{}:
		for key, item := range v {
			normalized, err := jsonNumbers(item, policy)
			if err != nil {
				return nil, err
			}
			v[key] = normalized
		}
	}
	return value, nil
}

// Only interface fields can contain numbers introduced by UseNumber. Walk the
// typed envelope to reach them; typed numbers, raw JSON, timestamps and vector
// element types remain unchanged. This keeps callers from copying the policy
// for each request, export or backup envelope.
func normalizeDecodedJSON(value reflect.Value, policy jsonNumberPolicy) error {
	if !value.IsValid() {
		return nil
	}
	plan := jsonNormalizationPlanFor(value.Type())
	if !plan.needed {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() && value.CanSet() {
			normalized, err := jsonNumbers(value.Interface(), policy)
			if err != nil {
				return err
			}
			value.Set(reflect.ValueOf(normalized))
		}
	case reflect.Pointer:
		if !value.IsNil() {
			return normalizeDecodedJSON(value.Elem(), policy)
		}
	case reflect.Struct:
		for _, index := range plan.fields {
			field := value.Field(index)
			if field.CanSet() {
				if err := normalizeDecodedJSON(field, policy); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if err := normalizeDecodedJSON(value.Index(index), policy); err != nil {
				return err
			}
		}
	case reflect.Map:
		if properties, ok := value.Interface().(map[string]interface{}); ok {
			_, err := jsonNumbers(properties, policy)
			return err
		}
		for iter := value.MapRange(); iter.Next(); {
			item := reflect.New(value.Type().Elem()).Elem()
			item.Set(iter.Value())
			if err := normalizeDecodedJSON(item, policy); err != nil {
				return err
			}
			value.SetMapIndex(iter.Key(), item)
		}
	}
	return nil
}

// UseNumber changes only interface values. Metadata-only plans keep ordinary
// typed control records, vectors, binary payloads and typed maps on their
// standard decoder path. The plan contains no policy or decoded value.
type jsonNormalizationPlan struct {
	needed bool
	fields []int
}

var jsonNormalizationPlans sync.Map // reflect.Type -> *jsonNormalizationPlan

func jsonNormalizationPlanFor(typ reflect.Type) *jsonNormalizationPlan {
	if cached, ok := jsonNormalizationPlans.Load(typ); ok {
		return cached.(*jsonNormalizationPlan)
	}
	plan := &jsonNormalizationPlan{needed: jsonContainsInterfaces(typ, map[reflect.Type]bool{})}
	if plan.needed && typ.Kind() == reflect.Struct {
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			if field.PkgPath == "" && field.Tag.Get("json") != "-" && jsonContainsInterfaces(field.Type, map[reflect.Type]bool{}) {
				plan.fields = append(plan.fields, index)
			}
		}
	}
	actual, _ := jsonNormalizationPlans.LoadOrStore(typ, plan)
	return actual.(*jsonNormalizationPlan)
}

func jsonContainsInterfaces(typ reflect.Type, seen map[reflect.Type]bool) bool {
	if cached, ok := jsonNormalizationPlans.Load(typ); ok {
		return cached.(*jsonNormalizationPlan).needed
	}
	if seen[typ] {
		return false
	}
	seen[typ] = true
	switch typ.Kind() {
	case reflect.Interface:
		return true
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return jsonContainsInterfaces(typ.Elem(), seen)
	case reflect.Struct:
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			if field.PkgPath == "" && field.Tag.Get("json") != "-" && jsonContainsInterfaces(field.Type, seen) {
				return true
			}
		}
	}
	// Cache only completed root searches, never a partial result inside a
	// recursive type graph: another branch may reach an interface via its parent.
	return false
}

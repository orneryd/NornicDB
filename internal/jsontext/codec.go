// Package jsontext preserves exact JSON string identities, including escaped
// unpaired UTF16 units, while delegating JSON grammar and typed binding to
// established parsers. Persisted and wire output is always standard JSON.
package jsontext

import (
	"bytes"
	"context"
	std "encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	alt "github.com/orneryd/go-json"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Marshal encodes standard JSON. Existing valid strings retain standard Go JSON
// escaping; newly admitted lone UTF16 units use their standard \uXXXX spelling.
func Marshal(value any) ([]byte, error) {
	return marshal(value, true)
}

func marshal(value any, escapeHTML bool) ([]byte, error) {
	options := []alt.EncodeOptionFunc{alt.DisableNormalizeUTF8()}
	if !escapeHTML {
		options = append(options, alt.DisableHTMLEscape())
	}
	raw, err := alt.MarshalWithOption(value, options...)
	switch failure := err.(type) {
	case *alt.UnsupportedTypeError:
		return nil, &std.UnsupportedTypeError{Type: failure.Type}
	case *alt.UnsupportedValueError:
		return nil, &std.UnsupportedValueError{Value: failure.Value, Str: failure.Str}
	case *alt.MarshalerError:
		return nil, &std.MarshalerError{Type: failure.Type, Err: failure.Err}
	}
	if err != nil {
		return nil, err
	}
	return projectJSONStrings(raw, wireJSON, false).bytes, nil
}

// Unmarshal binds a complete standard JSON value without normalizing escaped
// lone UTF16 units into replacement characters. Numeric typed binding is fixed
// by the destination; dynamic exact-number fields use NumberMap or NumberList.
type DecodeStage uint8

const (
	RawInput DecodeStage = iota
	ProjectedInput
)

func Unmarshal(raw []byte, out any, stages ...DecodeStage) error {
	stage := RawInput
	if len(stages) > 1 {
		return errors.New("jsontext: multiple input stages")
	}
	if len(stages) == 1 {
		stage = stages[0]
	}
	if stage != RawInput && stage != ProjectedInput {
		return errors.New("jsontext: invalid input stage")
	}
	if stage == RawInput && !std.Valid(raw) {
		var invalid any
		return std.Unmarshal(raw, &invalid)
	}
	projection := stringProjection{bytes: raw}
	if stage == RawInput {
		projection = projectJSONStrings(raw, rawJSON, true)
	}
	projected, edits := projection.bytes, projection.edits
	// Only validated/projected input reaches this opt-in; ordinary upstream
	// decoders retain standard UTF8 normalization.
	err := alt.UnmarshalWithOption(projected, out, alt.DecodePreserveInvalidUTF8())
	var source *cyphertext.Source
	if len(edits) > 0 {
		source = cyphertext.NewSource(context.Background(), string(raw), cyphertext.AcceptedSource).Rewrite(string(projected), edits)
	}
	switch failure := err.(type) {
	case *alt.UnmarshalTypeError:
		offset := int(failure.Offset)
		if source != nil {
			if original, ok := source.OriginalOffset(offset, false, cyphertext.SourceExtent); ok {
				offset = original
			}
		}
		var fieldPath []string
		located := false
		decoder := std.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		_, _ = readValue(decoder, raw, false, nil, func(path []string, start, end int) {
			if !located && end >= offset {
				offset = end
				fieldPath = append([]string(nil), path...)
				located = true
			}
		})

		return &std.UnmarshalTypeError{Value: failure.Value, Type: failure.Type, Offset: int64(offset), Struct: failure.Struct, Field: jsonFieldPath(reflect.TypeOf(out), fieldPath, failure.Field)}
	case *alt.InvalidUnmarshalError:
		return &std.InvalidUnmarshalError{Type: failure.Type}
	}

	return err
}

// Decoder uses the standard parser to frame one original JSON value. Decode
// preserves first-value stream behavior and leaves trailing-value policy to its
// caller. Buffered exposes bytes already read beyond the completed value.
type Decoder struct{ frame *std.Decoder }

func NewDecoder(reader io.Reader) *Decoder { return &Decoder{frame: std.NewDecoder(reader)} }
func (d *Decoder) Decode(out any) error {
	var raw std.RawMessage
	if err := d.frame.Decode(&raw); err != nil {
		return err
	}
	return Unmarshal(raw, out)
}
func (d *Decoder) Buffered() io.Reader { return d.frame.Buffered() }
func (d *Decoder) InputOffset() int64  { return d.frame.InputOffset() }

// DecodeValue reads one generic JSON tree with exact Number values. It does
// not consume a following value; callers requiring complete input check EOF.
func (d *Decoder) DecodeValue() (any, error) {
	var raw std.RawMessage
	if err := d.frame.Decode(&raw); err != nil {
		return nil, err
	}
	return decodeValue(raw, false)
}

// UnmarshalValue requires a complete generic JSON value and retains exact
// number lexemes rather than rounding integers through float64.
func UnmarshalValue(raw []byte) (any, error) {
	if !std.Valid(raw) {
		var invalid any
		return nil, std.Unmarshal(raw, &invalid)
	}
	return decodeValue(raw, false)
}

// RawMessage stores standard JSON even when invoked from transient projected
// typed binding. It never exposes internal raw WTF8 as persisted or wire JSON.
type RawMessage []byte

func (r *RawMessage) UnmarshalJSON(raw []byte) error {
	if r == nil {
		return errors.New("jsontext: nil RawMessage receiver")
	}
	*r = append((*r)[:0], projectJSONStrings(raw, opaqueJSON, false).bytes...)
	return nil
}
func (r RawMessage) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	if !std.Valid(r) {
		var invalid any
		return nil, std.Unmarshal(r, &invalid)
	}
	return r, nil
}

// NumberMap is a dynamic object whose numeric leaves retain exact JSON Numbers.
type NumberMap map[string]any

func (m *NumberMap) UnmarshalJSON(raw []byte) error {
	object, err := decodeNumberContainer[map[string]any](raw, "json: cannot unmarshal non-object into NumberMap")
	if err != nil {
		return err
	}
	if object == nil || *m == nil {
		*m = object
	} else {
		for key, value := range object {
			(*m)[key] = value
		}
	}
	return nil
}

// NumberList is a dynamic array whose numeric leaves retain exact JSON Numbers.
type NumberList []any

func (m *NumberList) UnmarshalJSON(raw []byte) error {
	list, err := decodeNumberContainer[[]any](raw, "json: cannot unmarshal non-array into NumberList")
	if err != nil {
		return err
	}
	*m = list
	return nil
}

// decodeNumberContainer owns exact-number decoding, null and container shape.
// Destination reuse remains the concrete container's assignment policy.
func decodeNumberContainer[T map[string]any | []any](raw []byte, mismatch string) (T, error) {
	var zero T
	value, err := decodeValue(raw, true)
	if err != nil {
		return zero, err
	}
	if value == nil {
		return zero, nil
	}
	typed, ok := value.(T)
	if !ok {
		return zero, errors.New(mismatch)
	}
	return typed, nil
}

func decodeValue(raw []byte, projected bool) (any, error) {
	decoder := std.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := readValue(decoder, raw, projected, nil, nil)
	if err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, errors.New("json: trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

// jsonFieldPath projects only error metadata, never binds or writes values.
// Supported typed binding reports a Go-field path; standard errors expose its
// declared JSON-tag path. Promoted fields use reflect's existing field lookup.
func jsonFieldPath(owner reflect.Type, path []string, unresolved string) string {
	result := make([]string, 0, len(path))
	for _, name := range path {
		for owner != nil && (owner.Kind() == reflect.Pointer || owner.Kind() == reflect.Slice || owner.Kind() == reflect.Array) {
			owner = owner.Elem()
		}
		if owner != nil && owner.Kind() == reflect.Map {
			owner = owner.Elem()
			continue
		}
		if owner == nil || owner.Kind() != reflect.Struct {
			return unresolved
		}
		var selected *reflect.StructField
		for _, field := range reflect.VisibleFields(owner) {
			tag := strings.Split(field.Tag.Get("json"), ",")[0]
			if field.PkgPath != "" || tag == "-" {
				continue
			}
			nested := field.Type
			for nested.Kind() == reflect.Pointer {
				nested = nested.Elem()
			}
			if field.Anonymous && tag == "" && nested.Kind() == reflect.Struct {
				continue
			}
			fieldName := tag
			if fieldName == "" {
				fieldName = field.Name
			}
			if fieldName == name {
				copy := field
				selected = &copy
				break
			}
			if selected == nil && strings.EqualFold(fieldName, name) {
				copy := field
				selected = &copy
			}
		}
		if selected == nil {
			return unresolved
		}
		tag := strings.Split(selected.Tag.Get("json"), ",")[0]
		if tag == "" {
			tag = selected.Name
		}
		result = append(result, tag)
		owner = selected.Type
	}
	return strings.Join(result, ".")
}

// Encoder writes one standard JSON value followed by a newline. It preserves
// first-write errors and supports the standard HTML escaping selection.
type Encoder struct {
	writer     io.Writer
	escapeHTML bool
}

func NewEncoder(writer io.Writer) *Encoder    { return &Encoder{writer: writer, escapeHTML: true} }
func (e *Encoder) SetEscapeHTML(enabled bool) { e.escapeHTML = enabled }
func (e *Encoder) Encode(value any) error {
	raw, err := marshal(value, e.escapeHTML)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	written, err := e.writer.Write(raw)
	if err == nil && written != len(raw) {
		return io.ErrShortWrite
	}
	return err
}

// TokenReader retains a caller-owned complete buffer for lossless ordered token
// traversal. Its embedded Decoder shares the same framing and typed binding.
type TokenReader struct {
	*Decoder
	raw []byte
}

func NewTokenReader(raw []byte) *TokenReader {
	return &TokenReader{Decoder: NewDecoder(bytes.NewReader(raw)), raw: raw}
}
func (d *TokenReader) Token() (any, error) { return readJSONToken(d.frame, d.raw, false) }
func (d *TokenReader) More() bool          { return d.frame.More() }

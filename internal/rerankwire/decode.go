// Package rerankwire owns validation-preserving acceptance of every built-in
// reranker response. It is module-internal because its exported identifiers
// exist only to share the acceptance owner between search and provider packages.
package rerankwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
)

var (
	errRequiredValue           = errors.New("rerank response is missing a required value")
	errInvalidEnvelope         = errors.New("rerank response does not contain exactly one JSON value")
	errMultipleRepresentations = errors.New("rerank response contains multiple ranking representations")
	jsonUnmarshalerType        = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	requiredMarkerType         = reflect.TypeOf((*requiredMarker)(nil)).Elem()
	bindingCache               sync.Map
)

var (
	jsonFence = []byte("```")
	jsonBOM   = []byte{0xef, 0xbb, 0xbf}
)

// Required is a JSON scalar or collection whose presence is part of the wire
// contract. The zero value means absent; JSON null is present but invalid.
type Required[T any] struct {
	value   T
	present bool
	null    bool
}

func (Required[T]) rerankRequired() {}

type requiredMarker interface {
	rerankRequired()
}

func (r *Required[T]) UnmarshalJSON(data []byte) error {
	r.present = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		r.null = true
		return nil
	}
	return json.Unmarshal(data, &r.value)
}

func requiredValue[T any](value T) Required[T] {
	return Required[T]{value: value, present: true}
}

func (r Required[T]) get() (T, error) {
	if !r.present || r.null {
		var zero T
		return zero, errRequiredValue
	}
	return r.value, nil
}

// IndexedResult is the shared presence-aware indexed JSON representation.
type IndexedResult struct {
	Index          Required[int]     `json:"index"`
	Score          Required[float64] `json:"score"`
	RelevanceScore Required[float64] `json:"relevance_score"`
	Document       string            `json:"document,omitempty"`
}

type scoreField uint8

const (
	scoreFieldScore scoreField = iota
	scoreFieldRelevance
)

type inputKind uint8

const (
	indexedInput inputKind = iota
	positionalInput
	orderedInput
	typedInput
)

type input struct {
	kind          inputKind
	indexed       []IndexedResult
	positional    []Required[float64]
	order         []Required[int]
	orderScores   []Required[float64]
	scoresPresent bool
	typed         []TypedResult
	field         scoreField
	population    int
}

// TypedResult represents a result whose index and score came from a typed
// provider API rather than JSON. Its constructor marks both fields present.
type TypedResult struct {
	Index    int
	Score    float64
	Document string
}

// AcceptedResult is the sole normalized output of built-in provider decoding.
type AcceptedResult struct {
	Index        int
	Score        float64
	ScorePresent bool
	Document     string
}

func indexed(values []IndexedResult, field scoreField) input {
	return input{kind: indexedInput, indexed: values, field: field}
}

func positional(values []Required[float64]) input {
	return input{kind: positionalInput, positional: values}
}

func ordered(order []Required[int], scores []Required[float64], scoresPresent bool, population int) input {
	return input{kind: orderedInput, order: order, orderScores: scores, scoresPresent: scoresPresent, population: population}
}

func typed(values []TypedResult) input {
	return input{kind: typedInput, typed: values}
}

// DecodeResponse is the only JSON decoding entry point exposed to provider
// adapters. All adapters therefore share decodeEnvelope's grammar and
// decodeBoundJSON's field-binding rules.
func DecodeResponse(reader io.Reader, value any) error {
	raw, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	valueJSON, err := decodeEnvelope(raw)
	if err != nil {
		return err
	}
	return decodeBoundJSON(valueJSON, value)
}

type acceptanceEntry struct {
	index         Required[int]
	score         Required[float64]
	requireScore  bool
	synthesizeFor int
	document      string
}

func accept(input input) ([]AcceptedResult, error) {
	switch input.kind {
	case indexedInput:
		entries := make([]acceptanceEntry, len(input.indexed))
		for index, item := range input.indexed {
			score := item.Score
			if input.field == scoreFieldRelevance {
				score = item.RelevanceScore
			}
			entries[index] = acceptanceEntry{index: item.Index, score: score, requireScore: true, document: item.Document}
		}
		return acceptEntries(entries)
	case positionalInput:
		entries := make([]acceptanceEntry, len(input.positional))
		for index, score := range input.positional {
			entries[index] = acceptanceEntry{index: requiredValue(index), score: score, requireScore: true}
		}
		return acceptEntries(entries)
	case orderedInput:
		if input.scoresPresent && len(input.orderScores) != len(input.order) {
			return nil, fmt.Errorf("rerank response score count does not match order")
		}
		entries := make([]acceptanceEntry, len(input.order))
		for index, candidateIndex := range input.order {
			entry := acceptanceEntry{index: candidateIndex, synthesizeFor: input.population}
			if input.scoresPresent {
				entry.score = input.orderScores[index]
				entry.requireScore = true
			}
			entries[index] = entry
		}
		return acceptEntries(entries)
	case typedInput:
		entries := make([]acceptanceEntry, len(input.typed))
		for index, item := range input.typed {
			entries[index] = acceptanceEntry{
				index: requiredValue(item.Index), score: requiredValue(item.Score),
				requireScore: true, document: item.Document,
			}
		}
		return acceptEntries(entries)
	default:
		return nil, errors.New("unknown rerank provider input")
	}
}

func acceptExclusive(inputs ...input) ([]AcceptedResult, error) {
	var selected []AcceptedResult
	for _, input := range inputs {
		accepted, err := accept(input)
		if err != nil {
			return nil, err
		}
		if len(accepted) == 0 {
			continue
		}
		if selected != nil {
			return nil, errMultipleRepresentations
		}
		selected = accepted
	}
	if selected == nil {
		return []AcceptedResult{}, nil
	}
	return selected, nil
}

// AcceptCrossEncoder validates every supported cross-encoder representation
// and rejects a response that supplies more than one non-empty ranking.
func AcceptCrossEncoder(results []IndexedResult, scores []Required[float64], rankings []IndexedResult, population int) ([]AcceptedResult, error) {
	accepted, err := acceptExclusive(
		indexed(results, scoreFieldRelevance),
		positional(scores),
		indexed(rankings, scoreFieldScore),
	)
	return validateAcceptedResults(accepted, population, err)
}

// AcceptVoyage validates both Voyage response spellings without preferring one.
func AcceptVoyage(data, results []IndexedResult, population int) ([]AcceptedResult, error) {
	accepted, err := acceptExclusive(
		indexed(data, scoreFieldRelevance),
		indexed(results, scoreFieldRelevance),
	)
	return validateAcceptedResults(accepted, population, err)
}

// AcceptTyped validates a provider result that arrived through a typed API.
func AcceptTyped(values []TypedResult, population int) ([]AcceptedResult, error) {
	accepted, err := accept(typed(values))
	return validateAcceptedResults(accepted, population, err)
}

// AcceptLLM owns envelope selection and presence-aware binding for model
// output. Every response must satisfy the one-envelope JSON grammar before a
// ranking can be interpreted.
func AcceptLLM(raw string, population int) ([]AcceptedResult, error) {
	accepted, err := acceptLLM(raw, population)
	return validateAcceptedResults(accepted, population, err)
}

func validateAcceptedResults(accepted []AcceptedResult, population int, err error) ([]AcceptedResult, error) {
	if err != nil {
		return nil, err
	}
	if len(accepted) == 0 || population <= 0 {
		return nil, errors.New("rerank response contains no valid results")
	}
	var localSeen [128]bool
	seen := localSeen[:]
	if population > len(localSeen) {
		seen = make([]bool, population)
	} else {
		seen = seen[:population]
	}
	for _, result := range accepted {
		if result.Index < 0 || result.Index >= population || seen[result.Index] || math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			return nil, errors.New("rerank response contains invalid results")
		}
		seen[result.Index] = true
	}
	return accepted, nil
}

func acceptEntries(entries []acceptanceEntry) ([]AcceptedResult, error) {
	accepted := make([]AcceptedResult, len(entries))
	for ordinal, entry := range entries {
		index, err := entry.index.get()
		if err != nil {
			return nil, err
		}
		result := AcceptedResult{Index: index, Document: entry.document}
		if entry.requireScore {
			result.Score, err = entry.score.get()
			if err != nil {
				return nil, err
			}
			result.ScorePresent = true
		} else {
			population := entry.synthesizeFor
			if population <= 0 {
				population = 1
			}
			result.Score = 1 - float64(ordinal)/float64(population)
		}
		accepted[ordinal] = result
	}
	return accepted, nil
}

type llmResponse struct {
	Ranked Required[[]IndexedResult]     `json:"ranked"`
	Order  Required[[]Required[int]]     `json:"order"`
	Scores Required[[]Required[float64]] `json:"scores"`
}

func acceptLLM(raw string, population int) ([]AcceptedResult, error) {
	value, err := decodeEnvelope([]byte(raw))
	if err != nil {
		return nil, err
	}
	switch value[0] {
	case '[':
		var order []Required[int]
		if err := decodeBoundJSON(value, &order); err != nil {
			return nil, err
		}
		return accept(ordered(order, nil, false, population))
	case '{':
		var response llmResponse
		if err := decodeBoundJSON(value, &response); err != nil {
			return nil, err
		}
		inputs := make([]input, 0, 2)
		if response.Ranked.present {
			ranked, err := response.Ranked.get()
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, indexed(ranked, scoreFieldScore))
		}
		if response.Order.present {
			order, err := response.Order.get()
			if err != nil {
				return nil, err
			}
			var scores []Required[float64]
			if response.Scores.present {
				scores, err = response.Scores.get()
				if err != nil {
					return nil, err
				}
			}
			inputs = append(inputs, ordered(order, scores, response.Scores.present, population))
		}
		return acceptExclusive(inputs...)
	default:
		return nil, errInvalidEnvelope
	}
}

// decodeEnvelope owns the complete provider envelope grammar. A response is
// either one exact JSON value, or one fenced block whose body is one exact JSON
// value. A fence may have one optional language tag and ordinary prose outside
// it; additional JSON values, fences, malformed suffixes and empty bodies fail.
func decodeEnvelope(raw []byte) (json.RawMessage, error) {
	input := trimEnvelopePrefix(raw)
	if len(input) == 0 {
		return nil, errInvalidEnvelope
	}
	// Prefer an exact bare value before looking for fences. Backticks may be
	// ordinary characters inside a JSON string and must not become syntax.
	if json.Valid(input) {
		return bytes.TrimSpace(input), nil
	}
	if fenceAt := bytes.Index(input, jsonFence); fenceAt >= 0 {
		return decodeFencedEnvelope(input, fenceAt)
	}
	return decodeExactJSON(input)
}

func decodeFencedEnvelope(input []byte, fenceAt int) (json.RawMessage, error) {
	prefix := input[:fenceAt]
	afterFence := input[fenceAt+len(jsonFence):]
	lineEnd := bytes.IndexByte(afterFence, '\n')
	if lineEnd < 0 || !validFenceTag(bytes.TrimSpace(afterFence[:lineEnd])) {
		return nil, errInvalidEnvelope
	}
	body := trimEnvelopePrefix(afterFence[lineEnd+1:])
	value, offset, err := decodeLeadingJSONValue(body)
	if err != nil {
		return nil, err
	}
	afterValue := trimJSONSpace(body[offset:])
	if !bytes.HasPrefix(afterValue, jsonFence) {
		return nil, errInvalidEnvelope
	}
	suffix := afterValue[len(jsonFence):]
	if err := validateEnvelopeProse(prefix); err != nil {
		return nil, err
	}
	if err := validateEnvelopeProse(suffix); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeExactJSON(input []byte) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidEnvelope, err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errInvalidEnvelope
		}
		return nil, fmt.Errorf("%w: %v", errInvalidEnvelope, err)
	}
	return bytes.TrimSpace(value), nil
}

func decodeLeadingJSONValue(input []byte) (json.RawMessage, int, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	var value json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		return nil, 0, fmt.Errorf("%w: %v", errInvalidEnvelope, err)
	}
	return bytes.TrimSpace(value), int(decoder.InputOffset()), nil
}

func trimEnvelopePrefix(input []byte) []byte {
	input = bytes.TrimSpace(input)
	if bytes.HasPrefix(input, jsonBOM) {
		input = bytes.TrimSpace(input[len(jsonBOM):])
	}
	return input
}

func trimJSONSpace(input []byte) []byte {
	return bytes.TrimLeft(input, " \t\r\n")
}

func validFenceTag(tag []byte) bool {
	if len(tag) == 0 {
		return true
	}
	if len(bytes.Fields(tag)) != 1 {
		return false
	}
	return !bytes.ContainsAny(tag, "`{}[]\"'")
}

func validateEnvelopeProse(prose []byte) error {
	prose = bytes.TrimSpace(prose)
	if len(prose) == 0 {
		return nil
	}
	if bytes.Contains(prose, jsonFence) || bytes.Contains(prose, jsonBOM) {
		return errInvalidEnvelope
	}
	// Outside-fence prose cannot begin a JSON scalar or trailing comma at a
	// lexical boundary. That makes truncated second values fail closed without
	// confusing digits or hyphens embedded in ordinary words for values.
	for index, value := range prose {
		if value == '{' || value == '[' || value == '}' || value == ']' {
			return errInvalidEnvelope
		}
		if index > 0 && isEnvelopeWordByte(prose[index-1]) {
			continue
		}
		if value == '"' || value == '-' || value == ',' || value >= '0' && value <= '9' {
			return errInvalidEnvelope
		}
		if !possibleJSONScalarStart(value) {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(prose[index:]))
		var candidate json.RawMessage
		if decoder.Decode(&candidate) == nil {
			return errInvalidEnvelope
		}
		rest := strings.TrimSpace(string(prose[index:]))
		for _, literal := range []string{"true", "false", "null"} {
			if rest != "" && strings.HasPrefix(literal, rest) {
				return errInvalidEnvelope
			}
		}
	}
	return nil
}

func possibleJSONScalarStart(value byte) bool {
	return value == '"' || value == '-' || value >= '0' && value <= '9' || value == 't' || value == 'f' || value == 'n'
}

func isEnvelopeWordByte(value byte) bool {
	return value == '_' || value >= '0' && value <= '9' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

type jsonBinding struct {
	fields  []jsonBindingField
	element *jsonBinding
}

type jsonBindingField struct {
	name    string
	binding *jsonBinding
}

type jsonBindingCacheEntry struct {
	binding *jsonBinding
}

func decodeBoundJSON(raw json.RawMessage, value any) error {
	valueType := reflect.TypeOf(value)
	if valueType == nil || valueType.Kind() != reflect.Pointer || reflect.ValueOf(value).IsNil() {
		return errors.New("rerank response target must be a non-nil pointer")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := validateBoundJSONValue(decoder, bindingForType(valueType.Elem())); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errInvalidEnvelope
		}
		return err
	}
	return json.Unmarshal(raw, value)
}

func bindingForType(valueType reflect.Type) *jsonBinding {
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if cached, ok := bindingCache.Load(valueType); ok {
		return cached.(jsonBindingCacheEntry).binding
	}
	binding := buildBindingForType(valueType)
	bindingCache.Store(valueType, jsonBindingCacheEntry{binding: binding})
	return binding
}

func buildBindingForType(valueType reflect.Type) *jsonBinding {
	if valueType.Implements(requiredMarkerType) {
		valueField, ok := valueType.FieldByName("value")
		if !ok {
			return nil
		}
		return bindingForType(valueField.Type)
	}
	if valueType.Implements(jsonUnmarshalerType) || reflect.PointerTo(valueType).Implements(jsonUnmarshalerType) {
		return nil
	}
	switch valueType.Kind() {
	case reflect.Array, reflect.Slice:
		return &jsonBinding{element: bindingForType(valueType.Elem())}
	case reflect.Struct:
		binding := &jsonBinding{}
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name := field.Name
			if tag := field.Tag.Get("json"); tag != "" {
				name = strings.Split(tag, ",")[0]
				if name == "-" {
					continue
				}
				if name == "" {
					name = field.Name
				}
			}
			binding.fields = append(binding.fields, jsonBindingField{name: name, binding: bindingForType(field.Type)})
		}
		return binding
	default:
		return nil
	}
}

func (binding *jsonBinding) resolveField(name string) (int, *jsonBinding, bool) {
	if binding == nil {
		return 0, nil, false
	}
	for index := range binding.fields {
		if binding.fields[index].name == name {
			return index, binding.fields[index].binding, true
		}
	}
	for index := range binding.fields {
		if strings.EqualFold(binding.fields[index].name, name) {
			return index, binding.fields[index].binding, true
		}
	}
	return 0, nil, false
}

func validateBoundJSONValue(decoder *json.Decoder, binding *jsonBinding) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, structured := token.(json.Delim)
	if !structured {
		return nil
	}
	switch delimiter {
	case '{':
		var seenBits uint64
		var seenLarge map[int]struct{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("rerank response contains a non-string JSON field")
			}
			fieldIndex, fieldBinding, recognized := binding.resolveField(key)
			if recognized {
				if fieldIndex < 64 {
					bit := uint64(1) << fieldIndex
					if seenBits&bit != 0 {
						return fmt.Errorf("rerank response contains duplicate bound JSON field %q", key)
					}
					seenBits |= bit
				} else {
					if seenLarge == nil {
						seenLarge = make(map[int]struct{})
					}
					if _, duplicate := seenLarge[fieldIndex]; duplicate {
						return fmt.Errorf("rerank response contains duplicate bound JSON field %q", key)
					}
					seenLarge[fieldIndex] = struct{}{}
				}
			}
			if err := validateBoundJSONValue(decoder, fieldBinding); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		var element *jsonBinding
		if binding != nil {
			element = binding.element
		}
		for decoder.More() {
			if err := validateBoundJSONValue(decoder, element); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("rerank response contains an invalid JSON delimiter")
	}
}

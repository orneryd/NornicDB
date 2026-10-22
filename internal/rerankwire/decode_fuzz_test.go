package rerankwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"unicode"
)

var llmOutsideEnvelopeSeeds = []string{
	"2 0",
	"2 0 null",
	"2 0 true",
	"2 0]",
	"2 0,",
	`2 0 "tail"`,
	"2 0 1e",
	"ranks 2 then 0",
	"-1 2",
	"0 0",
	`{"order":[2,0],"scores":[0.9,0.8]} candidates ranked`,
}

func FuzzRerankEnvelopeContract(f *testing.F) {
	object := `{"order":[2,0],"scores":[0.9,0.8]}`
	fence := func(value string) string { return "```json\n" + value + "\n```" }
	for _, seed := range []string{
		fence(object) + "\nCandidates ranked.",
		fence(object) + "\n" + fence(object),
		"[2,0]", object,
		fence(`{"order":[2,0],"scores":[0.9,0.8],"reason":"nested ` + "```json ```" + `"}`),
		`{"order":[0,2],"order":[2,0],"scores":[0.9,0.8]}`,
		`{"order":[0,2],"ord\u0065r":[2,0],"scores":[0.9,0.8]}`,
		fence(""), " \n\t" + object + "\r\n ", "\ufeff" + object, object + "\ufeff",
		object + " null", object + " true", object + " 42", object + ` "ignored"`,
		"[2,0]]", object + "}", object + ",",
		fence(object) + "\n```json\nnull\n```",
		fence(object) + "\n" + fence(""),
		fence(object) + "\n```json\ntru",
		`{"order":[-1],"ORDER":[2,0],"scores":[0.9,0.8]}`,
		`{"ranked":[{"index":-1,"INDEX":2,"score":0.9},{"index":0,"score":0.8}]}`,
		`{"ranked":[{"index":2,"score":0.1,"SCORE":0.9},{"index":0,"score":0.8}]}`,
		`{"results":[{"index":-1,"relevance_score":0.1}],"RESULTS":[{"index":2,"relevance_score":0.9}]}`,
		`{"results":[{"index":-1,"INDEX":2,"relevance_score":0.9}]}`,
		`{"data":[{"index":-1,"relevance_score":0.1}],"DATA":[{"index":2,"relevance_score":0.9}]}`,
		`{"data":[{"index":-1,"INDEX":2,"relevance_score":0.9}]}`,
		`{"ORDER":[2,0],"SCORES":[0.9,0.8]}`,
		`{"order":[2,0],"scores":[0.9,0.8],"unknown":1,"UNKNOWN":2}`,
		"```\n[2,0]\n```", "```JSON\n[2,0]\n```", "```javascript\n[2,0]\n```",
		"Ranking follows.\n```json\n" + object + "\n```\nDone.",
		"{\"ORDER\":[0],\"```\":[]}", "```\n[0]\v```",
		`[null]`, `{"ranked":[{"index":null,"score":0.9}]}`,
		`{"ranked":[{"index":0,"score":null}]}`,
		`{"data":[{"index":0,"relevance_score":0.9,"score":"bad"}]}`,
		`{"data":[{"index":0,"relevance_score":0.9,"document":0}]}`,
		`{"results":null,"rankings":[{"index":0,"score":0.9}]}`,
		`{"data":null,"results":[{"index":0,"relevance_score":0.9}]}`,
		`{"ranked":[{"index":0,"score":0.9}],"scores":null}`,
		fence(object) + ` "`, fence(object) + ` -`, fence(object) + ` 1e`, fence(object) + ` ,`,
	} {
		f.Add([]byte(seed))
	}
	for _, seed := range llmOutsideEnvelopeSeeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		const population = 3
		checks := []struct {
			name string
			got  func() ([]AcceptedResult, error)
			want func() ([]AcceptedResult, bool)
		}{
			{"llm", func() ([]AcceptedResult, error) { return AcceptLLM(string(input), population) }, func() ([]AcceptedResult, bool) { return referenceAcceptLLM(input, population) }},
			{"cross_encoder", func() ([]AcceptedResult, error) { return fuzzAcceptCrossEncoder(input, population) }, func() ([]AcceptedResult, bool) { return referenceAcceptCrossEncoder(input, population) }},
			{"voyage", func() ([]AcceptedResult, error) { return fuzzAcceptVoyage(input, population) }, func() ([]AcceptedResult, bool) { return referenceAcceptVoyage(input, population) }},
		}
		for _, check := range checks {
			got, err := check.got()
			gotOK := err == nil
			want, wantOK := check.want()
			if gotOK != wantOK {
				t.Fatalf("%s acceptance mismatch input=%q got=%+v err=%v want=%+v wantOK=%v", check.name, input, got, err, want, wantOK)
			}
			if gotOK && !referenceAcceptedEqual(got, want) {
				t.Fatalf("%s canonical ranking mismatch input=%q got=%+v want=%+v", check.name, input, got, want)
			}
		}
	})
}

func fuzzAcceptCrossEncoder(input []byte, population int) ([]AcceptedResult, error) {
	var wire struct {
		Results  []IndexedResult     `json:"results"`
		Scores   []Required[float64] `json:"scores"`
		Rankings []IndexedResult     `json:"rankings"`
	}
	if err := DecodeResponse(bytes.NewReader(input), &wire); err != nil {
		return nil, err
	}
	return AcceptCrossEncoder(wire.Results, wire.Scores, wire.Rankings, population)
}

func fuzzAcceptVoyage(input []byte, population int) ([]AcceptedResult, error) {
	var wire struct {
		Data    []IndexedResult `json:"data"`
		Results []IndexedResult `json:"results"`
		Model   string          `json:"model"`
		Usage   struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := DecodeResponse(bytes.NewReader(input), &wire); err != nil {
		return nil, err
	}
	return AcceptVoyage(wire.Data, wire.Results, population)
}

func referenceAcceptLLM(input []byte, population int) ([]AcceptedResult, bool) {
	raw, ok := referenceEnvelope(input)
	if !ok {
		return nil, false
	}
	results, ok := referenceStructuredRanking(raw, population)
	return results, ok && referenceValidRanking(results, population)
}

func referenceAcceptCrossEncoder(input []byte, population int) ([]AcceptedResult, bool) {
	members, ok := referenceProviderMembers(input)
	if !ok {
		return nil, false
	}
	results, resultsPresent, ok := referenceIndexedField(members, "results", "relevance_score")
	if !ok {
		return nil, false
	}
	scores, scoresPresent, ok := referenceScoreField(members, "scores")
	if !ok {
		return nil, false
	}
	rankings, rankingsPresent, ok := referenceIndexedField(members, "rankings", "score")
	if !ok {
		return nil, false
	}
	return referenceSelectRepresentation(population,
		referenceRepresentation{results, resultsPresent},
		referenceRepresentation{scores, scoresPresent},
		referenceRepresentation{rankings, rankingsPresent},
	)
}

func referenceAcceptVoyage(input []byte, population int) ([]AcceptedResult, bool) {
	members, ok := referenceProviderMembers(input)
	if !ok || !referenceValidVoyageMetadata(members) {
		return nil, false
	}
	data, dataPresent, ok := referenceIndexedField(members, "data", "relevance_score")
	if !ok {
		return nil, false
	}
	results, resultsPresent, ok := referenceIndexedField(members, "results", "relevance_score")
	if !ok {
		return nil, false
	}
	return referenceSelectRepresentation(population,
		referenceRepresentation{data, dataPresent},
		referenceRepresentation{results, resultsPresent},
	)
}

func referenceValidVoyageMetadata(members []referenceMember) bool {
	model, present, err := referenceField(members, "model")
	if err != nil {
		return false
	}
	if present {
		var value string
		if json.Unmarshal(model, &value) != nil {
			return false
		}
	}
	usage, present, err := referenceField(members, "usage")
	if err != nil || !present || bytes.Equal(bytes.TrimSpace(usage), []byte("null")) {
		return err == nil
	}
	usageMembers, err := referenceObjectMembers(usage)
	if err != nil {
		return false
	}
	tokens, present, err := referenceField(usageMembers, "total_tokens")
	if err != nil {
		return false
	}
	if present {
		var value int
		if json.Unmarshal(tokens, &value) != nil {
			return false
		}
	}
	return true
}

type referenceRepresentation struct {
	results []AcceptedResult
	present bool
}

func referenceSelectRepresentation(population int, representations ...referenceRepresentation) ([]AcceptedResult, bool) {
	var selected []AcceptedResult
	for _, representation := range representations {
		if !representation.present || len(representation.results) == 0 {
			continue
		}
		if selected != nil {
			return nil, false
		}
		selected = representation.results
	}
	return selected, selected != nil && referenceValidRanking(selected, population)
}

func referenceProviderMembers(input []byte) ([]referenceMember, bool) {
	raw, ok := referenceEnvelope(input)
	if !ok || len(raw) == 0 || raw[0] != '{' {
		return nil, false
	}
	members, err := referenceObjectMembers(raw)
	return members, err == nil
}

func referenceIndexedField(members []referenceMember, name, scoreName string) ([]AcceptedResult, bool, bool) {
	raw, present, err := referenceField(members, name)
	if err != nil {
		return nil, false, false
	}
	if !present {
		return nil, false, true
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, true
	}
	results, ok := referenceIndexedItems(raw, scoreName)
	if !ok {
		return nil, false, false
	}
	return results, true, true
}

func referenceIndexedItems(raw json.RawMessage, scoreName string) ([]AcceptedResult, bool) {
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	results := make([]AcceptedResult, len(items))
	for ordinal, item := range items {
		itemMembers, err := referenceObjectMembers(item)
		if err != nil || referenceKnownDuplicate(itemMembers, "index", "score", "relevance_score", "document") {
			return nil, false
		}
		indexRaw, indexPresent, err := referenceField(itemMembers, "index")
		if err != nil {
			return nil, false
		}
		scoreRaw, scorePresent, err := referenceField(itemMembers, scoreName)
		if err != nil || !indexPresent || !scorePresent {
			return nil, false
		}
		index, indexOK := referenceRequiredInt(indexRaw)
		score, scoreOK := referenceRequiredFloat(scoreRaw)
		if !indexOK || !scoreOK || !referenceOptionalIndexedFields(itemMembers, scoreName) {
			return nil, false
		}
		results[ordinal] = AcceptedResult{Index: index, Score: score, ScorePresent: true}
	}
	return results, true
}

func referenceOptionalIndexedFields(members []referenceMember, selectedScore string) bool {
	for _, name := range []string{"score", "relevance_score"} {
		if name == selectedScore {
			continue
		}
		raw, present, err := referenceField(members, name)
		if err != nil {
			return false
		}
		if present && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			var value float64
			if json.Unmarshal(raw, &value) != nil {
				return false
			}
		}
	}
	document, present, err := referenceField(members, "document")
	if err != nil || !present || bytes.Equal(bytes.TrimSpace(document), []byte("null")) {
		return err == nil
	}
	var value string
	return json.Unmarshal(document, &value) == nil
}

func referenceRequiredInt(raw json.RawMessage) (int, bool) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var value int
	return value, json.Unmarshal(raw, &value) == nil
}

func referenceRequiredFloat(raw json.RawMessage) (float64, bool) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var value float64
	return value, json.Unmarshal(raw, &value) == nil
}

func referenceIntSlice(raw json.RawMessage) ([]int, bool) {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	result := make([]int, len(values))
	for index, rawValue := range values {
		value, ok := referenceRequiredInt(rawValue)
		if !ok {
			return nil, false
		}
		result[index] = value
	}
	return result, true
}

func referenceFloatSlice(raw json.RawMessage) ([]float64, bool) {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	result := make([]float64, len(values))
	for index, rawValue := range values {
		value, ok := referenceRequiredFloat(rawValue)
		if !ok {
			return nil, false
		}
		result[index] = value
	}
	return result, true
}

func referenceScoreField(members []referenceMember, name string) ([]AcceptedResult, bool, bool) {
	raw, present, err := referenceField(members, name)
	if err != nil {
		return nil, false, false
	}
	if !present {
		return nil, false, true
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, true
	}
	values, ok := referenceFloatSlice(raw)
	if !ok {
		return nil, false, false
	}
	results := make([]AcceptedResult, len(values))
	for index, score := range values {
		results[index] = AcceptedResult{Index: index, Score: score, ScorePresent: true}
	}
	return results, true, true
}

func referenceEnvelope(input []byte) (json.RawMessage, bool) {
	input = referenceTrimPrefix(input)
	if len(input) == 0 {
		return nil, false
	}
	if json.Valid(input) {
		return append(json.RawMessage(nil), bytes.TrimSpace(input)...), true
	}
	opening := bytes.Index(input, []byte("```"))
	if opening < 0 {
		return nil, false
	}
	afterOpening := input[opening+3:]
	lineEnd := bytes.IndexByte(afterOpening, '\n')
	if lineEnd < 0 || !referenceFenceTag(bytes.TrimSpace(afterOpening[:lineEnd])) {
		return nil, false
	}
	prefix := input[:opening]
	body := afterOpening[lineEnd+1:]
	validClosings := 0
	var accepted json.RawMessage
	for searchAt := 0; ; {
		closing := bytes.Index(body[searchAt:], []byte("```"))
		if closing < 0 {
			break
		}
		closing += searchAt
		candidate := referenceFencedValue(body[:closing])
		suffix := body[closing+3:]
		if json.Valid(candidate) && referenceProse(prefix) && referenceProse(suffix) {
			validClosings++
			accepted = append(accepted[:0], bytes.TrimSpace(candidate)...)
		}
		searchAt = closing + 3
	}
	return accepted, validClosings == 1
}

func referenceFencedValue(input []byte) []byte {
	input = bytes.TrimLeftFunc(input, unicode.IsSpace)
	if bytes.HasPrefix(input, []byte{0xef, 0xbb, 0xbf}) {
		input = bytes.TrimLeftFunc(input[3:], unicode.IsSpace)
	}
	return bytes.TrimRight(input, " \t\r\n")
}

func referenceTrimPrefix(input []byte) []byte {
	input = bytes.TrimSpace(input)
	if bytes.HasPrefix(input, []byte{0xef, 0xbb, 0xbf}) {
		input = bytes.TrimSpace(input[3:])
	}
	return input
}

func referenceFenceTag(tag []byte) bool {
	return len(tag) == 0 || len(bytes.Fields(tag)) == 1 && !bytes.ContainsAny(tag, "`{}[]\"'")
}

func referenceProse(prose []byte) bool {
	prose = bytes.TrimSpace(prose)
	if len(prose) == 0 {
		return true
	}
	if bytes.Contains(prose, []byte("```")) || bytes.Contains(prose, []byte{0xef, 0xbb, 0xbf}) {
		return false
	}
	for index, value := range prose {
		if value == '{' || value == '[' || value == '}' || value == ']' {
			return false
		}
		if index > 0 && referenceWordByte(prose[index-1]) {
			continue
		}
		if value == '"' || value == '-' || value == ',' || value >= '0' && value <= '9' {
			return false
		}
		if !referenceJSONScalarStart(value) {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(prose[index:]))
		var candidate json.RawMessage
		if decoder.Decode(&candidate) == nil {
			return false
		}
		rest := strings.TrimSpace(string(prose[index:]))
		for _, literal := range []string{"true", "false", "null"} {
			if rest != "" && strings.HasPrefix(literal, rest) {
				return false
			}
		}
	}
	return true
}

func referenceJSONScalarStart(value byte) bool {
	return value == '"' || value == '-' || value >= '0' && value <= '9' || value == 't' || value == 'f' || value == 'n'
}

func referenceWordByte(value byte) bool {
	return value == '_' || value >= '0' && value <= '9' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

type referenceMember struct {
	name  string
	value json.RawMessage
}

func referenceObjectMembers(raw json.RawMessage) ([]referenceMember, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("not an object")
	}
	var members []referenceMember
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		members = append(members, referenceMember{name: key.(string), value: value})
	}
	_, err = decoder.Token()
	return members, err
}

func referenceField(members []referenceMember, name string) (json.RawMessage, bool, error) {
	var value json.RawMessage
	found := false
	for _, member := range members {
		if !strings.EqualFold(member.name, name) {
			continue
		}
		if found {
			return nil, false, errors.New("duplicate bound field")
		}
		found = true
		value = member.value
	}
	return value, found, nil
}

func referenceStructuredRanking(raw json.RawMessage, population int) ([]AcceptedResult, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, false
	}
	if raw[0] == '[' {
		order, ok := referenceIntSlice(raw)
		if !ok {
			return nil, false
		}
		return referenceOrdered(order, nil, population), len(order) > 0
	}
	if raw[0] != '{' {
		return nil, false
	}
	members, err := referenceObjectMembers(raw)
	if err != nil {
		return nil, false
	}
	rankedRaw, rankedPresent, err := referenceField(members, "ranked")
	if err != nil {
		return nil, false
	}
	orderRaw, orderPresent, err := referenceField(members, "order")
	if err != nil {
		return nil, false
	}
	scoresRaw, scoresPresent, err := referenceField(members, "scores")
	if err != nil {
		return nil, false
	}
	var ranked []AcceptedResult
	if rankedPresent {
		var ok bool
		ranked, ok = referenceIndexedItems(rankedRaw, "score")
		if !ok {
			return nil, false
		}
	}
	var order []int
	if orderPresent {
		var ok bool
		order, ok = referenceIntSlice(orderRaw)
		if !ok {
			return nil, false
		}
	}
	var scores []float64
	if scoresPresent {
		if bytes.Equal(bytes.TrimSpace(scoresRaw), []byte("null")) {
			if orderPresent {
				return nil, false
			}
		} else if parsed, ok := referenceFloatSlice(scoresRaw); !ok {
			return nil, false
		} else {
			scores = parsed
		}
	}
	if len(ranked) > 0 && len(order) > 0 {
		return nil, false
	}
	if len(ranked) > 0 {
		return ranked, true
	}
	if len(order) == 0 || scoresPresent && len(scores) != len(order) {
		return nil, false
	}
	return referenceOrdered(order, scores, population), true
}

func referenceKnownDuplicate(members []referenceMember, names ...string) bool {
	for _, name := range names {
		if _, _, err := referenceField(members, name); err != nil {
			return true
		}
	}
	return false
}

func referenceOrdered(order []int, scores []float64, population int) []AcceptedResult {
	results := make([]AcceptedResult, len(order))
	for ordinal, index := range order {
		result := AcceptedResult{Index: index, Score: 1 - float64(ordinal)/float64(max(population, 1))}
		if scores != nil {
			result.Score = scores[ordinal]
			result.ScorePresent = true
		}
		results[ordinal] = result
	}
	return results
}

func referenceValidRanking(results []AcceptedResult, population int) bool {
	if len(results) == 0 {
		return false
	}
	seen := make(map[int]struct{}, len(results))
	for _, result := range results {
		if result.Index < 0 || result.Index >= population || math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			return false
		}
		if _, duplicate := seen[result.Index]; duplicate {
			return false
		}
		seen[result.Index] = struct{}{}
	}
	return true
}

func referenceAcceptedEqual(left, right []AcceptedResult) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Index != right[index].Index || left[index].Score != right[index].Score || left[index].ScorePresent != right[index].ScorePresent {
			return false
		}
	}
	return true
}

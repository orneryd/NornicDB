package jsontext

import (
	"bytes"
	std "encoding/json"
	"errors"
)

// readValue uses Token's structural and numeric authority. InputOffset selects
// the original validated string lexeme, avoiding Token's lossy UTF16 string.
type tokenVisitor func([]string, int, int)

func readValue(decoder *std.Decoder, raw []byte, projected bool, path []string, visit tokenVisitor) (any, error) {
	before := decoder.InputOffset()
	token, err := readJSONToken(decoder, raw, projected)
	if err != nil {
		return nil, err
	}
	if visit != nil {
		visit(path, int(before), int(decoder.InputOffset()))
	}
	switch value := token.(type) {
	case string:
		return value, nil
	case std.Delim:
		switch value {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				key, err := readValue(decoder, raw, projected, nil, nil)
				if err != nil {
					return nil, err
				}
				item, err := readValue(decoder, raw, projected, append(path, key.(string)), visit)
				if err != nil {
					return nil, err
				}
				name, ok := key.(string)
				if !ok {
					return nil, errors.New("jsontext: validated object key is not a string")
				}
				object[name] = item
			}
			_, err := decoder.Token()
			return object, err
		case '[':
			list := []any{}
			for decoder.More() {
				item, err := readValue(decoder, raw, projected, path, visit)
				if err != nil {
					return nil, err
				}
				list = append(list, item)
			}
			_, err := decoder.Token()
			return list, err
		}
		return nil, errors.New("jsontext: unexpected closing delimiter")
	}
	return token, nil
}

// readJSONToken is the one lossless string-token owner for both generic trees
// and ordered remote value traversal. Structure still belongs to Decoder.Token.
func readJSONToken(decoder *std.Decoder, raw []byte, projected bool) (any, error) {
	before := decoder.InputOffset()
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if _, ok := token.(string); !ok {
		return token, nil
	}
	segment := raw[before:decoder.InputOffset()]
	start := bytes.IndexByte(segment, '"')
	if start < 0 {
		return nil, errors.New("jsontext: validated string lexeme missing")
	}
	policy := rawJSON
	if projected {
		policy = projectedJSON
	}
	lexeme := projectJSONStrings(segment[start:], policy, false).bytes
	var text string
	if err := Unmarshal(lexeme, &text, ProjectedInput); err != nil {
		return nil, err
	}
	return text, nil
}

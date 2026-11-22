package text

import "fmt"

// CapitalizeChecked preserves the Java substring failure on an empty input.
// The legacy Go convenience function Capitalize keeps its existing signature.
func CapitalizeChecked(text string) (string, error) {
	if text == "" {
		return "", fmt.Errorf("apoc.text.capitalize: empty string")
	}
	return Capitalize(text), nil
}

// DecapitalizeNullable carries the registered function's null result without
// converting null into the empty string. Non-string arguments remain errors.
func DecapitalizeNullable(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("apoc.text.decapitalize: expected string")
	}
	return Decapitalize(text), nil
}

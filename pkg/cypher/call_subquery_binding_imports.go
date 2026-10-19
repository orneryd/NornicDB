package cypher

import "strings"

// callSubqueryBindingImports is shared by the typed scope and execution frame.
// Scoped stars import the whole outer frame. Legacy importing WITH selects its
// own projection; otherwise the existing implicit-import extension imports only
// referenced outer variables, rather than treating RETURN * as an import.
func callSubqueryBindingImports[T any](body string, outer map[string]T, scoped bool, imports []string) ([]string, bool, error) {
	if scoped && imports != nil {
		return imports, false, nil
	}
	legacy := false
	if !scoped {
		var err error
		_, legacy, err = unscopedCallImports(body, func(name string) bool { _, exists := outer[name]; return exists && !strings.HasPrefix(name, "$") }, false)
		if err != nil {
			return nil, false, err
		}
	}
	if !legacy {
		for name := range outer {
			if !strings.HasPrefix(name, "$") && (scoped || isIdentifierReferenced(body, name)) {
				imports = append(imports, name)
			}
		}
	}
	return imports, legacy, nil
}

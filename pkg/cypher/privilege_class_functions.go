package cypher

import cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"

var builtinFunctionPrivileges = func() map[string]PrivilegeClass {
	classes := make(map[string]PrivilegeClass, len(cypherFunctionCatalog))
	for _, function := range cypherFunctionCatalog {
		if !function.privilege.Valid() {
			panic("function " + function.name + ": explicit privilege class required")
		}
		key := lowerASCII(function.name)
		if prior, exists := classes[key]; exists && prior != function.privilege {
			panic("function " + function.name + ": conflicting overload privileges")
		}
		classes[key] = function.privilege
	}
	return classes
}()

func functionPrivilegeClass(name string) (PrivilegeClass, bool) {
	class, found := builtinFunctionPrivileges[lowerASCII(name)]
	if !found {
		class, found = cypherfn.Class(name)
	}
	if PluginFunctionLookup != nil {
		if handler, installed := PluginFunctionLookup(name); installed {
			contract, declared := handler.(PluginFunctionContract)
			if !declared || !contract.Privilege.Valid() {
				return 0, true
			}
			if !found || class == PrivilegeNone {
				return contract.Privilege, true
			}
			if contract.Privilege == PrivilegeNone {
				return class, true
			}
			return class | contract.Privilege, true
		}
	}
	return class, found
}

// containsPrivilegedFunctionCall marks expression rows that must retain the
// executing context. It uses the same contract as authorization: graph and
// catalog readers cannot fall back to an anonymous row-evaluation context.
func containsPrivilegedFunctionCall(expression string) bool {
	needsContext := false
	_ = forEachFunctionCall(expression, func(name, _ string) error {
		if class, found := functionPrivilegeClass(name); found && class != PrivilegeNone {
			needsContext = true
		}
		return nil
	})
	return needsContext
}

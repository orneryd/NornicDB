package cypher

// functionArity is how many arguments a built-in function takes: the fewest
// any of its signatures requires and the most any allows; maximum -1 is any
// number.
type functionArity struct {
	minimum int
	maximum int
}

// functionArityOverrides are the counts Neo4j 5.26 accepts where they differ
// from its signatures: coalesce takes any number of arguments, trim also takes
// the forms its FROM syntax stands for, trim(specification, input) and
// trim(specification, characters, input), and timestamp takes an ignored one.
var functionArityOverrides = map[string]functionArity{
	"coalesce":  {minimum: 1, maximum: -1},
	"trim":      {minimum: 1, maximum: 3},
	"timestamp": {minimum: 0, maximum: 1},
}

// functionSyntaxForms are catalog entries written with their own syntax, not
// a list of arguments: reduce(acc = init, x IN list | expression), the list
// predicates any / all / none / single (x IN list WHERE …) and exists.
var functionSyntaxForms = map[string]bool{
	"reduce": true, "all": true, "any": true, "none": true, "single": true, "exists": true,
}

// functionArities are the argument counts of the built-in functions, keyed by
// lower-case name, from their catalog signatures (cypherFunctionCatalog): an
// argument with a default value is optional (date(), date.truncate(unit)).
// NornicDB's own extensions, whose entries don't list their arguments, aren't
// checked, nor are functionSyntaxForms. Each count was checked against Neo4j
// 5.26 calling every function with zero to five arguments.
var functionArities, maxFunctionArityNameLength = buildFunctionArities()

func buildFunctionArities() (map[string]functionArity, int) {
	arities := make(map[string]functionArity)
	unlisted := make(map[string]bool)
	longest := 0
	for _, function := range cypherFunctionCatalog {
		name := lowerASCII(function.name)
		if function.arguments == nil {
			unlisted[name] = true
			continue
		}
		required := 0
		for _, argument := range function.arguments {
			if argument.Default == "" && !argument.Optional {
				required++
			}
		}
		arity, seen := arities[name]
		if !seen || required < arity.minimum {
			arity.minimum = required
		}
		if !seen || len(function.arguments) > arity.maximum {
			arity.maximum = len(function.arguments)
		}
		arities[name] = arity
	}
	for name := range unlisted {
		delete(arities, name)
	}
	for name := range functionSyntaxForms {
		delete(arities, name)
	}
	for name, arity := range functionArityOverrides {
		arities[name] = arity
	}
	for name := range arities {
		longest = max(longest, len(name))
	}
	return arities, longest
}

// lookupFunctionArity finds a built-in function's argument count by name,
// case-insensitively, without allocating: every name followed by "(" in a
// statement passes through it.
func lookupFunctionArity(name string) (functionArity, bool) {
	var buffer [64]byte
	if len(name) > maxFunctionArityNameLength || len(name) > len(buffer) {
		return functionArity{}, false
	}
	for i := 0; i < len(name); i++ {
		buffer[i] = asciiLowerByte(name[i])
	}
	arity, ok := functionArities[string(buffer[:len(name)])]
	return arity, ok
}

// checkFunctionArity is Neo4j's compile-time error for a call to function
// with count arguments outside its arity (functionParameterCountError).
func checkFunctionArity(function string, arity functionArity, count int) error {
	if count < arity.minimum {
		return functionParameterCountError(function, false)
	}
	if arity.maximum >= 0 && count > arity.maximum {
		return functionParameterCountError(function, true)
	}
	return nil
}

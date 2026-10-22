package listinputs

// FromPairsOutcome is pinned to Neo4j 5.26.30 + APOC 5.26.30, reference
// i20261021234616-53352-a3aa (52 Bolt cases, both transaction modes).
// Go-only scalar aliases have the same value contract as their wire forms.
// Stored forms use the reference's static property type, independently of
// the more permissive storage API used to install typed test fixtures.
type FromPairsOutcome struct {
	JSON string
	Code string
}

func FromPairsExpectation(input Input, form string) FromPairsOutcome {
	const syntax = "Neo.ClientError.Statement.SyntaxError"
	const typeError = "Neo.ClientError.Statement.TypeError"
	if form == "stored" || form == "typed-property" || form == "written-property" {
		return FromPairsOutcome{Code: syntax}
	}
	switch input.Name {
	case "string", "string-array", "named-strings":
		return FromPairsOutcome{Code: syntax}
	case "maps":
		if form == "literal" {
			return FromPairsOutcome{Code: syntax}
		}
		return FromPairsOutcome{JSON: `{"{rank=1}":null,"{rank=3}":null}`}
	case "nested", "nested-array":
		return FromPairsOutcome{JSON: `{"1":null,"3":null}`}
	case "nested-string":
		return FromPairsOutcome{JSON: `{"a":null,"c":null}`}
	case "mixed":
		return FromPairsOutcome{Code: "Neo.ClientError.Procedure.ProcedureCallFailed"}
	case "any", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "bool", "array", "named-ints":
		return FromPairsOutcome{Code: typeError}
	default:
		panic("unclassified fromPairs fixture: " + input.Name)
	}
}

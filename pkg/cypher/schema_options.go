// Schema OPTIONS payload ownership.
//
// Every CREATE INDEX and CREATE CONSTRAINT form ends with the same optional
// tail: OPTIONS and exactly one payload, either a map literal or a parameter
// reference. Neo4j reads OPTIONS $options exactly as it reads OPTIONS {...},
// and there it applies the map's configuration, so this file owns that payload
// end to end: schemaOptionsTail admits its syntax, schemaOptionMap binds the
// map value it designates when the statement runs, and schemaVectorOptions
// reads the vector configuration a bound map carries.
//
// Syntax and binding are separate on purpose. Admission is structural and runs
// wherever the statement is parsed; binding reads expressions and parameters,
// so it runs only at the execution boundary, after the whole statement's
// syntax is verified. EXPLAIN validates syntax and compiles metadata without
// binding, so no option expression is evaluated and no parameter is read.
package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// schemaOptions is an admitted OPTIONS payload as the statement wrote it. It
// carries no value: a map literal is evaluated and a parameter is read at the
// execution boundary, never while syntax is admitted.
type schemaOptions struct {
	// literal is the map literal's source text, braces included.
	literal string
	// parameter is the parameter's name, unquoted ($`a b` has the name a b).
	parameter string
}

// schemaOptionsTail admits the OPTIONS tail shared by every CREATE INDEX and
// CREATE CONSTRAINT form: the one owner of tail admission that both statement
// families used to repeat. The payload is one map literal or one parameter
// reference — the ANTLR grammar's (OPTIONS (mapLit | parameter))? and this
// owner agree — so the keyword alone, or the keyword followed by anything
// other than that payload, is a syntax error. The native parser previously
// checked only that the tail started with the keyword, so a bare OPTIONS built
// the index, and kept the constraint, that the ANTLR grammar rejects (NOR-300).
//
// What follows the payload ends the statement; it is not OPTIONS syntax. The
// shared trivia reader skips its whitespace and comments, and one optional
// statement delimiter may precede that trivia, as statement framing elsewhere.
// Anything else left over is real trailing syntax and stays rejected.
func schemaOptionsTail(tail string) (schemaOptions, error) {
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return schemaOptions{}, nil
	}
	if !startsWithKeywordFold(tail, "OPTIONS") {
		return schemaOptions{}, localizedError(localization.CypherSchemaInvalidTrailingSyntax(), nil)
	}
	keywordEnd, ok := keywordSpanAt(tail, 0, "OPTIONS")
	if !ok {
		return schemaOptions{}, localizedError(localization.CypherSchemaInvalidClause("OPTIONS"), nil)
	}
	payload := tail[queryGapEnd(tail, keywordEnd):]
	if strings.HasPrefix(payload, "{") {
		_, rest, matched := extractDelimitedSection(payload, '{', '}')
		if matched && schemaOptionsEndsStatement(rest) {
			return schemaOptions{literal: strings.TrimSpace(payload[:len(payload)-len(rest)])}, nil
		}
		return schemaOptions{}, localizedError(localization.CypherSchemaInvalidClause("OPTIONS"), nil)
	}
	if name, end, ok := schemaOptionsParameter(payload); ok && schemaOptionsEndsStatement(payload[end:]) {
		return schemaOptions{parameter: name}, nil
	}
	return schemaOptions{}, localizedError(localization.CypherSchemaInvalidClause("OPTIONS"), nil)
}

// schemaOptionsEndsStatement reports whether rest is only the statement's
// trailing framing: trivia, and at most one delimiter before closing trivia.
func schemaOptionsEndsStatement(rest string) bool {
	end := queryGapEnd(rest, 0)
	if end < len(rest) && rest[end] == ';' {
		end = queryGapEnd(rest, end+1)
	}
	return end == len(rest)
}

// schemaOptionsParameter reads text as exactly one parameter reference and
// returns its name and the reference's end. The canonical parameter scanner
// owns reference admission, so a quoted name ($`a b`) reads here as it reads
// everywhere else, while a dynamic label ($( … )) is not a reference.
func schemaOptionsParameter(text string) (name string, end int, ok bool) {
	scanParameterReferences(text, func(dollar, start, referenceEnd int) {
		if ok || dollar != 0 {
			return
		}
		name, end, ok = cyphertext.SymbolicNameValue(text[start:referenceEnd]), referenceEnd, true
	})
	return name, end, ok
}

// schemaOptionMap binds the admitted payload to the map value it designates:
// the one typed binding owner for both forms. A literal map is evaluated by
// the statement's expression evaluator, so a pure function call such as
// toLower(...) means there what it means anywhere else, and a parameter reads
// the value bound to its name in this statement. subject names the schema
// object in the error a non-map payload produces.
func (e *StorageExecutor) schemaOptionMap(ctx context.Context, options schemaOptions, subject string) (map[string]interface{}, error) {
	switch {
	case options.literal != "":
		return e.evaluateMapLiteralFromValues(ctx, options.literal, nil)
	case options.parameter != "":
		value, supplied := getParamsFromContext(ctx)[options.parameter]
		if !supplied {
			return nil, parameterMissingError([]string{options.parameter})
		}
		return schemaOptionMapValue(value, subject)
	default:
		return nil, nil
	}
}

// schemaOptionMapValue checks that a bound option payload is a map. Neo4j
// classifies a null or wrong-typed map as InvalidArguments — not as a type
// error — whether or not the statement would have created the object (IF NOT
// EXISTS included), because the options are read before the schema change is
// admitted.
func schemaOptionMapValue(value interface{}, subject string) (map[string]interface{}, error) {
	switch typed := value.(type) {
	case nil:
		return nil, schemaOptionsNotMapError(subject, nil)
	case map[string]interface{}:
		return typed, nil
	}
	return nil, schemaOptionsNotMapError(subject, value)
}

// schemaOptionsNotMapError is Neo4j's error for schema options that are not a
// map value.
func schemaOptionsNotMapError(subject string, value interface{}) error {
	return newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidArguments",
		fmt.Sprintf("Could not create %s with options '%s'. Expected a map value.", subject, neo4jValueRepr(value)))
}

// schemaVectorOptions reads the vector configuration a bound options map
// carries: indexConfig's vector.dimensions (an integer) and
// vector.similarity_function (a string). A declared value of the wrong type is
// an error rather than a silent fall back to the default, so the index applies
// the configuration the statement declared or refuses the statement. An absent
// option keeps the default, and so does an empty similarity function, as it
// always has.
func schemaVectorOptions(values map[string]interface{}, defaultDimensions int, defaultSimilarity string) (int, string, error) {
	dimensions, similarity := defaultDimensions, defaultSimilarity
	config := map[string]interface{}(nil)
	if raw, supplied := values["indexConfig"]; supplied {
		typed, ok := raw.(map[string]interface{})
		if !ok {
			return 0, "", schemaVectorOptionError("indexConfig", "a map", raw)
		}
		config = typed
	}
	if raw, supplied := config["vector.dimensions"]; supplied {
		value, ok := cypherIntegerValue(raw)
		if !ok {
			return 0, "", schemaVectorOptionError("vector.dimensions", "an integer", raw)
		}
		dimensions = int(value)
	}
	if raw, supplied := config["vector.similarity_function"]; supplied {
		value, ok := raw.(string)
		if !ok {
			return 0, "", schemaVectorOptionError("vector.similarity_function", "a string", raw)
		}
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			similarity = trimmed
		}
	}
	return dimensions, similarity, nil
}

// schemaVectorOptionError is the error for one vector index configuration
// option whose declared value has the wrong type.
func schemaVectorOptionError(option, expected string, value interface{}) error {
	return newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidArguments",
		fmt.Sprintf("Index configuration option %q must be %s, but was %s.", option, expected, neo4jValueRepr(value)))
}

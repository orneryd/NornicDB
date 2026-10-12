package cypher

import (
	"regexp"
	"strconv"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Cypher 25 functions (Neo4j 2025.11 to 2026.05): the string.* namespace and
// cardinality() (coll.* are in coll_functions.go). They follow Neo4j 2026.09: null in, null out;
// an argument of a type the signature excludes is the "Type mismatch"
// SyntaxError (cypherfn.TypeMismatchError); an index outside the list is
// "Function argument to 'f()' is out of range" (ArgumentError). Lists compare
// with Cypher's equality (1 is 1.0) and order with its orderability (null
// last), as ORDER BY does. They are NornicDB extensions under Cypher 5.
func init() {
	cypherfn.Register("string.indexof", fnStringIndexOf)
	cypherfn.Register("string.join", fnStringJoin)
	cypherfn.Register("string.regexreplace", fnStringRegexReplace)
	cypherfn.Register("cardinality", fnCardinality)
	cypherfn.Register("property_exists", fnPropertyExists)
}

// stringArguments evaluates a string function's arguments: null is true when
// any is null; each must otherwise be a string.
func stringArguments(ctx cypherfn.Context, function string, args []string, count int) ([]string, bool, error) {
	if len(args) != count {
		return nil, false, argumentCountError(function, strconv.Itoa(count), len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, false, err
	}
	texts := make([]string, count)
	for index, value := range values {
		if value == nil {
			return nil, true, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, false, &cypherfn.TypeMismatchError{Function: function, Expected: "String", Value: value}
		}
		texts[index] = text
	}
	return texts, false, nil
}

// fnStringIndexOf is string.indexOf(text, search): the character offset of
// search's first occurrence in text, or -1.
func fnStringIndexOf(ctx cypherfn.Context, args []string) (interface{}, error) {
	texts, null, err := stringArguments(ctx, "string.indexOf", args, 2)
	if err != nil || null {
		return nil, err
	}
	at := strings.Index(texts[0], texts[1])
	if at < 0 {
		return int64(-1), nil
	}
	return int64(len([]rune(texts[0][:at]))), nil
}

func fnStringJoin(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("string.join", "2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	list, isList := cypherListValue(values[0])
	if !isList {
		return nil, &cypherfn.TypeMismatchError{Function: "string.join", Expected: "List<String>", Value: values[0]}
	}
	separator, ok := values[1].(string)
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "string.join", Expected: "String", Value: values[1]}
	}
	parts := make([]string, 0, len(list))
	for _, item := range list {
		if item == nil {
			continue
		}
		text, ok := item.(string)
		if !ok {
			return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
				localization.CypherCoreStringJoinElementType(neo4jValueRepr(item)))
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, separator), nil
}

// fnStringRegexReplace is string.regexReplace(text, regex, replacement):
// every match of regex replaced, with Java's replacement syntax ($1, ${name},
// \ escapes) as Neo4j uses. A replacement Java rejects is an error only when
// regex matches, as in Neo4j.
func fnStringRegexReplace(ctx cypherfn.Context, args []string) (interface{}, error) {
	texts, null, err := stringArguments(ctx, "string.regexReplace", args, 3)
	if err != nil || null {
		return nil, err
	}
	re, err := GetCachedRegex(texts[1])
	if err != nil {
		return nil, newSemanticError("Neo.ClientError.Statement.SemanticError", "InvalidRegex", "Invalid Regex: "+err.Error())
	}
	if !re.MatchString(texts[0]) {
		return texts[0], nil
	}
	template, err := javaReplacementTemplate(texts[2], re)
	if err != nil {
		return nil, err
	}
	return re.ReplaceAllString(texts[0], template), nil
}

// javaReplacementTemplate turns a Java Matcher replacement into a Go
// Regexp.Expand template: $n takes as many digits as name an existing group
// (Java's rule), ${name} a named group, a backslash escapes the next
// character, and a literal $ is written $$. A reference Java rejects (no
// such group, a $ naming nothing, a trailing backslash) is Neo4j's
// ExecutionFailed with Java's message.
func javaReplacementTemplate(replacement string, re *regexp.Regexp) (string, error) {
	fail := func(message localization.Message) (string, error) {
		return "", localizedStatusError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidArgument", message)
	}
	var template strings.Builder
	for index := 0; index < len(replacement); index++ {
		character := replacement[index]
		switch {
		case character == '\\':
			if index++; index >= len(replacement) {
				return fail(localization.CypherCoreRegexReplacementEscapeMissing())
			}
			if replacement[index] == '$' {
				template.WriteString("$$")
			} else {
				template.WriteByte(replacement[index])
			}
		case character != '$':
			template.WriteByte(character)
		case index+1 >= len(replacement):
			return fail(localization.CypherCoreRegexReplacementGroupIndexMissing())
		case replacement[index+1] == '{':
			end := strings.IndexByte(replacement[index:], '}')
			if end < 0 {
				return fail(localization.CypherCoreRegexReplacementNamedGroupUnterminated())
			}
			name := replacement[index+2 : index+end]
			if re.SubexpIndex(name) < 0 {
				return fail(localization.CypherCoreRegexReplacementNoNamedGroup(name))
			}
			template.WriteString("${" + name + "}")
			index += end
		case replacement[index+1] >= '0' && replacement[index+1] <= '9':
			group := int(replacement[index+1] - '0')
			if group > re.NumSubexp() {
				return fail(localization.CypherCoreRegexReplacementNoGroup(group))
			}
			end := index + 2
			for end < len(replacement) && replacement[end] >= '0' && replacement[end] <= '9' {
				next := group*10 + int(replacement[end]-'0')
				if next > re.NumSubexp() {
					break
				}
				group = next
				end++
			}
			template.WriteString("${" + strconv.Itoa(group) + "}")
			index = end - 1
		default:
			return fail(localization.CypherCoreRegexReplacementIllegalGroupReference())
		}
	}
	return template.String(), nil
}

// fnCardinality is cardinality(value): a list's or map's number of items, a
// path's number of nodes and relationships.
func fnCardinality(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("cardinality", "1", len(args))
	}
	value, err := ctx.Eval(args[0])
	if err != nil || value == nil {
		return nil, err
	}
	if elements, isPath := cypherPathElements(value); isPath {
		return int64(len(elements)), nil
	}
	if list, isList := cypherListValue(value); isList {
		return int64(len(list)), nil
	}
	if object, isMap := value.(map[string]interface{}); isMap {
		return int64(len(object)), nil
	}
	return nil, &cypherfn.TypeMismatchError{Function: "cardinality", Expected: "Map, Path or List<T>", Value: value}
}

// fnPropertyExists is property_exists(element, key), Cypher 25's
// PROPERTY_EXISTS(n, key), whose bare key canonicalizeFunctionAliases writes
// as a string: whether a node or relationship has a (non-null) property of
// that key. A null element gives null; anything else is a type mismatch.
func fnPropertyExists(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("property_exists", "2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	key, isString := values[1].(string)
	if !isString {
		return nil, &cypherfn.TypeMismatchError{Function: "property_exists", Expected: "String", Value: values[1]}
	}
	var properties map[string]interface{}
	switch element := values[0].(type) {
	case *storage.Node:
		properties = element.Properties
	case *storage.Edge:
		properties = element.Properties
	default:
		return nil, &cypherfn.TypeMismatchError{Function: "property_exists", Expected: "Node or Relationship", Value: values[0]}
	}
	return properties[key] != nil, nil
}

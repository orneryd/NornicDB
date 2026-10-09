package cypher

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Cypher 25 functions (Neo4j 2025.11 to 2026.05): the coll.* and string.*
// namespaces and cardinality(). They follow Neo4j 2026.09: null in, null out;
// an argument of a type the signature excludes is the "Type mismatch"
// SyntaxError (cypherfn.TypeMismatchError); an index outside the list is
// "Function argument to 'f()' is out of range" (ArgumentError). Lists compare
// with Cypher's equality (1 is 1.0) and order with its orderability (null
// last), as ORDER BY does. They are NornicDB extensions under Cypher 5.
func init() {
	cypherfn.Register("coll.distinct", fnCollDistinct)
	cypherfn.Register("coll.flatten", fnCollFlatten)
	cypherfn.Register("coll.indexof", fnCollIndexOf)
	cypherfn.Register("coll.insert", fnCollInsert)
	cypherfn.Register("coll.max", fnCollExtreme("coll.max", 1))
	cypherfn.Register("coll.min", fnCollExtreme("coll.min", -1))
	cypherfn.Register("coll.remove", fnCollRemove)
	cypherfn.Register("coll.sort", fnCollSort)
	cypherfn.Register("string.indexof", fnStringIndexOf)
	cypherfn.Register("string.join", fnStringJoin)
	cypherfn.Register("string.regexreplace", fnStringRegexReplace)
	cypherfn.Register("cardinality", fnCardinality)
}

// listArguments evaluates a function's arguments and returns its first as a
// list. null is true when any argument is null, which makes the result null.
func listArguments(ctx cypherfn.Context, function string, args []string, count int) (values []interface{}, list []interface{}, null bool, err error) {
	if len(args) != count {
		return nil, nil, false, argumentCountError(function, strconv.Itoa(count), len(args))
	}
	values, err = evalArgs(ctx, args)
	if err != nil {
		return nil, nil, false, err
	}
	for _, value := range values {
		if value == nil {
			return values, nil, true, nil
		}
	}
	list, isList := cypherListValue(values[0])
	if !isList {
		return nil, nil, false, &cypherfn.TypeMismatchError{Function: function, Expected: "List<T>", Value: values[0]}
	}
	return values, list, false, nil
}

// indexArgument is a function's integer index argument, within [0, limit].
func indexArgument(function string, value interface{}, limit int) (int, error) {
	index, ok := cypherIntegerValue(value)
	if !ok {
		return 0, &cypherfn.TypeMismatchError{Function: function, Expected: "Integer", Value: value}
	}
	if index < 0 || index > int64(limit) {
		return 0, functionArgumentOutOfRange(function)
	}
	return int(index), nil
}

func functionArgumentOutOfRange(function string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherCoreFunctionArgumentOutOfRange(function))
}

func fnCollDistinct(ctx cypherfn.Context, args []string) (interface{}, error) {
	_, list, null, err := listArguments(ctx, "coll.distinct", args, 1)
	if err != nil || null {
		return nil, err
	}
	seen := make(map[string]struct{}, len(list))
	result := make([]interface{}, 0, len(list))
	for _, item := range list {
		key := cypherEquivalenceKey(item)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result, nil
}

func fnCollFlatten(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, argumentCountError("coll.flatten", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	list, isList := cypherListValue(values[0])
	if !isList {
		return nil, &cypherfn.TypeMismatchError{Function: "coll.flatten", Expected: "List<T>", Value: values[0]}
	}
	depth := int64(1)
	if len(values) == 2 {
		var ok bool
		if depth, ok = cypherIntegerValue(values[1]); !ok {
			return nil, &cypherfn.TypeMismatchError{Function: "coll.flatten", Expected: "Integer", Value: values[1]}
		}
		if depth < 0 {
			return nil, functionArgumentOutOfRange("coll.flatten")
		}
	}
	return flattenListDepth(list, depth), nil
}

// flattenListDepth replaces each list in list by its items, depth levels
// deep; a negative depth flattens every level (apoc.coll.flatten).
func flattenListDepth(list []interface{}, depth int64) []interface{} {
	result := make([]interface{}, 0, len(list))
	for _, item := range list {
		if inner, isList := cypherListValue(item); isList && depth != 0 {
			result = append(result, flattenListDepth(inner, depth-1)...)
			continue
		}
		result = append(result, item)
	}
	return result
}

func fnCollIndexOf(ctx cypherfn.Context, args []string) (interface{}, error) {
	values, list, null, err := listArguments(ctx, "coll.indexOf", args, 2)
	if err != nil || null {
		return nil, err
	}
	for index, item := range list {
		if cypherEquality(item, values[1]) == true {
			return int64(index), nil
		}
	}
	return int64(-1), nil
}

func fnCollInsert(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 3 {
		return nil, argumentCountError("coll.insert", "3", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	list, isList := cypherListValue(values[0])
	if !isList {
		return nil, &cypherfn.TypeMismatchError{Function: "coll.insert", Expected: "List<T>", Value: values[0]}
	}
	index, err := indexArgument("coll.insert", values[1], len(list))
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(list)+1)
	result = append(result, list[:index]...)
	result = append(result, values[2])
	return append(result, list[index:]...), nil
}

func fnCollRemove(ctx cypherfn.Context, args []string) (interface{}, error) {
	values, list, null, err := listArguments(ctx, "coll.remove", args, 2)
	if err != nil || null {
		return nil, err
	}
	index, err := indexArgument("coll.remove", values[1], len(list)-1)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(list)-1)
	result = append(result, list[:index]...)
	return append(result, list[index+1:]...), nil
}

// fnCollExtreme is coll.max (direction 1) or coll.min (-1): the largest or
// smallest item by orderability, in which null is the largest.
func fnCollExtreme(function string, direction int) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		_, list, null, err := listArguments(ctx, function, args, 1)
		if err != nil || null || len(list) == 0 {
			return nil, err
		}
		best := list[0]
		for _, item := range list[1:] {
			if compareValuesForSort(item, best)*direction > 0 {
				best = item
			}
		}
		return best, nil
	}
}

func fnCollSort(ctx cypherfn.Context, args []string) (interface{}, error) {
	_, list, null, err := listArguments(ctx, "coll.sort", args, 1)
	if err != nil || null {
		return nil, err
	}
	result := append(make([]interface{}, 0, len(list)), list...)
	sort.SliceStable(result, func(left, right int) bool { return compareValuesForSort(result[left], result[right]) < 0 })
	return result, nil
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
// \ escapes) as Neo4j uses.
func fnStringRegexReplace(ctx cypherfn.Context, args []string) (interface{}, error) {
	texts, null, err := stringArguments(ctx, "string.regexReplace", args, 3)
	if err != nil || null {
		return nil, err
	}
	re, err := GetCachedRegex(texts[1])
	if err != nil {
		return nil, newSemanticError("Neo.ClientError.Statement.SemanticError", "InvalidRegex", "Invalid Regex: "+err.Error())
	}
	return re.ReplaceAllString(texts[0], javaReplacementTemplate(texts[2], re)), nil
}

// javaReplacementTemplate turns a Java Matcher replacement into a Go
// Regexp.Expand template: $n takes as many digits as name an existing group
// (Java's rule), ${name} a named group, a backslash escapes the next
// character, and a literal $ is written $$.
func javaReplacementTemplate(replacement string, re *regexp.Regexp) string {
	var template strings.Builder
	for index := 0; index < len(replacement); index++ {
		character := replacement[index]
		switch {
		case character == '\\' && index+1 < len(replacement):
			index++
			if replacement[index] == '$' {
				template.WriteString("$$")
			} else {
				template.WriteByte(replacement[index])
			}
		case character == '$' && index+1 < len(replacement) && replacement[index+1] == '{':
			end := strings.IndexByte(replacement[index:], '}')
			if end < 0 {
				template.WriteString("$$")
				continue
			}
			template.WriteString(replacement[index : index+end+1])
			index += end
		case character == '$' && index+1 < len(replacement) && replacement[index+1] >= '0' && replacement[index+1] <= '9':
			group := int(replacement[index+1] - '0')
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
		case character == '$':
			template.WriteString("$$")
		default:
			template.WriteByte(character)
		}
	}
	return template.String()
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

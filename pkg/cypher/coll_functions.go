package cypher

import (
	"sort"

	"github.com/orneryd/nornicdb/pkg/localization"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// The coll.* list functions of Cypher 25 (#907), as Neo4j 2026.09 computes
// them. They order values as ORDER BY does (compareValuesForSort: null after
// everything, NaN after every number) and tell values apart as DISTINCT does
// (cypherEquivalenceKey). A null list, index or depth gives null. Neo4j 5.26
// and Cypher 5 statements don't have them; NornicDB offers them in every
// statement.

func init() {
	cypherfn.Register("coll.distinct", fnCollDistinct)
	cypherfn.Register("coll.flatten", fnCollFlatten)
	cypherfn.Register("coll.indexof", fnCollIndexOf)
	cypherfn.Register("coll.insert", fnCollInsert)
	cypherfn.Register("coll.max", fnCollExtreme("coll.max", 1))
	cypherfn.Register("coll.min", fnCollExtreme("coll.min", -1))
	cypherfn.Register("coll.remove", fnCollRemove)
	cypherfn.Register("coll.sort", fnCollSort)
}

// collArguments evaluates a coll.* call's arguments: the list first, then
// the rest. null is true when the list or a later argument the function
// needs is null.
func collArguments(ctx cypherfn.Context, function string, args []string, minimum, maximum int) (list []interface{}, rest []interface{}, null bool, err error) {
	if len(args) < minimum || len(args) > maximum {
		return nil, nil, false, argumentCountError(function, collArity(minimum, maximum), len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, nil, false, err
	}
	if values[0] == nil {
		return nil, nil, true, nil
	}
	items, isList := cypherListValue(values[0])
	if !isList {
		return nil, nil, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid(function, "a List", neo4jValueRepr(values[0])))
	}
	return items, values[1:], false, nil
}

func collArity(minimum, maximum int) string {
	if minimum == maximum {
		return string(rune('0' + minimum))
	}
	return string(rune('0'+minimum)) + " or " + string(rune('0'+maximum))
}

// collIndex reads an INTEGER argument; null is true for null.
func collIndex(function string, value interface{}) (index int64, null bool, err error) {
	if value == nil {
		return 0, true, nil
	}
	index, ok := cypherIntegerValue(value)
	if !ok {
		return 0, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid(function, "an Integer", neo4jValueRepr(value)))
	}
	return index, false, nil
}

// functionArgumentOutOfRange is Neo4j's ArgumentError for an index, depth
// or limit outside what the function takes (coll.insert, replace, …).
func functionArgumentOutOfRange(function string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
		localization.CypherCoreFunctionArgumentOutOfRange(function))
}

// fnCollDistinct is coll.distinct(list): the first of every set of
// equivalent values, in order (1 and 1.0 are one value; two NaNs two).
func fnCollDistinct(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, _, null, err := collArguments(ctx, "coll.distinct", args, 1, 1)
	if err != nil || null {
		return nil, err
	}
	seen := make(map[string]struct{}, len(list))
	out := make([]interface{}, 0, len(list))
	for _, item := range list {
		key := cypherEquivalenceKey(item)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}

// fnCollFlatten is coll.flatten(list [, depth]): the list with nested lists
// spliced in, depth levels deep (1 by default; 0 keeps the list). A negative
// depth is out of range.
func fnCollFlatten(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, rest, null, err := collArguments(ctx, "coll.flatten", args, 1, 2)
	if err != nil || null {
		return nil, err
	}
	depth := int64(1)
	if len(rest) == 1 {
		var nullDepth bool
		if depth, nullDepth, err = collIndex("coll.flatten", rest[0]); err != nil || nullDepth {
			return nil, err
		}
		if depth < 0 {
			return nil, functionArgumentOutOfRange("coll.flatten")
		}
	}
	return collFlatten(make([]interface{}, 0, len(list)), list, depth), nil
}

func collFlatten(out, list []interface{}, depth int64) []interface{} {
	for _, item := range list {
		if nested, isList := cypherListValue(item); isList && depth > 0 {
			out = collFlatten(out, nested, depth-1)
			continue
		}
		out = append(out, item)
	}
	return out
}

// fnCollIndexOf is coll.indexOf(list, value): the index of the first item
// equal to value, -1 when none is (an item whose equality is unknown, such
// as null, doesn't match); null for a null value.
func fnCollIndexOf(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, rest, null, err := collArguments(ctx, "coll.indexOf", args, 2, 2)
	if err != nil || null || rest[0] == nil {
		return nil, err
	}
	for index, item := range list {
		if equal, known := cypherEquality(item, rest[0]).(bool); known && equal {
			return int64(index), nil
		}
	}
	return int64(-1), nil
}

// fnCollInsert is coll.insert(list, index, value): value inserted before
// list[index]; index runs from 0 to the list's length.
func fnCollInsert(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, rest, null, err := collArguments(ctx, "coll.insert", args, 3, 3)
	if err != nil || null {
		return nil, err
	}
	index, nullIndex, err := collIndex("coll.insert", rest[0])
	if err != nil || nullIndex {
		return nil, err
	}
	if index < 0 || index > int64(len(list)) {
		return nil, functionArgumentOutOfRange("coll.insert")
	}
	out := make([]interface{}, 0, len(list)+1)
	out = append(out, list[:index]...)
	out = append(out, rest[1])
	return append(out, list[index:]...), nil
}

// fnCollRemove is coll.remove(list, index): the list without list[index];
// index runs from 0 to the list's length less one, and an empty list is
// its own error.
func fnCollRemove(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, rest, null, err := collArguments(ctx, "coll.remove", args, 2, 2)
	if err != nil || null {
		return nil, err
	}
	index, nullIndex, err := collIndex("coll.remove", rest[0])
	if err != nil || nullIndex {
		return nil, err
	}
	if len(list) == 0 {
		return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
			localization.CypherCoreFunctionListArgumentEmpty("coll.remove", "list"))
	}
	if index < 0 || index >= int64(len(list)) {
		return nil, functionArgumentOutOfRange("coll.remove")
	}
	out := make([]interface{}, 0, len(list)-1)
	out = append(out, list[:index]...)
	return append(out, list[index+1:]...), nil
}

// fnCollExtreme is coll.max (sign 1) or coll.min (sign -1): the greatest or
// least item in ORDER BY order, the first of equal ones; null for an empty
// list. null sorts after everything, so a list holding null has max null,
// and min of anything else.
func fnCollExtreme(function string, sign int) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		list, _, null, err := collArguments(ctx, function, args, 1, 1)
		if err != nil || null || len(list) == 0 {
			return nil, err
		}
		best := list[0]
		for _, item := range list[1:] {
			if compareValuesForSort(item, best)*sign > 0 {
				best = item
			}
		}
		return best, nil
	}
}

// fnCollSort is coll.sort(list): the list in ORDER BY order, equal items
// kept in their order.
func fnCollSort(ctx cypherfn.Context, args []string) (interface{}, error) {
	list, _, null, err := collArguments(ctx, "coll.sort", args, 1, 1)
	if err != nil || null {
		return nil, err
	}
	out := append([]interface{}(nil), list...)
	sort.SliceStable(out, func(i, j int) bool { return compareValuesForSort(out[i], out[j]) < 0 })
	if out == nil {
		out = []interface{}{}
	}
	return out, nil
}

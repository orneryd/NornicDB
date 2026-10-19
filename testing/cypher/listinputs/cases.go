// Package listinputs supplies public-query representation tests shared by the
// executor, Bolt and HTTP tests. Expectations come from equivalent literal
// queries, never from the list-conversion implementation under test.
package listinputs

import "strings"

type Input struct {
	Name    string
	Value   interface{}
	Literal string
}

type namedStrings []string
type namedInts []int64

func Inputs() []Input {
	return []Input{
		{"any", []interface{}{int64(3), int64(1), int64(3)}, "[3,1,3]"},
		{"string", []string{"c", "a", "c"}, "['c','a','c']"},
		{"int", []int{3, 1, 3}, "[3,1,3]"},
		{"int8", []int8{3, 1, 3}, "[3,1,3]"},
		{"int16", []int16{3, 1, 3}, "[3,1,3]"},
		{"int32", []int32{3, 1, 3}, "[3,1,3]"},
		{"int64", []int64{3, 1, 3}, "[3,1,3]"},
		{"uint", []uint{3, 1, 3}, "[3,1,3]"},
		{"uint8", []uint8{3, 1, 3}, "[3,1,3]"},
		{"uint16", []uint16{3, 1, 3}, "[3,1,3]"},
		{"uint32", []uint32{3, 1, 3}, "[3,1,3]"},
		{"uint64", []uint64{3, 1, 3}, "[3,1,3]"},
		{"float32", []float32{3, 1, 3}, "[3.0,1.0,3.0]"},
		{"float64", []float64{3.5, 1.5, 3.5}, "[3.5,1.5,3.5]"},
		{"bool", []bool{true, false, true}, "[true,false,true]"},
		{"array", [3]int64{3, 1, 3}, "[3,1,3]"},
		{"string-array", [3]string{"c", "a", "c"}, "['c','a','c']"},
		{"named-strings", namedStrings{"c", "a", "c"}, "['c','a','c']"},
		{"named-ints", namedInts{3, 1, 3}, "[3,1,3]"},
		{"maps", []map[string]interface{}{{"rank": int64(3)}, {"rank": int64(1)}, {"rank": int64(3)}}, "[{rank:3},{rank:1},{rank:3}]"},
		{"nested", [][]int64{{3}, {1}, {3}}, "[[3],[1],[3]]"},
		{"nested-string", [][]string{{"c"}, {"a"}, {"c"}}, "[['c'],['a'],['c']]"},
		{"nested-array", [3][1]int64{{3}, {1}, {3}}, "[[3],[1],[3]]"},
		{"mixed", []interface{}{[]int64{3}, []string{"a"}, nil}, "[[3],['a'],null]"},
	}
}

// Byte slices are binary in Bolt and base64 strings in JSON, so they are
// covered as runtime lists only in the executor matrix. The transport matrix
// exercises the ordinary driver list shapes and stored scalar property arrays.
func TransportInputs() []Input {
	var out []Input
	for _, input := range Inputs() {
		switch input.Name {
		case "any", "string", "int64", "float64", "bool", "nested", "maps":
			out = append(out, input)
		}
	}
	return out
}

// Cypher property writes accept scalar arrays. Nested list/map property shapes
// are exercised through supported storage API fixtures and public query reads.
func (input Input) CypherWritableProperty() bool {
	return input.Name != "nested" && input.Name != "maps"
}

// PropertyValue keeps every storage-supported typed list unchanged. Other Go
// representations are valid runtime parameters but cannot be stored directly;
// those fixtures use the equivalent literal's supported property shape. This
// deliberately does not call the conversion implementation being tested.
func (input Input) PropertyValue() interface{} {
	switch input.Value.(type) {
	case []interface{}, []byte, []string, []int, []int32, []int64, []float32, []float64, []bool:
		return input.Value
	}
	switch input.Literal {
	case "[3,1,3]":
		return []int64{3, 1, 3}
	case "[3,-1,3]":
		return []int64{3, -1, 3}
	case "['c','a','c']":
		return []string{"c", "a", "c"}
	case "[{rank:3},{rank:1},{rank:3}]":
		return []interface{}{map[string]interface{}{"rank": int64(3)}, map[string]interface{}{"rank": int64(1)}, map[string]interface{}{"rank": int64(3)}}
	case "[[3],[1],[3]]":
		return []interface{}{[]int64{3}, []int64{1}, []int64{3}}
	case "[['c'],['a'],['c']]":
		return []interface{}{[]string{"c"}, []string{"a"}, []string{"c"}}
	}
	panic("missing property fixture: " + input.Name)
}

// Queries covers every built-in apoc.coll function and the adjacent public list
// consumers. %s occurs only where a runtime list is consumed, including the
// second operand and lists nested inside other list/map arguments.
func Queries() []string {
	return []string{
		"apoc.coll.flatten(%s)", "apoc.coll.flatten([%s],true)",
		"apoc.coll.toSet(%s)", "apoc.coll.sum(%s)", "apoc.coll.avg(%s)",
		"apoc.coll.min(%s)", "apoc.coll.max(%s)", "apoc.coll.sort(%s)",
		"apoc.coll.sortNodes(%s,'rank')", "apoc.coll.reverse(%s)",
		"apoc.coll.union(%s,[9])", "apoc.coll.union([9],%s)",
		"apoc.coll.unionAll(%s,[9])", "apoc.coll.unionAll([9],%s)",
		"apoc.coll.intersection(%s,%s)", "apoc.coll.subtract(%s,[9])",
		"apoc.coll.contains(%s,head(%s))", "apoc.coll.containsAll(%s,%s)",
		"apoc.coll.containsAny(%s,%s)", "apoc.coll.indexOf(%s,head(%s))",
		"apoc.coll.split(%s,head(%s))", "apoc.coll.partition(%s,2)",
		"apoc.coll.pairs(%s)", "apoc.coll.zip(%s,[4,5,6])", "apoc.coll.zip([4,5,6],%s)",
		"apoc.coll.frequencies(%s)", "apoc.coll.occurrences(%s,head(%s))",
		"apoc.text.join(%s,',')", "apoc.map.fromLists(['a','b','c'],%s)",
		"apoc.map.fromLists(%s,['x','y','z'])", "apoc.map.fromPairs([%s])", "apoc.map.fromPairs(%s)",
		"apoc.map.clean({a:3,b:1,c:9},%s,[])",
		"apoc.map.clean({a:3,b:1,c:9},[],%s)",
		"coll.distinct(%s)", "coll.flatten(%s)", "coll.indexOf(%s,head(%s))",
		"coll.insert(%s,1,9)", "coll.max(%s)", "coll.min(%s)",
		"coll.remove(%s,1)", "coll.sort(%s)",
		"head(%s)", "last(%s)", "tail(%s)", "reverse(%s)", "size(%s)",
		"isEmpty(%s)", "polygon(%s)", "lineString(%s)",
		"reduce(acc=[],x IN %s | acc + [x])", "[x IN %s | x]",
		"toIntegerList(%s)", "toFloatList(%s)", "toStringList(%s)", "toBooleanList(%s)",
	}
}

func Query(template, input string) string {
	return "RETURN " + strings.ReplaceAll(template, "%s", input) + " AS v"
}

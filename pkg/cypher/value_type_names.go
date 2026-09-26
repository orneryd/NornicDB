package cypher

import (
	"fmt"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Value type names (#657).
//
// Every place that names a value's type in an error or a type function
// classifies the value once (cypherValueKindOf) and reads the name from
// valueTypeNames in the style its message needs. Neo4j uses four:
//   - cypher: the Cypher type in "Type mismatch: expected X but was Integer";
//   - runtime: the storage value class in run-time errors ("Cannot add
//     `Long` and `Map`"), with stored arrays as LongArray, StringArray, …;
//   - typeSystem: the type-system name valueType() and procedure signatures
//     use (INTEGER, ZONED DATETIME, …);
//   - apoc: apoc.meta.type's name (INTEGER, DATE_TIME, …).

// cypherValueKind is the Cypher type of a runtime value.
type cypherValueKind uint8

const (
	valueKindOther cypherValueKind = iota
	valueKindNull
	valueKindBoolean
	valueKindInteger
	valueKindFloat
	valueKindString
	valueKindMap
	valueKindList
	valueKindNode
	valueKindRelationship
	valueKindPath
	valueKindDate
	valueKindTime
	valueKindLocalTime
	valueKindDateTime
	valueKindLocalDateTime
	valueKindDuration
)

// valueTypeName is one kind's name in each style.
type valueTypeName struct {
	cypher     string
	runtime    string
	typeSystem string
	apoc       string
}

// valueTypeNames is the one table of value type names.
var valueTypeNames = [...]valueTypeName{
	valueKindOther:         {cypher: "Any", runtime: "Any", typeSystem: "ANY", apoc: "ANY"},
	valueKindNull:          {cypher: "Null", runtime: "NoValue", typeSystem: "NULL", apoc: "NULL"},
	valueKindBoolean:       {cypher: "Boolean", runtime: "Boolean", typeSystem: "BOOLEAN", apoc: "BOOLEAN"},
	valueKindInteger:       {cypher: "Integer", runtime: "Long", typeSystem: "INTEGER", apoc: "INTEGER"},
	valueKindFloat:         {cypher: "Float", runtime: "Double", typeSystem: "FLOAT", apoc: "FLOAT"},
	valueKindString:        {cypher: "String", runtime: "String", typeSystem: "STRING", apoc: "STRING"},
	valueKindMap:           {cypher: "Map", runtime: "Map", typeSystem: "MAP", apoc: "MAP"},
	valueKindList:          {cypher: "List", runtime: "List", typeSystem: "LIST", apoc: "LIST"},
	valueKindNode:          {cypher: "Node", runtime: "NodeIdReference", typeSystem: "NODE", apoc: "NODE"},
	valueKindRelationship:  {cypher: "Relationship", runtime: "RelationshipReference", typeSystem: "RELATIONSHIP", apoc: "RELATIONSHIP"},
	valueKindPath:          {cypher: "Path", runtime: "Path", typeSystem: "PATH", apoc: "PATH"},
	valueKindDate:          {cypher: "Date", runtime: "Date", typeSystem: "DATE", apoc: "DATE"},
	valueKindTime:          {cypher: "Time", runtime: "Time", typeSystem: "ZONED TIME", apoc: "TIME"},
	valueKindLocalTime:     {cypher: "LocalTime", runtime: "LocalTime", typeSystem: "LOCAL TIME", apoc: "LOCAL_TIME"},
	valueKindDateTime:      {cypher: "DateTime", runtime: "DateTime", typeSystem: "ZONED DATETIME", apoc: "DATE_TIME"},
	valueKindLocalDateTime: {cypher: "LocalDateTime", runtime: "LocalDateTime", typeSystem: "LOCAL DATETIME", apoc: "LOCAL_DATE_TIME"},
	valueKindDuration:      {cypher: "Duration", runtime: "Duration", typeSystem: "DURATION", apoc: "DURATION"},
}

// pathTypeMarker names a path value (paths are carried as maps holding a
// _pathResult) for the classifier.
type pathTypeMarker struct{}

// cypherValueKindOf classifies a runtime value. Paths carried as maps with a
// _pathResult are paths; any Go map is a map and any slice a list.
func cypherValueKindOf(value interface{}) cypherValueKind {
	switch v := value.(type) {
	case nil:
		return valueKindNull
	case bool:
		return valueKindBoolean
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return valueKindInteger
	case float32, float64:
		return valueKindFloat
	case string:
		return valueKindString
	case *storage.Node:
		return valueKindNode
	case *storage.Edge:
		return valueKindRelationship
	case pathTypeMarker, *PathResult, PathResult:
		return valueKindPath
	case CypherDate, *CypherDate:
		return valueKindDate
	case CypherTime, *CypherTime:
		return valueKindTime
	case CypherLocalTime, *CypherLocalTime:
		return valueKindLocalTime
	case CypherDateTime, *CypherDateTime:
		return valueKindDateTime
	case CypherLocalDateTime, *CypherLocalDateTime:
		return valueKindLocalDateTime
	case CypherDuration, *CypherDuration:
		return valueKindDuration
	case map[string]interface{}:
		if _, isPath := v["_pathResult"]; isPath {
			return valueKindPath
		}
		return valueKindMap
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Map:
		return valueKindMap
	case reflect.Slice, reflect.Array:
		return valueKindList
	}
	return valueKindOther
}

// cypherTypeName is the Cypher type name Neo4j uses in "Type mismatch:
// expected X but was <type>" (Integer, List, Node, …). A Go value of no
// Cypher type is named by its Go type.
func cypherTypeName(value interface{}) string {
	if kind := cypherValueKindOf(value); kind != valueKindOther {
		return valueTypeNames[kind].cypher
	}
	return fmt.Sprintf("%T", value)
}

// neo4jValueTypeName is the storage value class Neo4j names in run-time
// errors: Long, Double, String, Map, NoValue, and for a stored array its
// element class plus Array (LongArray, StringArray, …); any other list is a
// List.
func neo4jValueTypeName(value interface{}) string {
	kind := cypherValueKindOf(value)
	if kind == valueKindList {
		if element, stored := storedArrayElementKind(value); stored {
			return valueTypeNames[element].runtime + "Array"
		}
	}
	if kind != valueKindOther {
		return valueTypeNames[kind].runtime
	}
	return fmt.Sprintf("%T", value)
}

// cypherTypeSystemName is the type-system name of a value's type (INTEGER,
// LIST, ZONED DATETIME, …), without nullability.
func cypherTypeSystemName(value interface{}) string {
	return valueTypeNames[cypherValueKindOf(value)].typeSystem
}

// apocValueTypeName is apoc.meta.type's name for a value's type.
func apocValueTypeName(value interface{}) string {
	return valueTypeNames[cypherValueKindOf(value)].apoc
}

// storedArrayElementKind reports whether a list is a stored property array
// and its element kind: a list whose elements are all of one scalar type
// (Long, Double, String, Boolean). An empty list or any other list is a
// List.
func storedArrayElementKind(value interface{}) (cypherValueKind, bool) {
	element := valueKindOther
	for _, item := range toAnySlice(value) {
		kind := cypherValueKindOf(item)
		switch kind {
		case valueKindInteger, valueKindFloat, valueKindString, valueKindBoolean:
		default:
			return valueKindOther, false
		}
		if element != valueKindOther && element != kind {
			return valueKindOther, false
		}
		element = kind
	}
	return element, element != valueKindOther
}

// Package apocmeta owns the engine's native APOC metadata. Pure metadata is
// shared by the server and bundled plugin without engine build-tag dependencies.
package apocmeta

import "strings"

// Argument is the canonical argument metadata of a native value function.
type Argument struct {
	Name, Type, Description, Default string
	Optional, IsDeprecated           bool
}

// Function is a native scalar function and its SHOW FUNCTIONS metadata.
type Function struct {
	Name, Category, Signature, Description string
	ArgumentDescriptions                   []string
	Arguments                              []Argument
	NativeValue                            bool
}

var functions = append([]Function{
	{Name: "apoc.coll.avg", Signature: "apoc.coll.avg(coll :: LIST<INTEGER | FLOAT>) :: FLOAT", Description: "Returns the average of the numbers in the `LIST<INTEGER | FLOAT>`.", ArgumentDescriptions: []string{"The list to return the average from."}},
	{Name: "apoc.coll.contains", Signature: "apoc.coll.contains(coll :: LIST<ANY>, value :: ANY) :: BOOLEAN", Description: "Returns whether or not the given value exists in the given collection.", ArgumentDescriptions: []string{"The list to search for the given value.", "The value in the list to check for the existence of."}},
	{Name: "apoc.coll.containsAll", Signature: "apoc.coll.containsAll(coll1 :: LIST<ANY>, coll2 :: LIST<ANY>) :: BOOLEAN", Description: "Returns whether or not all of the given values exist in the given collection.", ArgumentDescriptions: []string{"The list to search for the given values in.", "The list of values in the given list to check for the existence of."}},
	{Name: "apoc.coll.containsAny", Signature: "apoc.coll.containsAny(coll1 :: LIST<ANY>, coll2 :: LIST<ANY>) :: BOOLEAN", Description: "Returns whether or not any of the given values exist in the given collection. (NornicDB extension)", ArgumentDescriptions: []string{"The list to search for the given values.", "The values in the list to check for the existence of."}},
	{Name: "apoc.coll.flatten", Signature: "apoc.coll.flatten(coll :: LIST<ANY>, recursive = false :: BOOLEAN) :: LIST<ANY>", Description: "Flattens the given `LIST<ANY>` (to flatten nested `LIST<ANY>` values, set recursive to true).", ArgumentDescriptions: []string{"The list to flatten.", "Whether nested list items should also be flattened."}},
	{Name: "apoc.coll.frequencies", Signature: "apoc.coll.frequencies(coll :: LIST<ANY>) :: LIST<ANY>", Description: "Returns a `LIST<ANY>` of frequencies of the items in the collection, keyed by `item` and `count`.", ArgumentDescriptions: []string{"The list to return items and their count from."}},
	{Name: "apoc.coll.indexOf", Signature: "apoc.coll.indexOf(coll :: LIST<ANY>, value :: ANY) :: INTEGER", Description: "Returns the index for the first occurrence of the specified value in the `LIST<ANY>`.", ArgumentDescriptions: []string{"The list to find the given value in.", "The value to find the first occurrence of in the given list."}},
	{Name: "apoc.coll.intersection", Signature: "apoc.coll.intersection(list1 :: LIST<ANY>, list2 :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the distinct intersection of two `LIST<ANY>` values.", ArgumentDescriptions: []string{"The list of values to compare against `list2` and form an intersection from.", "The list of values to compare against `list1` and form an intersection from."}},
	{Name: "apoc.coll.max", Signature: "apoc.coll.max(values :: LIST<ANY>) :: ANY", Description: "Returns the maximum of all values in the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The list to find the maximum in."}},
	{Name: "apoc.coll.min", Signature: "apoc.coll.min(values :: LIST<ANY>) :: ANY", Description: "Returns the minimum of all values in the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The list to find the minimum in."}},
	{Name: "apoc.coll.occurrences", Signature: "apoc.coll.occurrences(coll :: LIST<ANY>, item :: ANY) :: INTEGER", Description: "Returns the count of the given item in the collection.", ArgumentDescriptions: []string{"The list to collect the count of the given value from.", "The value to count in the given list."}},
	{Name: "apoc.coll.pairs", Signature: "apoc.coll.pairs(list :: LIST<ANY>) :: LIST<ANY>", Description: "Returns a `LIST<ANY>` of adjacent elements in the `LIST<ANY>` ([1,2],[2,3],[3,null]).", ArgumentDescriptions: []string{"The list to create pairs from."}},
	{Name: "apoc.coll.partition", Signature: "apoc.coll.partition(coll :: LIST<ANY>, batchSize :: INTEGER) :: LIST<ANY>", Description: "Partitions the original `LIST<ANY>` into a new `LIST<ANY>` of the given batch size.\nThe final `LIST<ANY>` may be smaller than the given batch size.", ArgumentDescriptions: []string{"The list to partition into smaller sublists.", "The max size of each partitioned sublist."}},
	{Name: "apoc.coll.reverse", Signature: "apoc.coll.reverse(coll :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the given `LIST<ANY>` in reverse order. (NornicDB extension)", ArgumentDescriptions: []string{"The list to reverse."}},
	{Name: "apoc.coll.sort", Signature: "apoc.coll.sort(coll :: LIST<ANY>) :: LIST<ANY>", Description: "Sorts the given `LIST<ANY>` into ascending order.", ArgumentDescriptions: []string{"The list to be sorted."}},
	{Name: "apoc.coll.sortNodes", Signature: "apoc.coll.sortNodes(coll :: LIST<NODE>, prop :: STRING) :: LIST<ANY>", Description: "Sorts the given `LIST<NODE>` by the property of the nodes into descending order.", ArgumentDescriptions: []string{"The list of nodes to be sorted.", "The property key on the node to be used to sort the list by."}},
	{Name: "apoc.coll.split", Signature: "apoc.coll.split(coll :: LIST<ANY>, value :: ANY) :: LIST<LIST<ANY>>", Description: "Splits a collection by the given value. The value itself will not be part of the resulting `LIST<ANY>` values. (A procedure in APOC; a function here.)", ArgumentDescriptions: []string{"The list to split into parts.", "The value to split the given list by."}},
	{Name: "apoc.coll.subtract", Signature: "apoc.coll.subtract(list1 :: LIST<ANY>, list2 :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the first `LIST<ANY>` as a set with all the elements of the second `LIST<ANY>` removed.", ArgumentDescriptions: []string{"The list to remove values from.", "The list of values to be removed from `list1`."}},
	{Name: "apoc.coll.sum", Signature: "apoc.coll.sum(coll :: LIST<INTEGER | FLOAT>) :: FLOAT", Description: "Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`.", ArgumentDescriptions: []string{"The list of numbers to create a sum from."}},
	{Name: "apoc.coll.toSet", Signature: "apoc.coll.toSet(coll :: LIST<ANY>) :: LIST<ANY>", Description: "Returns a unique `LIST<ANY>` from the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The list of values to remove all duplicates from."}},
	{Name: "apoc.coll.union", Signature: "apoc.coll.union(list1 :: LIST<ANY>, list2 :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the distinct union of the two given `LIST<ANY>` values.", ArgumentDescriptions: []string{"The list of values to compare against `list2` and form a distinct union from.", "The list of values to compare against `list1` and form a distinct union from."}},
	{Name: "apoc.coll.unionAll", Signature: "apoc.coll.unionAll(list1 :: LIST<ANY>, list2 :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the full union of the two given `LIST<ANY>` values (duplicates included).", ArgumentDescriptions: []string{"The list of values to compare against `list2` and form a union from.", "The list of values to compare against `list1` and form a union from."}},
	{Name: "apoc.coll.zip", Signature: "apoc.coll.zip(list1 :: LIST<ANY>, list2 :: LIST<ANY>) :: LIST<ANY>", Description: "Returns the two given `LIST<ANY>` values zipped together as a `LIST<LIST<ANY>>`.", ArgumentDescriptions: []string{"The list to zip together with `list2`.", "The list to zip together with `list1`."}},
	{Name: "apoc.convert.fromJsonList", Signature: "apoc.convert.fromJsonList(list :: STRING, path =  :: STRING, pathOptions = null :: LIST<STRING>) :: LIST<ANY>", Description: "Converts the given JSON list into a Cypher `LIST<STRING>`.", ArgumentDescriptions: []string{"A JSON stringified list.", "A JSON path expression used to extract a certain part from the list.", "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')"}},
	{Name: "apoc.convert.fromJsonMap", Signature: "apoc.convert.fromJsonMap(map :: STRING, path =  :: STRING, pathOptions = null :: LIST<STRING>) :: MAP", Description: "Converts the given JSON map into a Cypher `MAP`.", ArgumentDescriptions: []string{"A JSON stringified map.", "A JSON path expression used to extract a certain part from the map.", "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')"}},
	{Name: "apoc.convert.toJson", Signature: "apoc.convert.toJson(value :: ANY) :: STRING", Description: "Serializes the given JSON value.", ArgumentDescriptions: []string{"The value to serialize."}},
	{Name: "apoc.create.uuid", Signature: "apoc.create.uuid() :: STRING", Description: "Returns a UUID.", ArgumentDescriptions: []string{}},
	{Name: "apoc.map.clean", Signature: "apoc.map.clean(map :: MAP, keys :: LIST<STRING>, values :: LIST<ANY>) :: MAP", Description: "Filters the keys and values contained in the given `LIST<ANY>` values.", ArgumentDescriptions: []string{"The map to clean.", "The list of property keys to be removed.", "The list of values to be removed."}},
	{Name: "apoc.map.fromLists", Signature: "apoc.map.fromLists(keys :: LIST<STRING>, values :: LIST<ANY>) :: MAP", Description: "Creates a `MAP` from the keys and values in the given `LIST<ANY>` values.", ArgumentDescriptions: []string{"A list of keys to create a map from.", "A list of values associated with the keys to create a map from."}},
	{Name: "apoc.map.fromPairs", Signature: "apoc.map.fromPairs(pairs :: LIST<LIST<ANY>>) :: MAP", Description: "Creates a `MAP` from the given `LIST<LIST<ANY>>` of key-value pairs.", ArgumentDescriptions: []string{"A list of pairs to create a map from."}},
	{Name: "apoc.map.merge", Signature: "apoc.map.merge(map1 :: MAP, map2 :: MAP) :: MAP", Description: "Merges the two given `MAP` values into one `MAP`.", ArgumentDescriptions: []string{"The first map to merge with the second map.", "The second map to merge with the first map."}},
	{Name: "apoc.map.removeKey", Signature: "apoc.map.removeKey(map :: MAP, key :: STRING, config = {} :: MAP) :: MAP", Description: "Removes the given key from the `MAP` (recursively if recursive is true).", ArgumentDescriptions: []string{"The map to be updated.", "The key to remove from the map.", "{ recursive = false :: BOOLEAN }"}},
	{Name: "apoc.map.removeKeys", Signature: "apoc.map.removeKeys(map :: MAP, keys :: LIST<STRING>, config = {} :: MAP) :: MAP", Description: "Removes the given keys from the `MAP` (recursively if recursive is true).", ArgumentDescriptions: []string{"The map to be updated.", "The keys to remove from the map.", "{ recursive = false :: BOOLEAN }"}},
	{Name: "apoc.map.setKey", Signature: "apoc.map.setKey(map :: MAP, key :: STRING, value :: ANY) :: MAP", Description: "Adds or updates the given entry in the `MAP`.", ArgumentDescriptions: []string{"The map to be updated.", "The key to add or update the map with.", "The value to set the given key to."}},
	{Name: "apoc.meta.isType", Signature: "apoc.meta.isType(value :: ANY, type :: STRING) :: BOOLEAN", Description: "Returns whether the type name of the given value (as apoc.meta.type returns it) is the given type. (NornicDB extension; APOC 5's is apoc.meta.cypher.isType)", ArgumentDescriptions: []string{"The value to check the type of.", "The type name to compare with."}},
	{Name: "apoc.meta.type", Signature: "apoc.meta.type(value :: ANY) :: STRING", Description: "Returns the type name of the given value. (NornicDB extension; APOC 5's is apoc.meta.cypher.type)", ArgumentDescriptions: []string{"The value to return the type name of."}},
	{Name: "apoc.text.join", Signature: "apoc.text.join(texts :: LIST<STRING>, delimiter :: STRING) :: STRING", Description: "Joins the given `STRING` values using the given delimiter.", ArgumentDescriptions: []string{"The list of strings to be concatenated using the given delimiter.", "The given delimiter to join the given strings with."}},
	{Name: "apoc.util.md5", Signature: "apoc.util.md5(values :: LIST<ANY>) :: STRING", Description: "Returns the MD5 checksum of the concatenation of all `STRING` values in the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The values to hash."}},
	{Name: "apoc.util.sha1", Signature: "apoc.util.sha1(values :: LIST<ANY>) :: STRING", Description: "Returns the SHA1 of the concatenation of all `STRING` values in the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The values to hash."}},
	{Name: "apoc.util.sha256", Signature: "apoc.util.sha256(values :: LIST<ANY>) :: STRING", Description: "Returns the SHA256 of the concatenation of all `STRING` values in the given `LIST<ANY>`.", ArgumentDescriptions: []string{"The values to hash."}},
}, nativeValueFunctions()...)

var byName = func() map[string]Function {
	names := make(map[string]Function, len(functions))
	for _, function := range functions {
		names[strings.ToLower(function.Name)] = function
	}
	return names
}()

// NativeFunctions returns metadata without lending its canonical slices.
func NativeFunctions() []Function {
	result := append([]Function(nil), functions...)
	for i := range result {
		result[i].ArgumentDescriptions = append([]string(nil), result[i].ArgumentDescriptions...)
		result[i].Arguments = append([]Argument(nil), result[i].Arguments...)
	}
	return result
}

// lookup excludes other namespaces before case folding. Every native name
// belongs to APOC; checking an ordinary mixed-case Cypher function must not
// allocate a lower-case string just to discover that it is not an APOC name.
func lookup(name string) (Function, bool) {
	if len(name) < len("apoc.") || !strings.EqualFold(name[:len("apoc.")], "apoc.") {
		return Function{}, false
	}
	function, found := byName[strings.ToLower(name)]
	return function, found
}

// IsNative reports whether the engine implements name natively.
func IsNative(name string) bool { _, found := lookup(name); return found }

// IsNativeValue reports functions that operate on native query entities.
func IsNativeValue(name string) bool { function, _ := lookup(name); return function.NativeValue }

// IsNativeValueCaseMismatch recognizes a native-value name with a different
// spelling from its canonical metadata entry. Case-folded membership alone
// cannot distinguish camelCase canonical names from genuine mismatches.
func IsNativeValueCaseMismatch(name string) bool {
	function, found := lookup(name)
	return found && function.NativeValue && name != function.Name
}

// NativeFunctionNames returns a caller-owned lower-case native name list.
func NativeFunctionNames() []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		names = append(names, strings.ToLower(function.Name))
	}
	return names
}

func nativeFunctionSignature(name, result string, arguments []Argument) string {
	parts := make([]string, len(arguments))
	for i, arg := range arguments {
		parts[i] = arg.Name + " :: " + arg.Type
		if arg.Optional {
			parts[i] += " = " + arg.Default
		}
	}
	return name + "(" + strings.Join(parts, ", ") + ") :: " + result
}

func nativeValueFunctions() []Function {
	var out []Function
	add := func(name, result string, arguments ...Argument) {
		out = append(out, Function{Name: name, Category: "APOC", Signature: nativeFunctionSignature(name, result, arguments), Arguments: arguments, NativeValue: true, Description: "Operates on native query values in the current graph."})
	}
	node := Argument{Name: "node", Type: "NODE"}
	rel := Argument{Name: "relationship", Type: "RELATIONSHIP"}
	path := Argument{Name: "path", Type: "PATH"}
	filter := Argument{Name: "relationshipFilter", Type: "STRING", Optional: true, Default: ""}
	add("apoc.node.id", "STRING", node)
	add("apoc.node.labels", "LIST<STRING>", node)
	add("apoc.node.degree", "INTEGER", node, filter)
	add("apoc.rel.id", "STRING", rel)
	add("apoc.rel.type", "STRING", rel)
	add("apoc.rel.startNode", "NODE", rel)
	add("apoc.rel.endNode", "NODE", rel)
	add("apoc.nodes.connected", "BOOLEAN", node, Argument{Name: "other", Type: "NODE"}, filter)
	add("apoc.path.elements", "LIST<ANY>", path)
	add("apoc.path.slice", "PATH", path, Argument{Name: "offset", Type: "INTEGER", Optional: true, Default: "0"}, Argument{Name: "length", Type: "INTEGER", Optional: true, Default: "-1"})
	add("apoc.path.combine", "PATH", path, Argument{Name: "other", Type: "PATH"})
	add("apoc.temporal.format", "STRING", Argument{Name: "input", Type: "ANY"}, Argument{Name: "format", Type: "STRING", Optional: true, Default: "yyyy-MM-dd"})
	return out
}

package cypher

import "strings"

// cypherFunctionSpec is one built-in function. The catalog is the one table
// of built-in functions (#698): SHOW FUNCTIONS lists the listed entries
// (showFunctionRows), and the unknown-function check accepts every entry's
// name (builtInCypherFunctions). listed is false for the path and list syntax
// the parser accepts in function-call position (shortestPath,
// allShortestPaths, reduce), which SHOW FUNCTIONS doesn't report. Entries
// marked "(NornicDB extension)" aren't Neo4j functions.
type cypherFunctionSpec struct {
	name        string
	category    string
	signature   string
	description string
	aggregating bool
	listed      bool
}

// cypherFunctionCatalog is the built-in function table.
var cypherFunctionCatalog = []cypherFunctionSpec{
	{name: "id", category: "Scalar", signature: "id(entity :: ANY) :: INTEGER", description: "Returns the id of a node or relationship", aggregating: false, listed: true},
	{name: "elementId", category: "Scalar", signature: "elementId(entity :: ANY) :: STRING", description: "Returns the element id of a node or relationship", aggregating: false, listed: true},
	{name: "labels", category: "List", signature: "labels(node :: NODE) :: LIST<STRING>", description: "Returns labels of a node", aggregating: false, listed: true},
	{name: "type", category: "Scalar", signature: "type(relationship :: RELATIONSHIP) :: STRING", description: "Returns the type of a relationship", aggregating: false, listed: true},
	{name: "keys", category: "List", signature: "keys(entity :: ANY) :: LIST<STRING>", description: "Returns the property keys of a node or relationship", aggregating: false, listed: true},
	{name: "properties", category: "Scalar", signature: "properties(entity :: ANY) :: MAP", description: "Returns all properties of a node or relationship", aggregating: false, listed: true},
	{name: "coalesce", category: "Scalar", signature: "coalesce(expression :: ANY...) :: ANY", description: "Returns first non-null value", aggregating: false, listed: true},
	{name: "head", category: "Scalar", signature: "head(list :: LIST<ANY>) :: ANY", description: "Returns the first element of a list", aggregating: false, listed: true},
	{name: "last", category: "Scalar", signature: "last(list :: LIST<ANY>) :: ANY", description: "Returns the last element of a list", aggregating: false, listed: true},
	{name: "tail", category: "List", signature: "tail(list :: LIST<ANY>) :: LIST<ANY>", description: "Returns all but the first element of a list", aggregating: false, listed: true},
	{name: "size", category: "Scalar", signature: "size(list :: LIST<ANY>) :: INTEGER", description: "Returns the number of elements in a list", aggregating: false, listed: true},
	{name: "length", category: "Scalar", signature: "length(path :: PATH) :: INTEGER", description: "Returns the length of a path", aggregating: false, listed: true},
	{name: "reverse", category: "String", signature: "reverse(original :: LIST<ANY> | STRING) :: LIST<ANY> | STRING", description: "Reverses a list or string", aggregating: false, listed: true},
	{name: "range", category: "List", signature: "range(start :: INTEGER, end :: INTEGER, step :: INTEGER = 1) :: LIST<INTEGER>", description: "Returns a list of integers", aggregating: false, listed: true},
	{name: "toString", category: "String", signature: "toString(expression :: ANY) :: STRING", description: "Converts expression to string", aggregating: false, listed: true},
	{name: "toInteger", category: "Scalar", signature: "toInteger(expression :: ANY) :: INTEGER", description: "Converts expression to integer", aggregating: false, listed: true},
	{name: "toFloat", category: "Scalar", signature: "toFloat(expression :: ANY) :: FLOAT", description: "Converts expression to float", aggregating: false, listed: true},
	{name: "toBoolean", category: "Scalar", signature: "toBoolean(expression :: ANY) :: BOOLEAN", description: "Converts expression to boolean", aggregating: false, listed: true},
	{name: "toLower", category: "String", signature: "toLower(original :: STRING) :: STRING", description: "Converts string to lowercase", aggregating: false, listed: true},
	{name: "toUpper", category: "String", signature: "toUpper(original :: STRING) :: STRING", description: "Converts string to uppercase", aggregating: false, listed: true},
	{name: "trim", category: "String", signature: "trim(original :: STRING) :: STRING", description: "Trims whitespace from string", aggregating: false, listed: true},
	{name: "ltrim", category: "String", signature: "ltrim(original :: STRING) :: STRING", description: "Trims leading whitespace", aggregating: false, listed: true},
	{name: "rtrim", category: "String", signature: "rtrim(original :: STRING) :: STRING", description: "Trims trailing whitespace", aggregating: false, listed: true},
	{name: "replace", category: "String", signature: "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING", description: "Replaces all occurrences", aggregating: false, listed: true},
	{name: "split", category: "String", signature: "split(original :: STRING, splitDelimiter :: STRING) :: LIST<STRING>", description: "Splits string by delimiter", aggregating: false, listed: true},
	{name: "substring", category: "String", signature: "substring(original :: STRING, start :: INTEGER, length :: INTEGER = NULL) :: STRING", description: "Returns substring", aggregating: false, listed: true},
	{name: "left", category: "String", signature: "left(original :: STRING, length :: INTEGER) :: STRING", description: "Returns left part of string", aggregating: false, listed: true},
	{name: "right", category: "String", signature: "right(original :: STRING, length :: INTEGER) :: STRING", description: "Returns right part of string", aggregating: false, listed: true},
	{name: "abs", category: "Numeric", signature: "abs(expression :: NUMBER) :: NUMBER", description: "Returns absolute value", aggregating: false, listed: true},
	{name: "ceil", category: "Numeric", signature: "ceil(expression :: FLOAT) :: INTEGER", description: "Returns ceiling value", aggregating: false, listed: true},
	{name: "floor", category: "Numeric", signature: "floor(expression :: FLOAT) :: INTEGER", description: "Returns floor value", aggregating: false, listed: true},
	{name: "round", category: "Numeric", signature: "round(expression :: FLOAT) :: INTEGER", description: "Rounds to nearest integer", aggregating: false, listed: true},
	{name: "sign", category: "Numeric", signature: "sign(expression :: NUMBER) :: INTEGER", description: "Returns sign of number", aggregating: false, listed: true},
	{name: "sqrt", category: "Logarithmic", signature: "sqrt(expression :: FLOAT) :: FLOAT", description: "Returns square root", aggregating: false, listed: true},
	{name: "rand", category: "Numeric", signature: "rand() :: FLOAT", description: "Returns random float between 0 and 1", aggregating: false, listed: true},
	{name: "randomUUID", category: "Scalar", signature: "randomUUID() :: STRING", description: "Returns a random UUID", aggregating: false, listed: true},
	{name: "sin", category: "Trigonometric", signature: "sin(expression :: FLOAT) :: FLOAT", description: "Returns sine", aggregating: false, listed: true},
	{name: "cos", category: "Trigonometric", signature: "cos(expression :: FLOAT) :: FLOAT", description: "Returns cosine", aggregating: false, listed: true},
	{name: "tan", category: "Trigonometric", signature: "tan(expression :: FLOAT) :: FLOAT", description: "Returns tangent", aggregating: false, listed: true},
	{name: "log", category: "Logarithmic", signature: "log(expression :: FLOAT) :: FLOAT", description: "Returns natural logarithm", aggregating: false, listed: true},
	{name: "log10", category: "Logarithmic", signature: "log10(expression :: FLOAT) :: FLOAT", description: "Returns base-10 logarithm", aggregating: false, listed: true},
	{name: "exp", category: "Logarithmic", signature: "exp(expression :: FLOAT) :: FLOAT", description: "Returns e raised to power", aggregating: false, listed: true},
	{name: "pi", category: "Trigonometric", signature: "pi() :: FLOAT", description: "Returns pi constant", aggregating: false, listed: true},
	{name: "e", category: "Logarithmic", signature: "e() :: FLOAT", description: "Returns Euler's number", aggregating: false, listed: true},
	{name: "timestamp", category: "Scalar", signature: "timestamp() :: INTEGER", description: "Returns current timestamp in milliseconds", aggregating: false, listed: true},
	{name: "datetime", category: "Temporal", signature: "datetime(input :: ANY = NULL) :: DATETIME", description: "Creates a datetime", aggregating: false, listed: true},
	{name: "date", category: "Temporal", signature: "date(input :: ANY = NULL) :: DATE", description: "Creates a date", aggregating: false, listed: true},
	{name: "time", category: "Temporal", signature: "time(input :: ANY = NULL) :: TIME", description: "Creates a time", aggregating: false, listed: true},
	{name: "count", category: "Aggregating", signature: "count(expression :: ANY) :: INTEGER", description: "Returns count", aggregating: true, listed: true},
	{name: "sum", category: "Aggregating", signature: "sum(expression :: NUMBER) :: NUMBER", description: "Returns sum", aggregating: true, listed: true},
	{name: "avg", category: "Aggregating", signature: "avg(expression :: NUMBER) :: FLOAT", description: "Returns average", aggregating: true, listed: true},
	{name: "min", category: "Aggregating", signature: "min(expression :: ANY) :: ANY", description: "Returns minimum", aggregating: true, listed: true},
	{name: "max", category: "Aggregating", signature: "max(expression :: ANY) :: ANY", description: "Returns maximum", aggregating: true, listed: true},
	{name: "collect", category: "Aggregating", signature: "collect(expression :: ANY) :: LIST<ANY>", description: "Collects values into list", aggregating: true, listed: true},
	{name: "exists", category: "Predicate", signature: "exists(expression :: ANY) :: BOOLEAN", description: "Returns true if expression is not null", aggregating: false, listed: true},
	{name: "isEmpty", category: "Predicate", signature: "isEmpty(list :: LIST<ANY> | MAP | STRING) :: BOOLEAN", description: "Returns true if empty", aggregating: false, listed: true},
	{name: "all", category: "Predicate", signature: "all(variable IN list WHERE predicate) :: BOOLEAN", description: "Returns true if all match", aggregating: false, listed: true},
	{name: "any", category: "Predicate", signature: "any(variable IN list WHERE predicate) :: BOOLEAN", description: "Returns true if any match", aggregating: false, listed: true},
	{name: "none", category: "Predicate", signature: "none(variable IN list WHERE predicate) :: BOOLEAN", description: "Returns true if none match", aggregating: false, listed: true},
	{name: "single", category: "Predicate", signature: "single(variable IN list WHERE predicate) :: BOOLEAN", description: "Returns true if exactly one matches", aggregating: false, listed: true},
	{name: "point", category: "Spatial", signature: "point(input :: MAP) :: POINT", description: "Creates a point", aggregating: false, listed: true},
	{name: "distance", category: "Spatial", signature: "distance(point1 :: POINT, point2 :: POINT) :: FLOAT", description: "Returns distance between points", aggregating: false, listed: true},
	{name: "polygon", category: "Spatial", signature: "polygon(points :: LIST<POINT>) :: POLYGON", description: "Creates a polygon from a list of points", aggregating: false, listed: true},
	{name: "lineString", category: "Spatial", signature: "lineString(points :: LIST<POINT>) :: LINESTRING", description: "Creates a lineString from a list of points", aggregating: false, listed: true},
	{name: "point.intersects", category: "Spatial", signature: "point.intersects(point :: POINT, polygon :: POLYGON) :: BOOLEAN", description: "Checks if point intersects with polygon", aggregating: false, listed: true},
	{name: "point.contains", category: "Spatial", signature: "point.contains(polygon :: POLYGON, point :: POINT) :: BOOLEAN", description: "Checks if polygon contains point", aggregating: false, listed: true},
	{name: "vector.similarity.cosine", category: "Vector", signature: "vector.similarity.cosine(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", description: "Cosine similarity", aggregating: false, listed: true},
	{name: "vector.similarity.euclidean", category: "Vector", signature: "vector.similarity.euclidean(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", description: "Euclidean similarity", aggregating: false, listed: true},
	{name: "kalman.init", category: "Kalman", signature: "kalman.init(config? :: MAP) :: STRING", description: "Create new Kalman filter state (basic scalar filter for noise smoothing)", aggregating: false, listed: true},
	{name: "kalman.process", category: "Kalman", signature: "kalman.process(measurement :: FLOAT, state :: STRING, target? :: FLOAT) :: MAP", description: "Process measurement, returns {value, state}", aggregating: false, listed: true},
	{name: "kalman.predict", category: "Kalman", signature: "kalman.predict(state :: STRING, steps :: INTEGER) :: FLOAT", description: "Predict state n steps into the future", aggregating: false, listed: true},
	{name: "kalman.state", category: "Kalman", signature: "kalman.state(state :: STRING) :: FLOAT", description: "Get current state estimate from state JSON", aggregating: false, listed: true},
	{name: "kalman.reset", category: "Kalman", signature: "kalman.reset(state :: STRING) :: STRING", description: "Reset filter state to initial values", aggregating: false, listed: true},
	{name: "kalman.velocity.init", category: "Kalman", signature: "kalman.velocity.init(initialPos? :: FLOAT, initialVel? :: FLOAT) :: STRING", description: "Create 2-state Kalman filter (position + velocity for trend tracking)", aggregating: false, listed: true},
	{name: "kalman.velocity.process", category: "Kalman", signature: "kalman.velocity.process(measurement :: FLOAT, state :: STRING) :: MAP", description: "Process measurement, returns {value, velocity, state}", aggregating: false, listed: true},
	{name: "kalman.velocity.predict", category: "Kalman", signature: "kalman.velocity.predict(state :: STRING, steps :: INTEGER) :: FLOAT", description: "Predict position n steps into the future", aggregating: false, listed: true},
	{name: "kalman.adaptive.init", category: "Kalman", signature: "kalman.adaptive.init(config? :: MAP) :: STRING", description: "Create adaptive Kalman filter (auto-switches between basic and velocity modes)", aggregating: false, listed: true},
	{name: "kalman.adaptive.process", category: "Kalman", signature: "kalman.adaptive.process(measurement :: FLOAT, state :: STRING) :: MAP", description: "Process measurement, returns {value, mode, state}", aggregating: false, listed: true},
	{name: "btrim", category: "String", signature: "btrim(original :: STRING, trimCharacterString :: STRING = ' ') :: STRING", description: "Removes the given characters (default: whitespace) from both ends of a string", aggregating: false, listed: true},
	{name: "char_length", category: "String", signature: "char_length(input :: STRING) :: INTEGER", description: "Returns the number of characters in a string", aggregating: false, listed: true},
	{name: "character_length", category: "String", signature: "character_length(input :: STRING) :: INTEGER", description: "Returns the number of characters in a string", aggregating: false, listed: true},
	{name: "isNaN", category: "Numeric", signature: "isNaN(input :: INTEGER | FLOAT) :: BOOLEAN", description: "Returns true if the number is NaN", aggregating: false, listed: true},
	{name: "lower", category: "String", signature: "lower(input :: STRING) :: STRING", description: "Converts a string to lowercase", aggregating: false, listed: true},
	{name: "normalize", category: "String", signature: "normalize(input :: STRING, normalForm = NFC :: [NFC, NFD, NFKC, NFKD]) :: STRING", description: "Normalizes a string to a Unicode normal form (default NFC)", aggregating: false, listed: true},
	{name: "nullIf", category: "Scalar", signature: "nullIf(v1 :: ANY, v2 :: ANY) :: ANY", description: "Returns null if the two values are equal, otherwise the first", aggregating: false, listed: true},
	{name: "radians", category: "Trigonometric", signature: "radians(input :: FLOAT) :: FLOAT", description: "Converts degrees to radians", aggregating: false, listed: true},
	{name: "toBooleanList", category: "List", signature: "toBooleanList(input :: LIST<ANY>) :: LIST<BOOLEAN>", description: "Converts a list to a list of booleans; unconvertible items become null", aggregating: false, listed: true},
	{name: "toFloatList", category: "List", signature: "toFloatList(input :: LIST<ANY>) :: LIST<FLOAT>", description: "Converts a list to a list of floats; unconvertible items become null", aggregating: false, listed: true},
	{name: "toIntegerList", category: "List", signature: "toIntegerList(input :: LIST<ANY>) :: LIST<INTEGER>", description: "Converts a list to a list of integers; unconvertible items become null", aggregating: false, listed: true},
	{name: "toStringList", category: "List", signature: "toStringList(input :: LIST<ANY>) :: LIST<STRING>", description: "Converts a list to a list of strings; unconvertible items become null", aggregating: false, listed: true},
	{name: "upper", category: "String", signature: "upper(input :: STRING) :: STRING", description: "Converts a string to uppercase", aggregating: false, listed: true},
	{name: "valueType", category: "Scalar", signature: "valueType(input :: ANY) :: STRING", description: "Returns the Cypher type name of a value", aggregating: false, listed: true},
	{name: "acos", category: "Trigonometric", signature: "acos(input :: FLOAT) :: FLOAT", description: "Returns the arccosine of a number, in radians", aggregating: false, listed: true},
	{name: "asin", category: "Trigonometric", signature: "asin(input :: FLOAT) :: FLOAT", description: "Returns the arcsine of a number, in radians", aggregating: false, listed: true},
	{name: "atan", category: "Trigonometric", signature: "atan(input :: FLOAT) :: FLOAT", description: "Returns the arctangent of a number, in radians", aggregating: false, listed: true},
	{name: "atan2", category: "Trigonometric", signature: "atan2(y :: FLOAT, x :: FLOAT) :: FLOAT", description: "Returns the arctangent2 of a set of coordinates, in radians", aggregating: false, listed: true},
	{name: "cot", category: "Trigonometric", signature: "cot(input :: FLOAT) :: FLOAT", description: "Returns the cotangent of a number", aggregating: false, listed: true},
	{name: "degrees", category: "Trigonometric", signature: "degrees(input :: FLOAT) :: FLOAT", description: "Converts radians to degrees", aggregating: false, listed: true},
	{name: "duration", category: "Temporal", signature: "duration(input :: ANY) :: DURATION", description: "Creates a duration", aggregating: false, listed: true},
	{name: "endNode", category: "Scalar", signature: "endNode(input :: RELATIONSHIP) :: NODE", description: "Returns the end node of a relationship", aggregating: false, listed: true},
	{name: "haversin", category: "Trigonometric", signature: "haversin(input :: FLOAT) :: FLOAT", description: "Returns half the versine of a number", aggregating: false, listed: true},
	{name: "localdatetime", category: "Temporal", signature: "localdatetime(input = DEFAULT_TEMPORAL_ARGUMENT :: ANY) :: LOCAL DATETIME", description: "Creates a local datetime", aggregating: false, listed: true},
	{name: "localtime", category: "Temporal", signature: "localtime(input = DEFAULT_TEMPORAL_ARGUMENT :: ANY) :: LOCAL TIME", description: "Creates a local time", aggregating: false, listed: true},
	{name: "nodes", category: "List", signature: "nodes(input :: PATH) :: LIST<NODE>", description: "Returns the nodes of a path", aggregating: false, listed: true},
	{name: "percentileCont", category: "Aggregating", signature: "percentileCont(input :: FLOAT, percentile :: FLOAT) :: FLOAT", description: "Returns the percentile of a value, interpolating between values", aggregating: true, listed: true},
	{name: "percentileDisc", category: "Aggregating", signature: "percentileDisc(input :: INTEGER | FLOAT, percentile :: FLOAT) :: INTEGER | FLOAT", description: "Returns the nearest value to a percentile", aggregating: true, listed: true},
	{name: "relationships", category: "List", signature: "relationships(input :: PATH) :: LIST<RELATIONSHIP>", description: "Returns the relationships of a path", aggregating: false, listed: true},
	{name: "startNode", category: "Scalar", signature: "startNode(input :: RELATIONSHIP) :: NODE", description: "Returns the start node of a relationship", aggregating: false, listed: true},
	{name: "stdev", category: "Aggregating", signature: "stdev(input :: FLOAT) :: FLOAT", description: "Returns the sample standard deviation", aggregating: true, listed: true},
	{name: "stdevp", category: "Aggregating", signature: "stdevp(input :: FLOAT) :: FLOAT", description: "Returns the population standard deviation", aggregating: true, listed: true},
	{name: "toBooleanOrNull", category: "Scalar", signature: "toBooleanOrNull(input :: ANY) :: BOOLEAN", description: "Converts a value to a boolean, or null", aggregating: false, listed: true},
	{name: "toFloatOrNull", category: "Scalar", signature: "toFloatOrNull(input :: ANY) :: FLOAT", description: "Converts a value to a float, or null", aggregating: false, listed: true},
	{name: "toIntegerOrNull", category: "Scalar", signature: "toIntegerOrNull(input :: ANY) :: INTEGER", description: "Converts a value to an integer, or null", aggregating: false, listed: true},
	{name: "toStringOrNull", category: "String", signature: "toStringOrNull(input :: ANY) :: STRING", description: "Converts a value to a string, or null", aggregating: false, listed: true},
	{name: "cosh", category: "Trigonometric", signature: "cosh(input :: FLOAT) :: FLOAT", description: "Returns the hyperbolic cosine (NornicDB extension)", aggregating: false, listed: true},
	{name: "coth", category: "Trigonometric", signature: "coth(input :: FLOAT) :: FLOAT", description: "Returns the hyperbolic cotangent (NornicDB extension)", aggregating: false, listed: true},
	{name: "sinh", category: "Trigonometric", signature: "sinh(input :: FLOAT) :: FLOAT", description: "Returns the hyperbolic sine (NornicDB extension)", aggregating: false, listed: true},
	{name: "tanh", category: "Trigonometric", signature: "tanh(input :: FLOAT) :: FLOAT", description: "Returns the hyperbolic tangent (NornicDB extension)", aggregating: false, listed: true},
	{name: "format", category: "String", signature: "format(format :: STRING, values :: ANY...) :: STRING", description: "Formats values with a printf-style pattern (NornicDB extension)", aggregating: false, listed: true},
	{name: "lpad", category: "String", signature: "lpad(original :: STRING, length :: INTEGER, padding :: STRING = ' ') :: STRING", description: "Pads a string on the left to a length (NornicDB extension)", aggregating: false, listed: true},
	{name: "rpad", category: "String", signature: "rpad(original :: STRING, length :: INTEGER, padding :: STRING = ' ') :: STRING", description: "Pads a string on the right to a length (NornicDB extension)", aggregating: false, listed: true},
	{name: "power", category: "Numeric", signature: "power(base :: NUMBER, exponent :: NUMBER) :: FLOAT", description: "Returns base raised to exponent (NornicDB extension)", aggregating: false, listed: true},
	{name: "toInt", category: "Scalar", signature: "toInt(expression :: ANY) :: INTEGER", description: "Converts a value to an integer; toInteger's older name (NornicDB extension)", aggregating: false, listed: true},
	{name: "allshortestpaths"},
	{name: "reduce"},
	{name: "shortestpath"},
}

// builtInCypherFunctions is every catalog name, lower-cased, for the
// unknown-function check.
var builtInCypherFunctions = func() map[string]struct{} {
	names := make(map[string]struct{}, len(cypherFunctionCatalog))
	for _, function := range cypherFunctionCatalog {
		names[strings.ToLower(function.name)] = struct{}{}
	}
	return names
}()

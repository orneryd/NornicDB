package localization

const MessageCypherCoreListIndexNonInteger MessageID = "cyphercore.list_index_non_integer"
const MessageCypherCoreListIndexNonNumber MessageID = "cyphercore.list_index_non_number"
const MessageCypherCoreListIndexBoolean MessageID = "cyphercore.list_index_boolean"

// CypherCoreListIndexNonInteger describes a runtime numeric list index that is not an integer.
func CypherCoreListIndexNonInteger(value string) Message {
	return cypherCoreMessage(MessageCypherCoreListIndexNonInteger, "Cannot access a list using an non-integer number index, got "+value, map[string]any{"Value": value})
}

// CypherCoreListIndexNonNumber describes a runtime list index that is not numeric.
func CypherCoreListIndexNonNumber(list, value string) Message {
	return cypherCoreMessage(MessageCypherCoreListIndexNonNumber, "Cannot access a list '"+list+"' using a non-number index, got "+value, map[string]any{"List": list, "Value": value})
}

// CypherCoreListIndexBoolean retains the value-class detail of a boolean list index.
func CypherCoreListIndexBoolean(list, value string, boolean bool) Message {
	class := "BooleanValue$2"
	if boolean {
		class = "BooleanValue$1"
	}
	return cypherCoreMessage(MessageCypherCoreListIndexBoolean, "Cannot access a list '"+list+"' using a non-number index, got "+value+": Expected "+value+" to be a org.neo4j.values.storable.NumberValue, but it was a org.neo4j.values.storable."+class, map[string]any{"Class": class, "List": list, "Value": value})
}

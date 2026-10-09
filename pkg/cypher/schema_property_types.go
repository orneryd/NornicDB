package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func parsePropertyType(typeName string) (storage.PropertyType, error) {
	normalized := strings.Join(strings.Fields(upperASCII(typeName)), " ")
	if strings.HasPrefix(normalized, "LIST") {
		return parseListPropertyType(typeName, normalized)
	}
	switch normalized {
	case "STRING":
		return storage.PropertyTypeString, nil
	case "INTEGER", "INT":
		return storage.PropertyTypeInteger, nil
	case "FLOAT":
		return storage.PropertyTypeFloat, nil
	case "BOOLEAN", "BOOL":
		return storage.PropertyTypeBoolean, nil
	case "DATE":
		return storage.PropertyTypeDate, nil
	case "DATETIME", "ZONED DATETIME", "ZONEDDATETIME":
		return storage.PropertyTypeZonedDateTime, nil
	case "LOCAL DATETIME", "LOCALDATETIME":
		return storage.PropertyTypeLocalDateTime, nil
	default:
		return "", localizedError(localization.CypherSchemaUnsupportedPropertyType(typeName), nil)
	}
}

// parseListPropertyType parses LIST<T> and LIST<T NOT NULL> (Neo4j's canonical
// form) for scalar T. Property values cannot hold null list elements, so both
// spellings normalize to LIST<T NOT NULL>. Nested lists are not property types.
func parseListPropertyType(typeName, normalized string) (storage.PropertyType, error) {
	unsupported := localizedError(localization.CypherSchemaUnsupportedPropertyType(typeName), nil)
	rest := strings.TrimSpace(strings.TrimPrefix(normalized, "LIST"))
	if !strings.HasPrefix(rest, "<") || !strings.HasSuffix(rest, ">") {
		return "", unsupported
	}
	inner := strings.TrimSpace(rest[1 : len(rest)-1])
	inner = strings.TrimSpace(strings.TrimSuffix(inner, "NOT NULL"))
	if inner == "" || strings.HasPrefix(inner, "LIST") {
		return "", unsupported
	}
	element, err := parsePropertyType(inner)
	if err != nil {
		return "", unsupported
	}
	return storage.ListPropertyType(element), nil
}

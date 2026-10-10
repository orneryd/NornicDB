package cypher

import (
	"regexp"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// propertyScalarTypes are the type-system names a property type constraint
// accepts for a value (Neo4j 5.26): the property value types. NODE, MAP,
// ANY, NULL, NOTHING and PROPERTY VALUE are not property types.
var propertyScalarTypes = map[string]storage.PropertyType{
	"BOOLEAN":        storage.PropertyTypeBoolean,
	"STRING":         storage.PropertyTypeString,
	"INTEGER":        storage.PropertyTypeInteger,
	"FLOAT":          storage.PropertyTypeFloat,
	"DATE":           storage.PropertyTypeDate,
	"LOCAL TIME":     storage.PropertyTypeLocalTime,
	"ZONED TIME":     storage.PropertyTypeZonedTime,
	"LOCAL DATETIME": storage.PropertyTypeLocalDateTime,
	"ZONED DATETIME": storage.PropertyTypeZonedDateTime,
	"DURATION":       storage.PropertyTypeDuration,
	"POINT":          storage.PropertyTypePoint,
}

// propertyTypeSpellingExtensions are NornicDB's own spellings of a property
// type, kept from before constraints read types as type predicates do:
// DATETIME, ZONEDDATETIME and LOCALDATETIME as words of their own.
var propertyTypeSpellingExtensions = regexp.MustCompile(`(?i)\b(ZONEDDATETIME|LOCALDATETIME|DATETIME)\b`)

// parsePropertyType reads a property type constraint's type
// (REQUIRE n.p IS :: T) with the type predicate's parser, so every Neo4j
// spelling and synonym is read the same way (TIME WITHOUT TIME ZONE, VARCHAR,
// ARRAY<…>), and returns it as Neo4j normalizes it: a union's members
// deduplicated and in Neo4j's type order, lists after the scalar types
// ("STRING | INTEGER | FLOAT", "DATE | LIST<DURATION NOT NULL>").
//
// As in Neo4j, a member is a property value type or LIST<T NOT NULL> of one,
// and NOT NULL on a member or the whole type, a nested list, a union inside a
// list and ANY<…> NOT NULL are invalid. LIST<T> without NOT NULL is
// NornicDB's kept spelling of LIST<T NOT NULL> (a property list holds no
// nulls).
func parsePropertyType(typeName string) (storage.PropertyType, error) {
	// An invalid property type is Neo4j's SyntaxError.
	unsupported := localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidType", localization.CypherSchemaUnsupportedPropertyType(typeName))
	text := strings.Join(strings.Fields(typeName), " ")
	text = propertyTypeSpellingExtensions.ReplaceAllStringFunc(text, func(word string) string {
		if strings.EqualFold(word, "LOCALDATETIME") {
			return "LOCAL DATETIME"
		}
		return "ZONED DATETIME"
	})
	// "ZONED ZONED DATETIME" / "LOCAL ZONED DATETIME" from a spelled-out
	// ZONED DATETIME or LOCAL DATETIME.
	text = strings.NewReplacer("ZONED ZONED DATETIME", "ZONED DATETIME", "LOCAL ZONED DATETIME", "LOCAL DATETIME").Replace(upperASCII(text))
	spec, err := parseCypherTypeSpec(text)
	if err != nil {
		return "", unsupported
	}
	var members []cypherTypeMember
	for _, member := range spec.members {
		if member.name == "UNION" && member.element != nil {
			members = append(members, member.element.members...)
			continue
		}
		members = append(members, member)
	}
	type orderedType struct {
		list  bool
		order int
		name  storage.PropertyType
	}
	seen := make(map[storage.PropertyType]bool, len(members))
	var types []orderedType
	for _, member := range members {
		if member.notNull {
			return "", unsupported
		}
		list := member.name == "LIST"
		scalar := member
		if list {
			if member.element == nil || len(member.element.members) != 1 {
				return "", unsupported
			}
			scalar = member.element.members[0]
		}
		propertyType, known := propertyScalarTypes[scalar.name]
		if !known {
			return "", unsupported
		}
		if list {
			propertyType = storage.ListPropertyType(propertyType)
		}
		if !seen[propertyType] {
			seen[propertyType] = true
			types = append(types, orderedType{list: list, order: valueTypeOrder[scalar.name], name: propertyType})
		}
	}
	sort.Slice(types, func(i, j int) bool {
		if types[i].list != types[j].list {
			return !types[i].list
		}
		return types[i].order < types[j].order
	})
	names := make([]string, len(types))
	for index, propertyType := range types {
		names[index] = string(propertyType.name)
	}
	return storage.PropertyType(strings.Join(names, storage.PropertyTypeUnionSeparator)), nil
}

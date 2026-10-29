package cypher

import (
	"fmt"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// generatedSchemaName is the construction owner for implicit schema names.
// Framed components retain entity scope, rule kind and every decoded name;
// no byte inside a label, type or property is reserved as a separator.
func generatedSchemaName(category string, relationship bool, kind string, names ...string) string {
	cyphertext.RecordNameConstruction(category)
	entity := "node"
	if relationship {
		entity = "relationship"
	}
	return category + "_" + fmt.Sprintf("%q", append([]string{entity, kind}, names...))
}

package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// db.schema.nodeTypeProperties() and db.schema.relTypeProperties() list
// the property schema derived from the data, as Neo4j does: one row per
// node category (its exact label set) or relationship type and property,
// with every type the property has there and whether every node or
// relationship of the category has it. A category whose members have no
// properties gets one row with a null property. Property types are named
// as Neo4j 5.26 names them in a Cypher 5 statement (Long, StringArray) and
// as Neo4j 2026.09 does in a Cypher 25 statement (INTEGER NOT NULL,
// LIST<STRING NOT NULL> NOT NULL). Neo4j's row and type order is its hash
// order; NornicDB sorts both.

func schemaTypePropertiesProcedureSpec(name string, nodes bool) ProcedureSpec {
	description := localization.CypherProcedureMetadata(name)
	spec := ProcedureSpec{
		Name:               name,
		Description:        description.Fallback,
		DescriptionMessage: description,
		Mode:               ProcedureModeRead,
		WorksOnSystem:      true,
	}
	if nodes {
		spec.Signature = name + "() :: (nodeType :: STRING, nodeLabels :: LIST<STRING>, propertyName :: STRING, propertyTypes :: LIST<STRING>, mandatory :: BOOLEAN)"
		spec.Returns = []ProcedureColumn{
			{Name: "nodeType", Type: "STRING", Description: "A name generated from the labels on the node."},
			{Name: "nodeLabels", Type: "LIST<STRING>", Description: "A list containing the labels on a category of node."},
			{Name: "propertyName", Type: "STRING", Description: "A property key on a category of node."},
			{Name: "propertyTypes", Type: "LIST<STRING>", Description: "All types of a property belonging to a node category."},
			{Name: "mandatory", Type: "BOOLEAN", Description: "Whether or not the property is present on all nodes belonging to a node category."},
		}
		return spec
	}
	spec.Signature = name + "() :: (relType :: STRING, propertyName :: STRING, propertyTypes :: LIST<STRING>, mandatory :: BOOLEAN)"
	spec.Returns = []ProcedureColumn{
		{Name: "relType", Type: "STRING", Description: "A name generated from the type on the relationship."},
		{Name: "propertyName", Type: "STRING", Description: "A property key on a category of relationship."},
		{Name: "propertyTypes", Type: "LIST<STRING>", Description: "All types of a property belonging to a relationship category."},
		{Name: "mandatory", Type: "BOOLEAN", Description: "Whether or not the property is present on all relationships belonging to a relationship category."},
	}
	return spec
}

// schemaTypeCategory is one node category or relationship type: how many
// members it has and, per property, how many have it and its type names.
type schemaTypeCategory struct {
	labels     []string
	members    int
	properties map[string]*schemaTypeProperty
}

type schemaTypeProperty struct {
	members int
	types   map[string]bool
}

func (category *schemaTypeCategory) add(properties map[string]interface{}, cypher25 bool) {
	category.members++
	for name, value := range properties {
		property := category.properties[name]
		if property == nil {
			property = &schemaTypeProperty{types: make(map[string]bool, 1)}
			category.properties[name] = property
		}
		property.members++
		property.types[schemaPropertyTypeName(value, cypher25)] = true
	}
}

// callDbSchemaTypeProperties is db.schema.nodeTypeProperties() (nodes) or
// db.schema.relTypeProperties().
func (e *StorageExecutor) callDbSchemaTypeProperties(ctx context.Context, nodes bool) (*ExecuteResult, error) {
	cypher25 := cypherVersionFromContext(ctx) == "25"
	categories := make(map[string]*schemaTypeCategory)
	category := func(key string, labels []string) *schemaTypeCategory {
		found := categories[key]
		if found == nil {
			found = &schemaTypeCategory{labels: labels, properties: make(map[string]*schemaTypeProperty)}
			categories[key] = found
		}
		return found
	}
	if nodes {
		all, err := e.storage.AllNodes()
		if err != nil {
			return nil, err
		}
		for _, node := range all {
			labels := append([]string(nil), node.Labels...)
			sort.Strings(labels)
			category(schemaTypeKey(labels), labels).add(node.Properties, cypher25)
		}
	} else {
		all, err := e.storage.AllEdges()
		if err != nil {
			return nil, err
		}
		for _, edge := range all {
			category(schemaTypeKey([]string{edge.Type}), nil).add(edge.Properties, cypher25)
		}
	}

	keys := make([]string, 0, len(categories))
	for key := range categories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := &ExecuteResult{Columns: []string{"relType", "propertyName", "propertyTypes", "mandatory"}, Rows: [][]interface{}{}}
	if nodes {
		result.Columns = []string{"nodeType", "nodeLabels", "propertyName", "propertyTypes", "mandatory"}
	}
	for _, key := range keys {
		found := categories[key]
		head := []interface{}{key}
		if nodes {
			head = append(head, append([]string{}, found.labels...))
		}
		if len(found.properties) == 0 {
			result.Rows = append(result.Rows, append(head, nil, nil, false))
			continue
		}
		names := make([]string, 0, len(found.properties))
		for name := range found.properties {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			property := found.properties[name]
			row := append(append([]interface{}(nil), head...), name, sortedStringSet(property.types), property.members == found.members)
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

// schemaTypeKey is a category's name: each label or type backtick-quoted
// after a colon (:`A`:`B`), "" for nodes without labels.
func schemaTypeKey(names []string) string {
	var key strings.Builder
	for _, name := range names {
		key.WriteString(":" + quoteSchemaName(name))
	}
	return key.String()
}

// schemaPropertyTypeName names a stored property value's type as
// db.schema.*TypeProperties lists it: Neo4j 5.26's names in a Cypher 5
// statement (Long, Double, DateTime, LongArray; an empty list is a
// StringArray), Neo4j 2026.09's in a Cypher 25 one (INTEGER NOT NULL,
// ZONED DATETIME NOT NULL, LIST<INTEGER NOT NULL> NOT NULL; an empty list
// is LIST<NOTHING> NOT NULL). A list of integers and floats is a float
// list, as Neo4j stores it.
func schemaPropertyTypeName(value interface{}, cypher25 bool) string {
	kind := cypherValueKindOf(value)
	if kind != valueKindList {
		if cypher25 {
			return valueTypeNames[kind].typeSystem + " NOT NULL"
		}
		return valueTypeNames[kind].runtime
	}
	element, known := valueKindOther, false
	for _, item := range toAnySlice(value) {
		itemKind := cypherValueKindOf(item)
		switch {
		case !known:
			element, known = itemKind, true
		case element == valueKindInteger && itemKind == valueKindFloat:
			element = valueKindFloat
		}
	}
	switch {
	case cypher25 && !known:
		return "LIST<NOTHING> NOT NULL"
	case cypher25:
		return "LIST<" + valueTypeNames[element].typeSystem + " NOT NULL> NOT NULL"
	case !known:
		return "StringArray"
	}
	return valueTypeNames[element].runtime + "Array"
}

package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.schema.nodeTypeProperties and db.schema.relTypeProperties derive the
// property schema from the data, with Neo4j 5.26's type names in a Cypher 5
// statement and Neo4j 2026.09's in a Cypher 25 one (both recorded on Neo4j
// with this data; Neo4j's order is its hash order, NornicDB's sorted). On
// main they were ProcedureNotFound.
func TestSchemaTypeProperties(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_type_properties"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Zt {a: 1, b: 'x'}), (:Zt {a: 2.5}), (:Zt:Zu {c: [1, 2], d: date('2020-01-01')}), "+
		"(:Zv)-[:ZR {w: 1}]->(:Zv), ()-[:ZS]->(), ({zz: 1}), (:Ze {e: []}), (:Ze {e: [1, 2]}), (:Zf {m: [1.0, 2]})", nil)
	require.NoError(t, err)
	call := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{
		{"", []string{}, "zz", []string{"Long"}, false},
		{":`Ze`", []string{"Ze"}, "e", []string{"LongArray", "StringArray"}, true},
		{":`Zf`", []string{"Zf"}, "m", []string{"DoubleArray"}, true},
		{":`Zt`", []string{"Zt"}, "a", []string{"Double", "Long"}, true},
		{":`Zt`", []string{"Zt"}, "b", []string{"String"}, false},
		{":`Zt`:`Zu`", []string{"Zt", "Zu"}, "c", []string{"LongArray"}, true},
		{":`Zt`:`Zu`", []string{"Zt", "Zu"}, "d", []string{"Date"}, true},
		{":`Zv`", []string{"Zv"}, nil, nil, false},
	}, call("CYPHER 5 CALL db.schema.nodeTypeProperties()"))
	require.Equal(t, [][]interface{}{
		{"", []string{}, "zz", []string{"INTEGER NOT NULL"}, false},
		{":`Ze`", []string{"Ze"}, "e", []string{"LIST<INTEGER NOT NULL> NOT NULL", "LIST<NOTHING> NOT NULL"}, true},
		{":`Zf`", []string{"Zf"}, "m", []string{"LIST<FLOAT NOT NULL> NOT NULL"}, true},
		{":`Zt`", []string{"Zt"}, "a", []string{"FLOAT NOT NULL", "INTEGER NOT NULL"}, true},
		{":`Zt`", []string{"Zt"}, "b", []string{"STRING NOT NULL"}, false},
		{":`Zt`:`Zu`", []string{"Zt", "Zu"}, "c", []string{"LIST<INTEGER NOT NULL> NOT NULL"}, true},
		{":`Zt`:`Zu`", []string{"Zt", "Zu"}, "d", []string{"DATE NOT NULL"}, true},
		{":`Zv`", []string{"Zv"}, nil, nil, false},
	}, call("CYPHER 25 CALL db.schema.nodeTypeProperties()"))
	require.Equal(t, [][]interface{}{
		{":`ZR`", "w", []string{"Long"}, true},
		{":`ZS`", nil, nil, false},
	}, call("CYPHER 5 CALL db.schema.relTypeProperties()"))
	require.Equal(t, [][]interface{}{
		{":`ZR`", "w", []string{"INTEGER NOT NULL"}, true},
		{":`ZS`", nil, nil, false},
	}, call("CYPHER 25 CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory RETURN *"))

	for value, want := range map[string][2]string{
		"zoned datetime": {"DateTime", "ZONED DATETIME NOT NULL"},
		"local time":     {"LocalTime", "LOCAL TIME NOT NULL"},
		"duration":       {"Duration", "DURATION NOT NULL"},
		"point list":     {"PointArray", "LIST<POINT NOT NULL> NOT NULL"},
		"boolean list":   {"BooleanArray", "LIST<BOOLEAN NOT NULL> NOT NULL"},
		"mixed numbers":  {"DoubleArray", "LIST<FLOAT NOT NULL> NOT NULL"},
	} {
		stored := map[string]interface{}{
			"zoned datetime": CypherDateTime{},
			"local time":     CypherLocalTime{},
			"duration":       &CypherDuration{},
			"point list":     []interface{}{CypherPoint{}},
			"boolean list":   []bool{true},
			"mixed numbers":  []interface{}{int64(1), 2.5},
		}[value]
		require.Equal(t, want[0], schemaPropertyTypeName(stored, false), value)
		require.Equal(t, want[1], schemaPropertyTypeName(stored, true), value)
	}
}

type schemaTypePropertiesErrEngine struct {
	storage.Engine
	err error
}

func (e *schemaTypePropertiesErrEngine) AllNodes() ([]*storage.Node, error) { return nil, e.err }
func (e *schemaTypePropertiesErrEngine) AllEdges() ([]*storage.Edge, error) { return nil, e.err }

func TestSchemaTypePropertiesStoreError(t *testing.T) {
	scanErr := errors.New("scan failed")
	exec := NewStorageExecutor(&schemaTypePropertiesErrEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_type_properties_err"), err: scanErr})
	for _, nodes := range []bool{true, false} {
		_, err := exec.callDbSchemaTypeProperties(context.Background(), nodes)
		require.ErrorIs(t, err, scanErr)
	}
}

package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestTokenListingCreationOrder(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (:Bb), (:Aa)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "MATCH (a:Aa), (b:Bb) CREATE (b)-[:Yy]->(a), (a)-[:Xx]->(b)", nil)
	require.NoError(t, err)
	for _, test := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL db.labels() YIELD label RETURN label", [][]interface{}{{"Bb"}, {"Aa"}}},
		{"CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType", [][]interface{}{{"Yy"}, {"Xx"}}},
		{"CALL db.labels() YIELD label LIMIT 1 RETURN label", [][]interface{}{{"Bb"}}},
	} {
		t.Run(test.query, func(t *testing.T) {
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Rows)
		})
	}
	_, err = executor.Execute(ctx, "MATCH (b:Bb) DETACH DELETE b", nil)
	require.NoError(t, err)
	result, err := executor.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Aa"}}, result.Rows)
	_, err = executor.Execute(ctx, "CREATE (:Bb)", nil)
	require.NoError(t, err)
	result, err = executor.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Bb"}, {"Aa"}}, result.Rows)
}

func TestSharedProcedureMetadataMatchesPinnedNeo4j(t *testing.T) {
	compareSharedProcedureMetadata(t, "", 28)
}

func TestSharedTokenProcedureMetadataMatchesPinnedNeo4j(t *testing.T) {
	compareSharedProcedureMetadata(t, " WHERE name IN ['db.labels', 'db.relationshipTypes', 'db.propertyKeys']", 3)
}

func compareSharedProcedureMetadata(t *testing.T, filter string, minimum int) {
	t.Helper()
	endpoint := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if endpoint == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare procedure metadata")
	}
	query := "SHOW PROCEDURES YIELD name, signature, description, mode, worksOnSystem, argumentDescription, returnDescription, admin, isDeprecated, deprecatedBy, option" + filter + " RETURN * ORDER BY name"
	payload, err := json.Marshal(map[string]interface{}{"statements": []map[string]string{{"statement": query}}})
	require.NoError(t, err)
	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Post(strings.TrimRight(endpoint, "/")+"/db/neo4j/tx/commit", "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reference struct {
		Results []struct {
			Columns []string `json:"columns"`
			Data    []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []interface{} `json:"errors"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reference))
	require.Empty(t, reference.Errors)
	require.Len(t, reference.Results, 1)
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, reference.Results[0].Columns, result.Columns)
	native := procedureMetadataRowsByName(result)
	matched, executed := 0, 0
	for _, data := range reference.Results[0].Data {
		name, ok := data.Row[0].(string)
		require.True(t, ok)
		if actual, shared := native[name]; shared {
			matched++
			t.Run(name, func(t *testing.T) {
				executed++
				for _, setter := range []struct {
					name   string
					entity string
				}{
					{"db.create.setNodeVectorProperty", "node"},
					{"db.create.setRelationshipVectorProperty", "relationship"},
				} {
					if name == setter.name {
						require.Equal(t, "Set a vector property on a given "+setter.entity+" in a more space efficient representation than Cypher's SET.", data.Row[2])
						data.Row[2] = "Sets vector property on a " + setter.entity
					}
				}
				if name == "dbms.listConfig" {
					require.Equal(t, "List the currently active configuration settings of Neo4j.", data.Row[2])
					data.Row[2] = "List the currently active configuration settings of NornicDB."
				}
				if name == "db.schema.visualization" {
					description := data.Row[2].(string)
					const referenceStatistics = "due to the information available in the count store"
					require.Equal(t, 1, strings.Count(description, referenceStatistics))
					data.Row[2] = strings.Replace(description, referenceStatistics, "from independently observed start and end label sets", 1)
				}
				if name == "db.index.vector.queryNodes" || name == "db.index.vector.queryRelationships" {
					description := data.Row[2].(string)
					const referenceRange = "The similarity score is a value between [0, 1]; where 0 indicates least similar, 1 most similar."
					require.Equal(t, 1, strings.Count(description, referenceRange))
					data.Row[2] = strings.Replace(description, referenceRange, "Native cosine scores are in [-1, 1]; Euclidean scores are in (0, 1]; native dot-product scores are unbounded.", 1)
				}
				if name == "db.index.fulltext.queryNodes" || name == "db.index.fulltext.queryRelationships" {
					description := data.Row[2].(string)
					require.Equal(t, 1, strings.Count(description, "Lucene query score"))
					require.Equal(t, 1, strings.Count(description, "analyzer: 'whitespace'"))
					description = strings.Replace(description, "Lucene query score", "native fulltext query score", 1)
					data.Row[2] = strings.Replace(description, "analyzer: 'whitespace'", "analyzer: 'none'", 1)
				}
				if name == "db.index.fulltext.listAvailableAnalyzers" {
					require.Equal(t, name+"() :: (analyzer :: STRING, description :: STRING, stopwords :: LIST<STRING>)", data.Row[1])
					data.Row[1] = name + "() :: (analyzer :: STRING, description :: STRING, stopwords :: LIST<STRING>, kind :: STRING, version :: STRING, digest :: STRING, dynamicLoad :: BOOLEAN, selectedDatabases :: LIST<STRING>)"
					returns := data.Row[6].([]interface{})
					require.Len(t, returns, 3)
					for _, column := range []struct {
						name        string
						typeName    string
						description string
					}{
						{"kind", "STRING", "The native analyzer implementation kind."},
						{"version", "STRING", "The registered stemmer plugin version."},
						{"digest", "STRING", "The abbreviated registered plugin artifact digest."},
						{"dynamicLoad", "BOOLEAN", "Whether this platform supports dynamically loaded stemmer plugins."},
						{"selectedDatabases", "LIST<STRING>", "The database selections reported for this analyzer."},
					} {
						returns = append(returns, map[string]interface{}{"name": column.name, "type": column.typeName, "description": column.description, "isDeprecated": false})
					}
					data.Row[6] = returns
				}
				if name == "db.info" {
					require.Equal(t, "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", data.Row[1])
					data.Row[1] = "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)"
					columns, ok := data.Row[6].([]interface{})
					require.True(t, ok)
					data.Row[6] = append(columns,
						map[string]interface{}{"name": "nodeCount", "type": "INTEGER", "description": "The number of nodes in the database.", "isDeprecated": false},
						map[string]interface{}{"name": "relationshipCount", "type": "INTEGER", "description": "The number of relationships in the database.", "isDeprecated": false},
					)
				}
				if name == "dbms.components" {
					columns, ok := data.Row[6].([]interface{})
					require.True(t, ok)
					for _, column := range columns {
						metadata, ok := column.(map[string]interface{})
						require.True(t, ok)
						if metadata["name"] == "edition" {
							require.Equal(t, "The Neo4j edition of the DBMS.", metadata["description"])
							metadata["description"] = "The NornicDB edition of the DBMS."
						}
					}
				}
				require.Equal(t, data.Row, actual)
			})
		}
	}
	require.GreaterOrEqual(t, matched, minimum)
	t.Logf("matched %d shared procedure definitions; executed %d comparisons", matched, executed)
}

func TestShowProceduresLocalizesBuiltInMetadata(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_metadata_localization"))
	exec.SetLocalizationRenderer(manager)

	tests := []struct {
		name        string
		tag         language.Tag
		description string
	}{
		{name: "en-US", tag: language.AmericanEnglish, description: "List all labels attached to nodes within a database according to the user's access rights. The procedure returns empty results if the user is not authorized to view those labels."},
		{name: "es-ES", tag: language.EuropeanSpanish, description: "Enumera todas las etiquetas de la base de datos"},
		{name: "en-XA", tag: language.MustParse("en-XA"), description: "[!! List all labels attached to nodes within a database according to the user's access rights. The procedure returns empty results if the user is not authorized to view those labels. !!]"},
	}

	var englishRows map[string][]interface{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := localization.WithPreferences(context.Background(), test.tag)
			result, executeErr := exec.Execute(ctx, "SHOW PROCEDURES YIELD name, signature, description, mode, worksOnSystem", nil)
			require.NoError(t, executeErr)
			require.Equal(t, []string{"name", "signature", "description", "mode", "worksOnSystem"}, result.Columns)

			row := requireProcedureMetadataRow(t, result, "db.labels")
			require.Equal(t, []interface{}{
				"db.labels",
				"db.labels() :: (label :: STRING)",
				test.description,
				"READ",
				true,
			}, row)

			rows := procedureMetadataRowsByName(result)
			if test.tag == language.AmericanEnglish {
				englishRows = rows
				return
			}
			require.Len(t, rows, len(englishRows))
			for name, englishRow := range englishRows {
				localizedRow, ok := rows[name]
				require.True(t, ok, "procedure %s missing for locale %s", name, test.tag)
				require.Equal(t, englishRow[0], localizedRow[0], "name changed for %s", name)
				require.Equal(t, englishRow[1], localizedRow[1], "signature changed for %s", name)
				require.Equal(t, englishRow[3:], localizedRow[3:], "mode metadata changed for %s", name)
			}
		})
	}
}

func TestBuiltInProcedureMetadataDescriptorCoverage(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	coreCount := 0
	apocCount := 0
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if len(spec.Name) >= len("apoc.") && spec.Name[:len("apoc.")] == "apoc." {
			apocCount++
			require.Empty(t, spec.DescriptionMessage.ID, "APOC metadata must remain literal: %s", spec.Name)
			continue
		}
		coreCount++
		require.NotEmpty(t, spec.DescriptionMessage.ID, "core metadata requires a descriptor: %s", spec.Name)
		require.Equal(t, spec.Description, spec.DescriptionMessage.Fallback, "English fallback changed: %s", spec.Name)
	}
	require.Equal(t, 71, coreCount)
	require.Equal(t, 30, apocCount) // apoc.path.subgraphAll and expandConfig (#907)
}

func TestShowProceduresPreservesUserDefinedLiteralMetadata(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	const description = "Literal user-defined description"
	err := RegisterUserProcedure(ProcedureSpec{
		Name:        "custom.localized_literal",
		Signature:   "custom.localized_literal(value :: ANY) :: (value :: ANY)",
		Description: description,
		Mode:        ProcedureModeRead,
		MinArgs:     1,
		MaxArgs:     1,
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)

	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_metadata_user_literal"))
	exec.SetLocalizationRenderer(manager)

	ctx := localization.WithPreferences(context.Background(), language.EuropeanSpanish)
	result, err := exec.Execute(ctx, "SHOW PROCEDURES YIELD name, signature, description, mode, worksOnSystem", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{
		"custom.localized_literal",
		"custom.localized_literal(value :: ANY) :: (value :: ANY)",
		description,
		"READ",
		false,
	}, requireProcedureMetadataRow(t, result, "custom.localized_literal"))
}

func TestShowProceduresPreservesCanonicalFieldMetadata(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	require.NoError(t, registry.RegisterUser(ProcedureSpec{
		Name:         "custom.old",
		Signature:    "custom.old(value :: STRING = 'sample') :: (value :: STRING)",
		Mode:         ProcedureModeRead,
		Admin:        true,
		IsDeprecated: true,
		DeprecatedBy: "custom.new",
		MinArgs:      0,
		MaxArgs:      1,
		Params: []ProcedureParam{{
			Name: "value", Type: "STRING", Optional: true,
			Description: "Literal argument description", Default: "DefaultParameterValue{value=sample, type=STRING}", IsDeprecated: true,
		}},
		Returns: []ProcedureColumn{{Name: "value", Type: "STRING", Description: "Literal result description", IsDeprecated: true}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	}))
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW PROCEDURES YIELD argumentDescription, returnDescription, admin, isDeprecated, deprecatedBy, option RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		[]interface{}{map[string]interface{}{"name": "value", "type": "STRING", "description": "Literal argument description", "default": "DefaultParameterValue{value=sample, type=STRING}", "isDeprecated": true}},
		[]interface{}{map[string]interface{}{"name": "value", "type": "STRING", "description": "Literal result description", "isDeprecated": true}},
		true, true, "custom.new", map[string]interface{}{"deprecated": true},
	}}, result.Rows)
}

func TestBuiltInProcedureSignatureMetadataDefaults(t *testing.T) {
	for _, signature := range []string{
		"custom.defaults(required :: STRING, options = {} :: MAP) :: (value :: INTEGER)",
		"custom.defaults(required :: STRING, options :: MAP = {}) :: (value :: INTEGER)",
	} {
		t.Run(signature, func(t *testing.T) {
			registry := NewProcedureRegistry()
			replaceProcedureRegistryForTest(t, registry)
			registerProcedure(ProcedureSpec{Name: "custom.defaults", Signature: signature, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 2}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{}, nil
			})
			specs := registry.ListBuiltIns()
			require.Len(t, specs, 1)
			require.Equal(t, []ProcedureParam{
				{Name: "required", Type: "STRING"},
				{Name: "options", Type: "MAP", Optional: true, Default: "DefaultParameterValue{value={}, type=MAP}"},
			}, specs[0].Params)
			require.Equal(t, []ProcedureColumn{{Name: "value", Type: "INTEGER"}}, specs[0].Returns)
			require.Equal(t, 1, specs[0].MinArgs)
			require.Equal(t, 2, specs[0].MaxArgs)
		})
	}
}

func TestFunctionSignatureDefaultDescriptions(t *testing.T) {
	for _, testCase := range []struct {
		signature string
		value     string
		typeName  string
	}{
		{"f(value = 300 :: INTEGER) :: VOID", "300", "INTEGER"},
		{"f(value :: INTEGER = 300) :: VOID", "300", "INTEGER"},
		{"f(value = 'some, text' :: STRING) :: VOID", "some, text", "STRING"},
		{"f(value :: STRING = 'some, text') :: VOID", "some, text", "STRING"},
		{"f(value = '' :: STRING) :: VOID", "", "STRING"},
		{"f(value :: STRING = null) :: VOID", "null", "STRING"},
		{"f(value :: BOOLEAN = false) :: VOID", "false", "BOOLEAN"},
		{"f(value :: LIST<STRING> = []) :: VOID", "[]", "LIST<STRING>"},
	} {
		t.Run(testCase.signature, func(t *testing.T) {
			arguments, returns := functionSignatureDescriptions(testCase.signature)
			require.Equal(t, []interface{}{map[string]interface{}{
				"name": "value", "type": testCase.typeName, "description": "", "isDeprecated": false,
				"default": "DefaultParameterValue{value=" + testCase.value + ", type=" + testCase.typeName + "}",
			}}, arguments)
			require.Equal(t, "VOID", returns)
		})
	}
}

func requireProcedureMetadataRow(t *testing.T, result *ExecuteResult, name string) []interface{} {
	t.Helper()
	for _, row := range result.Rows {
		if len(row) > 0 && row[0] == name {
			return row
		}
	}
	require.FailNow(t, "procedure metadata row not found", name)
	return nil
}

func procedureMetadataRowsByName(result *ExecuteResult) map[string][]interface{} {
	rows := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			rows[row[0].(string)] = row
		}
	}
	return rows
}

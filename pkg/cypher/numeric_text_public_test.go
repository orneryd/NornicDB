// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Public-query numeric-text contract and independent token/value oracle; no production formatter supplies expected output.
package cypher

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocexport "github.com/orneryd/nornicdb/apoc/export"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type numericTextCase struct {
	name     string
	value    interface{}
	text     string
	floating bool
	decoded  interface{}
}

// Expected tokens and decoded kinds are literals. float32's shortest decimal
// becomes the corresponding JSON/Cypher FLOAT, not its widened binary tail.
func numericTextCases() []numericTextCase {
	return []numericTextCase{
		{"int", int(7), "7", false, int64(7)},
		{"int8", int8(-7), "-7", false, int64(-7)},
		{"int16", int16(-32768), "-32768", false, int64(-32768)},
		{"int32", int32(2147483647), "2147483647", false, int64(2147483647)},
		{"uint", uint(7), "7", false, int64(7)},
		{"uint8", uint8(255), "255", false, int64(255)},
		{"uint16", uint16(65535), "65535", false, int64(65535)},
		{"uint32", uint32(4294967295), "4294967295", false, int64(4294967295)},
		{"integer_before_2p53", int64(9007199254740991), "9007199254740991", false, int64(9007199254740991)},
		{"integer_after_2p53", int64(9007199254740993), "9007199254740993", false, int64(9007199254740993)},
		{"negative_integer_after_2p53", int64(-9007199254740993), "-9007199254740993", false, int64(-9007199254740993)},
		{"int64_max", int64(math.MaxInt64), "9223372036854775807", false, int64(math.MaxInt64)},
		{"int64_min", int64(math.MinInt64), "-9223372036854775808", false, int64(math.MinInt64)},
		{"uint64_max", uint64(math.MaxUint64), "18446744073709551615", false, uint64(math.MaxUint64)},
		{"float32_whole", float32(2), "2.0", true, float64(2)},
		{"float32_shortest", float32(1.2), "1.2", true, float64(1.2)},
		{"float64_whole", float64(2), "2.0", true, float64(2)},
		{"float64_negative_zero", math.Copysign(0, -1), "-0.0", true, math.Copysign(0, -1)},
		{"float64_subnormal", math.SmallestNonzeroFloat64, "4.9E-324", true, math.SmallestNonzeroFloat64},
		{"float64_large_whole", float64(1 << 60), "1.152921504606847E18", true, float64(1 << 60)},
		{"float64_decimal", float64(1.25), "1.25", true, float64(1.25)},
	}
}

func numericTextExecutor(t *testing.T, names ...string) *StorageExecutor {
	t.Helper()
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := make(map[string]interface{}, len(names))
	for _, name := range names {
		info, found := apoc.GetInfo(name)
		require.True(t, found, "the real APOC catalog must expose %s", name)
		handlers[name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_text"))
	exec.SetAllowLocalAPOCFileAccess(true)
	return exec
}

func numericTextQuery(t *testing.T, exec *StorageExecutor, query string, parameters map[string]interface{}) interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, parameters)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	require.Len(t, result.Rows[0], 1, query)
	return result.Rows[0][0]
}

func requireNumericTextToken(t *testing.T, c numericTextCase, token string) {
	t.Helper()
	if !c.floating {
		assert.Equal(t, c.text, token, "INTEGER digits must remain exact without a FLOAT token")
		return
	}
	assert.True(t, strings.ContainsAny(token, ".eE"), "FLOAT lost its fractional/exponent token: %q", token)
	value, err := strconv.ParseFloat(token, 64)
	require.NoError(t, err)
	assert.Equal(t, math.Float64bits(c.decoded.(float64)), math.Float64bits(value), "FLOAT value/negative zero changed")
}

func numericTextJSON(t *testing.T, text string) interface{} {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value interface{}
	require.NoError(t, decoder.Decode(&value))
	return value
}

func TestNumericTextPublicQueryFunctions(t *testing.T) {
	exec := numericTextExecutor(t, "apoc.convert.toString", "apoc.convert.toStringList")
	previous := rowValueProgramsDisabled.Load()
	t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, c := range numericTextCases() {
			for _, name := range []string{"toString", "toStringOrNull", "apoc.convert.toString", "apoc.convert.toStringList"} {
				for _, row := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/row=%t/programsDisabled=%t", c.name, name, row, disabled), func(t *testing.T) {
						argument := "$v"
						prefix := "RETURN "
						if row {
							prefix, argument = "UNWIND [$v] AS value WITH value RETURN ", "value"
						}
						list := strings.HasSuffix(name, "List")
						if list {
							argument = "[" + argument + "]"
						}
						got := numericTextQuery(t, exec, prefix+name+"("+argument+") AS text", map[string]interface{}{"v": c.value})
						if list {
							values := reflect.ValueOf(got)
							require.Equal(t, reflect.Slice, values.Kind())
							require.Equal(t, 1, values.Len())
							assert.Equal(t, c.text, values.Index(0).Interface())
						} else {
							assert.Equal(t, c.text, got)
						}
					})
				}
			}
		}
	}
	for _, text := range []string{"2", "2.0", "9007199254740993", "18446744073709551615"} {
		for _, name := range []string{"toString", "apoc.convert.toString"} {
			t.Run(name+"/text="+text, func(t *testing.T) {
				got := numericTextQuery(t, exec, "RETURN "+name+"($v) AS text", map[string]interface{}{"v": text})
				require.Equal(t, text, got, "numeric-looking strings retain their string representation")
			})
		}
	}
}

var numericTextProperty = regexp.MustCompile(`\bv:\s*([^}\r\n]+)`)

// Public results can outlive the next query's reuse of a formatting buffer.
func TestNumericTextPublicOutputOwnership(t *testing.T) {
	exec := numericTextExecutor(t, "apoc.convert.toString", "apoc.export.cypher")
	values := []float64{math.Copysign(0, -1), 2, 1.25}
	display := numericTextQuery(t, exec, "RETURN apoc.convert.toString($v) AS text", map[string]interface{}{"v": values}).(string)
	parameters := map[string]interface{}{
		"nodes": []*apocexport.Node{{ID: 1, Labels: []string{"NumericText"}, Properties: map[string]interface{}{"v": values}}},
		"edges": []*apocexport.Relationship{},
	}
	source := numericTextQuery(t, exec, "RETURN apoc.export.cypher($nodes,$edges) AS text", parameters).(string)
	for i := 0; i < 32; i++ {
		later := []float64{float64(i) + 0.5, 123456.25}
		_ = numericTextQuery(t, exec, "RETURN apoc.convert.toString($v) AS text", map[string]interface{}{"v": later})
		parameters["nodes"] = []*apocexport.Node{{ID: 9, Properties: map[string]interface{}{"v": later}}}
		_ = numericTextQuery(t, exec, "RETURN apoc.export.cypher($nodes,$edges) AS text", parameters)
	}
	require.Equal(t, "[-0.0 2.0 1.25]", display)
	require.Equal(t, "CREATE (n1:NumericText {v: [-0.0, 2.0, 1.25]});\n", source)
}

func TestNumericTextPublicCatalogExports(t *testing.T) {
	exec := numericTextExecutor(t, "apoc.export.cypher", "apoc.export.graphML", "apoc.export.csv")
	for _, c := range numericTextCases() {
		t.Run(c.name, func(t *testing.T) {
			parameters := map[string]interface{}{
				"nodes": []*apocexport.Node{{ID: 1, Labels: []string{"NumericText"}, Properties: map[string]interface{}{"v": c.value}}},
				"edges": []*apocexport.Relationship{{ID: 1, Type: "VALUE", StartNode: 1, EndNode: 1, Properties: map[string]interface{}{"v": c.value}}},
			}
			t.Run("cypher", func(t *testing.T) {
				text := numericTextQuery(t, exec, "RETURN apoc.export.cypher($nodes,$edges) AS text", parameters).(string)
				tokens := numericTextProperty.FindAllStringSubmatch(text, -1)
				require.Len(t, tokens, 2, "node and relationship public source: %s", text)
				for _, token := range tokens {
					actual := strings.TrimSpace(token[1])
					t.Run(actual, func(t *testing.T) { requireNumericTextToken(t, c, actual) })
					if _, highUnsigned := c.decoded.(uint64); !highUnsigned {
						got := numericTextQuery(t, exec, "RETURN "+actual+" AS value", nil)
						assert.Equal(t, c.decoded, got, "the public consumer must read the emitted token with its original kind/value")
						if c.floating {
							if value, ok := got.(float64); ok {
								assert.Equal(t, math.Float64bits(c.decoded.(float64)), math.Float64bits(value))
							}
						}
					}
					// Cypher literals have no unsigned integer above MaxInt64.
					// Its exact output digits are checked above; no unsigned
					// native-Cypher round-trip or Neo4j parity is claimed.
				}
			})
			t.Run("graphml", func(t *testing.T) {
				text := numericTextQuery(t, exec, "RETURN apoc.export.graphML($nodes,$edges) AS text", parameters).(string)
				var document struct {
					Values []struct {
						Key   string `xml:"key,attr"`
						Value string `xml:",chardata"`
					} `xml:"graph>node>data"`
					Edges []struct {
						Key   string `xml:"key,attr"`
						Value string `xml:",chardata"`
					} `xml:"graph>edge>data"`
				}
				require.NoError(t, xml.Unmarshal([]byte(text), &document))
				seen := 0
				for _, value := range append(document.Values, document.Edges...) {
					if value.Key == "v" {
						seen++
						requireNumericTextToken(t, c, value.Value)
					}
				}
				require.Equal(t, 2, seen)
			})
			t.Run("csv", func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "catalog.csv")
				parameters["file"] = path
				_ = numericTextQuery(t, exec, "RETURN apoc.export.csv($nodes,$file) AS result", parameters)
				text, err := os.ReadFile(path)
				require.NoError(t, err)
				rows, err := csv.NewReader(strings.NewReader(string(text))).ReadAll()
				require.NoError(t, err)
				require.Len(t, rows, 2)
				require.Len(t, rows[1], 3)
				requireNumericTextToken(t, c, rows[1][2])
			})
		})
	}
}

func TestNumericTextPublicCALLExports(t *testing.T) {
	for _, c := range numericTextCases() {
		// These are native CREATE's existing property-admission boundaries,
		// not text encoding policies. Widening float32 is lossless. Native
		// unsigned overflow currently admits Double (NOR-68 candidate
		// ad8ae4d2), whereas PropertyJSON imports retain exact uint64.
		switch c.name {
		case "float32_shortest":
			c.text, c.decoded = "1.2000000476837158", float64(1.2000000476837158)
		case "uint64_max":
			c.text, c.floating, c.decoded = "1.8446744073709552E19", true, float64(18446744073709551616.0)
		}
		t.Run(c.name, func(t *testing.T) {
			exec := numericTextExecutor(t)
			_, err := exec.Execute(context.Background(), "CREATE (n:NumericText {v:$v})", map[string]interface{}{"v": c.value})
			require.NoError(t, err)
			admitted := numericTextQuery(t, exec, "MATCH (n:NumericText) RETURN n.v AS value", nil)
			require.Equal(t, c.decoded, admitted, "the output oracle starts at the explicitly admitted property value")
			if c.floating {
				require.Equal(t, math.Float64bits(c.decoded.(float64)), math.Float64bits(admitted.(float64)))
			}
			_, err = exec.Execute(context.Background(), "MATCH (n:NumericText) CREATE (n)-[:VALUE {v:$v}]->(n)", map[string]interface{}{"v": c.value})
			require.NoError(t, err)
			for _, format := range []string{"json", "csv"} {
				for _, scope := range []string{"all", "query"} {
					t.Run(format+"/"+scope, func(t *testing.T) {
						path := filepath.Join(t.TempDir(), "native."+format)
						arguments := "$file"
						if scope == "query" {
							arguments = "'MATCH (n:NumericText)-[r:VALUE]->() RETURN n.v AS v,r.v AS w'," + arguments
						}
						_, err := exec.Execute(context.Background(), "CALL apoc.export."+format+"."+scope+"("+arguments+") YIELD file RETURN file", map[string]interface{}{"file": path})
						require.NoError(t, err)
						raw, err := os.ReadFile(path)
						require.NoError(t, err)
						var values []interface{}
						if format == "json" {
							document := numericTextJSON(t, string(raw)).(map[string]interface{})
							if scope == "query" {
								values = document["data"].([]interface{})[0].([]interface{})
							} else {
								for _, key := range []string{"nodes", "relationships"} {
									entity := document[key].([]interface{})[0].(map[string]interface{})
									values = append(values, entity["properties"].(map[string]interface{})["v"])
								}
							}
						} else {
							reader := csv.NewReader(strings.NewReader(string(raw)))
							reader.FieldsPerRecord, reader.Comment = -1, '#'
							rows, err := reader.ReadAll()
							require.NoError(t, err)
							if scope == "query" {
								require.Len(t, rows, 2)
								for _, cell := range rows[1] {
									values = append(values, numericTextJSON(t, cell))
								}
							} else {
								for _, row := range rows {
									if len(row) > 0 && strings.HasPrefix(row[len(row)-1], "{") {
										properties := numericTextJSON(t, row[len(row)-1]).(map[string]interface{})
										values = append(values, properties["v"])
									}
								}
							}
						}
						require.Len(t, values, 2)
						for _, value := range values {
							number, ok := value.(json.Number)
							require.True(t, ok, "numeric output became %T(%v)", value, value)
							requireNumericTextToken(t, c, string(number))
						}
					})
				}
			}
		})
	}
}

// Package transactionalcalltest holds one public contract matrix for Execute,
// Bolt and HTTP. oracle.json was captured from Neo4j 5.26.30, including a
// fresh read after every query. Unsupported options have an explicit local
// rejection contract rather than being advertised as implemented.
package transactionalcalltest

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed oracle.json
var oracle []byte

//go:embed syntax-constructors.json
var syntaxConstructors []byte

// SyntaxConstructors is the grammar-constructor inventory shared by the
// source guard and the type-resolved remote census.
func SyntaxConstructors() map[string]map[string]string {
	var constructors map[string]map[string]string
	if err := json.Unmarshal(syntaxConstructors, &constructors); err != nil {
		panic(err)
	}
	return constructors
}

type Case struct {
	Name        string         `json:"case"`
	Query       string         `json:"query"`
	Params      map[string]any `json:"params"`
	Explicit    bool           `json:"explicit"`
	Before      string         `json:"before,omitempty"`
	Unsupported bool           `json:"unsupported"`
	Code        string         `json:"code"`
	Values      []int64        `json:"values"`
	Rows        [][]any        `json:"rows,omitempty"`
}

type Outcome struct {
	Code   string
	Values []int64
	Rows   [][]any
}

const Reset = "MATCH (n:ReviewBatch) DETACH DELETE n"
const Read = "MATCH (n:ReviewBatch) RETURN n.v ORDER BY n.v"

func Check(t *testing.T, run func(*testing.T, Case) Outcome) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(oracle))
	decoder.UseNumber()
	var cases []Case
	require.NoError(t, decoder.Decode(&cases))
	for _, sample := range cases {
		sample.Params, _ = normalize(sample.Params).(map[string]any)
		if sample.Unsupported {
			sample.Code = "Neo.ClientError.Statement.SyntaxError"
			sample.Values = nil
		}
		t.Run(sample.Name, func(t *testing.T) {
			got := run(t, sample)
			assert.Equal(t, sample.Code, got.Code, sample.Query)
			if sample.Rows != nil {
				expected, err := json.Marshal(sample.Rows)
				require.NoError(t, err)
				actual, err := json.Marshal(got.Rows)
				require.NoError(t, err)
				assert.JSONEq(t, string(expected), string(actual), sample.Query)
			}
			if len(sample.Values) == 0 {
				assert.Empty(t, got.Values, sample.Query)
			} else {
				assert.Equal(t, sample.Values, got.Values, sample.Query)
			}
		})
	}
}

func normalize(value any) any {
	switch v := value.(type) {
	case json.Number:
		if integer, err := v.Int64(); err == nil {
			return integer
		}
		floating, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			panic(err)
		}
		return floating
	case map[string]any:
		for key, item := range v {
			v[key] = normalize(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = normalize(item)
		}
		return v
	default:
		return value
	}
}

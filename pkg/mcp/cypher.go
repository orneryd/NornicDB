package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

const (
	cypherDefaultRows = 100
	cypherMaxRows     = 1000
	cypherMaxBytes    = 1 << 20
)

func getCypherTool(defaultDatabase string) Tool {
	schema, _ := json.Marshal(map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"database":   databaseParamSchema(defaultDatabase),
			"query":      map[string]interface{}{"type": "string", "minLength": 1, "description": "Native Cypher statement, with $name placeholders for bound parameters."},
			"parameters": map[string]interface{}{"type": "object", "additionalProperties": true, "description": "Bound statement parameters, including nested maps and lists."},
			"max_rows":   map[string]interface{}{"type": "integer", "minimum": 1, "maximum": cypherMaxRows, "default": cypherDefaultRows},
		},
		"required":             []string{"query"},
		"additionalProperties": false,
	})
	return Tool{
		Name:        ToolCypher,
		Description: "Execute parameterized native Cypher, including traversal and APOC, GDS and retrieval procedures, with the caller's database permissions and execution limits. Returns columns, rows, stats, metadata and explicit truncation (default 100 rows, maximum 1000 rows and 1 MiB of result JSON). Native retrieval paging uses its own n/qid/discard parameters; no generic query cursor is created. Use smaller native pages/projections when truncated. Example: query='CALL db.retrieve($request)', parameters={request:{query:'topic', where:'node.allowed = $allowed', limit:10}, allowed:true}.",
		InputSchema: schema,
	}
}

// cypherArgumentsFromJSON preserves integer precision and FLOAT/INTEGER types
// using the native HTTP/APOC number decoder. Other tools retain their existing
// argument decoding. The envelope has already passed JSON syntax validation.
func cypherArgumentsFromJSON(body []byte, rpc bool) map[string]interface{} {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var envelope map[string]interface{}
	_ = decoder.Decode(&envelope)
	if rpc {
		envelope, _ = envelope["params"].(map[string]interface{})
	}
	args, _ := envelope["arguments"].(map[string]interface{})
	cypher.JSONNumbers(args)
	return args
}

type cypherToolResult struct {
	Columns    []string               `json:"columns"`
	Rows       [][]interface{}        `json:"rows"`
	Stats      *cypher.QueryStats     `json:"stats"`
	Metadata   map[string]interface{} `json:"metadata"`
	Truncation cypherTruncation       `json:"truncation"`
}

type cypherTruncation struct {
	Truncated    bool     `json:"truncated"`
	Reasons      []string `json:"reasons"`
	ReturnedRows int      `json:"returned_rows"`
	MaxRows      int      `json:"max_rows"`
	MaxBytes     int      `json:"max_bytes"`
}

func cypherStructuredContent(result interface{}) interface{} {
	if native, ok := result.(*cypherToolResult); ok {
		return native
	}
	return nil
}

func (s *Server) handleCypher(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	for name := range args {
		switch name {
		case "database", "query", "parameters", "max_rows":
		default:
			return nil, fmt.Errorf("unknown cypher argument %q", name)
		}
	}
	query, ok := args["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query must be a non-empty string")
	}
	if database, present := args["database"]; present {
		if _, ok := database.(string); !ok {
			return nil, fmt.Errorf("database must be a string")
		}
	}
	var parameters map[string]interface{}
	if raw, present := args["parameters"]; present {
		parameters, ok = raw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("parameters must be an object")
		}
	}
	maxRows := cypherDefaultRows
	if raw, present := args["max_rows"]; present {
		switch value := raw.(type) {
		case int:
			maxRows = value
		case int64:
			if value < 1 || value > cypherMaxRows {
				return nil, fmt.Errorf("max_rows must be an integer from 1 to %d", cypherMaxRows)
			}
			maxRows = int(value)
		case float64:
			if value < 1 || value > cypherMaxRows || value != float64(int(value)) {
				return nil, fmt.Errorf("max_rows must be an integer from 1 to %d", cypherMaxRows)
			}
			maxRows = int(value)
		default:
			return nil, fmt.Errorf("max_rows must be an integer from 1 to %d", cypherMaxRows)
		}
		if maxRows < 1 || maxRows > cypherMaxRows {
			return nil, fmt.Errorf("max_rows must be an integer from 1 to %d", cypherMaxRows)
		}
	}
	executor, _, err := s.getExecutorAndGetNode(ctx)
	if err != nil {
		return nil, err
	}
	if executor == nil {
		return nil, fmt.Errorf("native Cypher executor unavailable")
	}
	result, err := executor.Execute(ctx, query, parameters)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("native Cypher executor returned no result")
	}
	return boundedCypherResult(ctx, result, maxRows)
}

func boundedCypherResult(ctx context.Context, native *cypher.ExecuteResult, maxRows int) (*cypherToolResult, error) {
	result := &cypherToolResult{
		Columns: native.Columns, Rows: make([][]interface{}, 0), Stats: native.Stats, Metadata: native.Metadata,
		Truncation: cypherTruncation{MaxRows: maxRows, MaxBytes: cypherMaxBytes},
	}
	if result.Columns == nil {
		result.Columns = []string{}
	}
	// Reserve the largest marker before adding whole rows. The final marker is
	// no larger; values and native metadata are never shortened or rewritten.
	result.Truncation.Reasons = []string{"database_max_results", "max_rows", "max_bytes"}
	result.Truncation.ReturnedRows = maxRows
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	remaining := cypherMaxBytes - len(encoded)
	if remaining < 0 {
		return nil, fmt.Errorf("Cypher columns and metadata exceed the %d byte result bound", cypherMaxBytes)
	}
	result.Truncation.Reasons = []string{}
	result.Truncation.ReturnedRows = 0
	if truncated, _ := native.Metadata["result_truncated"].(bool); truncated {
		result.Truncation.Reasons = append(result.Truncation.Reasons, "database_max_results")
	}
	if len(native.Rows) > maxRows {
		result.Truncation.Reasons = append(result.Truncation.Reasons, "max_rows")
	}
	for index, row := range native.Rows {
		if index >= maxRows {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		if len(encoded)+1 > remaining {
			result.Truncation.Reasons = append(result.Truncation.Reasons, "max_bytes")
			break
		}
		remaining -= len(encoded) + 1
		result.Rows = append(result.Rows, row)
	}
	result.Truncation.Truncated = len(result.Truncation.Reasons) > 0
	result.Truncation.ReturnedRows = len(result.Rows)
	return result, nil
}

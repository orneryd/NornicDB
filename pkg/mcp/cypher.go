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
	cypherDefaultRows          = 100
	cypherMaxRows              = 1000
	cypherMaxBytes             = 1 << 20
	cypherExecutionMemoryBytes = 16 << 20
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
		Description: "Execute parameterized native Cypher, including traversal and APOC, GDS and retrieval procedures, with the authenticated caller's database permissions and execution limits. Returns columns, rows, stats, metadata and explicit truncation (default 100 rows, maximum 1000 rows and 1 MiB for the complete MCP response). Native retrieval paging uses its own n/qid/discard parameters; no generic query cursor is created. Use smaller native pages/projections when truncated. Example: query='CALL db.retrieve($request)', parameters={request:{query:'topic', where:'node.allowed = $allowed', limit:10}, allowed:true}.",
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
	wire       []byte
}

type cypherTruncation struct {
	Truncated    bool     `json:"truncated"`
	Reasons      []string `json:"reasons"`
	ReturnedRows int      `json:"returned_rows"`
	MaxRows      int      `json:"max_rows"`
	MaxBytes     int      `json:"max_bytes"`
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
	budget := newCypherResultBudget(ctx, maxRows)
	stream := cypher.NewBoundedResultStream(budget.consume)
	executionCtx := cypher.WithResultStream(cypher.WithStatementMemoryLimit(ctx, cypherExecutionMemoryBytes), stream)
	result, err := executor.Execute(executionCtx, query, parameters)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("native Cypher executor returned no result")
	}
	return budget.finish(ctx, result)
}

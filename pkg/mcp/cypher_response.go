package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type cypherEnvelope struct {
	rpc bool
	id  interface{}
}
type cypherEnvelopeKey struct{}

func withCypherEnvelope(ctx context.Context, id interface{}) context.Context {
	return context.WithValue(ctx, cypherEnvelopeKey{}, cypherEnvelope{rpc: true, id: id})
}

// encode is the sole wire representation used for both admission accounting
// and the final response. It includes text escaping, structured duplication,
// the direct/JSON-RPC envelope and the terminating newline.
func (e cypherEnvelope) encode(result interface{}, failure string) ([]byte, error) {
	response := CallToolResponse{}
	if failure != "" {
		response.IsError = true
		response.Content = []Content{{Type: "text", Text: failure}}
	} else {
		encoded, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		response.Content = []Content{{Type: "text", Text: string(encoded)}}
		response.StructuredContent = result
	}
	var value interface{} = response
	if e.rpc {
		value = map[string]interface{}{"jsonrpc": "2.0", "id": e.id, "result": response}
	}
	encoded, err := json.Marshal(value)
	return append(encoded, '\n'), err
}

type cypherResultBudget struct {
	result      *cypherToolResult
	envelope    cypherEnvelope
	initialized bool
}

func newCypherResultBudget(ctx context.Context, maxRows int) *cypherResultBudget {
	envelope, _ := ctx.Value(cypherEnvelopeKey{}).(cypherEnvelope)
	return &cypherResultBudget{envelope: envelope, result: &cypherToolResult{
		Columns: []string{}, Rows: make([][]interface{}, 0),
		Truncation: cypherTruncation{Reasons: []string{}, MaxRows: maxRows, MaxBytes: cypherMaxBytes},
	}}
}

func (b *cypherResultBudget) reason(reason string) {
	for _, current := range b.result.Truncation.Reasons {
		if current == reason {
			return
		}
	}
	b.result.Truncation.Reasons = append(b.result.Truncation.Reasons, reason)
	b.result.Truncation.Truncated = true
}

func (b *cypherResultBudget) init(columns []string) error {
	if b.initialized {
		return nil
	}
	b.initialized = true
	converted, err := cypher.JSONTransportValue(columns, cypherMaxBytes/2)
	if err != nil {
		return err
	}
	// Columns are strings; validate their budget before keeping the slice.
	_ = converted
	if columns != nil {
		b.result.Columns = columns
	}
	marker := b.result.Truncation
	b.result.Truncation.Reasons = []string{"database_max_results", "max_rows", "max_bytes"}
	b.result.Truncation.ReturnedRows = b.result.Truncation.MaxRows
	encoded, err := b.envelope.encode(b.result, "")
	b.result.Truncation = marker
	if err != nil {
		return err
	}
	// Reserve native counters and the marker; arbitrary metadata is budgeted
	// at completion and can evict retained rows without retaining extra rows.
	if len(encoded)+1024 > cypherMaxBytes {
		return fmt.Errorf("Cypher columns and envelope exceed response byte limit")
	}
	return nil
}

func (b *cypherResultBudget) finish(ctx context.Context, native *cypher.ExecuteResult) (*cypherToolResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.init(native.Columns); err != nil {
		return nil, err
	}
	b.result.Rows = native.Rows
	b.result.Stats = native.Stats
	metadata, err := cypher.JSONTransportValue(native.Metadata, cypherMaxBytes/2)
	if err != nil {
		return nil, err
	}
	if native.Metadata != nil {
		b.result.Metadata = metadata.(map[string]interface{})
	}
	if truncated, _ := native.Metadata["result_truncated"].(bool); truncated {
		b.reason("database_max_results")
	}
	if output := resultstream.BudgetFromContext(ctx); output != nil {
		encoded, err := output.EncodeFinal(cypherMaxBytes, func() ([]byte, error) {
			b.result.Rows = output.Rows
			for _, reason := range output.Reasons {
				b.reason(reason)
			}
			b.result.Truncation.ReturnedRows = len(output.Rows)
			return b.envelope.encode(b.result, "")
		})
		if err != nil {
			return nil, err
		}
		b.result.wire = encoded
		return b.result, nil
	}
	{
		b.result.Truncation.ReturnedRows = len(b.result.Rows)
		encoded, err := b.envelope.encode(b.result, "")
		if err != nil {
			return nil, err
		}
		if len(encoded) <= cypherMaxBytes {
			b.result.wire = encoded
			return b.result, nil
		}
		return nil, fmt.Errorf("Cypher result and envelope exceed response byte limit")
	}
}

func (s *Server) writeCypherResponse(w http.ResponseWriter, ctx context.Context, result interface{}, err error) {
	var encoded []byte
	if native, ok := result.(*cypherToolResult); ok && err == nil {
		encoded = native.wire
	}
	if err != nil {
		envelope, _ := ctx.Value(cypherEnvelopeKey{}).(cypherEnvelope)
		message := s.renderError(ctx, err)
		if len(message) > 4096 {
			message = message[:4096] + " [error message truncated]"
		}
		encoded, _ = envelope.encode(nil, message)
		if len(encoded) > cypherMaxBytes {
			envelope.id = nil
			encoded, _ = envelope.encode(nil, "MCP request ID exceeds response byte limit")
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

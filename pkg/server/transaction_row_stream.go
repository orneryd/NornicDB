package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type transactionRowStreamKey struct{}

// transactionRowStream writes the ordinary transaction JSON incrementally.
// It reuses appendStatementResult for entity/temporal/REST conversion and the
// ordinary transaction runner for authorization, errors and commit/rollback.
type transactionRowStream struct {
	http.ResponseWriter
	server         *Server
	database       string
	statement      StatementRequest
	started, first bool
	err            error
	pull           bool
	demand         *json.Decoder
}

func (s *Server) withTransactionRowStream(w http.ResponseWriter, r *http.Request, database string) (http.ResponseWriter, *http.Request) {
	stream := &transactionRowStream{ResponseWriter: w, server: s, database: database, first: true}
	stream.pull = r.Header.Get(resultstream.StreamHeader) == "pull-v1"
	if stream.pull && r.ProtoMajor == 1 {
		stream.err = http.NewResponseController(w).EnableFullDuplex()
	}
	ctx := context.WithValue(r.Context(), transactionRowStreamKey{}, stream)
	ctx = cypher.WithStatementMemoryLimit(ctx, 16<<20)
	ctx = resultstream.WithConsumer(ctx, stream.consume)
	w.Header().Set(resultstream.StreamHeader, "1")
	return stream, r.WithContext(ctx)
}

func (s *transactionRowStream) write(data []byte) error {
	if s.err == nil {
		_, s.err = s.ResponseWriter.Write(data)
	}
	return s.err
}

func (s *transactionRowStream) consume(columns []string, row []any) (bool, error) {
	if s.pull {
		var credit int
		if err := s.demand.Decode(&credit); err != nil {
			return false, err
		}
		if credit != 1 {
			return false, fmt.Errorf("invalid row demand")
		}
	}
	if !s.started {
		s.started = true
		s.Header().Set("Content-Type", "application/json")
		s.ResponseWriter.WriteHeader(http.StatusOK)
		encoded, err := json.Marshal(columns)
		if err != nil {
			return false, err
		}
		if err := s.write(append(append([]byte(`{"results":[{"columns":`), encoded...), []byte(`,"data":[`)...)); err != nil {
			return false, err
		}
	}
	response := TransactionResponse{}
	s.server.appendStatementResult(&response, &cypher.ExecuteResult{Columns: columns, Rows: [][]any{row}}, s.database, false, s.statement.ResultDataContents)
	encoded, err := json.Marshal(response.Results[0].Data[0])
	if err != nil {
		return false, err
	}
	if !s.first {
		if err := s.write([]byte(",")); err != nil {
			return false, err
		}
	}
	s.first = false
	if err := s.write(encoded); err != nil {
		return false, err
	}
	// ResponseController unwraps middleware writers when they support Unwrap.
	// A small write still reaches the socket when a wrapper exposes Flush.
	if err := http.NewResponseController(s.ResponseWriter).Flush(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *transactionRowStream) writeJSON(status int, value interface{}) {
	if !s.started {
		s.Header().Set("Content-Type", "application/json")
		s.ResponseWriter.WriteHeader(status)
		s.err = json.NewEncoder(s.ResponseWriter).Encode(value)
		return
	}
	// The final object contains only the native summary: execution emitted
	// its rows already. Preserve all fields, including late native errors.
	encoded, err := json.Marshal(value)
	if err != nil {
		s.err = err
		return
	}
	var summary map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &summary); err != nil {
		s.err = err
		return
	}
	var results []map[string]json.RawMessage
	if err := json.Unmarshal(summary["results"], &results); err != nil {
		s.err = err
		return
	}
	_ = s.write([]byte("]"))
	if len(results) > 0 {
		delete(results[0], "columns")
		delete(results[0], "data")
		for key, value := range results[0] {
			name, _ := json.Marshal(key)
			_ = s.write(append(append(append([]byte(","), name...), ':'), value...))
		}
	}
	_ = s.write([]byte("}]"))
	delete(summary, "results")
	for key, value := range summary {
		name, _ := json.Marshal(key)
		_ = s.write(append(append(append([]byte(","), name...), ':'), value...))
	}
	_ = s.write([]byte("}\n"))
}

func configureTransactionRowStream(ctx context.Context, statements []StatementRequest) error {
	stream, _ := ctx.Value(transactionRowStreamKey{}).(*transactionRowStream)
	if stream == nil {
		return nil
	}
	if len(statements) != 1 {
		return fmt.Errorf("incremental transaction output requires one statement")
	}
	stream.statement = statements[0]
	return nil
}

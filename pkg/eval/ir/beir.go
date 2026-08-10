package ir

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Document is the logical BEIR corpus retrieval unit.
type Document struct {
	ID       string         `json:"_id"`
	Title    string         `json:"title"`
	Text     string         `json:"text"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Query is an official BEIR query.
type Query struct {
	ID       string         `json:"_id"`
	Text     string         `json:"text"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// QueryManifest records the exact query subset shared by all benchmark variants.
type QueryManifest struct {
	Dataset  string   `json:"dataset"`
	Split    string   `json:"split"`
	Seed     uint64   `json:"seed"`
	QueryIDs []string `json:"query_ids"`
	SHA256   string   `json:"sha256"`
}

// DocumentSink accepts a BEIR document while preserving its retrieval identity.
type DocumentSink interface {
	StoreDocument(context.Context, Document) error
}

// IngestStats reports the number of corpus documents accepted by a sink.
type IngestStats struct {
	Documents int64
}

// IngestDocuments streams corpus.jsonl into sink without retaining the corpus
// in memory. The sink is responsible for transactional batching and index life cycle.
func IngestDocuments(ctx context.Context, reader io.Reader, sink DocumentSink) (IngestStats, error) {
	if sink == nil {
		return IngestStats{}, fmt.Errorf("document sink must not be nil")
	}
	stats := IngestStats{}
	err := ReadDocuments(reader, func(document Document) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sink.StoreDocument(ctx, document); err != nil {
			return fmt.Errorf("store document %q: %w", document.ID, err)
		}
		stats.Documents++
		return nil
	})
	return stats, err
}

// ReadDocuments streams official BEIR corpus.jsonl entries to visit.
func ReadDocuments(reader io.Reader, visit func(Document) error) error {
	if visit == nil {
		return fmt.Errorf("document visitor must not be nil")
	}
	seen := make(map[string]struct{})
	return readJSONLines(reader, "corpus", func(line int, raw []byte) error {
		var document Document
		if err := json.Unmarshal(raw, &document); err != nil {
			return fmt.Errorf("corpus line %d: %w", line, err)
		}
		if strings.TrimSpace(document.ID) == "" {
			return fmt.Errorf("corpus line %d: missing _id", line)
		}
		if _, exists := seen[document.ID]; exists {
			return fmt.Errorf("corpus line %d: duplicate _id %q", line, document.ID)
		}
		seen[document.ID] = struct{}{}
		return visit(document)
	})
}

// ReadQueries loads official BEIR queries.jsonl and rejects duplicate IDs.
func ReadQueries(reader io.Reader) ([]Query, error) {
	queries := make([]Query, 0)
	seen := make(map[string]struct{})
	err := readJSONLines(reader, "queries", func(line int, raw []byte) error {
		var query Query
		if err := json.Unmarshal(raw, &query); err != nil {
			return fmt.Errorf("queries line %d: %w", line, err)
		}
		if strings.TrimSpace(query.ID) == "" || strings.TrimSpace(query.Text) == "" {
			return fmt.Errorf("queries line %d: missing _id or text", line)
		}
		if _, exists := seen[query.ID]; exists {
			return fmt.Errorf("queries line %d: duplicate _id %q", line, query.ID)
		}
		seen[query.ID] = struct{}{}
		queries = append(queries, query)
		return nil
	})
	return queries, err
}

// FilterQueriesWithQrels retains only queries that have relevance judgments.
func FilterQueriesWithQrels(queries []Query, qrels Qrels) []Query {
	filtered := make([]Query, 0, len(queries))
	for _, query := range queries {
		if _, exists := qrels[query.ID]; exists {
			filtered = append(filtered, query)
		}
	}
	return filtered
}

// NewQueryManifest deterministically selects query IDs and records their digest.
func NewQueryManifest(dataset, split string, queries []Query, limit int, seed uint64) (QueryManifest, error) {
	if strings.TrimSpace(dataset) == "" || strings.TrimSpace(split) == "" {
		return QueryManifest{}, fmt.Errorf("dataset and split must not be empty")
	}
	ids := make([]string, len(queries))
	for index, query := range queries {
		ids[index] = query.ID
	}
	selected, err := SelectQueryIDs(ids, limit, seed)
	if err != nil {
		return QueryManifest{}, err
	}
	digest := sha256.Sum256([]byte(strings.Join(selected, "\n") + "\n"))
	return QueryManifest{Dataset: dataset, Split: split, Seed: seed, QueryIDs: selected, SHA256: hex.EncodeToString(digest[:])}, nil
}

func readJSONLines(reader io.Reader, name string, visit func(int, []byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(strings.TrimSpace(string(raw))) == 0 {
			continue
		}
		if err := visit(line, raw); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s JSONL: %w", name, err)
	}
	return nil
}

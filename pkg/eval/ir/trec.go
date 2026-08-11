package ir

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Qrels maps each query ID to its graded relevance judgments.
type Qrels map[string]map[string]int

// RunResult is one ranked retrieval result expressed with its BEIR document ID.
type RunResult struct {
	DocumentID string
	Score      float64
}

// WriteRun writes a standard six-column TREC run. Results must already be in
// final rank order and use evaluation-facing BEIR document IDs.
func WriteRun(writer io.Writer, queryID string, results []RunResult, tag string) error {
	if strings.TrimSpace(queryID) == "" || strings.TrimSpace(tag) == "" {
		return fmt.Errorf("query ID and run tag must not be empty")
	}
	seen := make(map[string]struct{}, len(results))
	for index, result := range results {
		if strings.TrimSpace(result.DocumentID) == "" {
			return fmt.Errorf("result %d: document ID must not be empty", index)
		}
		if math.IsNaN(result.Score) || math.IsInf(result.Score, 0) {
			return fmt.Errorf("result %d: score must be finite", index)
		}
		if _, exists := seen[result.DocumentID]; exists {
			return fmt.Errorf("result %d: duplicate document ID %q", index, result.DocumentID)
		}
		seen[result.DocumentID] = struct{}{}
		if _, err := fmt.Fprintf(writer, "%s Q0 %s %d %.17g %s\n", queryID, result.DocumentID, index+1, result.Score, tag); err != nil {
			return err
		}
	}
	return nil
}

// ReadQrels parses the standard four-column TREC qrels format.
func ReadQrels(reader io.Reader) (Qrels, error) {
	qrels := make(Qrels)
	scanner := bufio.NewScanner(reader)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if line == 1 && len(fields) == 3 && fields[0] == "query-id" && fields[1] == "corpus-id" && fields[2] == "score" {
			continue
		}
		var queryID, documentID, gradeText string
		switch len(fields) {
		case 3: // Official BEIR TSV: query-id, corpus-id, score.
			queryID, documentID, gradeText = fields[0], fields[1], fields[2]
		case 4: // Standard TREC qrels: query-id, iteration, corpus-id, score.
			queryID, documentID, gradeText = fields[0], fields[2], fields[3]
		default:
			return nil, fmt.Errorf("qrels line %d: want 3 (BEIR) or 4 (TREC) fields, got %d", line, len(fields))
		}
		grade, err := strconv.Atoi(gradeText)
		if err != nil {
			return nil, fmt.Errorf("qrels line %d: invalid grade: %w", line, err)
		}
		if qrels[queryID] == nil {
			qrels[queryID] = make(map[string]int)
		}
		qrels[queryID][documentID] = grade
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return qrels, nil
}

// ReadRun parses a standard six-column TREC run file into ranked document IDs.
func ReadRun(reader io.Reader) (map[string][]string, error) {
	type entry struct {
		document string
		rank     int
		score    float64
	}
	entries := make(map[string][]entry)
	scanner := bufio.NewScanner(reader)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 6 {
			return nil, fmt.Errorf("run line %d: want 6 fields, got %d", line, len(fields))
		}
		rank, err := strconv.Atoi(fields[3])
		if err != nil || rank < 1 {
			return nil, fmt.Errorf("run line %d: invalid rank", line)
		}
		score, err := strconv.ParseFloat(fields[4], 64)
		if err != nil {
			return nil, fmt.Errorf("run line %d: invalid score: %w", line, err)
		}
		entries[fields[0]] = append(entries[fields[0]], entry{document: fields[2], rank: rank, score: score})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	run := make(map[string][]string, len(entries))
	for queryID, values := range entries {
		sort.Slice(values, func(left, right int) bool {
			if values[left].rank != values[right].rank {
				return values[left].rank < values[right].rank
			}
			if values[left].score != values[right].score {
				return values[left].score > values[right].score
			}
			return values[left].document < values[right].document
		})
		run[queryID] = make([]string, len(values))
		for index, value := range values {
			run[queryID][index] = value.document
		}
	}
	return run, nil
}

// Evaluate calculates macro-average metrics, counting each qrels query once.
func Evaluate(qrels Qrels, run map[string][]string) Metrics {
	if len(qrels) == 0 {
		return Metrics{}
	}
	total := Metrics{}
	for queryID, judgments := range qrels {
		metrics := Compute(run[queryID], judgments)
		total.RecallAt10 += metrics.RecallAt10
		total.RecallAt100 += metrics.RecallAt100
		total.NDCGAt10 += metrics.NDCGAt10
		total.MRRAt10 += metrics.MRRAt10
		total.MAPAt100 += metrics.MAPAt100
	}
	count := float64(len(qrels))
	return Metrics{RecallAt10: total.RecallAt10 / count, RecallAt100: total.RecallAt100 / count, NDCGAt10: total.NDCGAt10 / count, MRRAt10: total.MRRAt10 / count, MAPAt100: total.MAPAt100 / count}
}

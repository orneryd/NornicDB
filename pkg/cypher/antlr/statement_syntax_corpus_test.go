// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	goast "go/ast"
	goparser "go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// This census intentionally includes every string, including invalid statements
// and non-query data. It does not use a keyword filter to guess which literals
// are Cypher. Constant concatenations and JSON fixture strings are included.
// Runtime-generated inputs can be supplied as a JSON array or text map through
// NORNICDB_TEST_SYNTAX_CORPUS; that is input data, never a compiler overlay.
func statementSyntaxCorpus(t *testing.T) (map[string]string, int, int, int) {
	t.Helper()
	inputs := map[string]string{}
	files, concatenations, fixtures := 0, 0, 0
	add := func(source, location string) {
		if _, exists := inputs[source]; !exists {
			inputs[source] = location
		}
	}
	var constantText func(goast.Expr) (string, bool)
	constantText = func(expression goast.Expr) (string, bool) {
		switch expression := expression.(type) {
		case *goast.BasicLit:
			if expression.Kind == token.STRING {
				value, err := strconv.Unquote(expression.Value)
				return value, err == nil
			}
		case *goast.ParenExpr:
			return constantText(expression.X)
		case *goast.BinaryExpr:
			if expression.Op == token.ADD {
				left, a := constantText(expression.X)
				right, b := constantText(expression.Y)
				return left + right, a && b
			}
		}
		return "", false
	}
	var jsonStrings func(interface{}, string)
	jsonStrings = func(value interface{}, location string) {
		switch value := value.(type) {
		case string:
			add(value, location)
		case []interface{}:
			for _, item := range value {
				jsonStrings(item, location)
			}
		case map[string]interface{}:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				jsonStrings(value[key], location)
			}
		}
	}
	root := filepath.Clean("..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			files++
			positions := token.NewFileSet()
			file, err := goparser.ParseFile(positions, path, nil, 0)
			if err != nil {
				return err
			}
			goast.Inspect(file, func(node goast.Node) bool {
				if expression, ok := node.(goast.Expr); ok {
					if text, known := constantText(expression); known {
						add(text, positions.Position(expression.Pos()).String())
						if _, joined := expression.(*goast.BinaryExpr); joined {
							concatenations++
						}
					}
				}
				return true
			})
		} else if strings.HasSuffix(path, ".json") && strings.Contains(filepath.ToSlash(path), "/testdata/") {
			fixtures++
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var value interface{}
			if err := json.Unmarshal(data, &value); err != nil {
				return err
			}
			jsonStrings(value, path)
		} else if strings.HasSuffix(path, ".cypher") {
			fixtures++
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			add(string(data), path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("NORNICDB_TEST_SYNTAX_CORPUS"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value interface{}
		if err := json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		jsonStrings(value, path)
	}
	if files == 0 || len(inputs) == 0 {
		t.Fatal("empty Cypher corpus census")
	}
	return inputs, files, concatenations, fixtures
}

func TestStatementSyntaxCorpusPredictionParity(t *testing.T) {
	inputs, files, concatenations, fixtures := statementSyntaxCorpus(t)
	sources := make([]string, 0, len(inputs))
	for source := range inputs {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	census := sha256.New()
	valid, invalid, mismatches, inspections := 0, 0, 0, 0
	for index, source := range sources {
		fmt.Fprintf(census, "%d:%s", len(source), source)
		want, wantErr := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeLL)
		got, gotErr := parseStatementSyntax(context.Background(), cyphertext.NewSource(context.Background(), source, RawSource), StatementEntry, nil)
		pureSLL, pureSLLErr := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeSLL)
		wantMessage, gotMessage := "", ""
		if wantErr != nil {
			invalid++
			wantMessage = wantErr.Error()
		} else {
			valid++
		}
		if gotErr != nil {
			gotMessage = gotErr.Error()
		}
		pureSLLMessage := ""
		if pureSLLErr != nil {
			pureSLLMessage = pureSLLErr.Error()
		}
		if wantMessage != pureSLLMessage || !reflect.DeepEqual(want, pureSLL) {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("raw SLL prediction mismatch at %s sha256=%x LL error=%q SLL-first error=%q", inputs[source], sha256.Sum256([]byte(source)), wantMessage, pureSLLMessage)
			}
		}
		if wantMessage != gotMessage || !reflect.DeepEqual(want, got) {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("prediction mismatch at %s sha256=%x LL error=%q SLL-first error=%q", inputs[source], sha256.Sum256([]byte(source)), wantMessage, gotMessage)
			}
		}
		// Exercise the actual inspection owner, then its query consumer. It
		// must preserve full Script acceptance/errors while carrying a body
		// inspection through the existing independent LL body production.
		ctx := WithStatementSyntax(context.Background(), source)
		_, _ = InspectSyntax(ctx, source, false, RawSource)
		inspected, inspectionErr := QuerySyntax(ctx, source, false, RawSource)
		inspectionMessage := ""
		if inspectionErr != nil {
			inspectionMessage = inspectionErr.Error()
		}
		if wantMessage != inspectionMessage {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("inspection admission mismatch at %s sha256=%x LL error=%q inspected error=%q", inputs[source], sha256.Sum256([]byte(source)), wantMessage, inspectionMessage)
			}
		} else if inspectionErr == nil {
			expected := want
			if inspected.Root().Rule() == CypherParserRULE_subqueryBody {
				expected, inspectionErr = parseStatementSyntaxPrediction(context.Background(), source, ClauseEntry, nil, antlr4.PredictionModeLL)
			}
			inspections++
			if inspectionErr != nil || !reflect.DeepEqual(expected, inspected.data) {
				mismatches++
				if mismatches <= 10 {
					t.Errorf("inspection tree mismatch at %s sha256=%x reference error=%v", inputs[source], sha256.Sum256([]byte(source)), inspectionErr)
				}
			}
		}
		if (index+1)%1000 == 0 {
			t.Logf("corpus progress %d/%d valid=%d invalid=%d mismatches=%d", index+1, len(sources), valid, invalid, mismatches)
		}
	}
	t.Logf("CORPUS_PARITY files=%d JSON/cypher fixtures=%d constantConcatenations=%d uniqueInputs=%d valid=%d invalid=%d inspectionChecks=%d mismatches=%d censusSHA256=%x", files, fixtures, concatenations, len(sources), valid, invalid, inspections, mismatches, census.Sum(nil))
	if mismatches != 0 {
		t.Fail()
	}
}

// Freeze exactly the same input population for cross-revision diagnostics.
// Source bytes are base64 encoded before JSON, preserving invalid UTF-8 and
// embedded NULs from negative fixtures. This does not change the parity test.
func TestStatementSyntaxCorpusFrozenInputExport(t *testing.T) {
	if os.Getenv("NORNICDB_TEST_SYNTAX_CORPUS_EXPORT") != "1" {
		t.Skip("explicit diagnostic input export only")
	}
	inputs, files, concatenations, fixtures := statementSyntaxCorpus(t)
	sources := make([]string, 0, len(inputs))
	for source := range inputs {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	type input struct {
		SourceBase64 string `json:"sourceBase64"`
		Location     string `json:"location"`
	}
	values := make([]input, 0, len(sources))
	census := sha256.New()
	for _, source := range sources {
		fmt.Fprintf(census, "%d:%s", len(source), source)
		values = append(values, input{base64.StdEncoding.EncodeToString([]byte(source)), inputs[source]})
	}
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	packed := compressed.Bytes()
	const size = 32 << 10
	for start, index := 0, 0; start < len(packed); start, index = start+size, index+1 {
		end := min(start+size, len(packed))
		record, err := json.Marshal(map[string]any{"author": "Codex R17 implementation agent", "index": index, "bytes": len(packed), "sha256": fmt.Sprintf("%x", sha256.Sum256(packed)), "censusSHA256": fmt.Sprintf("%x", census.Sum(nil)), "inputs": len(values), "goFiles": files, "concatenations": concatenations, "fixtures": fixtures, "base64": base64.StdEncoding.EncodeToString(packed[start:end])})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("R17_CORPUS_FROZEN_CHUNK %s\n", record)
	}
}

// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"context"
	"crypto/sha256"
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
)

// This census intentionally includes every string, including invalid statements
// and non-query data. It does not use a keyword filter to guess which literals
// are Cypher. Constant concatenations and JSON fixture strings are included.
// Runtime-generated inputs can be supplied as a JSON array or text map through
// NORNICDB_TEST_SYNTAX_CORPUS; that is input data, never a compiler overlay.
func TestStatementSyntaxCorpusPredictionParity(t *testing.T) {
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
	sources := make([]string, 0, len(inputs))
	for source := range inputs {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	census := sha256.New()
	valid, invalid, mismatches := 0, 0, 0
	for index, source := range sources {
		fmt.Fprintf(census, "%d:%s", len(source), source)
		want, wantErr := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeLL)
		got, gotErr := parseStatementSyntax(context.Background(), source, StatementEntry, nil)
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
		if wantMessage != gotMessage || !reflect.DeepEqual(want, got) {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("prediction mismatch at %s sha256=%x LL error=%q SLL-first error=%q", inputs[source], sha256.Sum256([]byte(source)), wantMessage, gotMessage)
			}
		}
		if (index+1)%1000 == 0 {
			t.Logf("corpus progress %d/%d valid=%d invalid=%d mismatches=%d", index+1, len(sources), valid, invalid, mismatches)
		}
	}
	t.Logf("CORPUS_PARITY files=%d JSON/cypher fixtures=%d constantConcatenations=%d uniqueInputs=%d valid=%d invalid=%d mismatches=%d censusSHA256=%x", files, fixtures, concatenations, len(sources), valid, invalid, mismatches, census.Sum(nil))
	if mismatches != 0 {
		t.Fail()
	}
}

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

func TestPR1682PredictionCorpusParity(t *testing.T) {
	inputs, files, concatenations, fixtures := statementSyntaxCorpus(t)
	sources := make([]string, 0, len(inputs))
	for source := range inputs {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	census := sha256.New()
	failures := 0
	for i, source := range sources {
		fmt.Fprintf(census, "%d:%s", len(source), source)
		ctx := context.Background()
		projection := cyphertext.NewSource(ctx, source, RawSource)
		want, wantErr := parseStatementSyntaxWithMode(ctx, projection, StatementEntry, nil, antlr4.PredictionModeLL, false)
		got, gotErr := parseStatementSyntax(ctx, projection, StatementEntry, nil)
		wantMessage, gotMessage := "", ""
		if wantErr != nil {
			wantMessage = wantErr.Error()
		}
		if gotErr != nil {
			gotMessage = gotErr.Error()
		}
		if wantMessage != gotMessage || !sameSyntaxStructure(want, got) {
			failures++
			if failures <= 10 {
				t.Errorf("grammar mismatch at %s sha256=%x LL=%q SLL=%q", inputs[source], sha256.Sum256([]byte(source)), wantMessage, gotMessage)
			}
		}
		if (i+1)%1000 == 0 {
			t.Logf("parity progress %d/%d failures=%d", i+1, len(sources), failures)
		}
	}
	t.Logf("CORPUS_PARITY files=%d fixtures=%d concatenations=%d inputs=%d mismatches=%d sha256=%x", files, fixtures, concatenations, len(sources), failures, census.Sum(nil))
	if failures != 0 {
		t.Fail()
	}
}

func TestPR1682PredictionEntryParity(t *testing.T) {
	inputs := []string{"", "MATCH (n) RETURN n", "CALL (x) { RETURN x }", "USE graph.name RETURN 1", "WITH x RETURN x", "RETURN CASE WHEN x > 1 THEN CASE WHEN x = 2 THEN 'a' ELSE 'b' END ELSE 'c' END AS value", "x + 1", "(n:Thing)-[:R]->(m)", "RETURN '", "RETURN 1 /*", "RETURN $x"}
	entries := []SyntaxEntry{StatementEntry, FragmentEntry, ClauseEntry, ExpressionEntry, PatternEntry, CallEntry, UseEntry}
	for _, entry := range entries {
		for _, source := range inputs {
			t.Run(fmt.Sprintf("entry-%d/%x", entry, sha256.Sum256([]byte(source))), func(t *testing.T) {
				ctx := t.Context()
				projection := cyphertext.NewSource(ctx, source, RawSource)
				want, wantErr := parseStatementSyntaxWithMode(ctx, projection, entry, nil, antlr4.PredictionModeLL, false)
				got, gotErr := parseStatementSyntax(ctx, projection, entry, nil)
				wantMessage, gotMessage := "", ""
				if wantErr != nil {
					wantMessage = wantErr.Error()
				}
				if gotErr != nil {
					gotMessage = gotErr.Error()
				}
				if wantMessage != gotMessage || !sameSyntaxStructure(want, got) {
					t.Errorf("entry %d source %q LL=%q SLL=%q", entry, source, wantMessage, gotMessage)
				}
			})
		}
	}
}

// Independent parses must have different product identities. Compare every
// retained structural and diagnostic field while excluding only that process-
// local ownership number, which is separately checked by the identity tests.
func sameSyntaxStructure(want, got *statementSyntaxData) bool {
	if want == nil || got == nil {
		return want == got
	}
	wantCopy, gotCopy := *want, *got
	wantCopy.identity, gotCopy.identity = 0, 0
	return reflect.DeepEqual(wantCopy, gotCopy)
}

func TestPR1682PredictionStructureComparison(t *testing.T) {
	projection := cyphertext.NewSource(t.Context(), "RETURN 1", RawSource)
	want, err := parseStatementSyntax(t.Context(), projection, StatementEntry, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseStatementSyntax(t.Context(), projection, StatementEntry, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want.identity == got.identity || !sameSyntaxStructure(want, got) {
		t.Fatal("independent equal parses must have different identities and equal structure")
	}
	changed := *got
	changed.source += " "
	if sameSyntaxStructure(want, &changed) {
		t.Fatal("source differences must remain visible")
	}
	changed = *got
	changed.nodes = append([]syntaxNodeData(nil), got.nodes...)
	changed.nodes[0].end++
	if sameSyntaxStructure(want, &changed) {
		t.Fatal("node span differences must remain visible")
	}
	changed = *got
	changed.tokens = append([]syntaxTokenData(nil), got.tokens...)
	changed.tokens[0].kind++
	if sameSyntaxStructure(want, &changed) {
		t.Fatal("token differences must remain visible")
	}
}

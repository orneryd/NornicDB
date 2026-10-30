package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Each string is one grammar token: quoted strings and names stay opaque.
// Every adjacent pair is exercised with every trivia spelling, then all gaps
// together. A unique result alias prevents syntax-cache hits hiding validation.
func TestMapEntrySyntaxTriviaMatrix(t *testing.T) {
	const opaque = "/* { : } */ // CALL () `name`"
	const quoted = "'/* { : } */ // CALL () `name`'"
	seeds := []struct {
		name   string
		tokens []string
		want   interface{}
		write  bool
		bad    bool
	}{
		{"call_return", []string{"CALL", "{", "CREATE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "1", ",", "s", ":", quoted, "}", ")", "RETURN", "n", ".", "k", "AS", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), true, false},
		{"call_empty_import", []string{"CALL", "(", ")", "{", "CREATE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "1", ",", "s", ":", quoted, "}", ")", "RETURN", "n", ".", "k", "AS", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), true, false},
		{"call_import", []string{"WITH", "1", "AS", "x", "CALL", "(", "x", ")", "{", "CREATE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "x", ",", "s", ":", quoted, "}", ")", "RETURN", "n", ".", "k", "AS", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), true, false},
		{"call_unit", []string{"CALL", "(", ")", "{", "CREATE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "1", ",", "s", ":", quoted, "}", ")", "}", "RETURN", "1", "AS", "out"}, int64(1), true, false},
		{"nested_call", []string{"CALL", "{", "CALL", "(", ")", "{", "CREATE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "1", ",", "s", ":", quoted, "}", ")", "RETURN", "n", ".", "k", "AS", "k", "}", "RETURN", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), true, false},
		{"merge_return", []string{"CALL", "{", "MERGE", "(", "n", ":", "LexicalMatrix", "{", "k", ":", "1", ",", "s", ":", quoted, "}", ")", "RETURN", "n", ".", "k", "AS", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), true, false},
		{"empty_map", []string{"RETURN", "{", "}", "AS", "out"}, map[string]interface{}{}, false, false},
		{"keyword_keys", []string{"RETURN", "{", "`ca``ll`", ":", quoted, ",", "match", ":", "1", "}", "AS", "out"}, map[string]interface{}{"ca`ll": opaque, "match": int64(1)}, false, false},
		{"nested_map", []string{"RETURN", "{", "outer", ":", "{", "create", ":", "1", "}", ",", "call", ":", quoted, "}", "AS", "out"}, map[string]interface{}{"outer": map[string]interface{}{"create": int64(1)}, "call": opaque}, false, false},
		{"projection", []string{"WITH", "{", "match", ":", "1", ",", "call", ":", quoted, "}", "AS", "m", "RETURN", "m", "{", ".", "match", ",", ".", "*", ",", "create", ":", "2", "}", "AS", "out"}, map[string]interface{}{"match": int64(1), "call": opaque, "create": int64(2)}, false, false},
		{"variable_projection", []string{"WITH", "{", "}", "AS", "m", ",", "1", "AS", "create", "RETURN", "m", "{", "create", "}", "AS", "out"}, map[string]interface{}{"create": int64(1)}, false, false},
		{"keyword_projection", []string{"WITH", "{", "match", ":", "1", "}", "AS", "count", "RETURN", "count", "{", ".", "match", "}", "AS", "out"}, map[string]interface{}{"match": int64(1)}, false, false},
		{"quoted_projection", []string{"WITH", "{", "match", ":", "1", "}", "AS", "`m{literal}`", "RETURN", "`m{literal}`", "{", ".", "match", "}", "AS", "out"}, map[string]interface{}{"match": int64(1)}, false, false},
		{"comma_only", []string{"RETURN", "{", ",", "}", "AS", "out"}, nil, false, true},
		{"keyless_number", []string{"RETURN", "{", "1", "}", "AS", "out"}, nil, false, true},
		{"keyless_list", []string{"RETURN", "{", "[", "]", "}", "AS", "out"}, nil, false, true},
		{"keyless_map", []string{"RETURN", "{", "{", "}", "}", "AS", "out"}, nil, false, true},
	}
	trivia := []struct{ name, text string }{
		{"space", " "}, {"tab", "\t"}, {"newline", "\n"}, {"crlf", "\r\n"},
		{"block", " /* gap ' ` // { CALL () : } */ "},
		{"line", " // gap ' ` /* { CALL () : }\n"},
		{"mixed", " \t/* gap */\r\n// gap\n "},
	}
	for modeIndex, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			for seedIndex, seed := range seeds {
				t.Run(seed.name, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lexical_matrix"))
					ctx := context.Background()
					sequence := 0
					run := func(name string, gap int, separator string) {
						alias := fmt.Sprintf("lex_%d_%d_%d", modeIndex, seedIndex, sequence)
						sequence++
						var query strings.Builder
						for index, token := range seed.tokens {
							if index > 0 {
								if gap == -1 || gap == index-1 {
									query.WriteString(separator)
								} else {
									query.WriteByte(' ')
								}
							}
							if index == len(seed.tokens)-1 {
								query.WriteString(alias)
							} else {
								query.WriteString(token)
							}
						}
						t.Run(name, func(t *testing.T) {
							if mode == "explicit" {
								if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
									t.Fatal(err)
								}
								t.Cleanup(func() {
									if _, err := executor.Execute(ctx, "ROLLBACK", nil); err != nil {
										t.Error(err)
									}
								})
							} else if seed.write {
								if _, err := executor.Execute(ctx, "MATCH (n:LexicalMatrix) DETACH DELETE n", nil); err != nil {
									t.Fatal(err)
								}
							}
							// Check the raw owner too: Execute may normalize trivia.
							preflight := validateStaticMapKeys(query.String())
							if seed.bad && preflight == nil {
								t.Error("raw preflight accepted malformed map")
							}
							if !seed.bad && preflight != nil {
								t.Errorf("raw preflight: %v", preflight)
							}
							result, err := executor.Execute(ctx, query.String(), nil)
							if seed.bad {
								if err == nil {
									t.Fatal("expected malformed map syntax to fail")
								}
								code, _ := nornicerrors.Neo4jStatus(err)
								if code != "Neo.ClientError.Statement.SyntaxError" {
									t.Fatalf("status=%s: %v", code, err)
								}
								requireSemanticDetail(t, err, "UnexpectedSyntax")
								return
							}
							if err != nil {
								t.Fatalf("query=%s: %v", query.String(), err)
							}
							if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || !reflect.DeepEqual(result.Rows[0][0], seed.want) {
								t.Fatalf("rows=%#v want=%#v query=%s", result.Rows, seed.want, query.String())
							}
							if len(result.Columns) != 1 || result.Columns[0] != alias {
								t.Fatalf("columns=%v want=%s", result.Columns, alias)
							}
							if seed.write {
								stored, err := executor.Execute(ctx, "MATCH (n:LexicalMatrix) RETURN n.k, n.s", nil)
								if err != nil {
									t.Fatal(err)
								}
								if !reflect.DeepEqual(stored.Rows, [][]interface{}{{int64(1), opaque}}) {
									t.Fatalf("stored rows=%#v", stored.Rows)
								}
							}
						})
					}
					for _, gap := range trivia {
						run("all_"+gap.name, -1, gap.text)
						for boundary := 0; boundary+1 < len(seed.tokens); boundary++ {
							run(fmt.Sprintf("gap_%02d_%s", boundary, gap.name), boundary, gap.text)
						}
					}
				})
			}
		})
	}
}

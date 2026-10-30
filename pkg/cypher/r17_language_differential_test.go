// Author: Codex R17 implementation agent, 27 October 2026.
package cypher

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// This portable test consumes a frozen export from the committed corpus owner.
// Run the identical input bytes and test on the release and candidate, then
// compare every record. It never executes a query or mutates graph/catalog data.
// Canonical meaning here is the complete retained Script structure, decoded
// symbolic identities, source/token extents and structural effect/call facts.
// Whole-case execution and existing public tests remain separate obligations.
func TestR17LanguageDifferential(t *testing.T) {
	path := os.Getenv("NORNICDB_R17_LANGUAGE_CORPUS")
	if path == "" {
		t.Skip("explicit cross-revision corpus diagnostic only")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var inputs []struct {
		SourceBase64 string `json:"sourceBase64"`
		Location     string `json:"location"`
	}
	if err := json.NewDecoder(reader).Decode(&inputs); err != nil {
		t.Fatal(err)
	}
	if len(inputs) == 0 {
		t.Fatal("empty frozen language corpus")
	}
	previous := config.GetParserType()
	defer config.SetParserType(previous)
	census := sha256.New()
	for _, input := range inputs {
		text, err := base64.StdEncoding.DecodeString(input.SourceBase64)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(census, "%d:%s", len(text), text)
	}
	for _, backend := range []string{"nornic", "antlr"} {
		config.SetParserType(backend)
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		accepted := 0
		for index, input := range inputs {
			raw, err := base64.StdEncoding.DecodeString(input.SourceBase64)
			if err != nil {
				t.Fatal(err)
			}
			query := string(raw)
			ctx, source := cyphertext.WithSource(context.Background(), query)
			validation := source.Err
			if validation == nil {
				validation = executor.validateSyntax(ctx, source.Text, false, syntaxStatement)
			}
			meaning, grammarError := "", ""
			if validation == nil {
				accepted++
				syntax, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.StatementEntry, false, cypherantlr.RawSource)
				grammarError = r17ErrorBytes(err)
				if syntax.Valid() {
					meaning = r17CanonicalMeaning(syntax)
				}
			}
			// Prefix reading is a distinct public contract. Its source window,
			// decoded reference and exact rejection bytes must also remain equal.
			prefix, prefixErr := cypherantlr.ReadUseReference(ctx, source)
			prefixJSON, err := json.Marshal(prefix)
			if err != nil {
				t.Fatal(err)
			}
			var prefixPosition *cypherantlr.UseReferenceError
			errors.As(prefixErr, &prefixPosition)
			record, err := json.Marshal(map[string]any{
				"parser": backend, "index": index, "sourceSHA256": fmt.Sprintf("%x", sha256.Sum256(raw)),
				"accepted": validation == nil, "diagnosticBase64": r17ErrorBytes(validation),
				"canonicalMeaningSHA256": meaning, "canonicalGrammarDiagnosticBase64": grammarError,
				"prefixBase64": base64.StdEncoding.EncodeToString(prefixJSON), "prefixDiagnosticBase64": r17ErrorBytes(prefixErr),
				"prefixDiagnosticPosition": prefixPosition,
			})
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("R17_LANGUAGE_RECORD %s\n", record)
			if (index+1)%1000 == 0 {
				t.Logf("language progress parser=%s inputs=%d/%d", backend, index+1, len(inputs))
			}
		}
		fmt.Printf("R17_LANGUAGE_COMPLETE parser=%s inputs=%d accepted=%d rejected=%d censusSHA256=%x\n", backend, len(inputs), accepted, len(inputs)-accepted, census.Sum(nil))
	}
}

func r17ErrorBytes(err error) string {
	if err == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(err.Error()))
}

func r17MeaningText(out hash.Hash, text string) {
	fmt.Fprintf(out, "%d:", len(text))
	_, _ = out.Write([]byte(text))
}

func r17CanonicalMeaning(syntax cypherantlr.StatementSyntax) string {
	out := sha256.New()
	r17MeaningText(out, syntax.Source())
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		start, end := node.Span()
		fmt.Fprintf(out, "n:%d:%d:%d:%d;", node.Rule(), start, end, node.ChildCount())
		if name, ok := node.Name(); ok {
			r17MeaningText(out, name)
		}
		return nil
	})
	for index := 0; index < syntax.TokenCount(); index++ {
		token := syntax.Token(index)
		start, end := token.Span()
		fmt.Fprintf(out, "t:%d:%d:%d:%d;", token.Kind(), token.Channel(), start, end)
	}
	effects := syntax.Effects()
	fmt.Fprintf(out, "writes:%t;remoteReadOnly:%t;calls:%d;", effects.Writes(), effects.RemoteReadOnly(), effects.CallCount())
	for index := 0; index < effects.CallCount(); index++ {
		call := effects.Call(index)
		r17MeaningText(out, call.Name())
		r17MeaningText(out, call.Text())
	}
	return fmt.Sprintf("%x", out.Sum(nil))
}

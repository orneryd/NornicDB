package observability_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var credentialedClientErrorBoundaries = map[string]map[string]bool{
	"cmd/eval/main.go": {
		"*safeClient.Get": true, "*safeClient.Post": true, "*HTTPSearcher.Search": true,
	},
	"pkg/auth/oauth.go": {
		"*OAuthManager.ExchangeCode": true, "*OAuthManager.GetUserInfo": true, "*OAuthManager.RefreshOAuthToken": true,
	},
	"pkg/embed/embed.go": {
		"*OllamaEmbedder.Embed": true, "*OllamaEmbedder.EmbedBatch": true, "*OpenAIEmbedder.embedBatchRequest": true,
	},
	"pkg/heimdall/generator_chat_completion.go": {
		"*chatCompletionGenerator.Generate": true, "*chatCompletionGenerator.GenerateWithTools": true,
	},
	"pkg/heimdall/generator_ollama.go": {
		"*ollamaGenerator.GenerateStream": true,
	},
	"pkg/heimdall/generator_openai.go": {
		"*openAIGenerator.GenerateStream": true,
	},
	"pkg/search/rerank.go": {
		"*CrossEncoder.Rerank": true,
	},
	"pkg/storage/remote_engine.go": {
		"NewRemoteEngine":      true,
		"*boltTransport.query": true, "*boltTransport.queryBatch": true,
		"*boltTransport.close":        true,
		"*RemoteEngine.BeginCypherTx": true,
		"*remoteCypherTx.QueryCypher": true, "*remoteCypherTx.finish": true,
		"*httpTransport.doRequestToURL": true,
	},
	"pkg/voyage/client.go": {
		"*Client.postOnce": true,
	},
}

var configuredHTTPClientFiles = []string{
	"cmd/eval/main.go",
	"pkg/auth/oauth.go",
	"pkg/embed/embed.go",
	"pkg/embed/orca.go",
	"pkg/embed/voyage.go",
	"pkg/heimdall/generator_ollama.go",
	"pkg/heimdall/generator_openai.go",
	"pkg/search/rerank.go",
	"pkg/storage/remote_engine.go",
	"pkg/voyage/client.go",
}

func configuredBaseExpression(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		name := strings.ToLower(x.Name)
		return name == "apiurl" || name == "baseurl" || name == "serverurl" || name == "issuer" || name == "endpoint"
	case *ast.SelectorExpr:
		name := strings.ToLower(x.Sel.Name)
		return name == "apiurl" || name == "baseurl" || name == "issuer" || name == "url" || configuredBaseExpression(x.X)
	case *ast.CallExpr:
		for _, arg := range x.Args {
			if configuredBaseExpression(arg) {
				return true
			}
		}
	case *ast.BinaryExpr:
		return configuredBaseExpression(x.X) || configuredBaseExpression(x.Y)
	case *ast.ParenExpr:
		return configuredBaseExpression(x.X)
	}
	return false
}

func TestConfiguredHTTPClientsDoNotConcatenateRoutes(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, relative := range configuredHTTPClientFiles {
		path := filepath.Join(root, filepath.FromSlash(relative))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fileset := token.NewFileSet()
		file, err := parser.ParseFile(fileset, path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch x := node.(type) {
			case *ast.BinaryExpr:
				if x.Op == token.ADD && (configuredBaseExpression(x.X) || configuredBaseExpression(x.Y)) {
					t.Errorf("%s: configured request routes must use observability.ComposeRequestURL", fileset.Position(x.Pos()))
				}
			case *ast.CallExpr:
				selector, ok := x.Fun.(*ast.SelectorExpr)
				if !ok || len(x.Args) == 0 {
					return true
				}
				name := selector.Sel.Name
				if (name == "Sprintf" || name == "JoinPath") && configuredBaseExpression(x.Args[0]) {
					t.Errorf("%s: configured request routes must use observability.ComposeRequestURL", fileset.Position(x.Pos()))
				}
			}
			return true
		})
	}
}

func TestCredentialedHTTPClientErrorsUseBoundaryOwner(t *testing.T) {
	root := filepath.Join("..", "..")
	for relative, expected := range credentialedClientErrorBoundaries {
		path := filepath.Join(root, filepath.FromSlash(relative))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fileset := token.NewFileSet()
		file, err := parser.ParseFile(fileset, path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]bool{}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			identity := fn.Name.Name
			if fn.Recv != nil {
				identity = expressionText(fn.Recv.List[0].Type) + "." + fn.Name.Name
			}
			if !expected[identity] {
				continue
			}
			found[identity] = true
			if !namedErrorResult(fn) {
				t.Errorf("%s: credentialed client boundary %s must use a named error result", fileset.Position(fn.Pos()), identity)
				continue
			}
			if !deferredClientErrorOwner(fn) {
				t.Errorf("%s: credentialed client errors must pass through observability.RedactClientError", fileset.Position(fn.Pos()))
			}
		}
		for identity := range expected {
			if !found[identity] {
				t.Errorf("%s: stale credentialed client error boundary %s", relative, identity)
			}
		}
	}
}

func expressionText(node ast.Node) string {
	var output bytes.Buffer
	_ = format.Node(&output, token.NewFileSet(), node)
	return output.String()
}

func namedErrorResult(fn *ast.FuncDecl) bool {
	if fn.Type.Results == nil {
		return false
	}
	for _, result := range fn.Type.Results.List {
		if expressionText(result.Type) != "error" {
			continue
		}
		for _, name := range result.Names {
			if name.Name == "err" {
				return true
			}
		}
	}
	return false
}

func deferredClientErrorOwner(fn *ast.FuncDecl) bool {
	for _, statement := range fn.Body.List {
		deferStatement, ok := statement.(*ast.DeferStmt)
		if !ok {
			continue
		}
		owned := false
		ast.Inspect(deferStatement.Call, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				return true
			}
			left, ok := assignment.Lhs[0].(*ast.Ident)
			call, callOK := assignment.Rhs[0].(*ast.CallExpr)
			if !ok || !callOK || left.Name != "err" {
				return true
			}
			selector, selectorOK := call.Fun.(*ast.SelectorExpr)
			if selectorOK && selector.Sel.Name == "RedactClientError" {
				owned = true
			}
			return true
		})
		if owned {
			return true
		}
	}
	return false
}

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestReviewR7RemainingMechanisms(t *testing.T) {
	for _, q := range []string{
		"WITH 1 AS x WHERE x = 1 /* 2 users */ RETURN x",
		"WITH {`path\\`:1, `2 users`:2} AS m WHERE m.`path\\` = 1 RETURN m.`path\\`",
	} {
		t.Run(q, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r7_remaining"))
			got, err := e.Execute(context.Background(), q, nil)
			require.NoError(t, err, q)
			require.Equal(t, [][]interface{}{{int64(1)}}, got.Rows, q)
		})
	}
}
func TestReviewR7GuardRejectsPartialReader(t *testing.T) {
	// The private quote loop and identifier reader are the same combination
	// already present in hasUnexpectedIdentifierAfterNumber and hasRevealCall.
	source := `package cypher
 func partial(text string) {
  quote := byte(0)
  for i:=0; i<len(text); i++ {
   if quote != 0 { if text[i] == quote { quote=0 }; continue }
   if text[i] == '\'' || text[i] == '"' { quote=text[i]; continue }
   _,_,_ = scanIdentifierToken(text,i)
  }
 }`
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "partial.go", source, 0)
	require.NoError(t, err)
	body := f.Decls[0].(*ast.FuncDecl).Body
	require.NotEmpty(t, scannerQuoteMechanisms(fs, body))
	require.False(t, scannerUsesCanonicalQuoteReader("pkg/cypher/partial.go", body), "identifier parsing does not own the independent quote loop")
}

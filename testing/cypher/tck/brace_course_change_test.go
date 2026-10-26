package tck

import (
	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
	"testing"
)

func TestBraceCourseChangeBolt(t *testing.T) {
	driver, shutdown := startConformanceServer(t)
	defer shutdown()
	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		executor := newBoltDifferentialExecutor(t, driver, "nornic", mode)
		querytextmatrix.BraceCourseChangeCases(func(item querytextmatrix.MapBraceCase) {
			t.Run(item.Name+"/"+string(mode), func(t *testing.T) {
				actual := executor.Execute(t.Context(), item.Query)
				if item.SyntaxError {
					if actual.Code != "Neo.ClientError.Statement.SyntaxError" {
						t.Fatalf("wanted SyntaxError, got %+v", actual)
					}
				} else if actual.Failed() {
					t.Fatalf("valid projection: %+v", actual)
				}
			})
		})
	}
}

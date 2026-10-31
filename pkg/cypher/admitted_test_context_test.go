package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// admittedTestContext is the request admission a test performs when it calls
// an unexported transaction entry directly instead of Execute.
func admittedTestContext() context.Context {
	ctx, _ := storage.AdmitVectorWork(context.Background())
	return ctx
}

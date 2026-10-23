package cypher_test

import (
	"github.com/orneryd/nornicdb/internal/transactionalcalltest"
	"testing"
)

func TestTransactionalCallStructuralMatrix(t *testing.T) {
	transactionalcalltest.CheckExecute(t)
}

package transactionalcalltest_test

import (
	"github.com/orneryd/nornicdb/internal/transactionalcalltest"
	"testing"
)

// Exercise the fixture against the product, not against a copy of its oracle.
func TestTransactionalCallReferenceMatrix(t *testing.T) {
	transactionalcalltest.CheckExecute(t)
}

package bolt

import "github.com/orneryd/nornicdb/pkg/lifecycle"

// testTxOwner is the server lifetime a directly driven transaction lifecycle
// enrolls its timeout timer in.
func testTxOwner() *lifecycle.WorkLifetime { return &lifecycle.WorkLifetime{} }

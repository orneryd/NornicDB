package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// NOR-300 reproduction (PR #1699): a registered WRITE procedure must be a write
// to every consumer. kp.suppress is registered ProcedureModeWrite.
const nor300WriteCall = "MATCH (n:Doc) CALL kp.suppress(n) RETURN n"

func TestNOR300_RegisteredWriteProcedureQueryInfo(t *testing.T) {
	info := NewQueryAnalyzer(8).Analyze(context.Background(), nor300WriteCall)
	assert.False(t, info.IsReadOnly, "registered WRITE procedure classified read-only (cache/stream/OpType consumers)")
	assert.True(t, info.IsWriteQuery, "registered WRITE procedure not classified as write")
}

func TestNOR300_RegisteredWriteProcedureCallTail(t *testing.T) {
	assert.True(t, isPotentialWriteTail(context.Background(), "CALL kp.suppress(n) RETURN n"),
		"CALL tail with registered WRITE procedure takes the read-only tail fast paths")
}

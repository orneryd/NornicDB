// SPDX-License-Identifier: MIT
package cypher

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Current upstream preserves the conflict sentinel but localizes its English
// text to "conflict detected:". Bolt exposes that sentinel as typed Outdated.
// Retain the historical text fallback without accepting generic not-found.
func eshu6579DiagnosticWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	var wire *neo4j.Neo4jError
	if errors.As(err, &wire) && wire.Code == "Neo.TransientError.Transaction.Outdated" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "conflict:") && strings.Contains(msg, "changed after transaction start")
}

func TestEshu6579DiagnosticRetryContract(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"wrapped typed outdated", fmt.Errorf("wrapped: %w", &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.Outdated", Msg: "conflict detected: edge x changed after transaction start"}), true},
		{"legacy text", errors.New("commit failed: conflict: edge x changed after transaction start"), true},
		{"target syntax not found", &neo4j.Neo4jError{Code: "Neo.ClientError.Statement.SyntaxError", Msg: "UNWIND MATCH failed: not found"}, false},
		{"untyped outdated text", errors.New("Neo.TransientError.Transaction.Outdated"), false},
		{"unrelated", errors.New("permission denied"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := eshu6579DiagnosticWriteConflict(tc.err); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
	target := &neo4j.Neo4jError{Code: "Neo.ClientError.Statement.SyntaxError", Msg: "UNWIND MATCH failed: not found"}
	if eshu6579SharedSnapshotConflict(target, eshu6579Retract) || eshu6579SharedSnapshotConflict(target, eshu6579Upsert) {
		t.Fatal("target not-found must remain terminal")
	}
	update := &neo4j.Neo4jError{Code: "Neo.ClientError.Statement.SyntaxError", Msg: "UNWIND MERGE chain relationship update failed: not found"}
	if !eshu6579SharedSnapshotConflict(update, eshu6579Upsert) || eshu6579SharedSnapshotConflict(update, eshu6579Retract) {
		t.Fatal("known snapshot retry must remain MERGE-only")
	}
}

package bolt

import (
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A prior write pins B's snapshot before A commits the exact pattern B will
// MERGE. B must report a retryable conflict; a fresh retry matches A's entity.
// Exercise the node-producing routes through the public explicit Bolt API.
func TestMergeAdmissionFamilyAfterPriorWrite(t *testing.T) {
	for _, constraint := range []struct{ name, expression string }{
		{"scalar_unique", "k.id IS UNIQUE"},
		{"composite_unique", "(k.tenant, k.id) IS UNIQUE"},
		{"node_key", "(k.tenant, k.id) IS NODE KEY"},
	} {
		for _, route := range []struct{ name, query string }{
			{"node", "MERGE (k:AdmissionKey {tenant:7, id:42}) RETURN k.id"},
			{"unbound_path", "MERGE (k:AdmissionKey {tenant:7, id:42})-[:LINK]->(:AdmissionOther) RETURN k.id"},
			{"bound_start", "MATCH (a:AdmissionAnchor {id:0}) MERGE (a)-[:LINK]->(k:AdmissionKey {tenant:7, id:42}) RETURN k.id"},
			{"two_relationships", "MERGE (k:AdmissionKey {tenant:7, id:42})-[:LINK]->(:AdmissionMid)-[:LINK]->(:AdmissionEnd) RETURN k.id"},
			{"key_at_end", "MERGE (:AdmissionOther)-[:LINK]->(k:AdmissionKey {tenant:7, id:42}) RETURN k.id"},
			{"named_path", "MERGE p=(k:AdmissionKey {tenant:7, id:42})-[:LINK]->(:AdmissionOther) RETURN k.id"},
			{"unwind", "UNWIND [42] AS id MERGE (k:AdmissionKey {tenant:7, id:id})-[:LINK]->(:AdmissionOther) RETURN k.id"},
			{"subquery", "CALL { MERGE (k:AdmissionKey {tenant:7, id:42})-[:LINK]->(:AdmissionOther) RETURN k } RETURN k.id"},
		} {
			t.Run(constraint.name+"/"+route.name, func(t *testing.T) {
				base := storage.NewMemoryEngine()
				t.Cleanup(func() { _ = base.Close() })
				_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
				peer := openBoltTestConn(t, port)
				defer peer.Close()
				runBoltQueryAndCollectRecords(t, peer, "CREATE CONSTRAINT admission_key FOR (k:AdmissionKey) REQUIRE "+constraint.expression)
				runBoltQueryAndCollectRecords(t, peer, "CREATE (:AdmissionAnchor {id:0})")
				loser := openBoltTestConn(t, port)
				defer loser.Close()
				requireNoError(t, SendBegin(t, loser, nil))
				requireNoError(t, ReadSuccess(t, loser))
				runBoltQueryAndCollectRecords(t, loser, "CREATE (:AdmissionAudit {id:'loser'})")
				runBoltQueryAndCollectRecords(t, peer, route.query)
				code, message := runBoltQueryExpectFailure(t, loser, route.query)
				if code != "Neo.TransientError.Transaction.Outdated" {
					t.Errorf("same committed pattern: got %s (%s), want retryable Outdated", code, message)
				}
				runBoltQueryAndCollectRecords(t, peer, route.query)
				records := runBoltQueryAndCollectRecords(t, peer, "MATCH (k:AdmissionKey) RETURN count(k)")
				if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(1) {
					t.Errorf("fresh retry must match one key, got %v", records)
				}
			})
		}
	}
}

// Integers above 2^53 that differ only beyond float64 precision are distinct
// tuples, as in Neo4j. The generic composite index files both under one
// rounded key: B's MERGE must still find A's exact tuple behind the older
// adjacent one and report Outdated, and a fresh retry matches A's node.
func TestMergeAdmissionTupleIntegerPrecision(t *testing.T) {
	for _, kind := range []string{"UNIQUE", "NODE KEY"} {
		t.Run(kind, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "test"))
			peer := openBoltTestConn(t, port)
			defer peer.Close()
			runBoltQueryAndCollectRecords(t, peer, "CREATE CONSTRAINT precise_tuple FOR (k:PreciseTuple) REQUIRE (k.tenant, k.id) IS "+kind)
			older := map[string]any{"tenant": int64(7), "id": int64(9007199254740992)}
			params := map[string]any{"tenant": int64(7), "id": int64(9007199254740993)}
			runBoltQueryWithParams(t, peer, "CREATE (:PreciseTuple {tenant:$tenant, id:$id})", older)
			loser := openBoltTestConn(t, port)
			defer loser.Close()
			requireNoError(t, SendBegin(t, loser, nil))
			requireNoError(t, ReadSuccess(t, loser))
			runBoltQueryAndCollectRecords(t, loser, "CREATE (:AdmissionAudit {id:'loser'})")
			// The adjacent tuple is not a duplicate of the older one.
			runBoltQueryWithParams(t, peer, "CREATE (:PreciseTuple {tenant:$tenant, id:$id})", params)
			merge := "MERGE (k:PreciseTuple {tenant:$tenant, id:$id}) RETURN k.id"
			requireNoError(t, SendRun(t, loser, merge, params, nil))
			code, message, err := AssertFailure(t, loser)
			requireNoError(t, err)
			if code != "Neo.TransientError.Transaction.Outdated" {
				t.Errorf("same tuple behind an adjacent index candidate: got %s (%s), want retryable Outdated", code, message)
			}
			records := runBoltQueryWithParams(t, peer, merge, params)
			if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(9007199254740993) {
				t.Errorf("fresh retry must match the exact tuple, got %v", records)
			}
			records = runBoltQueryAndCollectRecords(t, peer, "MATCH (k:PreciseTuple) RETURN count(k)")
			if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(2) {
				t.Errorf("both adjacent tuples must exist once, got %v", records)
			}
		})
	}
}

func runBoltQueryWithParams(t *testing.T, conn net.Conn, query string, params map[string]any) [][]any {
	t.Helper()
	requireNoError(t, SendRun(t, conn, query, params, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	var records [][]any
	for {
		msgType, msgData, err := ReadMessage(conn)
		requireNoError(t, err)
		switch msgType {
		case MsgRecord:
			fields, _, err := decodePackStreamList(msgData, 0)
			requireNoError(t, err)
			records = append(records, fields)
		case MsgSuccess:
			return records
		default:
			t.Fatalf("unexpected Bolt message type 0x%02X for query %q", msgType, query)
		}
	}
}

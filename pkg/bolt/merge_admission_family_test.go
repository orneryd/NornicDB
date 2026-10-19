package bolt

import (
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

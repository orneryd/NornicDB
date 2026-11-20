package bolt

import (
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestNOR371WireMergeStagedBeforePeerCommitKeepsRetainedParameters is the
// public Bolt path for the successful-staging producer: the loser's RUN MERGE
// (key X, SET id Y) succeeds inside an explicit transaction before the peer
// commits id Y; the peer then commits Y, and the loser's COMMIT is classified
// with the parameters the session recorded at RUN.
//
// Over the wire the client's RUN params are encoded in the RUN message and
// decoded into a fresh, server-owned map per RUN. Nothing in the server mutates
// that map after RUN, so at COMMIT the classifier still reads key X / value Y,
// which is not retry-safe, and the public status is
// ConstraintValidationFailed. The reused-map answer (key Y / value Y reported
// retry-safe) is therefore not reachable through this transport; it is
// reachable only through the exported classifier with a caller-mutated map
// (pkg/cypher TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit).
// The peer can commit id Y here because the loser reserved only its MERGE key
// X; the reserved-key bound is covered in the cypher test.
func TestNOR371WireMergeStagedBeforePeerCommitKeepsRetainedParameters(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	_, port := startBoltIntegrationServer(t, storage.NewNamespacedEngine(base, "nor371wire"), boltIntegrationExplicitTx)

	setup := openBoltTestConn(t, port)
	defer setup.Close()
	runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT nor371_wire_doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)

	loser := openBoltTestConn(t, port)
	defer loser.Close()
	requireNoError(t, SendBegin(t, loser, nil))
	requireNoError(t, ReadSuccess(t, loser))
	runBoltQueryWithParams(t, loser, "MERGE (n:Doc {id: $key}) SET n.id = $value",
		map[string]any{"key": "X", "value": "Y"})

	peer := openBoltTestConn(t, port)
	defer peer.Close()
	runBoltQueryWithParams(t, peer, "MERGE (n:Doc {id: 'Y'}) ON CREATE SET n.owner = 'peer'", nil)

	requireNoError(t, SendCommit(t, loser))
	code, message, err := AssertFailure(t, loser)
	requireNoError(t, err)
	if code != "Neo.ClientError.Schema.ConstraintValidationFailed" {
		t.Fatalf("loser COMMIT with retained RUN params: got %s (%s), want hard ConstraintValidationFailed", code, message)
	}

	records := runBoltQueryWithParams(t, peer, "MATCH (n:Doc) RETURN n.id, n.owner ORDER BY n.id", nil)
	if len(records) != 1 || len(records[0]) != 2 || records[0][0] != "Y" || records[0][1] != "peer" {
		t.Fatalf("committed graph after the loser's failed COMMIT: got %v, want [[Y peer]]", records)
	}
}

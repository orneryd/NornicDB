package bolt

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestBoltExplicitTransactionsCreateIndependentSameLabelNodes(t *testing.T) {
	baseStore, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if baseStore != nil {
		fixtureowner.Own(t, baseStore)
	}
	if err != nil {
		t.Fatalf("create Badger store: %v", err)
	}
	t.Cleanup(func() { _ = baseStore.Close() })
	store := storage.NewNamespacedEngine(baseStore, "neo4j")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	first := openBoltTestConn(t, port)
	second := openBoltTestConn(t, port)
	for _, conn := range []net.Conn{first, second} {
		requireNoError(t, SendBegin(t, conn, nil))
		requireNoError(t, ReadSuccess(t, conn))
	}

	runBoltQueryAndCollectRecords(t, first, "CREATE (:Work {writer: 1})")
	runBoltQueryAndCollectRecords(t, second, "CREATE (:Work {writer: 2})")

	requireNoError(t, SendCommit(t, first))
	requireNoError(t, ReadSuccess(t, first))
	requireNoError(t, SendCommit(t, second))
	requireNoError(t, ReadSuccess(t, second))

	verify := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, verify, "MATCH (n:Work) RETURN count(n)")
	if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(2) {
		t.Fatalf("expected two committed Work nodes, got %v", records)
	}
}

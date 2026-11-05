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
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

	first := openBoltTestConn(t, port)
	second := openBoltTestConn(t, port)
	for _, conn := range []net.Conn{first, second} {
		requireNoError(t, SendBegin(t, conn, nil))
		requireNoError(t, ReadSuccess(t, conn))
	}

	runBoltQueryWithParams(t, first, "CREATE (:Work {writer: 1})", nil)
	runBoltQueryWithParams(t, second, "CREATE (:Work {writer: 2})", nil)

	requireNoError(t, SendCommit(t, first))
	requireNoError(t, ReadSuccess(t, first))
	requireNoError(t, SendCommit(t, second))
	requireNoError(t, ReadSuccess(t, second))

	verify := openBoltTestConn(t, port)
	records := runBoltQueryWithParams(t, verify, "MATCH (n:Work) RETURN count(n)", nil)
	if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(2) {
		t.Fatalf("expected two committed Work nodes, got %v", records)
	}
}

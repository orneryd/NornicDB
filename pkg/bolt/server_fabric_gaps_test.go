package bolt

import (
	"bufio"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestSessionGetExecutorForDatabaseUsesForwardedAuth(t *testing.T) {
	manager := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		},
		defaultDB: "nornic",
	}

	session := &Session{
		server:              &Server{dbManager: manager},
		forwardedAuthHeader: "Bearer forwarded-token",
	}

	if _, err := session.getExecutorForDatabase("nornic"); err != nil {
		t.Fatalf("getExecutorForDatabase failed: %v", err)
	}
	if manager.lastAuth != "Bearer forwarded-token" {
		t.Fatalf("expected auth-aware storage resolution, got %q", manager.lastAuth)
	}
}

func TestSessionGetExecutorForDatabase_CachesExecutorsWithoutForwardedAuth(t *testing.T) {
	manager := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		},
		defaultDB: "nornic",
	}

	session := &Session{
		server: &Server{
			dbManager: manager,
			executors: make(map[string]QueryExecutor),
		},
	}

	execA, err := session.getExecutorForDatabase("nornic")
	if err != nil {
		t.Fatalf("first getExecutorForDatabase failed: %v", err)
	}
	execB, err := session.getExecutorForDatabase("nornic")
	if err != nil {
		t.Fatalf("second getExecutorForDatabase failed: %v", err)
	}
	if execA != execB {
		t.Fatalf("expected cached executor reuse")
	}
	if manager.getCalls != 1 {
		t.Fatalf("expected one storage lookup with cache reuse, got %d", manager.getCalls)
	}
}

func TestSessionGetExecutorForDatabase_DoesNotCacheAuthScopedExecutors(t *testing.T) {
	manager := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		},
		defaultDB: "nornic",
	}

	session := &Session{
		server: &Server{
			dbManager: manager,
			executors: make(map[string]QueryExecutor),
		},
		forwardedAuthHeader: "Bearer forwarded-token",
	}

	execA, err := session.getExecutorForDatabase("nornic")
	if err != nil {
		t.Fatalf("first getExecutorForDatabase failed: %v", err)
	}
	execB, err := session.getExecutorForDatabase("nornic")
	if err != nil {
		t.Fatalf("second getExecutorForDatabase failed: %v", err)
	}
	if execA == execB {
		t.Fatalf("expected auth-scoped executor lookup to bypass cache")
	}
	if manager.getCalls != 2 {
		t.Fatalf("expected two storage lookups for auth-scoped routing, got %d", manager.getCalls)
	}
}

func TestHandleRunRejectsDatabaseSwitchInExplicitTransaction(t *testing.T) {
	manager := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic":       fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			"translations": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		},
		defaultDB: "nornic",
	}

	conn := &mockConn{}
	session := &Session{
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		server:        &Server{config: DefaultConfig(), dbManager: manager},
		baseExec:      &mockExecutor{},
		executor:      &mockExecutor{},
		database:      "nornic",
		authenticated: true,
		messageBuf:    make([]byte, 0, 4096),
	}

	beginData := encodePackStreamMap(map[string]any{"db": "translations"})
	if err := session.handleBegin(beginData); err != nil {
		t.Fatalf("handleBegin failed: %v", err)
	}
	manager.lastGetDB = ""

	runData := append(encodePackStreamString("RETURN 1 AS one"), 0xA0)
	runData = append(runData, encodePackStreamMap(map[string]any{"db": "nornic"})...)
	if err := session.handleRun(runData); err != nil {
		t.Fatalf("handleRun transport error: %v", err)
	}
	if err := session.writer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if manager.lastGetDB != "" {
		t.Fatalf("expected no db manager routing call on mismatched tx database, got %q", manager.lastGetDB)
	}
	if len(conn.writeData) == 0 {
		t.Fatalf("expected failure response to be written")
	}
}

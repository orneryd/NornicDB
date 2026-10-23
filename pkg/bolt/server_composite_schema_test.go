package bolt

import (
	"bufio"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// compositeAwareMockDBManager extends mockDBManager with ExistsOrIsConstituent
// so that constituentAwareExists recognizes dotted composite.alias names.
type compositeAwareMockDBManager struct {
	mockDBManager
	constituents map[string]bool // e.g. "translations.tr" -> true
}

func (m *compositeAwareMockDBManager) ExistsOrIsConstituent(name string) bool {
	if _, ok := m.stores[name]; ok {
		return true
	}
	if m.constituents != nil {
		return m.constituents[strings.ToLower(name)]
	}
	return false
}

func TestConstituentAwareExists_FallbackToExists(t *testing.T) {
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		},
		defaultDB: "nornic",
	}

	if !constituentAwareExists(mgr, "nornic") {
		t.Fatal("expected true for existing database")
	}
	if constituentAwareExists(mgr, "nonexistent") {
		t.Fatal("expected false for non-existent database")
	}
}

func TestConstituentAwareExists_WithConstituentResolver(t *testing.T) {
	mgr := &compositeAwareMockDBManager{
		mockDBManager: mockDBManager{
			stores: map[string]storage.Engine{
				"translations": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			},
			defaultDB: "translations",
		},
		constituents: map[string]bool{
			"translations.tr":  true,
			"translations.txt": true,
		},
	}

	if !constituentAwareExists(mgr, "translations") {
		t.Fatal("expected true for composite database")
	}
	if !constituentAwareExists(mgr, "translations.tr") {
		t.Fatal("expected true for dotted constituent reference")
	}
	if constituentAwareExists(mgr, "translations.missing") {
		t.Fatal("expected false for non-existent constituent")
	}
}

func TestBoltHandleHello_CompositeAliasNotRejected(t *testing.T) {
	mgr := &compositeAwareMockDBManager{
		mockDBManager: mockDBManager{
			stores: map[string]storage.Engine{
				"translations": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			},
			defaultDB: "translations",
		},
		constituents: map[string]bool{
			"translations.tr": true,
		},
	}

	conn := &mockConn{}
	session := &Session{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		server:     &Server{config: DefaultConfig(), dbManager: mgr},
		messageBuf: make([]byte, 0, 4096),
	}
	session.server.config.RequireAuth = false

	// Build a HELLO message with db: "translations.tr"
	helloData := encodePackStreamMap(map[string]any{
		"scheme":     "none",
		"db":         "translations.tr",
		"user_agent": "test/1.0",
	})

	err := session.handleHello(helloData)
	if err != nil {
		t.Fatalf("handleHello failed: %v", err)
	}

	if session.database != "translations.tr" {
		t.Fatalf("expected session database 'translations.tr', got %q", session.database)
	}

	// Verify no FAILURE was sent (flush and check output)
	_ = session.writer.Flush()
	output := string(conn.writeData)
	if strings.Contains(output, "DatabaseNotFound") {
		t.Fatal("expected no DatabaseNotFound error for composite.alias in HELLO")
	}
}

func TestBoltHandleRun_CompositeAliasExistenceCheckPasses(t *testing.T) {
	// Verify that handleRun resolves a dotted composite.alias database name
	// through the full flow: existence check → executor creation → query execution.
	// The mock provides a real MemoryEngine keyed by "translations.tr" so that
	// GetStorage succeeds and a real cypher.StorageExecutor is created.
	mgr := &compositeAwareMockDBManager{
		mockDBManager: mockDBManager{
			stores: map[string]storage.Engine{
				"nornic":          fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
				"translations":    fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
				"translations.tr": fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			},
			defaultDB: "nornic",
		},
		constituents: map[string]bool{
			"translations.tr": true,
		},
	}

	conn := &mockConn{}
	session := &Session{
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		server:        &Server{config: DefaultConfig(), dbManager: mgr},
		executor:      &mockExecutor{},
		baseExec:      &mockExecutor{},
		database:      "nornic",
		authenticated: true,
		messageBuf:    make([]byte, 0, 4096),
	}

	// Build RUN message: query="RETURN 1 AS one", params={}, metadata={db:"translations.tr"}
	runData := append(encodePackStreamString("RETURN 1 AS one"), 0xA0)
	runData = append(runData, encodePackStreamMap(map[string]any{"db": "translations.tr"})...)

	err := session.handleRun(runData)
	if err != nil {
		t.Fatalf("handleRun returned transport error: %v", err)
	}
	if err := session.writer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	output := string(conn.writeData)

	// Must not contain any failure indicators.
	if strings.Contains(output, "does not exist") {
		t.Fatal("handleRun rejected composite.alias with 'does not exist'")
	}
	if strings.Contains(output, "DatabaseNotFound") {
		t.Fatal("handleRun rejected composite.alias with DatabaseNotFound")
	}

	// The query should have produced a result — verify the session has it stored.
	if session.lastResult == nil {
		t.Fatal("expected lastResult to be set after successful RUN")
	}
	if len(session.lastResult.Columns) == 0 {
		t.Fatal("expected non-empty columns in result")
	}
}

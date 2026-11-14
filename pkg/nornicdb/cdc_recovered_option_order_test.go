package nornicdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replayOrderStore is one process lifetime of the crash-copy fixture: the DB
// Open returns and, when the server would have created one, the database
// manager over it. The server creates the manager only after Open returns, so
// a lifetime without one is the raw start-up path whose journal replay this
// test observes.
type replayOrderStore struct {
	db      *DB
	manager *multidb.DatabaseManager
	writer  *cypher.StorageExecutor
	closed  bool
}

// replayOrderConfig is the writer's durability mode: durable synchronous
// commits and no automatic snapshot, so the later writes stay in the journal.
func replayOrderConfig() *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALAutoCompactionEnabled = false
	return cfg
}

// openReplayOrderStore opens dir and nothing else: no database manager exists
// before Open returns, so start-up loads the store's persisted capture modes
// and replays its journal exactly as a restart does.
func openReplayOrderStore(t *testing.T, dir string) *replayOrderStore {
	t.Helper()
	db, err := Open(dir, replayOrderConfig())
	require.NoError(t, err)
	store := &replayOrderStore{db: db}
	t.Cleanup(func() { store.close(t) })
	return store
}

// startManager creates the database manager over store, as the server does
// after Open returns, and the managed executor its statements run on.
func (s *replayOrderStore) startManager(t *testing.T) {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(t.Context(), s.db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	s.manager = manager
	engine, err := manager.GetStorage(s.db.defaultDatabaseName())
	require.NoError(t, err)
	s.writer = cypher.NewStorageExecutor(engine)
}

// setEnrichment sets the database's persisted capture option through the
// manager, which is the option's one writer.
func (s *replayOrderStore) setEnrichment(t *testing.T, mode string) {
	t.Helper()
	require.NoError(t, s.manager.SetDatabaseOption(t.Context(), s.db.defaultDatabaseName(), multidb.OptionTxLogEnrichment, mode))
}

// enrichment is the database's persisted capture option.
func (s *replayOrderStore) enrichment(t *testing.T) string {
	t.Helper()
	options, err := s.manager.DatabaseOptions(s.db.defaultDatabaseName())
	require.NoError(t, err)
	return options[multidb.OptionTxLogEnrichment]
}

// close releases the lifetime's owners and can be called again by the test's
// cleanup.
func (s *replayOrderStore) close(t *testing.T) {
	t.Helper()
	if s == nil || s.closed {
		return
	}
	s.closed = true
	if s.db != nil {
		require.NoError(t, s.db.Close())
	}
	if s.manager != nil {
		require.NoError(t, s.manager.Close())
	}
}

// copyReplayOrderStore copies dir's durable state into a crash copy: exactly
// what the store held when the copy was taken, without the journal commits the
// caller splices in afterwards.
func copyReplayOrderStore(t *testing.T, dir string) string {
	t.Helper()
	crashCopy := t.TempDir()
	require.NoError(t, os.CopyFS(crashCopy, os.DirFS(dir)))
	return crashCopy
}

// spliceReplayOrderJournal replaces the crash copy's journal with dir's, which
// holds the commits the crash copy's store never applied.
func spliceReplayOrderJournal(t *testing.T, dir, crashCopy string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(filepath.Join(crashCopy, "wal")))
	require.NoError(t, os.CopyFS(filepath.Join(crashCopy, "wal"), os.DirFS(filepath.Join(dir, "wal"))))
}

// replayOrderFeed reads the feed after cursor: the metadata and event of every
// change the statement lists.
func replayOrderFeed(t *testing.T, db *DB, cursor string) []map[string]interface{} {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(),
		"CALL db.cdc.query($from) YIELD metadata, event RETURN metadata, event",
		map[string]any{"from": cursor})
	require.NoError(t, err)
	rows := make([]map[string]interface{}, len(result.Rows))
	for index, row := range result.Rows {
		require.Len(t, row, 2)
		metadata, ok := row[0].(map[string]interface{})
		require.True(t, ok, "metadata %#v", row[0])
		event, ok := row[1].(map[string]interface{})
		require.True(t, ok, "event %#v", row[1])
		rows[index] = map[string]interface{}{"metadata": metadata, "event": event}
	}
	return rows
}

// replayOrderAfterProperties is a node change's after state properties.
func replayOrderAfterProperties(t *testing.T, row map[string]interface{}) map[string]interface{} {
	t.Helper()
	event := row["event"].(map[string]interface{})
	state, ok := event["state"].(map[string]interface{})
	require.True(t, ok, "state %#v", event["state"])
	after, ok := state["after"].(map[string]interface{})
	require.True(t, ok, "after %#v", state["after"])
	properties, ok := after["properties"].(map[string]interface{})
	require.True(t, ok, "after properties %#v", after["properties"])
	return properties
}

// replayOrderCaptureModes is every row's captureMode, in feed order: the mode
// the commit that published the change was captured in.
func replayOrderCaptureModes(t *testing.T, rows []map[string]interface{}) []string {
	t.Helper()
	modes := make([]string, len(rows))
	for index, row := range rows {
		mode, ok := row["metadata"].(map[string]interface{})["captureMode"].(string)
		require.True(t, ok, "captureMode %#v", row["metadata"].(map[string]interface{})["captureMode"])
		modes[index] = mode
	}
	return modes
}

// replayOrderAfterStates is every row's after state properties, in feed order.
func replayOrderAfterStates(t *testing.T, rows []map[string]interface{}) []map[string]interface{} {
	t.Helper()
	states := make([]map[string]interface{}, len(rows))
	for index, row := range rows {
		states[index] = replayOrderAfterProperties(t, row)
	}
	return states
}

// replayOrderUnmanagedCreate runs one statement through the DB surface a raw
// Open exposes, before any database manager exists.
func replayOrderUnmanagedCreate(t *testing.T, db *DB, statement string) {
	t.Helper()
	_, err := db.ExecuteCypher(t.Context(), statement, nil)
	require.NoError(t, err)
}

// replayOrderUnmanagedWrite is replayOrderUnmanagedCreate with the feed rows
// that statement captured.
func replayOrderUnmanagedWrite(t *testing.T, db *DB, statement string) []map[string]interface{} {
	t.Helper()
	cursor := cdcCurrent(t, db)
	replayOrderUnmanagedCreate(t, db, statement)
	return replayOrderFeed(t, db, cursor)
}

// replayOrderCapturedEvents is every change event the store holds for the
// default database, whatever capture mode it is in now: the log itself, which
// the database's CDC procedures refuse to read when its mode is OFF.
func replayOrderCapturedEvents(t *testing.T, db *DB) []storage.ChangeEvent {
	t.Helper()
	capture, ok := storage.ChangeCaptureOf(db.GetBaseStorageForManager())
	require.True(t, ok, "the store captures changes")
	events, err := capture.ChangeEvents(db.defaultDatabaseName(), 0, 0, -1)
	require.NoError(t, err)
	return events
}

// replayOrderStoredNode is the stored K node with this name, as the statement
// returns it: the properties the recovery left in the store.
func replayOrderStoredNode(t *testing.T, db *DB, name string) map[string]interface{} {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), "MATCH (n:K {name: $name}) RETURN properties(n) AS properties", map[string]any{"name": name})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "the store holds one %q node", name)
	require.Len(t, result.Rows[0], 1)
	node, ok := result.Rows[0][0].(map[string]interface{})
	require.True(t, ok, "node properties %#v", result.Rows[0][0])
	return node
}

// #1810: the crash copy holds the store's durable state from before a database
// option change and a graph change, and the journal that holds both. Start-up
// loads the capture modes from the physical catalog, which still holds the
// option before the change, and then replays the journal: the option's catalog
// record first, so the graph change replayed after it must be captured in the
// option that record sets, not in the mode the physical catalog still held when
// Open read it. The change must be one FULL event with the node's whole after
// state, the option must be persisted, the statements this raw Open serves must
// use that same recovered FULL mode before the manager exists, and a second
// restart must not replay the change again.
func TestRecoveredCommitUsesCatalogOptionReplayOrder(t *testing.T) {
	root := t.TempDir()

	// The writer's database: durable, DIFF, one node whose change it records
	// the feed position after.
	baseline := openReplayOrderStore(t, root)
	baseline.startManager(t)
	baseline.setEnrichment(t, "DIFF")
	_, err := baseline.writer.Execute(t.Context(), "CREATE (:K {name: 'before', kept: 'unchanged'})", nil)
	require.NoError(t, err)
	cursor := cdcCurrent(t, baseline.db)
	baseline.close(t)

	// The state a crash between journal and store write leaves: the store
	// without the later writes, and the journal that holds them.
	crashCopy := copyReplayOrderStore(t, root)

	source := openReplayOrderStore(t, root)
	source.startManager(t)
	source.setEnrichment(t, "FULL")
	_, err = source.writer.Execute(t.Context(), "MATCH (n:K {name: 'before'}) SET n.name = 'after'", nil)
	require.NoError(t, err)
	source.close(t)
	spliceReplayOrderJournal(t, root, crashCopy)

	// Raw start-up of the crashed copy: the manager the server creates after
	// Open does not exist yet, exactly as on the restart that runs the journal.
	recovered := openReplayOrderStore(t, crashCopy)
	rows := replayOrderFeed(t, recovered.db, cursor)
	require.Len(t, rows, 1, "the replayed change is in the feed exactly once")
	// Both facts of the captured change are this test's contract, so both are
	// reported from the one run: the mode it was captured in, and the after
	// state that mode writes.
	assert.Equal(t, []string{"FULL"}, replayOrderCaptureModes(t, rows),
		"the change replayed after the option's catalog record is captured in FULL")
	assert.Equal(t, []map[string]interface{}{{"name": "after", "kept": "unchanged"}}, replayOrderAfterStates(t, rows),
		"FULL captures the node's whole after state")

	// The replayed catalog record is also the runtime mode of the statements
	// this raw Open serves: a managed-free statement captures in FULL.
	unmanaged := replayOrderUnmanagedWrite(t, recovered.db, "CREATE (:K {name: 'unmanaged', kept: 'unchanged'})")
	assert.Equal(t, []string{"FULL"}, replayOrderCaptureModes(t, unmanaged),
		"a statement of this raw Open captures in the recovered FULL mode")
	assert.Equal(t, []map[string]interface{}{{"name": "unmanaged", "kept": "unchanged"}}, replayOrderAfterStates(t, unmanaged))

	// The replayed catalog record is also the database's persisted option.
	recovered.startManager(t)
	require.Equal(t, "FULL", recovered.enrichment(t), "the replayed catalog record persists the option")
	require.Equal(t, int64(2), countNodes(t, recovered.db, "MATCH (n:K) RETURN count(n)"))
	recovered.close(t)

	// A second restart must not replay the change again.
	restarted := openReplayOrderStore(t, crashCopy)
	rows = replayOrderFeed(t, restarted.db, cursor)
	require.Len(t, rows, 2, "a second restart does not duplicate the recovered change")
	require.Equal(t, []string{"FULL", "FULL"}, replayOrderCaptureModes(t, rows))
	require.Equal(t, []map[string]interface{}{
		{"name": "after", "kept": "unchanged"},
		{"name": "unmanaged", "kept": "unchanged"},
	}, replayOrderAfterStates(t, rows))
	require.Equal(t, int64(2), countNodes(t, restarted.db, "MATCH (n:K) RETURN count(n)"))
	require.Equal(t, map[string]interface{}{"name": "after", "kept": "unchanged"}, replayOrderStoredNode(t, restarted.db, "after"))
}

// #1810: the crash copy's store still holds DIFF and its journal holds three
// commits: a graph change admitted in DIFF, the option's record setting FULL,
// and a graph change admitted in FULL. Start-up loads DIFF from the physical
// catalog and replays the journal in order, so the earlier change must be
// captured in DIFF, which the physical catalog also holds, and the later change
// in FULL, which only its own record decided. The DIFF event holds the changed
// property alone and the FULL event the node's whole after state; the recovered
// catalog then serves the statements of this raw Open, and a second restart
// repeats the same feed without duplicating it.
func TestRecoveredCommitKeepsRecordedCaptureModeBeforeOption(t *testing.T) {
	root := t.TempDir()

	baseline := openReplayOrderStore(t, root)
	baseline.startManager(t)
	baseline.setEnrichment(t, "DIFF")
	_, err := baseline.writer.Execute(t.Context(), "CREATE (:K {name: 'before', kept: 'unchanged'})", nil)
	require.NoError(t, err)
	cursor := cdcCurrent(t, baseline.db)
	baseline.close(t)
	crashCopy := copyReplayOrderStore(t, root)

	// The writer's later lifetime: a change in DIFF, the option set to FULL,
	// then a change in FULL, all journaled after the crash copy was taken.
	source := openReplayOrderStore(t, root)
	source.startManager(t)
	_, err = source.writer.Execute(t.Context(), "MATCH (n:K {name: 'before'}) SET n.name = 'earlier'", nil)
	require.NoError(t, err)
	source.setEnrichment(t, "FULL")
	_, err = source.writer.Execute(t.Context(), "MATCH (n:K {name: 'earlier'}) SET n.name = 'after'", nil)
	require.NoError(t, err)
	source.close(t)
	spliceReplayOrderJournal(t, root, crashCopy)

	// Raw start-up of the crashed copy, before the server's manager exists.
	recovered := openReplayOrderStore(t, crashCopy)
	rows := replayOrderFeed(t, recovered.db, cursor)
	require.Len(t, rows, 2, "each replayed change is in the feed exactly once")
	assert.Equal(t, []string{"DIFF", "FULL"}, replayOrderCaptureModes(t, rows),
		"each replayed change is captured in the mode its own record decided")
	assert.Equal(t, []map[string]interface{}{
		{"name": "earlier"},
		{"name": "after", "kept": "unchanged"},
	}, replayOrderAfterStates(t, rows),
		"the earlier change is DIFF's changed property and the later one FULL's whole after state")

	// The recovered catalog serves this raw Open's statements too.
	unmanaged := replayOrderUnmanagedWrite(t, recovered.db, "CREATE (:K {name: 'unmanaged', kept: 'unchanged'})")
	assert.Equal(t, []string{"FULL"}, replayOrderCaptureModes(t, unmanaged),
		"a statement of this raw Open captures in the recovered FULL mode")
	assert.Equal(t, []map[string]interface{}{{"name": "unmanaged", "kept": "unchanged"}}, replayOrderAfterStates(t, unmanaged))

	recovered.startManager(t)
	require.Equal(t, "FULL", recovered.enrichment(t), "the replayed catalog record persists the option")
	require.Equal(t, map[string]interface{}{"name": "after", "kept": "unchanged"}, replayOrderStoredNode(t, recovered.db, "after"))
	require.Equal(t, int64(2), countNodes(t, recovered.db, "MATCH (n:K) RETURN count(n)"))
	recovered.close(t)

	restarted := openReplayOrderStore(t, crashCopy)
	rows = replayOrderFeed(t, restarted.db, cursor)
	require.Len(t, rows, 3, "a second restart does not duplicate a recovered change")
	require.Equal(t, []string{"DIFF", "FULL", "FULL"}, replayOrderCaptureModes(t, rows))
	require.Equal(t, []map[string]interface{}{
		{"name": "earlier"},
		{"name": "after", "kept": "unchanged"},
		{"name": "unmanaged", "kept": "unchanged"},
	}, replayOrderAfterStates(t, rows))
	require.Equal(t, int64(2), countNodes(t, restarted.db, "MATCH (n:K) RETURN count(n)"))
	require.Equal(t, map[string]interface{}{"name": "after", "kept": "unchanged"}, replayOrderStoredNode(t, restarted.db, "after"))
}

// #1810: the crash copy's store holds FULL and its journal holds the option's
// record setting OFF followed by a graph change admitted in OFF. Start-up loads
// FULL from the physical catalog, replays the option record and then the graph
// change, which must not be captured: the record of the commit that published
// it decided OFF, while the mode Open read from the physical catalog was FULL.
// The recovered catalog then serves this raw Open's statements in OFF too, and
// a second restart repeats the same empty log with the recovered node stored.
func TestRecoveredCommitKeepsRecordedOffAfterReplay(t *testing.T) {
	root := t.TempDir()

	baseline := openReplayOrderStore(t, root)
	baseline.startManager(t)
	baseline.setEnrichment(t, "FULL")
	_, err := baseline.writer.Execute(t.Context(), "CREATE (:K {name: 'before', kept: 'unchanged'})", nil)
	require.NoError(t, err)
	baseline.close(t)
	crashCopy := copyReplayOrderStore(t, root)

	source := openReplayOrderStore(t, root)
	source.startManager(t)
	source.setEnrichment(t, "OFF")
	_, err = source.writer.Execute(t.Context(), "CREATE (:K {name: 'uncaptured', kept: 'unchanged'})", nil)
	require.NoError(t, err)
	source.close(t)
	spliceReplayOrderJournal(t, root, crashCopy)

	// Raw start-up of the crashed copy: the journal's change is applied and
	// its change event is suppressed. The database's CDC procedures are off in
	// OFF mode, so the log the capture writes is read directly: it holds the
	// baseline's one event and nothing of the replayed change.
	recovered := openReplayOrderStore(t, crashCopy)
	require.Equal(t, int64(2), countNodes(t, recovered.db, "MATCH (n:K) RETURN count(n)"), "the replayed change is stored")
	events := replayOrderCapturedEvents(t, recovered.db)
	require.Len(t, events, 1, "the replayed change is not captured: its record decided OFF")
	require.Equal(t, storage.ChangeCaptureFull, events[0].Mode)
	require.Equal(t, "before", events[0].After.Properties["name"])

	// The recovered catalog serves this raw Open's statements in OFF.
	replayOrderUnmanagedCreate(t, recovered.db, "CREATE (:K {name: 'also-uncaptured', kept: 'unchanged'})")
	require.Len(t, replayOrderCapturedEvents(t, recovered.db), 1,
		"a statement of this raw Open captures nothing in the recovered OFF mode")

	recovered.startManager(t)
	require.Equal(t, "OFF", recovered.enrichment(t), "the replayed catalog record persists the option")
	require.Equal(t, map[string]interface{}{"name": "uncaptured", "kept": "unchanged"}, replayOrderStoredNode(t, recovered.db, "uncaptured"))
	require.Equal(t, map[string]interface{}{"name": "also-uncaptured", "kept": "unchanged"}, replayOrderStoredNode(t, recovered.db, "also-uncaptured"))
	require.Equal(t, int64(3), countNodes(t, recovered.db, "MATCH (n:K) RETURN count(n)"))
	recovered.close(t)

	restarted := openReplayOrderStore(t, crashCopy)
	require.Len(t, replayOrderCapturedEvents(t, restarted.db), 1,
		"a second restart does not capture a change its commit did not")
	require.Equal(t, int64(3), countNodes(t, restarted.db, "MATCH (n:K) RETURN count(n)"))
	require.Equal(t, map[string]interface{}{"name": "uncaptured", "kept": "unchanged"}, replayOrderStoredNode(t, restarted.db, "uncaptured"))
	require.Equal(t, map[string]interface{}{"name": "also-uncaptured", "kept": "unchanged"}, replayOrderStoredNode(t, restarted.db, "also-uncaptured"))
}

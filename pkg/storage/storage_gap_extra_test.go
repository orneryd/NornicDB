package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type temporalStringer string

func (t temporalStringer) String() string { return string(t) }

type lookupEngine struct {
	Engine
	ids []NodeID
	err error
}

func (l *lookupEngine) ForEachNodeIDByLabel(_ string, visit func(NodeID) bool) error {
	if l.err != nil {
		return l.err
	}
	for _, id := range l.ids {
		if !visit(id) {
			break
		}
	}
	return nil
}

type captureLogger struct {
	entries []map[string]any
}

type exportableOnlyEngine struct {
	Engine
	allNodes []*Node
	allEdges []*Edge
	nodeErr  error
	edgeErr  error
}

type constraintValidationEngine struct {
	Engine
	nodes []*Node
	err   error
}

func (l *captureLogger) Log(level string, msg string, fields map[string]any) {
	entry := map[string]any{
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		entry[k] = v
	}
	l.entries = append(l.entries, entry)
}

func (e *exportableOnlyEngine) AllNodes() ([]*Node, error) {
	if e.nodeErr != nil {
		return nil, e.nodeErr
	}
	return e.allNodes, nil
}

func (e *exportableOnlyEngine) AllEdges() ([]*Edge, error) {
	if e.edgeErr != nil {
		return nil, e.edgeErr
	}
	return e.allEdges, nil
}

func (e *constraintValidationEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.nodes, nil
}

func newIsolatedBadgerEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestTemporalHelpers(t *testing.T) {
	now := time.Date(2026, 3, 4, 12, 34, 56, 0, time.UTC)

	t.Run("coerce temporal values", func(t *testing.T) {
		ptr := now
		tests := []struct {
			name  string
			value interface{}
			want  time.Time
			ok    bool
		}{
			{"time", now, now, true},
			{"time pointer", &ptr, now, true},
			{"nil time pointer", (*time.Time)(nil), time.Time{}, false},
			{"rfc3339 string", now.Format(time.RFC3339), now, true},
			{"stringer", temporalStringer(now.Format(time.RFC3339Nano)), now, true},
			{"int64 unix", now.Unix(), now, true},
			{"int unix", int(now.Unix()), now, true},
			{"float64 unix", float64(now.Unix()), now, true},
			{"unsupported", []byte("bad"), time.Time{}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, ok := coerceTemporalTime(tt.value)
				assert.Equal(t, tt.ok, ok)
				if tt.ok {
					assert.True(t, got.Equal(tt.want.UTC()), "got=%s want=%s", got, tt.want.UTC())
				}
			})
		}
	})

	t.Run("parse temporal string variants", func(t *testing.T) {
		cases := []string{
			now.Format(time.RFC3339Nano),
			now.Format(time.RFC3339),
			now.Format("2006-01-02T15:04:05"),
			now.Format("2006-01-02 15:04:05"),
			now.Format("2006-01-02"),
		}
		for _, raw := range cases {
			parsed, ok := parseTemporalString(raw)
			require.True(t, ok, raw)
			require.False(t, parsed.IsZero())
		}
		_, ok := parseTemporalString(" ")
		require.False(t, ok)
		_, ok = parseTemporalString("not-a-time")
		require.False(t, ok)
	})

	t.Run("interval overlap", func(t *testing.T) {
		base := temporalInterval{start: now, end: now.Add(2 * time.Hour), hasEnd: true, nodeID: "a"}
		overlap := temporalInterval{start: now.Add(time.Hour), end: now.Add(3 * time.Hour), hasEnd: true, nodeID: "b"}
		disjoint := temporalInterval{start: now.Add(3 * time.Hour), end: now.Add(4 * time.Hour), hasEnd: true, nodeID: "c"}
		openEnded := temporalInterval{start: now.Add(time.Hour), hasEnd: false, nodeID: "d"}

		assert.True(t, intervalsOverlap(base, overlap))
		assert.False(t, intervalsOverlap(base, disjoint))
		assert.True(t, intervalsOverlap(base, openEnded))
		assert.False(t, intervalsOverlap(temporalInterval{}, overlap))
	})
}

func TestLabelNodeIDLookupHelpers(t *testing.T) {
	t.Run("invalid engine", func(t *testing.T) {
		_, err := FirstNodeIDByLabel(nil, "Person")
		require.ErrorIs(t, err, ErrInvalidData)

		ids, err := NodeIDsByLabel(nil, "Person", 1)
		require.ErrorIs(t, err, ErrInvalidData)
		require.Nil(t, ids)
	})

	t.Run("lookup engine path", func(t *testing.T) {
		base := NewMemoryEngine()
		engine := &lookupEngine{Engine: base, ids: []NodeID{"n1", "n2", "n3"}}

		id, err := FirstNodeIDByLabel(engine, "Person")
		require.NoError(t, err)
		require.Equal(t, NodeID("n1"), id)

		ids, err := NodeIDsByLabel(engine, "Person", 2)
		require.NoError(t, err)
		require.Equal(t, []NodeID{"n1", "n2"}, ids)
	})

	t.Run("lookup engine errors and empty", func(t *testing.T) {
		wantErr := errors.New("boom")
		engine := &lookupEngine{Engine: NewMemoryEngine(), err: wantErr}
		_, err := FirstNodeIDByLabel(engine, "Person")
		require.ErrorIs(t, err, wantErr)

		engine = &lookupEngine{Engine: NewMemoryEngine()}
		_, err = FirstNodeIDByLabel(engine, "Person")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("fallback engine path", func(t *testing.T) {
		engine := NewNamespacedEngine(NewMemoryEngine(), "test")
		_, err := engine.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)

		id, err := FirstNodeIDByLabel(engine, "Person")
		require.NoError(t, err)
		require.NotEmpty(t, id)

		ids, err := NodeIDsByLabel(engine, "Person", 1)
		require.NoError(t, err)
		require.Len(t, ids, 1)

		allIDs, err := NodeIDsByLabel(engine, "Missing", 0)
		require.NoError(t, err)
		require.Empty(t, allIDs)
	})
}

func TestNamespaceAndLoggerHelpers(t *testing.T) {
	t.Run("namespace prefix parsing", func(t *testing.T) {
		prefix, ok := namespacePrefixFromID("system:config")
		require.True(t, ok)
		require.Equal(t, "system:", prefix)

		for _, id := range []string{"", "plain", ":leading"} {
			prefix, ok = namespacePrefixFromID(id)
			require.False(t, ok, id)
			require.Empty(t, prefix)
		}

		require.True(t, isSystemNamespaceID("system:user"))
		require.False(t, isSystemNamespaceID("tenant:user"))
		require.False(t, isSystemNamespaceID("invalid"))
	})

	t.Run("default wal logger writes through slog adapter", func(t *testing.T) {
		// Phase 2 LOG-01: defaultWALLogger no longer routes through the
		// stdlib log printer; it now wraps a *slog.Logger via newSlogWALLogger.
		// This test asserts the slog adapter contract by giving the WAL
		// logger an in-memory JSON slog handler and verifying the level/
		// msg/attrs land in the JSON record.
		var buf bytes.Buffer
		jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
		walLogger := newSlogWALLogger(slog.New(jsonHandler))

		walLogger.Log("info", "structured", map[string]any{"seq": 7})
		out := buf.String()
		require.Contains(t, out, `"level":"INFO"`)
		require.Contains(t, out, `"msg":"structured"`)
		require.Contains(t, out, `"seq":7`)
		require.Contains(t, out, `"subsystem":"wal"`)

		buf.Reset()
		walLogger.Log("error", "structured-error", map[string]any{"reason": "checksum"})
		out = buf.String()
		require.Contains(t, out, `"level":"ERROR"`)
		require.Contains(t, out, `"msg":"structured-error"`)
		require.Contains(t, out, `"reason":"checksum"`)

		// The defaultWALLogger{} value type still satisfies the WALLogger
		// interface (kept as a safety net for older callers); it routes
		// records to a discard slog handler so existing call sites that
		// build defaultWALLogger{} literals stay compileable.
		var legacy WALLogger = defaultWALLogger{}
		legacy.Log("info", "no-panic-on-discard", map[string]any{"k": "v"})
	})
}

func TestSchemaDefinitionPersistenceHelpers(t *testing.T) {
	t.Run("export and replace round trip", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, sm.AddConstraint(Constraint{Name: "user_name_exists", Type: ConstraintExists, Label: "User", Properties: []string{"name"}}))
		require.NoError(t, sm.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger))
		require.NoError(t, sm.AddPropertyIndex("user_name_idx", "User", []string{"name"}))
		require.NoError(t, sm.AddCompositeIndex("user_loc_idx", "User", []string{"country", "city"}))
		require.NoError(t, sm.AddFulltextIndex("user_search_idx", []string{"User"}, []string{"bio"}))
		require.NoError(t, sm.AddVectorIndex("user_embedding_idx", "User", "embedding", 3, "cosine"))
		require.NoError(t, sm.AddRangeIndex("user_age_range", "User", "age"))

		def := sm.ExportDefinition()
		require.NotNil(t, def)
		require.Equal(t, schemaDefinitionVersion, def.Version)
		require.Len(t, def.Constraints, 2)
		require.Len(t, def.PropertyTypeConstraints, 1)
		require.Len(t, def.PropertyIndexes, 1)
		require.Len(t, def.CompositeIndexes, 1)
		require.Len(t, def.FulltextIndexes, 1)
		require.Len(t, def.VectorIndexes, 1)
		require.Len(t, def.RangeIndexes, 1)

		restored := NewSchemaManager()
		require.NoError(t, restored.ReplaceFromDefinition(def))

		constraints := restored.GetConstraints()
		require.Len(t, constraints, 1)
		require.NoError(t, restored.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		restored.RegisterUniqueValue("User", "email", "alice@example.com", "node-1")
		require.Error(t, restored.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		_, ok := restored.GetCompositeIndex("user_loc_idx")
		require.True(t, ok)
		_, ok = restored.GetFulltextIndex("user_search_idx")
		require.True(t, ok)
		_, ok = restored.GetVectorIndex("user_embedding_idx")
		require.True(t, ok)
	})

	t.Run("replace nil definition noops", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.ReplaceFromDefinition(nil))
	})

	t.Run("export definition is sorted and deep copied", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.AddConstraint(Constraint{
			Name:       "b_exists",
			Type:       ConstraintExists,
			Label:      "User",
			Properties: []string{"name"},
		}))
		require.NoError(t, sm.AddUniqueConstraint("a_unique", "User", "email"))
		require.NoError(t, sm.AddPropertyTypeConstraint("b_type", "User", "age", PropertyTypeInteger))
		require.NoError(t, sm.AddPropertyTypeConstraint("a_type", "Account", "age", PropertyTypeInteger))
		require.NoError(t, sm.AddPropertyIndex("b_prop_idx", "User", []string{"name"}))
		require.NoError(t, sm.AddPropertyIndex("a_prop_idx", "Account", []string{"name"}))
		require.NoError(t, sm.AddCompositeIndex("b_comp_idx", "User", []string{"city", "country"}))
		require.NoError(t, sm.AddCompositeIndex("a_comp_idx", "Account", []string{"tenant", "email"}))
		require.NoError(t, sm.AddFulltextIndex("z_fulltext", []string{"User"}, []string{"bio", "title"}))
		require.NoError(t, sm.AddFulltextIndex("a_fulltext", []string{"Account"}, []string{"notes"}))
		require.NoError(t, sm.AddVectorIndex("z_vector", "User", "embedding", 3, "cosine"))
		require.NoError(t, sm.AddVectorIndex("a_vector", "Account", "embedding", 4, "euclidean"))
		require.NoError(t, sm.AddRangeIndex("z_range", "User", "age"))
		require.NoError(t, sm.AddRangeIndex("a_range", "Account", "score"))

		def := sm.ExportDefinition()
		require.NotNil(t, def)
		require.Len(t, def.Constraints, 2)
		require.Equal(t, "a_unique", def.Constraints[0].Name)
		require.Equal(t, "b_exists", def.Constraints[1].Name)
		require.Len(t, def.PropertyTypeConstraints, 2)
		require.Equal(t, "a_type", def.PropertyTypeConstraints[0].Name)
		require.Equal(t, "b_type", def.PropertyTypeConstraints[1].Name)
		require.Equal(t, "a_prop_idx", def.PropertyIndexes[0].Name)
		require.Equal(t, "b_prop_idx", def.PropertyIndexes[1].Name)
		require.Equal(t, "a_comp_idx", def.CompositeIndexes[0].Name)
		require.Equal(t, "b_comp_idx", def.CompositeIndexes[1].Name)
		require.Equal(t, "a_fulltext", def.FulltextIndexes[0].Name)
		require.Equal(t, "z_fulltext", def.FulltextIndexes[1].Name)
		require.Equal(t, "a_vector", def.VectorIndexes[0].Name)
		require.Equal(t, "z_vector", def.VectorIndexes[1].Name)
		require.Equal(t, "a_range", def.RangeIndexes[0].Name)
		require.Equal(t, "z_range", def.RangeIndexes[1].Name)

		def.Constraints[0].Properties[0] = "mutated"
		def.PropertyIndexes[0].Properties[0] = "mutated"
		def.CompositeIndexes[0].Properties[0] = "mutated"
		def.FulltextIndexes[0].Labels[0] = "Mutated"
		def.FulltextIndexes[0].Properties[0] = "mutated"

		fresh := sm.ExportDefinition()
		require.Equal(t, "email", fresh.Constraints[0].Properties[0])
		require.Equal(t, "name", fresh.PropertyIndexes[0].Properties[0])
		require.Equal(t, "tenant", fresh.CompositeIndexes[0].Properties[0])
		require.Equal(t, "Account", fresh.FulltextIndexes[0].Labels[0])
		require.Equal(t, "notes", fresh.FulltextIndexes[0].Properties[0])
	})
}

func TestBadgerSchemaHelpers(t *testing.T) {
	t.Run("parse schema namespace from key", func(t *testing.T) {
		ns, ok := parseSchemaNamespaceFromKey(schemaKey("alpha"))
		require.True(t, ok)
		require.Equal(t, "alpha", ns)

		_, ok = parseSchemaNamespaceFromKey([]byte{prefixSchema})
		require.False(t, ok)
		_, ok = parseSchemaNamespaceFromKey([]byte{prefixNode, 'a', 0})
		require.False(t, ok)
		_, ok = parseSchemaNamespaceFromKey([]byte{prefixSchema, 0})
		require.False(t, ok)
	})

	t.Run("persist and load schema definitions", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		def := &SchemaDefinition{
			Constraints: []Constraint{{
				Name:       "user_email",
				Type:       ConstraintUnique,
				Label:      "User",
				Properties: []string{"email"},
			}},
			PropertyIndexes: []SchemaPropertyIndexDef{{
				Name:       "user_name_idx",
				Label:      "User",
				Properties: []string{"name"},
			}},
		}
		require.NoError(t, engine.persistSchemaDefinition("testns", def))

		engine.schemasMu.Lock()
		engine.schemas = make(map[string]*SchemaManager)
		engine.schemasMu.Unlock()

		require.NoError(t, engine.loadPersistedSchemas())

		sm := engine.GetSchemaForNamespace("testns")
		require.NotNil(t, sm)
		require.Len(t, sm.GetConstraints(), 1)
		require.Len(t, sm.GetIndexes(), 1)
	})

	t.Run("persist schema validation errors", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		require.Error(t, engine.persistSchemaDefinition("", &SchemaDefinition{}))
		require.Error(t, engine.persistSchemaDefinition("ns", nil))
	})

	t.Run("load persisted schema invalid key and invalid json", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, txn.Set([]byte{prefixSchema, 0}, []byte(`{}`)))
			return nil
		}))
		require.Error(t, engine.loadPersistedSchemas())

		engine2 := newIsolatedBadgerEngine(t)
		require.NoError(t, engine2.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, txn.Set(schemaKey("broken"), []byte(`{not-json`)))
			return nil
		}))
		require.Error(t, engine2.loadPersistedSchemas())
	})

	t.Run("rebuild unique constraint values", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		node1 := &Node{ID: "nornic:u1", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "alice@example.com"}}
		node2 := &Node{ID: "nornic:u2", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "bob@example.com"}}
		_, err := engine.CreateNode(node1)
		require.NoError(t, err)
		_, err = engine.CreateNode(node2)
		require.NoError(t, err)

		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, engine.rebuildUniqueConstraintValues("nornic", sm))
		require.Error(t, sm.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		require.NoError(t, engine.rebuildUniqueConstraintValues("", sm))
		require.NoError(t, engine.rebuildUniqueConstraintValues("nornic", nil))
	})

	t.Run("get schema default and cached namespace", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		defaultSchema := engine.GetSchemaForNamespace("")
		require.NotNil(t, defaultSchema)
		require.Same(t, defaultSchema, engine.GetSchemaForNamespace("nornic"))

		custom := engine.GetSchemaForNamespace("custom")
		require.Same(t, custom, engine.GetSchemaForNamespace("custom"))
	})
}

func TestWALDiagnosticsHelpers(t *testing.T) {
	t.Run("backup corrupted wal", func(t *testing.T) {
		dir := t.TempDir()
		logger := &captureLogger{}
		wal := &WAL{config: &WALConfig{Dir: dir, Logger: logger}}

		walPath := filepath.Join(dir, "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte("corrupted-data"), 0644))

		backupPath := wal.backupCorruptedWAL(walPath)
		require.NotEmpty(t, backupPath)
		data, err := os.ReadFile(backupPath)
		require.NoError(t, err)
		require.Equal(t, "corrupted-data", string(data))

		missing := wal.backupCorruptedWAL(filepath.Join(dir, "missing.log"))
		require.Empty(t, missing)
		require.NotEmpty(t, logger.entries)
	})

	t.Run("backup corrupted wal handles create and copy failures", func(t *testing.T) {
		srcDir := filepath.Join(t.TempDir(), "src")
		require.NoError(t, os.MkdirAll(srcDir, 0o755))
		srcPath := filepath.Join(srcDir, "wal.log")
		require.NoError(t, os.WriteFile(srcPath, []byte("data"), 0o644))

		logger := &captureLogger{}
		// Force backup create failure: configured backup dir is a file.
		backupRoot := filepath.Join(t.TempDir(), "backup-root-file")
		require.NoError(t, os.WriteFile(backupRoot, []byte("not-dir"), 0o644))
		wal := &WAL{config: &WALConfig{Dir: backupRoot, Logger: logger}}
		require.Empty(t, wal.backupCorruptedWAL(srcPath))

		// Force copy failure by passing directory as source path.
		goodBackupDir := t.TempDir()
		wal = &WAL{config: &WALConfig{Dir: goodBackupDir, Logger: logger}}
		require.Empty(t, wal.backupCorruptedWAL(srcDir))
	})

	t.Run("report corruption writes diagnostics and callback", func(t *testing.T) {
		dir := t.TempDir()
		logger := &captureLogger{}
		var callbackDiag *CorruptionDiagnostics
		var callbackErr error

		wal := &WAL{
			config: &WALConfig{
				Dir:    dir,
				Logger: logger,
				OnCorruption: func(diag *CorruptionDiagnostics, cause error) {
					callbackDiag = diag
					callbackErr = cause
				},
			},
		}

		diag := &CorruptionDiagnostics{
			Timestamp:      time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC),
			WALPath:        filepath.Join(dir, "wal.log"),
			CorruptedSeq:   7,
			FileSize:       42,
			LastGoodSeq:    6,
			ExpectedCRC:    11,
			ActualCRC:      22,
			BackupPath:     filepath.Join(dir, "wal-corrupted-backup.log"),
			SuspectedCause: "disk",
			RecoveryAction: "truncate",
		}
		cause := fmt.Errorf("checksum mismatch")

		wal.reportCorruption(diag, cause)

		require.True(t, wal.degraded.Load())
		stored, ok := wal.lastCorruption.Load().(*CorruptionDiagnostics)
		require.True(t, ok)
		require.Equal(t, diag, stored)
		require.Equal(t, diag, callbackDiag)
		require.EqualError(t, callbackErr, cause.Error())
		require.NotEmpty(t, logger.entries)
		require.Equal(t, "error", logger.entries[0]["level"])

		diagnosticPath := filepath.Join(dir, "wal-corruption-20260304-100000.json")
		_, err := os.Stat(diagnosticPath)
		require.NoError(t, err)

		wal.reportCorruption(nil, nil)
	})
}

func TestStreamingFallbackAndCollectionHelpers(t *testing.T) {
	t.Run("stream nodes and edges with exportable fallback", func(t *testing.T) {
		engine := &exportableOnlyEngine{
			Engine: NewMemoryEngine(),
			allNodes: []*Node{
				{ID: "n1", Labels: []string{"Person", "User"}},
				{ID: "n2", Labels: []string{"Person"}},
			},
			allEdges: []*Edge{
				{ID: "e1", Type: "KNOWS"},
				{ID: "e2", Type: "LIKES"},
			},
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})

		var nodeIDs []NodeID
		require.NoError(t, StreamNodesWithFallback(context.Background(), engine, 1, func(node *Node) error {
			nodeIDs = append(nodeIDs, node.ID)
			return nil
		}))
		assert.Equal(t, []NodeID{"n1", "n2"}, nodeIDs)

		var edgeIDs []EdgeID
		require.NoError(t, StreamEdgesWithFallback(context.Background(), engine, 1, func(edge *Edge) error {
			edgeIDs = append(edgeIDs, edge.ID)
			return nil
		}))
		assert.Equal(t, []EdgeID{"e1", "e2"}, edgeIDs)

		count, err := CountNodesWithLabel(context.Background(), engine, "Person")
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)

		labels, err := CollectLabels(context.Background(), engine)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"Person", "User"}, labels)

		types, err := CollectEdgeTypes(context.Background(), engine)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"KNOWS", "LIKES"}, types)
	})

	t.Run("stream fallback surfaces context and callback errors", func(t *testing.T) {
		engine := &exportableOnlyEngine{
			Engine:   NewMemoryEngine(),
			allNodes: []*Node{{ID: "n1"}},
			allEdges: []*Edge{{ID: "e1", Type: "REL"}},
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := StreamNodesWithFallback(ctx, engine, 1, func(node *Node) error { return nil })
		require.ErrorIs(t, err, context.Canceled)

		errBoom := errors.New("node visitor failed")
		err = StreamNodesWithFallback(context.Background(), engine, 1, func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		err = StreamEdgesWithFallback(ctx, engine, 1, func(edge *Edge) error { return nil })
		require.ErrorIs(t, err, context.Canceled)

		errBoom = errors.New("edge visitor failed")
		err = StreamEdgesWithFallback(context.Background(), engine, 1, func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("stream fallback returns load errors", func(t *testing.T) {
		engine := &exportableOnlyEngine{
			Engine:  NewMemoryEngine(),
			nodeErr: errors.New("all nodes failed"),
			edgeErr: errors.New("all edges failed"),
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})

		err := StreamNodesWithFallback(context.Background(), engine, 1, func(node *Node) error { return nil })
		require.ErrorContains(t, err, "all nodes failed")

		err = StreamEdgesWithFallback(context.Background(), engine, 1, func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "all edges failed")
	})
}

func TestLoaderEmbeddingFinderAndWALDegradedHelpers(t *testing.T) {
	t.Run("loader embedding finder", func(t *testing.T) {
		nonExportable := NewMemoryEngine()
		t.Cleanup(func() { _ = nonExportable.Close() })
		assert.Nil(t, FindNodeNeedingEmbedding(nonExportable))

		exportable := &exportableOnlyEngine{
			Engine: NewMemoryEngine(),
			allNodes: []*Node{
				{ID: "embedded", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2}}},
				{ID: "needs", Labels: []string{"Doc"}, Properties: map[string]any{"text": "embed me"}},
			},
		}
		t.Cleanup(func() {
			if closer, ok := exportable.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})
		got := FindNodeNeedingEmbedding(exportable)
		require.NotNil(t, got)
		assert.Equal(t, NodeID("needs"), got.ID)

		exportable.nodeErr = errors.New("loader nodes failed")
		assert.Nil(t, FindNodeNeedingEmbedding(exportable))
	})

	t.Run("wal degraded status and diagnostics", func(t *testing.T) {
		wal := &WAL{}
		assert.False(t, wal.IsDegraded())
		assert.Nil(t, wal.LastCorruptionDiagnostics())

		diag := &CorruptionDiagnostics{Operation: "crc_mismatch"}
		wal.degraded.Store(true)
		wal.lastCorruption.Store(diag)
		assert.True(t, wal.IsDegraded())
		assert.Equal(t, diag, wal.LastCorruptionDiagnostics())

		other := &WAL{}
		other.lastCorruption.Store("unexpected")
		assert.Nil(t, other.LastCorruptionDiagnostics())
	})
}

func TestConstraintValidationCreationHelpers(t *testing.T) {
	t.Run("dispatcher routes and unknown types error", func(t *testing.T) {
		engine := &constraintValidationEngine{
			Engine: NewMemoryEngine(),
			nodes: []*Node{
				{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"email": "a@example.com", "id": "1", "name": "alice"}},
			},
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})

		require.NoError(t, ValidateConstraintOnCreationForEngine(engine, Constraint{
			Type:       ConstraintUnique,
			Label:      "Person",
			Properties: []string{"email"},
		}))
		require.NoError(t, ValidateConstraintOnCreationForEngine(engine, Constraint{
			Type:       ConstraintNodeKey,
			Label:      "Person",
			Properties: []string{"id", "name"},
		}))
		require.NoError(t, ValidateConstraintOnCreationForEngine(engine, Constraint{
			Type:       ConstraintExists,
			Label:      "Person",
			Properties: []string{"name"},
		}))

		err := ValidateConstraintOnCreationForEngine(engine, Constraint{Type: ConstraintType("weird")})
		require.ErrorContains(t, err, "unknown constraint type")
	})

	t.Run("node key helper validates property counts nulls and duplicates", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		err := validateNodeKeyConstraintOnCreationWithEngine(&constraintValidationEngine{Engine: base}, Constraint{
			Type:       ConstraintNodeKey,
			Label:      "Person",
			Properties: nil,
		})
		require.ErrorContains(t, err, "at least 1 property")

		engine := &constraintValidationEngine{
			Engine: base,
			nodes: []*Node{
				{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"id": "1"}},
			},
		}
		err = validateNodeKeyConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintNodeKey,
			Label:      "Person",
			Properties: []string{"id", "name"},
		})
		var violation *ConstraintViolationError
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintNodeKey, violation.Type)

		engine.nodes = []*Node{
			{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"id": "1", "name": "alice"}},
			{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"id": "1", "name": "alice"}},
		}
		err = validateNodeKeyConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintNodeKey,
			Label:      "Person",
			Properties: []string{"id", "name"},
		})
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintNodeKey, violation.Type)
	})

	t.Run("temporal helper validates arity scan errors and temporal violations", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		err := validateTemporalConstraintOnCreationWithEngine(&constraintValidationEngine{Engine: base}, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from"},
		})
		require.ErrorContains(t, err, "requires 3 properties")

		engine := &constraintValidationEngine{
			Engine: base,
			err:    errors.New("scan failed"),
		}
		err = validateTemporalConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from", "to"},
		})
		require.ErrorContains(t, err, "scanning nodes")

		engine.err = nil
		engine.nodes = []*Node{
			{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"from": time.Now(), "to": time.Now().Add(time.Hour)}},
		}
		err = validateTemporalConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from", "to"},
		})
		var violation *ConstraintViolationError
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintTemporal, violation.Type)

		now := time.Date(2026, 3, 6, 10, 0, 0, 0, time.UTC)
		engine.nodes = []*Node{
			{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"id": "a", "from": "bad", "to": now}},
		}
		err = validateTemporalConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from", "to"},
		})
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintTemporal, violation.Type)

		engine.nodes = []*Node{
			{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"id": "a", "from": now, "to": now.Add(2 * time.Hour)}},
			{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"id": "a", "from": now.Add(time.Hour), "to": now.Add(3 * time.Hour)}},
		}
		err = validateTemporalConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from", "to"},
		})
		require.ErrorAs(t, err, &violation)
		require.Equal(t, ConstraintTemporal, violation.Type)

		engine.nodes = []*Node{
			{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"id": "a", "from": now, "to": now.Add(time.Hour)}},
			{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"id": "a", "from": now.Add(2 * time.Hour), "to": now.Add(3 * time.Hour)}},
		}
		require.NoError(t, validateTemporalConstraintOnCreationWithEngine(engine, Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"id", "from", "to"},
		}))
	})
}

func TestDecodeNodeWithEmbeddings(t *testing.T) {
	t.Run("loads separate embeddings and tolerates missing chunks", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("embedded-node"))
		data, err := encodeValue(&Node{
			ID:                         nodeID,
			EmbeddingsStoredSeparately: true,
			EmbedMeta:                  map[string]any{"chunk_count": int16(3)},
		})
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			emb0, err := encodeEmbedding([]float32{1, 2})
			if err != nil {
				return err
			}
			emb2, err := encodeEmbedding([]float32{5, 6})
			if err != nil {
				return err
			}
			if err := txn.Set(embeddingKey(nodeID, 0), emb0); err != nil {
				return err
			}
			return txn.Set(embeddingKey(nodeID, 2), emb2)
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			node, err := decodeNodeWithEmbeddings(txn, data, nodeID)
			require.NoError(t, err)
			require.False(t, node.EmbeddingsStoredSeparately)
			require.Len(t, node.ChunkEmbeddings, 2)
			assert.Equal(t, []float32{1, 2}, node.ChunkEmbeddings[0])
			assert.Equal(t, []float32{5, 6}, node.ChunkEmbeddings[1])
			return nil
		}))
	})

	t.Run("parses string chunk counts and reports bad chunk payloads", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("embedded-node-str"))
		data, err := encodeValue(&Node{
			ID:                         nodeID,
			EmbeddingsStoredSeparately: true,
			EmbedMeta:                  map[string]any{"chunk_count": "1"},
		})
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(embeddingKey(nodeID, 0), []byte("bad-embedding"))
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			node, err := decodeNodeWithEmbeddings(txn, data, nodeID)
			require.ErrorContains(t, err, "failed to decode embedding chunk 0")
			require.Nil(t, node)
			return nil
		}))
	})

	t.Run("supports numeric chunk_count variants and zero/default paths", func(t *testing.T) {
		type testCase struct {
			name       string
			chunkCount interface{}
			wantChunks int
		}
		cases := []testCase{
			{name: "int8", chunkCount: int8(1), wantChunks: 1},
			{name: "uint16", chunkCount: uint16(1), wantChunks: 1},
			{name: "uint64", chunkCount: uint64(1), wantChunks: 1},
			{name: "float64", chunkCount: float64(1), wantChunks: 1},
			{name: "zero", chunkCount: 0, wantChunks: 0},
			{name: "nil default", chunkCount: nil, wantChunks: 0},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				engine := createTestBadgerEngine(t)
				nodeID := NodeID(prefixTestID("embed-meta-" + tc.name))
				data, err := encodeValue(&Node{
					ID:                         nodeID,
					EmbeddingsStoredSeparately: true,
					EmbedMeta:                  map[string]any{"chunk_count": tc.chunkCount},
				})
				require.NoError(t, err)

				if tc.wantChunks > 0 {
					require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
						emb, err := encodeEmbedding([]float32{7, 8})
						if err != nil {
							return err
						}
						return txn.Set(embeddingKey(nodeID, 0), emb)
					}))
				}

				require.NoError(t, engine.withView(func(txn *badger.Txn) error {
					node, err := decodeNodeWithEmbeddings(txn, data, nodeID)
					require.NoError(t, err)
					require.NotNil(t, node)
					if tc.wantChunks == 0 {
						assert.Empty(t, node.ChunkEmbeddings)
						assert.True(t, node.EmbeddingsStoredSeparately)
					} else {
						assert.Len(t, node.ChunkEmbeddings, tc.wantChunks)
						assert.False(t, node.EmbeddingsStoredSeparately)
					}
					return nil
				}))
			})
		}
	})
}

func TestSeparateEmbeddingChunkHelpers(t *testing.T) {
	countChunks := func(t *testing.T, engine *BadgerEngine, nodeID NodeID) int {
		t.Helper()
		count := 0
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = embeddingPrefix(nodeID)
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				count++
			}
			return nil
		}))
		return count
	}

	t.Run("deleteEmbeddingChunksBatched removes all chunks across batches", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("chunk-delete"))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			for i := 0; i < 260; i++ {
				emb, err := encodeEmbedding([]float32{float32(i)})
				if err != nil {
					return err
				}
				if err := txn.Set(embeddingKey(nodeID, i), emb); err != nil {
					return err
				}
			}
			return nil
		}))
		require.Equal(t, 260, countChunks(t, engine, nodeID))
		require.NoError(t, engine.deleteEmbeddingChunksBatched(nodeID))
		require.Equal(t, 0, countChunks(t, engine, nodeID))
	})

	t.Run("replaceSeparateEmbeddingChunks rewrites chunks and supports empty replacement", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("chunk-replace"))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			oldEmb, err := encodeEmbedding([]float32{1, 2, 3})
			if err != nil {
				return err
			}
			if err := txn.Set(embeddingKey(nodeID, 0), oldEmb); err != nil {
				return err
			}
			return txn.Set(embeddingKey(nodeID, 1), oldEmb)
		}))

		require.NoError(t, engine.replaceSeparateEmbeddingChunks(nodeID, [][]float32{{9, 9}, {8, 8, 8}}))
		require.Equal(t, 2, countChunks(t, engine, nodeID))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			item, err := txn.Get(embeddingKey(nodeID, 1))
			require.NoError(t, err)
			return item.Value(func(val []byte) error {
				emb, err := decodeEmbedding(val)
				require.NoError(t, err)
				assert.Equal(t, []float32{8, 8, 8}, emb)
				return nil
			})
		}))

		require.NoError(t, engine.replaceSeparateEmbeddingChunks(nodeID, nil))
		require.Equal(t, 0, countChunks(t, engine, nodeID))
	})
}

func TestBadgerConstraintValidationHelpers(t *testing.T) {
	t.Run("compareValues handles numeric and typed comparisons", func(t *testing.T) {
		assert.True(t, compareValues(int(3), int(3)))
		assert.True(t, compareValues(int64(4), int64(4)))
		assert.True(t, compareValues(float64(1.5), float64(1.5)))
		assert.True(t, compareValues("x", "x"))
		assert.True(t, compareValues(true, true))
		assert.True(t, compareValues(int(3), int64(3)))
		assert.True(t, compareValues(float64(3), int64(3)))
		assert.False(t, compareValues("x", "y"))
	})

	t.Run("validateNodeConstraintsInTxn covers nil malformed and violation branches", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.GetSchemaForNamespace("test").AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddConstraint(Constraint{
				Name:       "ignored_multi_unique",
				Type:       ConstraintUnique,
				Label:      "User",
				Properties: []string{"a", "b"},
			}))
			require.NoError(t, engine.validateNodeConstraintsInTxn(txn, nil, schema, "test", ""))
			require.NoError(t, engine.validateNodeConstraintsInTxn(txn, &Node{ID: "test:u0", Labels: []string{"User"}}, nil, "test", ""))

			err := engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u1",
				Labels:     []string{"User"},
				Properties: map[string]any{"name": "ok"},
			}, schema, "test", "")
			require.NoError(t, err)
			return nil
		}))

		_, err := engine.CreateNode(&Node{
			ID:         "test:existing-user",
			Labels:     []string{"User"},
			Properties: map[string]any{"email": "dup@example.com", "tenant": "t1", "username": "alice", "name": "Alice", "account": "acct", "from": time.Date(2026, 3, 7, 10, 0, 0, 0, time.UTC), "to": time.Date(2026, 3, 7, 11, 0, 0, 0, time.UTC), "age": int64(30)},
		})
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			schema := NewSchemaManager()
			require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))
			require.NoError(t, schema.AddConstraint(Constraint{
				Name:       "user_key",
				Type:       ConstraintNodeKey,
				Label:      "User",
				Properties: []string{"tenant", "username"},
			}))
			require.NoError(t, schema.AddConstraint(Constraint{
				Name:       "user_name_exists",
				Type:       ConstraintExists,
				Label:      "User",
				Properties: []string{"name"},
			}))
			require.NoError(t, schema.AddConstraint(Constraint{
				Name:       "user_temporal",
				Type:       ConstraintTemporal,
				Label:      "User",
				Properties: []string{"account", "from", "to"},
			}))
			require.NoError(t, schema.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger))

			err := engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u2",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "dup@example.com", "tenant": "t2", "username": "bob", "name": "Bob", "account": "acct-2", "from": time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC), "to": time.Date(2026, 3, 7, 13, 0, 0, 0, time.UTC), "age": int64(31)},
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u3",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "other@example.com", "tenant": "t1", "name": "Bob", "account": "acct-3", "from": time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC), "to": time.Date(2026, 3, 7, 13, 0, 0, 0, time.UTC), "age": int64(31)},
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u4",
				Labels:     []string{"User"},
				Properties: nil,
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u5",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "other@example.com", "tenant": "t5", "username": "eve", "name": "Eve"},
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u6",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "other@example.com", "tenant": "t6", "username": "mallory", "name": "Mallory", "account": "acct-6", "from": "bad", "to": time.Date(2026, 3, 7, 13, 0, 0, 0, time.UTC), "age": int64(31)},
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u7",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "other@example.com", "tenant": "t7", "username": "trent", "name": "Trent", "account": "acct", "from": time.Date(2026, 3, 7, 10, 30, 0, 0, time.UTC), "to": time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC), "age": int64(31)},
			}, schema, "test", "")
			require.Error(t, err)

			err = engine.validateNodeConstraintsInTxn(txn, &Node{
				ID:         "test:u8",
				Labels:     []string{"User"},
				Properties: map[string]any{"email": "other@example.com", "tenant": "t8", "username": "oscar", "name": "Oscar", "account": "acct-8", "from": time.Date(2026, 3, 7, 14, 0, 0, 0, time.UTC), "to": time.Date(2026, 3, 7, 15, 0, 0, 0, time.UTC), "age": "old"},
			}, schema, "test", "")
			require.Error(t, err)

			return nil
		}))
	})

	t.Run("encode and decode edge helpers validate payloads", func(t *testing.T) {
		data, err := encodeEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL", Properties: map[string]any{"ok": "yes"}})
		require.NoError(t, err)
		edge, err := decodeEdge(data)
		require.NoError(t, err)
		assert.Equal(t, EdgeID("e1"), edge.ID)

		_, err = encodeEdge(&Edge{ID: "e2", StartNode: "n1", EndNode: "n2", Type: "REL", Properties: map[string]any{"bad": func() {}}})
		require.Error(t, err)

		_, err = decodeEdge([]byte("not-an-edge"))
		require.Error(t, err)
	})
}

package storage

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Public graph and mutation APIs are the oracle here: an acknowledged key
// change must release only its predecessor's claim, and a rejected write must
// leave every other committed node unchanged. No derived index layout is pinned.
func TestPR1655UniqueTransitionsLifecycle(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, producer := range []string{"direct", "explicit", "implicit"} {
			t.Run(fmt.Sprintf("writebehind=%t/%s", writeBehind, producer), func(t *testing.T) {
				options := BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour}
				engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
				require.NoError(t, err)
				target := NewNamespacedEngine(engine, "test")
				schema := target.GetSchema()
				require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
				expected := make(map[NodeID]string)
				create := func(id NodeID, value string) {
					t.Helper()
					_, err := target.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": value}})
					require.NoError(t, err, "create %s with released key %s", id, value)
					expected[id] = value
				}
				rejected := 0
				rejectDuplicate := func(value string) {
					t.Helper()
					rejected++
					id := NodeID(fmt.Sprintf("rejected-%d", rejected))
					_, err := target.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": value}})
					var violation *ConstraintViolationError
					require.ErrorAs(t, err, &violation, "committed key %s must have exactly one owner", value)
					require.Equal(t, ConstraintUnique, violation.Type)
					_, err = target.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound, "a rejected duplicate publishes no body")
				}
				assertGraph := func() {
					t.Helper()
					nodes, err := target.GetNodesByLabel("Doc")
					require.NoError(t, err)
					actual := make(map[NodeID]string, len(nodes))
					owners := make(map[string]NodeID, len(nodes))
					for _, node := range nodes {
						value, ok := node.Properties["id"].(string)
						require.True(t, ok)
						prior, duplicate := owners[value]
						require.False(t, duplicate, "key %s is held by %s and %s", value, prior, node.ID)
						owners[value] = node.ID
						actual[node.ID] = value
					}
					require.Len(t, nodes, len(expected))
					require.Equal(t, expected, actual)
					for id, value := range expected {
						node, err := target.GetNode(id)
						require.NoError(t, err)
						require.Equal(t, value, node.Properties["id"], "point and label reads agree for %s", id)
					}
				}
				mutate := func(id NodeID, value string, remove bool) {
					t.Helper()
					if producer == "direct" {
						if remove {
							require.NoError(t, target.DeleteNode(id))
						} else {
							before, err := target.GetNode(id)
							require.NoError(t, err)
							after := *before
							after.Properties = maps.Clone(before.Properties)
							after.Properties["id"] = value
							require.NoError(t, target.UpdateNode(NodeUpdateRequest{Before: before, After: &after}))
						}
					} else {
						tx, err := engine.BeginTransaction()
						require.NoError(t, err)
						defer tx.Rollback()
						require.NoError(t, tx.SetNamespace("test"))
						require.NoError(t, tx.SetImplicit(producer == "implicit"))
						qualified := NodeID("test:" + string(id))
						if remove {
							require.NoError(t, tx.DeleteGraphNode(qualified, CascadeEdges))
						} else {
							before, err := tx.GetNode(qualified)
							require.NoError(t, err)
							after := *before
							after.Properties = maps.Clone(before.Properties)
							after.Properties["id"] = value
							require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
						}
						require.NoError(t, tx.Commit())
					}
					if remove {
						_, err := target.GetNode(id)
						require.ErrorIs(t, err, ErrNotFound)
						delete(expected, id)
					} else {
						expected[id] = value
					}
				}

				create("a", "old")
				create("keeper", "held")
				// Staging a rename publishes neither its new claim nor release
				// of its old claim. A separate writer may take the new key.
				rollback, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer rollback.Rollback()
				before, err := rollback.GetNode("test:a")
				require.NoError(t, err)
				after := *before
				after.Properties = maps.Clone(before.Properties)
				after.Properties["id"] = "rolled-back"
				require.NoError(t, rollback.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
				rejectDuplicate("old")
				create("independent", "rolled-back")
				require.NoError(t, rollback.Rollback())
				assertGraph()

				mutate("a", "renamed", false)
				create("b", "old")
				rejectDuplicate("old")
				assertGraph() // Includes the unflushed ACK in implicit mode.
				require.NoError(t, engine.FlushWriteBehind(nil))
				assertGraph()
				rejectDuplicate("old") // Replay cannot erase the newer owner's claim.
				mutate("a", "final", false)
				create("c", "renamed")
				mutate("b", "", true)
				create("d", "old")
				rejectDuplicate("final")
				rejectDuplicate("held")
				assertGraph()

				require.NoError(t, schema.DropConstraint("doc_id"))
				mutate("a", "after-drop", false)
				create("e", "final")
				require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
				rejectDuplicate("after-drop")
				rejectDuplicate("final")
				require.NoError(t, engine.FlushWriteBehind(nil))
				assertGraph()

				require.NoError(t, engine.Close())
				engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
				require.NoError(t, err)
				target = NewNamespacedEngine(engine, "test")
				assertGraph()
				for _, value := range expected {
					rejectDuplicate(value)
				}
				mutate("d", "", true)
				create("after-restart", "old")
				require.NoError(t, engine.FlushWriteBehind(nil))
				assertGraph()
				rejectDuplicate("old")
			})
		}
	}
}

// A stale writer carrying its original key must lose as an ordinary conflict,
// even after a third writer reuses that key. It cannot overwrite the rename,
// steal the third writer's key or publish its earlier independent mutation.
func TestPR1655UniqueTransitionsStaleWriterAfterReuse(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("writebehind=%t/implicit=%t", writeBehind, implicit), func(t *testing.T) {
				engine := openPR1655AcceptanceEngine(t, writeBehind)
				target := NewNamespacedEngine(engine, "test")
				schema := target.GetSchema()
				require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
				_, err := target.CreateNode(&Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "old", "note": "seed"}})
				require.NoError(t, err)
				loser, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer loser.Rollback()
				require.NoError(t, loser.SetImplicit(implicit))
				before, err := loser.GetNode("test:a")
				require.NoError(t, err)
				after := *before
				after.Properties = maps.Clone(before.Properties)
				after.Properties["note"] = "loser"
				require.NoError(t, loser.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
				_, err = loser.CreateNode(&Node{ID: "test:marker", Properties: map[string]interface{}{"note": "must roll back"}})
				require.NoError(t, err)
				peer, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer peer.Rollback()
				require.NoError(t, peer.SetImplicit(true))
				peerBefore, err := peer.GetNode("test:a")
				require.NoError(t, err)
				peerAfter := *peerBefore
				peerAfter.Properties = maps.Clone(peerBefore.Properties)
				peerAfter.Properties["id"] = "renamed"
				peerAfter.Properties["note"] = "peer"
				require.NoError(t, peer.UpdateGraphNode(NodeUpdateRequest{Before: peerBefore, After: &peerAfter, Policy: ClientNodeEdits}))
				require.NoError(t, peer.Commit())
				_, err = target.CreateNode(&Node{ID: "third", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "old"}})
				require.NoError(t, err)
				err = loser.Commit()
				require.ErrorIs(t, err, ErrConflict)
				var violation *ConstraintViolationError
				require.NotErrorAs(t, err, &violation, "unchanged keys remain ordinary retryable conflicts")
				require.NoError(t, engine.FlushWriteBehind(nil))
				a, err := target.GetNode("a")
				require.NoError(t, err)
				require.Equal(t, "renamed", a.Properties["id"])
				require.Equal(t, "peer", a.Properties["note"])
				third, err := target.GetNode("third")
				require.NoError(t, err)
				require.Equal(t, "old", third.Properties["id"])
				_, err = target.GetNode("marker")
				require.ErrorIs(t, err, ErrNotFound)
				nodes, err := target.GetNodesByLabel("Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 2)
			})
		}
	}
}

// Prefix deletion must publish the same claim release as an ordinary node
// deletion, without freeing a node outside the selected prefix.
func TestPR1655UniqueTransitionsPrefixReleasesClaim(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			engine := openPR1655AcceptanceEngine(t, writeBehind)
			target := NewNamespacedEngine(engine, "test")
			other := NewNamespacedEngine(engine, "test2")
			for _, store := range []*NamespacedEngine{target, other} {
				schema := store.GetSchema()
				require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
				require.NoError(t, schema.AddUniqueConstraint("doc_code", "Doc", "code"))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(store, schema))
			}
			for _, store := range []*NamespacedEngine{target, other} {
				for id, key := range map[NodeID]string{"prefix-victim": "released", "keeper": "held"} {
					_, err := store.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": key, "code": "code-" + key}})
					require.NoError(t, err)
				}
			}
			removed, edges, err := engine.DeleteByPrefix("test:prefix-")
			require.NoError(t, err)
			require.Equal(t, int64(1), removed)
			require.Zero(t, edges)
			_, err = target.GetNode("prefix-victim")
			require.ErrorIs(t, err, ErrNotFound)
			for _, property := range []string{"id", "code"} {
				value := "released"
				if property == "code" {
					value = "code-released"
				}
				_, found, constrained, complete := target.GetSchema().LookupUniqueConstraintValue("Doc", property, value)
				require.True(t, constrained)
				require.True(t, complete, "bulk retirement preserves completeness")
				require.False(t, found, "the deleted owner releases every constraint entry")
				holder, found, constrained, complete := other.GetSchema().LookupUniqueConstraintValue("Doc", property, value)
				require.True(t, found)
				require.True(t, constrained)
				require.True(t, complete)
				require.Equal(t, NodeID("test2:prefix-victim"), holder, "a similarly named namespace is untouched")
			}
			_, err = target.CreateNode(&Node{ID: "replacement", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "released", "code": "code-released"}})
			require.NoError(t, err, "prefix deletion releases all deleted claims with the body")
			for _, store := range []*NamespacedEngine{target, other} {
				for _, property := range []string{"id", "code"} {
					properties := map[string]interface{}{"id": "new-id", "code": "new-code"}
					if property == "id" {
						properties[property] = "held"
					} else {
						properties[property] = "code-held"
					}
					_, err := store.CreateNode(&Node{ID: NodeID("duplicate-" + property), Labels: []string{"Doc"}, Properties: properties})
					var violation *ConstraintViolationError
					require.ErrorAs(t, err, &violation, "another owner's %s remains reserved", property)
					require.Equal(t, []string{property}, violation.Properties)
				}
			}
			require.NoError(t, engine.FlushWriteBehind(nil))
			for _, store := range []*NamespacedEngine{target, other} {
				nodes, err := store.GetNodesByLabel("Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 2)
				keeper, err := store.GetNode("keeper")
				require.NoError(t, err)
				require.Equal(t, "held", keeper.Properties["id"])
			}
		})
	}
}

// A peer can reclaim a released value only after the destructive lifecycle
// publishes. The drop must never erase that peer's newly acquired claim.
func TestPR1655UniqueTransitionsConcurrentPrefixReuse(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			engine := openPR1655AcceptanceEngine(t, writeBehind)
			target := NewNamespacedEngine(engine, "test")
			require.NoError(t, target.GetSchema().AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, target.GetSchema()))
			_, err := target.CreateNode(&Node{ID: "prefix-victim", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "reclaimed"}})
			require.NoError(t, err)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer release()
			engine.dropPrefixTailHook = func(prefix string) error { close(entered); <-resume; return nil }
			dropped := make(chan error, 1)
			go func() { _, _, err := engine.DeleteByPrefix("test:prefix-"); dropped <- err }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("prefix drop did not reach its publication boundary")
			}
			// This is the actual lifecycle gate, not a timing assertion about
			// whether the peer goroutine happened to be scheduled already.
			if engine.writeBarrier.TryRLock() {
				engine.writeBarrier.RUnlock()
				t.Fatal("prefix drop released write authority before retiring its claims")
			}
			_, found, _, complete := target.GetSchema().LookupUniqueConstraintValue("Doc", "id", "reclaimed")
			require.True(t, complete)
			require.False(t, found, "the successful primary drop already retired its claim")
			started, created := make(chan struct{}), make(chan error, 1)
			go func() {
				close(started)
				_, err := target.CreateNode(&Node{ID: "peer", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "reclaimed"}})
				created <- err
			}()
			<-started
			release()
			require.NoError(t, <-dropped)
			require.NoError(t, <-created)
			engine.dropPrefixTailHook = nil
			require.NoError(t, engine.FlushWriteBehind(nil))
			peer, err := target.GetNode("peer")
			require.NoError(t, err)
			require.Equal(t, "reclaimed", peer.Properties["id"])
			_, err = target.CreateNode(&Node{ID: "duplicate", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "reclaimed"}})
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation, "bulk retirement cannot erase the peer's later reservation")
			nodes, err := target.GetNodesByLabel("Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
		})
	}
}

func TestPR1655UniqueTransitionsPrimaryDropRetiresBeforeCleanupFailure(t *testing.T) {
	engine := openPR1655AcceptanceEngine(t, true)
	target := NewNamespacedEngine(engine, "test")
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
	_, err := target.CreateNode(&Node{ID: "prefix-victim", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "released"}})
	require.NoError(t, err)
	failure := errors.New("injected post-primary prefix cleanup failure")
	engine.dropPrefixTailHook = func(string) error { return failure }
	_, _, err = engine.DeleteByPrefix("test:prefix-")
	require.ErrorIs(t, err, failure)
	engine.dropPrefixTailHook = nil
	_, found, constrained, complete := schema.LookupUniqueConstraintValue("Doc", "id", "released")
	require.True(t, constrained)
	require.True(t, complete)
	require.False(t, found, "a later cleanup failure cannot restore an already deleted node's claim")
}

// Restore installs fresh schema managers after the buffer already exists;
// reopening constructs the buffer after schemas load. Both lifetimes must
// keep acknowledged unflushed claims visible to logical validation.
func TestPR1655UniqueTransitionsRestoredSchemaPendingClaims(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			source := openPR1655AcceptanceEngine(t, writeBehind)
			original := NewNamespacedEngine(source, "test")
			require.NoError(t, original.GetSchema().AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(original, original.GetSchema()))
			_, err := original.CreateNode(&Node{ID: "original", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "key"}})
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "claims.backup")
			require.NoError(t, source.Backup(backup))
			target := openPR1655AcceptanceEngine(t, writeBehind)
			require.NoError(t, target.Restore(backup))
			restored := NewNamespacedEngine(target, "test")
			tx, err := target.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.DeleteGraphNode("test:original", CascadeEdges))
			require.NoError(t, tx.Commit())
			_, found, constrained, complete := restored.GetSchema().LookupUniqueConstraintValue("Doc", "id", "key")
			require.False(t, found, "the acknowledged delete shadows the restored stored claim")
			require.True(t, constrained)
			require.True(t, complete)
			_, err = restored.CreateNode(&Node{ID: "replacement", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "key"}})
			require.NoError(t, err, "an unflushed delete releases a restored schema's claim")
			require.NoError(t, target.FlushWriteBehind(nil))
			_, err = restored.GetNode("original")
			require.ErrorIs(t, err, ErrNotFound)
			replacement, err := restored.GetNode("replacement")
			require.NoError(t, err)
			require.Equal(t, "key", replacement.Properties["id"])
			_, err = restored.CreateNode(&Node{ID: "duplicate", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "key"}})
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation)
		})
	}
}

// ACK-time release permits another ID to acquire the key before old physical
// replay. The old delete must not unregister either of that new owner's keys,
// in either database, when its generation eventually flushes.
func TestPR1655UniqueTransitionsBufferedDeleteThenReuse(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			engine := openPR1655AcceptanceEngine(t, writeBehind)
			for _, namespace := range []string{"test", "test2"} {
				target := NewNamespacedEngine(engine, namespace)
				schema := target.GetSchema()
				require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
				require.NoError(t, schema.AddUniqueConstraint("doc_code", "Doc", "code"))
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
				_, err := target.CreateNode(&Node{ID: "old-owner", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "key", "code": "code-key"}})
				require.NoError(t, err)
				peer, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, peer.SetImplicit(true))
				require.NoError(t, peer.DeleteGraphNode(NodeID(namespace+":old-owner"), CascadeEdges))
				require.NoError(t, peer.Commit())
				_, err = target.CreateNode(&Node{ID: "new-owner", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "key", "code": "code-key"}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.FlushWriteBehind(nil))
			for _, namespace := range []string{"test", "test2"} {
				target := NewNamespacedEngine(engine, namespace)
				for _, property := range []string{"id", "code"} {
					value := "key"
					properties := map[string]interface{}{"id": "another", "code": "another-code"}
					if property == "code" {
						value = "code-key"
					}
					properties[property] = value
					holder, found, constrained, complete := target.GetSchema().LookupUniqueConstraintValue("Doc", property, value)
					require.True(t, found)
					require.True(t, constrained)
					require.True(t, complete)
					require.Equal(t, NodeID(namespace+":new-owner"), holder)
					_, err := target.CreateNode(&Node{ID: NodeID("duplicate-" + property), Labels: []string{"Doc"}, Properties: properties})
					var violation *ConstraintViolationError
					require.ErrorAs(t, err, &violation, "old replay cannot erase a reused %s claim", property)
					require.Equal(t, []string{property}, violation.Properties)
				}
				nodes, err := target.GetNodesByLabel("Doc")
				require.NoError(t, err)
				require.Len(t, nodes, 1)
				_, err = target.GetNode("old-owner")
				require.ErrorIs(t, err, ErrNotFound)
				owner, err := target.GetNode("new-owner")
				require.NoError(t, err)
				require.Equal(t, "key", owner.Properties["id"])
				require.Equal(t, "code-key", owner.Properties["code"])
			}
		})
	}
}

// A generation can contain several logical images of one node. Its
// intermediate claim is already released at ACK time; replay cannot take it
// away from a different node that acquired it before the generation flushed.
func TestPR1655UniqueTransitionsIntermediateAcknowledgedClaims(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			engine := openPR1655AcceptanceEngine(t, writeBehind)
			target := NewNamespacedEngine(engine, "test")
			schema := target.GetSchema()
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, schema.AddUniqueConstraint("doc_code", "Doc", "code"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, schema))
			_, err := target.CreateNode(&Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "seed", "code": "code-seed"}})
			require.NoError(t, err)
			for _, value := range []string{"old", "renamed"} {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				before, err := tx.GetNode("test:a")
				require.NoError(t, err)
				after := *before
				after.Properties = maps.Clone(before.Properties)
				after.Properties["id"] = value
				after.Properties["code"] = "code-" + value
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
				require.NoError(t, tx.Commit())
			}
			_, err = target.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "old", "code": "code-old"}})
			require.NoError(t, err, "the final ACK already released the intermediate claim")
			require.NoError(t, engine.FlushWriteBehind(nil))
			for _, property := range []string{"id", "code"} {
				value := "old"
				properties := map[string]interface{}{"id": "unused", "code": "unused-code"}
				if property == "code" {
					value = "code-old"
				}
				properties[property] = value
				_, err := target.CreateNode(&Node{ID: NodeID("duplicate-" + property), Labels: []string{"Doc"}, Properties: properties})
				var violation *ConstraintViolationError
				require.ErrorAs(t, err, &violation, "an intermediate replay image cannot steal the %s claim", property)
				require.Equal(t, []string{property}, violation.Properties)
			}
			for id, value := range map[NodeID]string{"a": "renamed", "b": "old"} {
				node, err := target.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, value, node.Properties["id"])
				require.Equal(t, "code-"+value, node.Properties["code"])
			}
			nodes, err := target.GetNodesByLabel("Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 2)
		})
	}
}

func TestPR1655UniqueTransitionsRecoveredIntermediateClaims(t *testing.T) {
	for _, scenario := range []struct {
		payloadBytes int
		semantic     bool
	}{{0, false}, {5 << 20, false}, {0, true}} {
		payloadBytes := scenario.payloadBytes
		t.Run(fmt.Sprintf("payload=%d/semantic=%t", payloadBytes, scenario.semantic), func(t *testing.T) {
			dir := t.TempDir()
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
			target := NewNamespacedEngine(engine, "test")
			require.NoError(t, target.GetSchema().AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, target.GetSchema()))
			_, err := target.CreateNode(&Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "seed"}})
			require.NoError(t, err)
			for _, value := range []string{"old", "renamed"} {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				before, err := tx.GetNode("test:a")
				require.NoError(t, err)
				after := *before
				after.Properties = maps.Clone(before.Properties)
				after.Properties["id"] = value
				if payloadBytes > 0 {
					after.Properties["payload"] = strings.Repeat(value[:1], payloadBytes)
				}
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
				if value == "old" && scenario.semantic {
					payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "unique-interposed", Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:semantic", Labels: []string{"Other"}, Properties: map[string]any{"value": int64(42)}}}}, Mode: CommitModeDirect})
					require.NoError(t, err)
					require.NoError(t, engine.bufferedCommitWAL.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
				}
			}
			_, err = target.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"id": "old"}})
			require.NoError(t, err)
			crashClose(t, engine, engine.bufferedCommitWAL)
			recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
			_, err = RecoverBufferedCommits(recovered, recovered.bufferedCommitWAL)
			require.NoError(t, err)
			target = NewNamespacedEngine(recovered, "test")
			if scenario.semantic {
				node, err := target.GetNode("semantic")
				require.NoError(t, err)
				require.EqualValues(t, 42, node.Properties["value"])
			}
			for id, value := range map[NodeID]string{"a": "renamed", "b": "old"} {
				node, err := target.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, value, node.Properties["id"])
			}
			_, err = target.CreateNode(&Node{ID: "duplicate", Labels: []string{"Doc"}, Properties: map[string]any{"id": "old"}})
			var violation *ConstraintViolationError
			require.ErrorAs(t, err, &violation, "recovery cannot release the surviving owner's key")
		})
	}
}

func TestPR1655UniqueTransitionsAtomicSwap(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writebehind=%t", writeBehind), func(t *testing.T) {
			engine := openPR1655AcceptanceEngine(t, writeBehind)
			target := NewNamespacedEngine(engine, "test")
			require.NoError(t, target.GetSchema().AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(target, target.GetSchema()))
			for id, value := range map[NodeID]string{"a": "x", "b": "y"} {
				_, err := target.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"id": value}})
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			for _, mutation := range []struct {
				id    NodeID
				value string
			}{{"test:a", "temporary"}, {"test:b", "x"}, {"test:a", "y"}} {
				id, value := mutation.id, mutation.value
				before, err := tx.GetNode(id)
				require.NoError(t, err)
				after := *before
				after.Properties = maps.Clone(before.Properties)
				after.Properties["id"] = value
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: &after, Policy: ClientNodeEdits}))
			}
			require.NoError(t, tx.Commit())
			require.NoError(t, engine.FlushWriteBehind(nil))
			for id, value := range map[NodeID]string{"a": "y", "b": "x"} {
				node, err := target.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, value, node.Properties["id"])
				holder, found, exists, _ := target.GetSchema().LookupUniqueConstraintValue("Doc", "id", value)
				require.True(t, found)
				require.True(t, exists)
				require.Equal(t, NodeID("test:"+string(id)), holder)
				_, err = target.CreateNode(&Node{ID: NodeID("duplicate-" + string(id)), Labels: []string{"Doc"}, Properties: map[string]any{"id": value}})
				var violation *ConstraintViolationError
				require.ErrorAs(t, err, &violation)
			}
		})
	}
}

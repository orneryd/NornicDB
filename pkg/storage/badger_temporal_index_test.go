package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTemporal_AsOfAndCurrentPointer(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		name := "EngineAsOfAndCurrentPointer"
		if transactional {
			name = "TransactionCurrentPointerFollowsCommittedWrites"
		}
		t.Run(name, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			constraint := Constraint{Name: "fact_temporal", Type: ConstraintTemporal, Label: "FactVersion", Properties: []string{"fact_key", "valid_from", "valid_to"}}
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
			closedID, currentID := NodeID(prefixTestID("fact-v1")), NodeID(prefixTestID("fact-v2"))
			key := "k1"
			closedStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			closedEnd := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
			currentStart := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
			type query struct {
				at   time.Time
				want NodeID
			}
			queries := []query{{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), closedID}, {time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), currentID}}
			var base time.Time
			if transactional {
				base = time.Now().UTC().Truncate(time.Second)
				closedID, currentID = NodeID(prefixTestID("fact-closed")), NodeID(prefixTestID("fact-current"))
				key = "txn-pointer"
				closedStart, closedEnd, currentStart = base.Add(-4*time.Hour), base.Add(-2*time.Hour), base.Add(-1*time.Hour)
				queries = []query{{base, currentID}}
			}
			makeVersion := func(id NodeID, start time.Time, end interface{}) *Node {
				return &Node{ID: id, Labels: []string{"FactVersion"}, Properties: map[string]interface{}{"fact_key": key, "valid_from": start, "valid_to": end}}
			}
			_, err := engine.CreateNode(makeVersion(closedID, closedStart, closedEnd))
			require.NoError(t, err)
			current := makeVersion(currentID, currentStart, nil)
			if transactional {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				_, err = tx.CreateNode(current)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			} else {
				_, err = engine.CreateNode(current)
				require.NoError(t, err)
			}
			for _, query := range queries {
				node, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", key, "valid_from", "valid_to", query.at)
				require.NoError(t, err)
				require.NotNil(t, node)
				require.Equal(t, query.want, node.ID)
			}
			desc := makeTemporalDescriptor("test", constraint, key)
			var gotID NodeID
			err = engine.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(temporalCurrentKey(desc))
				require.NoError(t, err)
				return item.Value(func(val []byte) error {
					gotID = NodeID(append([]byte(nil), val...))
					if transactional {
						require.Equal(t, currentID, gotID)
					}
					return nil
				})
			})
			require.NoError(t, err)
			if !transactional {
				require.Equal(t, currentID, gotID)
			}
			if transactional {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.DeleteGraphNode(currentID, CascadeEdges))
				require.NoError(t, tx.Commit())
				node, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", key, "valid_from", "valid_to", base)
				require.NoError(t, err)
				require.Nil(t, node)
				err = engine.withView(func(txn *badger.Txn) error {
					_, err := txn.Get(temporalCurrentKey(desc))
					require.ErrorIs(t, err, badger.ErrKeyNotFound)
					return nil
				})
				require.NoError(t, err)
			}
		})
	}
}

func TestBadgerEngine_TemporalAdjacentOverlapChecks(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{
		Name:       "person_temporal",
		Type:       ConstraintTemporal,
		Label:      "Person",
		Properties: []string{"account", "from", "to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))

	makeNode := func(id, key string, start, end time.Time) *Node {
		return &Node{
			ID:     NodeID(prefixTestID(id)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": key,
				"from":    start,
				"to":      end,
			},
		}
	}

	base := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(makeNode("acct-v1", "acct", base, base.Add(2*time.Hour)))
	require.NoError(t, err)
	_, err = engine.CreateNode(makeNode("acct-v2", "acct", base.Add(3*time.Hour), base.Add(5*time.Hour)))
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	err = tx.checkTemporalConstraint(makeNode("acct-middle-ok", "acct", base.Add(2*time.Hour), base.Add(3*time.Hour)), constraint)
	require.NoError(t, err)
	err = tx.checkTemporalConstraint(makeNode("acct-middle-bad", "acct", base.Add(90*time.Minute), base.Add(4*time.Hour)), constraint)
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
}

func TestBadgerEngine_RebuildTemporalIndexes(t *testing.T) {
	engine := createTestBadgerEngine(t)
	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	v2Start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	_, err := engine.CreateNode(&Node{
		ID:     NodeID(prefixTestID("rebuild-v1")),
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "rebuild",
			"valid_from": v1Start,
			"valid_to":   v1End,
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{
		ID:     NodeID(prefixTestID("rebuild-v2")),
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "rebuild",
			"valid_from": v2Start,
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	constraint := Constraint{
		Name:       "fact_temporal",
		Type:       ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))
	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))

	node, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", "rebuild", "valid_from", "valid_to", time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, NodeID(prefixTestID("rebuild-v2")), node.ID)
}

func TestBadgerEngine_PruneTemporalHistory(t *testing.T) {
	engine := createTestBadgerEngine(t)
	constraint := Constraint{
		Name:       "fact_temporal",
		Type:       ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(constraint))

	base := time.Now().UTC().Add(-10 * 24 * time.Hour)
	for i := 0; i < 4; i++ {
		start := base.Add(time.Duration(i) * 24 * time.Hour)
		var end interface{}
		if i < 3 {
			end = start.Add(24 * time.Hour)
		}
		_, err := engine.CreateNode(&Node{
			ID:     NodeID(prefixTestID("prune-" + constraintValueKey(i))),
			Labels: []string{"FactVersion"},
			Properties: map[string]interface{}{
				"fact_key":   "prune",
				"valid_from": start,
				"valid_to":   end,
			},
		})
		require.NoError(t, err)
	}

	deleted, err := engine.PruneTemporalHistory(context.Background(), TemporalPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)

	nodes, err := engine.GetNodesByLabel("FactVersion")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	node, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", "prune", "valid_from", "valid_to", time.Now().UTC())
	require.NoError(t, err)
	require.NotNil(t, node)
}

func BenchmarkBadgerTemporalAsOfGhostOfVersioning(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, engine.Close())
	})
	constraint := Constraint{
		Name:       "fact_temporal",
		Type:       ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}
	require.NoError(b, engine.GetSchemaForNamespace("test").AddConstraint(constraint))

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	b.StopTimer()
	for i := 0; i < 1000; i++ {
		start := base.Add(time.Duration(i) * time.Hour)
		var end interface{}
		if i < 999 {
			end = start.Add(time.Hour)
		}
		_, err := engine.CreateNode(&Node{
			ID:     NodeID(prefixTestID("ghost-" + constraintValueKey(i))),
			Labels: []string{"FactVersion"},
			Properties: map[string]interface{}{
				"fact_key":   "ghost",
				"valid_from": start,
				"valid_to":   end,
			},
		})
		require.NoError(b, err)
	}
	initialTS := base.Add(30 * time.Minute)
	currentTS := base.Add(999*time.Hour + 30*time.Minute)
	b.StartTimer()

	b.Run("initial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", "ghost", "valid_from", "valid_to", initialTS)
			require.NoError(b, err)
		}
	})

	b.Run("current", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := engine.GetTemporalNodeAsOfInNamespace("test", "FactVersion", "fact_key", "ghost", "valid_from", "valid_to", currentTS)
			require.NoError(b, err)
		}
	})
}

package storage

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// This virtual test file is the fresh published-develop family original. It
// asserts list-bearing preconditions locally because 4db intentionally has no
// future temporalKeySpec.listBearing helper. Product behavior is exercised only
// through the existing 4db functions and public storage routes.

func tlfTemporalConstraint(label string, keyProps []string, relationship bool) Constraint {
	properties := append(append([]string(nil), keyProps...), "from", "to")
	c := Constraint{Name: "tlf_temporal", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: label, Properties: properties}
	if relationship {
		c.EntityType = ConstraintEntityRelationship
	}
	return c
}

func tlfEngine(t *testing.T, relationship bool) (*BadgerEngine, *NamespacedEngine) {
	t.Helper()
	inner := createTestBadgerEngine(t)
	require.False(t, inner.writeBehindEnabled())
	engine := NewNamespacedEngine(inner, "test")
	if relationship {
		for _, id := range []NodeID{"left", "right"} {
			_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"TLF_ENDPOINT"}})
			require.NoError(t, err)
		}
	}
	return inner, engine
}

func tlfSeed(t *testing.T, engine *NamespacedEngine, c Constraint, id string, properties map[string]interface{}) {
	t.Helper()
	require.NoError(t, validatePropertiesForStorage(properties), "public write accepts the property values")
	if c.EffectiveEntityType() == ConstraintEntityRelationship {
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(id), StartNode: "left", EndNode: "right", Type: c.Label, Properties: properties}))
		return
	}
	_, err := engine.CreateNode(&Node{ID: NodeID(id), Labels: []string{c.Label}, Properties: properties})
	require.NoError(t, err)
}

func tlfColdProperties(t *testing.T, inner *BadgerEngine, c Constraint, id string, expected map[string]interface{}) map[string]interface{} {
	t.Helper()
	var properties map[string]interface{}
	err := inner.withView(func(txn *badger.Txn) error {
		if c.EffectiveEntityType() == ConstraintEntityNode {
			node, err := inner.loadNodeForTemporalTxn(txn, NodeID("test:"+id), false)
			if err != nil {
				return err
			}
			require.NotNil(t, node)
			properties = node.Properties
			return nil
		}
		edgeID := EdgeID("test:" + id)
		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			return err
		}
		return item.Value(func(body []byte) error {
			edge, err := inner.decodeEdgeBodyByID(body, edgeID)
			if err == nil {
				properties = edge.Properties
			}
			return err
		})
	})
	require.NoError(t, err, "physical record decode bypasses object cache")
	require.NoError(t, validatePropertiesForStorage(properties))
	for name, want := range expected {
		switch name {
		case "from", "to":
			continue
		}
		got := properties[name]
		if reflect.TypeOf(want).Kind() == reflect.Slice {
			require.Equal(t, reflect.Slice, reflect.TypeOf(got).Kind())
			equal, lists := equalConstraintLists(want, got)
			require.True(t, lists)
			require.True(t, equal, "cold list %s keeps its logical contents", name)
			if expected, ok := want.([]float64); ok {
				require.IsType(t, []float64{}, got)
				actual := got.([]float64)
				require.Len(t, actual, len(expected))
				for i := range expected {
					require.Equal(t, math.Float64bits(expected[i]), math.Float64bits(actual[i]))
				}
			}
			if _, ok := want.([]int64); ok {
				require.IsType(t, []int64{}, got)
			}
			continue
		}
		if numeric, ok := numericConstraintValue(want); ok {
			gotNumeric, gotOK := numericConstraintValue(got)
			require.True(t, gotOK)
			require.Equal(t, numeric, gotNumeric)
			if float, ok := want.(float64); ok && math.Signbit(float) {
				require.IsType(t, float64(0), got)
				require.Equal(t, math.Float64bits(float), math.Float64bits(got.(float64)))
				require.True(t, math.Signbit(got.(float64)), "negative-zero Signbit survives storage")
			}
			if _, ok := want.(int64); ok {
				require.IsType(t, int64(0), got)
			}
			continue
		}
		require.Equal(t, want, got)
	}
	t.Logf("TLF_COLD id=%s entity=%s properties=%T:%v", id, c.EffectiveEntityType(), properties, properties)
	return properties
}

func tlfKeyPrecondition(t *testing.T, c Constraint, properties map[string]interface{}, requireComposite bool) (temporalKeySpec, interface{}) {
	t.Helper()
	spec, ok := splitTemporalKeySpec(c.Properties)
	require.True(t, ok)
	key, missing := spec.keyValue(properties)
	require.Empty(t, missing, "complete temporal grouping key")
	if requireComposite {
		require.True(t, spec.composite(), "R4 input must be composite")
		values, ok := key.([]interface{})
		require.True(t, ok)
		require.Len(t, values, len(spec.keyProps))
	}
	values := []interface{}{key}
	if spec.composite() {
		values = key.([]interface{})
	}
	listBearing := false
	for _, value := range values {
		kind := reflect.TypeOf(value).Kind()
		if kind == reflect.Slice || kind == reflect.Array {
			listBearing = true
			break
		}
	}
	require.True(t, listBearing, "independent test precondition: original complete key is list-bearing")
	return spec, key
}

func tlfKeys(t *testing.T, c Constraint, pair [2]map[string]interface{}, equal, requireComposite bool) (temporalKeySpec, interface{}, interface{}) {
	t.Helper()
	spec, first := tlfKeyPrecondition(t, c, pair[0], requireComposite)
	_, second := tlfKeyPrecondition(t, c, pair[1], requireComposite)
	require.Equal(t, equal, spec.keyEqual(first, second), "canonical equality precondition")
	t.Logf("TLF_KEY composite=%t equal=%t hashesEqual=%t properties=%v", spec.composite(), equal, spec.keyHash(first) == spec.keyHash(second), c.Properties)
	return spec, first, second
}

func tlfOutcome(t *testing.T, err error, violation bool) {
	t.Helper()
	if !violation {
		require.NoError(t, err)
		return
	}
	require.Error(t, err, "TEMPORAL overlap must be rejected")
	var typed *ConstraintViolationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ConstraintTemporal, typed.Type)
}

func tlfRegister(t *testing.T, inner *BadgerEngine, engine *NamespacedEngine, c Constraint) {
	t.Helper()
	require.NoError(t, ValidateConstraintOnCreationForEngine(engine, c), "empty-label public DDL setup")
	require.NoError(t, engine.GetSchema().AddConstraint(c))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(engine, engine.GetSchema(), []Constraint{c}))
	require.True(t, engine.GetSchema().HasConstraintNamed(c.Name))
	require.True(t, inner.temporalIndexAuthoritative(engine.GetSchema(), c))
	t.Log("TLF_SETUP constraint-first authoritative=true writeBehind=false")
}

func tlfForceScan(t *testing.T, inner *BadgerEngine, engine *NamespacedEngine, c Constraint) {
	t.Helper()
	schema := engine.GetSchema()
	schema.markTemporalIndexCreated(c, false)
	schema.mu.Lock()
	delete(schema.temporalIndexCompleteKeys, temporalIndexCompletenessKey(c))
	schema.mu.Unlock()
	require.False(t, inner.temporalIndexAuthoritative(schema, c))
}

func tlfListPair(order int, signedZero bool) [2]interface{} {
	first, second := interface{}([]int64{1}), interface{}([]float64{1})
	if signedZero {
		first, second = []int64{0}, []float64{math.Copysign(0, -1)}
	}
	if order == 1 {
		first, second = second, first
	}
	return [2]interface{}{first, second}
}

func tlfListProperties(value interface{}, composite bool, startOffset, endOffset time.Duration) map[string]interface{} {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	properties := map[string]interface{}{"key": value, "from": base.Add(startOffset), "to": base.Add(endOffset)}
	if composite {
		properties["tenant"] = "same"
	}
	return properties
}

func tlfListConstraint(composite, relationship bool) Constraint {
	keyProps := []string{"key"}
	if composite {
		keyProps = []string{"tenant", "key"}
	}
	label := "TLF_LIST"
	if relationship {
		label = "TLF_LIST_LINK"
	}
	return tlfTemporalConstraint(label, keyProps, relationship)
}

func TestTemporalListFamilyTemporalListHash(t *testing.T) {
	for _, signedZero := range []bool{false, true} {
		for _, composite := range []bool{false, true} {
			for order := 0; order < 2; order++ {
				t.Run(fmt.Sprintf("signedZero=%t/composite=%t/order=%d", signedZero, composite, order), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c, values := tlfListConstraint(composite, false), tlfListPair(order, signedZero)
					pair := [2]map[string]interface{}{tlfListProperties(values[0], composite, 0, 10*time.Hour), tlfListProperties(values[1], composite, 5*time.Hour, 15*time.Hour)}
					for i, id := range []string{"first", "second"} {
						tlfSeed(t, engine, c, id, pair[i])
						pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
					}
					spec, first, second := tlfKeys(t, c, pair, true, composite)
					t.Log("TLF_HASH current-list-invariant-entered")
					require.Equal(t, spec.keyHash(first), spec.keyHash(second), "equal supported list keys must share one history")
				})
			}
		}
	}
}

func TestTemporalListFamilyTemporalListEnforcement(t *testing.T) {
	for _, composite := range []bool{false, true} {
		for order := 0; order < 2; order++ {
			for _, mode := range []string{"pending", "direct", "transaction", "scan-direct", "scan-transaction"} {
				t.Run(fmt.Sprintf("composite=%t/order=%d/%s", composite, order, mode), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c, values := tlfListConstraint(composite, false), tlfListPair(order, false)
					tlfRegister(t, inner, engine, c)
					first := tlfListProperties(values[0], composite, 0, 10*time.Hour)
					probe := tlfListProperties(values[1], composite, 5*time.Hour, 15*time.Hour)
					if mode == "pending" {
						tx, err := inner.BeginTransaction()
						require.NoError(t, err)
						defer tx.Rollback()
						require.NoError(t, tx.SetNamespace("test"))
						_, err = tx.CreateNode(&Node{ID: "test:first", Labels: []string{c.Label}, Properties: first})
						require.NoError(t, err)
						_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{c.Label}, Properties: probe})
						phase := "stage-second"
						if err == nil {
							phase, err = "commit", tx.Commit()
						}
						t.Logf("TLF_LIST_ENFORCEMENT mode=pending phase=%s err=%v", phase, err)
						tlfOutcome(t, err, true)
						return
					}
					tlfSeed(t, engine, c, "first", first)
					first = tlfColdProperties(t, inner, c, "first", first)
					pair := [2]map[string]interface{}{first, probe}
					spec, _, key := tlfKeys(t, c, pair, true, composite)
					interval, ok := spec.interval(probe)
					require.True(t, ok)
					scanErr := inner.withView(func(txn *badger.Txn) error {
						return inner.legacyScanForTemporalOverlapInTxn(txn, "test", c, key, interval.start, interval.end, interval.hasEnd, "test:second")
					})
					t.Logf("TLF_SCAN list explicit err=%v", scanErr)
					tlfOutcome(t, scanErr, true)
					scanMode := strings.HasPrefix(mode, "scan-")
					if scanMode {
						tlfForceScan(t, inner, engine, c)
					} else {
						history, complete, err := inner.temporalKeyHistoryNodesLatest(context.Background(), "test", c, &Node{ID: "test:second", Labels: []string{c.Label}, Properties: probe})
						require.NoError(t, err)
						require.True(t, complete)
						t.Logf("TLF_HISTORY current-list authoritative=true entries=%d", len(history))
					}
					var err error
					phase := "direct"
					if strings.HasSuffix(mode, "transaction") {
						tx, beginErr := inner.BeginTransaction()
						require.NoError(t, beginErr)
						defer tx.Rollback()
						require.NoError(t, tx.SetNamespace("test"))
						require.NoError(t, tx.SetDeferredConstraintValidation(true))
						_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{c.Label}, Properties: probe})
						require.NoError(t, err, "deferred stage reaches Commit")
						phase, err = "commit", tx.Commit()
					} else {
						_, err = engine.CreateNode(&Node{ID: "second", Labels: []string{c.Label}, Properties: probe})
					}
					t.Logf("TLF_LIST_ENFORCEMENT mode=%s phase=%s authoritative=%t err=%v", mode, phase, !scanMode, err)
					tlfOutcome(t, err, true)
				})
			}
		}
	}
}

func tlfCreationPair(order int, shape string, composite bool) [2]map[string]interface{} {
	values := tlfListPair(order, true)
	first := tlfListProperties(values[0], composite, 0, 10*time.Hour)
	second := tlfListProperties(values[1], composite, 5*time.Hour, 15*time.Hour)
	switch shape {
	case "same":
		second["key"] = first["key"]
	case "unequal":
		second["key"] = []int64{2}
	case "adjacent":
		second["from"] = first["to"]
	}
	return [2]map[string]interface{}{first, second}
}

func tlfExpectedCreationEqual(shape string) bool { return shape != "unequal" }

func TestTemporalListFamilyTemporalCreationNode(t *testing.T) {
	for _, composite := range []bool{false, true} {
		for order := 0; order < 2; order++ {
			for _, shape := range []string{"signed-zero", "same", "unequal", "adjacent"} {
				t.Run(fmt.Sprintf("composite=%t/order=%d/%s", composite, order, shape), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c, pair := tlfListConstraint(composite, false), tlfCreationPair(order, shape, composite)
					for i, id := range []string{"first", "second"} {
						tlfSeed(t, engine, c, id, pair[i])
						pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
					}
					tlfKeys(t, c, pair, tlfExpectedCreationEqual(shape), composite)
					require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
					err := ValidateConstraintOnCreationForEngine(engine, c)
					t.Logf("TLF_NODE_DDL shape=%s err=%v", shape, err)
					require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
					tlfOutcome(t, err, shape == "signed-zero" || shape == "same")
				})
			}
		}
	}
}

type tlfStreamGuard struct {
	*NamespacedEngine
	calls, visits, fallback int
}

func (g *tlfStreamGuard) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	g.calls++
	return g.NamespacedEngine.StreamEdgesByType(ctx, edgeType, func(edge *Edge) error {
		g.visits++
		return visit(edge)
	})
}

func (g *tlfStreamGuard) GetEdgesByType(string) ([]*Edge, error) {
	g.fallback++
	return nil, ErrInvalidData
}

var _ EdgeTypeStreamer = (*tlfStreamGuard)(nil)

func tlfRelationshipDDL(t *testing.T, c Constraint, engine *NamespacedEngine, edges []*Edge, route string) error {
	t.Helper()
	guard := &tlfStreamGuard{NamespacedEngine: engine}
	var err error
	if route == "stream" {
		err = ValidateConstraintOnCreationForEngine(guard, c)
		require.Equal(t, 1, guard.calls)
		require.Equal(t, 2, guard.visits)
	} else {
		require.Len(t, edges, 2)
		err = validateRelTemporalOnCreationForEngine(edges, c)
		require.Zero(t, guard.calls)
	}
	require.Zero(t, guard.fallback)
	t.Logf("TLF_REL_DDL route=%s calls=%d visits=%d fallback=%d err=%v", route, guard.calls, guard.visits, guard.fallback, err)
	return err
}

func TestTemporalListFamilyTemporalCreationRelationship(t *testing.T) {
	for _, composite := range []bool{false, true} {
		for order := 0; order < 2; order++ {
			for _, route := range []string{"stream", "slice"} {
				for _, shape := range []string{"signed-zero", "same", "unequal", "adjacent"} {
					t.Run(fmt.Sprintf("composite=%t/order=%d/%s/%s", composite, order, route, shape), func(t *testing.T) {
						inner, engine := tlfEngine(t, true)
						c, pair := tlfListConstraint(composite, true), tlfCreationPair(order, shape, composite)
						var edges []*Edge
						for i, id := range []string{"first", "second"} {
							tlfSeed(t, engine, c, id, pair[i])
							pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
							edges = append(edges, &Edge{ID: EdgeID("test:" + id), StartNode: "test:left", EndNode: "test:right", Type: c.Label, Properties: pair[i]})
						}
						tlfKeys(t, c, pair, tlfExpectedCreationEqual(shape), composite)
						require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
						err := tlfRelationshipDDL(t, c, engine, edges, route)
						require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
						tlfOutcome(t, err, shape == "signed-zero" || shape == "same")
					})
				}
			}
		}
	}
}

func tlfMixedConstraint(position int, relationship bool) Constraint {
	keyProps := []string{"scalar", "list"}
	if position == 1 {
		keyProps = []string{"list", "scalar"}
	}
	label := "TLF_MIXED"
	if relationship {
		label = "TLF_MIXED_LINK"
	}
	return tlfTemporalConstraint(label, keyProps, relationship)
}

func tlfMixedPair(order int, shape string) [2]map[string]interface{} {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	first := map[string]interface{}{"scalar": int64(0), "list": []int64{1}, "from": base, "to": base.Add(10 * time.Hour)}
	second := map[string]interface{}{"scalar": math.Copysign(0, -1), "list": []float64{1}, "from": base.Add(5 * time.Hour), "to": base.Add(15 * time.Hour)}
	if order == 1 {
		first["scalar"], second["scalar"] = second["scalar"], first["scalar"]
		first["list"], second["list"] = second["list"], first["list"]
	}
	switch shape {
	case "same":
		second["scalar"], second["list"] = first["scalar"], first["list"]
	case "unequal":
		second["list"] = []int64{2}
	case "adjacent":
		second["from"] = first["to"]
	}
	return [2]map[string]interface{}{first, second}
}

func TestTemporalListFamilyTemporalMixedHash(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			t.Run(fmt.Sprintf("position=%d/order=%d", position, order), func(t *testing.T) {
				inner, engine := tlfEngine(t, false)
				c, pair := tlfMixedConstraint(position, false), tlfMixedPair(order, "mixed")
				for i, id := range []string{"first", "second"} {
					tlfSeed(t, engine, c, id, pair[i])
					pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
				}
				spec, first, second := tlfKeys(t, c, pair, true, true)
				t.Log("TLF_HASH mixed-composite-invariant-entered")
				require.Equal(t, spec.keyHash(first), spec.keyHash(second), "equal complete composite/list-bearing keys must share one history")
			})
		}
	}
}

func TestTemporalListFamilyTemporalMixedNodeDDL(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			for _, shape := range []string{"mixed", "same", "unequal", "adjacent"} {
				t.Run(fmt.Sprintf("position=%d/order=%d/%s", position, order, shape), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c, pair := tlfMixedConstraint(position, false), tlfMixedPair(order, shape)
					for i, id := range []string{"first", "second"} {
						tlfSeed(t, engine, c, id, pair[i])
						pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
					}
					tlfKeys(t, c, pair, shape != "unequal", true)
					require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
					err := ValidateConstraintOnCreationForEngine(engine, c)
					t.Logf("TLF_MIXED_NODE_DDL shape=%s err=%v", shape, err)
					require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
					tlfOutcome(t, err, shape == "mixed" || shape == "same")
				})
			}
		}
	}
}

func TestTemporalListFamilyTemporalMixedRelationshipDDL(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			for _, route := range []string{"stream", "slice"} {
				for _, shape := range []string{"mixed", "same", "unequal", "adjacent"} {
					t.Run(fmt.Sprintf("position=%d/order=%d/%s/%s", position, order, route, shape), func(t *testing.T) {
						inner, engine := tlfEngine(t, true)
						c, pair := tlfMixedConstraint(position, true), tlfMixedPair(order, shape)
						var edges []*Edge
						for i, id := range []string{"first", "second"} {
							tlfSeed(t, engine, c, id, pair[i])
							pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
							edges = append(edges, &Edge{ID: EdgeID("test:" + id), StartNode: "test:left", EndNode: "test:right", Type: c.Label, Properties: pair[i]})
						}
						tlfKeys(t, c, pair, shape != "unequal", true)
						require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
						err := tlfRelationshipDDL(t, c, engine, edges, route)
						require.False(t, engine.GetSchema().HasConstraintNamed(c.Name))
						tlfOutcome(t, err, shape == "mixed" || shape == "same")
					})
				}
			}
		}
	}
}

func TestTemporalListFamilyTemporalMixedEnforcement(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			for _, mode := range []string{"direct", "transaction", "scan-direct", "scan-transaction"} {
				for _, shape := range []string{"mixed", "same", "unequal", "adjacent"} {
					t.Run(fmt.Sprintf("position=%d/order=%d/%s/%s", position, order, mode, shape), func(t *testing.T) {
						inner, engine := tlfEngine(t, false)
						c, pair := tlfMixedConstraint(position, false), tlfMixedPair(order, shape)
						tlfRegister(t, inner, engine, c)
						tlfSeed(t, engine, c, "first", pair[0])
						pair[0] = tlfColdProperties(t, inner, c, "first", pair[0])
						spec, _, key := tlfKeys(t, c, pair, shape != "unequal", true)
						interval, ok := spec.interval(pair[1])
						require.True(t, ok)
						scanErr := inner.withView(func(txn *badger.Txn) error {
							return inner.legacyScanForTemporalOverlapInTxn(txn, "test", c, key, interval.start, interval.end, interval.hasEnd, "test:second")
						})
						violation := shape == "mixed" || shape == "same"
						t.Logf("TLF_SCAN mixed explicit shape=%s err=%v", shape, scanErr)
						tlfOutcome(t, scanErr, violation)
						scanMode := strings.HasPrefix(mode, "scan-")
						if scanMode {
							tlfForceScan(t, inner, engine, c)
						} else {
							history, complete, err := inner.temporalKeyHistoryNodesLatest(context.Background(), "test", c, &Node{ID: "test:second", Labels: []string{c.Label}, Properties: pair[1]})
							require.NoError(t, err)
							require.True(t, complete)
							t.Logf("TLF_HISTORY mixed authoritative=true entries=%d", len(history))
						}
						var err error
						phase := "direct"
						if strings.HasSuffix(mode, "transaction") {
							tx, beginErr := inner.BeginTransaction()
							require.NoError(t, beginErr)
							defer tx.Rollback()
							require.NoError(t, tx.SetNamespace("test"))
							require.NoError(t, tx.SetDeferredConstraintValidation(true))
							_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{c.Label}, Properties: pair[1]})
							require.NoError(t, err, "deferred stage reaches Commit")
							phase, err = "commit", tx.Commit()
						} else {
							_, err = engine.CreateNode(&Node{ID: "second", Labels: []string{c.Label}, Properties: pair[1]})
						}
						t.Logf("TLF_MIXED_ENFORCEMENT mode=%s phase=%s authoritative=%t shape=%s err=%v", mode, phase, !scanMode, shape, err)
						tlfOutcome(t, err, violation)
					})
				}
			}
		}
	}
}

func TestTemporalListFamilyTemporalMixedIsCurrent(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			for _, scanMode := range []bool{false, true} {
				t.Run(fmt.Sprintf("position=%d/order=%d/scan=%t", position, order, scanMode), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c, pair := tlfMixedConstraint(position, false), tlfMixedPair(order, "mixed")
					tlfRegister(t, inner, engine, c)
					tlfSeed(t, engine, c, "first", pair[0])
					pair[0] = tlfColdProperties(t, inner, c, "first", pair[0])
					spec, _, key := tlfKeys(t, c, pair, true, true)
					if scanMode {
						tlfForceScan(t, inner, engine, c)
					}
					asOf := pair[0]["from"].(time.Time).Add(time.Hour)
					scanned, err := inner.currentTemporalNodeByScanInNamespace("test", c, key, asOf)
					require.NoError(t, err)
					require.NotNil(t, scanned)
					require.Equal(t, NodeID("test:first"), scanned.ID)
					projected := map[string]interface{}{"scalar": pair[1]["scalar"], "list": pair[1]["list"], "from": pair[0]["from"], "to": pair[0]["to"]}
					projectedKey, missing := spec.keyValue(projected)
					require.Empty(t, missing)
					require.True(t, spec.keyEqual(key, projectedKey))
					for _, properties := range []map[string]interface{}{pair[0], projected} {
						current, err := inner.IsCurrentTemporalNodeInNamespace("test", &Node{ID: "test:first", Labels: []string{c.Label}, Properties: properties}, asOf)
						t.Logf("TLF_CURRENT authoritative=%t scalar=%T:%v current=%t err=%v", !scanMode, properties["scalar"], properties["scalar"], current, err)
						require.NoError(t, err)
						require.True(t, current)
					}
				})
			}
		}
	}
}

func TestTemporalListFamilyTemporalPureScalarHashPreservation(t *testing.T) {
	single, ok := splitTemporalKeySpec([]string{"k", "from", "to"})
	require.True(t, ok)
	composite, ok := splitTemporalKeySpec([]string{"a", "b", "from", "to"})
	require.True(t, ok)
	negative := math.Copysign(0, -1)
	for _, tc := range []struct {
		name string
		spec temporalKeySpec
		key  interface{}
		want string
	}{
		{"single-golden", single, "x", "04ee7de12e412be0b97c4e157eee9f1ba05053a1341bdc7dbb028f45484fc968"},
		{"composite-golden", composite, []interface{}{int64(7), "name"}, "e3c410237d53c78a40d468600ccc0f979db262768e2542236ef734a3beea5ced"},
		{"single-positive-zero", single, int64(0), stableTemporalKeyDigest(float64(0))},
		{"single-negative-zero", single, negative, stableTemporalKeyDigest(negative)},
		{"composite-positive-zero", composite, []interface{}{int64(0), "plain"}, stableTemporalKeyDigest(float64(0), "plain")},
		{"composite-negative-zero", composite, []interface{}{negative, "plain"}, stableTemporalKeyDigest(negative, "plain")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.spec.keyHash(tc.key))
			t.Log("TLF_PRESERVE pure-scalar-history-hash")
		})
	}
}

func tlfLockSnapshot(t *testing.T, c Constraint, values map[string]interface{}) map[uniqueConstraintLockKey]bool {
	t.Helper()
	sm := NewSchemaManager()
	require.NoError(t, sm.AddConstraint(c))
	node := &Node{ID: "test:lock", Labels: []string{c.Label}, Properties: values}
	release, err := sm.lockConstraintKeysOf(context.Background(), "tlf-lock", node)
	require.NoError(t, err)
	sm.uniqueConstraintCommitLocksMu.Lock()
	actual := make(map[uniqueConstraintLockKey]bool)
	for key := range sm.uniqueConstraintCommitLocks {
		actual[key] = true
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	release()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
	sm.uniqueConstraintCommitLocksMu.Unlock()
	return actual
}

func TestTemporalListFamilyConstraintLockPreservation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		kind       ConstraintType
		properties []string
		values     map[string]interface{}
	}{
		{"temporal-scalar-single", ConstraintTemporal, []string{"a", "from", "to"}, map[string]interface{}{"a": math.Copysign(0, -1)}},
		{"temporal-scalar-composite", ConstraintTemporal, []string{"a", "b", "from", "to"}, map[string]interface{}{"a": math.Copysign(0, -1), "b": "plain"}},
		{"temporal-single-list-int", ConstraintTemporal, []string{"a", "from", "to"}, map[string]interface{}{"a": []int64{1}}},
		{"temporal-single-list-float", ConstraintTemporal, []string{"a", "from", "to"}, map[string]interface{}{"a": []float64{1}}},
		{"unique-single", ConstraintUnique, []string{"a"}, map[string]interface{}{"a": int64(1)}},
		{"unique-composite", ConstraintUnique, []string{"a", "b"}, map[string]interface{}{"a": int64(0), "b": []int64{1}}},
		{"node-key-single", ConstraintNodeKey, []string{"a"}, map[string]interface{}{"a": int64(1)}},
		{"node-key-composite", ConstraintNodeKey, []string{"a", "b"}, map[string]interface{}{"a": int64(0), "b": []int64{1}}},
		{"temporal-incomplete", ConstraintTemporal, []string{"a", "b", "from", "to"}, map[string]interface{}{"b": []int64{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Constraint{Name: "tlf_lock", Type: tc.kind, EntityType: ConstraintEntityNode, Label: "TLF_LOCK", Properties: tc.properties}
			actual := tlfLockSnapshot(t, c, tc.values)
			props := c.Properties
			if c.Type == ConstraintTemporal {
				props = props[:len(props)-2]
			}
			values := make([]interface{}, 0, len(props))
			complete := true
			for _, prop := range props {
				value, ok := constraintLockValue(&Node{Properties: tc.values}, prop)
				if !ok {
					complete = false
					break
				}
				values = append(values, value)
			}
			if !complete {
				require.Empty(t, actual)
				return
			}
			expected := uniqueConstraintLockKey{label: c.Label, property: props[0], value: values[0]}
			if len(props) > 1 {
				expected.property = string(c.Type) + ":" + strings.Join(props, ",")
				expected.value = constraintCompositeKey(values)
			}
			require.Equal(t, map[uniqueConstraintLockKey]bool{expected: true}, actual)
			t.Logf("TLF_PRESERVE lock type=%s properties=%v", c.Type, props)
		})
	}
	t.Run("single-list-equivalent-key", func(t *testing.T) {
		c := Constraint{Name: "tlf_lock", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "TLF_LOCK", Properties: []string{"a", "from", "to"}}
		integer := tlfLockSnapshot(t, c, map[string]interface{}{"a": []int64{1}})
		floating := tlfLockSnapshot(t, c, map[string]interface{}{"a": []float64{1}})
		require.Equal(t, integer, floating, "published single-list lock equivalence remains exact")
	})
}

func TestTemporalListFamilyCanonicalNodeDDLControls(t *testing.T) {
	for _, kind := range []ConstraintType{ConstraintUnique, ConstraintNodeKey} {
		for order := 0; order < 2; order++ {
			t.Run(fmt.Sprintf("kind=%s/order=%d", kind, order), func(t *testing.T) {
				_, engine := tlfEngine(t, false)
				values := tlfListPair(order, false)
				for i, id := range []string{"first", "second"} {
					_, err := engine.CreateNode(&Node{ID: NodeID(id), Labels: []string{"TLF_CANONICAL"}, Properties: map[string]interface{}{"key": values[i], "group": "same"}})
					require.NoError(t, err)
				}
				c := Constraint{Name: "tlf_canonical", Type: kind, EntityType: ConstraintEntityNode, Label: "TLF_CANONICAL", Properties: []string{"key", "group"}}
				err := ValidateConstraintOnCreationForEngine(engine, c)
				t.Logf("TLF_CANONICAL_DDL kind=%s order=%d err=%v", kind, order, err)
				require.Error(t, err)
				var typed *ConstraintViolationError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, kind, typed.Type)
			})
		}
	}
}

func TestTemporalListFamilyTemporalListControls(t *testing.T) {
	for _, composite := range []bool{false, true} {
		for order := 0; order < 2; order++ {
			t.Run(fmt.Sprintf("composite=%t/order=%d", composite, order), func(t *testing.T) {
				inner, engine := tlfEngine(t, false)
				c, values := tlfListConstraint(composite, false), tlfListPair(order, false)
				tlfRegister(t, inner, engine, c)
				first := tlfListProperties(values[0], composite, 0, 10*time.Hour)
				tlfSeed(t, engine, c, "first", first)
				different := tlfListProperties([]float64{2}, composite, 5*time.Hour, 15*time.Hour)
				tlfSeed(t, engine, c, "different", different)
				adjacent := tlfListProperties(values[1], composite, 10*time.Hour, 20*time.Hour)
				tlfSeed(t, engine, c, "adjacent", adjacent)
				t.Log("TLF_CONTROL distinct-list and adjacent-window accepted")
			})
		}
	}
}

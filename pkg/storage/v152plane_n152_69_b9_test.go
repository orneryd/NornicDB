package storage

// NOR-287 reproduction (release head d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5).
//
// Scope of this file: CompositeEngine.GetSchema and the composite count reads
// that DB.Stats consumes, when a constituent cannot be admitted
// (accessConstituent returns an error) or returns no schema manager.
//
// The production code is unchanged. The file keeps:
//   - healthy controls that prove the successful empty and populated
//     semantics this ticket must preserve, and
//   - fail-first witnesses for the constituent-admission skip that publishes a
//     partial count or schema as success.
//
// The composite fan-out skip is the same root NOR-312 records for composite
// reads whose participant cannot be opened; the witnesses here exist to keep
// the NOR-287 evidence complete, not to claim a second fix owner.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// errNOR287Admission stands in for the error a fabric statement's
// accessConstituent returns when one constituent's participant cannot be
// opened (pkg/cypher/fabric_participants.go, compositeStatement.router).
var errNOR287Admission = errors.New("nor287: constituent participant cannot be opened")

// nor287NilSchemaEngine admits access but returns no schema manager, the second
// skip branch in CompositeEngine.GetSchema.
type nor287NilSchemaEngine struct{ Engine }

func (nor287NilSchemaEngine) GetSchema() *SchemaManager { return nil }

// nor287CountFaultEngine fails count reads the way a storage engine whose
// durable owner is gone does, without ending the fixture's lifecycle.
type nor287CountFaultEngine struct {
	Engine
	err error
}

func (e nor287CountFaultEngine) NodeCount(context.Context) (int64, error) { return 0, e.err }

func (e nor287CountFaultEngine) EdgeCount(context.Context) (int64, error) { return 0, e.err }

type nor287IndexShape struct {
	Name       string
	Type       string
	Label      string
	EntityType string
	Properties []string
}

type nor287UniqueShape struct {
	Label    string
	Property string
}

// nor287Composite builds the read-write composite the storage manager builds
// for a composite database with local constituents.
func nor287Composite(constituents map[string]Engine) *CompositeEngine {
	names := make(map[string]string, len(constituents))
	modes := make(map[string]string, len(constituents))
	for alias := range constituents {
		names[alias] = alias
		modes[alias] = "read_write"
	}
	return NewCompositeEngine(constituents, names, modes)
}

// nor287DeniedAdmission returns the statement-owned view a composite statement
// runs on: every constituent is admitted except the denied aliases.
func nor287DeniedAdmission(composite *CompositeEngine, denied ...string) *CompositeEngine {
	return composite.WithConstituentAccess(nil, func(alias string, engine Engine, _ bool) (Engine, error) {
		for _, candidate := range denied {
			if alias == candidate {
				return nil, errNOR287Admission
			}
		}
		return engine, nil
	})
}

func nor287IndexShapes(schema *SchemaManager) map[string]nor287IndexShape {
	shapes := make(map[string]nor287IndexShape)
	for _, raw := range nonLookupIndexes(schema.GetIndexes()) {
		index, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := index["name"].(string)
		kind, _ := index["type"].(string)
		label, _ := index["label"].(string)
		entityType, _ := index["entityType"].(string)
		properties := anyToStringSlice(index["properties"])
		sort.Strings(properties)
		shapes[name] = nor287IndexShape{Name: name, Type: kind, Label: label, EntityType: entityType, Properties: properties}
	}
	return shapes
}

func nor287SortedIndexNames(schema *SchemaManager) []string {
	shapes := nor287IndexShapes(schema)
	names := make([]string, 0, len(shapes))
	for name := range shapes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func nor287ConstraintNames(schema *SchemaManager) []string {
	names := make([]string, 0, len(schema.GetAllConstraints()))
	for _, constraint := range schema.GetAllConstraints() {
		names = append(names, constraint.Name)
	}
	sort.Strings(names)
	return names
}

func nor287UniqueShapes(t *testing.T, schema *SchemaManager) map[string]nor287UniqueShape {
	t.Helper()
	constraints, err := schema.GetConstraints(context.Background())
	require.NoError(t, err)
	shapes := make(map[string]nor287UniqueShape, len(constraints))
	for i := range constraints {
		constraint := &constraints[i]
		shapes[constraint.Name] = nor287UniqueShape{Label: constraint.Label, Property: constraint.Property}
	}
	return shapes
}

func nor287AddIndex(t *testing.T, engine Engine, name, label string, properties ...string) {
	t.Helper()
	require.NoError(t, engine.GetSchema().AddPropertyIndex(name, label, properties))
}

func nor287AddUniqueConstraint(t *testing.T, engine Engine, name, label, property string) {
	t.Helper()
	require.NoError(t, engine.GetSchema().AddConstraint(Constraint{
		Name:       name,
		Type:       ConstraintUnique,
		Label:      label,
		Properties: []string{property},
	}))
}

// nor287Namespaced returns two local constituents of the shape the database
// manager builds: a namespaced view of one storage owner per constituent.
func nor287Namespaced(t *testing.T) (Engine, Engine) {
	t.Helper()
	inner := newTestMemoryEngine(t)
	return NewNamespacedEngine(inner, "a"), NewNamespacedEngine(inner, "b")
}

func nor287SeedGraph(t *testing.T, engine Engine, nodes, edges int) {
	t.Helper()
	ids := make([]NodeID, 0, nodes)
	for i := 0; i < nodes; i++ {
		id := NodeID(fmt.Sprintf("n-%d", i))
		ids = append(ids, id)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]any{"i": int64(i)},
		})
		require.NoError(t, err)
	}
	for i := 0; i < edges; i++ {
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(fmt.Sprintf("e-%d", i)),
			StartNode: ids[i],
			EndNode:   ids[i+1],
			Type:      "RELATES_TO",
		}))
	}
}

// nor287SeededComposite is a healthy composite whose constituents hold two
// nodes and one edge, and three nodes and two edges respectively.
func nor287SeededComposite(t *testing.T) *CompositeEngine {
	t.Helper()
	a, b := nor287Namespaced(t)
	nor287SeedGraph(t, a, 2, 1)
	nor287SeedGraph(t, b, 3, 2)
	return nor287Composite(map[string]Engine{"a": a, "b": b})
}

// TestNOR287CompositeSchemaHealthyControls pins the semantics that must survive
// any correction: a healthy empty composite yields a valid empty merged schema,
// and a healthy populated composite yields the full union of its constituents.
func TestNOR287CompositeSchemaHealthyControls(t *testing.T) {
	t.Run("healthy empty constituents merge to an empty schema", func(t *testing.T) {
		composite := nor287Composite(map[string]Engine{"a": newTestMemoryEngine(t), "b": newTestMemoryEngine(t)})

		merged := composite.GetSchema()
		require.NotNil(t, merged)
		require.Empty(t, nor287IndexShapes(merged), "a healthy empty composite has no declared index")
		require.Empty(t, nor287ConstraintNames(merged), "a healthy empty composite has no declared constraint")
	})

	t.Run("healthy populated constituents merge the complete union", func(t *testing.T) {
		a := newTestMemoryEngine(t)
		b := newTestMemoryEngine(t)
		nor287AddIndex(t, a, "idx_a_person_name", "Person", "name")
		nor287AddUniqueConstraint(t, a, "c_a_person_email", "Person", "email")
		nor287AddIndex(t, b, "idx_b_company_age", "Company", "age")
		nor287AddUniqueConstraint(t, b, "c_b_company_tax", "Company", "tax")

		merged := nor287Composite(map[string]Engine{"a": a, "b": b}).GetSchema()
		require.NotNil(t, merged)
		// A unique constraint owns a range index of the same name; the merge
		// republishes that index with its owning constraint.
		require.Equal(t, map[string]nor287IndexShape{
			"idx_a_person_name": {
				Name: "idx_a_person_name", Type: "PROPERTY", Label: "Person", Properties: []string{"name"},
			},
			"idx_b_company_age": {
				Name: "idx_b_company_age", Type: "PROPERTY", Label: "Company", Properties: []string{"age"},
			},
			"c_a_person_email": {
				Name: "c_a_person_email", Type: "RANGE", Label: "Person", EntityType: "NODE", Properties: []string{"email"},
			},
			"c_b_company_tax": {
				Name: "c_b_company_tax", Type: "RANGE", Label: "Company", EntityType: "NODE", Properties: []string{"tax"},
			},
		}, nor287IndexShapes(merged))
		require.Equal(t, []string{"c_a_person_email", "c_b_company_tax"}, nor287ConstraintNames(merged))
		require.Equal(t, map[string]nor287UniqueShape{
			"c_a_person_email": {Label: "Person", Property: "email"},
			"c_b_company_tax":  {Label: "Company", Property: "tax"},
		}, nor287UniqueShapes(t, merged))
	})

	t.Run("closed but admitted constituents still merge their schema", func(t *testing.T) {
		a := newTestMemoryEngine(t)
		closed := newTestMemoryEngine(t)
		nor287AddIndex(t, a, "idx_a_person_name", "Person", "name")
		nor287AddIndex(t, closed, "idx_b_company_age", "Company", "age")
		require.NoError(t, closed.Close())

		merged := nor287Composite(map[string]Engine{"a": a, "b": closed}).GetSchema()
		require.Equal(t, []string{"idx_a_person_name", "idx_b_company_age"}, nor287SortedIndexNames(merged),
			"an admitted constituent's schema is merged whatever its record lifecycle state")
	})
}

// TestNOR287CompositeSchemaAdmissionFailureWitness is the fail-first witness for
// the composite schema swallow: a constituent whose participant cannot be
// admitted disappears from the merged schema, and GetSchema reports success.
func TestNOR287CompositeSchemaAdmissionFailureWitness(t *testing.T) {
	healthy := func(t *testing.T) (Engine, Engine) {
		t.Helper()
		a := newTestMemoryEngine(t)
		b := newTestMemoryEngine(t)
		nor287AddIndex(t, a, "idx_a_person_name", "Person", "name")
		nor287AddUniqueConstraint(t, a, "c_a_person_email", "Person", "email")
		nor287AddIndex(t, b, "idx_b_company_age", "Company", "age")
		nor287AddUniqueConstraint(t, b, "c_b_company_tax", "Company", "tax")
		return a, b
	}

	t.Run("current output equals the healthy single-constituent merge", func(t *testing.T) {
		a, b := healthy(t)
		denied := nor287DeniedAdmission(nor287Composite(map[string]Engine{"a": a, "b": b}), "b").GetSchema()

		aOnly := nor287Composite(map[string]Engine{"a": a}).GetSchema()
		require.Equal(t, nor287IndexShapes(aOnly), nor287IndexShapes(denied),
			"the merged schema of a two-constituent composite whose second participant failed is exactly the schema of the first")
		require.Equal(t, nor287ConstraintNames(aOnly), nor287ConstraintNames(denied))
		require.NotContains(t, nor287IndexShapes(denied), "idx_b_company_age")
		require.NotContains(t, nor287ConstraintNames(denied), "c_b_company_tax")
	})

	t.Run("failed merge must not impersonate a healthy subset", func(t *testing.T) {
		a, b := healthy(t)
		merged := nor287DeniedAdmission(nor287Composite(map[string]Engine{"a": a, "b": b}), "b").GetSchema()
		// Do not demand metadata from a constituent whose access failed, and
		// do not prescribe a new error-returning API. An explicit nil failure
		// is distinguishable; a successful-looking healthy subset is not.
		if merged == nil {
			return
		}
		healthySubset := nor287Composite(map[string]Engine{"a": a}).GetSchema()
		require.NotEqual(t, nor287IndexShapes(healthySubset), nor287IndexShapes(merged),
			"failed two-constituent merge is indistinguishable from a successful one-constituent schema")
	})
}

// TestNOR287CompositeSchemaAllAdmissionFailuresMatchHealthyEmpty characterizes
// the indistinguishable case: when every constituent is denied, the merged
// schema is the same value a healthy empty composite returns.
func TestNOR287CompositeSchemaAllAdmissionFailuresMatchHealthyEmpty(t *testing.T) {
	a := newTestMemoryEngine(t)
	b := newTestMemoryEngine(t)
	nor287AddIndex(t, a, "idx_a_person_name", "Person", "name")
	nor287AddIndex(t, b, "idx_b_company_age", "Company", "age")

	allDenied := nor287DeniedAdmission(nor287Composite(map[string]Engine{"a": a, "b": b}), "a", "b").GetSchema()
	require.NotNil(t, allDenied, "GetSchema has no error channel: it returns a manager, never a failure")
	require.Empty(t, nor287IndexShapes(allDenied))
	require.Empty(t, nor287ConstraintNames(allDenied))
	require.Equal(t,
		nor287IndexShapes(nor287Composite(map[string]Engine{}).GetSchema()),
		nor287IndexShapes(allDenied),
		"a fully failed composite schema is indistinguishable from a healthy empty one")
}

// TestNOR287CompositeSchemaNilConstituentSkipped covers the second skip branch:
// a constituent engine that returns no schema manager is dropped silently.
func TestNOR287CompositeSchemaNilConstituentSkipped(t *testing.T) {
	a := newTestMemoryEngine(t)
	nor287AddIndex(t, a, "idx_a_person_name", "Person", "name")
	nilSchema := nor287NilSchemaEngine{Engine: newTestMemoryEngine(t)}

	merged := nor287Composite(map[string]Engine{"a": a, "b": nilSchema}).GetSchema()
	require.NotNil(t, merged)
	require.Equal(t, []string{"idx_a_person_name"}, nor287SortedIndexNames(merged))
}

// TestNOR287CompositeCountAdmissionFailureWitness is the fail-first witness for
// the count reads DB.Stats consumes: a denied constituent is skipped and the
// partial sum is returned with a nil error, although NodeCount and EdgeCount
// both declare an error result.
func TestNOR287CompositeCountAdmissionFailureWitness(t *testing.T) {
	t.Run("current counts publish the admitted subset", func(t *testing.T) {
		view := nor287DeniedAdmission(nor287SeededComposite(t), "b")
		nodes, err := view.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(2), nodes)
		edges, err := view.EdgeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(1), edges)
	})

	t.Run("node count must report the denied constituent", func(t *testing.T) {
		view := nor287DeniedAdmission(nor287SeededComposite(t), "b")
		nodes, err := view.NodeCount(t.Context())
		require.ErrorIs(t, err, errNOR287Admission, "NodeCount returned %d nodes and no error after a constituent admission failure", nodes)
		require.Zero(t, nodes)
	})

	t.Run("edge count must report the denied constituent", func(t *testing.T) {
		view := nor287DeniedAdmission(nor287SeededComposite(t), "b")
		edges, err := view.EdgeCount(t.Context())
		require.ErrorIs(t, err, errNOR287Admission, "EdgeCount returned %d edges and no error after a constituent admission failure", edges)
		require.Zero(t, edges)
	})
}

// TestNOR287CompositeCountControls pins the count behavior that is not in
// question: healthy totals, propagated constituent read errors and cancellation.
func TestNOR287CompositeCountControls(t *testing.T) {
	t.Run("healthy empty composite", func(t *testing.T) {
		composite := nor287Composite(map[string]Engine{"a": newTestMemoryEngine(t), "b": newTestMemoryEngine(t)})
		nodes, err := composite.NodeCount(t.Context())
		require.NoError(t, err)
		require.Zero(t, nodes)
		edges, err := composite.EdgeCount(t.Context())
		require.NoError(t, err)
		require.Zero(t, edges)
	})

	t.Run("healthy populated composite", func(t *testing.T) {
		composite := nor287SeededComposite(t)
		nodes, err := composite.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(5), nodes)
		edges, err := composite.EdgeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(3), edges)
	})

	t.Run("constituent read failure is propagated", func(t *testing.T) {
		failing := nor287CountFaultEngine{Engine: newTestMemoryEngine(t), err: ErrStorageClosed}
		composite := nor287Composite(map[string]Engine{"a": failing, "b": newTestMemoryEngine(t)})

		nodes, err := composite.NodeCount(t.Context())
		require.Error(t, err, "a constituent read failure must reach the caller")
		require.ErrorIs(t, err, ErrStorageClosed)
		require.Zero(t, nodes)
		require.Contains(t, err.Error(), "constituent 'a'")

		edges, err := composite.EdgeCount(t.Context())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrStorageClosed)
		require.Zero(t, edges)
	})

	t.Run("cancelled request is not answered with a count", func(t *testing.T) {
		a, _ := nor287Namespaced(t)
		nor287SeedGraph(t, a, 1, 0)
		composite := nor287Composite(map[string]Engine{"a": a})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		nodes, err := composite.NodeCount(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, nodes)
	})
}

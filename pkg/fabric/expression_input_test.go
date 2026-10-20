package fabric

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestNativeOwnedExpressionInput(t *testing.T) {
	query := ReadQueryStructure("WITH [x IN [1, 2] | x + 1] AS xs CALL { WITH xs RETURN xs AS kept } RETURN xs, kept")
	if query.Err != nil {
		t.Fatal(query.Err)
	}
	first, call, last := &query.Clauses[0], &query.Clauses[1], &query.Clauses[2]
	for _, clause := range []*QueryClause{first, call, last} {
		if clause.Input.Text() != clause.ExecutionText || clause.Input.facts != query.Input.facts {
			t.Fatalf("clause lost its native source identity: %s", clause.Text)
		}
	}
	item := first.Projection.Items[0]
	if item.Input.Text() != item.Expression || item.Input.facts != query.Input.facts {
		t.Fatal("projection did not retain its native expression view")
	}
	scopes := item.Input.Scopes()
	if len(scopes) != 1 || scopes[0].Declarations[0].Name != "x" {
		t.Fatalf("local scopes: %+v", scopes)
	}
	for _, reference := range item.Input.References() {
		if reference.Name != "x" || !reference.Local {
			t.Fatalf("comprehension reference lost its binding: %+v", reference)
		}
	}
	if got := last.Input.References(); len(got) != 2 || got[0].Name != "xs" || got[1].Name != "kept" || got[0].Local || got[1].Local {
		t.Fatalf("last clause references: %+v", got)
	}
	if got := call.Nested.Clauses[1].Projection.Items[0].Input; got.Text() != "xs" || got.facts != query.Input.facts {
		t.Fatal("nested query reparsed or lost the root source")
	}
	// A local is an input to a narrower operand view. Its declaration belongs
	// to the enclosing comprehension, not to that operand's own expression.
	projection := scopes[0].Projection
	inner := item.Input.Slice(projection.Start, projection.End)
	if got := inner.References(); len(got) != 1 || got[0].Name != "x" || got[0].Local {
		t.Fatalf("narrow view's input reference: %+v", got)
	}
}

func TestNativeOwnedExpressionInputConcurrent(t *testing.T) {
	query := ReadQueryStructure("WITH [reader IN [1, 2] | reader + 1] AS values RETURN values")
	if query.Err != nil {
		t.Fatal(query.Err)
	}
	var workers sync.WaitGroup
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			input := query.Clauses[0].Projection.Items[0].Input
			scopes, references := input.Scopes(), input.References()
			if len(scopes) != 1 || len(references) != 1 || references[0].Name != "reader" || !references[0].Local {
				t.Errorf("concurrent fact view changed: scopes=%+v references=%+v", scopes, references)
			}
			for at := range input.Text() {
				input.ScopesAt(at)
			}
		}()
	}
	workers.Wait()
}

func TestNativeOwnedExpressionInputDuplicateText(t *testing.T) {
	query := ReadQueryStructure("WITH 7 AS x RETURN [x IN [1] | x], x")
	if query.Err != nil {
		t.Fatal(query.Err)
	}
	items := query.Clauses[1].Projection.Items
	inner := items[0].Input.References()
	outer := items[1].Input.References()
	if len(inner) != 1 || !inner[0].Local || len(outer) != 1 || outer[0].Local {
		t.Fatalf("identical name text conflated source occurrences: inner=%+v outer=%+v", inner, outer)
	}
}

func TestNativeOwnedExpressionInputIndexedScopes(t *testing.T) {
	query := ReadQueryStructure("RETURN [x IN [1] | [x IN [x] | x]], [y IN [2] | y]")
	if query.Err != nil {
		t.Fatal(query.Err)
	}
	input := query.Clauses[0].Input
	scopes := input.Scopes()
	for at := range input.Text() {
		var expected []ExpressionScope
		for _, scope := range scopes {
			if scope.Span.Start <= at && at < scope.Span.End {
				expected = append(expected, scope)
			}
		}
		if actual := input.ScopesAt(at); !reflect.DeepEqual(expected, actual) && len(expected)+len(actual) != 0 {
			t.Fatalf("indexed scopes at %d: got %+v want %+v", at, actual, expected)
		}
	}
}

func TestNativeOwnedExpressionInputUnionBranches(t *testing.T) {
	query := ReadQueryStructure("RETURN [x IN [1] | x] AS result UNION ALL RETURN [x IN [2] | x] AS result")
	if query.Err != nil {
		t.Fatal(query.Err)
	}
	branches := query.Branches()
	if len(branches) != 2 {
		t.Fatalf("branches: %d", len(branches))
	}
	for _, branch := range branches {
		if branch.Input.facts != query.Input.facts || len(branch.Input.Scopes()) != 1 {
			t.Fatal("UNION branch lost statement-owned native facts")
		}
	}
}

func TestNativeOwnedQueryOperands(t *testing.T) {
	q := ReadQueryStructure("WITH [1,2] AS values MATCH (n:A) WHERE n.id > 1 RETURN [x IN values | CASE WHEN x = 1 THEN x ELSE 0 END] AS result ORDER BY size(result)")
	if q.Err != nil {
		t.Fatal(q.Err)
	}
	for _, clause := range q.Clauses {
		for _, operand := range clause.Operands() {
			if operand.Input.facts != q.Input.facts {
				t.Fatal("operand changed statement owner")
			}
			if clause.Kind == "MATCH" && !operand.Predicate {
				t.Fatal("WHERE lost its predicate phase")
			}
			for _, child := range operand.Input.ChildExpressions() {
				if child.facts != q.Input.facts {
					t.Fatal("recursive operand changed source owner")
				}
			}
		}
	}
	roles := ReadQueryStructure("WITH [2,3] AS values RETURN [x IN values | x] AS result").BindingRoles()
	if _, local := roles["x"]; local {
		t.Fatalf("expression local became a CALL import: %v", roles)
	}
}

func TestNativeOwnedNestedQueryViews(t *testing.T) {
	for _, source := range []string{
		"RETURN COUNT { WITH 1 AS x RETURN x } AS result",
		"RETURN COLLECT { WITH 1 AS x CALL { RETURN 2 AS y } RETURN x+y AS value UNION RETURN 3 AS value } AS result",
		"RETURN EXISTS { MATCH (n) WHERE n.id=1 } AS result",
		"RETURN COUNT { (n)-->(m) WHERE m.id=1 } AS result",
	} {
		q := ReadQueryStructure(source)
		if q.Err != nil {
			t.Fatalf("%s: %v", source, q.Err)
		}
		queries := q.Input.QueryScopes()
		if len(queries) != 1 {
			t.Fatalf("query scopes in %s: %d", source, len(queries))
		}
		for _, query := range queries {
			if query.Err != nil {
				t.Fatalf("nested %s: %v", source, query.Err)
			}
			if query.Input.facts != q.Input.facts || len(query.Clauses) == 0 {
				t.Fatal("nested query lost original native owner")
			}
			for _, clause := range query.Clauses {
				if clause.Input.facts != q.Input.facts {
					t.Fatal("nested clause was reparsed")
				}
			}
		}
	}
}

func TestNativeSyntaxFactDemand(t *testing.T) {
	for _, source := range []string{"MATCH (n:Person) RETURN n.name LIMIT 10", "MATCH (n:Person) WHERE n.age > 30 RETURN count(n)", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name,b.name LIMIT 20"} {
		if QuerySyntaxDemand(source).Has(NativeAdmissionSyntax) {
			t.Fatalf("scalar physical path requested structural admission: %s", source)
		}
	}
	for _, source := range []string{"WITH true AS unused CALL { WITH true AS value RETURN value } RETURN value", "USE comp.one RETURN 1", "RETURN `x,y`", "RETURN CASE WHEN true THEN 1 END"} {
		if !QuerySyntaxDemand(source).Has(NativeAdmissionSyntax) {
			t.Fatalf("structural admission demand dropped: %s", source)
		}
	}
}

func TestNativeOwnedExecutionClauseBoundary(t *testing.T) {
	q := ReadQueryStructure("WITH 9 AS x OPTIONAL CALL (x) { RETURN x AS result } RETURN x,result")
	if q.Err != nil {
		t.Fatal(q.Err)
	}
	call := q.First("CALL")
	view := call.ExecutionClause()
	if view.Remaining != "" || call.Remaining == "" {
		t.Fatal("clause execution view lost its endpoint")
	}
	if view.Input != call.Input || view.Nested != call.Nested || !view.Optional || !view.Scoped || !reflect.DeepEqual(view.ScopeNames, call.ScopeNames) {
		t.Fatal("clause bounding dropped logical facts")
	}
}

func TestStatementOwnedFunctionArguments(t *testing.T) {
	query := ReadQueryStructure("WITH ' x ' AS x RETURN trim(BOTH ' ' FROM x), coalesce('x,y',size([1,2]))")
	require.NoError(t, query.Err)
	input := query.Input
	for _, sample := range []struct {
		open      int
		arguments []string
	}{
		{strings.Index(input.Text(), "trim(") + 4, []string{"' '", "x"}},
		{strings.Index(input.Text(), "coalesce(") + 8, []string{"'x,y'", "size([1,2])"}},
		{strings.Index(input.Text(), "size(") + 4, []string{"[1,2]"}},
	} {
		arguments, found := input.FunctionArguments(sample.open)
		require.True(t, found)
		var got []string
		for _, argument := range arguments {
			start, end, ok := argument.RangeIn(input)
			require.True(t, ok)
			require.Equal(t, argument.Text(), input.Text()[start:end])
			got = append(got, argument.Text())
		}
		require.Equal(t, sample.arguments, got)
	}
}

func TestStatementOwnedInvocationImportReferences(t *testing.T) {
	query := ReadQueryStructure("MATCH (i)-[r]->(x) WHERE EXISTS { CALL (x) { MATCH (x)<--(y) RETURN y } RETURN y } RETURN x")
	require.NoError(t, query.Err)
	found := false
	for _, nested := range query.Input.QueryScopes() {
		if !nested.Starts("CALL") {
			continue
		}
		found = true
		call := nested.First("CALL")
		require.True(t, call.Scoped)
		require.Equal(t, []string{"x"}, call.ScopeNames)
		declares, present := nested.BindingRoles()["x"]
		require.True(t, present)
		require.False(t, declares, "the structural import reference precedes the child's MATCH declaration")
	}
	require.True(t, found)
	for _, source := range []string{"CALL db.labels() YIELD label RETURN label", "CALL db.labels() YIELD label AS renamed RETURN renamed"} {
		query := ReadQueryStructure(source)
		require.NoError(t, query.Err)
		output := "label"
		if strings.Contains(source, "renamed") {
			output = "renamed"
		}
		require.True(t, query.BindingRoles()[output], source)
	}
}

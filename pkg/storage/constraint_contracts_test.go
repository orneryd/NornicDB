package storage

import (
	"testing"
)

// --- constraintContractEqual ---

func TestConstraintContractEqual_Identical(t *testing.T) {
	a := ConstraintContract{
		Name:              "c1",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Person",
		Definition:        "def1",
		Entries: []ConstraintContractEntry{
			{Kind: "boolean-node", Expression: "n.age > 0"},
		},
	}
	b := ConstraintContract{
		Name:              "c1",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Person",
		Definition:        "def1",
		Entries: []ConstraintContractEntry{
			{Kind: "boolean-node", Expression: "n.age > 0"},
		},
	}
	if !constraintContractEqual(a, b) {
		t.Fatal("expected identical contracts to be equal")
	}
}

func TestConstraintContractEqual_DifferentName(t *testing.T) {
	a := ConstraintContract{Name: "c1"}
	b := ConstraintContract{Name: "c2"}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different names to be unequal")
	}
}

func TestConstraintContractEqual_DifferentEntityType(t *testing.T) {
	a := ConstraintContract{Name: "c1", TargetEntityType: "NODE"}
	b := ConstraintContract{Name: "c1", TargetEntityType: "RELATIONSHIP"}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different entity types to be unequal")
	}
}

func TestConstraintContractEqual_DifferentLabelOrType(t *testing.T) {
	a := ConstraintContract{Name: "c1", TargetLabelOrType: "Person"}
	b := ConstraintContract{Name: "c1", TargetLabelOrType: "Company"}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different labels to be unequal")
	}
}

func TestConstraintContractEqual_DifferentDefinition(t *testing.T) {
	a := ConstraintContract{Name: "c1", Definition: "def1"}
	b := ConstraintContract{Name: "c1", Definition: "def2"}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different definitions to be unequal")
	}
}

func TestConstraintContractEqual_DifferentEntryCount(t *testing.T) {
	a := ConstraintContract{
		Name:    "c1",
		Entries: []ConstraintContractEntry{{Kind: "boolean-node"}},
	}
	b := ConstraintContract{Name: "c1"}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different entry counts to be unequal")
	}
}

func TestConstraintContractEqual_DifferentEntryKind(t *testing.T) {
	a := ConstraintContract{
		Name:    "c1",
		Entries: []ConstraintContractEntry{{Kind: "boolean-node"}},
	}
	b := ConstraintContract{
		Name:    "c1",
		Entries: []ConstraintContractEntry{{Kind: "primitive-node"}},
	}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different entry kinds to be unequal")
	}
}

func TestConstraintContractEqual_DifferentEntryProperties(t *testing.T) {
	a := ConstraintContract{
		Name: "c1",
		Entries: []ConstraintContractEntry{
			{Kind: "primitive-node", Properties: []string{"a", "b"}},
		},
	}
	b := ConstraintContract{
		Name: "c1",
		Entries: []ConstraintContractEntry{
			{Kind: "primitive-node", Properties: []string{"a", "c"}},
		},
	}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different properties to be unequal")
	}
}

func TestConstraintContractEqual_DifferentPropertyCount(t *testing.T) {
	a := ConstraintContract{
		Name: "c1",
		Entries: []ConstraintContractEntry{
			{Kind: "primitive-node", Properties: []string{"a"}},
		},
	}
	b := ConstraintContract{
		Name: "c1",
		Entries: []ConstraintContractEntry{
			{Kind: "primitive-node", Properties: []string{"a", "b"}},
		},
	}
	if constraintContractEqual(a, b) {
		t.Fatal("expected contracts with different property counts to be unequal")
	}
}

func TestConstraintContractEqual_EmptyEntries(t *testing.T) {
	a := ConstraintContract{Name: "c1"}
	b := ConstraintContract{Name: "c1"}
	if !constraintContractEqual(a, b) {
		t.Fatal("expected contracts with no entries to be equal")
	}
}

// --- parseConstraintPattern ---

func TestParseConstraintPattern_Outgoing(t *testing.T) {
	pattern, err := parseConstraintPattern("(n:Person)-[:KNOWS]->(:Person)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "OUTGOING" {
		t.Fatalf("expected OUTGOING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "KNOWS" {
		t.Fatalf("expected KNOWS, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 1 || pattern.TargetLabels[0] != "Person" {
		t.Fatalf("expected TargetLabels=[Person], got %v", pattern.TargetLabels)
	}
}

func TestParseConstraintPattern_OutgoingNoTargetLabel(t *testing.T) {
	pattern, err := parseConstraintPattern("(n)-[:LIKES]->()")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "OUTGOING" {
		t.Fatalf("expected OUTGOING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "LIKES" {
		t.Fatalf("expected LIKES, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 0 {
		t.Fatalf("expected empty target labels, got %v", pattern.TargetLabels)
	}
}

func TestParseConstraintPattern_Incoming(t *testing.T) {
	pattern, err := parseConstraintPattern("(n:Person)<-[:FOLLOWS]-()")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "INCOMING" {
		t.Fatalf("expected INCOMING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "FOLLOWS" {
		t.Fatalf("expected FOLLOWS, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 0 {
		t.Fatalf("expected empty target labels, got %v", pattern.TargetLabels)
	}
}

func TestParseConstraintPattern_Unsupported(t *testing.T) {
	_, err := parseConstraintPattern("gibberish")
	if err == nil {
		t.Fatal("expected error for unsupported pattern")
	}
}

func TestParseConstraintPattern_MultiLabel(t *testing.T) {
	pattern, err := parseConstraintPattern("(n)-[:KNOWS]->(:Person:Employee)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "OUTGOING" {
		t.Fatalf("expected OUTGOING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "KNOWS" {
		t.Fatalf("expected KNOWS, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 2 || pattern.TargetLabels[0] != "Person" || pattern.TargetLabels[1] != "Employee" {
		t.Fatalf("expected TargetLabels=[Person Employee], got %v", pattern.TargetLabels)
	}
}

func TestParseConstraintPattern_VariableOnBothSides(t *testing.T) {
	pattern, err := parseConstraintPattern("(n:Person)-[:KNOWS]->(m:Person)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "OUTGOING" {
		t.Fatalf("expected OUTGOING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "KNOWS" {
		t.Fatalf("expected KNOWS, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 1 || pattern.TargetLabels[0] != "Person" {
		t.Fatalf("expected TargetLabels=[Person], got %v", pattern.TargetLabels)
	}
}

func TestParseConstraintPattern_IncomingWithLabels(t *testing.T) {
	pattern, err := parseConstraintPattern("(n)<-[:MANAGES]-(m:Manager:Admin)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pattern.Direction != "INCOMING" {
		t.Fatalf("expected INCOMING, got %s", pattern.Direction)
	}
	if pattern.RelationType != "MANAGES" {
		t.Fatalf("expected MANAGES, got %s", pattern.RelationType)
	}
	if len(pattern.TargetLabels) != 2 || pattern.TargetLabels[0] != "Manager" || pattern.TargetLabels[1] != "Admin" {
		t.Fatalf("expected TargetLabels=[Manager Admin], got %v", pattern.TargetLabels)
	}
}

// --- parseCountPatternExpression ---

func TestParseCountPatternExpression_Valid(t *testing.T) {
	matched, pattern, comparator, threshold, err := parseCountPatternExpression("COUNT { (n)-[:KNOWS]->() } >= 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if pattern.Direction != "OUTGOING" || pattern.RelationType != "KNOWS" {
		t.Fatalf("unexpected pattern: %+v", pattern)
	}
	if comparator != ">=" {
		t.Fatalf("expected >=, got %s", comparator)
	}
	if threshold != 1 {
		t.Fatalf("expected threshold 1, got %d", threshold)
	}
}

func TestParseCountPatternExpression_NoMatch(t *testing.T) {
	matched, _, _, _, err := parseCountPatternExpression("n.name IN ['Alice']")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatal("expected no match")
	}
}

func TestParseCountPatternExpression_WithMatch(t *testing.T) {
	matched, pattern, comparator, threshold, err := parseCountPatternExpression("COUNT { MATCH (n:Person)-[:LIKES]->(m:Movie) } = 0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if pattern.Direction != "OUTGOING" || pattern.RelationType != "LIKES" || len(pattern.TargetLabels) != 1 || pattern.TargetLabels[0] != "Movie" {
		t.Fatalf("unexpected pattern: %+v", pattern)
	}
	if comparator != "=" || threshold != 0 {
		t.Fatalf("unexpected comparator=%q threshold=%d", comparator, threshold)
	}
}

// --- parseNotExistsPatternExpression ---

func TestParseNotExistsPatternExpression_Valid(t *testing.T) {
	matched, pattern, err := parseNotExistsPatternExpression("NOT EXISTS { (n)-[:OWNS]->(m:Car) }")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if pattern.Direction != "OUTGOING" || pattern.RelationType != "OWNS" || len(pattern.TargetLabels) != 1 || pattern.TargetLabels[0] != "Car" {
		t.Fatalf("unexpected pattern: %+v", pattern)
	}
}

func TestParseNotExistsPatternExpression_NoMatch(t *testing.T) {
	matched, _, err := parseNotExistsPatternExpression("n.age > 0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatal("expected no match")
	}
}

// --- parseEndpointPropertyEqualityExpression ---

func TestParseEndpointPropertyEqualityExpression_Valid(t *testing.T) {
	matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression("startNode(r).region = endNode(r).region")
	if !matched {
		t.Fatal("expected match")
	}
	if leftProp != "region" {
		t.Fatalf("expected leftProp=region, got %s", leftProp)
	}
	if rightProp != "region" {
		t.Fatalf("expected rightProp=region, got %s", rightProp)
	}
}

func TestParseEndpointPropertyEqualityExpression_NoMatch(t *testing.T) {
	matched, _, _ := parseEndpointPropertyEqualityExpression("n.name = 'Alice'")
	if matched {
		t.Fatal("expected no match")
	}
}

// --- parseRelationshipPropertyInExpression ---

func TestParseRelationshipPropertyInExpression_Valid(t *testing.T) {
	matched, property, values, err := parseRelationshipPropertyInExpression("r.status IN ['active', 'pending']")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if property != "status" {
		t.Fatalf("expected property=status, got %s", property)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0] != "active" || values[1] != "pending" {
		t.Fatalf("unexpected values: %v", values)
	}
}

func TestParseRelationshipPropertyInExpression_NoMatch(t *testing.T) {
	matched, _, _, err := parseRelationshipPropertyInExpression("n.age > 0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatal("expected no match")
	}
}

// --- parseRelationshipPropertyComparisonExpression ---

func TestParseRelationshipPropertyComparisonExpression_GreaterThan(t *testing.T) {
	matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression("r.weight > 0.5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if property != "weight" {
		t.Fatalf("expected property=weight, got %s", property)
	}
	if comparator != ">" {
		t.Fatalf("expected comparator=>, got %s", comparator)
	}
	if value != 0.5 {
		t.Fatalf("expected value=0.5, got %v", value)
	}
}

func TestParseRelationshipPropertyComparisonExpression_LessThanEqual(t *testing.T) {
	matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression("r.score <= 100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Fatal("expected match")
	}
	if property != "score" || comparator != "<=" {
		t.Fatalf("unexpected property=%s comparator=%s", property, comparator)
	}
	if value != int64(100) {
		t.Fatalf("expected value=100, got %v (%T)", value, value)
	}
}

func TestParseRelationshipPropertyComparisonExpression_NoMatch(t *testing.T) {
	matched, _, _, _, err := parseRelationshipPropertyComparisonExpression("not a comparison")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatal("expected no match")
	}
}

func TestParseRelationshipPropertyComparisonExpression_InExprExcluded(t *testing.T) {
	// An IN expression like "r.status IN ['a']" should NOT match as a comparison
	matched, _, _, _, err := parseRelationshipPropertyComparisonExpression("r.status IN ['a']")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Fatal("expected IN expression to not match as comparison")
	}
}

// --- compareConstraintExpressionValue ---

func TestCompareConstraintExpressionValue_Equal(t *testing.T) {
	if !compareConstraintExpressionValue(42, "=", int64(42)) {
		t.Fatal("expected 42 = 42")
	}
}

func TestCompareConstraintExpressionValue_NotEqual(t *testing.T) {
	if !compareConstraintExpressionValue(42, "<>", int64(43)) {
		t.Fatal("expected 42 <> 43")
	}
	if !compareConstraintExpressionValue(42, "!=", int64(43)) {
		t.Fatal("expected 42 != 43")
	}
}

func TestCompareConstraintExpressionValue_Greater(t *testing.T) {
	if !compareConstraintExpressionValue(10.0, ">", 5.0) {
		t.Fatal("expected 10 > 5")
	}
	if compareConstraintExpressionValue(5.0, ">", 10.0) {
		t.Fatal("expected 5 not > 10")
	}
}

func TestCompareConstraintExpressionValue_GreaterEqual(t *testing.T) {
	if !compareConstraintExpressionValue(10.0, ">=", 10.0) {
		t.Fatal("expected 10 >= 10")
	}
	if !compareConstraintExpressionValue(11.0, ">=", 10.0) {
		t.Fatal("expected 11 >= 10")
	}
	if compareConstraintExpressionValue(9.0, ">=", 10.0) {
		t.Fatal("expected 9 not >= 10")
	}
}

func TestCompareConstraintExpressionValue_Less(t *testing.T) {
	if !compareConstraintExpressionValue(5.0, "<", 10.0) {
		t.Fatal("expected 5 < 10")
	}
	if compareConstraintExpressionValue(10.0, "<", 5.0) {
		t.Fatal("expected 10 not < 5")
	}
}

func TestCompareConstraintExpressionValue_LessEqual(t *testing.T) {
	if !compareConstraintExpressionValue(10.0, "<=", 10.0) {
		t.Fatal("expected 10 <= 10")
	}
	if !compareConstraintExpressionValue(9.0, "<=", 10.0) {
		t.Fatal("expected 9 <= 10")
	}
	if compareConstraintExpressionValue(11.0, "<=", 10.0) {
		t.Fatal("expected 11 not <= 10")
	}
}

func TestCompareConstraintExpressionValue_UnknownComparator(t *testing.T) {
	if compareConstraintExpressionValue(10, "~", 10) {
		t.Fatal("expected unknown comparator to return false")
	}
}

// --- compareGreaterConstraint / compareLessConstraint ---

func TestCompareGreaterConstraint_Numeric(t *testing.T) {
	if !compareGreaterConstraint(10.0, 5.0) {
		t.Fatal("expected 10.0 > 5.0")
	}
	if compareGreaterConstraint(5.0, 10.0) {
		t.Fatal("expected 5.0 not > 10.0")
	}
}

func TestCompareGreaterConstraint_StringFallback(t *testing.T) {
	if !compareGreaterConstraint("b", "a") {
		t.Fatal("expected 'b' > 'a'")
	}
	if compareGreaterConstraint("a", "b") {
		t.Fatal("expected 'a' not > 'b'")
	}
}

func TestCompareLessConstraint_Numeric(t *testing.T) {
	if !compareLessConstraint(3.0, 7.0) {
		t.Fatal("expected 3.0 < 7.0")
	}
	if compareLessConstraint(7.0, 3.0) {
		t.Fatal("expected 7.0 not < 3.0")
	}
}

func TestCompareLessConstraint_StringFallback(t *testing.T) {
	if !compareLessConstraint("a", "b") {
		t.Fatal("expected 'a' < 'b'")
	}
	if compareLessConstraint("b", "a") {
		t.Fatal("expected 'b' not < 'a'")
	}
}

// --- compareInt ---

func TestCompareInt_AllComparators(t *testing.T) {
	tests := []struct {
		actual     int
		comparator string
		expected   int
		want       bool
	}{
		{5, "=", 5, true},
		{5, "=", 3, false},
		{5, "<>", 3, true},
		{5, "<>", 5, false},
		{5, "!=", 3, true},
		{5, "!=", 5, false},
		{5, ">", 3, true},
		{5, ">", 5, false},
		{5, ">=", 5, true},
		{5, ">=", 6, false},
		{3, "<", 5, true},
		{5, "<", 5, false},
		{5, "<=", 5, true},
		{6, "<=", 5, false},
		{5, "~", 5, false}, // unknown
	}
	for _, tt := range tests {
		got := compareInt(tt.actual, tt.comparator, tt.expected)
		if got != tt.want {
			t.Errorf("compareInt(%d, %q, %d) = %v, want %v", tt.actual, tt.comparator, tt.expected, got, tt.want)
		}
	}
}

// --- evaluatePropertyInExpression ---

func TestEvaluatePropertyInExpression_Found(t *testing.T) {
	if !evaluatePropertyInExpression("active", []interface{}{"active", "pending"}) {
		t.Fatal("expected 'active' to be found in list")
	}
}

func TestEvaluatePropertyInExpression_NotFound(t *testing.T) {
	if evaluatePropertyInExpression("deleted", []interface{}{"active", "pending"}) {
		t.Fatal("expected 'deleted' to not be found in list")
	}
}

func TestEvaluatePropertyInExpression_NilActual(t *testing.T) {
	// CHECK semantics: an absent property satisfies a domain entry; presence
	// is required separately with IS NOT NULL.
	if !evaluatePropertyInExpression(nil, []interface{}{"a", "b"}) {
		t.Fatal("expected nil actual to satisfy the IN predicate")
	}
}

// --- parseContractLiteral ---

func TestParseContractLiteral_String(t *testing.T) {
	val, err := parseContractLiteral("'hello'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %v", val)
	}
}

func TestParseContractLiteral_DoubleQuotedString(t *testing.T) {
	val, err := parseContractLiteral(`"world"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %v", val)
	}
}

func TestParseContractLiteral_Integer(t *testing.T) {
	val, err := parseContractLiteral("42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int64(42) {
		t.Fatalf("expected int64(42), got %v (%T)", val, val)
	}
}

func TestParseContractLiteral_Float(t *testing.T) {
	val, err := parseContractLiteral("3.14")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 3.14 {
		t.Fatalf("expected 3.14, got %v", val)
	}
}

func TestParseContractLiteral_BoolTrue(t *testing.T) {
	val, err := parseContractLiteral("true")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != true {
		t.Fatalf("expected true, got %v", val)
	}
}

func TestParseContractLiteral_BoolFalse(t *testing.T) {
	val, err := parseContractLiteral("FALSE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != false {
		t.Fatalf("expected false, got %v", val)
	}
}

func TestParseContractLiteral_Null(t *testing.T) {
	val, err := parseContractLiteral("null")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

func TestParseContractLiteral_Unsupported(t *testing.T) {
	_, err := parseContractLiteral("some_identifier")
	if err == nil {
		t.Fatal("expected error for unsupported literal")
	}
}

// --- splitTopLevelCSV ---

func TestSplitTopLevelCSV_Simple(t *testing.T) {
	parts := splitTopLevelCSV("a, b, c")
	if len(parts) != 3 || parts[0] != "a" || parts[1] != "b" || parts[2] != "c" {
		t.Fatalf("unexpected parts: %v", parts)
	}
}

func TestSplitTopLevelCSV_QuotedCommas(t *testing.T) {
	parts := splitTopLevelCSV("'hello, world', 'foo'")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
}

func TestSplitTopLevelCSV_NestedBrackets(t *testing.T) {
	parts := splitTopLevelCSV("[1,2], [3,4]")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
}

func TestSplitTopLevelCSV_Empty(t *testing.T) {
	parts := splitTopLevelCSV("")
	if len(parts) != 0 {
		t.Fatalf("expected 0 parts for empty input, got %d", len(parts))
	}
}

// --- GetAllConstraintContracts ---

func TestGetAllConstraintContracts_Empty(t *testing.T) {
	sm := NewSchemaManager()
	contracts := sm.GetAllConstraintContracts()
	if len(contracts) != 0 {
		t.Fatalf("expected 0 contracts, got %d", len(contracts))
	}
}

func TestGetAllConstraintContracts_Sorted(t *testing.T) {
	sm := NewSchemaManager()
	// Add via bundle to get them into the map
	err := sm.AddConstraintContractBundle(ConstraintContract{
		Name:              "z_contract",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Person",
	}, nil, nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sm.AddConstraintContractBundle(ConstraintContract{
		Name:              "a_contract",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Person",
	}, nil, nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	contracts := sm.GetAllConstraintContracts()
	if len(contracts) != 2 {
		t.Fatalf("expected 2 contracts, got %d", len(contracts))
	}
	if contracts[0].Name != "a_contract" || contracts[1].Name != "z_contract" {
		t.Fatalf("expected sorted order, got %s, %s", contracts[0].Name, contracts[1].Name)
	}
}

// --- constraintContractNamespaceForNode / constraintContractNamespaceForEdge ---

func TestConstraintContractNamespaceForNode_Nil(t *testing.T) {
	_, ok := constraintContractNamespaceForNode(nil)
	if ok {
		t.Fatal("expected false for nil node")
	}
}

func TestConstraintContractNamespaceForEdge_Nil(t *testing.T) {
	_, ok := constraintContractNamespaceForEdge(nil)
	if ok {
		t.Fatal("expected false for nil edge")
	}
}

func TestConstraintContractNamespaceForEdge_FromStartNode(t *testing.T) {
	edge := &Edge{
		ID:        "edge1",
		StartNode: "mydb::node1",
		EndNode:   "node2",
	}
	dbName, ok := constraintContractNamespaceForEdge(edge)
	if !ok {
		t.Fatal("expected namespace from start node")
	}
	if dbName != "mydb" {
		t.Fatalf("expected 'mydb', got %s", dbName)
	}
}

func TestConstraintContractNamespaceForEdge_FromEdgeID(t *testing.T) {
	edge := &Edge{
		ID:        "mydb::edge1",
		StartNode: "node1",
		EndNode:   "node2",
	}
	dbName, ok := constraintContractNamespaceForEdge(edge)
	if !ok {
		t.Fatal("expected namespace from edge ID")
	}
	if dbName != "mydb" {
		t.Fatalf("expected 'mydb', got %s", dbName)
	}
}

func TestConstraintContractNamespaceForEdge_FromEndNode(t *testing.T) {
	edge := &Edge{
		ID:        "edge1",
		StartNode: "node1",
		EndNode:   "mydb::node2",
	}
	dbName, ok := constraintContractNamespaceForEdge(edge)
	if !ok {
		t.Fatal("expected namespace from end node")
	}
	if dbName != "mydb" {
		t.Fatalf("expected 'mydb', got %s", dbName)
	}
}

func TestConstraintContractNamespaceForEdge_NoPrefixes(t *testing.T) {
	edge := &Edge{ID: "edge1", StartNode: "node1", EndNode: "node2"}
	_, ok := constraintContractNamespaceForEdge(edge)
	if ok {
		t.Fatal("expected false when no database prefix found")
	}
}

func TestBadgerTransactionEvaluateConstraintContractExpressions(t *testing.T) {
	tx := &BadgerTransaction{
		pendingNodes: map[NodeID]*Node{
			"tenant::start": {ID: "tenant::start", Properties: map[string]interface{}{"region": "west"}},
			"tenant::end":   {ID: "tenant::end", Properties: map[string]interface{}{"region": "west"}},
		},
		deletedNodes: map[NodeID]struct{}{},
	}

	node := &Node{ID: "tenant::n1", Properties: map[string]interface{}{"status": "active"}}
	ok, err := tx.evaluateNodeConstraintContractExpressionLocked(node, "n.status IN ['active', 'pending']")
	if err != nil || !ok {
		t.Fatalf("expected node IN expression to pass, ok=%v err=%v", ok, err)
	}

	ok, err = tx.evaluateNodeConstraintContractExpressionLocked(node, "n.status IN ['disabled']")
	if err != nil || ok {
		t.Fatalf("expected node IN expression to fail without error, ok=%v err=%v", ok, err)
	}

	ok, err = tx.evaluateNodeConstraintContractExpressionLocked(node, "n.status = 'active'")
	if err != nil || !ok {
		t.Fatalf("expected node comparison expression to pass, ok=%v err=%v", ok, err)
	}

	edge := &Edge{ID: "tenant::e1", StartNode: "tenant::start", EndNode: "tenant::end", Properties: map[string]interface{}{"status": "open", "weight": 7}}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "r.status IN ['open', 'closed']")
	if err != nil || !ok {
		t.Fatalf("expected relationship IN expression to pass, ok=%v err=%v", ok, err)
	}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "startNode(r).region = endNode(r).region")
	if err != nil || !ok {
		t.Fatalf("expected endpoint equality expression to pass, ok=%v err=%v", ok, err)
	}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "r.weight >= 7")
	if err != nil || !ok {
		t.Fatalf("expected relationship comparison expression to pass, ok=%v err=%v", ok, err)
	}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "startNode(r) <> endNode(r)")
	if err != nil || !ok {
		t.Fatalf("expected distinct endpoint expression to pass, ok=%v err=%v", ok, err)
	}

	loop := &Edge{ID: "tenant::loop", StartNode: "tenant::start", EndNode: "tenant::start"}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(loop, "startNode(r) <> endNode(r)")
	if err != nil || ok {
		t.Fatalf("expected distinct endpoint expression to fail without error, ok=%v err=%v", ok, err)
	}

	tx.deletedNodes["tenant::end"] = struct{}{}
	_, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "startNode(r).region = endNode(r).region")
	if err == nil {
		t.Fatal("expected missing endpoint error")
	}
}

func TestBadgerTransactionCurrentStateHelpers(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })

	if _, err := engine.CreateNode(&Node{ID: "tenant::n1"}); err != nil {
		t.Fatalf("create n1: %v", err)
	}
	if _, err := engine.CreateNode(&Node{ID: "tenant::n2"}); err != nil {
		t.Fatalf("create n2: %v", err)
	}
	if _, err := engine.CreateNode(&Node{ID: "tenant::n3"}); err != nil {
		t.Fatalf("create n3: %v", err)
	}
	if err := engine.CreateEdge(&Edge{ID: "tenant::out", StartNode: "tenant::n1", EndNode: "tenant::n2", Type: "REL"}); err != nil {
		t.Fatalf("create out edge: %v", err)
	}
	if err := engine.CreateEdge(&Edge{ID: "tenant::in", StartNode: "tenant::n2", EndNode: "tenant::n1", Type: "REL"}); err != nil {
		t.Fatalf("create in edge: %v", err)
	}

	tx := &BadgerTransaction{
		engine: engine,
		pendingNodes: map[NodeID]*Node{
			"tenant::pending": {ID: "tenant::pending", Properties: map[string]interface{}{"v": 1}},
		},
		pendingEdges: map[EdgeID]*Edge{
			"tenant::out":         {ID: "tenant::out", StartNode: "tenant::n3", EndNode: "tenant::n1", Type: "REL"},
			"tenant::pending-out": {ID: "tenant::pending-out", StartNode: "tenant::n1", EndNode: "tenant::n3", Type: "REL"},
			"tenant::pending-in":  {ID: "tenant::pending-in", StartNode: "tenant::n3", EndNode: "tenant::n1", Type: "REL"},
		},
		deletedNodes: map[NodeID]struct{}{"tenant::deleted": {}},
		deletedEdges: map[EdgeID]struct{}{"tenant::in": {}},
	}

	node, err := tx.currentNodeLocked("tenant::pending")
	if err != nil || node == nil || node.ID != "tenant::pending" {
		t.Fatalf("expected pending node, node=%v err=%v", node, err)
	}
	node, err = tx.currentNodeLocked("tenant::deleted")
	if err != nil || node != nil {
		t.Fatalf("expected deleted node to be hidden, node=%v err=%v", node, err)
	}

	edge, err := tx.currentEdgeLocked("tenant::pending-out")
	if err != nil || edge == nil || edge.ID != "tenant::pending-out" {
		t.Fatalf("expected pending edge, edge=%v err=%v", edge, err)
	}
	tx.deletedEdges["tenant::deleted-edge"] = struct{}{}
	edge, err = tx.currentEdgeLocked("tenant::deleted-edge")
	if err != nil || edge != nil {
		t.Fatalf("expected deleted edge to be hidden, edge=%v err=%v", edge, err)
	}

	outgoing, err := tx.currentOutgoingEdgesLocked("tenant::n1")
	if err != nil {
		t.Fatalf("currentOutgoingEdgesLocked: %v", err)
	}
	if !edgeListContains(outgoing, "tenant::pending-out") || edgeListContains(outgoing, "tenant::out") {
		t.Fatalf("unexpected outgoing edges: %+v", outgoing)
	}

	incoming, err := tx.currentIncomingEdgesLocked("tenant::n1")
	if err != nil {
		t.Fatalf("currentIncomingEdgesLocked: %v", err)
	}
	if !edgeListContains(incoming, "tenant::pending-in") || !edgeListContains(incoming, "tenant::out") || edgeListContains(incoming, "tenant::in") {
		t.Fatalf("unexpected incoming edges: %+v", incoming)
	}

	adjacent, err := tx.currentAdjacentEdgesLocked("tenant::n1")
	if err != nil {
		t.Fatalf("currentAdjacentEdgesLocked: %v", err)
	}
	if !edgeListContains(adjacent, "tenant::pending-out") || !edgeListContains(adjacent, "tenant::pending-in") {
		t.Fatalf("unexpected adjacent edges: %+v", adjacent)
	}
}

func edgeListContains(edges []*Edge, id EdgeID) bool {
	for _, edge := range edges {
		if edge != nil && edge.ID == id {
			return true
		}
	}
	return false
}

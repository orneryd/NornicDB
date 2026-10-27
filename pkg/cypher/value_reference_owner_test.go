package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueReferenceNamespacesAndPolicies(t *testing.T) {
	params := map[string]interface{}{
		"row":    map[string]interface{}{"props": map[string]interface{}{"v": "parameter"}, "mixed": map[interface{}]interface{}{"value": int64(7)}},
		"123abc": int64(11), "present": nil,
		"café": map[string]interface{}{"界": int64(19)},
	}
	rows := map[string]interface{}{"row": map[string]interface{}{"props": map[string]interface{}{"v": "row"}}, "present": nil}
	ctx := withValueBindings(withParams(context.Background(), params), rows)
	for _, test := range []struct {
		expression string
		mode       valueReferenceMode
		want       interface{}
		found      bool
	}{
		{"$row.props.v", parameterPath, "parameter", true},
		{"row.props.v", rowPath, "row", true},
		{"$row['props'].v", parameterPath, "parameter", true},
		{"row[\"props\"].v", rowPath, "row", true},
		{"$row.mixed.value", parameterPath, int64(7), true},
		{"$123abc", parameterName, int64(11), true},
		{"$café.界", parameterPath, int64(19), true},
		{"$present", parameterName, nil, true},
		{"$present", parameterPath, nil, true},
		{"present", rowPath, nil, true},
		{"$present.v", parameterPath, nil, false},
		{"$missing", parameterName, nil, false},
		{"$missing.v", parameterPath, nil, false},
		{"missing.v", rowPath, nil, false},
		{"row.props.v", parameterPath, nil, false},
		{"$row.props.v", rowPath, nil, false},
		{"$row.props.v", parameterName, nil, false},
		{"$row['props']", parameterName, nil, false},
		{"$`row`.props", parameterPath, nil, false},
		{"$row.`props`", parameterPath, nil, false},
		{"$row[props]", parameterPath, nil, false},
		{"$row.props.v + 1", parameterPath, nil, false},
		{"$row.props.v()", parameterPath, nil, false},
	} {
		t.Run(test.expression+"/"+map[valueReferenceMode]string{parameterName: "parameter-name", parameterPath: "parameter-path", rowPath: "row-path"}[test.mode], func(t *testing.T) {
			value, found := resolveValueReference(ctx, test.expression, test.mode)
			require.Equal(t, test.found, found)
			require.Equal(t, test.want, value)
		})
	}
}

func TestValueReferencePublicSetNamespaceIsolation(t *testing.T) {
	exec, _ := newTestExecutor(t)
	params := map[string]interface{}{"row": map[string]interface{}{"props": map[string]interface{}{"v": "parameter"}}}
	for _, test := range []struct{ query, want string }{
		{"UNWIND [{props: {v:'row'}}] AS row CREATE (n:RowReferenceOwner) SET n += row.props RETURN n.v AS v", "row"},
		{"CREATE (n:ParameterReferenceOwner) SET n += $row.props RETURN n.v AS v", "parameter"},
	} {
		result, err := exec.Execute(context.Background(), test.query, params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
	}
}

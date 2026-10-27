package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactValueCopiesChangedBranchesAndPreservesTypes(t *testing.T) {
	const credential = "synthetic-value-owned-482719"
	unchanged := map[string]any{"count": int64(7), "fraction": float32(1.25), "enabled": true}
	nested := map[string]any{"password": "synthetic-value-password-72431", "visible": "ordinary"}
	stringsInput := []string{"visible", credential}
	items := []any{nested, stringsInput, credential, int32(9), nil}
	input := map[string]any{"items": items, "unchanged": unchanged, "safe": "visible"}
	want := map[string]any{"items": []any{map[string]any{"password": "synthetic-value-password-72431", "visible": "ordinary"}, []string{"visible", credential}, credential, int32(9), nil}, "unchanged": map[string]any{"count": int64(7), "fraction": float32(1.25), "enabled": true}, "safe": "visible"}

	safe := RedactValue(input, credential).(map[string]any)
	require.Equal(t, want, input, "projection must not change caller input")
	safeItems := safe["items"].([]any)
	require.Equal(t, RedactedPlaceholder(), safeItems[0].(map[string]any)["password"])
	require.Equal(t, []string{"visible", RedactedPlaceholder()}, safeItems[1])
	require.Equal(t, RedactedPlaceholder(), safeItems[2])
	require.Equal(t, int32(9), safeItems[3])
	require.Nil(t, safeItems[4])
	require.Equal(t, unchanged, safe["unchanged"])
	require.Equal(t, reflect.ValueOf(unchanged).UnsafePointer(), reflect.ValueOf(safe["unchanged"]).UnsafePointer(), "unchanged branches stay shared")

	// Every changed ancestor must be a distinct container, including []string.
	safeItems[0].(map[string]any)["visible"] = "projected only"
	safeItems[1].([]string)[0] = "projected only"
	safeItems[3] = int32(10)
	safe["safe"] = "projected only"
	require.Equal(t, want, input)
}

func TestRedactValuePreservesNilAndUnchangedContainers(t *testing.T) {
	var nilMap map[string]any
	var nilList []any
	var nilStrings []string
	for _, input := range []any{nil, nilMap, nilList, nilStrings, int64(5), false} {
		require.Equal(t, input, RedactValue(input))
	}
	for _, input := range []any{map[string]any{"visible": "safe"}, []any{"safe", int64(3)}, []string{"safe"}} {
		safe := RedactValue(input)
		require.Equal(t, input, safe)
		require.Equal(t, reflect.ValueOf(input).UnsafePointer(), reflect.ValueOf(safe).UnsafePointer())
	}
}

func TestRedactValueProtectsNestedNamedFieldsAndURLs(t *testing.T) {
	input := []any{map[string]any{"auth": map[string]any{"password": []any{"synthetic-nested-password"}}, "endpoint": "https://value-user:value-password@example.invalid/path?api_key=value-key", "safe": "visible"}}
	safe := RedactValue(input).([]any)[0].(map[string]any)
	require.Equal(t, RedactedPlaceholder(), safe["auth"].(map[string]any)["password"])
	for _, marker := range []string{"value-user", "value-password", "value-key"} {
		require.NotContains(t, safe["endpoint"], marker)
	}
	require.Contains(t, safe["endpoint"], "example.invalid/path")
	require.Equal(t, "visible", safe["safe"])
	require.Equal(t, []any{"synthetic-nested-password"}, input[0].(map[string]any)["auth"].(map[string]any)["password"])
}

func TestRedactingHandlerStructuredValuesHonorExtraPolicyWithoutMutation(t *testing.T) {
	t.Setenv("NORNICDB_LOG_REDACT_EXTRA", "customer_id")
	input := map[string]any{"rows": []any{map[string]any{"customer_id": "synthetic-customer-id", "password": "synthetic-handler-password", "safe": "line1\r\nline2", "count": int64(7)}}, "strings": []string{"ordinary\r\nvalue", "https://handler-user:handler-password@example.invalid/path"}}
	want := map[string]any{"rows": []any{map[string]any{"customer_id": "synthetic-customer-id", "password": "synthetic-handler-password", "safe": "line1\r\nline2", "count": int64(7)}}, "strings": []string{"ordinary\r\nvalue", "https://handler-user:handler-password@example.invalid/path"}}
	var output bytes.Buffer
	logger := slog.New(newRedactingHandler(slog.NewJSONHandler(&output, nil), redactSetFromEnv()))
	logger.LogAttrs(context.Background(), slog.LevelInfo, "structured", slog.Any("params", input))
	logger.With("params", input).Info("derived")
	require.Equal(t, want, input)
	for _, marker := range []string{"synthetic-customer-id", "synthetic-handler-password", "handler-user", "handler-password"} {
		require.NotContains(t, output.String(), marker)
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		params := record["params"].(map[string]any)
		row := params["rows"].([]any)[0].(map[string]any)
		require.Equal(t, RedactedPlaceholder(), row["customer_id"])
		require.Equal(t, "line1line2", row["safe"])
		require.Equal(t, float64(7), row["count"])
		require.Equal(t, "ordinaryvalue", params["strings"].([]any)[0])
	}
	// Environment extensions belong to logging; generic display remains scoped
	// to its canonical sensitive field policy.
	require.Equal(t, "visible", RedactValue(map[string]any{"customer_id": "visible"}).(map[string]any)["customer_id"])
}

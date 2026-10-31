package heimdall

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONIngestionActionArguments(t *testing.T) {
	const document = `{"action":"store","params":{"v":9007199254740993,"d":2.0}}`
	parsed := parseActionEnvelope(document)
	require.NotNil(t, parsed)
	require.Equal(t, int64(9007199254740993), parsed.Params["v"])
	require.Equal(t, float64(2), parsed.Params["d"])
	plugin := tryParseActionResponse(document)
	require.NotNil(t, plugin)
	require.Equal(t, int64(9007199254740993), plugin.Params["v"])
	require.Equal(t, float64(2), plugin.Params["d"])
}

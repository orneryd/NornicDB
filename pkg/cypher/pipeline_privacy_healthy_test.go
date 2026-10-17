package cypher

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type privacyHealthyFormatter struct{}

func (privacyHealthyFormatter) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("formatted"))
}

func TestPipelinePrivacyReaderHealthyWrappers(t *testing.T) {
	items := []interface{}{int64(2), int64(1), int64(2)}
	require.Equal(t, []interface{}{int64(2), int64(1)}, toSet(items))
	require.Equal(t, []interface{}{int64(1), int64(2)}, flattenList([]interface{}{[]interface{}{int64(1)}, int64(2)}, true))
	require.Equal(t, []interface{}{int64(1), int64(2), int64(2)}, sortedCypherValues(items))
	require.Equal(t, []interface{}{int64(1), int64(2)}, collFlatten(nil, []interface{}{[]interface{}{int64(1)}, int64(2)}, 1))
	require.Empty(t, cleanMapWithCancellation(nil, nil, nil, nil))
	fields := map[string]interface{}{"keep": 1, "remove": 2}
	require.Equal(t, map[string]interface{}{"keep": 1}, cleanMapWithCancellation(fields, []interface{}{"remove"}, nil, nil))
	for _, value := range []interface{}{privacyHealthyFormatter{}, errors.New("bounded error"), []interface{}{new(int)}, uint(3), complex(1, 2)} {
		probe := pipelineCancellation{ctx: context.Background()}
		require.Equal(t, fmt.Sprint(value), pipelineDisplayString(value, &probe))
		var out strings.Builder
		appendEquivalenceGoValue(&out, reflect.ValueOf(value), &probe, false)
		require.Equal(t, fmt.Sprintf("%#v", value), out.String())
	}
	instant := time.Now()
	for _, value := range []interface{}{&instant, &CypherDateTime{Time: instant}, (*CypherVector)(nil), &CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}} {
		ctx, cancel := context.WithCancel(context.Background())
		probe := pipelineCancellation{ctx: ctx}
		require.Equal(t, fmt.Sprint(value), pipelineDisplayString(value, &probe))
		require.NoError(t, probe.err)
		cancel()
	}
	vector := CypherVector{Type: VectorFloat64, Floats: make([]float64, 1024)}
	runPrivacyReaderSamples(t, func(p *pipelineCancellation) interface{} { return vector.stringWithCancellation(p) })
	runPrivacyReaderSamples(t, func(p *pipelineCancellation) interface{} { return vector.coordinatesWithCancellation(p) })
}

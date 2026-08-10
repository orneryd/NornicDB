package ir

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadQrelsRunAndEvaluate(t *testing.T) {
	qrels, err := ReadQrels(strings.NewReader("q1 0 d1 2\nq1 0 d2 1\nq2 0 d3 1\n"))
	require.NoError(t, err)
	run, err := ReadRun(strings.NewReader("q1 Q0 d2 2 0.5 test\nq1 Q0 d1 1 0.9 test\nq2 Q0 miss 1 0.8 test\n"))
	require.NoError(t, err)
	metrics := Evaluate(qrels, run)
	assert.Equal(t, 0.5, metrics.RecallAt100)
	assert.Equal(t, 0.5, metrics.MRRAt10)
	assert.Equal(t, 0.5, metrics.NDCGAt10)
}

func TestReadQrelsAcceptsOfficialBEIRTSV(t *testing.T) {
	qrels, err := ReadQrels(strings.NewReader("query-id\tcorpus-id\tscore\nq1\td1\t2\nq1\td2\t1\n"))
	require.NoError(t, err)
	assert.Equal(t, 2, qrels["q1"]["d1"])
	assert.Equal(t, 1, qrels["q1"]["d2"])
}

func TestReadQrelsRejectsInvalidInput(t *testing.T) {
	_, err := ReadQrels(strings.NewReader("q1 d1\n"))
	require.Error(t, err)
	_, err = ReadRun(strings.NewReader("q1 Q0 d1 bad 0.9 test\n"))
	require.Error(t, err)
}

func TestWriteRunRoundTrip(t *testing.T) {
	var output bytes.Buffer
	require.NoError(t, WriteRun(&output, "q1", []RunResult{{DocumentID: "d2", Score: 0.2}, {DocumentID: "d1", Score: 0.1}}, "dense_prf_dice"))
	run, err := ReadRun(&output)
	require.NoError(t, err)
	assert.Equal(t, []string{"d2", "d1"}, run["q1"])
}

func TestWriteRunRejectsInvalidResults(t *testing.T) {
	var output bytes.Buffer
	require.Error(t, WriteRun(&output, "q1", []RunResult{{DocumentID: "d1"}, {DocumentID: "d1"}}, "variant"))
	require.Error(t, WriteRun(&output, "", nil, "variant"))
}

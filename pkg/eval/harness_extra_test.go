package eval

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHarness_SetThresholds(t *testing.T) {
	h := NewHarness(nil)

	custom := Thresholds{
		Precision10: 0.9,
		Recall10:    0.8,
		MRR:         0.85,
		NDCG10:      0.75,
		HitRate:     0.95,
	}
	h.SetThresholds(custom)

	h.mu.RLock()
	actual := h.thresholds
	h.mu.RUnlock()

	assert.Equal(t, 0.9, actual.Precision10)
	assert.Equal(t, 0.8, actual.Recall10)
	assert.Equal(t, 0.85, actual.MRR)
	assert.Equal(t, 0.75, actual.NDCG10)
	assert.Equal(t, 0.95, actual.HitRate)
}

func TestHarness_AddTestCase(t *testing.T) {
	h := NewHarness(nil)
	assert.Len(t, h.testCases, 0)

	tc := TestCase{
		Name:     "my-test",
		Query:    "hello world",
		Expected: []string{"n1", "n2"},
	}
	h.AddTestCase(tc)
	assert.Len(t, h.testCases, 1)
	assert.Equal(t, "my-test", h.testCases[0].Name)
}

func TestHarness_AddTestCases(t *testing.T) {
	h := NewHarness(nil)

	cases := []TestCase{
		{Name: "t1", Query: "q1", Expected: []string{"n1"}},
		{Name: "t2", Query: "q2", Expected: []string{"n2"}},
		{Name: "t3", Query: "q3", Expected: []string{"n3"}},
	}
	h.AddTestCases(cases)
	assert.Len(t, h.testCases, 3)
}

func TestHarness_LoadSuite_ValidJSON(t *testing.T) {
	suite := TestSuite{
		Name:        "test-suite",
		Description: "A test suite for eval",
		Version:     "1.0",
		Created:     time.Now(),
		TestCases: []TestCase{
			{Name: "tc1", Query: "find everything", Expected: []string{"n1"}},
			{Name: "tc2", Query: "search nodes", Expected: []string{"n2", "n3"}},
		},
	}

	data, err := json.Marshal(suite)
	require.NoError(t, err)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "suite.json")
	err = os.WriteFile(path, data, 0644)
	require.NoError(t, err)

	h := NewHarness(nil)
	err = h.LoadSuite(path)
	require.NoError(t, err)
	assert.Len(t, h.testCases, 2)
	assert.Equal(t, "tc1", h.testCases[0].Name)
	assert.Equal(t, "tc2", h.testCases[1].Name)
}

func TestHarness_LoadSuite_FileNotFound(t *testing.T) {
	h := NewHarness(nil)
	err := h.LoadSuite("/nonexistent/path/suite.json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read")
}

func TestHarness_LoadSuite_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.json")
	err := os.WriteFile(path, []byte("{not valid json}"), 0644)
	require.NoError(t, err)

	h := NewHarness(nil)
	err = h.LoadSuite(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse")
}

func TestHarness_LoadSuite_AppendsToCases(t *testing.T) {
	suite := TestSuite{
		Name: "extra",
		TestCases: []TestCase{
			{Name: "extra-tc", Query: "extra query", Expected: []string{"ex1"}},
		},
	}
	data, _ := json.Marshal(suite)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "extra.json")
	_ = os.WriteFile(path, data, 0644)

	h := NewHarness(nil)
	h.AddTestCase(TestCase{Name: "existing-tc", Query: "existing", Expected: []string{"e1"}})

	err := h.LoadSuite(path)
	require.NoError(t, err)
	assert.Len(t, h.testCases, 2, "LoadSuite should append, not replace")
}

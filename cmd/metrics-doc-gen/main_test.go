package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "counter vec", raw: "CounterVec", want: "counter"},
		{name: "counter", raw: "Counter", want: "counter"},
		{name: "histogram vec", raw: "HistogramVec", want: "histogram"},
		{name: "histogram", raw: "Histogram", want: "histogram"},
		{name: "gauge vec", raw: "GaugeVec", want: "gauge"},
		{name: "gauge", raw: "Gauge", want: "gauge"},
		{name: "summary vec", raw: "SummaryVec", want: "summary"},
		{name: "summary", raw: "Summary", want: "summary"},
		{name: "unknown lowercased", raw: "Timer", want: "timer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeType(tt.raw); got != tt.want {
				t.Fatalf("normalizeType(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestScanFileExtractsMetricsWithNamespaceSubsystemLabelsAndContinuedHelp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog_bolt.go")
	content := `package observability

var catalog = []*CounterVec{
	NewCounterVec(reg,
		MetricOpts{
			Namespace: "customdb",
			Subsystem: "sessions",
			Name:      "opened_total",
			Help: "opened sessions " +
				"across transports",
		},
		[]string{"transport", "database"}),
	NewGauge(reg,
		MetricOpts{
			Name: "customdb_ready",
			Help: "ready flag|escaped later",
		},
		[]string{}),
	NewHistogram(reg,
		MetricOpts{
			Help: "missing name should be ignored",
		},
		[]string{}),
}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	got, err := scanFile(path)
	if err != nil {
		t.Fatalf("scanFile returned error: %v", err)
	}

	want := []metricEntry{
		{
			Name:      "customdb_sessions_opened_total",
			Type:      "counter",
			Help:      "opened sessions across transports",
			Labels:    []string{"transport", "database"},
			Subsystem: "bolt",
		},
		{
			Name:      "customdb_ready",
			Type:      "gauge",
			Help:      "ready flag|escaped later",
			Labels:    nil,
			Subsystem: "bolt",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scanFile entries mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestScanCatalogsOnlyReadsCatalogFilesAndWrapsErrors(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ignored.go"), []byte(`package p`), 0644); err != nil {
		t.Fatalf("write ignored file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog_search.go"), []byte(`package p

func build() {
	requestLabels := []string{"mode", "stage"}
	_ = NewCounterVec(reg,
		MetricOpts{
			Namespace: "nornicdb",
			Subsystem: "search",
			Name:      "duration_seconds",
			Help:      "latency",
		},
		requestLabels)
}
`), 0644); err != nil {
		t.Fatalf("write catalog file: %v", err)
	}

	entries, err := scanCatalogs(dir)
	if err != nil {
		t.Fatalf("scanCatalogs returned error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("scanCatalogs found %d entries, want 1: %#v", len(entries), entries)
	}
	if entries[0].Name != "nornicdb_search_duration_seconds" || entries[0].Type != "counter" || entries[0].Subsystem != "search" {
		t.Fatalf("unexpected entry: %#v", entries[0])
	}
	if !reflect.DeepEqual(entries[0].Labels, []string{"mode", "stage"}) {
		t.Fatalf("labels = %#v, want mode/stage", entries[0].Labels)
	}

	emptyDir := t.TempDir()
	empty, err := scanCatalogs(emptyDir)
	if err != nil {
		t.Fatalf("scanCatalogs empty dir returned error: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("scanCatalogs empty dir = %#v, want no entries", empty)
	}
}

func TestGroupBySubsystemAndSortedKeys(t *testing.T) {
	entries := []metricEntry{
		{Name: "b_one", Subsystem: "bolt"},
		{Name: "s_one", Subsystem: "search"},
		{Name: "b_two", Subsystem: "bolt"},
	}

	grouped := groupBySubsystem(entries)
	if got := len(grouped["bolt"]); got != 2 {
		t.Fatalf("bolt group length = %d, want 2", got)
	}
	if got := grouped["bolt"][0].Name; got != "b_one" {
		t.Fatalf("bolt group preserved order got first %q, want b_one", got)
	}
	if got := sortedKeys(grouped); !reflect.DeepEqual(got, []string{"bolt", "search"}) {
		t.Fatalf("sortedKeys = %#v, want bolt/search", got)
	}
}

func TestScanFileReturnsOpenError(t *testing.T) {
	_, err := scanFile(filepath.Join(t.TempDir(), "missing.go"))
	if err == nil {
		t.Fatal("scanFile missing file returned nil error")
	}
}

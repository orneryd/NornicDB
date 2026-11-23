package cypher

// DIAGNOSTIC (scratch branch only): PD W161 item 8 cause run. Counts every
// real evaluation of the retained where per candidate across all pages of one
// retrieval, records the caller stack of re-evaluations (first 3 per page),
// then repeats the run with each candidate's verdict memoised for the
// retrieval and compares evaluations, time and returned ids.

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pdW161EvalRecorder struct {
	mu        sync.Mutex
	form      string
	page      int
	perForm   map[string]map[string]int // form -> candidate document id -> evaluations
	pageEvals map[string][]int          // form -> evaluations per page
	pageRe    map[string][]int          // form -> re-evaluations per page
	pageStack map[string]int            // form|page -> stacks recorded on that page
	stacks    map[string]int            // stack signature -> re-evaluations seen with it
	firstPage map[string]string         // stack signature -> first form/page seen
}

func newPDW161EvalRecorder() *pdW161EvalRecorder {
	return &pdW161EvalRecorder{
		perForm:   map[string]map[string]int{},
		pageEvals: map[string][]int{},
		pageRe:    map[string][]int{},
		pageStack: map[string]int{},
		stacks:    map[string]int{},
		firstPage: map[string]string{},
	}
}

func (r *pdW161EvalRecorder) setPage(form string, page int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.form, r.page = form, page
	for len(r.pageEvals[form]) <= page {
		r.pageEvals[form] = append(r.pageEvals[form], 0)
		r.pageRe[form] = append(r.pageRe[form], 0)
	}
}

func pdW161CallerSignature(skip int) string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(skip, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	parts := make([]string, 0, 16)
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "nornicdb/pkg/") && !strings.Contains(frame.File, "_test.go") {
			file := frame.File
			if i := strings.Index(file, "/pkg/"); i >= 0 {
				file = file[i+1:]
			}
			fn := frame.Function
			if i := strings.LastIndex(fn, "/"); i >= 0 {
				fn = fn[i+1:]
			}
			parts = append(parts, fmt.Sprintf("%s(%s:%d)", fn, file, frame.Line))
		}
		if !more || len(parts) >= 14 {
			break
		}
	}
	return strings.Join(parts, " <- ")
}

func (r *pdW161EvalRecorder) observe(t *testing.T) func(candidate *storage.Node) {
	return func(candidate *storage.Node) {
		doc, _ := candidate.Properties["document_id"].(string)
		if doc == "" {
			doc = "node:" + string(candidate.ID)
		}
		r.mu.Lock()
		form, page := r.form, r.page
		counts := r.perForm[form]
		if counts == nil {
			counts = map[string]int{}
			r.perForm[form] = counts
		}
		counts[doc]++
		seen := counts[doc]
		if page < len(r.pageEvals[form]) {
			r.pageEvals[form][page]++
		}
		record := false
		key := fmt.Sprintf("%s|%d", form, page)
		if seen > 1 {
			if page < len(r.pageRe[form]) {
				r.pageRe[form][page]++
			}
			if r.pageStack[key] < 3 {
				r.pageStack[key]++
				record = true
			}
		}
		r.mu.Unlock()
		if !record {
			return
		}
		signature := pdW161CallerSignature(3)
		r.mu.Lock()
		r.stacks[signature]++
		if _, ok := r.firstPage[signature]; !ok {
			r.firstPage[signature] = key
		}
		r.mu.Unlock()
		t.Logf("PD_W161_REEVAL item=8 form=%s page=%d doc=%s evaluation=%d stack=%s", form, page, doc, seen, signature)
	}
}

func (r *pdW161EvalRecorder) report(t *testing.T, pass string) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	totals := map[string]int{}
	forms := make([]string, 0, len(r.perForm))
	for form := range r.perForm {
		forms = append(forms, form)
	}
	sort.Strings(forms)
	for _, form := range forms {
		counts := r.perForm[form]
		total := 0
		histogram := map[int]int{}
		maxDoc, maxCount := "", 0
		for doc, c := range counts {
			total += c
			histogram[c]++
			if c > maxCount || (c == maxCount && doc < maxDoc) {
				maxDoc, maxCount = doc, c
			}
		}
		totals[form] = total
		keys := make([]int, 0, len(histogram))
		for k := range histogram {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		hist := make([]string, 0, len(keys))
		for _, k := range keys {
			hist = append(hist, fmt.Sprintf("%dx:%d", k, histogram[k]))
		}
		reTotal := 0
		for _, re := range r.pageRe[form] {
			reTotal += re
		}
		t.Logf("PD_W161_EVALS item=8 pass=%s form=%s evaluations=%d distinct_documents=%d re_evaluations=%d per_document_histogram=%s max_document=%s:%d",
			pass, form, total, len(counts), reTotal, strings.Join(hist, ","), maxDoc, maxCount)
		t.Logf("PD_W161_PAGES item=8 pass=%s form=%s evaluations_per_page=%v re_evaluations_per_page=%v", pass, form, r.pageEvals[form], r.pageRe[form])
	}
	signatures := make([]string, 0, len(r.stacks))
	for signature := range r.stacks {
		signatures = append(signatures, signature)
	}
	sort.Slice(signatures, func(i, j int) bool { return r.stacks[signatures[i]] > r.stacks[signatures[j]] })
	for _, signature := range signatures {
		t.Logf("PD_W161_STACK item=8 pass=%s recorded=%d first=%s stack=%s", pass, r.stacks[signature], r.firstPage[signature], signature)
	}
	return totals
}

func TestPDW161Item8CauseRetainedEvaluations(t *testing.T) {
	schema := []string{"CREATE INDEX pd_w161_item8_doc_id IF NOT EXISTS FOR (n:PDW161Document) ON (n.id)"}
	t.Cleanup(func() {
		retainedWhereDiagnosticObserver = nil
		retainedWhereDiagnosticMemo = false
		pdW161Item8PageHook = nil
	})

	run := func(pass string, memo bool) (map[string]int, []string, []string, float64, float64) {
		recorder := newPDW161EvalRecorder()
		retainedWhereDiagnosticObserver = recorder.observe(t)
		retainedWhereDiagnosticMemo = memo
		pdW161Item8PageHook = recorder.setPage
		storedIDs, directIDs, storedTotal, directTotal := runPDW161Item8(t, "range_index_"+pass, schema)
		retainedWhereDiagnosticObserver = nil
		retainedWhereDiagnosticMemo = false
		pdW161Item8PageHook = nil
		return recorder.report(t, pass), storedIDs, directIDs, float64(storedTotal.Microseconds()) / 1000, float64(directTotal.Microseconds()) / 1000
	}

	plainTotals, plainStored, plainDirect, plainStoredMs, plainDirectMs := run("as_is", false)
	memoTotals, memoStored, memoDirect, memoStoredMs, memoDirectMs := run("memo_per_retrieval", true)
	require.Equal(t, plainStored, memoStored, "memoised stored form must return the same ids")
	require.Equal(t, plainDirect, memoDirect, "memoised typed form must return the same ids")
	t.Logf("PD_W161_BYPASS item=8 typed_evaluations as_is=%d memo=%d stored_evaluations as_is=%d memo=%d typed_total_ms as_is=%.3f memo=%.3f stored_total_ms as_is=%.3f memo=%.3f rows_equal=true rows=%d",
		plainTotals["typed_one_hop_retained_where"], memoTotals["typed_one_hop_retained_where"],
		plainTotals["stored_verdict"], memoTotals["stored_verdict"],
		plainDirectMs, memoDirectMs, plainStoredMs, memoStoredMs, len(plainDirect))
}

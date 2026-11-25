package cypher

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPDW1616MarkSerialWait(t *testing.T) {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	p := &pdw1616Probe{t: t, store: storage.NewNamespacedEngine(engine, "test")}
	setup := p.executor()
	pdw1616Command(t, setup, "CREATE CONSTRAINT pdw_doc FOR (d:PDDocument) REQUIRE d.id IS UNIQUE", nil)
	pdw1616Command(t, setup, "UNWIND range(0, 555) AS i CREATE (:PDDocument {id:'doc-' + toString(i), summary_input:'old', summary_status:'ready'})", nil)
	const writerCount = 7
	const delay = 1250 * time.Millisecond
	var items []any
	for i := 0; i < 333; i++ {
		prev := "old"
		if i < writerCount {
			prev = "stale"
		}
		items = append(items, map[string]any{"id": fmt.Sprintf("doc-%d", i), "previous": prev, "fingerprint": "new"})
	}
	q := pdw1616Stock["mark"].text
	for _, tc := range []struct {
		name, text string
		withWait   bool
	}{
		{"stock-quiet", q, false},
		{"stock-seven-holders", q, true},
		{"omit-eager-lock-seven-holders", strings.Replace(q, "SET n += {}\n", "", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &pdw1616Probe{t: t, store: p.store}
			ctx := t.Context()
			holders := make([]*StorageExecutor, 0, writerCount)
			if tc.name != "stock-quiet" {
				for _, item := range items[:writerCount] {
					holder := probe.executor()
					pdw1616Command(t, holder, "BEGIN", nil)
					pdw1616Command(t, holder, "MATCH (n:PDDocument {id:$id}) SET n.writer=true", item.(map[string]any))
					holders = append(holders, holder)
				}
			}
			contender := probe.executor()
			pdw1616Command(t, contender, "BEGIN", nil)
			var events int
			waiting := make(chan struct{}, writerCount*2)
			restore := storage.SetUniqueKeyLockWaitHookForTesting(func() {
				select {
				case waiting <- struct{}{}:
				default:
				}
			})
			defer restore()
			done := make(chan error, 1)
			start := time.Now()
			go func() {
				_, err := probe.profile("mark-controlled/"+tc.name, contender, tc.text, map[string]any{"items": items}, 1)
				done <- err
			}()
			if tc.withWait {
				for i, holder := range holders {
					select {
					case <-waiting:
						events++
						t.Logf("PDW1616 mark-controlled wait=%d at=%v", i+1, time.Since(start))
					case err := <-done:
						t.Fatalf("mark returned before lock %d: %v", i+1, err)
					case <-time.After(4 * time.Second):
						t.Fatalf("mark did not wait on lock %d", i+1)
					}
					time.Sleep(delay)
					pdw1616Command(t, holder, "ROLLBACK", nil)
				}
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(6 * time.Second):
				t.Fatal("mark did not finish")
			}
			if !tc.withWait {
				require.Empty(t, waiting)
			}
			v := pdw1616Command(t, contender, "MATCH (n:PDDocument {summary_input:'new'}) RETURN count(n)", nil)
			require.Equal(t, [][]any{{int64(333 - writerCount)}}, v.Rows)
			pdw1616Command(t, contender, "ROLLBACK", nil)
			if !tc.withWait {
				for _, holder := range holders {
					_, err := holder.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			}
			t.Logf("PDW1616 mark-controlled case=%s holders=%d waits=%d releaseDelay=%v changed=%v", tc.name, len(holders), events, delay, v.Rows[0][0])
		})
	}
}

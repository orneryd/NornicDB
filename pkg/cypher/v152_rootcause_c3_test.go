package cypher

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func v152RCCServiceEngine(t *testing.T, svc *search.Service) storage.Engine {
	t.Helper()
	field := reflect.ValueOf(svc).Elem().FieldByName("engine")
	require.True(t, field.IsValid())
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(storage.Engine)
}

// TestV152RCCFulltextServiceAnchoredToTransaction logs, after the first
// full-text call, which engine the retained unmanaged service was built on
// (executor.go:1243 search.NewService(e.storage)) and whether a later
// auto-commit request engine passes the SameEventView gate at
// fulltext_documents.go:441. Variant: dropping only the transaction-built
// binding lets the next committed call build a service on the committed engine
// and the rescans stop.
func TestV152RCCFulltextServiceAnchoredToTransaction(t *testing.T) {
	const query = "CALL db.index.fulltext.queryNodes('source_text','hotel') YIELD node,score RETURN node.id,score ORDER BY node.id"
	for _, txFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("txFirst=%t", txFirst), func(t *testing.T) {
			exec, store := fulltextMaintainedFixture(t, 80)
			if txFirst {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				_, err = exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				_, err = exec.Execute(t.Context(), "ROLLBACK", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			svc := exec.search()
			require.NotNil(t, svc)
			engine := v152RCCServiceEngine(t, svc)
			same := storage.SameEventView(exec.storage, engine)
			before := store.scans.Load()
			_, err = exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			_, err = exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			after := store.scans.Load()
			t.Logf("RCC_FULLTEXT owner=pkg/search/fulltext_documents.go:441 txFirst=%t service_engine=%T request_engine=%T same_event_view=%v scans_before=%d scans_after=%d", txFirst, engine, exec.storage, same, before, after)
			if txFirst {
				exec.searchService.Store(nil)
				_, err = exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				rebuilt := v152RCCServiceEngine(t, exec.search())
				vBefore := store.scans.Load()
				_, err = exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				_, err = exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				t.Logf("RCC_FULLTEXT_VARIANT binding_dropped=true service_engine=%T same_event_view=%v scans_before=%d scans_after=%d", rebuilt, storage.SameEventView(exec.storage, rebuilt), vBefore, store.scans.Load())
				require.Equal(t, vBefore, store.scans.Load(), "variant: a service on the committed engine reuses its corpus")
			} else {
				require.True(t, same)
			}
			assert.True(t, same, "the retained service must share the committed request's event view")
			assert.Equal(t, before, after, "committed requests must reuse the corpus")
		})
	}
}

package cypher

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// The probe cancels a real lifetime on observation inside the selected owner.
// Unlike the public-entry fixture, it also exercises private cache producers.
type usePreparationCancelContext struct {
	context.Context
	cancel  context.CancelFunc
	owner   string
	once    sync.Once
	reached bool
}

var usePreparationProbeID atomic.Uint64

func (c *usePreparationCancelContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	var pcs [80]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, c.owner) {
			c.once.Do(func() { c.reached = true; c.cancel() })
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

func TestUsePreparationAPIErrorsAndHealthyRetry(t *testing.T) {
	const query = "MATCH (n) RETURN n SKIP 1 LIMIT 2"
	tests := []struct {
		name string
		run  func(*testing.T, context.Context) error
	}{
		{"Build", func(t *testing.T, ctx context.Context) error {
			ast, err := NewASTBuilder().Build(ctx, query)
			if err == nil {
				require.Equal(t, query, ast.RawQuery)
				require.NotEmpty(t, ast.Clauses)
			} else {
				require.Nil(t, ast)
			}
			return err
		}},
		{"GetAST", func(t *testing.T, ctx context.Context) error {
			info := &QueryInfo{rawQuery: query}
			ast, err := info.GetAST(ctx)
			if err == nil {
				require.NotNil(t, ast)
				require.True(t, info.HasAST())
			} else {
				require.Nil(t, ast)
				require.False(t, info.HasAST())
			}
			return err
		}},
		{"ParseIndexHints", func(t *testing.T, ctx context.Context) error {
			hints, clean, err := ParseIndexHints(ctx, "MATCH (n:UsePrep) USING INDEX n:UsePrep(id) RETURN n")
			if err == nil {
				require.Len(t, hints, 1)
				require.Equal(t, "UsePrep", hints[0].Label)
				require.NotContains(t, clean, "USING")
			} else {
				require.Nil(t, hints)
				require.Empty(t, clean)
			}
			return err
		}},
		{"FindKeywordIndex", func(t *testing.T, ctx context.Context) error {
			index, err := FindKeywordIndex(ctx, query, "RETURN")
			if err == nil {
				require.Equal(t, strings.Index(query, "RETURN"), index)
			} else {
				require.Equal(t, -1, index)
			}
			return err
		}},
		{"ContainsKeyword", func(t *testing.T, ctx context.Context) error {
			found, err := ContainsKeyword(ctx, query, "RETURN")
			require.Equal(t, err == nil, found)
			return err
		}},
		{"SplitByKeyword", func(t *testing.T, ctx context.Context) error {
			parts, err := SplitByKeyword(ctx, "MATCH (a) MATCH (b)", "MATCH")
			if err == nil {
				require.Equal(t, []string{"", "(a) ", "(b)"}, parts)
			} else {
				require.Nil(t, parts)
			}
			return err
		}},
		{"SplitByMatch", func(t *testing.T, ctx context.Context) error {
			parts, err := SplitByMatch(ctx, "MATCH (a) MATCH (b)")
			if err == nil {
				require.Equal(t, []string{"", "(a) ", "(b)"}, parts)
			} else {
				require.Nil(t, parts)
			}
			return err
		}},
		{"SplitByCreate", func(t *testing.T, ctx context.Context) error {
			parts, err := SplitByCreate(ctx, "CREATE (a) CREATE (b)")
			if err == nil {
				require.Equal(t, []string{"", "(a) ", "(b)"}, parts)
			} else {
				require.Nil(t, parts)
			}
			return err
		}},
		{"ExtractLimit", func(t *testing.T, ctx context.Context) error {
			value, found, err := ExtractLimit(ctx, query)
			if err == nil {
				require.Equal(t, 2, value)
				require.True(t, found)
			} else {
				require.Zero(t, value)
				require.False(t, found)
			}
			return err
		}},
		{"ExtractSkip", func(t *testing.T, ctx context.Context) error {
			value, found, err := ExtractSkip(ctx, query)
			if err == nil {
				require.Equal(t, 1, value)
				require.True(t, found)
			} else {
				require.Zero(t, value)
				require.False(t, found)
			}
			return err
		}},
		{"ExtractLimitString", func(t *testing.T, ctx context.Context) error {
			value, err := ExtractLimitString(ctx, query)
			if err == nil {
				require.Equal(t, "2", value)
			} else {
				require.Empty(t, value)
			}
			return err
		}},
		{"ExtractSkipString", func(t *testing.T, ctx context.Context) error {
			value, err := ExtractSkipString(ctx, query)
			if err == nil {
				require.Equal(t, "1", value)
			} else {
				require.Empty(t, value)
			}
			return err
		}},
		{"DetectQueryPattern", func(t *testing.T, ctx context.Context) error {
			info, err := DetectQueryPattern(ctx, query)
			if err == nil {
				require.Equal(t, 2, info.Limit)
			} else {
				require.Equal(t, PatternInfo{}, info)
			}
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, deadline := range []bool{false, true} {
				ctx, cancel := context.WithCancel(t.Context())
				want := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				require.True(t, want == tt.run(t, ctx), "retain exact cancellation/deadline error")
				cancel()
				require.NoError(t, tt.run(t, t.Context()))
			}
		})
	}
}

func TestUsePreparationKeywordCacheRetainsHealthyVerdict(t *testing.T) {
	query := fmt.Sprintf("USE GRAPH limit RETURN 7 AS use_preparation_keyword_cache_probe_%d LIMIT 2", usePreparationProbeID.Add(1))
	key := keywordIndexCacheKey{s: query, keyword: "LIMIT", from: 0}
	_, present := defaultKeywordIndexCache.get(key)
	require.False(t, present)
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &usePreparationCancelContext{Context: parent, cancel: cancel, owner: ".useClauseReferenceEnd"}
	require.Equal(t, -1, cachedKeywordIndexFromDefault(ctx, query, "LIMIT", 0))
	require.True(t, ctx.reached)
	require.Same(t, context.Canceled, parent.Err())
	_, present = defaultKeywordIndexCache.get(key)
	require.False(t, present, "canceled name recognition must not publish a negative verdict")
	want := strings.LastIndex(query, "LIMIT")
	require.Equal(t, want, cachedKeywordIndexFromDefault(t.Context(), query, "LIMIT", 0))
	got, present := defaultKeywordIndexCache.get(key)
	require.True(t, present)
	require.Equal(t, want, got)
}

func TestUsePreparationASTCacheRetriesCanceledBuild(t *testing.T) {
	info := &QueryInfo{rawQuery: "RETURN 7 AS use_preparation_ast_cache_probe"}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &usePreparationCancelContext{Context: parent, cancel: cancel, owner: ".(*ASTBuilder).splitIntoClauses"}
	ast, err := info.GetAST(ctx)
	require.True(t, ctx.reached)
	require.Same(t, context.Canceled, err)
	require.Nil(t, ast)
	require.False(t, info.HasAST())
	healthy, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.Len(t, healthy.Clauses, 1)
	require.True(t, info.HasAST())
	again, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.Same(t, healthy, again)
}

func TestUsePreparationTemplateCachesRejectCanceledVerdicts(t *testing.T) {
	e := &StorageExecutor{}
	suffix := usePreparationProbeID.Add(1)
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	create := fmt.Sprintf("CREATE (use_prep:UsePrep%d {id: 7})", suffix)
	require.False(t, e.pipelineCreateTemplateFor(parent, create).usable)
	_, present := pipelineCreateTemplates.get(create)
	require.False(t, present)
	require.True(t, e.pipelineCreateTemplateFor(t.Context(), create).usable)
	match := fmt.Sprintf("MATCH (use_prep:UsePrep%d) WHERE use_prep.id = 7", suffix)
	require.False(t, e.pipelineNodeMatchTemplateFor(parent, match).usable)
	_, present = pipelineNodeMatchTemplates.get(match)
	require.False(t, present)
	require.True(t, e.pipelineNodeMatchTemplateFor(t.Context(), match).usable)
	merge := fmt.Sprintf("MERGE (use_prep)-[:USE_PREP%d {id: 7}]->(use_other)", suffix)
	require.False(t, e.pipelineMergeTemplateFor(parent, merge).usable)
	_, present = pipelineMergeTemplates.get(merge)
	require.False(t, present)
	require.True(t, e.pipelineMergeTemplateFor(t.Context(), merge).usable)
}

func TestUsePreparationFabricPlanCacheRetriesCanceledNameRecognition(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	catalog := fabric.NewCatalog()
	catalog.Register("union", &fabric.LocationLocal{DBName: "union"})
	query := fmt.Sprintf("USE GRAPH union RETURN 7 AS use_preparation_fabric_probe_%d", usePreparationProbeID.Add(1))
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &usePreparationCancelContext{Context: parent, cancel: cancel, owner: ".useClauseReferenceEnd"}
	fragment, err := e.planFabricQuery(ctx, catalog, query, "union")
	require.True(t, ctx.reached)
	require.Same(t, context.Canceled, err)
	require.Nil(t, fragment)
	_, present := e.fabricPlanCache.Get(query, "union")
	require.False(t, present)
	healthy, err := e.planFabricQuery(t.Context(), catalog, query, "union")
	require.NoError(t, err)
	require.IsType(t, &fabric.FragmentExec{}, healthy)
	cached, present := e.fabricPlanCache.Get(query, "union")
	require.True(t, present)
	require.Same(t, healthy, cached)
}

func TestUsePreparationRowProgramDoesNotCacheCanceledCompilation(t *testing.T) {
	name := fmt.Sprintf("use_preparation_program_probe_%d", usePreparationProbeID.Add(1))
	query := "CASE WHEN " + name + " > 0 THEN " + name + " ELSE 7 END"
	require.Nil(t, rowValueProgramFor(t.Context(), query, nil))
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &usePreparationCancelContext{Context: parent, cancel: cancel, owner: ".keywordIndexFrom"}
	require.Nil(t, rowValueProgramFor(ctx, query, nil))
	require.True(t, ctx.reached)
	program, present := rowValuePrograms.get(query)
	require.True(t, present)
	require.Same(t, rowValueSeen, program)
	healthy := rowValueProgramFor(t.Context(), query, nil)
	require.NotNil(t, healthy)
	require.NotSame(t, rowValueSeen, healthy)
	result, ok, err := healthy.root.eval(t.Context(), &StorageExecutor{}, map[string]interface{}{name: int64(3)}, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), result)
}

func TestUsePreparationCancellationDiagnosticsPreserveIdentityAndRedaction(t *testing.T) {
	query := "CREATE USER private SET PASSWORD 'use-preparation-private-secret'"
	for _, original := range []error{context.Canceled, context.DeadlineExceeded} {
		err := original
		redactAdminCommandError(query, &err)
		require.True(t, original == err)
		redactAdminCommandErrorSource(cyphertext.NormalizeSource(query), query, &err)
		require.True(t, original == err)
		wrapped := fmt.Errorf("use-preparation-private-secret: %w", original)
		redactAdminCommandError(query, &wrapped)
		require.True(t, errors.Is(wrapped, original))
		require.NotContains(t, wrapped.Error(), "use-preparation-private-secret")
	}
}

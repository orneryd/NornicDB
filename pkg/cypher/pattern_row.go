package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strconv"
	"strings"
)

type preparedPatternRowKey struct{}

type patternRowMap struct {
	view       braceView
	keys       []string
	parameters []string
}

type patternRowPlan struct {
	view    braceView
	pattern braceView
	maps    []patternRowMap
	err     error
}

// Only syntax is cached: each reached row supplies fresh values and context.
var patternRowPlans = newBoundedCache[string, *patternRowPlan](512, boundedCacheStringShardIndex)

// bindPatternProperties owns dynamic property values for one occurrence and row.
// The shared element/map readers compile syntax once. The existing materializer
// evaluates each map once per row; native parameters carry the values through
// physical retries and MERGE lookup/create without another producer call.
func (e *StorageExecutor) bindPatternProperties(ctx context.Context, pattern braceView, row map[string]interface{}) (context.Context, braceView, error) {
	if prepared, _ := ctx.Value(preparedPatternRowKey{}).(*cyphertext.Source); prepared == pattern.source {
		return ctx, pattern, nil
	}
	if _, found := pattern.patternProperty(blankRelationshipExpressions(pattern.text())); !found || patternMapsAreConstant(pattern) {
		return ctx, pattern, nil
	}
	plan := e.patternRowPlanFor(pattern, getParamsFromContext(ctx))
	if plan.err != nil {
		return ctx, pattern, plan.err
	}
	if len(plan.maps) == 0 {
		return ctx, pattern, nil
	}
	return plan.bind(ctx, e, pattern, row)
}

// patternRowPlanFor owns cached syntax and caller parameter reservation.
// Collision-specific plans are compiled afresh and never enter the syntax cache.
func (e *StorageExecutor) patternRowPlanFor(pattern braceView, reserved map[string]interface{}) *patternRowPlan {
	plan, cached := patternRowPlans.get(pattern.text())
	if !cached {
		plan = e.compilePatternRow(pattern, nil)
		patternRowPlans.put(pattern.text(), plan)
	}
	if cached {
		copy := *plan
		copy.view = pattern
		copy.pattern = braceView{syntax: pattern.syntax, source: plan.pattern.source.Rebase(plan.view.source, pattern.source)}
		copy.maps = append([]patternRowMap(nil), plan.maps...)
		for i := range copy.maps {
			copy.maps[i].view = braceView{syntax: pattern.syntax, source: copy.maps[i].view.source.Rebase(plan.view.source, pattern.source)}
		}
		plan = &copy
	}
	// A caller may use our usual private names as real query parameters.
	// Compile the same syntax with those names reserved before reading values.
	for _, properties := range plan.maps {
		collision := false
		for _, name := range properties.parameters {
			if _, exists := reserved[name]; exists {
				collision = true
				break
			}
		}
		if collision {
			plan = e.compilePatternRow(pattern, reserved)
			break
		}
	}
	return plan
}

// bind evaluates each property map once against this row and retains the values
// as native parameters for physical retries and MERGE lookup/create.
func (p *patternRowPlan) bind(ctx context.Context, e *StorageExecutor, pattern braceView, row map[string]interface{}) (context.Context, braceView, error) {
	outer := getParamsFromContext(ctx)
	rowCtx := ctx
	if row != nil {
		scope := valueBindingsLayer(ctx, len(row))
		for name, value := range row {
			scope[name] = value
		}
		rowCtx = withValueBindings(ctx, scope)
	}
	params := make(map[string]interface{}, len(outer)+len(p.maps))
	for name, value := range outer {
		params[name] = value
	}
	for _, properties := range p.maps {
		values, err := e.patternRowPropertyValues(rowCtx, properties.view)
		if err != nil {
			return ctx, pattern, err
		}
		for index, key := range properties.keys {
			params[properties.parameters[index]] = values[key]
		}
	}
	rowCtx = withParams(rowCtx, params)
	rowCtx = withBraceView(rowCtx, p.pattern)
	return context.WithValue(rowCtx, preparedPatternRowKey{}, p.pattern.source), p.pattern, nil
}

// compilePatternRow uses the canonical element walk and property-key reader.
// No property expression, bound row or runtime parameter value is evaluated.
func (e *StorageExecutor) compilePatternRow(view braceView, reserved map[string]interface{}) *patternRowPlan {
	pattern := view.text()
	plan := &patternRowPlan{view: view, pattern: view}
	var edits []labelRewriteEdit
	nextBinding := 0
	walk := labelExpressionRewriter{query: pattern, braceView: view}
	walk.visitElement = func(open, close int, _ bool) error {
		element := view.window(open+1, close)
		span, found := element.patternProperty(blankRelationshipExpressions(element.text()))
		if !found {
			return nil
		}
		propertyView := element.window(span.Start, span.End)
		keys, err := e.patternRowPropertyKeys(propertyView)
		if err != nil {
			return err
		}
		properties := patternRowMap{view: propertyView}
		for key := range keys {
			properties.keys = append(properties.keys, key)
		}
		sort.Strings(properties.keys)
		parameters := make(map[string]string, len(properties.keys))
		for _, key := range properties.keys {
			name := ""
			for {
				name = "__nornic_pattern_property_" + strconv.Itoa(nextBinding)
				nextBinding++
				if _, exists := reserved[name]; !exists && !strings.Contains(pattern, name) {
					break
				}
			}
			properties.parameters = append(properties.parameters, name)
			parameters[key] = name
		}
		entries, valid := propertyView.keyedEntries()
		if !valid {
			return propertyView.admission()
		}
		// Preserve every original separator and key. Only the values become
		// parameter references; entry projection remains the Source owner's
		// endpoint projection, with no map text or entry grammar rebuilt.
		for _, entry := range entries {
			base := open + 1 + span.Start
			edits = append(edits, labelRewriteEdit{start: base + entry.record.Value.Start, end: base + entry.record.Value.End, text: "$" + parameters[entry.key()]})
		}
		plan.maps = append(plan.maps, properties)
		return nil
	}
	var unusedPredicates []rewritePredicate
	plan.err = walk.patternElements(0, len(pattern), labelPatternCreate, &unusedPredicates)
	if plan.err == nil {
		_, rewrite := applyLabelRewriteEdits(pattern, edits, false)
		plan.pattern = view.rewrite(rewrite)
	}
	return plan
}

func (e *StorageExecutor) patternRowPropertyKeys(text braceView) (map[string]interface{}, error) {
	return patternPropertyKeys(context.Background(), text, true)
}

func (e *StorageExecutor) patternRowPropertyValues(ctx context.Context, text braceView) (map[string]interface{}, error) {
	return e.parsePropertyMap(ctx, text, true)
}

// A relationship template's property phase starts only with real endpoints.
func pipelineBoundMergeEndpoints(template *pipelineMergeTemplate, row pipelineRow) bool {
	if template.shape == nil {
		return false
	}
	_, start := pipelineBoundNode(row, template.shape.startContent.text())
	_, end := pipelineBoundNode(row, template.shape.endContent.text())
	return start && end
}

func (e *StorageExecutor) preparePatternNodeProperties(ctx context.Context, pattern braceView) (context.Context, map[string]interface{}, error) {
	rowCtx, prepared, err := e.bindPatternProperties(ctx, pattern, nil)
	if err != nil {
		return ctx, nil, err
	}
	_, text := splitNodePatternProperties(prepared)
	values, err := e.patternRowPropertyValues(rowCtx, text)
	return rowCtx, values, err
}

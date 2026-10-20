package cypher

import (
	"context"
	"sort"
	"strconv"
	"strings"
)

type preparedPatternRowKey struct{}

type patternRowMap struct {
	text       string
	keys       []string
	parameters []string
}

type patternRowPlan struct {
	pattern string
	maps    []patternRowMap
	err     error
}

// Only syntax is cached: each reached row supplies fresh values and context.
var patternRowPlans = newBoundedCache[string, *patternRowPlan](512, boundedCacheStringShardIndex)

// preparePatternRow owns dynamic property values for one occurrence and row.
// The shared element/map readers compile syntax once. The existing materializer
// evaluates each map once per row; native parameters carry the values through
// physical retries and MERGE lookup/create without another producer call.
func (e *StorageExecutor) preparePatternRow(ctx context.Context, pattern string, row map[string]interface{}) (context.Context, string, error) {
	if prepared, _ := ctx.Value(preparedPatternRowKey{}).(string); prepared == pattern {
		return ctx, pattern, nil
	}
	if !strings.Contains(pattern, "{") || patternMapsAreConstant(pattern) {
		return ctx, pattern, nil
	}
	plan, cached := patternRowPlans.get(pattern)
	if !cached {
		plan = e.compilePatternRow(pattern, nil)
		patternRowPlans.put(pattern, plan)
	}
	outer := getParamsFromContext(ctx)
	// A caller may use our usual private names as real query parameters.
	// Compile the same syntax with those names reserved before reading values.
	for _, properties := range plan.maps {
		collision := false
		for _, name := range properties.parameters {
			if _, exists := outer[name]; exists {
				collision = true
				break
			}
		}
		if collision {
			plan = e.compilePatternRow(pattern, outer)
			break
		}
	}
	if plan.err != nil {
		return ctx, pattern, plan.err
	}
	if len(plan.maps) == 0 {
		return ctx, pattern, nil
	}
	rowCtx := ctx
	if row != nil {
		scope := valueBindingsLayer(ctx, len(row))
		for name, value := range row {
			scope[name] = value
		}
		rowCtx = withValueBindings(ctx, scope)
	}
	params := make(map[string]interface{}, len(outer)+len(plan.maps))
	for name, value := range outer {
		params[name] = value
	}
	for _, properties := range plan.maps {
		values, err := e.patternRowPropertyValues(rowCtx, properties.text)
		if err != nil {
			return ctx, pattern, err
		}
		for index, key := range properties.keys {
			params[properties.parameters[index]] = values[key]
		}
	}
	rowCtx = withParams(rowCtx, params)
	return context.WithValue(rowCtx, preparedPatternRowKey{}, plan.pattern), plan.pattern, nil
}

// compilePatternRow uses the canonical element walk and property-key reader.
// No property expression, bound row or runtime parameter value is evaluated.
func (e *StorageExecutor) compilePatternRow(pattern string, reserved map[string]interface{}) *patternRowPlan {
	plan := &patternRowPlan{pattern: pattern}
	var edits []labelRewriteEdit
	nextBinding := 0
	walk := labelExpressionRewriter{query: pattern}
	walk.visitElement = func(open, close int, _ bool) error {
		_, propertyText := splitNodePatternProperties("(" + pattern[open+1:close] + ")")
		if propertyText == "" {
			return nil
		}
		propertyStart := close - len(propertyText)
		propertyEnd := findMatchingDelimiter(pattern[:close], propertyStart, '{', '}')
		if propertyEnd < propertyStart {
			return nil // The existing syntax validator owns malformed maps.
		}
		propertyText = pattern[propertyStart : propertyEnd+1]
		keys, err := e.patternRowPropertyKeys(propertyText)
		if err != nil {
			return err
		}
		properties := patternRowMap{text: propertyText}
		for key := range keys {
			properties.keys = append(properties.keys, key)
		}
		sort.Strings(properties.keys)
		var text strings.Builder
		text.WriteByte('{')
		separator := ""
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
			text.WriteString(separator)
			text.WriteString(cypher25NameText(key))
			text.WriteString(":$")
			text.WriteString(name)
			separator = ","
		}
		text.WriteByte('}')
		plan.maps = append(plan.maps, properties)
		edits = append(edits, labelRewriteEdit{start: propertyStart, end: propertyEnd + 1, text: text.String()})
		return nil
	}
	var unusedPredicates []string
	plan.err = walk.patternElements(0, len(pattern), labelPatternCreate, &unusedPredicates)
	if plan.err == nil {
		plan.pattern, _ = applyLabelRewriteEdits(pattern, edits, false)
	}
	return plan
}

func (e *StorageExecutor) patternRowPropertyKeys(text string) (map[string]interface{}, error) {
	return patternPropertyKeys(context.Background(), text, true)
}

func (e *StorageExecutor) patternRowPropertyValues(ctx context.Context, text string) (map[string]interface{}, error) {
	return e.parsePropertyMap(ctx, text, true)
}

// A relationship template's property phase starts only with real endpoints.
func pipelineBoundMergeEndpoints(template *pipelineMergeTemplate, row pipelineRow) bool {
	if template.shape == nil {
		return false
	}
	_, start := pipelineBoundNode(row, template.shape.startContent)
	_, end := pipelineBoundNode(row, template.shape.endContent)
	return start && end
}

func (e *StorageExecutor) preparePatternNodeProperties(ctx context.Context, pattern string) (context.Context, map[string]interface{}, error) {
	rowCtx, prepared, err := e.preparePatternRow(ctx, pattern, nil)
	if err != nil {
		return ctx, nil, err
	}
	_, text := splitNodePatternProperties(prepared)
	values, err := e.patternRowPropertyValues(rowCtx, text)
	return rowCtx, values, err
}

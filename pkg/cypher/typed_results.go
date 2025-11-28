// Typed query results for NornicDB.
// Provides compile-time type safety for common query patterns.

package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// TypedExecuteResult wraps query results with typed row access.
type TypedExecuteResult[T any] struct {
	Columns []string
	Rows    []T
	Stats   *QueryStats
}

// TypedExecute executes a Cypher query and decodes results into typed structs.
// This is a top-level function (not a method) to avoid polluting the executor interface.
//
// Usage:
//
//	result, err := TypedExecute[MemoryNode](ctx, executor, "MATCH (n:Memory) RETURN n", nil)
//	for _, node := range result.Rows {
//	    fmt.Println(node.Title, node.Content)
//	}
func TypedExecute[T any](ctx context.Context, exec *StorageExecutor, cypher string, params map[string]interface{}) (*TypedExecuteResult[T], error) {
	// Execute the raw query
	rawResult, err := exec.Execute(ctx, cypher, params)
	if err != nil {
		return nil, err
	}

	// Decode rows into typed structs
	typedRows := make([]T, 0, len(rawResult.Rows))
	for _, row := range rawResult.Rows {
		var decoded T
		if err := decodeRow(rawResult.Columns, row, &decoded); err != nil {
			return nil, fmt.Errorf("failed to decode row: %w", err)
		}
		typedRows = append(typedRows, decoded)
	}

	return &TypedExecuteResult[T]{
		Columns: rawResult.Columns,
		Rows:    typedRows,
		Stats:   rawResult.Stats,
	}, nil
}

// decodeRow decodes a row of values into a struct using column names as field mappings.
// Supports struct tags: `cypher:"column_name"` or `json:"column_name"`
func decodeRow(columns []string, values []interface{}, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	destElem := destVal.Elem()

	// Handle single value case (scalar return)
	if len(columns) == 1 && len(values) == 1 {
		if destElem.Kind() != reflect.Struct {
			// Direct assignment for scalar types
			return assignValue(destElem, values[0])
		}
	}

	// Handle map return (common for node properties)
	if len(values) == 1 {
		if m, ok := values[0].(map[string]interface{}); ok {
			return decodeMap(m, destElem)
		}
	}

	// Handle struct decoding from columns
	if destElem.Kind() == reflect.Struct {
		return decodeStruct(columns, values, destElem)
	}

	return fmt.Errorf("unsupported destination type: %v", destElem.Kind())
}

// decodeStruct decodes column/value pairs into a struct
func decodeStruct(columns []string, values []interface{}, destElem reflect.Value) error {
	destType := destElem.Type()

	// Build field mapping
	fieldMap := make(map[string]int)
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)

		// Check cypher tag first, then json tag, then field name
		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if name != "" {
				// Handle json tag with options like `json:"name,omitempty"`
				if idx := strings.Index(name, ","); idx != -1 {
					name = name[:idx]
				}
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}

		fieldMap[name] = i
	}

	// Map columns to fields
	for i, col := range columns {
		if i >= len(values) {
			break
		}

		// Normalize column name (handle n.property notation)
		colName := col
		if idx := strings.LastIndex(col, "."); idx != -1 {
			colName = col[idx+1:]
		}
		colName = strings.ToLower(colName)

		fieldIdx, ok := fieldMap[colName]
		if !ok {
			continue // Skip unmapped columns
		}

		field := destElem.Field(fieldIdx)
		if !field.CanSet() {
			continue
		}

		if err := assignValue(field, values[i]); err != nil {
			return fmt.Errorf("field %s: %w", col, err)
		}
	}

	return nil
}

// decodeMap decodes a map into a struct
func decodeMap(m map[string]interface{}, destElem reflect.Value) error {
	destType := destElem.Type()

	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		fieldVal := destElem.Field(i)

		if !fieldVal.CanSet() {
			continue
		}

		// Check cypher tag first, then json tag, then field name
		name := field.Tag.Get("cypher")
		if name == "" {
			name = field.Tag.Get("json")
			if name != "" {
				if idx := strings.Index(name, ","); idx != -1 {
					name = name[:idx]
				}
			}
		}
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}

		// Try exact match first, then lowercase
		val, ok := m[name]
		if !ok {
			val, ok = m[strings.ToLower(name)]
		}
		if !ok {
			val, ok = m[field.Name]
		}
		if !ok {
			continue
		}

		if err := assignValue(fieldVal, val); err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
	}

	return nil
}

// assignValue assigns a value to a reflect.Value with type conversion
func assignValue(field reflect.Value, val interface{}) error {
	if val == nil {
		return nil
	}

	valReflect := reflect.ValueOf(val)

	// Handle time.Time specially
	if field.Type() == reflect.TypeOf(time.Time{}) {
		switch v := val.(type) {
		case time.Time:
			field.Set(reflect.ValueOf(v))
			return nil
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				t, err = time.Parse("2006-01-02 15:04:05", v)
			}
			if err != nil {
				return fmt.Errorf("cannot parse time: %v", v)
			}
			field.Set(reflect.ValueOf(t))
			return nil
		case int64:
			field.Set(reflect.ValueOf(time.Unix(v, 0)))
			return nil
		}
	}

	// Direct assignment if types match
	if valReflect.Type().AssignableTo(field.Type()) {
		field.Set(valReflect)
		return nil
	}

	// Type conversion
	if valReflect.Type().ConvertibleTo(field.Type()) {
		field.Set(valReflect.Convert(field.Type()))
		return nil
	}

	// Handle numeric conversions
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := val.(type) {
		case float64:
			field.SetInt(int64(v))
			return nil
		case float32:
			field.SetInt(int64(v))
			return nil
		case int:
			field.SetInt(int64(v))
			return nil
		case int64:
			field.SetInt(v)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch v := val.(type) {
		case int:
			field.SetFloat(float64(v))
			return nil
		case int64:
			field.SetFloat(float64(v))
			return nil
		case float64:
			field.SetFloat(v)
			return nil
		case float32:
			field.SetFloat(float64(v))
			return nil
		}
	case reflect.String:
		field.SetString(fmt.Sprintf("%v", val))
		return nil
	case reflect.Bool:
		switch v := val.(type) {
		case bool:
			field.SetBool(v)
			return nil
		case int:
			field.SetBool(v != 0)
			return nil
		case int64:
			field.SetBool(v != 0)
			return nil
		}
	case reflect.Slice:
		// Handle slice assignment
		if valReflect.Kind() == reflect.Slice {
			newSlice := reflect.MakeSlice(field.Type(), valReflect.Len(), valReflect.Len())
			for i := 0; i < valReflect.Len(); i++ {
				if err := assignValue(newSlice.Index(i), valReflect.Index(i).Interface()); err != nil {
					return err
				}
			}
			field.Set(newSlice)
			return nil
		}
	}

	return fmt.Errorf("cannot assign %T to %v", val, field.Type())
}

// ============================================
// Common typed structs for NornicDB
// ============================================

// MemoryNode represents a memory/knowledge node with decay support.
type MemoryNode struct {
	ID          string    `cypher:"id" json:"id"`
	Title       string    `cypher:"title" json:"title"`
	Content     string    `cypher:"content" json:"content"`
	Type        string    `cypher:"type" json:"type"`
	Tags        []string  `cypher:"tags" json:"tags"`
	Weight      float64   `cypher:"weight" json:"weight"`
	Decay       float64   `cypher:"decay" json:"decay"`
	CreatedAt   time.Time `cypher:"created_at" json:"created_at"`
	UpdatedAt   time.Time `cypher:"updated_at" json:"updated_at"`
	LastAccess  time.Time `cypher:"last_access" json:"last_access"`
	AccessCount int64     `cypher:"access_count" json:"access_count"`
}

// MemoryEdge represents an edge between memory nodes with decay.
type MemoryEdge struct {
	ID          string    `cypher:"id" json:"id"`
	Source      string    `cypher:"source" json:"source"`
	Target      string    `cypher:"target" json:"target"`
	Type        string    `cypher:"type" json:"type"`
	Weight      float64   `cypher:"weight" json:"weight"`
	Decay       float64   `cypher:"decay" json:"decay"`
	CreatedAt   time.Time `cypher:"created_at" json:"created_at"`
	LastUpdated time.Time `cypher:"last_updated" json:"last_updated"`
}

// EmbeddingChunk represents a chunk with embedding data.
type EmbeddingChunk struct {
	ID        string    `cypher:"id" json:"id"`
	ParentID  string    `cypher:"parent_id" json:"parent_id"`
	Index     int       `cypher:"chunk_index" json:"chunk_index"`
	Text      string    `cypher:"text" json:"text"`
	Embedding []float32 `cypher:"embedding" json:"embedding"`
}

// SearchResult represents a search result with score.
type SearchResult struct {
	ID         string  `cypher:"id" json:"id"`
	Score      float64 `cypher:"score" json:"score"`
	Title      string  `cypher:"title" json:"title"`
	Content    string  `cypher:"content" json:"content"`
	Similarity float64 `cypher:"similarity" json:"similarity"`
}

// NodeCount represents a count aggregation result.
type NodeCount struct {
	Label string `cypher:"label" json:"label"`
	Count int64  `cypher:"count" json:"count"`
}

// ============================================
// Helper functions
// ============================================

// First returns the first row or zero value if empty.
func (r *TypedExecuteResult[T]) First() (T, bool) {
	if len(r.Rows) == 0 {
		var zero T
		return zero, false
	}
	return r.Rows[0], true
}

// IsEmpty returns true if no rows were returned.
func (r *TypedExecuteResult[T]) IsEmpty() bool {
	return len(r.Rows) == 0
}

// Count returns the number of rows.
func (r *TypedExecuteResult[T]) Count() int {
	return len(r.Rows)
}

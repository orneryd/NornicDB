# Autocomplete Plugin Feature

## Overview

The Autocomplete Plugin provides intelligent, database-aware Cypher query autocomplete suggestions. Unlike generic autocomplete, this feature uses actual database schema information (labels, properties, relationship types) to provide context-aware suggestions.

## Architecture

### Backend Components

1. **Plugin Action**: `heimdall_watcher_autocomplete_suggest`
   - Location: `plugins/heimdall/plugin.go` (Watcher plugin)
   - Queries the database for:
     - All node labels (`CALL db.labels() YIELD label`)
     - All relationship types (`CALL db.relationshipTypes() YIELD relationshipType`)
     - Sample properties from nodes (limited to 50 for performance)

2. **API Endpoint**: `/api/bifrost/autocomplete`
   - Location: `pkg/heimdall/handler.go`
   - Method: POST
   - Request: `{ "query": "MATCH (n", "cursor_position": 10 }`
   - Response: `{ "suggestion": "...", "schema": { "labels": [...], "properties": [...], "relTypes": [...] } }`

3. **SLM Integration**
   - If the action doesn't provide a suggestion, the handler uses the SLM to generate one
   - The SLM receives the current query plus schema context (labels, properties, relTypes)
   - Prompt includes instructions to add `LIMIT 25` if missing

### Frontend Components

1. **QueryAutocomplete Component**
   - Location: `ui/src/components/browser/QueryAutocomplete.tsx`
   - Calls `/api/bifrost/autocomplete` endpoint
   - Debounced (800ms) to avoid excessive API calls
   - Displays suggestions in a dropdown below the query editor

2. **Integration**
   - Integrated into `QueryPanel.tsx`
   - Toggleable via lightning bolt icon
   - Keyboard navigation support (Arrow keys, Enter, Escape)

## How It Works

1. **User Types Query**: User starts typing a Cypher query (e.g., `MATCH (n`)

2. **Debounce**: After 800ms pause, the component sends the partial query to `/api/bifrost/autocomplete`

3. **Backend Processing**:
   - Action queries database for schema (labels, properties, relTypes)
   - If SLM is available, generates completion using schema context
   - Returns both the suggestion and schema info

4. **Suggestion Display**: 
   - If a valid suggestion is returned, it appears in a dropdown
   - User can accept via click, Enter key, or continue typing

## Benefits

✅ **Database-Aware**: Uses actual labels, properties, and relationship types from your database  
✅ **Context-Aware**: Suggestions are based on what actually exists in your graph  
✅ **Intelligent**: SLM generates completions with full schema context  
✅ **Performance**: Debounced requests prevent excessive API calls  
✅ **User-Friendly**: Keyboard navigation and toggleable feature  

## API Details

### Endpoint

```
POST /api/bifrost/autocomplete
```

### Request

```json
{
  "query": "MATCH (n",
  "cursor_position": 10  // optional
}
```

### Response

```json
{
  "suggestion": "MATCH (n) RETURN n LIMIT 25",
  "schema": {
    "labels": ["Person", "File", "Document"],
    "properties": ["name", "age", "title"],
    "relTypes": ["KNOWS", "OWNS", "AUTHORED"]
  }
}
```

## Example Flow

1. User types: `MATCH (n:Per`
2. Backend queries: Gets all labels, finds "Person" exists
3. SLM generates: `MATCH (n:Person) RETURN n LIMIT 25`
4. UI displays suggestion
5. User presses Enter → Query is completed

## Future Enhancements

- Property value suggestions based on context
- Relationship pattern suggestions
- Multi-line query support
- Syntax error detection and correction
- Query optimization suggestions


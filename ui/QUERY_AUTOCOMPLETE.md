# Query Autocomplete Feature

## Overview

The Query Autocomplete feature uses Bifrost (Heimdall's AI assistant) to provide intelligent Cypher query suggestions as users type. This feature leverages the instruct model to understand partial queries and suggest completions.

## How It Works

1. **Debounced Input**: As the user types in the Cypher query editor, the input is debounced (800ms delay) to avoid excessive API calls.

2. **AI Suggestion Request**: When the user pauses typing, the current query is sent to Bifrost's `/api/bifrost/chat/completions` endpoint with a prompt asking for query completion.

3. **Suggestion Display**: If a valid suggestion is returned, it appears in a dropdown below the query editor.

4. **User Interaction**:
   - **Click**: Click on a suggestion to accept it
   - **Arrow Keys**: Navigate through suggestions
   - **Enter**: Accept the selected suggestion
   - **Escape**: Dismiss suggestions

## Features

- ✅ **AI-Powered**: Uses the instruct model (qwen3-0.6b-instruct) for intelligent suggestions
- ✅ **Debounced**: Prevents excessive API calls while typing
- ✅ **Keyboard Navigation**: Full keyboard support for accessibility
- ✅ **Toggleable**: Can be enabled/disabled via the lightning bolt icon
- ✅ **Non-Intrusive**: Only shows suggestions when meaningful completions are available
- ✅ **Error Handling**: Gracefully handles API failures without disrupting the user experience

## Components

### `QueryAutocomplete.tsx`

The main autocomplete component that:
- Monitors query changes
- Fetches suggestions from Bifrost API
- Displays suggestions in a dropdown
- Handles keyboard navigation

**Props:**
- `query`: Current Cypher query string
- `onSuggestionSelect`: Callback when user selects a suggestion
- `enabled`: Whether autocomplete is enabled
- `textareaRef`: Reference to the textarea for keyboard event handling

### Integration with `QueryPanel.tsx`

The QueryPanel component:
- Renders the QueryAutocomplete component
- Provides a toggle button to enable/disable autocomplete
- Handles suggestion selection by updating the query

## API Integration

The feature uses the existing Bifrost chat completions endpoint:

```typescript
POST /api/bifrost/chat/completions
{
  "model": "qwen3-0.6b-instruct",
  "messages": [{
    "role": "user",
    "content": "Complete this Cypher query: ..."
  }],
  "stream": false,
  "max_tokens": 256,
  "temperature": 0.3
}
```

## Configuration

- **Debounce Delay**: 800ms (configurable in `QueryAutocomplete.tsx`)
- **Model**: `qwen3-0.6b-instruct` (default, can be changed)
- **Temperature**: 0.3 (lower for more focused suggestions)
- **Max Tokens**: 256 (sufficient for query completions)

## User Experience

1. User starts typing a Cypher query: `MATCH (n`
2. After 800ms pause, the AI analyzes the partial query
3. A suggestion appears: `MATCH (n) RETURN n LIMIT 25`
4. User can:
   - Click the suggestion to accept
   - Use arrow keys to navigate
   - Press Enter to accept
   - Press Escape to dismiss
   - Continue typing to dismiss and get new suggestions

## Future Enhancements

Potential improvements:
- Multiple suggestion options
- Context-aware suggestions based on database schema
- Learning from user's query history
- Syntax highlighting in suggestions
- Partial query completion (insert at cursor position)
- Support for multi-line query suggestions

## Technical Notes

- The component automatically cancels previous requests when new ones are made
- Suggestions are only shown if they're meaningfully different from the current query
- The feature gracefully degrades if Bifrost is unavailable
- Keyboard events are handled via event listeners on the textarea element


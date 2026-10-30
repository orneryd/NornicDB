# Browser Components

This directory contains components extracted from `Browser.tsx` for better maintainability and reusability.

## Component Structure

```
components/
├── browser/
│   ├── Header.tsx              # Header with logo, stats, action buttons
│   ├── QueryPanel.tsx          # Cypher query editor and results
│   ├── SearchPanel.tsx         # Semantic search input and results
│   ├── NodeDetailsPanel.tsx    # Node details view (properties, labels, etc.)
│   └── QueryResultsTable.tsx   # Table view for query results
├── common/
│   ├── ExpandableCell.tsx      # Expandable cell for table results
│   ├── JsonPreview.tsx         # JSON preview component
│   ├── PropertyEditor.tsx     # Property editor for nodes
│   ├── EmbeddingStatus.tsx     # Embedding status display
│   └── NodeCard.tsx            # Reusable node card component
├── modals/
│   ├── DeleteConfirmModal.tsx # Delete confirmation modal
│   └── RegenerateConfirmModal.tsx # Regenerate embeddings modal
└── utils/
    └── nodeUtils.ts            # Node utility functions
```

## Refactoring Goals

1. **Separation of Concerns**: Each component has a single responsibility
2. **Reusability**: Components can be used across different pages
3. **Testability**: Smaller components are easier to unit test
4. **Maintainability**: Easier to find and fix bugs
5. **No Functionality Changes**: All existing functionality preserved


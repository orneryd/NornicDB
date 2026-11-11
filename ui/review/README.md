# UI review evidence

The screenshots in [screenshots](screenshots) use synthetic API fixtures. Desktop and mobile views were inspected after the browser suite passed. They show workspace, database management, schema, embeddings, backup/restore, users, account/tokens, database access, lifecycle, retention, knowledge policies and their guided setup, code graph, Galaxy, Fleet and pivot analysis. Additional captures cover failed inline saves, nested order details and mobile layouts.

## Local verification

- `npm test`: 26 passed.
- `npm run build`: passed; Vite reports a large initial bundle and an existing configuration-loader compatibility warning.
- `npm run test:browser`: 37 passed. After clarifying inverse half-life text and improving screenshot framing, the 14 affected operations/pivot tests passed again.
- `npm run inventory`: 333 control declarations in 43 files.
- `go test ./pkg/server -run 'APIToken|Auth|Route|UISetup|EmbedRegenerate|AllDatabaseOperations|BackupOperationPublicValidation' -count=1`: passed. The initial broader run caught shared-worker wake authorization; the correction preserves all-database admission when regeneration is absent.
- `go test ./pkg/nornicdb -run 'TestClearDatabaseEmbeddingsPreservesOtherNamespaces|TestDatabaseEmbeddingRegenerationKeepsOtherWorkerState' -count=1`: passed.
- An isolated source-built local server verified viewer token creation, preserved identity/roles, denied backup access, and explicit seeding/querying of 25,000 pivot orders with 1,000 account rows and 25 order details for a selected account. Earlier live Galaxy traversal and query graph checks used disposable demo data.

The complete Go suite and GitHub CI have not been run for this change. The manual UI review workflow respects the repository's approved-actions environment. No live restore or production embedding regeneration was performed.

## Accepted dependency limits

UI Grid 5.0.3 has upstream issues [7410](https://github.com/orneryd/ui-grid/issues/7410) and [7411](https://github.com/orneryd/ui-grid/issues/7411). Expanded grids render all rows through a supported setting until collapsed. Accessibility checks separate the exact SDK grid/pin-menu ownership finding and fail other findings. Dependency source is unchanged; this is not a claim of complete WCAG compliance.

See [interaction architecture](interaction-architecture.md) for intent, NN/g guidance and URL ownership.

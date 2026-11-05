# Release Graphify Actions

These workflows run from the default branch and use the existing Graphify tools on the target release branch. After merging this change, configure the following repository Actions secrets:

- `NORNICDB_URI`: the public HTTPS endpoint, for example `https://database.example.com:7474`.
- `NORNICDB_USER`: the database username.
- `NORNICDB_PASSWORD`: that user's password.
- `NORNICDB_CA_PEM`: the public PEM certificate that signs the endpoint's certificate. For a self-signed server, use its public certificate. Do not use the private key.

The certificate must match the hostname or IP address in `NORNICDB_URI`. TLS certificate and hostname verification remain enabled. GitHub-hosted Actions runners also need network access through the server's IP allowlist. The optional repository variable `NORNICDB_DATABASE` defaults to `nornicdbcode`; all release branches share this rolling graph.

A PR targeting `release/**` runs the advisory duplicate check automatically when its author and the event actor are repository maintainers or administrators. Otherwise, the bot posts an approval request. A maintainer or administrator can comment the exact `/graphify approve <commit-sha>` command shown there. An approval applies only to that commit; pushing a new commit requires a new approval. The check publishes findings, explicitly reports no findings, or reports an incomplete check. It does not itself block merging.

Each PR merged into a release branch triggers ingestion of the current branch head. Ingestion is serialized across release branches, and queued runs refresh the latest head. A maintainer or administrator can also run **Release Graphify ingestion** manually from Actions with a `release/...` branch name, including to seed or recover the index. Run that manual refresh after configuring secrets so the graph is current before relying on checks.

Keep the local hooks until the hosted workflows have been enabled and verified against a real PR and merge. Workflow runs are dependent on the configured database and its graph-search availability.

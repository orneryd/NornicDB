# macOS setup, upgrades, and startup settings

The installer preserves `~/.nornicdb/config.yaml`,
`~/.nornicdb/environment`, and
`~/Library/LaunchAgents/com.nornicdb.server.plist` on upgrade. Both package
editions and the developer installer use the same post-install script. An
existing configured service resumes after installation; a fresh install waits
for setup so encryption can be configured before the first database write.
An unfinished first-run marker is preserved.

Setup has three steps:

1. **Server / Security:** listen address, ports, authentication, encryption, and
   automatic startup. Existing values are loaded; opening setup applies no preset.
2. **Optional AI:** independently enable and configure Embeddings, Reranking,
   and Heimdall. Each service has its own provider, model, endpoint and credential.
   Embeddings also expose dimensions, workers and chunking. Remote model IDs are
   kept exactly as entered; GGUF downloads apply to local models only.
3. **Review:** inspect the selected services and startup overrides, then explicitly
   save and start the service. Disabled services retain their saved settings.

Settings uses the same AI and Advanced controls. Stop Server and Restart Server
ask for confirmation; Cancel leaves the service running unchanged.

## Advanced (optional)

The Advanced tab is available during setup and in Settings. It supports named
environment variables and the ordered argument list passed to `nornicdb`. Keep the `serve` row to run
the database; existing arguments before and after it retain their order.
Each argument row is exactly one argument; put a flag and its separate value in
separate rows. Spaces stay within a row. There is no shell evaluation, variable
expansion, command substitution, or shell quoting step. Runtime arguments can
also select a different config file, overriding the normal app configuration.

The existing LaunchAgent's arguments and environment are loaded. Recognized
server/service variables populate their ordinary editable settings; additional
variables appear in Advanced. This prevents an old generated environment value
from overriding an edit to the same field in Settings. Adding a recognized name
explicitly in Advanced creates an override. Remove it to use the ordinary field.

Precedence is **runtime arguments → Advanced environment overrides → ordinary
settings/YAML → server defaults**. Inspect Advanced if the server behaves
differently from an ordinary setting. These values are included in every
LaunchAgent written by the app and take effect when the service next starts.
Storage-upgrade authorization is a separate one-shot control and is not retained
as an Advanced override.

Environment values are hidden in the editor. The app writes YAML and the server
LaunchAgent with owner-only permissions (0600). Existing API keys are preserved;
new provider credentials can be entered in their protected service fields or as
the corresponding Advanced environment variable:

| Service | Provider | Model | Endpoint | Credential |
| --- | --- | --- | --- | --- |
| Embeddings | `NORNICDB_EMBEDDING_PROVIDER` | `NORNICDB_EMBEDDING_MODEL` | `NORNICDB_EMBEDDING_API_URL` | `NORNICDB_EMBEDDING_API_KEY` |
| Reranking | `NORNICDB_SEARCH_RERANK_PROVIDER` | `NORNICDB_SEARCH_RERANK_MODEL` | `NORNICDB_SEARCH_RERANK_API_URL` | `NORNICDB_SEARCH_RERANK_API_KEY` |
| Heimdall | `NORNICDB_HEIMDALL_PROVIDER` | `NORNICDB_HEIMDALL_MODEL` | `NORNICDB_HEIMDALL_API_URL` | `NORNICDB_HEIMDALL_API_KEY` |

Embedding concurrency uses `NORNICDB_EMBED_WORKER_NUM_WORKERS`; zero pauses the
workers. For OpenAI `text-embedding-3-small`, use 1536 dimensions unless you
explicitly configured a different supported dimension count.

The login service does **not** inherit your terminal's `.zshrc`. A terminal-only
`OPENAI_KEY` export must be supplied through the service's API-key field or
`NORNICDB_EMBEDDING_API_KEY` override. No credentials should be committed to Git.
The older installer environment file is preserved as user data; the app's
LaunchAgent environment is the actual startup contract, not a shell-sourced file.

If configuration cannot be read or parsed as a YAML mapping, saving is refused.
Correct the YAML/permissions before retrying; the app must not replace it with
defaults. Unsupported/custom YAML keys and provider-specific options are retained.

## Local checks

```sh
swift test --package-path macos/MenuBarApp
python3 -m unittest discover -s macos/tests -v
bash -n macos/scripts/{preinstall,postinstall,install.sh,build-installer.sh}
make macos-menubar
```

Installer tests execute the actual post-install script in a temporary filesystem
with mocked launchd/privileged commands. They never touch the installed database.

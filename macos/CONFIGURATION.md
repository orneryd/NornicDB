# macOS setup, upgrades, and startup settings

The installer preserves `~/.nornicdb/config.yaml`,
`~/.nornicdb/environment`, and
`~/Library/LaunchAgents/com.nornicdb.server.plist` on upgrade. Both package
editions and the developer installer use the same post-install script. Every installation opens setup to review the saved settings before starting
the server. On fresh installs this also allows encryption to be configured before
the first database write.
An unfinished first-run marker is preserved.

Setup has three steps:

1. **Server / Security:** listen address, ports, authentication, encryption, and
   automatic startup. Storage upgrade authorization is visible at the front of
   this page and again on Review, with the same selected state. Existing values
   are loaded; opening setup applies no preset.
2. **Optional AI:** configure K-Means Clustering, Memory Decay, Auto-TLP and the maximum active search cursor count, then independently enable and configure Embeddings, Reranking,
   and Heimdall. Each service has its own provider, model, endpoint and credential.
   Embeddings also expose dimensions, workers and chunking. Remote model IDs are
   kept exactly as entered; GGUF downloads apply to local models only.
   Each enabled service with **Local** selected shows its own **Download default
   model** button: BGE-M3 for Embeddings, BGE reranker for Reranking, and Qwen
   for Heimdall inference. Each button downloads only that service’s model,
   without changing provider/model selections. The tray retains its download-all
   action; all paths share the same downloader. Failed downloads preserve any
   existing valid model.
3. **Review:** inspect the selected services and startup overrides, then explicitly
   save and start the service. The wizard closes after launch submission rather than
   waiting for readiness. The tray stays yellow while the server process initializes,
   turns green when health succeeds, and red when no server process is running.
   Disabled services retain their saved settings.

Settings uses the same AI and Advanced controls. Choosing Settings closes any
open setup window; activating the app does not reopen setup. Pending setup review
remains available on the next app launch or reopen when no window is visible. Stop Server and Restart Server
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

## Replacing an older installation

Preinstall unloads both GUI agents, stops the login user's old menu-bar/server
processes, and waits for them to exit before replacing executable payloads. It
removes the old `/Applications/NornicDB.app` bundle and `/usr/local/bin/nornicdb`
so obsolete executable/resources cannot survive an overlay. The legacy
`~/Applications/NornicDB.app` is removed only when its bundle identifies it as
NornicDB. Configuration, launch settings, models and database files are retained.
If an old process cannot be stopped, installation fails before payload removal.

Postinstall creates a review marker **before** launching the menu-bar app. It
starts the canonical `/Applications/NornicDB.app/Contents/MacOS/NornicDB` through
one GUI LaunchAgent; it does not also run `open` or auto-start the database.
Reopening the app brings pending setup forward. Completing setup clears the
review marker only after server health succeeds.

The app retains a per-user operating-system lock so two updated copies cannot
create independent tray instances. The installed owner also retires older copies
that predate the lock. Source/build staging folders are not installation targets
and are not deleted by the installer; use the installed application.

Search cursor continuation uses `memory.search_cursor_max` and
`NORNICDB_SEARCH_CURSOR_MAX`. Zero disables continuation; a positive count enables
it with that process-wide maximum. Existing values are loaded, Settings tracks
edits, and Review displays the limit. Restart the server to apply changes.

Local model controls show **Ready** when a valid GGUF is already installed and
hide the download button. If the default exists but a different missing model is
selected, **Use installed default model** selects it explicitly. Downloads skip
valid installed defaults, including through the tray batch action.
**Open Models Folder** sits beside each local model's download/readiness controls
and opens the shared directory so users can add their own GGUF files. Enter the
custom filename in that service's model field. Missing directories are created.

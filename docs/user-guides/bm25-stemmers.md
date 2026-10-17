# BM25 Stemmer Plugins

NornicDB's BM25 analyzer is language-neutral by default: NFKC normalization,
Unicode case folding, Unicode token splitting, and exact token matching. The
database does not ship with French, Spanish, Dutch, Chinese, Ukrainian, or any
other language stemmer enabled or bundled.

When you need stemming, install a trusted local Go plugin and select it by ID
for the database that owns the corpus. NornicDB keeps the runtime contract
small: the server loads manifest-verified plugins, selects one stemmer ID per
database, and rebuilds BM25 whenever the analyzer fingerprint changes.

## What Ships

- `nornicdb` loads stemmer plugins from a configured directory.
- `nornicdb-snowball` packages generated Snowball Go source as a NornicDB
  stemmer plugin.
- No Snowball algorithms, Snowball runtime, or language stemmers are embedded
  in the server binary.
- `nornicdb-admin` is not part of this workflow and does not depend on
  Snowball.

Go plugins are supported on Unix-like platforms where Go supports
`-buildmode=plugin`. On Windows, keep `bm25_stemmer` set to `none`.

## Build The Packaging Tool

Build the standalone helper from this repository:

```bash
go build -o ./bin/nornicdb-snowball ./cmd/nornicdb-snowball
```

Build or install the upstream Snowball compiler separately. The official
Snowball project provides the compiler, algorithm files, and Go runtime package
at `github.com/snowballstem/snowball/go`.

```bash
git clone https://github.com/snowballstem/snowball.git ./third_party/snowball
git -C ./third_party/snowball checkout v3.1.1
make -C ./third_party/snowball
```

The examples use the current stable Snowball v3.1.1 compiler and Go runtime.
Keep both on the same release so generated code and runtime APIs stay aligned.
The plugin version passed to `nornicdb-snowball` should identify that build.

## Match The Server Binary

Go loads a plugin only if every package the plugin shares with the server was
built identically. That depends on the Go release, on build settings such as
`-trimpath`, `-race`, `GOEXPERIMENT`, compiler flags and the CPU architecture
level, and on the exact source of every shared dependency (for example
`golang.org/x/text`). Release binaries are built with `-trimpath` (Homebrew),
others without it, so a plugin built for one is refused by the other: "plugin
was built with a different version of package ...".

Pass the exact `nornicdb` binary that will load the plugin with `--target`:

1. `nornicdb-snowball` reads that binary's recorded build settings (the ones
   `go version -m <binary>` prints) and builds the plugin with them, ignoring
   your own `GOFLAGS`, `GOEXPERIMENT` and `go env -w` settings. It refuses to
   package when your `go` is a different release from the one that built the
   binary; `go version <binary>` shows which release you need.
2. It then runs `<binary> stemmer check <manifest>`, which loads the plugin
   exactly as server startup does, without opening a database or the network.
   The plugin and its manifest are kept only if the server binary accepts the
   plugin. If the server binary refuses it, the check times out or crashes, or
   the binary is too old to have `stemmer check`, both files are removed and
   packaging fails. On a refusal the error lists the recorded differences
   between the two builds, such as a shared module at another version. These
   explain the refusal; only the server binary decides.

The examples below use `./bin/nornicdb`; replace it with your server binary's
path, for example `"$(command -v nornicdb)"` for a Homebrew install.

Without `--target` the plugin is built with your toolchain's defaults and
nothing checks it: the command prints `packaged <id> (unverified: ...)`. Check
such a plugin yourself before deploying it:

```bash
nornicdb stemmer check ./plugins/stemmers/snowball-french.stemmer.json
```

### Docker and other machines

`--target` runs the server binary, so package on a machine that can run it:
the same operating system and CPU architecture, with the Go release that built
it. A plugin built on another machine is checked only if you run
`nornicdb stemmer check` where the server runs.

For a Docker deployment, package inside a container from the server image's
platform that has the image's `nornicdb` binary and the matching Go release
(`go version <binary>` inside the image shows it), then copy the plugin and its
manifest into the server's stemmer plugin directory. Running
`nornicdb stemmer check` in the server container before switching
`bm25_stemmer` to the new plugin confirms it loads there.

## Package A Snowball Algorithm

This example packages the official French Snowball algorithm. The same pattern
works for Spanish and Dutch.

```bash
mkdir -p ./build/stemmers/french ./plugins/stemmers
cd ./build/stemmers/french

go mod init example.com/acme/nornicdb-stemmers/french
go get github.com/snowballstem/snowball/go@v3.1.1+incompatible

../../../third_party/snowball/snowball \
  ../../../third_party/snowball/algorithms/french.sbl \
  -go \
  -P main \
  -goruntime github.com/snowballstem/snowball/go \
  -o stemmer

go mod tidy
go mod vendor
cd ../../..

./bin/nornicdb-snowball package \
  --language french \
  --id snowball.french \
  --version 3.1.1 \
  --module ./build/stemmers/french \
  --source ./build/stemmers/french/stemmer.go \
  --output ./plugins/stemmers/snowball-french.so \
  --target ./bin/nornicdb
```

The package command writes two files:

```text
plugins/stemmers/
  snowball-french.so
  snowball-french.stemmer.json
```

Keep both files together. The manifest records the plugin ID, ABI version,
language label, library filename, entrypoint symbol, and SHA-256 digest.

## Language Examples

Use the official Snowball algorithm files where they exist:

| Language | Snowball source file | Plugin ID | Output |
| --- | --- | --- | --- |
| French | `third_party/snowball/algorithms/french.sbl` | `snowball.french` | `plugins/stemmers/snowball-french.so` |
| Spanish | `third_party/snowball/algorithms/spanish.sbl` | `snowball.spanish` | `plugins/stemmers/snowball-spanish.so` |
| Dutch | `third_party/snowball/algorithms/dutch.sbl` | `snowball.dutch` | `plugins/stemmers/snowball-dutch.so` |

For example, Spanish only changes the directory, source file, ID, language
label, and output name:

```bash
./bin/nornicdb-snowball package \
  --language spanish \
  --id snowball.spanish \
  --version 3.1.1 \
  --module ./build/stemmers/spanish \
  --source ./build/stemmers/spanish/stemmer.go \
  --output ./plugins/stemmers/snowball-spanish.so \
  --target ./bin/nornicdb
```

Chinese and Ukrainian are not official upstream Snowball algorithms in the
published Snowball language set. A real deployment for either language must
vendor a vetted Snowball `.sbl` algorithm from your own source or from a
third-party source you have reviewed. Package it the same way:

```bash
./third_party/snowball/snowball \
  ./local-algorithms/ukrainian.sbl \
  -go \
  -P main \
  -goruntime github.com/snowballstem/snowball/go \
  -o ./build/stemmers/ukrainian/stemmer

./bin/nornicdb-snowball package \
  --language ukrainian \
  --id snowball.ukrainian \
  --version local-2026.09 \
  --module ./build/stemmers/ukrainian \
  --source ./build/stemmers/ukrainian/stemmer.go \
  --output ./plugins/stemmers/snowball-ukrainian.so \
  --target ./bin/nornicdb
```

```bash
./third_party/snowball/snowball \
  ./local-algorithms/chinese.sbl \
  -go \
  -P main \
  -goruntime github.com/snowballstem/snowball/go \
  -o ./build/stemmers/chinese/stemmer

./bin/nornicdb-snowball package \
  --language chinese \
  --id snowball.chinese \
  --version local-2026.09 \
  --module ./build/stemmers/chinese \
  --source ./build/stemmers/chinese/stemmer.go \
  --output ./plugins/stemmers/snowball-chinese.so \
  --target ./bin/nornicdb
```

Chinese search often needs segmentation more than suffix stemming. NornicDB's
current BM25 tokenizer remains the built-in Unicode tokenizer; a Chinese
stemmer plugin can normalize individual tokens, but it does not replace
tokenization.

## Configure NornicDB

Set the process-level plugin directory and the default stemmer ID:

```yaml
plugins:
  stemmers:
    directory: ./plugins/stemmers

search:
  bm25_stemmer: snowball.french
```

The equivalent environment variables are:

```bash
export NORNICDB_STEMMER_PLUGINS_DIR=./plugins/stemmers
export NORNICDB_SEARCH_BM25_STEMMER=snowball.french
```

Use `none` to keep the default analyzer:

```bash
export NORNICDB_SEARCH_BM25_STEMMER=none
```

You can also select stemmers per database. Per-database settings win over the
global default:

```yaml
databases:
  french_docs:
    db.nornic.search.bm25.stemmer: snowball.french
  spanish_docs:
    db.nornic.search.bm25.stemmer: snowball.spanish
  default_docs:
    db.nornic.search.bm25.stemmer: none
```

At runtime, update one database through the admin API:

```bash
curl -X PUT http://localhost:7474/admin/databases/french_docs/config \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "overrides": {
      "db.nornic.search.bm25.stemmer": "snowball.french"
    }
  }'
```

The update rebuilds that database's BM25 service from original stored text. If
the selected stemmer ID is unavailable or the rebuild fails, the update fails
and the previous search service stays active.

## Runtime Contract

- The selected plugin must be present at startup in
  `NORNICDB_STEMMER_PLUGINS_DIR` or `plugins.stemmers.directory`.
- The plugin ID is an operator-facing identifier such as `snowball.french`, not
  a filesystem path.
- A configured missing plugin is a startup/configuration error. NornicDB does
  not silently fall back to `none`.
- Go plugins cannot be unloaded. Add, remove, or replace plugin files with a
  process restart.
- Changing stemmer ID, plugin version, ABI version, digest, tokenizer, BM25
  format, or indexed property projection forces a BM25 rebuild.
- Phrase search remains literal against the stored text and does not invoke the
  stemmer.

## References

- [Snowball compiler and algorithms](https://github.com/snowballstem/snowball)
- [Snowball Go runtime package](https://pkg.go.dev/github.com/snowballstem/snowball/go)
- [Snowball published algorithm list](https://snowballstem.org/algorithms/)

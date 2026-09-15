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
make -C ./third_party/snowball
```

Pin the Snowball source revision you use in your own release process. The
plugin version you pass to `nornicdb-snowball` should identify that pinned
algorithm/runtime build.

## Package A Snowball Algorithm

This example packages the official French Snowball algorithm. The same pattern
works for Spanish and Dutch.

```bash
mkdir -p ./build/stemmers/french ./plugins/stemmers
cd ./build/stemmers/french

go mod init example.com/acme/nornicdb-stemmers/french
go get github.com/snowballstem/snowball/go@v3.0.1+incompatible

../../../third_party/snowball/snowball \
  ../../../third_party/snowball/algorithms/french.sbl \
  -go \
  -gopackage main \
  -goruntime github.com/snowballstem/snowball/go \
  -o stemmer

go mod tidy
go mod vendor
cd ../../..

./bin/nornicdb-snowball package \
  --language french \
  --id snowball.french \
  --version 3.0.1 \
  --module ./build/stemmers/french \
  --source ./build/stemmers/french/stemmer.go \
  --output ./plugins/stemmers/snowball-french.so
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
  --version 3.0.1 \
  --module ./build/stemmers/spanish \
  --source ./build/stemmers/spanish/stemmer.go \
  --output ./plugins/stemmers/snowball-spanish.so
```

Chinese and Ukrainian are not official upstream Snowball algorithms in the
published Snowball language set. A real deployment for either language must
vendor a vetted Snowball `.sbl` algorithm from your own source or from a
third-party source you have reviewed. Package it the same way:

```bash
./third_party/snowball/snowball \
  ./local-algorithms/ukrainian.sbl \
  -go \
  -gopackage main \
  -goruntime github.com/snowballstem/snowball/go \
  -o ./build/stemmers/ukrainian/stemmer

./bin/nornicdb-snowball package \
  --language ukrainian \
  --id snowball.ukrainian \
  --version local-2026.09 \
  --module ./build/stemmers/ukrainian \
  --source ./build/stemmers/ukrainian/stemmer.go \
  --output ./plugins/stemmers/snowball-ukrainian.so
```

```bash
./third_party/snowball/snowball \
  ./local-algorithms/chinese.sbl \
  -go \
  -gopackage main \
  -goruntime github.com/snowballstem/snowball/go \
  -o ./build/stemmers/chinese/stemmer

./bin/nornicdb-snowball package \
  --language chinese \
  --id snowball.chinese \
  --version local-2026.09 \
  --module ./build/stemmers/chinese \
  --source ./build/stemmers/chinese/stemmer.go \
  --output ./plugins/stemmers/snowball-chinese.so
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

# Convert search index files from gob to msgpack

This script converts vector and HNSW `.gob` files to msgpack at extensionless
paths (`vectors`, `hnsw`, `hnsw_ivf/0`, `1`, …), then removes the converted
files. It does not convert full-text indexes; those are built from graph storage
when a current full-text snapshot is unavailable.

## Steps

1. **Back up** the search index directory (e.g. `cp -r data/test/search data/test/search.bak`).
2. **Stop NornicDB** so files aren't being written.
3. **Run the script:**
   ```bash
   go run ./scripts/convert_search_index_to_msgpack --dir data/test/search
   ```
4. **Restart NornicDB.** It can load the converted vector and HNSW files.

## What gets converted

| Old path       | New path   |
|----------------|------------|
| `vectors.gob`  | `vectors`  |
| `hnsw.gob`     | `hnsw`     |
| `hnsw_ivf/N.gob` | `hnsw_ivf/N` |

After a successful convert, the script deletes each `.gob` file. If the new path already exists and is valid msgpack, that file is skipped (idempotent).

Default `--dir` is `data/test/search`. Use the path that contains your database subdirs (e.g. `nornic`, `translations`).

## Large indexes

Converting a very large `vectors.gob` (e.g. 3 GB) can take a minute or two. The script reads gob, writes msgpack to the new path, then removes the `.gob` file.

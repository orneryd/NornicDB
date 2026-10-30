# Reply: Config loading, GPU usage, embeddings, and `/help` behavior

Thanks for the detailed report — you’re not imagining it. There are a few rough edges here (some are docs/UX, some are actual behavior/flags), and we’ve made changes to address them.

## 1) Config file: `--config` unknown / “is `/config/nornicdb.yaml` loaded?”

### What was happening
- The docs referenced `--config`, but the CLI didn’t actually expose that flag in some builds, so `nornicdb serve --config ...` failed as “unknown flag”.
- Config auto-discovery existed, but it wasn’t container-friendly and it wasn’t obvious from logs whether a config file was used or not.

### What’s fixed now (in code)
- Added a global `--config` flag to `nornicdb` (works as `nornicdb serve --config /config/nornicdb.yaml`).
- Added `NORNICDB_CONFIG=/config/nornicdb.yaml` support.
- Added `/config/nornicdb.yaml` and `/config/config.yaml` to the auto-discovery search order.
- Startup now prints whether a config file was loaded or not.

### What to do (Docker)
Pick **one** of these (explicit is best):

**Option A (recommended):**
```bash
docker run ... \
  -e NORNICDB_CONFIG=/config/nornicdb.yaml \
  -v /path/on/host/nornicdb.yaml:/config/nornicdb.yaml:ro \
  timothyswt/nornicdb-amd64-cuda-bge-heimdall:latest
```

**Option B:**
```bash
docker run ... \
  timothyswt/nornicdb-amd64-cuda-bge-heimdall:latest \
  nornicdb serve --config /config/nornicdb.yaml
```

If config is loading, startup logs should include something like:
- `📄 Loaded config from: /config/nornicdb.yaml`

If you paste the first ~50 lines of container logs after startup, we can confirm immediately.

## 2) “Is CUDA being used?” and the confusing “✅ SLM model loaded on CPU”

There are two separate “GPU stories” that can be conflated:

1) **CUDA/Metal/OpenCL/Vulkan for vector search / embedding math** (NornicDB `pkg/gpu`)
2) **GPU offload for local GGUF models** (embedding model and/or Heimdall SLM via llama.cpp/yzma)

### What was confusing
- You can see llama.cpp CUDA buffer logs (meaning CUDA backend exists / tried to allocate), but then still see:
  - `✅ SLM model loaded on CPU (slower but functional)`

That message is emitted when Heimdall tries GPU offload and falls back. It didn’t report *why* clearly enough, and some config paths made “cpu-only vs auto” ambiguous.

### What’s fixed now (in code)
- Heimdall `gpu_layers: 0` now correctly means **CPU-only** (previously it could be treated as “auto” depending on where it was set).
- Heimdall now prints a clearer “Compute: GPU=…” status when the generator backend can report it.

### What to collect from the user (to diagnose CUDA)
Please include:
- the relevant config section for Heimdall and embeddings
- the startup log lines containing:
  - `🛡️ Loading Heimdall model: ...`
  - `GPU layers: ...`
  - any subsequent `GPU loading failed...`
  - and the `Compute: GPU=...` line (newer builds)

## 3) “Zero embeddings” / “it’s word search with zero semantic functionality”

### Likely root cause
In current releases, **embedding generation is disabled by default** unless explicitly enabled via config/env/CLI. If embeddings are disabled (or the embedder fails health checks), NornicDB falls back to keyword/full-text style behavior and you’ll see “no embeddings” symptoms.

### What’s fixed now (docs + UX)
- The docs now explicitly call out that embeddings are disabled by default and show how to enable them.
- Server startup now prints whether embeddings are enabled/disabled.

### What to do
Enable embeddings (pick one):

**Env:**
```bash
export NORNICDB_EMBEDDING_ENABLED=true
```

**CLI:**
```bash
nornicdb serve --embedding-enabled
```

Then verify embeddings are actually ready:
- Look for logs like:
  - `🔄 Loading embedding model: ...`
  - `✅ Embeddings ready: ...`
- If the embedder can’t load (model path wrong, model missing, etc.), the logs currently say it’s falling back to full-text.

## 4) `/help` opens a blank page — we need one screenshot to be sure

There are two different things that look like “/help”:

1) **A chat command inside the Heimdall/Bifrost panel**  
   - You open the UI, open the chat panel, type `/help` into the chat input.
   - Expected behavior: it prints an in-chat help message (it should *not* navigate).

2) **A browser URL path**  
   - You type `http://host:7474/help` in the address bar.
   - Expected behavior (newer builds): the UI SPA should load and route you back to the app (no blank page).
   - Older builds could fall through to the Neo4j discovery handler and look like a “blank” page depending on the browser and auth state.

### What’s fixed now (for URL `/help`)
- The server router now serves the UI for browser requests on deep links so `/help` won’t fall through to the discovery JSON.

### What we need from you to reproduce precisely
Please send **one** of:
- a screenshot of your browser address bar showing `/help`, plus the blank page content, **or**
- a screenshot of the Bifrost/Heimdall chat panel after typing `/help`.

Also tell us:
- are you typing `/help` into the **chat input**, or into the **browser URL**?

## Minimal “known-good” Docker snippet to validate end-to-end

If you want a quick sanity run, use a config file and explicit envs:

```bash
docker run --rm -it \
  -p 7474:7474 -p 7687:7687 \
  -v $PWD/nornicdb.yaml:/config/nornicdb.yaml:ro \
  -e NORNICDB_CONFIG=/config/nornicdb.yaml \
  -e NORNICDB_EMBEDDING_ENABLED=true \
  timothyswt/nornicdb-amd64-cuda-bge-heimdall:latest
```

Then paste the startup log lines that mention:
- config file loaded
- embeddings ready (or why they failed)
- Heimdall model loaded + GPU status

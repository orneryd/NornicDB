<script setup>
import { ref, onMounted, watch, useId, onBeforeUnmount } from "vue";
import { withBase } from "vitepress";
import { localSearch } from "./local-search";
import { apiFetch, historical } from "./api";
const props = defineProps({
    initialQuery: String,
    embedded: Boolean,
    externalQuery: String,
    externalScope: String,
    resultsId: String,
  }),
  inputId = useId(),
  query = ref(""),
  type = ref("docs"),
  results = ref([]),
  mode = ref("Browser full-text search"),
  loading = ref(false),
  loadError = ref(false),
  notice = ref("");
let sequence = 0,
  timer,
  noticeTimer,
  activeRequest;
function notify(message) {
  notice.value = message;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => (notice.value = ""), 8000);
}
async function search() {
  const request = ++sequence;
  clearTimeout(timer);
  activeRequest?.abort();
  activeRequest = undefined;
  loadError.value = false;
  mode.value = "Browser full-text search";
  if (!query.value.trim()) {
    results.value = [];
    loading.value = false;
    return;
  }
  const text = query.value,
    scope = type.value;
  let semantic = false;
  loading.value = true;
  localSearch(text, scope)
    .then((rows) => {
      if (request === sequence) {
        if (!semantic) results.value = rows;
        loading.value = false;
      }
    })
    .catch(() => {
      if (request === sequence) {
        loading.value = false;
        loadError.value = true;
      }
    });
  if (historical || text.trim().length < 3) return;
  timer = setTimeout(async () => {
    const controller = new AbortController();
    activeRequest = controller;
    const timeout = setTimeout(() => controller.abort(), 1800);
    try {
      const response = await apiFetch(
        "/search?" + new URLSearchParams({ q: text, type: scope }),
        { signal: controller.signal, cache: "no-cache" },
      );
      if (request !== sequence) return;
      if (!response.ok) {
        notify(
          response.status === 429 || response.status === 503
            ? "Nornic server is busy. Using full-text search instead."
            : "Semantic search is unavailable. Using full-text search instead.",
        );
        return;
      }
      const payload = await response.json();
      if (request === sequence && payload.results?.length) {
        semantic = true;
        results.value = payload.results;
        mode.value = "Semantic search";
        loading.value = false;
        notice.value = "";
      }
    } catch {
      if (request === sequence)
        notify(
          "Nornic search did not respond. Using full-text search instead.",
        );
    } finally {
      clearTimeout(timeout);
      if (activeRequest === controller) activeRequest = undefined;
    }
  }, 1000);
}
onMounted(() => {
  if (props.embedded) type.value = props.externalScope || "docs";
  query.value =
    (props.embedded ? props.externalQuery : undefined) ??
    props.initialQuery ??
    new URLSearchParams(location.search).get("q") ??
    (location.pathname.includes("/search")
      ? ""
      : decodeURIComponent(location.pathname)
          .slice(withBase("/").length)
          .replace(/\.html$/, "")
          .replace(/[\/_-]+/g, " ")
          .trim());
  if (query.value) search();
});
watch([query, type], search);
watch(
  () => props.externalQuery,
  (value) => {
    if (props.embedded) query.value = value || "";
  },
);
watch(
  () => props.externalScope,
  (value) => {
    if (props.embedded) type.value = value || "docs";
  },
);
onBeforeUnmount(() => {
  sequence++;
  clearTimeout(timer);
  activeRequest?.abort();
  clearTimeout(noticeTimer);
});
</script>
<template>
  <section
    class="search-page"
    :class="{ 'search-embedded': embedded }"
    :id="resultsId"
    :aria-label="embedded ? 'Search results' : undefined"
  >
    <h1 v-if="!embedded">Search documentation and code</h1>
    <p v-if="!embedded">
      Find a guide, configuration option, or implementation.
    </p>
    <template v-if="!embedded">
      <label :for="inputId">Search terms</label
      ><input
        :id="inputId"
        v-model="query"
        type="search"
        maxlength="240"
        placeholder="For example: hybrid search"
      /><label class="search-scope"
        >Search in
        <select v-model="type">
          <option value="docs">Documentation</option>
          <option value="code">Code</option>
        </select></label
      >
    </template>
    <p v-if="!embedded || query.trim()" class="status" role="status">
      {{ loading ? "Loading full-text results…" : mode
      }}{{ historical ? " · archived version" : "" }}
    </p>
    <p v-if="loadError" role="alert">
      The browser search index could not be loaded.
      <button @click="search">Retry browser search</button>
    </p>
    <article
      class="search-result"
      v-for="result in results"
      :key="result.id || result.url"
    >
      <a :href="withBase(result.url)"
        ><strong>{{ result.title }}</strong></a
      >
      <p>{{ result.text?.slice(0, 220) }}</p>
      <small>{{ result.file || result.category }}</small>
    </article>
    <p v-if="query.trim() && !loading && !results.length">
      No results. Try a function name, error message, or configuration variable.
    </p>
    <aside
      v-if="notice"
      class="search-fallback"
      role="status"
      aria-live="polite"
    >
      <span>{{ notice }}</span
      ><button @click="notice = ''" aria-label="Dismiss search notification">
        Dismiss
      </button>
    </aside>
  </section>
</template>

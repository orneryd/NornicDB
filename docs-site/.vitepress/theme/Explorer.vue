<script setup>
import { ref, computed, onMounted, watch, nextTick } from "vue";
import { withBase } from "vitepress";
import ForceNeighborhood from "./ForceNeighborhood.vue";
import { track } from "./analytics.mjs";
import { apiFetch } from "./api";
const props = defineProps({
  initialSymbol: String,
  contextDoc: String,
  embedded: Boolean,
});
const direction = ref("both"),
  trail = ref([]);
const query = ref("Search"),
  graph = ref({ nodes: [], links: [] }),
  selected = ref(null),
  snippet = ref(null),
  status = ref("Loading source index…"),
  issues = ref([]);
const matches = computed(() =>
  graph.value.nodes
    .filter((n) =>
      (n.name + " " + n.file).toLowerCase().includes(query.value.toLowerCase()),
    )
    .slice(0, 40),
);
const neighbors = computed(() => {
  if (!selected.value) return [];
  const incoming = graph.value.links.filter(
      (e) => e.target === selected.value.id,
    ),
    outgoing = graph.value.links.filter((e) => e.source === selected.value.id);
  if (direction.value === "in") return incoming.slice(0, 12);
  if (direction.value === "out") return outgoing.slice(0, 12);
  return [
    ...new Set([
      ...incoming.slice(0, 6),
      ...outgoing.slice(0, 6),
      ...incoming,
      ...outgoing,
    ]),
  ].slice(0, 12);
});
const byId = computed(() => new Map(graph.value.nodes.map((n) => [n.id, n])));
const calls = computed(() =>
  neighbors.value
    .map((e) => ({
      node: byId.value.get(
        e.source === selected.value.id ? e.target : e.source,
      ),
      direction: e.source === selected.value.id ? "Calls" : "Called by",
    }))
    .filter((e) => e.node),
);
let selection = 0;
async function select(node) {
  track("docs_code_explored");
  const sequence = ++selection;
  const previous = trail.value.findIndex((item) => item.id === node.id);
  if (previous >= 0) trail.value = trail.value.slice(0, previous + 1);
  else trail.value = [...trail.value.slice(-5), node];
  selected.value = node;
  snippet.value = null;
  issues.value = [];
  const params = new URLSearchParams(location.search);
  params.set("symbol", node.id);
  if (!props.embedded) history.replaceState(null, "", "?" + params);
  try {
    const data = await fetch(withBase("/snippets/" + node.key + ".json")).then(
      (r) => r.json(),
    );
    if (sequence === selection) snippet.value = data;
  } catch {
    status.value = "Source excerpt could not be loaded.";
  }
  await nextTick();
  apiFetch("/related?symbol=" + encodeURIComponent(node.id))
    .then((r) => (r.ok ? r.json() : null))
    .then((data) => {
      if (sequence === selection) issues.value = data?.items || [];
    })
    .catch(() => {});
}
onMounted(async () => {
  try {
    graph.value = await fetch(withBase("/code-graph.json")).then((r) =>
      r.json(),
    );
    status.value = graph.value.provenance;
    const id =
      props.initialSymbol || new URLSearchParams(location.search).get("symbol");
    await select(
      byId.value.get(id) ||
        byId.value.get("pkg/search/search.go:Service.Search") ||
        matches.value[0] ||
        graph.value.nodes[0],
    );
  } catch {
    status.value =
      "Code index unavailable. Open the source repository to inspect code.";
  }
});
</script>
<template>
  <p class="status" role="status">{{ status }}</p>
  <div class="explorer">
    <div v-if="!embedded">
      <label for="symbol-query">Find a symbol or file</label
      ><input id="symbol-query" v-model="query" type="search" />
      <div class="symbol-list">
        <button v-for="node in matches" :key="node.id" @click="select(node)">
          {{ node.name }}<small>{{ node.file }}</small>
        </button>
      </div>
    </div>
    <section class="graph-panel" v-if="selected">
      <nav class="call-trail" aria-label="Code navigation trail">
        <button v-for="node in trail" :key="node.id" @click="select(node)">
          {{ node.name }}
        </button>
      </nav>
      <h2>{{ selected.name }}</h2>
      <p class="status" style="overflow-wrap: anywhere">{{ selected.file }}</p>
      <pre v-if="snippet && embedded"><code>{{ snippet.body }}</code></pre>
      <label
        >Relationships
        <select v-model="direction">
          <option value="both">Callers and callees</option>
          <option value="in">Callers</option>
          <option value="out">Callees</option>
        </select></label
      ><span class="status"> · Up to 12 direct neighbors</span>
      <ForceNeighborhood
        :graph="graph"
        :selected="selected.id"
        :direction="direction"
        @select="(id) => select(byId.get(id))"
      />
      <div class="neighbors">
        <button
          v-for="call in calls"
          :key="call.direction + call.node.id"
          @click="select(call.node)"
        >
          {{ call.direction }} {{ call.node.name }}
        </button>
        <p v-if="!calls.length">
          No package-resolved direct calls recorded for this symbol. Dynamic
          dispatch is not included.
        </p>
      </div>
      <p>
        <a
          v-if="snippet"
          :href="`https://github.com/orneryd/NornicDB/blob/${snippet.revision}/${snippet.file}#L${snippet.line}-L${snippet.end}`"
          target="_blank"
          rel="noopener"
          >View source, lines {{ snippet.line }}–{{ snippet.end }}</a
        >
      </p>
      <pre v-if="snippet && !embedded"><code>{{ snippet.body }}</code></pre>
      <h3>Related documentation</h3>
      <ul v-if="selected.docs.length">
        <li v-for="doc in selected.docs" :key="doc.url">
          <a :href="withBase(doc.url)">{{ doc.title }}</a>
        </li>
      </ul>
      <p v-else>
        No explicit documentation reference was found for this symbol.
      </p>
      <h3 v-if="issues.length">Open issues and PRs mentioning this symbol</h3>
      <ul>
        <li v-for="issue in issues" :key="issue.url">
          <a :href="issue.url" target="_blank" rel="noopener"
            >{{ issue.type }}: {{ issue.title }}</a
          >
        </li>
      </ul>
    </section>
  </div>
</template>

<script setup>
import { ref, nextTick, onMounted, onBeforeUnmount, useId } from "vue";
import SearchPage from "./SearchPage.vue";
const input = ref(null),
  anchor = ref(null),
  panel = ref(null),
  opened = ref(false),
  query = ref(""),
  scope = ref("docs"),
  geometry = ref({}),
  fieldWidth = ref(null),
  id = useId();
function position() {
  if (!opened.value || !input.value) return;
  const rect = input.value.getBoundingClientRect(),
    viewport = window.visualViewport;
  const right =
    Math.min(
      innerWidth,
      (viewport?.offsetLeft || 0) + (viewport?.width || innerWidth),
    ) - 16;
  const bottom = (viewport?.offsetTop || 0) + (viewport?.height || innerHeight);
  fieldWidth.value = Math.max(80, Math.min(520, right - rect.left));
  const left =
    right - rect.left < 320 ? (viewport?.offsetLeft || 0) + 16 : rect.left;
  geometry.value = {
    left: left + "px",
    top: rect.bottom + 8 + "px",
    width: Math.max(80, Math.min(720, right - left)) + "px",
    maxHeight: Math.max(80, bottom - rect.bottom - 20) + "px",
  };
}
function open() {
  opened.value = true;
  position();
}
function close(restore = false) {
  if (restore) input.value?.focus({ preventScroll: true });
  opened.value = false;
}
function outside(event) {
  if (
    !anchor.value?.contains(event.target) &&
    !panel.value?.contains(event.target)
  )
    close();
}
function keys(event) {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    open();
    input.value?.focus({ preventScroll: true });
  } else if (event.key === "Escape" && opened.value) {
    event.preventDefault();
    close(true);
  }
}
function browse(event) {
  if (!["ArrowDown", "ArrowUp"].includes(event.key)) return;
  if (!opened.value) {
    event.preventDefault();
    open();
    return;
  }
  const links = [...(panel.value?.querySelectorAll(".search-result a") || [])];
  if (!links.length) return;
  event.preventDefault();
  const current = links.indexOf(document.activeElement);
  const next =
    current < 0
      ? event.key === "ArrowDown"
        ? 0
        : links.length - 1
      : (current + (event.key === "ArrowDown" ? 1 : -1) + links.length) %
        links.length;
  links[next].focus({ preventScroll: true });
  const area = panel.value,
    rect = links[next].getBoundingClientRect(),
    bounds = area.getBoundingClientRect();
  if (rect.bottom > bounds.bottom)
    area.scrollTop += rect.bottom - bounds.bottom + 12;
  if (rect.top < bounds.top) area.scrollTop -= bounds.top - rect.top + 12;
}
async function example(value) {
  query.value = value;
  await nextTick();
  input.value?.focus({ preventScroll: true });
}
onMounted(() => {
  document.addEventListener("keydown", keys);
  document.addEventListener("pointerdown", outside);
  document.addEventListener("focusin", outside);
  window.addEventListener("resize", position);
  window.addEventListener("scroll", position, { passive: true });
  visualViewport?.addEventListener("resize", position);
  visualViewport?.addEventListener("scroll", position);
});
onBeforeUnmount(() => {
  document.removeEventListener("keydown", keys);
  document.removeEventListener("pointerdown", outside);
  document.removeEventListener("focusin", outside);
  window.removeEventListener("resize", position);
  window.removeEventListener("scroll", position);
  visualViewport?.removeEventListener("resize", position);
  visualViewport?.removeEventListener("scroll", position);
});
</script>
<template>
  <div ref="anchor" class="inline-search" :class="{ 'is-open': opened }">
    <input
      ref="input"
      v-model="query"
      class="inline-search-input"
      :style="opened ? { width: fieldWidth + 'px' } : undefined"
      type="search"
      role="combobox"
      aria-haspopup="dialog"
      maxlength="240"
      autocomplete="off"
      spellcheck="false"
      aria-label="Search documentation and code"
      :aria-expanded="opened"
      :aria-controls="id + '-panel'"
      :aria-describedby="opened && !query.trim() ? id + '-guidance' : undefined"
      placeholder="How do I…?"
      @focus="open"
      @input="open"
      @click="open"
      @keydown="browse"
    />
    <kbd v-if="!opened" class="search-shortcut" aria-hidden="true">Ctrl K</kbd>
    <Teleport to="body">
      <section
        v-if="opened"
        ref="panel"
        class="search-popover"
        :id="id + '-panel'"
        role="dialog"
        aria-modal="false"
        :style="geometry"
        aria-label="Documentation search"
        @keydown="browse"
        @click="
          (event) => {
            if (event.target.closest('a')) close();
          }
        "
      >
        <div class="search-popover-toolbar">
          <label
            >Search in
            <select v-model="scope">
              <option value="docs">Documentation</option>
              <option value="code">Code</option>
            </select></label
          >
          <button @click="close(true)" aria-label="Close search">
            Esc <span aria-hidden="true">×</span>
          </button>
        </div>
        <div
          v-if="!query.trim()"
          :id="id + '-guidance'"
          class="search-guidance"
        >
          <p>Ask a question or search for a guide, setting, or code.</p>
          <div>
            <button @click="example('hybrid search')">Hybrid search</button
            ><button @click="example('authentication')">Authentication</button
            ><button @click="example('transactions')">Transactions</button>
          </div>
          <small
            >↑ ↓ browse results · Enter opens a result · Esc closes
            search</small
          >
        </div>
        <SearchPage
          v-if="opened"
          embedded
          :external-query="query"
          :external-scope="scope"
          :results-id="id + '-results'"
        />
      </section>
    </Teleport>
  </div>
</template>

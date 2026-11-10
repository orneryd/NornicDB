<script setup>
import { onMounted, onUnmounted, ref, watch } from 'vue';
import { useRoute, useData, withBase } from 'vitepress';
import { track, optedOut, setDisabled } from './analytics.mjs';
const route = useRoute(), { page } = useData(), disabled = ref(false);
let stop;
function pageview() {
  track('$pageview', { path: page.value.isNotFound ? '/NornicDB/404' : route.path.split(/[?#]/)[0] });
}
function click(event) {
  const link = event.target.closest?.('a[href]');
  if (!link) return;
  if (link.closest('.search-result')) track('docs_search_result_opened');
  const url = new URL(link.href, location.href);
  let destination;
  if (url.hostname === 'github.com') destination = url.pathname.includes('/releases') ? 'releases' : 'github';
  if (url.hostname === 'hub.docker.com') destination = 'docker';
  if (url.hostname === 'discord.gg') destination = 'community';
  if (destination) track('docs_outbound_clicked', { destination });
}
onMounted(() => {
  disabled.value = optedOut();
  pageview();
  stop = watch(() => route.path, pageview, {flush: 'post'});
  document.addEventListener('click', click);
});
onUnmounted(() => { stop?.(); document.removeEventListener('click', click); });
function change() { setDisabled(disabled.value); }
</script>
<template>
  <div class="analytics-preference">
    <a :href="withBase('/privacy')">Analytics privacy</a>
    <label><input v-model="disabled" type="checkbox" @change="change"> Disable usage analytics</label>
  </div>
</template>
<style scoped>
.analytics-preference { padding: 12px 24px; font-size: 12px; color: var(--vp-c-text-2); display: flex; flex-wrap: wrap; gap: 16px; }
label { display: flex; gap: 6px; align-items: center; }
@media (min-width: 960px) {
  .analytics-preference { margin-left: var(--vp-sidebar-width); }
}
</style>

<script setup>
import { nextTick, onMounted, onUnmounted, ref, watch } from 'vue';
import { useRoute, useData, withBase } from 'vitepress';
import { analyticsAvailable, consentKey, readConsent, syncConsent, track, writeConsent } from './analytics.mjs';
const route = useRoute(), { page } = useData();
const available = ref(false), prompt = ref(false);
const saveFailed = ref(false);
const settingsButton = ref(null), promptHeading = ref(null);
const privacyHref = withBase('/privacy').replace(/\/versions\/[^/]+\//, '/');
let openedFromSettings = false;
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
  available.value = analyticsAvailable();
  prompt.value = available.value && readConsent() === null;
  pageview();
  stop = watch(() => route.path, pageview, {flush: 'post'});
  document.addEventListener('click', click);
  window.addEventListener('storage', storageChanged);
});
onUnmounted(() => {
  stop?.();
  document.removeEventListener('click', click);
  window.removeEventListener('storage', storageChanged);
});
function storageChanged(event) {
  if (event.key !== consentKey && event.key !== null) return;
  syncConsent();
  if (!openedFromSettings) prompt.value = available.value && readConsent() === null;
}
async function openSettings() {
  openedFromSettings = true;
  prompt.value = true;
  await nextTick();
  promptHeading.value?.focus();
}
function closeSettings() {
  prompt.value = false;
  saveFailed.value = false;
  if (openedFromSettings) settingsButton.value?.focus({preventScroll: true});
  openedFromSettings = false;
}
function choose(choice) {
  const previous = readConsent();
  saveFailed.value = !writeConsent(choice);
  if (saveFailed.value) return;
  syncConsent();
  closeSettings();
  if (previous !== 'accepted' && readConsent() === 'accepted') pageview();
}
</script>
<template>
  <a class="analytics-setting" :href="privacyHref">Analytics privacy</a>
  <button v-if="available" ref="settingsButton" class="analytics-setting" type="button"
    aria-controls="analytics-consent" :aria-expanded="prompt" @click="openSettings">Analytics settings</button>
  <aside v-if="prompt" id="analytics-consent" class="analytics-consent" aria-labelledby="analytics-heading"
    @keydown.esc="openedFromSettings && closeSettings()">
    <h2 id="analytics-heading" ref="promptHeading" tabindex="-1">Allow optional analytics?</h2>
    <p>We use PostHog to understand how people use the docs. You can change your choice in Analytics settings. <a :href="privacyHref">Analytics privacy</a></p>
    <div class="analytics-actions">
      <button type="button" @click="choose('declined')">Decline analytics</button>
      <button type="button" @click="choose('accepted')">Accept analytics</button>
    </div>
    <p v-if="saveFailed" role="status">Your browser could not save this choice.</p>
  </aside>
</template>
<style scoped>
.analytics-setting {
  display: inline-flex;
  min-height: 44px;
  align-items: center;
  text-decoration: underline;
}
.analytics-setting:hover, .analytics-consent a { color: var(--vp-c-brand-1); text-decoration: underline; }
.analytics-consent {
  position: fixed;
  z-index: var(--vp-z-index-local-nav);
  inset-inline: 12px;
  inset-block-end: max(12px, env(safe-area-inset-bottom));
  max-inline-size: 32rem;
  max-block-size: min(70dvh, 24rem);
  overflow: auto;
  margin-inline: auto;
  padding: 16px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg);
  box-shadow: var(--vp-shadow-3);
}
.analytics-consent h2 { font-size: 16px; font-weight: 600; }
.analytics-consent p { margin-block: 6px 12px; font-size: 14px; color: var(--vp-c-text-2); }
.analytics-actions { display: flex; flex-wrap: wrap; gap: 8px; }
.analytics-actions button {
  flex: 1 1 8rem;
  min-height: 44px;
  padding: 6px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  font-weight: 600;
}
.analytics-actions button:hover { background: var(--vp-c-bg-soft); }
</style>

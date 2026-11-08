<script setup>
import { ref, onMounted } from "vue";
import { newestVersions } from "./versions.mjs";
const versions = ref([]),
  current = import.meta.env.VITE_DOCS_VERSION || "current";
onMounted(async () => {
  try {
    const response = await fetch("/NornicDB/versions.json");
    if (response.ok) versions.value = newestVersions(await response.json());
  } catch {}
});
function change(event) {
  location.href = event.target.value;
}
</script>
<template>
  <label class="version-picker"
    >Documentation version
    <select
      :value="
        current === 'current'
          ? '/NornicDB/'
          : '/NornicDB/versions/' + current + '/'
      "
      @change="change"
    >
      <option value="/NornicDB/">Current</option>
      <option v-for="v in versions" :key="v.tag" :value="v.url">
        {{ v.tag }}
      </option>
      <option
        v-if="current !== 'current' && !versions.some((v) => v.tag === current)"
        :value="'/NornicDB/versions/' + current + '/'"
      >
        {{ current }}
      </option>
    </select></label
  >
  <p v-if="current !== 'current'" class="status">
    {{ current }} · archived documentation · browser search only
  </p>
</template>
